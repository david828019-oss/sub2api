package service

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// echoWebSocketPlugin 模拟实现了 ForwardWebSocket 的插件：按 open 帧里的 Session_id 决定
// 握手结果，握手成功后回显文本帧，收到 "bye" 时以 1000 关闭。
type echoWebSocketPlugin struct {
	pluginv1.UnimplementedTransportPluginServer
	lastOpen chan *pluginv1.WebSocketOpen
}

func (p *echoWebSocketPlugin) ForwardWebSocket(stream pluginv1.TransportPlugin_ForwardWebSocketServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	p.lastOpen <- open
	switch open.GetHeaders()["Session_id"].GetValues()[0] {
	case "rejected":
		return stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Rejected{Rejected: &pluginv1.WebSocketRejected{
			StatusCode: http.StatusTooManyRequests,
			Headers:    map[string]*pluginv1.HeaderValues{"Retry-After": {Values: []string{"3"}}},
			Body:       []byte("limited"),
		}}})
	case "error":
		return stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Error{Error: &pluginv1.ForwardResponseError{
			Code: "CODEX_RELAY_RELAY_ERROR", Message: "not logged in",
		}}})
	}
	if err := stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Opened{Opened: &pluginv1.WebSocketOpened{
		StatusCode: 101,
		Headers:    map[string]*pluginv1.HeaderValues{"X-Codex-Turn-State": {Values: []string{"turn-1"}}},
	}}}); err != nil {
		return err
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return nil
		}
		message := frame.GetMessage()
		if message == nil {
			return nil
		}
		if string(message.GetPayload()) == "bye" {
			return stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Close{Close: &pluginv1.WebSocketClose{Code: 1000, Reason: "done"}}})
		}
		if err := stream.Send(&pluginv1.WebSocketServerFrame{Frame: &pluginv1.WebSocketServerFrame_Message{Message: &pluginv1.WebSocketMessage{
			Payload: append([]byte("echo:"), message.GetPayload()...),
		}}}); err != nil {
			return err
		}
	}
}

func startEchoWebSocketPlugin(t *testing.T) (*pluginRuntime, *echoWebSocketPlugin) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	plugin := &echoWebSocketPlugin{lastOpen: make(chan *pluginv1.WebSocketOpen, 4)}
	pluginv1.RegisterTransportPluginServer(server, plugin)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	runtime := &pluginRuntime{api: pluginv1.NewTransportPluginClient(conn), supportsWebSocket: true, done: make(chan struct{})}
	return runtime, plugin
}

func wsHeaders(session string) http.Header {
	headers := http.Header{}
	headers.Set("Session_id", session)
	headers.Set("Authorization", "Bearer account-token")
	return headers
}

func TestPluginRuntimeDialWebSocketRelaysFrames(t *testing.T) {
	runtime, plugin := startEchoWebSocketPlugin(t)
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 3}
	require.True(t, runtime.beginRequest())

	conn, status, headers, err := runtime.dialWebSocket(context.Background(), "wss://chatgpt.com/backend-api/codex/responses", wsHeaders("ok"), "http://proxy:8080", account)
	require.NoError(t, err)
	open := <-plugin.lastOpen
	assert.Equal(t, "wss://chatgpt.com/backend-api/codex/responses", open.GetUrl())
	assert.Equal(t, "http://proxy:8080", open.GetProxyUrl())
	assert.Equal(t, int64(7), open.GetAccountId())
	assert.Equal(t, 101, status)
	assert.Equal(t, "turn-1", headers.Get("X-Codex-Turn-State"))

	require.NoError(t, conn.WriteJSON(context.Background(), map[string]string{"type": "response.create"}))
	payload, err := conn.ReadMessage(context.Background())
	require.NoError(t, err)
	assert.Equal(t, `echo:{"type":"response.create"}`, string(payload))

	frameConn, ok := conn.(interface {
		WriteFrame(context.Context, coderws.MessageType, []byte) error
	})
	require.True(t, ok)
	require.NoError(t, frameConn.WriteFrame(context.Background(), coderws.MessageText, []byte("bye")))
	_, err = conn.ReadMessage(context.Background())
	assert.Equal(t, coderws.StatusNormalClosure, coderws.CloseStatus(err))
	_, err = conn.ReadMessage(context.Background())
	assert.Equal(t, coderws.StatusNormalClosure, coderws.CloseStatus(err), "terminal error is sticky")
	assert.ErrorIs(t, conn.Ping(context.Background()), errOpenAIWSConnClosed)

	require.NoError(t, conn.Close())
	assert.Equal(t, int64(0), runtime.inFlight.Load(), "Close releases the in-flight slot")
}

func TestPluginRuntimeDialWebSocketMapsHandshakeFailures(t *testing.T) {
	runtime, _ := startEchoWebSocketPlugin(t)
	account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	conn, status, headers, err := runtime.dialWebSocket(context.Background(), "wss://chatgpt.com/backend-api/codex/responses", wsHeaders("rejected"), "", account)
	assert.Nil(t, conn)
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.Equal(t, "3", headers.Get("Retry-After"))
	var handshakeErr *openAIWSHandshakeError
	require.True(t, errors.As(err, &handshakeErr))
	assert.Equal(t, "limited", string(handshakeErr.Body))

	_, _, _, err = runtime.dialWebSocket(context.Background(), "wss://chatgpt.com/backend-api/codex/responses", wsHeaders("error"), "", account)
	var transportErr *PluginTransportError
	require.True(t, errors.As(err, &transportErr))
	assert.Equal(t, "CODEX_RELAY_RELAY_ERROR", transportErr.Code)
	assert.False(t, transportErr.RequestSent)
}

type recordingOpenAIWSDialer struct{ calls int }

func (d *recordingOpenAIWSDialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	d.calls++
	return nil, 0, nil, io.EOF
}

func TestPluginRoutingOpenAIWSDialerRoutesOnlyPluginAccounts(t *testing.T) {
	manager := &PluginManager{}
	manager.route.Store(&pluginRoute{pluginID: 1, rolloutPercent: 100, runtime: &pluginRuntime{supportsWebSocket: false}})
	inner := &recordingOpenAIWSDialer{}
	dialer := newPluginRoutingOpenAIWSDialer(inner, func() *PluginManager { return manager })

	apiKey := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	_, _, _, err := dialer.Dial(withOpenAIWSDialAccount(context.Background(), apiKey), "wss://api.openai.com/v1/responses", nil, "")
	assert.ErrorIs(t, err, io.EOF)
	_, _, _, err = dialer.Dial(context.Background(), "wss://chatgpt.com/backend-api/codex/responses", nil, "")
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 2, inner.calls, "accounts outside the plugin binding keep the direct dialer")

	oauth := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	_, _, _, err = dialer.Dial(withOpenAIWSDialAccount(context.Background(), oauth), "wss://chatgpt.com/backend-api/codex/responses", nil, "")
	assert.Error(t, err)
	assert.Equal(t, 2, inner.calls, "plugin accounts never fall back to a direct dial")
}

func TestOpenAIOAuthWebSocketRouteFollowsPluginCapability(t *testing.T) {
	oauth := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	apiKey := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	httpOnly := &PluginManager{}
	httpOnly.route.Store(&pluginRoute{pluginID: 1, rolloutPercent: 100, runtime: &pluginRuntime{}})
	unavailable := &PluginManager{}
	unavailable.route.Store(&pluginRoute{pluginID: 1, rolloutPercent: 100, unavailable: "down"})
	withWebSocket := &PluginManager{}
	withWebSocket.route.Store(&pluginRoute{pluginID: 1, rolloutPercent: 100, runtime: &pluginRuntime{supportsWebSocket: true}})

	assert.Equal(t, OpenAIOAuthWebSocketDirect, withWebSocket.OpenAIOAuthWebSocketRouteFor(apiKey))
	assert.Equal(t, OpenAIOAuthWebSocketHTTPOnly, httpOnly.OpenAIOAuthWebSocketRouteFor(oauth))
	assert.Equal(t, OpenAIOAuthWebSocketHTTPOnly, unavailable.OpenAIOAuthWebSocketRouteFor(oauth))
	assert.Equal(t, OpenAIOAuthWebSocketViaPlugin, withWebSocket.OpenAIOAuthWebSocketRouteFor(oauth))

	ws := OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2, Reason: "ws_v2_enabled"}
	assert.Equal(t, "plugin_http_only", (&OpenAIGatewayService{pluginManager: httpOnly}).applyOpenAIWSPluginDecision(oauth, ws).Reason)
	assert.Equal(t, ws, (&OpenAIGatewayService{pluginManager: withWebSocket}).applyOpenAIWSPluginDecision(oauth, ws))
	assert.Equal(t, ws, (&OpenAIGatewayService{}).applyOpenAIWSPluginDecision(oauth, ws))
}
