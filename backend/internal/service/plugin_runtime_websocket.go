package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
)

// pluginWebSocketFrameBuffer 是插件 WS 读协程向调用方交付帧的缓冲深度。
const pluginWebSocketFrameBuffer = 64

// dialWebSocket 通过插件的 ForwardWebSocket 打开一条上游 WebSocket。
// 返回值与 openAIWSClientDialer.Dial 一致：握手被上游以 HTTP 响应拒绝时返回状态码、
// 响应头和 *openAIWSHandshakeError，便于现有重试/换号逻辑按原样处理。
// 流的生命周期独立于 ctx（ctx 只约束握手），与 coder/websocket 的 Dial 语义一致。
func (r *pluginRuntime) dialWebSocket(ctx context.Context, wsURL string, headers http.Header, proxyURL string, account *Account) (openAIWSClientConn, int, http.Header, error) {
	if account == nil {
		return nil, 0, nil, errors.New("插件 WebSocket 请求参数不完整")
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := r.api.ForwardWebSocket(streamCtx)
	if err != nil {
		cancel()
		return nil, 0, nil, normalizePluginRPCError(ctx, "创建插件 WebSocket 流", err, false)
	}
	requestID := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(account.ID, 36)
	if err := stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Open{Open: &pluginv1.WebSocketOpen{
		RequestId:          requestID,
		Url:                wsURL,
		Headers:            headersToPlugin(headers),
		ProxyUrl:           proxyURL,
		AccountId:          account.ID,
		AccountConcurrency: int32(account.Concurrency),
		Platform:           account.Platform,
		AccountType:        account.Type,
	}}}); err != nil {
		cancel()
		return nil, 0, nil, normalizePluginRPCError(ctx, "发送插件 WebSocket 握手", err, true)
	}

	type recvResult struct {
		frame *pluginv1.WebSocketServerFrame
		err   error
	}
	firstFrame := make(chan recvResult, 1)
	go func() {
		frame, err := stream.Recv()
		firstFrame <- recvResult{frame: frame, err: err}
	}()
	var first recvResult
	select {
	case first = <-firstFrame:
	case <-ctx.Done():
		cancel()
		return nil, 0, nil, ctx.Err()
	}
	if first.err != nil {
		cancel()
		return nil, 0, nil, normalizePluginRPCError(ctx, "接收插件 WebSocket 握手结果", first.err, true)
	}
	switch frame := first.frame.GetFrame().(type) {
	case *pluginv1.WebSocketServerFrame_Opened:
		conn := &pluginOpenAIWSClientConn{
			stream: stream,
			cancel: cancel,
			done:   r.finishRequest,
			frames: make(chan pluginWebSocketFrame, pluginWebSocketFrameBuffer),
		}
		go conn.readLoop()
		return conn, int(frame.Opened.GetStatusCode()), headersFromPlugin(frame.Opened.GetHeaders()), nil
	case *pluginv1.WebSocketServerFrame_Rejected:
		cancel()
		status := int(frame.Rejected.GetStatusCode())
		return nil, status, headersFromPlugin(frame.Rejected.GetHeaders()), &openAIWSHandshakeError{
			Body: frame.Rejected.GetBody(),
			Err:  fmt.Errorf("upstream rejected websocket handshake with status %d", status),
		}
	case *pluginv1.WebSocketServerFrame_Error:
		cancel()
		return nil, 0, nil, &PluginTransportError{Code: frame.Error.GetCode(), Message: frame.Error.GetMessage(), RequestSent: frame.Error.GetRequestSent()}
	default:
		cancel()
		return nil, 0, nil, &PluginTransportError{Code: "PLUGIN_INVALID_RESPONSE", Message: "插件未返回有效的 WebSocket 握手结果", RequestSent: true}
	}
}

type pluginWebSocketFrame struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

// pluginOpenAIWSClientConn 把插件 ForwardWebSocket 流适配为 openAIWSClientConn 与
// openaiwsv2.FrameConn，供连接池（ctx_pool）和 passthrough 两条路径复用。
type pluginOpenAIWSClientConn struct {
	stream pluginv1.TransportPlugin_ForwardWebSocketClient
	cancel context.CancelFunc
	done   func()

	sendMu    sync.Mutex
	frames    chan pluginWebSocketFrame
	closed    atomic.Bool
	closeOnce sync.Once
	// terminal 缓存读协程结束时的错误，此后所有读取都返回它。
	terminal atomic.Pointer[error]
}

var _ openAIWSClientConn = (*pluginOpenAIWSClientConn)(nil)
var _ openaiwsv2.FrameConn = (*pluginOpenAIWSClientConn)(nil)

// deliver 把帧交给读取方；连接关闭（流 ctx 取消）后放弃，避免读协程永久阻塞。
func (c *pluginOpenAIWSClientConn) deliver(frame pluginWebSocketFrame) bool {
	select {
	case c.frames <- frame:
		return true
	case <-c.stream.Context().Done():
		return false
	}
}

func (c *pluginOpenAIWSClientConn) readLoop() {
	defer close(c.frames)
	for {
		frame, err := c.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errOpenAIWSConnClosed
			} else {
				err = normalizePluginRPCError(c.stream.Context(), "接收插件 WebSocket 帧", err, true)
			}
			c.deliver(pluginWebSocketFrame{err: err})
			return
		}
		switch payload := frame.GetFrame().(type) {
		case *pluginv1.WebSocketServerFrame_Message:
			messageType := coderws.MessageText
			if payload.Message.GetBinary() {
				messageType = coderws.MessageBinary
			}
			if !c.deliver(pluginWebSocketFrame{messageType: messageType, payload: payload.Message.GetPayload()}) {
				return
			}
		case *pluginv1.WebSocketServerFrame_Close:
			c.deliver(pluginWebSocketFrame{err: coderws.CloseError{
				Code:   coderws.StatusCode(payload.Close.GetCode()),
				Reason: payload.Close.GetReason(),
			}})
			return
		case *pluginv1.WebSocketServerFrame_Error:
			c.deliver(pluginWebSocketFrame{err: &PluginTransportError{
				Code:        payload.Error.GetCode(),
				Message:     payload.Error.GetMessage(),
				RequestSent: payload.Error.GetRequestSent(),
			}})
			return
		}
	}
}

func (c *pluginOpenAIWSClientConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	if c == nil {
		return coderws.MessageText, nil, errOpenAIWSConnClosed
	}
	if terminal := c.terminal.Load(); terminal != nil {
		return coderws.MessageText, nil, *terminal
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case frame, ok := <-c.frames:
		if !ok {
			return coderws.MessageText, nil, errOpenAIWSConnClosed
		}
		if frame.err != nil {
			err := frame.err
			c.terminal.Store(&err)
			return coderws.MessageText, nil, err
		}
		return frame.messageType, frame.payload, nil
	case <-ctx.Done():
		return coderws.MessageText, nil, ctx.Err()
	}
}

func (c *pluginOpenAIWSClientConn) ReadMessage(ctx context.Context) ([]byte, error) {
	_, payload, err := c.ReadFrame(ctx)
	return payload, err
}

func (c *pluginOpenAIWSClientConn) WriteFrame(ctx context.Context, messageType coderws.MessageType, payload []byte) error {
	if c == nil || c.closed.Load() {
		return errOpenAIWSConnClosed
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	err := c.stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Message{Message: &pluginv1.WebSocketMessage{
		Binary:  messageType == coderws.MessageBinary,
		Payload: payload,
	}}})
	if err != nil {
		return normalizePluginRPCError(ctx, "发送插件 WebSocket 帧", err, true)
	}
	return nil
}

func (c *pluginOpenAIWSClientConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, payload)
}

// Ping 不经过插件：插件与 relay 之间的保活由插件自行维护，这里只报告流是否仍然存活。
func (c *pluginOpenAIWSClientConn) Ping(context.Context) error {
	if c == nil || c.closed.Load() || c.terminal.Load() != nil {
		return errOpenAIWSConnClosed
	}
	if c.stream.Context().Err() != nil {
		return errOpenAIWSConnClosed
	}
	return nil
}

func (c *pluginOpenAIWSClientConn) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.sendMu.Lock()
		_ = c.stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Close{Close: &pluginv1.WebSocketClose{
			Code: int32(coderws.StatusNormalClosure),
		}}})
		_ = c.stream.CloseSend()
		c.sendMu.Unlock()
		c.cancel()
		if c.done != nil {
			c.done()
		}
	})
	return nil
}

func (c *pluginOpenAIWSClientConn) CloseNow() error {
	return c.Close()
}
