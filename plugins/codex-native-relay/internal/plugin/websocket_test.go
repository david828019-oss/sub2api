package plugin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	coderws "github.com/coder/websocket"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/relay"
)

func TestForwardWebSocketRelaysFrames(t *testing.T) {
	fakeRelay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(relay.SecretHeader) != testSecret || r.Header.Get("Authorization") != "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, payload, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		_ = conn.Write(r.Context(), coderws.MessageText, append([]byte("echo:"), payload...))
		_ = conn.Close(coderws.StatusNormalClosure, "turn done")
	}))
	defer fakeRelay.Close()
	client := startClient(t)
	applyConfig(t, client, fakeRelay.URL)

	stream, err := client.ForwardWebSocket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Open{Open: &pluginv1.WebSocketOpen{
		Url:     "wss://chatgpt.com/backend-api/codex/responses",
		Headers: map[string]*pluginv1.HeaderValues{"Authorization": {Values: []string{"Bearer account"}}},
	}}}); err != nil {
		t.Fatal(err)
	}
	opened, err := stream.Recv()
	if err != nil || opened.GetOpened().GetStatusCode() != 101 {
		t.Fatalf("opened = %v, %v", opened, err)
	}
	if err := stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Message{Message: &pluginv1.WebSocketMessage{
		Payload: []byte(`{"type":"response.create"}`),
	}}}); err != nil {
		t.Fatal(err)
	}
	message, err := stream.Recv()
	if err != nil || string(message.GetMessage().GetPayload()) != `echo:{"type":"response.create"}` || message.GetMessage().GetBinary() {
		t.Fatalf("message = %v, %v", message, err)
	}
	closed, err := stream.Recv()
	if err != nil || closed.GetClose().GetCode() != int32(coderws.StatusNormalClosure) || closed.GetClose().GetReason() != "turn done" {
		t.Fatalf("close = %v, %v", closed, err)
	}
}

func TestForwardWebSocketReturnsUpstreamRejection(t *testing.T) {
	fakeRelay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("limited"))
	}))
	defer fakeRelay.Close()
	client := startClient(t)
	applyConfig(t, client, fakeRelay.URL)

	stream, err := client.ForwardWebSocket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pluginv1.WebSocketClientFrame{Frame: &pluginv1.WebSocketClientFrame_Open{Open: &pluginv1.WebSocketOpen{
		Url: "wss://chatgpt.com/backend-api/codex/responses",
	}}}); err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Recv()
	if err != nil || frame.GetRejected().GetStatusCode() != http.StatusTooManyRequests || string(frame.GetRejected().GetBody()) != "limited" {
		t.Fatalf("rejected = %v, %v", frame, err)
	}
}
