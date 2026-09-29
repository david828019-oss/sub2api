package relay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	coderws "github.com/coder/websocket"

	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/pluginconfig"
)

func wsRequest() WebSocketRequest {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer account-token")
	headers.Set("Session_id", "sess-1")
	headers.Set("Sec-Websocket-Key", "host-generated")
	return WebSocketRequest{URL: "wss://chatgpt.com/backend-api/codex/responses", Headers: headers}
}

func TestDialWebSocketBridgesThroughRelay(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := map[string]string{
			"path":          r.URL.Path,
			"secret":        r.Header.Get(SecretHeader),
			"authorization": r.Header.Get("Authorization"),
			"session":       r.Header.Get("Session_id"),
		}
		want := map[string]string{
			"path":          "/backend-api/codex/responses",
			"secret":        testSecret,
			"authorization": "",
			"session":       "sess-1",
		}
		for key, value := range want {
			if got[key] != value {
				t.Errorf("%s = %q, want %q", key, got[key], value)
			}
		}
		w.Header().Set("X-Codex-Turn-State", "turn-1")
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		messageType, payload, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		_ = conn.Write(r.Context(), messageType, append([]byte("echo:"), payload...))
		_ = conn.Close(coderws.StatusNormalClosure, "done")
	}))
	defer relay.Close()
	transport := configured(t, relay.URL, pluginconfig.CredentialModeCodex)

	opened, err := transport.DialWebSocket(context.Background(), wsRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = opened.Conn.CloseNow() }()
	if opened.Headers.Get("X-Codex-Turn-State") != "turn-1" {
		t.Errorf("handshake headers = %v", opened.Headers)
	}
	if err := opened.Conn.Write(context.Background(), coderws.MessageText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	_, payload, err := opened.Conn.Read(context.Background())
	if err != nil || string(payload) != "echo:hi" {
		t.Fatalf("read = %q, %v", payload, err)
	}
	_, _, err = opened.Conn.Read(context.Background())
	if coderws.CloseStatus(err) != coderws.StatusNormalClosure {
		t.Fatalf("close = %v", err)
	}
}

func TestDialWebSocketClassifiesHandshakeFailures(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Session_id") == "relay-error" {
			w.Header().Set(ErrorHeader, "relay")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"message":"Codex is not logged in"}}`))
			return
		}
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"usage_limit_reached"}}`))
	}))
	defer relay.Close()
	transport := configured(t, relay.URL, pluginconfig.CredentialModeCodex)

	_, err := transport.DialWebSocket(context.Background(), wsRequest())
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.StatusCode != http.StatusTooManyRequests ||
		rejected.Headers.Get("Retry-After") != "3" || string(rejected.Body) != `{"error":{"type":"usage_limit_reached"}}` {
		t.Fatalf("rejected = %#v, err = %v", rejected, err)
	}

	request := wsRequest()
	request.Headers.Set("Session_id", "relay-error")
	_, err = transport.DialWebSocket(context.Background(), request)
	var relayErr *Error
	if !errors.As(err, &relayErr) || relayErr.RequestSent || relayErr.Code != "CODEX_RELAY_RELAY_ERROR" {
		t.Fatalf("relay error = %#v", err)
	}

	request = wsRequest()
	request.URL = "wss://chatgpt.com/backend-api/codex/realtime"
	_, err = transport.DialWebSocket(context.Background(), request)
	if !errors.As(err, &relayErr) || relayErr.Code != "CODEX_RELAY_UNMATCHED" {
		t.Fatalf("unmatched = %#v", err)
	}
}
