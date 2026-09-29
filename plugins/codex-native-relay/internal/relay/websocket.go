package relay

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	coderws "github.com/coder/websocket"

	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/pluginconfig"
)

// maxWebSocketMessageBytes 与 sub2api 宿主一致放宽单帧上限，Codex 事件可能很大。
const maxWebSocketMessageBytes = 64 << 20

// webSocketHandshakeHeaders 由 WebSocket 客户端自行生成，不能沿用宿主的值。
var webSocketHandshakeHeaders = []string{
	"Sec-Websocket-Key", "Sec-Websocket-Version", "Sec-Websocket-Extensions", "Sec-Websocket-Protocol",
}

// WebSocketRequest 是宿主交给插件的一次上游 WebSocket 建连请求。
type WebSocketRequest struct {
	URL      string
	Headers  http.Header
	ProxyURL string
}

// Rejected 表示上游以普通 HTTP 响应拒绝了握手（例如 401、429），需原样交还宿主。
type Rejected struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func (r *Rejected) Error() string {
	return "upstream rejected websocket handshake: " + http.StatusText(r.StatusCode)
}

// OpenedWebSocket 是已经通过 relay 建立的上游 WebSocket。
type OpenedWebSocket struct {
	Conn    *coderws.Conn
	Headers http.Header
}

// DialWebSocket 通过 relay 打开 Codex Responses WebSocket。
// 错误为 *Rejected（上游 HTTP 拒绝）或 *Error（relay 或网络错误）。
func (t *Transport) DialWebSocket(ctx context.Context, request WebSocketRequest) (*OpenedWebSocket, error) {
	target, err := url.Parse(request.URL)
	if err != nil || target.Host == "" {
		return nil, &Error{Code: "CODEX_RELAY_BAD_REQUEST", Message: "宿主 WebSocket URL 无效"}
	}
	t.mu.RLock()
	cfg := t.cfg
	client := t.relayWS
	t.mu.RUnlock()

	if !IsRelayedWebSocket(target) {
		// 非 Codex Responses 的 WebSocket 没有 relay 端点可用，也不允许绕过 relay 直连。
		return nil, &Error{Code: "CODEX_RELAY_UNMATCHED", Message: "该 WebSocket 不是 Codex Responses 地址"}
	}
	if !cfg.Ready() {
		return nil, &Error{Code: "CODEX_RELAY_NOT_CONFIGURED", Message: "插件尚未配置 relay_secret"}
	}
	relayURL := relayTarget(cfg, target)
	relayURL.Scheme = webSocketScheme(cfg)
	headers := relayHeaders(cfg, request.Headers)
	for _, name := range webSocketHandshakeHeaders {
		headers.Del(name)
	}

	conn, response, err := coderws.Dial(ctx, relayURL.String(), &coderws.DialOptions{
		HTTPClient:      client,
		HTTPHeader:      headers,
		CompressionMode: coderws.CompressionDisabled,
	})
	if err != nil {
		return nil, handshakeError(response, err)
	}
	conn.SetReadLimit(maxWebSocketMessageBytes)
	var responseHeaders http.Header
	if response != nil {
		responseHeaders = cloneHeaders(response.Header, webSocketHandshakeHeaders)
	}
	return &OpenedWebSocket{Conn: conn, Headers: responseHeaders}, nil
}

func webSocketScheme(cfg pluginconfig.Config) string {
	if strings.HasPrefix(strings.ToLower(cfg.RelayURL), "https://") {
		return "wss"
	}
	return "ws"
}

// handshakeError 区分三种失败：relay 自身错误（带 X-Codex-Relay-Error）、上游 HTTP 拒绝、网络错误。
func handshakeError(response *http.Response, err error) error {
	if response == nil {
		return &Error{Code: "CODEX_RELAY_TRANSPORT", Message: sanitizeError(err), RequestSent: !isDialError(err)}
	}
	if stage := response.Header.Get(ErrorHeader); stage != "" {
		message := readRelayError(response)
		return &Error{
			Code:        "CODEX_RELAY_" + strings.ToUpper(stage) + "_ERROR",
			Message:     "codex relay 返回 " + http.StatusText(response.StatusCode) + ": " + message,
			RequestSent: stage != "relay",
		}
	}
	var body []byte
	if response.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(response.Body, 8<<10))
		_ = response.Body.Close()
	}
	return &Rejected{StatusCode: response.StatusCode, Headers: cloneHeaders(response.Header, nil), Body: body}
}
