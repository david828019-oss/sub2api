// Package relay 把 OpenAI OAuth 账号发往 ChatGPT Codex 后端的请求（Responses、compact、
// models、images、files 以及 Responses WebSocket）转发给本机 `codex relay`，
// 其余请求按配置直连上游或拒绝。
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/pluginconfig"
)

const (
	// SecretHeader 与 codex relay 约定的共享密钥请求头。
	SecretHeader = "X-Codex-Relay-Secret"
	// ErrorHeader 是 relay 自身错误的标记：relay 表示未发出上游请求，upstream 表示可能已发出。
	ErrorHeader = "X-Codex-Relay-Error"

	codexPrefix         = "/backend-api/codex"
	filesPrefix         = "/backend-api/files"
	maxDirectTransports = 32
	maxErrorBodyBytes   = 4 * 1024
)

// hopByHopHeaders 在任何方向都不转发。
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length",
}

// codexEndpoints 是 relay 能以 Codex 原生方式处理的 /backend-api/codex 下的端点。
var codexEndpoints = map[string]bool{
	"/responses":          true,
	"/responses/compact":  true,
	"/models":             true,
	"/images/generations": true,
	"/images/edits":       true,
}

// credentialHeaders 只在 passthrough 模式下交给 relay。
var credentialHeaders = []string{"Authorization", "Chatgpt-Account-Id", "Cookie", "X-Openai-Fedramp"}

// Error 描述一次转发失败，RequestSent 如实反映上游是否可能已收到请求。
type Error struct {
	Code        string
	Message     string
	RequestSent bool
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Request 是宿主传入的一次出站请求。
type Request struct {
	Method        string
	URL           string
	Headers       http.Header
	ProxyURL      string
	ContentLength int64
	HasBody       bool
	Body          io.Reader
}

// Transport 持有当前配置与可复用的连接池；配置切换是原子的。
type Transport struct {
	mu    sync.RWMutex
	cfg   pluginconfig.Config
	relay *http.Transport
	// relayWS 是 WebSocket 握手专用客户端：WebSocket 只能跑在 HTTP/1.1 上。
	relayWS *http.Client
	direct  map[string]*http.Transport
}

// New 使用默认配置创建 Transport。
func New() *Transport {
	cfg := pluginconfig.Default()
	return &Transport{cfg: cfg, relay: newRelayTransport(cfg), relayWS: newRelayWebSocketClient(cfg), direct: map[string]*http.Transport{}}
}

// Config 返回当前生效的配置副本。
func (t *Transport) Config() pluginconfig.Config {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cfg
}

// Apply 原子切换配置，并关闭旧连接池的空闲连接。
func (t *Transport) Apply(cfg pluginconfig.Config) {
	t.mu.Lock()
	oldRelay := t.relay
	oldRelayWS := t.relayWS
	oldDirect := t.direct
	t.cfg = cfg
	t.relay = newRelayTransport(cfg)
	t.relayWS = newRelayWebSocketClient(cfg)
	t.direct = map[string]*http.Transport{}
	t.mu.Unlock()
	oldRelay.CloseIdleConnections()
	oldRelayWS.CloseIdleConnections()
	for _, transport := range oldDirect {
		transport.CloseIdleConnections()
	}
}

// Close 释放所有空闲连接。
func (t *Transport) Close() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.relay.CloseIdleConnections()
	t.relayWS.CloseIdleConnections()
	for _, transport := range t.direct {
		transport.CloseIdleConnections()
	}
}

// IsRelayed 判断请求是否应交给 codex relay：ChatGPT 域名下的 Codex Responses（含
// /compact）、models、images 端点，以及 /backend-api/files 文件接口。
func IsRelayed(target *url.URL) bool {
	if target == nil {
		return false
	}
	host := strings.ToLower(target.Hostname())
	if host != "chatgpt.com" && !strings.HasSuffix(host, ".chatgpt.com") {
		return false
	}
	path := strings.TrimRight(target.Path, "/")
	if rest, ok := strings.CutPrefix(path, codexPrefix); ok {
		return codexEndpoints[rest]
	}
	if rest, ok := strings.CutPrefix(path, filesPrefix); ok {
		return rest == "" || strings.HasPrefix(rest, "/")
	}
	return false
}

// IsRelayedWebSocket 判断是否为 ChatGPT Codex Responses WebSocket 地址。
func IsRelayedWebSocket(target *url.URL) bool {
	if target == nil || (target.Scheme != "wss" && target.Scheme != "ws") {
		return false
	}
	host := strings.ToLower(target.Hostname())
	if host != "chatgpt.com" && !strings.HasSuffix(host, ".chatgpt.com") {
		return false
	}
	return strings.TrimRight(target.Path, "/") == codexPrefix+"/responses"
}

// RoundTrip 发送请求：Codex 后端请求走 relay，其余按 unmatched_requests 处理。
func (t *Transport) RoundTrip(ctx context.Context, request Request) (*http.Response, error) {
	target, err := url.Parse(request.URL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, &Error{Code: "CODEX_RELAY_BAD_REQUEST", Message: "宿主请求 URL 无效"}
	}
	t.mu.RLock()
	cfg := t.cfg
	relayTransport := t.relay
	t.mu.RUnlock()

	if IsRelayed(target) {
		if !cfg.Ready() {
			return nil, &Error{Code: "CODEX_RELAY_NOT_CONFIGURED", Message: "插件尚未配置 relay_secret"}
		}
		return t.sendToRelay(ctx, cfg, relayTransport, target, request)
	}
	if cfg.UnmatchedRequests == pluginconfig.UnmatchedReject {
		return nil, &Error{Code: "CODEX_RELAY_UNMATCHED", Message: "该请求不是 relay 支持的 Codex 后端请求，已按配置拒绝"}
	}
	transport, err := t.directTransport(cfg, request.ProxyURL)
	if err != nil {
		return nil, err
	}
	return send(ctx, transport, request.Method, target, cloneHeaders(request.Headers, nil), request)
}

func (t *Transport) sendToRelay(ctx context.Context, cfg pluginconfig.Config, transport *http.Transport, target *url.URL, request Request) (*http.Response, error) {
	relayURL := relayTarget(cfg, target)
	headers := relayHeaders(cfg, request.Headers)
	method := request.Method
	if method == "" {
		method = http.MethodPost
	}

	response, err := send(ctx, transport, method, relayURL, headers, request)
	if err != nil {
		return nil, err
	}
	stage := response.Header.Get(ErrorHeader)
	if stage == "" {
		return response, nil
	}
	message := readRelayError(response)
	return nil, &Error{
		Code:        "CODEX_RELAY_" + strings.ToUpper(stage) + "_ERROR",
		Message:     fmt.Sprintf("codex relay 返回 %d: %s", response.StatusCode, message),
		RequestSent: stage != "relay",
	}
}

// relayTarget 把上游地址映射到 relay：保留路径与查询串，替换协议和主机。
func relayTarget(cfg pluginconfig.Config, target *url.URL) *url.URL {
	relayURL, _ := url.Parse(cfg.RelayURL)
	relayURL.Path = strings.TrimRight(target.Path, "/")
	relayURL.RawQuery = target.RawQuery
	return relayURL
}

// relayHeaders 复制宿主请求头：codex 模式下去掉账号凭据（由 relay 注入本机 Codex 登录），并附上共享密钥。
func relayHeaders(cfg pluginconfig.Config, source http.Header) http.Header {
	var dropped []string
	if cfg.CredentialMode == pluginconfig.CredentialModeCodex {
		dropped = credentialHeaders
	}
	headers := cloneHeaders(source, dropped)
	headers.Set(SecretHeader, cfg.RelaySecret)
	return headers
}

func send(ctx context.Context, transport *http.Transport, method string, target *url.URL, headers http.Header, request Request) (*http.Response, error) {
	body := request.Body
	if !request.HasBody || body == nil {
		body = http.NoBody
	}
	outbound, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, &Error{Code: "CODEX_RELAY_BAD_REQUEST", Message: "无法构造出站请求"}
	}
	outbound.Header = headers
	if request.HasBody && request.ContentLength >= 0 {
		outbound.ContentLength = request.ContentLength
	}
	response, err := transport.RoundTrip(outbound)
	if err != nil {
		return nil, &Error{
			Code:        "CODEX_RELAY_TRANSPORT",
			Message:     sanitizeError(err),
			RequestSent: !isDialError(err),
		}
	}
	return response, nil
}

func (t *Transport) directTransport(cfg pluginconfig.Config, proxyURL string) (*http.Transport, error) {
	proxyURL = strings.TrimSpace(proxyURL)
	t.mu.RLock()
	transport, ok := t.direct[proxyURL]
	t.mu.RUnlock()
	if ok {
		return transport, nil
	}
	var proxy func(*http.Request) (*url.URL, error)
	if proxyURL != "" {
		parsed, err := url.Parse(proxyURL)
		if err != nil || parsed.Host == "" {
			return nil, &Error{Code: "CODEX_RELAY_BAD_PROXY", Message: "账号代理地址无效"}
		}
		proxy = http.ProxyURL(parsed)
	}
	transport = &http.Transport{
		Proxy:                 proxy,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: time.Duration(cfg.DirectResponseHeaderTimeoutSeconds) * time.Second,
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.direct[proxyURL]; ok {
		return existing, nil
	}
	if len(t.direct) >= maxDirectTransports {
		for key, old := range t.direct {
			old.CloseIdleConnections()
			delete(t.direct, key)
		}
	}
	t.direct[proxyURL] = transport
	return transport, nil
}

func newRelayTransport(cfg pluginconfig.Config) *http.Transport {
	return &http.Transport{
		// relay 在本机（或管理员显式允许的地址），永远不走账号代理或环境代理。
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: time.Duration(cfg.ResponseHeaderTimeoutSeconds) * time.Second,
	}
}

// newRelayWebSocketClient 复用 relay 连接参数，但关闭 HTTP/2，WebSocket 握手只能走 HTTP/1.1。
func newRelayWebSocketClient(cfg pluginconfig.Config) *http.Client {
	transport := newRelayTransport(cfg)
	transport.ForceAttemptHTTP2 = false
	return &http.Client{Transport: transport}
}

func cloneHeaders(source http.Header, dropped []string) http.Header {
	headers := source.Clone()
	if headers == nil {
		headers = http.Header{}
	}
	for _, name := range hopByHopHeaders {
		headers.Del(name)
	}
	for _, name := range dropped {
		headers.Del(name)
	}
	headers.Del(SecretHeader)
	return headers
}

func readRelayError(response *http.Response) string {
	defer func() { _ = response.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &payload) == nil && payload.Error.Message != "" {
		return payload.Error.Message
	}
	return http.StatusText(response.StatusCode)
}

func isDialError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// sanitizeError 只保留错误类别，避免把 URL 中的代理凭据等敏感信息带入日志或宿主提示。
func sanitizeError(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "请求已取消"
	case errors.Is(err, context.DeadlineExceeded):
		return "请求超时"
	case isDialError(err):
		return "无法连接目标地址"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "等待响应超时"
	}
	return "网络请求失败"
}
