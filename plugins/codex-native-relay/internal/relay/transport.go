// Package relay 把 OpenAI OAuth 账号的 Codex Responses 请求转发给本机 `codex relay`，
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

	codexResponsesPath  = "/backend-api/codex/responses"
	maxDirectTransports = 32
	maxErrorBodyBytes   = 4 * 1024
)

// hopByHopHeaders 在任何方向都不转发。
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length",
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
	mu     sync.RWMutex
	cfg    pluginconfig.Config
	relay  *http.Transport
	direct map[string]*http.Transport
}

// New 使用默认配置创建 Transport。
func New() *Transport {
	cfg := pluginconfig.Default()
	return &Transport{cfg: cfg, relay: newRelayTransport(cfg), direct: map[string]*http.Transport{}}
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
	oldDirect := t.direct
	t.cfg = cfg
	t.relay = newRelayTransport(cfg)
	t.direct = map[string]*http.Transport{}
	t.mu.Unlock()
	oldRelay.CloseIdleConnections()
	for _, transport := range oldDirect {
		transport.CloseIdleConnections()
	}
}

// Close 释放所有空闲连接。
func (t *Transport) Close() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.relay.CloseIdleConnections()
	for _, transport := range t.direct {
		transport.CloseIdleConnections()
	}
}

// IsCodexResponses 判断请求是否为 ChatGPT Codex Responses（含 /compact）端点。
func IsCodexResponses(target *url.URL) bool {
	if target == nil {
		return false
	}
	host := strings.ToLower(target.Hostname())
	if host != "chatgpt.com" && !strings.HasSuffix(host, ".chatgpt.com") {
		return false
	}
	path := strings.TrimRight(target.Path, "/")
	return path == codexResponsesPath || path == codexResponsesPath+"/compact"
}

// RoundTrip 发送请求：Codex Responses 走 relay，其余按 unmatched_requests 处理。
func (t *Transport) RoundTrip(ctx context.Context, request Request) (*http.Response, error) {
	target, err := url.Parse(request.URL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, &Error{Code: "CODEX_RELAY_BAD_REQUEST", Message: "宿主请求 URL 无效"}
	}
	t.mu.RLock()
	cfg := t.cfg
	relayTransport := t.relay
	t.mu.RUnlock()

	if IsCodexResponses(target) {
		if !cfg.Ready() {
			return nil, &Error{Code: "CODEX_RELAY_NOT_CONFIGURED", Message: "插件尚未配置 relay_secret"}
		}
		return t.sendToRelay(ctx, cfg, relayTransport, target, request)
	}
	if cfg.UnmatchedRequests == pluginconfig.UnmatchedReject {
		return nil, &Error{Code: "CODEX_RELAY_UNMATCHED", Message: "该请求不是 Codex Responses 请求，已按配置拒绝"}
	}
	transport, err := t.directTransport(cfg, request.ProxyURL)
	if err != nil {
		return nil, err
	}
	return send(ctx, transport, request.Method, target, cloneHeaders(request.Headers, nil), request)
}

func (t *Transport) sendToRelay(ctx context.Context, cfg pluginconfig.Config, transport *http.Transport, target *url.URL, request Request) (*http.Response, error) {
	relayURL, _ := url.Parse(cfg.RelayURL)
	relayURL.Path = strings.TrimRight(target.Path, "/")
	var dropped []string
	if cfg.CredentialMode == pluginconfig.CredentialModeCodex {
		dropped = credentialHeaders
	}
	headers := cloneHeaders(request.Headers, dropped)
	headers.Set(SecretHeader, cfg.RelaySecret)

	response, err := send(ctx, transport, http.MethodPost, relayURL, headers, request)
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
