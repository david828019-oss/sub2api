// Package pluginconfig 定义 Codex Native Relay 插件的配置结构、默认值和严格校验。
package pluginconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
)

const (
	// CredentialModeCodex 表示由 Codex relay 使用本机 `codex login` 的账号，插件不向 relay 发送账号 Token。
	CredentialModeCodex = "codex"
	// CredentialModePassthrough 表示把 Sub2API 账号的 Authorization / ChatGPT-Account-Id 交给 relay，
	// relay 以 Codex 原生客户端身份代发（需以 `codex relay --auth passthrough` 启动）。
	CredentialModePassthrough = "passthrough"

	// UnmatchedDirect 表示非 Codex Responses 请求（图片、文件等）由插件按原样直连上游。
	UnmatchedDirect = "direct"
	// UnmatchedReject 表示非 Codex Responses 请求直接失败，且不会发出上游请求。
	UnmatchedReject = "reject"

	DefaultRelayURL                    = "http://127.0.0.1:8788"
	DefaultResponseHeaderTimeout       = 300
	DefaultDirectResponseHeaderTimeout = 120
	maxTimeoutSeconds                  = 3600
	minSecretLength                    = 16
)

// Config 是插件持久化配置，字段统一使用 snake_case。
type Config struct {
	// RelayURL 是 `codex relay` 的监听地址，例如 http://127.0.0.1:8788。
	RelayURL string `json:"relay_url"`
	// RelaySecret 与 relay 的 CODEX_RELAY_SECRET 一致，通过 X-Codex-Relay-Secret 请求头发送。
	RelaySecret string `json:"relay_secret"`
	// CredentialMode 必须与 relay 的 --auth 参数一致：codex 或 passthrough。
	CredentialMode string `json:"credential_mode"`
	// AllowRemoteRelay 允许 relay_url 指向非回环地址（此时强烈建议使用 https）。
	AllowRemoteRelay bool `json:"allow_remote_relay"`
	// UnmatchedRequests 决定非 Codex Responses 请求的处理方式：direct 或 reject。
	UnmatchedRequests string `json:"unmatched_requests"`
	// ResponseHeaderTimeoutSeconds 是等待 relay 返回响应头的超时。
	ResponseHeaderTimeoutSeconds int `json:"response_header_timeout_seconds"`
	// DirectResponseHeaderTimeoutSeconds 是直连请求等待响应头的超时。
	DirectResponseHeaderTimeoutSeconds int `json:"direct_response_header_timeout_seconds"`
}

// Default 返回完整默认配置。默认没有密钥，因此需要管理员保存一次配置后才能转发。
func Default() Config {
	return Config{
		RelayURL:                           DefaultRelayURL,
		CredentialMode:                     CredentialModeCodex,
		UnmatchedRequests:                  UnmatchedDirect,
		ResponseHeaderTimeoutSeconds:       DefaultResponseHeaderTimeout,
		DirectResponseHeaderTimeoutSeconds: DefaultDirectResponseHeaderTimeout,
	}
}

// Parse 严格解析配置 JSON（拒绝未知字段），用默认值补全缺省字段并校验。
// 空输入视为空对象。
func Parse(raw []byte) (Config, error) {
	cfg := Default()
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) {
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("配置 JSON 无效: %w", err)
		}
		if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
			return Config{}, errors.New("配置 JSON 只能包含一个对象")
		}
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) normalize() {
	c.RelayURL = strings.TrimRight(strings.TrimSpace(c.RelayURL), "/")
	c.RelaySecret = strings.TrimSpace(c.RelaySecret)
	c.CredentialMode = strings.ToLower(strings.TrimSpace(c.CredentialMode))
	c.UnmatchedRequests = strings.ToLower(strings.TrimSpace(c.UnmatchedRequests))
	if c.RelayURL == "" {
		c.RelayURL = DefaultRelayURL
	}
	if c.CredentialMode == "" {
		c.CredentialMode = CredentialModeCodex
	}
	if c.UnmatchedRequests == "" {
		c.UnmatchedRequests = UnmatchedDirect
	}
	if c.ResponseHeaderTimeoutSeconds == 0 {
		c.ResponseHeaderTimeoutSeconds = DefaultResponseHeaderTimeout
	}
	if c.DirectResponseHeaderTimeoutSeconds == 0 {
		c.DirectResponseHeaderTimeoutSeconds = DefaultDirectResponseHeaderTimeout
	}
}

// Validate 校验已规范化的配置。
func (c Config) Validate() error {
	parsed, err := url.Parse(c.RelayURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("relay_url 必须是 http:// 或 https:// 开头的绝对地址")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return errors.New("relay_url 只能包含协议、主机和端口")
	}
	if !c.AllowRemoteRelay && !isLoopbackHost(parsed.Hostname()) {
		return errors.New("relay_url 不是本机回环地址；如确需远程 relay，请开启 allow_remote_relay 并使用 https")
	}
	if c.RelaySecret != "" && len(c.RelaySecret) < minSecretLength {
		return fmt.Errorf("relay_secret 至少需要 %d 个字符", minSecretLength)
	}
	if strings.ContainsAny(c.RelaySecret, "\r\n") {
		return errors.New("relay_secret 不能包含换行")
	}
	switch c.CredentialMode {
	case CredentialModeCodex, CredentialModePassthrough:
	default:
		return errors.New("credential_mode 只能是 codex 或 passthrough")
	}
	switch c.UnmatchedRequests {
	case UnmatchedDirect, UnmatchedReject:
	default:
		return errors.New("unmatched_requests 只能是 direct 或 reject")
	}
	if c.ResponseHeaderTimeoutSeconds < 1 || c.ResponseHeaderTimeoutSeconds > maxTimeoutSeconds {
		return fmt.Errorf("response_header_timeout_seconds 必须在 1 到 %d 之间", maxTimeoutSeconds)
	}
	if c.DirectResponseHeaderTimeoutSeconds < 1 || c.DirectResponseHeaderTimeoutSeconds > maxTimeoutSeconds {
		return fmt.Errorf("direct_response_header_timeout_seconds 必须在 1 到 %d 之间", maxTimeoutSeconds)
	}
	return nil
}

// Ready 返回配置是否足以转发请求（必须已设置密钥）。
func (c Config) Ready() bool {
	return c.RelaySecret != ""
}

// JSON 返回规范化后的完整配置 JSON。
func (c Config) JSON() []byte {
	raw, _ := json.Marshal(c)
	return raw
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
