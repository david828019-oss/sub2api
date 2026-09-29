package service

import (
	"context"
	"net/http"
)

type openAIWSDialAccountKey struct{}

// withOpenAIWSDialAccount 把发起建连的账号挂到 ctx 上。openAIWSClientDialer.Dial 的签名
// 不带账号，插件路由据此判断该连接是否需要交给 OpenAI OAuth 插件。
func withOpenAIWSDialAccount(ctx context.Context, account *Account) context.Context {
	if ctx == nil || account == nil {
		return ctx
	}
	return context.WithValue(ctx, openAIWSDialAccountKey{}, account)
}

func openAIWSDialAccountFromContext(ctx context.Context) *Account {
	if ctx == nil {
		return nil
	}
	account, _ := ctx.Value(openAIWSDialAccountKey{}).(*Account)
	return account
}

// pluginRoutingOpenAIWSDialer 在命中 OpenAI OAuth 插件的账号上改由插件建立上游 WebSocket，
// 其余账号原样交给内层 dialer。
type pluginRoutingOpenAIWSDialer struct {
	inner   openAIWSClientDialer
	manager func() *PluginManager
}

func newPluginRoutingOpenAIWSDialer(inner openAIWSClientDialer, manager func() *PluginManager) openAIWSClientDialer {
	if inner == nil {
		return nil
	}
	return &pluginRoutingOpenAIWSDialer{inner: inner, manager: manager}
}

func (d *pluginRoutingOpenAIWSDialer) Dial(ctx context.Context, wsURL string, headers http.Header, proxyURL string) (openAIWSClientConn, int, http.Header, error) {
	if account := openAIWSDialAccountFromContext(ctx); account != nil && d.manager != nil {
		if manager := d.manager(); manager != nil {
			conn, status, respHeaders, handled, err := manager.DialOpenAIOAuthWebSocket(ctx, wsURL, headers, proxyURL, account)
			if handled {
				return conn, status, respHeaders, err
			}
		}
	}
	return d.inner.Dial(ctx, wsURL, headers, proxyURL)
}

func (d *pluginRoutingOpenAIWSDialer) SnapshotTransportMetrics() OpenAIWSTransportMetricsSnapshot {
	if metrics, ok := d.inner.(openAIWSTransportMetricsDialer); ok {
		return metrics.SnapshotTransportMetrics()
	}
	return OpenAIWSTransportMetricsSnapshot{}
}

// applyOpenAIWSPluginDecision 在账号命中只支持 HTTP 的插件时把 WS 上游决策降级为 HTTP，
// 保证流量不会绕过插件直连上游。
func (s *OpenAIGatewayService) applyOpenAIWSPluginDecision(account *Account, decision OpenAIWSProtocolDecision) OpenAIWSProtocolDecision {
	if s == nil || s.pluginManager == nil || decision.Transport == OpenAIUpstreamTransportHTTPSSE {
		return decision
	}
	if s.pluginManager.OpenAIOAuthWebSocketRouteFor(account) == OpenAIOAuthWebSocketHTTPOnly {
		return openAIWSHTTPDecision("plugin_http_only")
	}
	return decision
}

func (s *OpenAIGatewayService) currentPluginManager() *PluginManager {
	if s == nil {
		return nil
	}
	return s.pluginManager
}
