// Package plugin 实现 Sub2API TransportPlugin gRPC 服务。
package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/pluginconfig"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/relay"
)

const (
	// ID 必须与 manifest.source.json 一致。
	ID         = "codex.native.relay"
	capability = "openai.oauth.outbound_transport.v1"
	chunkSize  = 32 * 1024
)

// Version 由构建时 -ldflags 注入，必须与清单版本一致。
var Version = "0.1.0"

// Server 实现 pluginv1.TransportPluginServer。
type Server struct {
	pluginv1.UnimplementedTransportPluginServer
	transport *relay.Transport
	// healthClient 仅用于 TestConfig 的主动探测。
	healthClient *http.Client
}

// NewServer 创建使用默认配置的插件服务。
func NewServer() *Server {
	return &Server{transport: relay.New(), healthClient: &http.Client{Timeout: 5 * time.Second}}
}

func (s *Server) GetInfo(context.Context, *pluginv1.GetInfoRequest) (*pluginv1.GetInfoResponse, error) {
	return &pluginv1.GetInfoResponse{
		PluginId:            ID,
		PluginVersion:       Version,
		ProtocolVersion:     pluginv1.ProtocolVersion,
		TransportApiVersion: pluginv1.TransportAPIVersion,
		Capabilities:        []string{capability},
	}, nil
}

func (s *Server) Health(context.Context, *pluginv1.HealthRequest) (*pluginv1.HealthResponse, error) {
	cfg := s.transport.Config()
	status, _ := json.Marshal(map[string]any{
		"relay_url":       cfg.RelayURL,
		"credential_mode": cfg.CredentialMode,
		"configured":      cfg.Ready(),
	})
	message := "ready"
	if !cfg.Ready() {
		message = "relay_secret 未配置，Codex Responses 请求会失败"
	}
	return &pluginv1.HealthResponse{Healthy: true, Message: message, StatusJson: string(status)}, nil
}

func (s *Server) ValidateConfig(_ context.Context, request *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	cfg, err := pluginconfig.Parse(request.GetConfigJson())
	if err != nil {
		return &pluginv1.ValidateConfigResponse{Valid: false, Message: err.Error()}, nil
	}
	return &pluginv1.ValidateConfigResponse{Valid: true, Message: "ok", NormalizedConfigJson: cfg.JSON()}, nil
}

func (s *Server) ApplyConfig(_ context.Context, request *pluginv1.ApplyConfigRequest) (*pluginv1.ApplyConfigResponse, error) {
	cfg, err := pluginconfig.Parse(request.GetConfigJson())
	if err != nil {
		return &pluginv1.ApplyConfigResponse{Applied: false, Message: err.Error()}, nil
	}
	s.transport.Apply(cfg)
	return &pluginv1.ApplyConfigResponse{Applied: true, Message: "ok"}, nil
}

// TestConfig 调用 relay 的 /healthz，验证地址、密钥以及 relay 的认证模式是否匹配。
func (s *Server) TestConfig(ctx context.Context, request *pluginv1.TestConfigRequest) (*pluginv1.TestConfigResponse, error) {
	cfg := s.transport.Config()
	if raw := request.GetConfigJson(); len(raw) > 0 {
		parsed, err := pluginconfig.Parse(raw)
		if err != nil {
			return &pluginv1.TestConfigResponse{Success: false, Message: err.Error()}, nil
		}
		cfg = parsed
	}
	if !cfg.Ready() {
		return &pluginv1.TestConfigResponse{Success: false, Message: "请先填写 relay_secret"}, nil
	}
	started := time.Now()
	probe, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.RelayURL+"/healthz", nil)
	if err != nil {
		return &pluginv1.TestConfigResponse{Success: false, Message: "relay_url 无效"}, nil
	}
	probe.Header.Set(relay.SecretHeader, cfg.RelaySecret)
	response, err := s.healthClient.Do(probe)
	latency := time.Since(started).Milliseconds()
	if err != nil {
		return &pluginv1.TestConfigResponse{Success: false, Message: "无法连接 codex relay，请确认已运行 `codex relay`", LatencyMs: latency}, nil
	}
	defer func() { _ = response.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode == http.StatusUnauthorized {
		return &pluginv1.TestConfigResponse{Success: false, Message: "relay_secret 与 CODEX_RELAY_SECRET 不一致", LatencyMs: latency}, nil
	}
	if response.StatusCode != http.StatusOK {
		return &pluginv1.TestConfigResponse{Success: false, Message: "codex relay 返回 " + response.Status, LatencyMs: latency}, nil
	}
	var health struct {
		AuthMode      string `json:"auth_mode"`
		CodexLoggedIn bool   `json:"codex_logged_in"`
		Version       string `json:"version"`
	}
	_ = json.Unmarshal(raw, &health)
	if health.AuthMode != cfg.CredentialMode {
		return &pluginv1.TestConfigResponse{
			Success:    false,
			Message:    "relay 的 --auth 为 " + health.AuthMode + "，与插件 credential_mode " + cfg.CredentialMode + " 不一致",
			LatencyMs:  latency,
			StatusJson: string(raw),
		}, nil
	}
	if health.AuthMode == pluginconfig.CredentialModeCodex && !health.CodexLoggedIn {
		return &pluginv1.TestConfigResponse{Success: false, Message: "relay 所在机器的 Codex 未登录，请运行 `codex login`", LatencyMs: latency, StatusJson: string(raw)}, nil
	}
	return &pluginv1.TestConfigResponse{Success: true, Message: "codex relay " + health.Version + " 连接正常", LatencyMs: latency, StatusJson: string(raw)}, nil
}

// Forward 接收请求帧、发出请求并按 start / body_chunk / end 顺序回传响应。
func (s *Server) Forward(stream pluginv1.TransportPlugin_ForwardServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return sendError(stream, &relay.Error{Code: "CODEX_RELAY_PROTOCOL", Message: "首帧必须是 start"})
	}

	bodyReader, bodyWriter := io.Pipe()
	go pumpRequestBody(stream, bodyWriter, cancel)

	response, err := s.transport.RoundTrip(ctx, relay.Request{
		Method:        start.GetMethod(),
		URL:           start.GetUrl(),
		Headers:       headersFromPlugin(start.GetHeaders()),
		ProxyURL:      start.GetProxyUrl(),
		ContentLength: start.GetContentLength(),
		HasBody:       start.GetHasBody(),
		Body:          bodyReader,
	})
	if err != nil {
		_ = bodyReader.CloseWithError(err)
		var relayErr *relay.Error
		if !errors.As(err, &relayErr) {
			relayErr = &relay.Error{Code: "CODEX_RELAY_INTERNAL", Message: "内部错误", RequestSent: true}
		}
		return sendError(stream, relayErr)
	}
	defer func() { _ = response.Body.Close() }()

	started := time.Now()
	if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{Start: &pluginv1.ForwardResponseStart{
		StatusCode:    int32(response.StatusCode),
		Status:        response.Status,
		Protocol:      response.Proto,
		ProtocolMajor: int32(response.ProtoMajor),
		ProtocolMinor: int32(response.ProtoMinor),
		Headers:       headersToPlugin(response.Header),
		ContentLength: response.ContentLength,
	}}}); err != nil {
		return err
	}

	var received int64
	buffer := make([]byte, chunkSize)
	for {
		read, readErr := response.Body.Read(buffer)
		if read > 0 {
			received += int64(read)
			chunk := append([]byte(nil), buffer[:read]...)
			if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{BodyChunk: chunk}}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return sendError(stream, &relay.Error{Code: "CODEX_RELAY_RESPONSE_READ", Message: "读取响应体失败", RequestSent: true})
		}
	}
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{End: &pluginv1.ForwardResponseEnd{
		BytesReceived: received,
		DurationMs:    time.Since(started).Milliseconds(),
	}}})
}

// pumpRequestBody 把 body_chunk 帧写入管道，body_end 时关闭；流出错时取消上游请求。
func pumpRequestBody(stream pluginv1.TransportPlugin_ForwardServer, writer *io.PipeWriter, cancel context.CancelFunc) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			_ = writer.CloseWithError(err)
			cancel()
			return
		}
		if chunk := frame.GetBodyChunk(); chunk != nil {
			if _, err := writer.Write(chunk); err != nil {
				return
			}
			continue
		}
		if frame.GetBodyEnd() {
			_ = writer.Close()
			return
		}
	}
}

func sendError(stream pluginv1.TransportPlugin_ForwardServer, err *relay.Error) error {
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Error{Error: &pluginv1.ForwardResponseError{
		Code:        err.Code,
		Message:     err.Message,
		RequestSent: err.RequestSent,
	}}})
}

func headersFromPlugin(headers map[string]*pluginv1.HeaderValues) http.Header {
	out := make(http.Header, len(headers))
	for key, values := range headers {
		if values != nil && strings.TrimSpace(key) != "" {
			out[http.CanonicalHeaderKey(key)] = append([]string(nil), values.GetValues()...)
		}
	}
	return out
}

func headersToPlugin(headers http.Header) map[string]*pluginv1.HeaderValues {
	out := make(map[string]*pluginv1.HeaderValues, len(headers))
	for key, values := range headers {
		out[key] = &pluginv1.HeaderValues{Values: append([]string(nil), values...)}
	}
	return out
}
