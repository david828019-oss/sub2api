package plugin

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/relay"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const testSecret = "0123456789abcdef0123"

func startClient(t *testing.T) pluginv1.TransportPluginClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pluginv1.RegisterTransportPluginServer(server, NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pluginv1.NewTransportPluginClient(conn)
}

func applyConfig(t *testing.T, client pluginv1.TransportPluginClient, relayURL string) {
	t.Helper()
	raw := []byte(`{"relay_url":"` + relayURL + `","relay_secret":"` + testSecret + `"}`)
	validated, err := client.ValidateConfig(context.Background(), &pluginv1.ValidateConfigRequest{ConfigJson: raw})
	if err != nil || !validated.GetValid() {
		t.Fatalf("validate: %v %v", validated, err)
	}
	applied, err := client.ApplyConfig(context.Background(), &pluginv1.ApplyConfigRequest{ConfigJson: validated.GetNormalizedConfigJson()})
	if err != nil || !applied.GetApplied() {
		t.Fatalf("apply: %v %v", applied, err)
	}
}

func forward(t *testing.T, client pluginv1.TransportPluginClient, body string) []*pluginv1.ForwardResponse {
	t.Helper()
	stream, err := client.Forward(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	frames := []*pluginv1.ForwardRequest{
		{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{
			Method:        http.MethodPost,
			Url:           "https://chatgpt.com/backend-api/codex/responses",
			Headers:       map[string]*pluginv1.HeaderValues{"Authorization": {Values: []string{"Bearer account"}}},
			ContentLength: int64(len(body)),
			HasBody:       true,
		}}},
	}
	for _, part := range []string{body[:len(body)/2], body[len(body)/2:]} {
		frames = append(frames, &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(part)}})
	}
	frames = append(frames, &pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	for _, frame := range frames {
		if err := stream.Send(frame); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.CloseSend()
	var responses []*pluginv1.ForwardResponse
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return responses
		}
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, frame)
	}
}

func TestGetInfoMatchesManifest(t *testing.T) {
	info, err := startClient(t).GetInfo(context.Background(), &pluginv1.GetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetPluginId() != ID || info.GetProtocolVersion() != pluginv1.ProtocolVersion ||
		info.GetTransportApiVersion() != pluginv1.TransportAPIVersion || len(info.GetCapabilities()) != 1 {
		t.Fatalf("unexpected info %+v", info)
	}
}

func TestForwardStreamsRelayResponse(t *testing.T) {
	relayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"input":"hello"}` || r.Header.Get(relay.SecretHeader) != testSecret || r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected relay request %q %v", body, r.Header)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n\n"))
	}))
	defer relayServer.Close()
	client := startClient(t)
	applyConfig(t, client, relayServer.URL)

	frames := forward(t, client, `{"input":"hello"}`)
	if len(frames) < 3 || frames[0].GetStart().GetStatusCode() != http.StatusOK || frames[len(frames)-1].GetEnd() == nil {
		t.Fatalf("unexpected frames %v", frames)
	}
	var body strings.Builder
	for _, frame := range frames[1 : len(frames)-1] {
		body.Write(frame.GetBodyChunk())
	}
	if body.String() != "event: response.completed\n\n" {
		t.Fatalf("unexpected body %q", body.String())
	}
}

func TestForwardReportsUnsentErrorFrame(t *testing.T) {
	client := startClient(t)
	frames := forward(t, client, "{}")
	if len(frames) != 1 || frames[0].GetError() == nil || frames[0].GetError().GetRequestSent() {
		t.Fatalf("expected a single unsent error frame, got %v", frames)
	}
}
