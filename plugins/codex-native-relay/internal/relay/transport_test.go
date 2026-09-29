package relay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/pluginconfig"
)

const testSecret = "0123456789abcdef0123"

func configured(t *testing.T, relayURL, mode string) *Transport {
	t.Helper()
	cfg, err := pluginconfig.Parse([]byte(`{"relay_url":"` + relayURL + `","relay_secret":"` + testSecret + `","credential_mode":"` + mode + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	transport := New()
	transport.Apply(cfg)
	t.Cleanup(transport.Close)
	return transport
}

func codexRequest(body string) Request {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer account-token")
	headers.Set("Chatgpt-Account-Id", "acct-1")
	headers.Set("Session_id", "sess-1")
	headers.Set("Content-Type", "application/json")
	return Request{
		Method:        http.MethodPost,
		URL:           "https://chatgpt.com/backend-api/codex/responses",
		Headers:       headers,
		ContentLength: int64(len(body)),
		HasBody:       true,
		Body:          strings.NewReader(body),
	}
}

func TestIsCodexResponses(t *testing.T) {
	cases := map[string]bool{
		"https://chatgpt.com/backend-api/codex/responses":         true,
		"https://chatgpt.com/backend-api/codex/responses/compact": true,
		"https://ab.chatgpt.com/backend-api/codex/responses":      true,
		"https://chatgpt.com/backend-api/codex/models":            false,
		"https://chatgpt.com/backend-api/files":                   false,
		"https://api.openai.com/v1/responses":                     false,
		"https://evilchatgpt.com/backend-api/codex/responses":     false,
	}
	for raw, want := range cases {
		parsed, _ := url.Parse(raw)
		if got := IsCodexResponses(parsed); got != want {
			t.Errorf("IsCodexResponses(%s) = %v, want %v", raw, got, want)
		}
	}
}

func TestCodexModeStripsAccountCredentials(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		checks := map[string]string{
			"path":          r.URL.Path,
			"secret":        r.Header.Get(SecretHeader),
			"authorization": r.Header.Get("Authorization"),
			"account":       r.Header.Get("Chatgpt-Account-Id"),
			"session":       r.Header.Get("Session_id"),
			"body":          string(body),
		}
		want := map[string]string{
			"path":          "/backend-api/codex/responses",
			"secret":        testSecret,
			"authorization": "",
			"account":       "",
			"session":       "sess-1",
			"body":          `{"model":"gpt-5"}`,
		}
		for key, value := range want {
			if checks[key] != value {
				t.Errorf("%s = %q, want %q", key, checks[key], value)
			}
		}
		if r.ContentLength != int64(len(body)) {
			t.Errorf("content length %d, want %d", r.ContentLength, len(body))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: done\n\n"))
	}))
	defer relay.Close()

	transport := configured(t, relay.URL, pluginconfig.CredentialModeCodex)
	response, err := transport.RoundTrip(context.Background(), codexRequest(`{"model":"gpt-5"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || string(body) != "data: done\n\n" {
		t.Fatalf("unexpected response %d %q", response.StatusCode, body)
	}
}

func TestPassthroughModeForwardsAccountCredentials(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-token" || r.Header.Get("Chatgpt-Account-Id") != "acct-1" {
			t.Errorf("credentials not forwarded: %v", r.Header)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer relay.Close()

	transport := configured(t, relay.URL, pluginconfig.CredentialModePassthrough)
	response, err := transport.RoundTrip(context.Background(), codexRequest("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d", response.StatusCode)
	}
}

func TestRelayErrorsMapToRequestSent(t *testing.T) {
	for stage, wantSent := range map[string]bool{"relay": false, "upstream": true} {
		relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(ErrorHeader, stage)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
		}))
		transport := configured(t, relay.URL, pluginconfig.CredentialModeCodex)
		_, err := transport.RoundTrip(context.Background(), codexRequest("{}"))
		relay.Close()
		var relayErr *Error
		if !errors.As(err, &relayErr) {
			t.Fatalf("%s: expected relay error, got %v", stage, err)
		}
		if relayErr.RequestSent != wantSent || !strings.Contains(relayErr.Message, "boom") {
			t.Errorf("%s: got %+v", stage, relayErr)
		}
	}
}

func TestUnreachableRelayIsNotSent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	transport := configured(t, "http://"+addr, pluginconfig.CredentialModeCodex)
	_, err = transport.RoundTrip(context.Background(), codexRequest("{}"))
	var relayErr *Error
	if !errors.As(err, &relayErr) || relayErr.RequestSent {
		t.Fatalf("expected unsent transport error, got %+v", err)
	}
}

func TestUnconfiguredSecretFailsWithoutSending(t *testing.T) {
	transport := New()
	_, err := transport.RoundTrip(context.Background(), codexRequest("{}"))
	var relayErr *Error
	if !errors.As(err, &relayErr) || relayErr.Code != "CODEX_RELAY_NOT_CONFIGURED" || relayErr.RequestSent {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestUnmatchedRequestsDirectOrReject(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-token" {
			t.Errorf("direct request must keep account credentials")
		}
		_, _ = w.Write([]byte("direct"))
	}))
	defer upstream.Close()
	request := codexRequest("")
	request.Method = http.MethodGet
	request.HasBody = false
	request.URL = upstream.URL + "/backend-api/files"

	transport := configured(t, "http://127.0.0.1:1", pluginconfig.CredentialModeCodex)
	response, err := transport.RoundTrip(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if string(body) != "direct" {
		t.Fatalf("unexpected body %q", body)
	}

	cfg := transport.Config()
	cfg.UnmatchedRequests = pluginconfig.UnmatchedReject
	transport.Apply(cfg)
	_, err = transport.RoundTrip(context.Background(), request)
	var relayErr *Error
	if !errors.As(err, &relayErr) || relayErr.Code != "CODEX_RELAY_UNMATCHED" || relayErr.RequestSent {
		t.Fatalf("unexpected error %+v", err)
	}
}
