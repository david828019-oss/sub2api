package pluginconfig

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseEmptyReturnsDefaults(t *testing.T) {
	for _, raw := range []string{"", "  ", "null", "{}"} {
		cfg, err := Parse([]byte(raw))
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", raw, err)
		}
		if !reflect.DeepEqual(cfg, Default()) {
			t.Fatalf("Parse(%q) = %+v, want defaults", raw, cfg)
		}
		if cfg.Ready() {
			t.Fatalf("default config must not be ready without a secret")
		}
	}
}

func TestParseNormalizes(t *testing.T) {
	cfg, err := Parse([]byte(`{"relay_url":" http://localhost:9000/ ","relay_secret":" 0123456789abcdef ","credential_mode":"PASSTHROUGH"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := Default()
	want.RelayURL = "http://localhost:9000"
	want.RelaySecret = "0123456789abcdef"
	want.CredentialMode = CredentialModePassthrough
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
	roundTrip, err := Parse(cfg.JSON())
	if err != nil || !reflect.DeepEqual(roundTrip, cfg) {
		t.Fatalf("normalized JSON must round-trip: %+v %v", roundTrip, err)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	cases := map[string]string{
		"unknown field":      `{"relay_url":"http://127.0.0.1:1","extra":1}`,
		"trailing data":      `{} {}`,
		"bad scheme":         `{"relay_url":"ftp://127.0.0.1"}`,
		"path in url":        `{"relay_url":"http://127.0.0.1:1/v1"}`,
		"credentials in url": `{"relay_url":"http://u:p@127.0.0.1:1"}`,
		"remote without opt": `{"relay_url":"https://relay.example.com"}`,
		"short secret":       `{"relay_secret":"short"}`,
		"bad mode":           `{"credential_mode":"api_key"}`,
		"bad unmatched":      `{"unmatched_requests":"drop"}`,
		"timeout too large":  `{"response_header_timeout_seconds":99999}`,
		"negative timeout":   `{"direct_response_header_timeout_seconds":-1}`,
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected error for %s", name, raw)
		}
	}
}

func TestParseAllowsRemoteRelayWhenOptedIn(t *testing.T) {
	cfg, err := Parse([]byte(`{"relay_url":"https://relay.example.com","allow_remote_relay":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.RelayURL, "https://") {
		t.Fatalf("unexpected relay url %q", cfg.RelayURL)
	}
}
