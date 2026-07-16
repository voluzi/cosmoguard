package cosmoguard

import "testing"

// TestValidateAuthEndpoints is the #4 regression: JWKS / introspection /
// external-validator endpoints must be https (or loopback http), so an
// on-path attacker can't serve a forged response over plaintext http and
// bypass authentication.
func TestValidateAuthEndpoints(t *testing.T) {
	cases := []struct {
		name    string
		method  AuthMethodConfig
		wantErr bool
	}{
		{"https jwks ok", AuthMethodConfig{JwksURL: "https://idp.example/jwks.json"}, false},
		{"http jwks rejected", AuthMethodConfig{JwksURL: "http://idp.example/jwks.json"}, true},
		{"loopback http jwks ok", AuthMethodConfig{JwksURL: "http://127.0.0.1:8080/jwks.json"}, false},
		{"localhost http jwks ok", AuthMethodConfig{JwksURL: "http://localhost/jwks.json"}, false},
		{"http introspection rejected", AuthMethodConfig{IntrospectionEndpoint: "http://idp/introspect"}, true},
		{"https introspection ok", AuthMethodConfig{IntrospectionEndpoint: "https://idp/introspect"}, false},
		{"http external rejected", AuthMethodConfig{Endpoint: "http://validator/check"}, true},
		{"empty ok", AuthMethodConfig{}, false},
		{"bad scheme rejected", AuthMethodConfig{JwksURL: "ftp://idp/jwks"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAuthEndpoints(&AuthConfig{Enable: true, Methods: []AuthMethodConfig{tc.method}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateAuthEndpoints err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}

	// Disabled auth: even an http:// non-loopback endpoint is inert and
	// must not fail startup.
	t.Run("disabled auth skips validation", func(t *testing.T) {
		err := validateAuthEndpoints(&AuthConfig{Enable: false, Methods: []AuthMethodConfig{{JwksURL: "http://idp.example/jwks.json"}}})
		if err != nil {
			t.Fatalf("disabled auth should skip endpoint validation, got %v", err)
		}
	})
}
