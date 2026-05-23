package cosmoguard

import (
	"strings"
	"testing"

	"gotest.tools/assert"
)

func TestEnvInterpolate(t *testing.T) {
	t.Setenv("FOO", "bar")
	t.Setenv("EMPTY", "")
	t.Setenv("URL", "http://example.com:1234")
	// SECRET intentionally unset.

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string // substring; empty means "no error"
	}{
		{
			name:  "plain string passes through unchanged",
			input: "hello world",
			want:  "hello world",
		},
		{
			name:  "single required var",
			input: "value=${FOO}",
			want:  "value=bar",
		},
		{
			name:  "multiple vars",
			input: "${FOO}-${FOO}",
			want:  "bar-bar",
		},
		{
			name:  "default form, var set",
			input: "${FOO:-fallback}",
			want:  "bar",
		},
		{
			name:  "default form, var unset",
			input: "${SECRET:-fallback}",
			want:  "fallback",
		},
		{
			name:  "default form, var empty treated as unset",
			input: "${EMPTY:-fallback}",
			want:  "fallback",
		},
		{
			name:  "default value can contain colons",
			input: "${MISSING:-http://x:1234}",
			want:  "http://x:1234",
		},
		{
			name:  "var with URL value containing colons",
			input: "${URL}",
			want:  "http://example.com:1234",
		},
		{
			name:    "required var unset → error",
			input:   "${SECRET}",
			wantErr: "SECRET",
		},
		{
			name:    "error form, var unset",
			input:   "${SECRET:?set this in env}",
			wantErr: "set this in env",
		},
		{
			name:  "error form, var set",
			input: "${FOO:?must be set}",
			want:  "bar",
		},
		{
			name:  "unmatched ${ preserved verbatim",
			input: "literal ${ not closed",
			want:  "literal ${ not closed",
		},
		{
			name:  "bare $ preserved",
			input: "price $5",
			want:  "price $5",
		},
		{
			name:  "bare $VAR (no braces) NOT expanded",
			input: "$FOO",
			want:  "$FOO",
		},
		{
			name:    "empty brace name",
			input:   "${}",
			wantErr: "empty variable name",
		},
		{
			// YAML comments must NOT expand their ${VAR} references —
			// they're operator-readable documentation, not live config.
			name:  "comment preserves ${VAR} verbatim",
			input: "key: ${FOO}\n# secret: ${SECRET}\nother: x",
			want:  "key: bar\n# secret: ${SECRET}\nother: x",
		},
		{
			// Single-quoted strings inhibit comment detection (single-
			// quoted YAML scalars can contain `#`).
			name:  "hash inside single-quoted string is not a comment",
			input: "key: 'value with # not a comment ${FOO}'\n",
			want:  "key: 'value with # not a comment bar'\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EnvInterpolate(tt.input)
			if tt.wantErr == "" {
				assert.NilError(t, err)
				assert.Equal(t, got, tt.want)
			} else {
				assert.Assert(t, err != nil, "expected error containing %q", tt.wantErr)
				assert.Assert(t, strings.Contains(err.Error(), tt.wantErr),
					"error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}
