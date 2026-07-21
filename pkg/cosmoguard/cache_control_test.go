package cosmoguard

import (
	"net/http"
	"testing"
)

func TestCacheableByUpstreamInspectsAllCacheControlValues(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{
			name:   "private on second line",
			values: []string{"public, max-age=60", "private"},
			want:   false,
		},
		{
			name:   "no-store on second line",
			values: []string{"public, max-age=60", "no-store"},
			want:   false,
		},
		{
			name:   "s-maxage zero on second line",
			values: []string{"max-age=60", "s-maxage=0"},
			want:   false,
		},
		{
			name:   "nonzero s-maxage overrides max-age zero across lines",
			values: []string{"max-age=0", "s-maxage=60"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := make(http.Header)
			for _, value := range tt.values {
				headers.Add("Cache-Control", value)
			}
			if got := cacheableByUpstream(headers); got != tt.want {
				t.Fatalf("cacheableByUpstream(%q) = %v, want %v", tt.values, got, tt.want)
			}
		})
	}
}
