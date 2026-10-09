package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpstreamRejectsMalformedWorkloadParameters(t *testing.T) {
	for _, query := range []string{"size=broken", "delay_ms=broken", "size=999999999999999999999", "delay_ms=1.5"} {
		u := &upstream{}
		rec := httptest.NewRecorder()
		u.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/probe?"+query, nil))
		if rec.Code != http.StatusBadRequest || u.requests.Load() != 0 {
			t.Fatalf("%s: status=%d requests=%d", query, rec.Code, u.requests.Load())
		}
	}
	for _, query := range []string{"", "size=", "size=32&delay_ms=0"} {
		u := &upstream{}
		rec := httptest.NewRecorder()
		u.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/probe?"+query, nil))
		if rec.Code != http.StatusOK || u.requests.Load() != 1 {
			t.Fatalf("valid %s: status=%d requests=%d", query, rec.Code, u.requests.Load())
		}
	}
}
