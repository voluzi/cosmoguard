package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

type guardStallTransport struct{}

func (guardStallTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}
func TestGuardSentinelUsesRequestDeadline(t *testing.T) {
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: guardStallTransport{}}
	defer func() { http.DefaultClient = old }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()
		start := time.Now()
		client := &guardClient{}
		_, _, err := client.request(ctx, guardTarget{Protocol: "http", Address: "127.0.0.1:1"}, "sentinel", 1024, "token")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(start) != 5*time.Second {
			t.Fatalf("sentinel wait=%s", time.Since(start))
		}
	})
}
