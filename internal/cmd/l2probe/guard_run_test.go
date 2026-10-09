package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type guardRunTransport struct {
	replay, limiter atomic.Int32
	failureKey      string
	timeout         bool
}

func (g *guardRunTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	key := r.URL.Query().Get("key")
	size, _ := strconv.Atoi(r.URL.Query().Get("size"))
	status := http.StatusOK
	var calls int32
	switch key {
	case "sentinel":
		calls = g.replay.Add(1)
		if calls > 1 {
			status = http.StatusUnauthorized
		}
	case "limiter-sentinel":
		calls = g.limiter.Add(1)
		if calls > 1 {
			status = http.StatusTooManyRequests
		}
	}
	if key == g.failureKey && calls >= 3 {
		if g.timeout {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return nil, errors.New("probe unavailable")
	}
	body, _ := json.Marshal(map[string]string{"value": string(expectedGuardBytes("/probe?"+key, size))})
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func guardRunTargets(t *testing.T) string {
	t.Helper()
	t.Setenv("PROBE_JWT_SECRET", "diagnostic-test-secret")
	file := filepath.Join(t.TempDir(), "targets.json")
	if err := os.WriteFile(file, []byte(`[{"protocol":"http","address":"127.0.0.1:1","size":1024,"sentinels":true}]`), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestGuardRunRejectsIncompletePeriodicSentinels(t *testing.T) {
	file := guardRunTargets(t)
	for _, key := range []string{"sentinel", "limiter-sentinel"} {
		for _, timeout := range []bool{false, true} {
			name := key + "/transport"
			if timeout {
				name = key + "/timeout"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					transport := &guardRunTransport{failureKey: key, timeout: timeout}
					old := http.DefaultClient
					http.DefaultClient = &http.Client{Transport: transport}
					defer func() { http.DefaultClient = old }()
					err := guardRun(t.Context(), file, 30*time.Second, 1, 1024, 1)
					expected := "lost replay sentinel"
					if key == "limiter-sentinel" {
						expected = "lost limiter sentinel"
					}
					if err == nil || !strings.Contains(err.Error(), expected) {
						t.Fatalf("run error=%v, want %s", err, expected)
					}
					cause := "probe unavailable"
					if timeout {
						cause = "context deadline exceeded"
					}
					if !strings.Contains(err.Error(), cause) {
						t.Fatalf("run error=%v, want %s", err, cause)
					}
				})
			})
		}
	}
}
