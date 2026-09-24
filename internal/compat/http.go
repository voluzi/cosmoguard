package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// maxBody bounds how much of one response is read. Both sides are cut at
// the same length, so a larger body is compared by its prefix.
const maxBody = 32 << 20

type httpDoer struct {
	client *http.Client
}

func newHTTPDoer(timeout time.Duration) *httpDoer {
	return &httpDoer{client: &http.Client{Timeout: timeout}}
}

// do sends one request. A 429 is retried with backoff, so a public node's
// rate limit does not read as a difference.
func (h *httpDoer) do(ctx context.Context, method, url string, body []byte, header map[string]string) Response {
	for attempt := 0; ; attempt++ {
		r := h.once(ctx, method, url, body, header)
		if r.Err != nil || r.Status != http.StatusTooManyRequests || attempt == 3 {
			return r
		}
		select {
		case <-ctx.Done():
			return Response{Err: ctx.Err()}
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
}

func (h *httpDoer) once(ctx context.Context, method, url string, body []byte, header map[string]string) Response {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return Response{Err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return Response{Err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return Response{Err: fmt.Errorf("reading body: %w", err)}
	}
	height, _ := strconv.ParseInt(resp.Header.Get("X-Cosmos-Block-Height"), 10, 64)
	if height == 0 {
		height, _ = strconv.ParseInt(resp.Header.Get("Grpc-Metadata-X-Cosmos-Block-Height"), 10, 64)
	}
	return Response{
		Status: resp.StatusCode,
		Body:   b,
		Height: height,
		Denied: resp.StatusCode == http.StatusForbidden,
	}
}

// getJSON fetches url from the node and decodes a 200 response into out.
func (h *httpDoer) getJSON(ctx context.Context, url string, header map[string]string, out any) error {
	r := h.do(ctx, http.MethodGet, url, nil, header)
	if r.Err != nil {
		return r.Err
	}
	if r.Status != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, r.Status)
	}
	return json.Unmarshal(r.Body, out)
}

// postJSONRPC calls a JSON-RPC method on the node and decodes its result.
func (h *httpDoer) postJSONRPC(ctx context.Context, url, method string, params any, out any) error {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return err
	}
	r := h.do(ctx, http.MethodPost, url, body, nil)
	if r.Err != nil {
		return r.Err
	}
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.Body, &env); err != nil {
		return fmt.Errorf("%s: status %d: %w", method, r.Status, err)
	}
	if env.Error != nil {
		return fmt.Errorf("%s: %s", method, env.Error.Message)
	}
	return json.Unmarshal(env.Result, out)
}
