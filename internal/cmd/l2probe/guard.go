package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type guardTarget struct {
	Protocol  string `json:"protocol"`
	Address   string `json:"address"`
	Path      string `json:"path"`
	Size      int    `json:"size"`
	Sentinels bool   `json:"sentinels"`
}

func guardToken(secret []byte, subject, jti string, expiry time.Time) (string, error) {
	claims := jwt.MapClaims{"sub": subject, "iss": "bounded-l2-probe", "exp": expiry.Unix()}
	if jti != "" {
		claims["jti"] = jti
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}
func expectedGuardBytes(key string, size int) []byte {
	h := sha256.Sum256([]byte("42:" + key))
	pattern := hex.EncodeToString(h[:])
	return []byte(strings.Repeat(pattern, (size+63)/64)[:size])
}

type guardClient struct {
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
}

func (c *guardClient) close() {
	for _, conn := range c.connections {
		_ = conn.Close()
	}
}

func (c *guardClient) request(ctx context.Context, t guardTarget, key string, size int, token string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	parameters, err := json.Marshal(map[string]any{"key": key, "size": size})
	if err != nil {
		return nil, 0, err
	}
	if t.Protocol == "grpc" {
		c.mu.Lock()
		conn := c.connections[t.Address]
		if conn == nil {
			var err error
			conn, err = grpc.NewClient(t.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithAuthority("bounded-l2-probe"), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
			if err != nil {
				c.mu.Unlock()
				return nil, 0, err
			}
			c.connections[t.Address] = conn
		}
		c.mu.Unlock()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		out := new(wrapperspb.BytesValue)
		err := conn.Invoke(ctx, "/cosmoguard.probe.Echo/Query", wrapperspb.Bytes(parameters), out)
		if grpcstatus.Code(err) == codes.Unauthenticated {
			return nil, http.StatusUnauthorized, nil
		}
		return out.Value, 200, err
	}
	if t.Protocol == "jsonrpc_ws" {
		path := t.Path
		if path == "" {
			path = "/websocket"
		}
		conn, response, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+t.Address+path, http.Header{"Authorization": {"Bearer " + token}, "Host": {"bounded-l2-probe"}})
		if err != nil {
			if response != nil {
				defer response.Body.Close()
				return nil, response.StatusCode, nil
			}
			return nil, 0, err
		}
		defer conn.Close()
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(deadline)
			_ = conn.SetWriteDeadline(deadline)
		}
		conn.SetReadLimit(4 << 20)
		if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "probe", "params": json.RawMessage(parameters)}); err != nil {
			return nil, 0, err
		}
		var out struct {
			Result string `json:"result"`
		}
		if err := conn.ReadJSON(&out); err != nil {
			return nil, 0, err
		}
		return []byte(out.Result), 200, nil
	}
	url := "http://" + t.Address
	method := http.MethodGet
	var body io.Reader
	if t.Protocol == "jsonrpc" {
		method = http.MethodPost
		body = bytes.NewReader(append(append([]byte(`{"jsonrpc":"2.0","id":1,"method":"probe","params":`), parameters...), '}'))
	} else {
		path := "/probe"
		if key == "limiter-sentinel" {
			path = "/limit"
		}
		url += fmt.Sprintf("%s?key=%s&size=%d", path, key, size)
	}
	r, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, 0, err
	}
	r.Host = "bounded-l2-probe"
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, response.StatusCode, nil
	}
	var out struct {
		Value  string `json:"value"`
		Result string `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&out); err != nil {
		return nil, 200, err
	}
	if t.Protocol == "jsonrpc" {
		return []byte(out.Result), 200, nil
	}
	return []byte(out.Value), 200, nil
}

// Allow brief replacement pauses, but do not pass a later sustained outage.
const guardProgressTimeout = 30 * time.Second

func guardRun(ctx context.Context, file string, duration time.Duration, workers, size, rps int) error {
	if workers < 1 || rps < 0 || (rps > 0 && time.Second/time.Duration(rps) == 0) || size < 0 || size > 2<<20 {
		return errors.New("invalid guard workload")
	}
	client := &guardClient{connections: make(map[string]*grpc.ClientConn)}
	defer client.close()
	secret := []byte(os.Getenv("PROBE_JWT_SECRET"))
	if len(secret) == 0 {
		return errors.New("set PROBE_JWT_SECRET to the test fixture secret")
	}
	readTargets := func() ([]guardTarget, error) {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var targets []guardTarget
		err = json.Unmarshal(b, &targets)
		if err != nil || len(targets) == 0 {
			return nil, errors.New("guard target file must contain a nonempty JSON array")
		}
		for _, target := range targets {
			if target.Size < 0 || target.Size > 2<<20 {
				return nil, errors.New("guard target size must be between 0 and 2 MiB")
			}
		}
		return targets, nil
	}
	initial, err := readTargets()
	if err != nil {
		return err
	}
	// A single fixture token must be rejected on every other connected member.
	token, err := guardToken(secret, "sentinel", fmt.Sprintf("sentinel-%d", time.Now().UnixNano()), time.Now().Add(6*time.Hour))
	if err != nil {
		return err
	}
	_, status, err := client.request(ctx, initial[0], "sentinel", 1024, token)
	if err != nil || status != 200 {
		return fmt.Errorf("seed replay sentinel: status=%d: %v", status, err)
	}
	for _, target := range initial {
		_, status, err := client.request(ctx, target, "sentinel", 1024, token)
		if err != nil || status != http.StatusUnauthorized {
			return fmt.Errorf("replay sentinel accepted or unavailable: status=%d: %v", status, err)
		}
	}
	limiterToken, err := guardToken(secret, "bucket-sentinel", "", time.Now().Add(6*time.Hour))
	if err != nil {
		return err
	}
	_, status, err = client.request(ctx, initial[0], "limiter-sentinel", 1024, limiterToken)
	if err != nil || status != 200 {
		return fmt.Errorf("seed limiter sentinel: status=%d: %v", status, err)
	}
	for _, target := range initial {
		if !target.Sentinels {
			continue
		}
		_, status, err = client.request(ctx, target, "limiter-sentinel", 1024, limiterToken)
		if err != nil || status != http.StatusTooManyRequests {
			return fmt.Errorf("shared limiter sentinel: status=%d: %v", status, err)
		}
	}
	checkCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	var sequence, succeeded, skipped atomic.Uint64
	var mu sync.Mutex
	var latencies []float64
	lastSuccess := time.Now()
	var firstErr error
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}
	var permits <-chan time.Time
	var pacing *time.Ticker
	if rps > 0 {
		pacing = time.NewTicker(time.Second / time.Duration(rps))
		defer pacing.Stop()
		permits = pacing.C
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			for ctx.Err() == nil {
				if permits != nil {
					select {
					case <-ctx.Done():
						return
					case <-permits:
					}
				}
				targets, err := readTargets()
				if err != nil {
					fail(err)
					return
				}
				i := sequence.Add(1)
				host, _, err := net.SplitHostPort(targets[0].Address)
				if err != nil {
					fail(err)
					return
				}
				stride := 0
				for _, candidate := range targets {
					h, _, err := net.SplitHostPort(candidate.Address)
					if err != nil {
						fail(err)
						return
					}
					if h != host {
						break
					}
					stride++
				}
				target := targets[(i+(i/160)*uint64(stride))%uint64(len(targets))]
				key := fmt.Sprintf("key-%d", i%10000)
				if i%10 == 0 {
					key = fmt.Sprintf("hot-%d", (i/10)%16)
				}
				requestSize := size
				if target.Size > 0 {
					requestSize = target.Size
				}
				if requestSize == 0 {
					requestSize = []int{1024, 16384, 256 << 10, 900 << 10}[i%4]
				}
				token, err := guardToken(secret, fmt.Sprintf("subject-%d", i%10000), fmt.Sprintf("request-%d-%d", time.Now().UnixNano(), i), time.Now().Add(time.Minute))
				if err != nil {
					fail(err)
					return
				}
				start := time.Now()
				callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				value, status, err := client.request(callCtx, target, key, requestSize, token)
				stop()
				if err != nil || status != 200 {
					if ctx.Err() != nil {
						return
					}
					skipped.Add(1)
					continue
				}
				if target.Protocol != "grpc" && target.Protocol != "jsonrpc" && target.Protocol != "jsonrpc_ws" {
					key = "/probe?" + key
				}
				if !bytes.Equal(value, expectedGuardBytes(key, requestSize)) {
					fail(fmt.Errorf("corrupt response from %s", target.Address))
					return
				}
				succeeded.Add(1)
				mu.Lock()
				lastSuccess = time.Now()
				if len(latencies) < 100000 {
					latencies = append(latencies, time.Since(start).Seconds()*1000)
				}
				mu.Unlock()
			}
		})
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			if ctx.Err() != nil {
				break
			}
			mu.Lock()
			values := latencies
			latencies = nil
			stalled := time.Since(lastSuccess) >= guardProgressTimeout
			mu.Unlock()
			if stalled {
				fail(fmt.Errorf("no successful guard requests for %s", guardProgressTimeout))
				break
			}
			sort.Float64s(values)
			percentile := func(q float64) float64 {
				if len(values) == 0 {
					return 0
				}
				return values[int(float64(len(values)-1)*q)]
			}
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"time": time.Now().UTC(), "seed": 42, "requests": sequence.Load(), "success": succeeded.Load(), "errors": skipped.Load(), "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99)})
			// Let an admitted assertion finish within its own deadline, even if traffic ends.
			targets, err := readTargets()
			if err != nil {
				fail(err)
				break
			}
			for _, target := range targets {
				if ctx.Err() != nil {
					break
				}
				_, status, err := client.request(checkCtx, target, "sentinel", 1024, token)
				if err != nil || status != http.StatusUnauthorized {
					fail(fmt.Errorf("lost replay sentinel on %s: status=%d: %v", target.Address, status, err))
					break
				}
				if ctx.Err() != nil {
					break
				}
				if target.Sentinels {
					_, status, err = client.request(checkCtx, target, "limiter-sentinel", 1024, limiterToken)
					if err != nil || status != http.StatusTooManyRequests {
						fail(fmt.Errorf("lost limiter sentinel on %s: status=%d: %v", target.Address, status, err))
						break
					}
				}
			}
		}
	}
	wg.Wait()
	if firstErr == nil && time.Since(lastSuccess) >= guardProgressTimeout {
		return fmt.Errorf("no successful guard requests for %s", guardProgressTimeout)
	}
	if firstErr == nil && succeeded.Load() == 0 {
		return errors.New("no successful guard requests")
	}
	return firstErr
}
