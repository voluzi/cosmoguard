package compat

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Endpoints are the base URLs of one side. GRPC is https://host[:port]
// for TLS or http://host:port for plaintext. Empty EVM URLs skip EVM.
type Endpoints struct {
	LCD   string
	RPC   string
	GRPC  string
	EVM   string
	EVMWS string
}

// Options configure a run.
type Options struct {
	Chain string
	Node  Endpoints
	Guard Endpoints
	// Height pins every query; 0 means the node's latest height minus 5.
	Height      int64
	Concurrency int
	Timeout     time.Duration
	// Protocols limits the run to these; empty means all.
	Protocols map[string]bool
	// Params adds or overrides request field values, for chain-specific
	// fields discovery does not know (e.g. topic_id).
	Params Params
	// Log receives progress lines.
	Log io.Writer
}

func (o Options) enabled(p string) bool { return len(o.Protocols) == 0 || o.Protocols[p] }

// Protocol names used in results and in Options.Protocols.
const (
	ProtoGRPC = "grpc"
	ProtoLCD  = "lcd"
	ProtoRPC  = "rpc"
	ProtoEVM  = "evm"
	ProtoWS   = "ws"
)

type task func(ctx context.Context) Result

// Run discovers endpoints on the node, calls each one directly and through
// cosmoguard, and returns the verdicts.
func Run(ctx context.Context, o Options) (*Report, error) {
	h := newHTTPDoer(o.Timeout)
	height := o.Height
	if height == 0 {
		latest, err := latestHeight(ctx, h, o.Node.RPC)
		if err != nil {
			return nil, fmt.Errorf("node status: %w", err)
		}
		height = latest - 5
	}
	rep := &Report{Chain: o.Chain, Height: height}
	logf := func(format string, args ...any) { fmt.Fprintf(o.Log, format+"\n", args...) }
	logf("pinned height %d", height)

	params, notes := discoverParams(ctx, h, o.Node, height)
	for _, n := range notes {
		logf("param discovery: %s", n)
	}
	for k, v := range o.Params {
		params[k] = v
	}

	var tasks []task
	if o.enabled(ProtoGRPC) || o.enabled(ProtoLCD) {
		node, err := dialGRPC(o.Node.GRPC)
		if err != nil {
			return nil, fmt.Errorf("dial node gRPC: %w", err)
		}
		defer node.Close()
		guard, err := dialGRPC(o.Guard.GRPC)
		if err != nil {
			return nil, fmt.Errorf("dial cosmoguard gRPC: %w", err)
		}
		defer guard.Close()
		dctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		methods, err := discoverMethods(dctx, node)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("discover gRPC methods: %w", err)
		}
		logf("discovered %d query methods", len(methods))
		for _, m := range methods {
			if o.enabled(ProtoGRPC) {
				tasks = append(tasks, grpcTask(node, guard, m, params, height))
			}
			if o.enabled(ProtoLCD) {
				for _, tmpl := range m.GETs {
					tasks = append(tasks, lcdTask(h, o, m, tmpl, params, height))
				}
			}
		}
	}
	if o.enabled(ProtoRPC) {
		tasks = append(tasks, cometTasks(ctx, h, o, params, height)...)
	}
	if o.enabled(ProtoEVM) && o.Node.EVM != "" && o.Guard.EVM != "" {
		tasks = append(tasks, evmTasks(ctx, h, o, height)...)
	}

	logf("running %d comparisons with concurrency %d", len(tasks), o.Concurrency)
	runTasks(ctx, tasks, o, rep)

	if o.enabled(ProtoWS) {
		logf("comparing WebSocket subscriptions")
		for _, res := range wsCompare(ctx, o) {
			rep.Add(res)
		}
	}
	return rep, nil
}

func runTasks(ctx context.Context, tasks []task, o Options, rep *Report) {
	sem := make(chan struct{}, max(o.Concurrency, 1))
	var wg sync.WaitGroup
	for _, t := range tasks {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			cctx, cancel := context.WithTimeout(ctx, 4*o.Timeout)
			defer cancel()
			rep.Add(t(cctx))
		}()
	}
	wg.Wait()
}

// heightRetries bounds how often a pair answered at different heights is
// fetched again before it is called unstable.
const heightRetries = 3

func grpcTask(node, guard *grpc.ClientConn, m Method, p Params, height int64) task {
	return func(ctx context.Context) Result {
		res := Result{Protocol: ProtoGRPC, Name: m.FullName}
		req, err := buildRequest(m.Desc, p)
		if err != nil {
			res.Class, res.Detail = Skipped, "building request: "+err.Error()
			return res
		}
		call := func(conn *grpc.ClientConn) Response {
			for attempt := 0; ; attempt++ {
				r := invoke(ctx, conn, m.FullName, req, height)
				if r.Status != int(codes.ResourceExhausted) || attempt == 3 {
					return r
				}
				select {
				case <-ctx.Done():
					return Response{Err: ctx.Err()}
				case <-time.After(time.Duration(attempt+1) * time.Second):
				}
			}
		}
		volatile := volatileMethods[m.FullName]
		fetch := func() (Response, Response, Response, Response) {
			return call(node), call(guard), call(node), call(guard)
		}
		render := func(r Response) Response { return r }
		if volatile {
			render = func(r Response) Response { return asJSON(m.Desc, r) }
		}
		res.Class, res.Detail = settle(fetch, volatile, render)
		return res
	}
}

// rounds bounds how often a differing endpoint is compared again. Public
// endpoints are often load-balanced pools of nodes on different versions
// and heights, so one mismatch is not a verdict; a cosmoguard defect
// reproduces in every round.
const rounds = 3

// settle compares an endpoint over up to rounds rounds and returns the
// verdict. It differs only when every round differs; a later match makes
// it unstable, keeping the first difference in the detail.
func settle(fetch func() (d1, g1, d2, g2 Response), volatile bool, render func(Response) Response) (Class, string) {
	var first string
	for round := 1; ; round++ {
		c, detail := settleRound(fetch, volatile, render)
		if c != Differs {
			if first != "" && c == Identical {
				return Unstable, fmt.Sprintf("matched in round %d after differing: %s", round, first)
			}
			return c, detail
		}
		if first == "" {
			first = detail
		}
		if round == rounds {
			return Differs, detail
		}
	}
}

// settleRound compares two node answers and two cosmoguard answers to one
// request, taken interleaved (node, cosmoguard, node, cosmoguard); the
// second cosmoguard answer is normally served from cache.
//
// Answers that report different heights are fetched again: a pinned query
// should never drift, but some public nodes ignore the pin. When the
// node's own two answers disagree, cosmoguard cannot be judged against
// it, so the endpoint is reported as unstable.
func settleRound(fetch func() (d1, g1, d2, g2 Response), volatile bool, render func(Response) Response) (Class, string) {
	for attempt := 1; ; attempt++ {
		d1, g1, d2, g2 := fetch()
		if !volatile && !SameHeight(d1, g1, d2, g2) {
			if attempt < heightRetries {
				continue
			}
			return Unstable, fmt.Sprintf("answered at heights node=%d,%d cosmoguard=%d,%d despite the pin", d1.Height, d2.Height, g1.Height, g2.Height)
		}
		d1, g1, d2, g2 = render(d1), render(g1), render(d2), render(g2)
		if d1.Err == nil && d2.Err == nil {
			if c, _ := Classify(d1, []Response{d2}, volatile); c != Identical {
				return Unstable, "the node's own answers differ between calls (load-balanced or non-deterministic)"
			}
		}
		return Classify(d1, []Response{g1, g2}, volatile)
	}
}

func lcdTask(h *httpDoer, o Options, m Method, tmpl string, p Params, height int64) task {
	return func(ctx context.Context) Result {
		res := Result{Protocol: ProtoLCD, Name: tmpl}
		path, missing := FillPath(tmpl, p)
		if len(missing) > 0 {
			res.Class = Skipped
			res.Detail = "no live value for " + strings.Join(missing, ", ")
			return res
		}
		pin := map[string]string{"x-cosmos-block-height": strconv.FormatInt(height, 10)}
		get := func(base string) Response { return h.do(ctx, http.MethodGet, base+path, nil, pin) }
		fetch := func() (Response, Response, Response, Response) {
			return get(o.Node.LCD), get(o.Guard.LCD), get(o.Node.LCD), get(o.Guard.LCD)
		}
		res.Class, res.Detail = settle(fetch, volatileMethods[m.FullName], func(r Response) Response { return r })
		return res
	}
}
