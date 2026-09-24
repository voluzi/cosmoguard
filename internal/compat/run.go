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
	// RoundDelay separates the comparison rounds of a differing endpoint;
	// set it above cosmoguard's cache TTL so each round reaches upstream.
	RoundDelay time.Duration
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

// noEVM explains why EVM comparisons were skipped.
const noEVM = "no EVM endpoint"

// Run discovers endpoints on the node, calls each one directly and through
// cosmoguard, and returns the verdicts.
func Run(ctx context.Context, o Options) (*Report, error) {
	h := newHTTPDoer(o.Timeout)
	chainID, latest, err := nodeStatus(ctx, h, o.Node.RPC)
	if err != nil {
		return nil, fmt.Errorf("node status: %w", err)
	}
	height := o.Height
	if height == 0 {
		height = latest - 5
	}
	rep := &Report{Chain: chainID, Height: height}
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
	var node, guard *grpc.ClientConn
	if o.enabled(ProtoGRPC) || o.enabled(ProtoLCD) {
		var err error
		if node, err = dialGRPC(o.Node.GRPC); err != nil {
			return nil, fmt.Errorf("dial node gRPC: %w", err)
		}
		defer node.Close()
		// LCD needs only the node's reflection; cosmoguard's gRPC is
		// dialled only when gRPC is compared.
		if o.enabled(ProtoGRPC) {
			if guard, err = dialGRPC(o.Guard.GRPC); err != nil {
				return nil, fmt.Errorf("dial cosmoguard gRPC: %w", err)
			}
			defer guard.Close()
		}
		dctx, cancel := context.WithTimeout(ctx, max(2*time.Minute, 6*o.Timeout))
		methods, err := discoverMethods(dctx, node)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("discover gRPC methods: %w", err)
		}
		logf("discovered %d query methods", len(methods))
		for _, m := range methods {
			if o.enabled(ProtoGRPC) {
				tasks = append(tasks, grpcTask(node, guard, m, params, height, o))
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
	if o.enabled(ProtoEVM) {
		if o.Node.EVM != "" && o.Guard.EVM != "" {
			tasks = append(tasks, evmTasks(ctx, h, o, height)...)
		} else {
			rep.Add(Result{Protocol: ProtoEVM, Name: "all methods", Class: Skipped, Detail: noEVM})
		}
	}

	tasks = append(tasks, crossHeightTasks(h, o, height, node, guard)...)

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

// taskBudget bounds one endpoint's comparison, so one pathological
// endpoint cannot hold up the report. It covers the worst case that still
// answers: a cross-height probe's two comparisons, each of up to rounds
// rounds of heightRetries fetches of five calls, plus its two node calls
// and every pause.
func taskBudget(o Options) time.Duration {
	const settles = 2
	calls := settles*rounds*heightRetries*5 + 2
	pauses := settles * rounds * heightRetries
	return time.Duration(calls)*o.Timeout + time.Duration(pauses)*o.RoundDelay
}

func runTasks(ctx context.Context, tasks []task, o Options, rep *Report) {
	sem := make(chan struct{}, max(o.Concurrency, 1))
	var wg sync.WaitGroup
	budget := taskBudget(o)
	for _, t := range tasks {
		select {
		case <-ctx.Done():
			// Interrupted: record nothing for the endpoints not started.
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			// Both cases were ready; the interrupt wins.
			<-sem
			break
		}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			tctx, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			res := t(tctx)
			switch {
			case ctx.Err() != nil && res.Class != Identical && res.Class != Skipped:
				res.Class, res.Detail = Unstable, "interrupted before a verdict"
			case ctx.Err() == nil && tctx.Err() != nil:
				// Calls cut off by the budget would read as cosmoguard
				// failures; there is no verdict.
				res.Class, res.Detail = Unstable, fmt.Sprintf("no verdict within %s", budget)
			}
			rep.Add(res)
		}()
	}
	wg.Wait()
}

// heightRetries bounds how often a pair answered at different heights is
// fetched again before it is called unstable.
const heightRetries = 3

func grpcTask(node, guard *grpc.ClientConn, m Method, p Params, height int64, o Options) task {
	return func(ctx context.Context) Result {
		res := Result{Protocol: ProtoGRPC, Name: m.FullName}
		req, err := buildRequest(m.Desc, p)
		if err != nil {
			res.Class, res.Detail = Skipped, "building request: "+err.Error()
			return res
		}
		call := func(conn *grpc.ClientConn) Response {
			for attempt := 0; ; attempt++ {
				cctx, cancel := context.WithTimeout(ctx, o.Timeout)
				r := invoke(cctx, conn, m.FullName, req, height)
				cancel()
				if !r.Throttled || attempt == 3 {
					return r
				}
				select {
				case <-ctx.Done():
					return Response{Err: ctx.Err()}
				case <-time.After(time.Duration(attempt+1) * time.Second):
				}
			}
		}
		c := comparison{
			fetch: func() (Response, Response, Response, Response) {
				return call(node), call(guard), call(node), call(guard)
			},
			volatile: volatileMethods[m.FullName],
			pinned:   height,
			delay:    o.RoundDelay,
		}
		if c.volatile {
			c.render = func(r Response) Response { return asJSON(m, r) }
		}
		res.Class, res.Detail = settle(ctx, c)
		return res
	}
}

// rounds bounds how often a differing endpoint is compared again. Public
// endpoints are often load-balanced pools of nodes on different versions
// and heights, so one mismatch is not a verdict; a cosmoguard defect
// reproduces in every round.
const rounds = 3

// comparison is one endpoint's request, played against both sides.
type comparison struct {
	// fetch returns two node answers and two cosmoguard answers, taken
	// interleaved (node, cosmoguard, node, cosmoguard); the second
	// cosmoguard answer is normally served from cache.
	fetch    func() (d1, g1, d2, g2 Response)
	volatile bool
	// render converts answers before comparison; nil keeps them as is.
	render func(Response) Response
	// pinned is the height the request pins, when answers report one.
	pinned int64
	// delay separates rounds, so a later round is not served from the
	// cache entry an earlier one filled.
	delay time.Duration
}

// settle compares an endpoint over up to rounds rounds and returns the
// verdict. It differs only when no round matched; a later match makes it
// unstable, keeping the first difference in the detail. A round in which
// the node failed is inconclusive and does not erase an earlier
// difference. An interrupted comparison has no verdict.
func settle(ctx context.Context, c comparison) (Class, string) {
	var first string
	for round := 1; ; round++ {
		class, detail := settleRound(ctx, c)
		if ctx.Err() != nil {
			return Unstable, "interrupted before a verdict"
		}
		switch {
		case class == Differs:
			if first == "" {
				first = detail
			}
		case class == Failed && first != "":
			// The node did not answer this round; keep the difference.
		case class == Identical && first != "":
			return Unstable, fmt.Sprintf("matched in round %d after differing: %s", round, first)
		default:
			return class, detail
		}
		if round == rounds {
			return Differs, first
		}
		if !sleep(ctx, c.delay) {
			return Unstable, "interrupted before a verdict"
		}
	}
}

// sleep waits d, reporting false if ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// settleRound compares one round. Answers at the wrong height are fetched
// again after the round delay, so the retry reaches upstream rather than
// replaying cosmoguard's cache: a pinned query should never drift, but
// some public nodes ignore the pin. With a pin, any answer stating another
// height is wrong (a cosmoguard cache hit may state none); without one,
// the answers must agree. If the node keeps honouring the pin while every
// cosmoguard answer that states a height states another one, cosmoguard
// lost the pin. When the node's own two answers disagree, cosmoguard
// cannot be judged against it, so the endpoint is reported as unstable.
func settleRound(ctx context.Context, c comparison) (Class, string) {
	render := c.render
	if render == nil {
		render = func(r Response) Response { return r }
	}
	for attempt := 1; ; attempt++ {
		d1, g1, d2, g2 := c.fetch()
		drift := !SameHeight(d1, g1, d2, g2)
		if c.pinned != 0 {
			drift = offPin(d1, c.pinned) || offPin(d2, c.pinned) || offPin(g1, c.pinned) || offPin(g2, c.pinned)
		}
		if !c.volatile && drift {
			if attempt < heightRetries {
				if !sleep(ctx, c.delay) {
					return Unstable, "interrupted before a verdict"
				}
				continue
			}
			detail := fmt.Sprintf("answered at heights node=%d,%d cosmoguard=%d,%d", d1.Height, d2.Height, g1.Height, g2.Height)
			if c.pinned != 0 && d1.Height == c.pinned && d2.Height == c.pinned && lostPin(c.pinned, g1, g2) {
				return Differs, fmt.Sprintf("the node honours the pinned height %d, cosmoguard does not: %s", c.pinned, detail)
			}
			return Unstable, detail + " despite the pin"
		}
		d1, g1, d2, g2 = render(d1), render(g1), render(d2), render(g2)
		switch class, detail := Classify(d1, []Response{d2}, c.volatile); class {
		case Identical:
		case Failed:
			return class, detail
		default:
			// Either node answer may be the failing one.
			if class, detail := Classify(d2, nil, c.volatile); class == Failed {
				return class, detail
			}
			return Unstable, "the node's own answers differ between calls (load-balanced or non-deterministic)"
		}
		return Classify(d1, []Response{g1, g2}, c.volatile)
	}
}

// lostPin reports cosmoguard answers of which at least one states a height
// and every one that does states one other than pinned. A cache hit may
// state no height at all.
func lostPin(pinned int64, rs ...Response) bool {
	stated := false
	for _, r := range rs {
		if r.Height == 0 {
			continue
		}
		stated = true
		if r.Height == pinned {
			return false
		}
	}
	return stated
}

// offPin reports an answer that states a height other than the pinned one.
func offPin(r Response, pinned int64) bool {
	return r.Height != 0 && r.Height != pinned
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
		res.Class, res.Detail = settle(ctx, comparison{
			fetch: func() (Response, Response, Response, Response) {
				return get(o.Node.LCD), get(o.Guard.LCD), get(o.Node.LCD), get(o.Guard.LCD)
			},
			volatile: volatileMethods[m.FullName],
			pinned:   height,
			delay:    o.RoundDelay,
		})
		return res
	}
}
