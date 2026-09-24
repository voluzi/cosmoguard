package compat

import (
	"context"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

// scripted returns a fetch that plays back one [node, guard, node, guard]
// round per call, repeating the last.
func scripted(rounds ...[4]Response) (func() (Response, Response, Response, Response), *int) {
	calls := 0
	return func() (Response, Response, Response, Response) {
		r := rounds[min(calls, len(rounds)-1)]
		calls++
		return r[0], r[1], r[2], r[3]
	}, &calls
}

func TestSettle(t *testing.T) {
	a, b := ok("a"), ok("b")
	at := func(r Response, h int64) Response { r.Height = h; return r }
	cmp := func(fetch func() (Response, Response, Response, Response)) comparison {
		return comparison{fetch: fetch}
	}
	ctx := t.Context()

	t.Run("identical in one round", func(t *testing.T) {
		fetch, calls := scripted([4]Response{a, a, a, a})
		c, _ := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Identical)
		assert.Equal(t, *calls, 1)
	})
	t.Run("differs only when every round differs", func(t *testing.T) {
		fetch, calls := scripted([4]Response{a, b, a, b})
		c, _ := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Differs)
		assert.Equal(t, *calls, rounds)
	})
	t.Run("rounds wait out the delay", func(t *testing.T) {
		fetch, _ := scripted([4]Response{a, b, a, b})
		start := time.Now()
		settle(ctx, comparison{fetch: fetch, delay: 20 * time.Millisecond})
		assert.Assert(t, time.Since(start) >= 40*time.Millisecond, "two pauses between three rounds")
	})
	t.Run("later match is unstable, keeping the first difference", func(t *testing.T) {
		fetch, _ := scripted([4]Response{a, b, a, b}, [4]Response{a, a, a, a})
		c, detail := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "matched in round 2"), detail)
	})
	t.Run("node disagreeing with itself is unstable", func(t *testing.T) {
		fetch, _ := scripted([4]Response{a, a, b, a})
		c, detail := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "node's own answers"), detail)
	})
	t.Run("node failing on one call is failed, not unstable", func(t *testing.T) {
		limited := Response{Status: 429, Throttled: true}
		for _, round := range [][4]Response{{limited, a, a, a}, {a, a, limited, a}} {
			fetch, _ := scripted(round)
			c, detail := settle(ctx, cmp(fetch))
			assert.Equal(t, c, Failed, detail)
		}
	})
	t.Run("height drift is refetched, then unstable", func(t *testing.T) {
		fetch, calls := scripted([4]Response{at(a, 5), at(a, 6), at(a, 5), at(a, 6)})
		c, detail := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "despite the pin"), detail)
		assert.Equal(t, *calls, heightRetries)
	})
	t.Run("node ignoring the pin is unstable", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(a, 9), at(a, 8), at(a, 9), at(a, 8)})
		c, _ := settle(ctx, comparison{fetch: fetch, pinned: 5})
		assert.Equal(t, c, Unstable)
	})
	t.Run("node answering off the pin is unstable even if cosmoguard's height is unknown", func(t *testing.T) {
		// A cosmoguard cache hit states no height; the node's stale
		// answer must not be compared against it.
		fetch, _ := scripted([4]Response{at(a, 9), at(b, 0), at(a, 9), at(b, 0)})
		c, detail := settle(ctx, comparison{fetch: fetch, pinned: 5})
		assert.Equal(t, c, Unstable, detail)
	})
	t.Run("cosmoguard losing the pin differs", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(a, 5), at(a, 9), at(a, 5), at(a, 9)})
		c, detail := settle(ctx, comparison{fetch: fetch, pinned: 5})
		assert.Equal(t, c, Differs)
		assert.Assert(t, strings.Contains(detail, "cosmoguard does not"), detail)
	})
	t.Run("drift that settles is compared", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(a, 5), at(a, 6), at(a, 5), at(a, 6)}, [4]Response{at(a, 6), at(a, 6), at(a, 6), at(a, 6)})
		c, _ := settle(ctx, cmp(fetch))
		assert.Equal(t, c, Identical)
	})
	t.Run("volatile ignores height", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(ok(`{"h":1}`), 5), at(ok(`{"h":2}`), 6), at(ok(`{"h":1}`), 5), at(ok(`{"h":2}`), 6)})
		c, _ := settle(ctx, comparison{fetch: fetch, volatile: true})
		assert.Equal(t, c, Identical)
	})
}

func TestRunTasksBudget(t *testing.T) {
	rep := &Report{}
	o := Options{Concurrency: 2, Timeout: time.Millisecond}
	slow := func(ctx context.Context) Result {
		<-ctx.Done()
		return Result{Protocol: ProtoRPC, Name: "slow", Class: Differs, Detail: "cosmoguard: context deadline exceeded"}
	}
	fast := func(context.Context) Result { return Result{Protocol: ProtoRPC, Name: "fast", Class: Identical} }
	runTasks(t.Context(), []task{slow, fast}, o, rep)
	assert.Equal(t, rep.Count(Unstable), 1, "a task cut off by its budget has no verdict")
	assert.Equal(t, rep.Count(Identical), 1)
	assert.Equal(t, rep.Count(Differs), 0)
}

func TestReport(t *testing.T) {
	r := &Report{Chain: "c", Height: 1}
	r.Add(Result{Protocol: ProtoLCD, Name: "/a", Class: Identical})
	r.Add(Result{Protocol: ProtoLCD, Name: "/b", Class: Skipped, Detail: "no live value for x"})
	assert.Assert(t, !r.HasDifferences())
	r.Add(Result{Protocol: ProtoGRPC, Name: "/m", Class: Differs, Detail: "at $.x"})
	assert.Assert(t, r.HasDifferences())

	var sb strings.Builder
	r.WriteSummary(&sb)
	out := sb.String()
	assert.Assert(t, strings.Contains(out, "DIFFERS (1):\n  grpc /m: at $.x"), out)
	assert.Assert(t, !strings.Contains(out, "no live value"), "skip reasons belong in the JSON report")

	r.Add(Result{Protocol: ProtoGRPC, Name: "/n", Class: Failed, Detail: "node: 504\n<html>" + strings.Repeat("x", 300)})
	sb.Reset()
	r.WriteSummary(&sb)
	assert.Assert(t, !strings.Contains(sb.String(), "<html>"), "summary entries stay on one line")
}
