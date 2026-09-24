package compat

import (
	"strings"
	"testing"

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

func identity(r Response) Response { return r }

func TestSettle(t *testing.T) {
	a, b := ok("a"), ok("b")
	at := func(r Response, h int64) Response { r.Height = h; return r }

	t.Run("identical in one round", func(t *testing.T) {
		fetch, calls := scripted([4]Response{a, a, a, a})
		c, _ := settle(fetch, false, identity)
		assert.Equal(t, c, Identical)
		assert.Equal(t, *calls, 1)
	})
	t.Run("differs only when every round differs", func(t *testing.T) {
		fetch, calls := scripted([4]Response{a, b, a, b})
		c, _ := settle(fetch, false, identity)
		assert.Equal(t, c, Differs)
		assert.Equal(t, *calls, rounds)
	})
	t.Run("later match is unstable, keeping the first difference", func(t *testing.T) {
		fetch, _ := scripted([4]Response{a, b, a, b}, [4]Response{a, a, a, a})
		c, detail := settle(fetch, false, identity)
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "matched in round 2"), detail)
	})
	t.Run("node disagreeing with itself is unstable", func(t *testing.T) {
		fetch, _ := scripted([4]Response{a, a, b, a})
		c, detail := settle(fetch, false, identity)
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "node's own answers"), detail)
	})
	t.Run("height drift is refetched, then unstable", func(t *testing.T) {
		fetch, calls := scripted([4]Response{at(a, 5), at(a, 6), at(a, 5), at(a, 6)})
		c, detail := settle(fetch, false, identity)
		assert.Equal(t, c, Unstable)
		assert.Assert(t, strings.Contains(detail, "despite the pin"), detail)
		assert.Equal(t, *calls, heightRetries)
	})
	t.Run("drift that settles is compared", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(a, 5), at(a, 6), at(a, 5), at(a, 6)}, [4]Response{at(a, 6), at(a, 6), at(a, 6), at(a, 6)})
		c, _ := settle(fetch, false, identity)
		assert.Equal(t, c, Identical)
	})
	t.Run("volatile ignores height", func(t *testing.T) {
		fetch, _ := scripted([4]Response{at(ok(`{"h":1}`), 5), at(ok(`{"h":2}`), 6), at(ok(`{"h":1}`), 5), at(ok(`{"h":2}`), 6)})
		c, _ := settle(fetch, true, identity)
		assert.Equal(t, c, Identical)
	})
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
