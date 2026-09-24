package compat

import (
	"testing"

	"gotest.tools/assert"
)

func TestBlockchainComparesPinnedMetas(t *testing.T) {
	var mask []string
	for _, c := range cometCalls(100, "", "") {
		if c.method == "blockchain" {
			assert.Assert(t, !c.volatile, "the pinned block_metas are compared, not only their shape")
			mask = c.mask
		}
	}
	render := maskFields(mask)
	assert.Assert(t, render != nil)
	body := func(last, hash string) Response {
		return ok(`{"jsonrpc":"2.0","id":1,"result":{"last_height":"` + last + `","block_metas":[{"block_id":{"hash":"` + hash + `"}}]}}`)
	}
	// Only the answering node's tip may differ.
	c, detail := Classify(render(body("105", "AA")), []Response{render(body("107", "AA"))}, false)
	assert.Equal(t, c, Identical, detail)
	c, _ = Classify(render(body("105", "AA")), []Response{render(body("105", "BB"))}, false)
	assert.Equal(t, c, Differs, "a changed block hash is a difference")
	assert.Assert(t, maskFields(nil) == nil)
}
