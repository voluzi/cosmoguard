package compat

import (
	"testing"

	"gotest.tools/assert"
)

func TestFillPath(t *testing.T) {
	p := Params{"account": "nibi1abc", "validator": "nibivaloper1x", "denom": "tf/nibi1z/utestate", "height": "10"}
	cases := []struct {
		tmpl, want string
		missing    []string
	}{
		{"/cosmos/bank/v1beta1/balances/{address}/by_denom", "/cosmos/bank/v1beta1/balances/nibi1abc/by_denom", nil},
		{"/x/{delegator_addr}/y/{validator_addr}", "/x/nibi1abc/y/nibivaloper1x", nil},
		// A single-segment variable escapes the slashes in the value...
		{"/cosmos/bank/v1beta1/denoms_metadata/{denom}", "/cosmos/bank/v1beta1/denoms_metadata/tf%2Fnibi1z%2Futestate", nil},
		// ...a ** variable keeps them.
		{"/cosmos/bank/v1beta1/supply/by_denom/{denom=**}", "/cosmos/bank/v1beta1/supply/by_denom/tf/nibi1z/utestate", nil},
		{"/blocks/{height}", "/blocks/10", nil},
		// A pattern with slashes is multi-segment too.
		{"/v1/{denom=tf/*/*}", "/v1/tf/nibi1z/utestate", nil},
		{"/emissions/{topic_id}/{address}", "/emissions//nibi1abc", []string{"topic_id"}},
		{"/no/vars", "/no/vars", nil},
	}
	for _, tc := range cases {
		got, missing := FillPath(tc.tmpl, p)
		assert.Equal(t, got, tc.want)
		assert.DeepEqual(t, missing, tc.missing)
	}
}

func TestLookup(t *testing.T) {
	p := Params{"account": "acc", "topic_id": "7", "height": "10"}
	for field, want := range map[string]string{
		"address":        "acc",
		"granter":        "acc",
		"pagination.key": "",
		"topic_id":       "7",  // a key of its own (from --param)
		"req.topic_id":   "7",  // dotted: last segment
		"block_height":   "10", // alias
	} {
		got, ok := p.Lookup(field)
		assert.Equal(t, ok, want != "", field)
		assert.Equal(t, got, want, field)
	}
}
