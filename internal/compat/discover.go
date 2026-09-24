package compat

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// latestHeight returns the node's latest block height.
func latestHeight(ctx context.Context, h *httpDoer, rpc string) (int64, error) {
	var st struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := h.getJSON(ctx, rpc+"/status", nil, &st); err != nil {
		return 0, err
	}
	return strconv.ParseInt(st.Result.SyncInfo.LatestBlockHeight, 10, 64)
}

// discoverParams reads live values for request fields from the node at
// the pinned height. A value that cannot be found is left out and the
// endpoints needing it are reported as skipped; notes say why.
func discoverParams(ctx context.Context, h *httpDoer, node Endpoints, height int64) (Params, []string) {
	p := Params{"height": strconv.FormatInt(height, 10)}
	var notes []string
	pin := map[string]string{"x-cosmos-block-height": strconv.FormatInt(height, 10)}
	get := func(key, path string, out any) bool {
		if err := h.getJSON(ctx, node.LCD+path, pin, out); err != nil {
			notes = append(notes, fmt.Sprintf("%s: %v", key, err))
			return false
		}
		return true
	}

	var vals struct {
		Validators []struct {
			OperatorAddress string `json:"operator_address"`
		} `json:"validators"`
	}
	if get("validator", "/cosmos/staking/v1beta1/validators?status=BOND_STATUS_BONDED&pagination.limit=1", &vals) && len(vals.Validators) > 0 {
		p["validator"] = vals.Validators[0].OperatorAddress
		var dels struct {
			DelegationResponses []struct {
				Delegation struct {
					DelegatorAddress string `json:"delegator_address"`
				} `json:"delegation"`
			} `json:"delegation_responses"`
		}
		if get("account", "/cosmos/staking/v1beta1/validators/"+p["validator"]+"/delegations?pagination.limit=1", &dels) && len(dels.DelegationResponses) > 0 {
			p["account"] = dels.DelegationResponses[0].Delegation.DelegatorAddress
		}
	}

	var set struct {
		Validators []struct {
			Address string `json:"address"`
		} `json:"validators"`
	}
	if get("consensus", "/cosmos/base/tendermint/v1beta1/validatorsets/"+p["height"]+"?pagination.limit=1", &set) && len(set.Validators) > 0 {
		p["consensus"] = set.Validators[0].Address
	}

	var supply struct {
		Supply []struct {
			Denom string `json:"denom"`
		} `json:"supply"`
	}
	if get("denom", "/cosmos/bank/v1beta1/supply?pagination.limit=50", &supply) && len(supply.Supply) > 0 {
		p["denom"] = supply.Supply[0].Denom
		// Prefer a native denom: its path form needs no escaping.
		for _, s := range supply.Supply {
			if !strings.Contains(s.Denom, "/") {
				p["denom"] = s.Denom
				break
			}
		}
	}

	var props struct {
		Proposals []struct {
			ID string `json:"id"`
		} `json:"proposals"`
	}
	if get("proposal_id", "/cosmos/gov/v1/proposals?pagination.limit=1&pagination.reverse=true", &props) && len(props.Proposals) > 0 {
		p["proposal_id"] = props.Proposals[0].ID
	}

	var chans struct {
		Channels []struct {
			ChannelID      string   `json:"channel_id"`
			PortID         string   `json:"port_id"`
			ConnectionHops []string `json:"connection_hops"`
		} `json:"channels"`
	}
	if get("channel_id", "/ibc/core/channel/v1/channels?pagination.limit=1", &chans) && len(chans.Channels) > 0 {
		c := chans.Channels[0]
		p["channel_id"], p["port_id"] = c.ChannelID, c.PortID
		if len(c.ConnectionHops) > 0 {
			p["connection_id"] = c.ConnectionHops[0]
			var conn struct {
				Connection struct {
					ClientID string `json:"client_id"`
				} `json:"connection"`
			}
			if get("client_id", "/ibc/core/connection/v1/connections/"+p["connection_id"], &conn) {
				p["client_id"] = conn.Connection.ClientID
			}
		}
	}

	if hash, err := findTxHash(ctx, h, node.RPC, height); err != nil {
		notes = append(notes, "tx_hash: "+err.Error())
	} else {
		p["tx_hash"] = hash
	}
	return p, notes
}

// findTxHash returns the hash of the first transaction at or below height,
// looking back at most 50 blocks.
func findTxHash(ctx context.Context, h *httpDoer, rpc string, height int64) (string, error) {
	for hh := height; hh > height-50 && hh > 0; hh-- {
		var blk struct {
			Result struct {
				Block struct {
					Data struct {
						Txs []string `json:"txs"`
					} `json:"data"`
				} `json:"block"`
			} `json:"result"`
		}
		if err := h.getJSON(ctx, fmt.Sprintf("%s/block?height=%d", rpc, hh), nil, &blk); err != nil {
			return "", err
		}
		if txs := blk.Result.Block.Data.Txs; len(txs) > 0 {
			raw, err := base64.StdEncoding.DecodeString(txs[0])
			if err != nil {
				return "", err
			}
			sum := sha256.Sum256(raw)
			return strings.ToUpper(hex.EncodeToString(sum[:])), nil
		}
	}
	return "", fmt.Errorf("no transaction in the 50 blocks up to %d", height)
}
