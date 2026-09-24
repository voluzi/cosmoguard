package cosmoguard

import (
	"fmt"
	"strings"
)

const (
	methodSubscribeCosmos      = "subscribe"
	methodUnsubscribeCosmos    = "unsubscribe"
	methodUnsubscribeAllCosmos = "unsubscribe_all"
	methodSubscribeEth         = "eth_subscribe"
	methodUnsubscribeEth       = "eth_unsubscribe"
)

func hasSubscriptionMethod(request *JsonRpcMsg) bool {
	if request.Method == methodSubscribeCosmos ||
		request.Method == methodUnsubscribeCosmos ||
		request.Method == methodUnsubscribeAllCosmos ||
		request.Method == methodSubscribeEth ||
		request.Method == methodUnsubscribeEth {
		return true
	}
	return false
}

func getSubscriptionParam(req *JsonRpcMsg) (string, error) {
	// Try to get it from dictionary with query key (cosmos only)
	if params, ok := req.Params.(map[string]interface{}); ok {
		if query, ok := params["query"].(string); ok {
			return query, nil
		}
	}

	// Otherwise, get from array of values (Both cosmos and eth)
	params, ok := req.Params.([]interface{})
	if !ok {
		return "", fmt.Errorf("bad params for subscribe")
	}

	// A client-supplied empty params array ({"method":"subscribe","params":[]})
	// would index params[0] out of range and panic the connection handler.
	// Guard it as a malformed request instead.
	if len(params) == 0 {
		return "", fmt.Errorf("missing subscription param")
	}

	query, ok := params[0].(string)

	if !ok {
		return "", fmt.Errorf("bad query format (should be string)")
	}

	// eth_subscribe options (log address/topics filters and the like)
	// follow the subscription name. The whole array is the subscription
	// key so distinct filters get distinct upstream subscriptions; the
	// encoder sorts object keys, so equivalent filters still share one.
	// A bare name starting with "[" is encoded too, so it cannot pass for
	// an encoded array.
	if req.Method == methodSubscribeEth && (len(params) > 1 || strings.HasPrefix(query, "[")) {
		key, err := json.Marshal(params)
		if err != nil {
			return "", fmt.Errorf("bad subscription params: %w", err)
		}
		return string(key), nil
	}

	return query, nil
}

// ethSubscribeParams rebuilds the eth_subscribe params from a key
// produced by getSubscriptionParam.
func ethSubscribeParams(param string) []interface{} {
	if strings.HasPrefix(param, "[") {
		var params []interface{}
		if err := json.Unmarshal([]byte(param), &params); err == nil {
			return params
		}
	}
	return []interface{}{param}
}

func isEthSubscriptionID(params string) bool {
	return strings.HasPrefix(params, "0x")
}
