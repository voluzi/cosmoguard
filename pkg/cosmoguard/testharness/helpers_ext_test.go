package testharness_test

// boolPtr returns a pointer to b, for the *bool config fields
// (MetricsConfig.Enable, RpcConfig.WebSocketEnabled).
func boolPtr(b bool) *bool { return &b }
