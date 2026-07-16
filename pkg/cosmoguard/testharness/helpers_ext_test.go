package testharness_test

// boolPtr returns a pointer to b, for the *bool config fields
// (MetricsConfig.Enable, RpcConfig.WebSocketEnabled).
func boolPtr(b bool) *bool { return &b }

// int64Ptr returns a pointer to n, for the *int64 config fields
// (ServerConfig.MaxRequestBody, ServerConfig.WSReadLimit).
func int64Ptr(n int64) *int64 { return &n }
