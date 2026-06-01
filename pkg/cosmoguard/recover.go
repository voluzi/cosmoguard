package cosmoguard

import (
	"net/http"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recoverHTTP guards an HTTP handler against panics in the proxy
// pipeline. On panic it logs the value + stack, then — if no response
// header has been written — replies 500. If the upstream had already
// committed bytes to the client, the recover just terminates the
// goroutine cleanly so the connection drops.
//
// Cosmoguard treats panics as bugs, not expected error paths. The
// recover exists so one malformed request can't take down the goroutine
// the http.Server hands it to.
func recoverHTTP(logger *Entry, w http.ResponseWriter, r *http.Request) {
	rec := recover()
	if rec == nil {
		return
	}
	fields := Fields{
		"panic":  rec,
		"method": r.Method,
		"path":   r.URL.Path,
		"stack":  string(debug.Stack()),
	}
	if logger != nil {
		logger.WithFields(fields).Error("panic in HTTP handler")
	}
	// Best-effort 500 — if upstream already wrote headers, this is a
	// no-op at the wire (Go's response writer drops late WriteHeader),
	// which is the correct outcome.
	defer func() { _ = recover() }() // swallow any panic from the late write
	w.WriteHeader(http.StatusInternalServerError)
}

// recoverWS guards a long-lived WebSocket connection handler. There's
// no response writer to recover into — the upgrade has already happened
// — so this only logs. The deferred caller (HandleConnection) will see
// the goroutine return and close the connection.
func recoverWS(logger *Entry, addr string) {
	rec := recover()
	if rec == nil {
		return
	}
	if logger != nil {
		logger.WithFields(Fields{
			"panic": rec,
			"peer":  addr,
			"stack": string(debug.Stack()),
		}).Error("panic in WebSocket handler")
	}
}

// recoverStream wraps a gRPC StreamHandler with panic recovery. On
// panic it logs the stack with the gRPC method (if extractable from
// the stream) and returns codes.Internal so the client sees a clean
// gRPC status rather than a stream reset.
func recoverStream(logger *Entry, inner grpc.StreamHandler) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) (err error) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			method, _ := grpc.MethodFromServerStream(stream)
			if logger != nil {
				logger.WithFields(Fields{
					"panic":  rec,
					"method": method,
					"stack":  string(debug.Stack()),
				}).Error("panic in gRPC handler")
			}
			err = status.Errorf(codes.Internal, "internal error")
		}()
		return inner(srv, stream)
	}
}
