package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v5/internal/boundedcall"
)

func TestBoundedBackendFailuresUseDebugLogs(t *testing.T) {
	for _, err := range []error{boundedcall.ErrRejected, errors.Join(boundedcall.ErrTimeout, context.DeadlineExceeded), errors.New("other backend error")} {
		var out bytes.Buffer
		logger := newEntry(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
		logCacheBackendError(logger, err, "cache failed")
		logLimiterBackendError(logger, err, "limiter failed")
		if boundedcall.IsFailure(err) {
			require.Equal(t, 2, bytes.Count(out.Bytes(), []byte("level=DEBUG")))
		} else {
			require.Contains(t, out.String(), "level=ERROR")
			require.Contains(t, out.String(), "level=WARN")
		}
	}
}
