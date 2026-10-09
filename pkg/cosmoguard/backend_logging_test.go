package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
	"github.com/voluzi/olric"
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

func TestL2WriteSkipsKeepBackendErrorsVisible(t *testing.T) {
	for _, tc := range []struct {
		cause error
		level string
	}{
		{olric.ErrWriteQuorum, "ERROR"},
		{errors.New("connection refused"), "ERROR"},
		{olricstore.ErrCapacity, "DEBUG"},
		{boundedcall.ErrRejected, "DEBUG"},
		{olric.ErrEntryTooLarge, "DEBUG"},
	} {
		var out bytes.Buffer
		logger := newEntry(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
		logCacheBackendError(logger, errors.Join(cache.ErrL2Skipped, tc.cause), "cache failed")
		require.Contains(t, out.String(), "level="+tc.level)
		require.Contains(t, out.String(), tc.cause.Error())
	}
}
