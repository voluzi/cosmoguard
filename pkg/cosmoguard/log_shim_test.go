package cosmoguard

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"gotest.tools/assert"
)

// TestLogShim_HonoursDefaultAfterSetDefault proves the package-level
// `log` follows whatever handler main.go installs via slog.SetDefault,
// even though `log` is constructed at package-init time before
// setupSlog has a chance to run. Regression guard for the bug where
// the old `log = newEntry(slog.Default())` snapshotted the initial
// stderr text-handler and silently dropped every CLI log-level /
// log-format override.
func TestLogShim_HonoursDefaultAfterSetDefault(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	log.Info("after-setdefault")
	got := buf.String()
	assert.Assert(t, strings.Contains(got, `"msg":"after-setdefault"`),
		"package-level log did not route to the new slog.Default; got %q", got)
}

// TestLogShim_PreboundAttrsTrackDefaultChanges proves WithField / WithFields
// chains also honour SetDefault swaps — bound attrs accumulate locally on
// the lazyDefaultHandler but the handler itself re-resolves slog.Default()
// on every Handle call.
func TestLogShim_PreboundAttrsTrackDefaultChanges(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	bound := log.WithField("rid", "abc").WithField("op", "test")

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

	bound.Info("chained")
	got := buf.String()
	assert.Assert(t, strings.Contains(got, `"rid":"abc"`), "expected pre-bound rid; got %q", got)
	assert.Assert(t, strings.Contains(got, `"op":"test"`), "expected pre-bound op; got %q", got)
	assert.Assert(t, strings.Contains(got, `"msg":"chained"`), "expected msg; got %q", got)
}
