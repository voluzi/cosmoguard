package cosmoguard

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// log_shim.go: cosmoguard internally emits logs via a package-level `log`
// variable that exposes a logrus-shaped API. Behind that API: log/slog.
// This lets existing call sites (`log.WithField(...).Info(...)`,
// `p.log.WithFields(map[string]interface{}{...}).Error(...)`) keep working
// after the v4 logrus→slog migration without rewriting ~80 call sites
// across ten files.
//
// The API surface mirrors logrus's `*log.Entry` exactly — same method
// names, same chaining shape, same fields-map literal type. Output goes
// through slog.Default() which `cmd/cosmoguard/main.go` configures with
// the JSON handler in the same shape v3 produced, so log consumers see
// no wire-format change.

// Fields is the logrus.Fields shape — operators were already passing
// `log.Fields{...}` literals; preserve the type so those compile.
type Fields = map[string]interface{}

// Entry wraps a *slog.Logger and exposes logrus-style method names. The
// pre-bound-attrs pattern from logrus carries over: WithField/WithFields/
// WithError return a new Entry with the attrs baked in, ready to chain.
type Entry struct {
	s *slog.Logger
}

// lazyDefaultHandler delegates every Handle call to slog.Default()'s
// current handler at emit time, not at init time. The package-level
// `log` is constructed at package-init, BEFORE main.go's setupSlog
// installs the CLI-configured handler via slog.SetDefault. A naïve
// `log = newEntry(slog.Default())` therefore snapshotted the initial
// stderr text-handler and every subsequent log line from this package
// silently bypassed --log-level / --log-format.
//
// Resolving slog.Default() inside Handle / Enabled / WithAttrs fixes
// that: bound attrs are accumulated locally and replayed against
// whichever handler is current when the record actually emits. Groups
// are handled the same way so a future `.WithGroup("foo")` call site
// keeps the lazy-resolve contract; cosmoguard doesn't use groups
// today but the cost of supporting them is trivial.
type lazyDefaultHandler struct {
	ops []handlerOp
}

type handlerOp struct {
	attrs []slog.Attr // set when group == ""
	group string      // set when attrs == nil
}

// build materializes the current slog.Default().Handler() with every
// recorded WithAttrs / WithGroup re-applied in order. Called on every
// Handle / Enabled so changes to slog.Default() take immediate effect.
func (h *lazyDefaultHandler) build() slog.Handler {
	out := slog.Default().Handler()
	for _, op := range h.ops {
		if op.group != "" {
			out = out.WithGroup(op.group)
			continue
		}
		if len(op.attrs) > 0 {
			out = out.WithAttrs(op.attrs)
		}
	}
	return out
}

func (h *lazyDefaultHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.build().Enabled(ctx, lvl)
}

func (h *lazyDefaultHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.build().Handle(ctx, r)
}

func (h *lazyDefaultHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	nh := &lazyDefaultHandler{ops: make([]handlerOp, len(h.ops), len(h.ops)+1)}
	copy(nh.ops, h.ops)
	nh.ops = append(nh.ops, handlerOp{attrs: attrs})
	return nh
}

func (h *lazyDefaultHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := &lazyDefaultHandler{ops: make([]handlerOp, len(h.ops), len(h.ops)+1)}
	copy(nh.ops, h.ops)
	nh.ops = append(nh.ops, handlerOp{group: name})
	return nh
}

// log is the package-level logger used by every call site in cosmoguard.
// Wraps lazyDefaultHandler so the entry point's slog.SetDefault from
// setupSlog flows through even though `log` is constructed at init.
var log = newEntry(slog.New(&lazyDefaultHandler{}))

func newEntry(s *slog.Logger) *Entry { return &Entry{s: s} }

// ---------- pre-bind chain methods (return *Entry) ----------

func (e *Entry) WithField(key string, value interface{}) *Entry {
	return newEntry(e.s.With(key, value))
}

func (e *Entry) WithFields(f Fields) *Entry {
	args := make([]any, 0, len(f)*2)
	for k, v := range f {
		args = append(args, k, v)
	}
	return newEntry(e.s.With(args...))
}

func (e *Entry) WithError(err error) *Entry {
	return newEntry(e.s.With("error", err))
}

// ---------- emit methods ----------

func (e *Entry) Info(args ...interface{})                  { e.s.Info(fmt.Sprint(args...)) }
func (e *Entry) Infof(format string, args ...interface{})  { e.s.Info(fmt.Sprintf(format, args...)) }
func (e *Entry) Warn(args ...interface{})                  { e.s.Warn(fmt.Sprint(args...)) }
func (e *Entry) Warnf(format string, args ...interface{})  { e.s.Warn(fmt.Sprintf(format, args...)) }
func (e *Entry) Error(args ...interface{})                 { e.s.Error(fmt.Sprint(args...)) }
func (e *Entry) Errorf(format string, args ...interface{}) { e.s.Error(fmt.Sprintf(format, args...)) }
func (e *Entry) Debug(args ...interface{})                 { e.s.Debug(fmt.Sprint(args...)) }
func (e *Entry) Debugf(format string, args ...interface{}) { e.s.Debug(fmt.Sprintf(format, args...)) }

// Fatal logs at Error level then exits 1. Matches logrus.Fatal — call
// sites use it sparingly (only main.go on startup failure).
func (e *Entry) Fatal(args ...interface{}) {
	e.s.Error(fmt.Sprint(args...))
	os.Exit(1)
}
func (e *Entry) Fatalf(format string, args ...interface{}) {
	e.s.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}

// ---------- package-level functions (no pre-bind) ----------
//
// Mirror logrus's `log.X(...)` shape. Implemented as methods on the
// package-level `log` *Entry, so e.g. `log.WithField(...)` calls the
// method on this object.
