package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard"
)

const (
	defaultConfigFileName = "cosmoguard.yaml"
	shutdownGrace         = 30 * time.Second
)

// Top-level flags live on the root FlagSet (flag.CommandLine). Subcommands
// each get their own FlagSet so subcommand-specific flags don't pollute
// the root help text.
var (
	configFile   string
	logLevel     string
	logFormat    string
	printVersion bool

	// Deprecated: kept for back-compat. `cosmoguard validate` and
	// `cosmoguard migrate-config` are the v4 subcommand forms.
	validateOnly  bool
	migrateConfig bool
)

func init() {
	homedir, _ := os.UserHomeDir()
	flag.StringVar(&configFile, "config", filepath.Join(homedir, defaultConfigFileName), "Path to configuration file.")
	flag.StringVar(&logLevel, "log-level", "info", "log level.")
	flag.StringVar(&logFormat, "log-format", "json", "log format (either json or text)")
	flag.BoolVar(&printVersion, "version", false, "print cosmoguard version")
	flag.BoolVar(&validateOnly, "validate", false, "[deprecated] use `cosmoguard validate` subcommand")
	flag.BoolVar(&migrateConfig, "migrate-config", false, "[deprecated] use `cosmoguard migrate-config` subcommand")
	flag.Usage = printUsage
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  cosmoguard [flags]                start the proxy
  cosmoguard validate [flags]       validate a config file and exit (0 OK, 1 error)
  cosmoguard migrate-config [flags] rewrite a v3 config in v4 form and exit
  cosmoguard version                print version + commit and exit

Flags:
`)
	flag.PrintDefaults()
}

// dispatch returns true when the first positional arg matches a known
// subcommand AND the subcommand handles it (exits or runs). Returns
// false when the user is invoking the proxy itself (no subcommand).
//
// Each subcommand registers its own FlagSet so it can document its own
// flag surface in `cosmoguard <sub> -h`.
func dispatch(args []string) (handled bool) {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "validate":
		runValidate(args[1:])
		return true
	case "migrate-config":
		runMigrate(args[1:])
		return true
	case "version":
		fmt.Printf("Version: %s\nCommit hash: %s\n", cosmoguard.Version, cosmoguard.CommitHash)
		os.Exit(0)
	}
	return false
}

// runValidate parses + prepares the config (defaults, sort, compile) and
// exits 0 on success, 1 on failure. Useful in CI / pre-deploy checks:
// catches malformed YAML, invalid globs, missing required env vars, bad
// CIDRs — without binding any ports.
func runValidate(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	homedir, _ := os.UserHomeDir()
	cfg := fs.String("config", filepath.Join(homedir, defaultConfigFileName), "Path to configuration file.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cosmoguard validate [flags] [config-path]\n\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)
	// Accept the config path as a positional too: `cosmoguard validate
	// my-config.yaml` mirrors the `gh repo view <name>` style.
	path := *cfg
	if fs.NArg() > 0 {
		path = fs.Arg(0)
	}
	if _, err := cosmoguard.ReadConfigFromFile(path); err != nil {
		fmt.Fprintf(os.Stderr, "config invalid: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("config OK")
	os.Exit(0)
}

// runMigrate rewrites a v3-shaped config file in v4 form, backing the
// original up to <file>.v3.bak. Cosmetic: v3 syntax keeps working at
// runtime — this only normalizes the file shape so operators don't
// have to remember the old vocabulary.
func runMigrate(args []string) {
	fs := flag.NewFlagSet("migrate-config", flag.ExitOnError)
	homedir, _ := os.UserHomeDir()
	cfg := fs.String("config", filepath.Join(homedir, defaultConfigFileName), "Path to configuration file.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cosmoguard migrate-config [flags] [config-path]\n\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)
	path := *cfg
	if fs.NArg() > 0 {
		path = fs.Arg(0)
	}
	n, err := cosmoguard.MigrateV3Config(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate failed: %v\n", err)
		os.Exit(1)
	}
	if n == 0 {
		fmt.Println("config already in v4 form; no changes")
		os.Exit(0)
	}
	fmt.Printf("migrated %d v3 element(s) in %s (backup: %s.v3.bak)\n", n, path, path)
	os.Exit(0)
}

func main() {
	// Subcommands dispatch FIRST so `cosmoguard validate -config x.yaml`
	// works regardless of where positional args land relative to flags.
	// Once we're past dispatch, fall through to the standard flag parse
	// for the run-the-proxy path.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		if dispatch(os.Args[1:]) {
			return
		}
	}

	flag.Parse()
	if printVersion {
		fmt.Printf("Version: %s\nCommit hash: %s\n", cosmoguard.Version, cosmoguard.CommitHash)
		os.Exit(0)
	}

	setupSlog(logLevel, logFormat)

	// Back-compat: --validate / --migrate-config still work, but emit a
	// one-line deprecation note so operators migrate to the subcommand
	// form.
	if validateOnly {
		slog.Warn("--validate is deprecated; use `cosmoguard validate`")
		runValidate([]string{"-config", configFile})
		return
	}
	if migrateConfig {
		slog.Warn("--migrate-config is deprecated; use `cosmoguard migrate-config`")
		runMigrate([]string{"-config", configFile})
		return
	}

	f, err := cosmoguard.NewFromFile(configFile)
	if err != nil {
		slog.Error("cosmoguard startup failed", "error", err)
		os.Exit(1)
	}

	// Trap SIGTERM/SIGINT for graceful shutdown. On signal: stop accepting
	// new connections, drain in-flight requests up to shutdownGrace,
	// release caches/limiters, then exit.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- f.Run() }()

	select {
	case err := <-runErrCh:
		// Run() returned — typically a fatal startup error or a config
		// watcher failure. Drain the rest of cosmoguard before exiting
		// so the OTLP exporter flushes buffered spans, metrics vecs
		// unregister, and proxy goroutines stop cleanly. Without this
		// drain, the process exited immediately on Run() error and
		// the last few seconds of telemetry — the exact window an
		// operator needs to diagnose the failure — were lost.
		if err != nil {
			slog.Error("cosmoguard.Run returned", "error", err)
			ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
			if sherr := f.Shutdown(ctx); sherr != nil {
				slog.Error("shutdown after Run error returned", "error", sherr)
			}
			cancel()
			os.Exit(1)
		}
	case sig := <-sigCh:
		slog.Info("shutdown signal received, draining", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := f.Shutdown(ctx); err != nil {
			slog.Error("shutdown returned error", "error", err)
			os.Exit(1)
		}
		slog.Info("shutdown complete")
	}
}

// setupSlog configures the default slog logger from the CLI flags. JSON
// output by default (matches v3 behavior); text handler when requested.
func setupSlog(levelStr, format string) {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "info", "":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		fmt.Fprintf(os.Stderr, "unknown log level %q\n", levelStr)
		os.Exit(2)
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	// Lowercase to match the level-string handling above; --log-format TEXT
	// (or Text, etc.) previously fell through to JSON silently.
	if strings.ToLower(format) == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}
