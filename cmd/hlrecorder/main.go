// Command hlrecorder is a launch-and-forget raw recorder for Hyperliquid perpetuals.
//
// It subscribes to the public market-data channels — one WebSocket connection per
// channel, every configured coin on each — and archives every frame it receives to
//
//	<data_dir>/<channel>/<UTC hour>.jsonl.gz
//
// as `<rx_ns>\t<verbatim frame>\n`. Nothing is parsed: a capture is irreplaceable, so
// the recorder's only job is to not lose bytes. The wire and file contract is
// RECORDER_SPEC.md; the design rationale is PERP_COLLECTOR_SPEC.md.
//
// Usage:
//
//	hlrecorder -config collector_hl.yaml
//	hlrecorder -config collector_hl.yaml -dry-run          # validate + print, no network
//	hlrecorder -config collector_hl.yaml -duration 1h      # a one-hour capture
//
// Exit codes: 0 for a run ended by a signal or -duration, 1 for a run that failed or
// stopped early (low disk, write error, startup failure), 2 for bad usage or config.
//
// A capture that stopped early says so in its own manifest.json, so a capture can be
// checked without consulting the logs that produced it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"

	"github.com/algoboy-kevin/go-collector/pkg/services/hyperliquid"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "collector_hl.yaml", "path to the config file")
	dataDir := flag.String("data-dir", "", "override data_dir from the config")
	duration := flag.Duration("duration", 0, "stop after this long (0 runs until signalled)")
	dryRun := flag.Bool("dry-run", false, "validate the config and print the plan, without connecting")
	logLevel := flag.String("log", "info", "log level: debug, info, warn, error")
	flag.Parse()

	if err := setupLogging(*logLevel); err != nil {
		fmt.Fprintf(os.Stderr, "hlrecorder: %v\n", err)
		return 2
	}

	cfg, err := hyperliquid.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hlrecorder: %v\n", err)
		return 2
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}

	if *dryRun {
		return printPlan(cfg, *configPath)
	}

	// A signal and a -duration both end the run the same way; only the reason recorded
	// in the manifest differs, and the recorder distinguishes them from ctx.Err().
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	rec, err := hyperliquid.NewRecorder(hyperliquid.RecorderConfig{
		Config: cfg,
		Info:   hl.NewInfoClient(cfg.Connector.InfoURL),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "hlrecorder: %v\n", err)
		return 2
	}

	slog.Info("hlrecorder starting",
		"config", *configPath,
		"data_dir", cfg.DataDir,
		"coins", strings.Join(cfg.Coins, ","),
		"channels", strings.Join(cfg.Channels, ","),
		"duration", durationLabel(*duration),
		"min_free_bytes", cfg.MinFree(),
	)

	start := time.Now()
	runErr := rec.Run(ctx)
	elapsed := time.Since(start)

	printSummary(rec, elapsed)

	if runErr != nil {
		slog.Error("hlrecorder stopped early", "reason", rec.StopReason(), "err", runErr)
		return 1
	}
	slog.Info("hlrecorder stopped cleanly", "reason", rec.StopReason(), "elapsed", elapsed.Round(time.Second))
	return 0
}

func setupLogging(level string) error {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "", "info":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return fmt.Errorf("unknown log level %q", level)
	}
	// Logs go to stderr so the summary on stdout stays readable and greppable.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	return nil
}

func durationLabel(d time.Duration) string {
	if d <= 0 {
		return "until signalled"
	}
	return d.String()
}

// printPlan validates and describes what a run would do, touching nothing.
//
// It builds the real subscriptions through the same code the recorder uses, so a
// config that prints cleanly is a config that can connect.
func printPlan(cfg *hyperliquid.Config, path string) int {
	plans, err := cfg.Plan()
	if err != nil {
		fmt.Fprintf(os.Stderr, "hlrecorder: %v\n", err)
		return 2
	}
	compression, err := cfg.CompressionLevel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "hlrecorder: %v\n", err)
		return 2
	}

	fmt.Printf("config      %s\n", path)
	fmt.Printf("data_dir    %s\n", cfg.DataDir)
	fmt.Printf("coins       %s\n", strings.Join(cfg.Coins, ","))
	fmt.Printf("channels    %d (one connection each)\n", len(plans))
	fmt.Printf("compression gzip level %d\n", compression)
	fmt.Printf("flush       %s\n", cfg.FlushEvery())
	fmt.Printf("queue       %d frames per channel\n", cfg.QueueSize())
	if floor := cfg.MinFree(); floor > 0 {
		fmt.Printf("disk floor  %d bytes\n", floor)
	} else {
		fmt.Printf("disk floor  disabled\n")
	}
	fmt.Printf("ws          %s\n", cfg.Connector.WSURL)
	fmt.Printf("info        %s\n", cfg.Connector.InfoURL)
	fmt.Println()
	for _, plan := range plans {
		fmt.Printf("  %-15s %d subscription(s)\n", plan.Channel, len(plan.Subscriptions))
		for _, frame := range plan.Frames {
			fmt.Printf("      %s\n", frame)
		}
	}
	fmt.Println("\nplan is valid; nothing was written and no connection was made")
	return 0
}

// printSummary writes the per-channel accounting to stdout, so an operator sees the
// numbers without opening manifest.json.
func printSummary(rec *hyperliquid.Recorder, elapsed time.Duration) {
	m := rec.Manifest()

	fmt.Printf("\n%s\n", strings.Repeat("-", 78))
	fmt.Printf("%-15s %10s %10s %7s %9s %10s\n",
		"CHANNEL", "FRAMES", "SENTINELS", "ACKS", "DROPS", "BYTES")
	fmt.Printf("%s\n", strings.Repeat("-", 78))

	var missingAcks bool
	for _, r := range m.ChannelReports {
		fmt.Printf("%-15s %10d %10d %7d %9d %10d\n",
			r.Channel, r.Frames, r.Sentinels, r.Acks, r.Drops, r.Bytes)
		if r.Acks < int64(r.Subscriptions) {
			missingAcks = true
		}
	}
	fmt.Printf("%s\n", strings.Repeat("-", 78))
	fmt.Printf("elapsed %s, stop reason %q\n", elapsed.Round(time.Second), m.StopReason)

	if missingAcks {
		fmt.Println("WARNING: some subscriptions were never acknowledged; see the logs and channel_reports[].acks")
	}
	for _, r := range m.ChannelReports {
		if r.Drops > 0 || r.DroppedSentinels > 0 || r.WriteErrors > 0 || r.Error != "" {
			fmt.Printf("WARNING: %s is not clean: drops=%d dropped_sentinels=%d write_errors=%d err=%q\n",
				r.Channel, r.Drops, r.DroppedSentinels, r.WriteErrors, r.Error)
		}
	}
	if m.Error != "" {
		fmt.Printf("run error: %s\n", m.Error)
	}
	fmt.Printf("manifest: %s/%s\n", rec.Config().DataDir, hyperliquid.ManifestFile)

	if errors.Is(rec.Err(), context.DeadlineExceeded) {
		fmt.Println("note: ended because -duration elapsed")
	}
}
