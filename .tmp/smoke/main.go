// Command smoke is a dev-time integration check against live Gamma and Binance.
//
// It has two modes.
//
// Plan (the default): builds the real epoch scheduler from a shipped config and
// asks it to PLAN the current instant — the same discovery, event selection and
// epoch mapping the live run uses, minus starting sessions and subscribing to the
// WS. A config that would record the wrong market therefore fails here first.
//
// -klines: watches the real kline_5m chain end to end for one closed candle —
// config → SubscribeKlines → BinanceKlineEvent → EventCollector.HandleEvent →
// final-only persistence → event_counts.binance_kline — and then decodes the
// events.gz it wrote, so the CONTEXT.md §7 integrity cross-check is known to have
// data before a deploy rather than after one.
//
// Not part of the collector binary. Run from the repo root:
//
//	go run ./.tmp/smoke
//	go run ./.tmp/smoke -config collector_daily.yaml
//	go run ./.tmp/smoke -klines
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	"github.com/algoboy-kevin/go-collector/pkg/services/collector"
	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"gopkg.in/yaml.v3"
)

type config struct {
	Connector polymarket.Config         `yaml:"connector"`
	Recording collector.RecordingConfig `yaml:"recording"`
	Series    []collector.SeriesConfig  `yaml:"series"`
}

func main() {
	path := flag.String("config", "collector.yaml", "config to plan from")
	klines := flag.Bool("klines", false, "watch the live binance kline_5m chain to disk instead of planning")
	symbol := flag.String("symbol", "BTCUSDT", "binance symbol for -klines")
	wait := flag.Duration("wait", 6*time.Minute, "how long -klines waits for a closed candle")
	root := flag.String("dir", "", "scratch dir for -klines (default: a fresh temp dir, printed on exit)")
	flag.Parse()

	raw, err := os.ReadFile(*path)
	if err != nil {
		fatal("read %s: %v", *path, err)
	}
	var cfg config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		fatal("parse %s: %v", *path, err)
	}

	if *klines {
		checkKlines(cfg, *symbol, *wait, *root)
		return
	}
	plan(cfg, *path)
}

func plan(cfg config, path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn := polymarket.New(false, cfg.Connector, time.Now)

	// Plan never writes, so the recording directory is irrelevant here.
	ec := collector.NewCollector(collector.CollectorConfig{RecordingDir: os.TempDir()}, conn)
	sched, err := collector.NewEpochScheduler(ec, conn, collector.SchedulerConfig{
		Recording: cfg.Recording,
		Series:    cfg.Series,
	})
	if err != nil {
		fatal("scheduler: %v", err)
	}

	now := time.Now()
	anchor, _ := cfg.Recording.Anchor()
	fmt.Printf("config=%s  now=%s\n", path, now.UTC().Format(time.RFC3339))
	fmt.Printf("anchor=%s  days=%d  ladder_offset_days=%d\n\n",
		anchor, cfg.Recording.Days, cfg.Recording.OffsetDays())

	targets, err := sched.Plan(ctx, now)
	if err != nil {
		fatal("plan: %v", err)
	}

	bySeries := map[string]int{}
	for _, t := range targets {
		bySeries[t.Spec.Name]++
		claim := "settlement"
		if t.OffsetDays > 0 {
			claim = fmt.Sprintf("offset +%d", t.OffsetDays)
		}
		fmt.Printf("✓ %-10s %-12s %s\n", t.Spec.Name, claim, t.Event.Slug)
		fmt.Printf("    settle=%s (%s)  epoch=%s  title_date=%s\n",
			t.SettleAt.UTC().Format(time.RFC3339),
			t.SettleAt.In(libs.EpochLocation).Format("2006-01-02 15:04 MST"),
			t.EpochID, libs.SettleDateET(t.SettleAt))
		fmt.Printf("    trading window %s → %s   would record %d of %d markets\n",
			t.Event.StartDate.UTC().Format("2006-01-02 15:04Z"),
			t.Event.EndDate.UTC().Format("2006-01-02 15:04Z"),
			t.Markets, len(t.Event.Markets))
		fmt.Printf("    source=%s  anchor=%s\n", t.Spec.Source, t.Spec.Anchor)
	}

	fmt.Println()
	failures := 0
	for _, sel := range sched.Series() {
		want := 1
		if sel.Spec.Ladder {
			want = cfg.Recording.OffsetDays() + 1
		}
		switch got := bySeries[sel.Spec.Name]; {
		case got == 0:
			fmt.Printf("✗ %-10s nothing to record at this instant\n", sel.Spec.Name)
			failures++
		case got != want:
			fmt.Printf("✗ %-10s planned %d targets, want %d (offset events included)\n",
				sel.Spec.Name, got, want)
			failures++
		}
	}

	if failures > 0 {
		fatal("%d series failed planning", failures)
	}
	fmt.Println("all series planned OK")
}

// checkKlines watches the real Binance 5m kline stream through the collector's
// own recorder — the path CONTEXT.md §7 depends on for its integrity cross-check
// — and then decodes what landed on disk.
//
// It fails loudly on the two ways that chain can be silently broken: no closed
// candle arriving at all (a subscription or parse problem), and an in-progress
// candle being persisted (which would double-count against the trade stream).
func checkKlines(cfg config, symbol string, wait time.Duration, root string) {
	anchor, err := cfg.Recording.Anchor()
	if err != nil {
		fatal("anchor: %v", err)
	}
	if root == "" {
		dir, err := os.MkdirTemp("", "smoke-klines-*")
		if err != nil {
			fatal("temp dir: %v", err)
		}
		root = dir
	}

	ctx, cancel := context.WithTimeout(context.Background(), wait+30*time.Second)
	defer cancel()

	conn := polymarket.New(false, cfg.Connector, time.Now)
	ec := collector.NewCollector(collector.CollectorConfig{
		RecordingDir: filepath.Join(root, "market"),
		EpochAnchor:  anchor,
	}, conn)

	fr := collector.NewFeedRecorder(filepath.Join(root, "binance"), symbol, collector.FeedSourceBinance, "spot", anchor)
	ec.AddFeedRecorder(fr)
	fr.Start(ctx)

	final := make(chan *connector.BinanceKlineEvent, 8)
	var interim atomic.Int64
	bn := binance.New(conn.Connector)
	bn.SetDispatcher(func(ev any) {
		switch e := ev.(type) {
		case *connector.BinanceKlineEvent:
			if e.IsFinal {
				select {
				case final <- e:
				default:
				}
			} else {
				interim.Add(1)
			}
		}
		// The real path, so persistence and the final-only rule are exercised
		// rather than simulated.
		ec.HandleEvent(ev)
	})
	if err := bn.Start(ctx, 0); err != nil {
		fatal("binance start: %v", err)
	}
	defer bn.Stop()
	if err := bn.SubscribeKlines(ctx, binance.MarketSpot, []string{symbol}, "5m"); err != nil {
		fatal("subscribe klines: %v", err)
	}

	fmt.Printf("watching binance spot %s kline_5m for one closed candle (up to %s)…\n", symbol, wait)
	var closed *connector.BinanceKlineEvent
	select {
	case closed = <-final:
	case <-time.After(wait):
		fatal("no closed %s 5m candle within %s (%d in-progress updates seen)",
			symbol, wait, interim.Load())
	}

	fmt.Printf("\n✓ closed candle received\n")
	fmt.Printf("    open_time=%s  close_time=%s  is_final=%v\n",
		closed.OpenTime.UTC().Format(time.RFC3339), closed.CloseTime.UTC().Format(time.RFC3339), closed.IsFinal)
	fmt.Printf("    o=%s h=%s l=%s c=%s  trades=%d\n",
		closed.Open, closed.High, closed.Low, closed.Close, closed.TradeCount)

	// The recorder encodes on its own goroutine, so let it drain the queue.
	bucket := fr.BucketName()
	time.Sleep(500 * time.Millisecond)
	fr.Close()

	bucketDir := filepath.Join(root, "binance", "spot", bucket)
	rows, nonFinal, err := inspectEventsGz(filepath.Join(bucketDir, "events.gz"))
	if err != nil {
		fatal("decode %s: %v", filepath.Join(bucketDir, "events.gz"), err)
	}
	meta, err := readFeedMeta(filepath.Join(bucketDir, "metadata.json"))
	if err != nil {
		fatal("read metadata: %v", err)
	}

	fmt.Printf("\nbucket   %s\n", bucketDir)
	fmt.Printf("    events.gz rows: %v  (in-progress klines seen and dropped: %d)\n", rows, interim.Load())
	fmt.Printf("    metadata.json : event_count=%d event_counts=%v\n", meta.EventCount, meta.EventCounts)

	failures := 0
	if rows["binance_kline"] == 0 {
		fmt.Println("✗ no binance_kline row was persisted — the §7 cross-check would have no data")
		failures++
	}
	if nonFinal > 0 {
		fmt.Printf("✗ %d in-progress klines were persisted, want 0\n", nonFinal)
		failures++
	}
	if got := meta.EventCounts["binance_kline"]; got != int64(rows["binance_kline"]) {
		fmt.Printf("✗ metadata says %d klines, eventsgz holds %d\n", got, rows["binance_kline"])
		failures++
	}
	if failures > 0 {
		fatal("%d kline checks failed (scratch dir kept: %s)", failures, root)
	}
	fmt.Println("\n✓ kline chain OK: subscribed → dispatched → final-only persisted → counted")
	fmt.Printf("  scratch dir kept for inspection: %s\n", root)
}

// inspectEventsGz decodes a feed bucket's events.gz and returns the row count per
// event type, plus how many binance_kline rows were NOT final (which must be 0:
// only a closed candle is worth a row, since the in-progress updates are
// redundant with aggTrade).
func inspectEventsGz(path string) (rows map[string]int, nonFinal int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, 0, err
	}
	defer gz.Close()

	rows = make(map[string]int)
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20) // book snapshots are large
	for sc.Scan() {
		var rec struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return nil, 0, fmt.Errorf("row %d: %w", rows[rec.Type], err)
		}
		rows[rec.Type]++
		if rec.Type == collector.FeedEventBinanceKline {
			var kl struct {
				IsFinal bool `json:"is_final"`
			}
			if err := json.Unmarshal(rec.Data, &kl); err != nil {
				return nil, 0, err
			}
			if !kl.IsFinal {
				nonFinal++
			}
		}
	}
	return rows, nonFinal, sc.Err()
}

// readFeedMeta parses a bucket's metadata.json.
func readFeedMeta(path string) (collector.FeedMetadata, error) {
	var meta collector.FeedMetadata
	raw, err := os.ReadFile(path)
	if err != nil {
		return meta, err
	}
	return meta, json.Unmarshal(raw, &meta)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
