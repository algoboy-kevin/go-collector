// Command collector records Polymarket market events to disk for DES
// (Discrete Event Simulation) backtest replay.
//
// It connects to the Polymarket WebSocket via the go-exchange-connector,
// subscribes to configured rolling market series, buffers all raw events
// (price changes, book snapshots, trades, tick changes, resolutions),
// and writes them as gzip-compressed JSONL files on market resolution.
//
// Usage:
//
//	# From project root (data_dir=data → data/market, data/rtds, data/binance):
//	go run ./cmd/collector -config collector.yaml
//	# Custom base data directory:
//	go run ./cmd/collector -config collector.yaml -recording-dir ./data
//
//	# Or build and run:
//	go build -o bin/collector ./cmd/collector
//	./bin/collector -config collector.yaml
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

	"github.com/algoboy-kevin/go-collector/pkg/services/collector"
	"github.com/algoboy-kevin/go-collector/pkg/services/runtime"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	"gopkg.in/yaml.v3"
)

// Config is the YAML configuration for the collector binary.
type Config struct {
	// DataDir is the base data directory; market/, rtds/ and binance/ live
	// here as subdirectories. Default: "data".
	DataDir    string                   `yaml:"data_dir"`
	RTDSDir    string                   `yaml:"rtds_dir"`    // optional override for rtds root
	BinanceDir string                   `yaml:"binance_dir"` // optional override for binance root
	Connector  polymarket.Config        `yaml:"connector"`
	Series     []collector.SeriesConfig `yaml:"series"`
	RTDS       collector.RTDSConfig     `yaml:"rtds"`
	Binance    collector.BinanceConfig  `yaml:"binance"`
}

func main() {
	configPath := flag.String("config", "collector.yaml", "path to config file")
	recordingDir := flag.String("recording-dir", "", "base data directory where market/, rtds/ and binance/ live (overrides config)")
	flag.Parse()

	// Load config.
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// CLI flag overrides config value (base data directory).
	if *recordingDir != "" {
		cfg.DataDir = *recordingDir
	}

	// Resolve the base data directory — market/, rtds/ and binance/ live here.
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = collector.DefaultDataDir
	}

	slog.Info("collector starting", "config", *configPath)

	// ── 1. Create RuntimeManager in COLLECTOR mode ──────────
	rt, err := runtime.New(runtime.ModeCollector)
	if err != nil {
		slog.Error("failed to create runtime", "err", err)
		os.Exit(1)
	}
	defer rt.Stop()

	// ── 2. Create connector (starts market WS) ──────────────
	conn := polymarket.New(false, cfg.Connector, rt.Now)
	if err := conn.Start(rt.RootContext()); err != nil {
		slog.Error("failed to start connector", "err", err)
		os.Exit(1)
	}

	defer conn.Stop()

	// ── 4. Create EventCollector ────────────────────────────
	// Market recordings live under {dataDir}/market.
	ec := collector.NewCollector(collector.CollectorConfig{
		RecordingDir: filepath.Join(dataDir, "market"),
	}, rt, conn)

	conn.SetDispatcher(ec.HandleEvent)

	// ── 4b. Track WebSocket connection status changes ──────
	conn.SetOnMarketStatusChange(func(status websocket.ConnectionStatus) {
		switch status {
		case websocket.StatusConnected:
			ec.RecordConnectionEvent("connected")
		case websocket.StatusDisconnected:
			ec.RecordConnectionEvent("disconnected")
		}
	})
	conn.SetOnRTDSStatusChange(func(status websocket.ConnectionStatus) {
		switch status {
		case websocket.StatusConnected:
			ec.RecordFeedConnection(collector.FeedSourceRTDS, "", "connected")
		case websocket.StatusDisconnected:
			ec.RecordFeedConnection(collector.FeedSourceRTDS, "", "disconnected")
		}
	})

	// ── 4c. RTDS recording — Polymarket reference prices (~1/sec) ──
	// Continuous per symbol; cut into hourly buckets (see FeedRecorder).
	// Symbols with "/" are Chainlink feeds (e.g. "btc/usd"), all others are
	// crypto_prices symbols (e.g. "btcusdt").
	if cfg.RTDS.CryptoPrices.Enabled {
		for _, sym := range cfg.RTDS.CryptoPrices.Symbols {
			fr := collector.NewFeedRecorder(feedDir(cfg.RTDSDir, dataDir, "rtds"), sym, collector.FeedSourceRTDS, "")
			ec.AddFeedRecorder(fr)
			fr.Start(rt.RootContext())
			if strings.Contains(sym, "/") {
				conn.SubscribeChainlinkPrices(rt.RootContext(), []string{sym})
			} else {
				conn.SubscribeCryptoPrices(rt.RootContext(), []string{sym})
			}
			slog.Info("collector: rtds recording enabled", "symbol", sym)
		}
	}

	// ── 4c'. Chainlink TWAP — the exact price Polymarket resolves on ──
	// Ground truth for validating your Python TWAP computation.
	if cfg.RTDS.ChainlinkTWAP != nil && cfg.RTDS.ChainlinkTWAP.Enabled {
		window := cfg.RTDS.ChainlinkTWAP.Window
		if window != 30 && window != 60 {
			window = 60
		}
		for _, feed := range cfg.RTDS.ChainlinkTWAP.Feeds {
			fr := collector.NewFeedRecorder(feedDir(cfg.RTDSDir, dataDir, "rtds"), feed, collector.FeedSourceChainlinkTWAP, "")
			ec.AddFeedRecorder(fr)
			fr.Start(rt.RootContext())
			conn.SubscribeChainlinkTWAP(rt.RootContext(), window, []string{feed})
			slog.Info("collector: chainlink twap recording enabled", "feed", feed, "window_s", window)
		}
	}

	// ── 4d. Binance recording — aggTrade + partial depth (top-20) ──
	// HFT data so the 60s TWAP / forecast features can be built in Python.
	bn := binance.New(conn.Connector)
	bn.SetDispatcher(ec.HandleEvent)
	bn.SetOnStatusChange(func(mkt binance.MarketType, status websocket.ConnectionStatus) {
		switch status {
		case websocket.StatusConnected:
			ec.RecordFeedConnection(collector.FeedSourceBinance, string(mkt), "connected")
		case websocket.StatusDisconnected:
			ec.RecordFeedConnection(collector.FeedSourceBinance, string(mkt), "disconnected")
		}
	})
	binanceStarted := false
	binanceMarkets := []struct {
		name string
		cfg  collector.BinanceMarketConfig
		mkt  binance.MarketType
	}{
		{"perp", cfg.Binance.Perp, binance.MarketPerp},
		{"spot", cfg.Binance.Spot, binance.MarketSpot},
	}
	for _, m := range binanceMarkets {
		if len(m.cfg.Symbols) == 0 {
			continue
		}
		if !binanceStarted {
			if err := bn.Start(rt.RootContext(), 0); err != nil {
				slog.Error("collector: failed to start binance", "err", err)
				break
			}
			defer bn.Stop()
			binanceStarted = true
		}
		for _, sym := range m.cfg.Symbols {
			fr := collector.NewFeedRecorder(feedDir(cfg.BinanceDir, dataDir, "binance"), sym, collector.FeedSourceBinance, m.name)
			ec.AddFeedRecorder(fr)
			fr.Start(rt.RootContext())
			subscribeBinanceStreams(bn, rt.RootContext(), m.mkt, sym, m.cfg.Streams)
			slog.Info("collector: binance recording enabled", "symbol", sym, "market", m.name)
		}
	}

	// ── 5. Start recording sessions for each series ─────────
	for _, sc := range cfg.Series {
		if err := ec.StartSeries(conn, sc); err != nil {
			slog.Error("failed to start series", "series", sc.Series, "err", err)
		}
	}

	// ── 5b. Feed liveness monitor (RTDS / binance) ──────────
	// Warns when a feed stops receiving events (silent WS death) and logs a
	// periodic health summary so binance + RTDS are visible like the market.
	ec.StartHealthMonitor(rt.RootContext(), 15*time.Second)

	// Arm the cron routines so they fire at interval boundaries.
	rt.Start()

	slog.Info("collector running", "active_sessions", ec.ActiveSessions())

	// ── 6. Wait for shutdown signal ─────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Also exit if all sessions complete.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if ec.ActiveSessions() == 0 {
					slog.Info("collector: all sessions finalized, shutting down")
					sigCh <- syscall.SIGTERM
					return
				}
			case <-rt.RootContext().Done():
				return
			}
		}
	}()

	sig := <-sigCh
	slog.Info("collector shutting down", "signal", sig)

	// ── 7. Graceful shutdown ────────────────────────────────
	// Stop connector first to tear down WS, then cancel runtime
	// (prevents reconnect racing between context cancel and WS stop).
	conn.Stop()  // stops WS immediately (no new events)
	ec.StopAll() // finalize remaining sessions + close feed buckets (writes metadata)
	rt.Stop()    // cancels root context last
	slog.Info("collector stopped")
}

// loadConfig loads configuration from a YAML file.
// Falls back to sensible defaults if the file doesn't exist.
func loadConfig(path string) (*Config, error) {
	cfg := &Config{
		DataDir: collector.DefaultDataDir,
		Series: []collector.SeriesConfig{
			{Series: "btc_5m", Count: 10},
		},
		RTDS: collector.RTDSConfig{
			CryptoPrices: collector.CryptoPricesConfig{
				Enabled: true,
				Symbols: []string{"btcusdt"},
			},
			ChainlinkTWAP: &collector.ChainlinkTWAPConfig{
				Enabled: true,
				Feeds:   []string{"btc/usd"},
				Window:  60,
			},
		},
		Binance: collector.BinanceConfig{
			Perp: collector.BinanceMarketConfig{
				Symbols: []string{"BTCUSDT"},
				Streams: []string{"aggTrade", "depth"},
			},
		},
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("config file not found, using defaults", "path", path)
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

// feedDir resolves the root dir for a feed: an explicit override when set,
// otherwise {base}/{feed} (e.g. base=data → data/rtds, data/binance).
func feedDir(explicit, base, feed string) string {
	if explicit != "" {
		return explicit
	}
	return filepath.Join(base, feed)
}

// subscribeBinanceStreams subscribes the configured Binance streams for a
// symbol. An empty stream list subscribes all (aggTrade + partial depth).
func subscribeBinanceStreams(bn *binance.WSBinance, ctx context.Context, mkt binance.MarketType, symbol string, streams []string) {
	enabled := func(name string) bool {
		for _, s := range streams {
			if strings.EqualFold(strings.TrimSpace(s), name) {
				return true
			}
		}
		return len(streams) == 0
	}
	if enabled("aggTrade") || enabled("trades") {
		bn.SubscribeTrades(ctx, mkt, []string{symbol})
	}
	// Partial book depth (@depth20@100ms): a passive top-20 snapshot pushed by
	// Binance — no local book / REST seeding. Replaces the old diff-depth full
	// book which dominated memory/volume. (Binance only supports 5/10/20 levels.)
	if enabled("depth") || enabled("book") {
		bn.SubscribePartialDepth(ctx, mkt, []string{symbol}, 20, "100ms")
	}
}
