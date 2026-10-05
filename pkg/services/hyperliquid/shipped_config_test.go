package hyperliquid

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
)

// TestShippedConfigDecodes mirrors pkg/services/collector's TestShippedConfigsDecode:
// the config in the repo root is validated here, so a shipped file that cannot start
// the recorder is a test failure rather than a 3am surprise on the droplet.
func TestShippedConfigDecodes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "collector_hl.yaml"))
	if err != nil {
		t.Fatalf("read shipped config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "collector_hl.yaml")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("stage config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("shipped config does not load: %v", err)
	}

	// The shipped defaults are a policy decision, so they are pinned here rather than
	// left to drift with a careless edit.
	if got, want := len(cfg.Coins), 5; got != want {
		t.Errorf("coins = %d, want %d", got, want)
	}
	if got, want := cfg.Coins[0], "BTC"; got != want {
		t.Errorf("first coin = %q, want %q (case is significant at the venue)", got, want)
	}
	for _, ch := range cfg.Channels {
		if ch == string(hl.ChannelCandle) {
			t.Error("the shipped config must not record candles")
		}
	}
	if !cfg.L2Book.Fast {
		t.Error("l2Book.fast should be on: it halves the largest channel")
	}
	if cfg.L2Book.NSigFigs != 0 || cfg.L2Book.Mantissa != 0 {
		t.Error("nSigFigs/mantissa must stay unset: they thin prices, not just levels")
	}
	if got, err := cfg.CompressionLevel(); err != nil || got != 6 {
		t.Errorf("compression = %d (%v), want gzip level 6", got, err)
	}
	if got := cfg.MinFree(); got != DefaultMinFreeBytes {
		t.Errorf("MinFree = %d, want the %d default", got, DefaultMinFreeBytes)
	}
	if got := cfg.FlushEvery(); got != time.Second {
		t.Errorf("flush interval = %s, want 1s", got)
	}
	if got := cfg.QueueSize(); got != DefaultQueueSize {
		t.Errorf("queue size = %d, want %d", got, DefaultQueueSize)
	}

	// And it must produce working subscriptions.
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plans) != len(cfg.Channels) {
		t.Errorf("got %d plans for %d channels", len(plans), len(cfg.Channels))
	}
	for _, p := range plans {
		if len(p.Subscriptions) != len(cfg.Coins) {
			t.Errorf("%s: %d subscriptions for %d coins", p.Channel, len(p.Subscriptions), len(cfg.Coins))
		}
	}
}
