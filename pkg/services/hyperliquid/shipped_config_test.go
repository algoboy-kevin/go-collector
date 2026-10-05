package hyperliquid

import (
	"os"
	"path/filepath"
	"slices"
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
	// Asset names must survive decoding exactly. The venue matches them literally, so a
	// lowercased coin or a stripped dex prefix is a different — or a non-existent —
	// market, and the failure mode is a closed connection rather than an error.
	for _, want := range []string{"SOL", "HYPE", "DOGE", "xyz:JP225", "vntl:OPENAI"} {
		if !slices.Contains(cfg.Coins, want) {
			t.Errorf("shipped coins %v are missing %q", cfg.Coins, want)
		}
	}
	// The shipped config deliberately exercises HIP-3, so its dex grouping must produce
	// three universes to dump: the default dex, plus vntl and xyz.
	groups := GroupCoinsByDex(cfg.Coins)
	if len(groups) != 3 {
		t.Errorf("GroupCoinsByDex = %+v, want 3 dexes (default, vntl, xyz)", groups)
	}
	if len(cfg.Channels) < 2 {
		t.Errorf("channels = %v, want more than one", cfg.Channels)
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
