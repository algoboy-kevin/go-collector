package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/services/collector"
)

// TestLoadConfigRejectsInvalidCoalescing pins that an unsupported coalescing
// window is a loud startup failure, not a silently-ignored knob.
func TestLoadConfigRejectsInvalidCoalescing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "collector.yaml")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}

	write("collector:\n  price_change_coalesce_ms: -1\n")
	if _, err := loadConfig(path); err == nil {
		t.Error("a negative price_change_coalesce_ms must fail startup")
	}

	write("collector:\n  price_change_coalesce_ms: 9223372036855\n")
	if _, err := loadConfig(path); err == nil {
		t.Error("a price_change_coalesce_ms that overflows time.Duration must fail startup")
	}

	write("collector:\n  price_change_coalesce_ms: 100\n")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig(100): %v", err)
	}
	if cfg.Collector.PriceChangeCoalesceMs != 100 {
		t.Errorf("parsed coalescing = %d, want 100", cfg.Collector.PriceChangeCoalesceMs)
	}
	if got := cfg.Collector.Apply(collector.CollectorConfig{}).PriceChangeCoalesce; got != 100*time.Millisecond {
		t.Errorf("applied coalescing = %v, want 100ms", got)
	}

	// Absent is the default: raw behaviour, no coalescing.
	write("collector:\n  market_silence_timeout_s: 60\n")
	cfg, err = loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig(default): %v", err)
	}
	if got := cfg.Collector.Apply(collector.CollectorConfig{}).PriceChangeCoalesce; got != 0 {
		t.Errorf("absent coalescing = %v, want 0 (raw)", got)
	}
}
