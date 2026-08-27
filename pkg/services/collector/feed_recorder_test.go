package collector

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func bookTicker(seq int64, ts time.Time) *connector.BinanceBookTickerEvent {
	return &connector.BinanceBookTickerEvent{
		SeqID:        seq,
		ReceivedAt:   ts,
		Timestamp:    ts,
		Symbol:       "BTCUSDT",
		Market:       "perp",
		BestBidPrice: "100000.0",
		BestBidQty:   "1.0",
		BestAskPrice: "100001.0",
		BestAskQty:   "1.0",
	}
}

// TestFeedRecorderCloseWritesMetadata verifies a bucket's events.gz AND
// metadata.json are written when the recorder is closed (graceful stop).
func TestFeedRecorderCloseWritesMetadata(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "perp")

	ts := time.Unix(1787394600, 0) // 18:30 UTC
	if !fr.Record(bookTicker(1, ts)) {
		t.Fatal("record failed")
	}
	fr.Close()

	dir := filepath.Join(root, fmt.Sprintf("%s_%d", bucketSymbol("BTCUSDT"), ts.Truncate(time.Hour).Unix()))
	if _, err := os.Stat(filepath.Join(dir, "events.gz")); err != nil {
		t.Fatalf("events.gz missing: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatalf("metadata.json missing after Close: %v", err)
	}
	var meta FeedMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("bad metadata: %v", err)
	}
	if meta.EventCount != 1 {
		t.Fatalf("event_count = %d, want 1", meta.EventCount)
	}
	if meta.Feed != FeedSourceBinance || meta.Symbol != "BTCUSDT" || meta.Market != "perp" {
		t.Fatalf("feed meta = %+v", meta)
	}
	if meta.Connections == nil {
		t.Fatalf("connections missing from metadata")
	}
	if meta.DataQuality == nil {
		t.Fatalf("data_quality missing from metadata")
	}
}

// TestFeedRecorderRotationWritesMetadata verifies the prior hour's bucket gets
// metadata.json when a new-hour event rotates the bucket.
func TestFeedRecorderRotationWritesMetadata(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "btcusdt", FeedSourceBinance, "perp")
	defer fr.Close()

	h1 := time.Unix(1787394600, 0) // 18:30
	h2 := time.Unix(1787398200, 0) // 19:30
	if !fr.Record(bookTicker(1, h1)) {
		t.Fatal("record h1 failed")
	}
	if !fr.Record(bookTicker(2, h2)) {
		t.Fatal("record h2 failed")
	}
	fr.Close()

	dir1 := filepath.Join(root, fmt.Sprintf("btcusdt_%d", h1.Truncate(time.Hour).Unix()))
	if _, err := os.Stat(filepath.Join(dir1, "metadata.json")); err != nil {
		t.Fatalf("rotated bucket metadata.json missing: %v", err)
	}
	dir2 := filepath.Join(root, fmt.Sprintf("btcusdt_%d", h2.Truncate(time.Hour).Unix()))
	if _, err := os.Stat(filepath.Join(dir2, "metadata.json")); err != nil {
		t.Fatalf("closed bucket metadata.json missing: %v", err)
	}
}

// TestFeedRecorderTickWritesMetadataForOpenBucket verifies the periodic tick
// writes metadata.json for the OPEN bucket (so metadata survives a hard kill
// before a clean close).
func TestFeedRecorderTickWritesMetadataForOpenBucket(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "btcusdt", FeedSourceBinance, "perp")
	defer fr.Close()

	ts := time.Unix(1787394600, 0) // 18:30 UTC
	if !fr.Record(bookTicker(1, ts)) {
		t.Fatal("record failed")
	}
	// Same hour, later wall-clock time → tick refreshes open-bucket metadata.
	fr.tick(time.Unix(1787394660, 0))

	dir := filepath.Join(root, fmt.Sprintf("btcusdt_%d", ts.Truncate(time.Hour).Unix()))
	if _, err := os.Stat(filepath.Join(dir, "metadata.json")); err != nil {
		t.Fatalf("tick did not write metadata.json for open bucket: %v", err)
	}
	// Bucket must still be open (events.gz present) after the tick.
	if _, err := os.Stat(filepath.Join(dir, "events.gz")); err != nil {
		t.Fatalf("events.gz missing after tick: %v", err)
	}
}
