package collector

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// testAnchor is the epoch rule used by the feed-bucket tests.
const testAnchor = libs.AnchorNoonET

// epochTS is a timestamp and the epoch id it must land in, under testAnchor.
// 2026-08-22T10:30Z is 06:30 ET — before the noon anchor — so it belongs to
// the epoch that ENDS at noon on the 22nd.
func epochTS(t *testing.T) (time.Time, string) {
	t.Helper()
	ts := time.Unix(1787394600, 0)
	if got := ts.UTC().Format(time.RFC3339); got != "2026-08-22T10:30:00Z" {
		t.Fatalf("test timestamp drifted: %s", got)
	}
	return ts, "2026-08-22"
}

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

// TestFeedRecorderRotatesParts pins the split of a bucket into parts. One epoch
// of aggTrade + top-20 depth runs to hundreds of MB compressed, so a bucket is
// cut at a size limit and every part is listed in metadata.json — with the first
// keeping the documented name, so a bucket that never rotates is unchanged.
func TestFeedRecorderRotatesParts(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "spot", testAnchor)
	// A small limit with a realistic payload: enough parts to exercise the
	// rotation several times, few enough that a sorted glob is still the order.
	fr.SetPartLimit(4 << 10)
	fr.Start(context.Background())

	ts, epochID := epochTS(t)
	// More than two flushes (feedFlushEvery), so the file grows mid-stream and
	// the rotation has to happen more than once.
	const events = 1500
	for i := 0; i < events; i++ {
		ev := bookTicker(int64(i+1), ts.Add(time.Duration(i)*time.Millisecond))
		// Vary the payload so the rows do not compress to nothing, which is what a
		// real book ticker stream looks like.
		ev.BestBidPrice = fmt.Sprintf("%d.%d", 100000+i, i%1000)
		ev.BestAskPrice = fmt.Sprintf("%d.%d", 100001+i, i%997)
		if !fr.Record(ev) {
			t.Fatalf("record %d failed", i)
		}
	}
	waitForEvents(t, fr, events)
	fr.Close()

	dir := filepath.Join(root, "spot", fmt.Sprintf("%s_%s", bucketSymbol("BTCUSDT"), epochID))
	meta := readBucketMeta(t, dir)

	if meta.EventCount != events {
		t.Errorf("metadata event_count = %d, want %d", meta.EventCount, events)
	}
	if len(meta.Parts) < 2 {
		t.Fatalf("bucket was not split: parts = %v", meta.Parts)
	}
	if meta.Parts[0] != "events.gz" {
		t.Errorf("first part = %q, want the documented name events.gz", meta.Parts[0])
	}

	// Every listed part exists, decodes, and together they hold exactly the
	// recorded rows — a rotation that dropped or duplicated a part would show up
	// here rather than in a post-processing gap.
	total := 0
	for _, name := range meta.Parts {
		total += countBucketRows(t, filepath.Join(dir, name))
	}
	if total != events {
		t.Errorf("parts hold %d rows, want %d", total, events)
	}

	onDisk, err := filepath.Glob(filepath.Join(dir, "events*.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk) != len(meta.Parts) {
		t.Errorf("%d parts on disk but %d listed in metadata: %v", len(onDisk), len(meta.Parts), meta.Parts)
	}

	// The read order is events.gz first, then the numbered parts ascending. A plain
	// glob would put events.001.gz ahead of events.gz, so this asserts the
	// documented order (which metadata.parts records) rather than sort order.
	for i, name := range meta.Parts[1:] {
		if want := fmt.Sprintf("events.%03d.gz", i+1); name != want {
			t.Errorf("part %d = %q, want %q (numeric read order)", i+1, name, want)
		}
	}
}

// TestFeedRecorderPartRotationCanBeDisabled keeps the pre-rotation layout
// reachable: one events.gz per bucket.
func TestFeedRecorderPartRotationCanBeDisabled(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "spot", testAnchor)
	fr.SetPartLimit(0)
	fr.Start(context.Background())

	ts, epochID := epochTS(t)
	const events = 600
	for i := 0; i < events; i++ {
		if !fr.Record(bookTicker(int64(i+1), ts.Add(time.Duration(i)*time.Millisecond))) {
			t.Fatalf("record %d failed", i)
		}
	}
	waitForEvents(t, fr, events)
	fr.Close()

	dir := filepath.Join(root, "spot", fmt.Sprintf("%s_%s", bucketSymbol("BTCUSDT"), epochID))
	meta := readBucketMeta(t, dir)
	if len(meta.Parts) != 1 || meta.Parts[0] != "events.gz" {
		t.Fatalf("parts = %v, want just events.gz", meta.Parts)
	}
	if got := countBucketRows(t, filepath.Join(dir, "events.gz")); got != events {
		t.Errorf("events.gz holds %d rows, want %d", got, events)
	}
}

// waitForEvents lets the recorder's writer goroutine drain the queue.
func waitForEvents(t *testing.T, fr *FeedRecorder, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for fr.EventCount() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d events (have %d)", want, fr.EventCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readBucketMeta(t *testing.T, dir string) FeedMetadata {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatalf("metadata.json: %v", err)
	}
	var meta FeedMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("parse metadata.json: %v", err)
	}
	return meta
}

// countBucketRows decodes one part and returns how many records it holds.
func countBucketRows(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open part %s: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip %s: %v", path, err)
	}
	defer gz.Close()

	rows := 0
	sc := bufio.NewScanner(gz)
	for sc.Scan() {
		rows++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return rows
}

// TestFeedRecorderCloseWritesMetadata verifies a bucket's events.gz AND
// metadata.json are written when the recorder is closed (graceful stop).
func TestFeedRecorderCloseWritesMetadata(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "perp", testAnchor)

	ts, epochID := epochTS(t)
	if !fr.Record(bookTicker(1, ts)) {
		t.Fatal("record failed")
	}
	fr.Close()

	// Bucket path is {root}/{market}/{symbol}_{epochID} — the market directory
	// is what keeps spot and perp apart.
	dir := filepath.Join(root, "perp", fmt.Sprintf("%s_%s", bucketSymbol("BTCUSDT"), epochID))
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
	if meta.EpochID != epochID {
		t.Errorf("meta.epoch_id = %q, want %q", meta.EpochID, epochID)
	}
	if meta.Anchor != string(testAnchor) {
		t.Errorf("meta.anchor = %q, want %q", meta.Anchor, testAnchor)
	}
	// The bucket window is the epoch window, not an hour.
	if got := meta.EndTime - meta.StartTime; got != int64(24*time.Hour/time.Millisecond) {
		t.Errorf("bucket window = %dms, want 24h", got)
	}
	if meta.Connections == nil {
		t.Fatalf("connections missing from metadata")
	}
	if meta.DataQuality == nil {
		t.Fatalf("data_quality missing from metadata")
	}
}

// TestFeedRecorderMarketPathSeparatesSpotAndPerp is the regression test for the
// collision that made spot and perp BTCUSDT write the same events.gz: lacking a
// market directory, the second os.Create truncated the first.
func TestFeedRecorderMarketPathSeparatesSpotAndPerp(t *testing.T) {
	root := t.TempDir()
	ts, epochID := epochTS(t)

	spot := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "spot", testAnchor)
	perp := NewFeedRecorder(root, "BTCUSDT", FeedSourceBinance, "perp", testAnchor)

	if !spot.Record(bookTicker(1, ts)) {
		t.Fatal("spot record failed")
	}
	if !perp.Record(bookTicker(2, ts)) {
		t.Fatal("perp record failed")
	}
	spot.Close()
	perp.Close()

	spotDir := filepath.Join(root, "spot", "btcusdt_"+epochID)
	perpDir := filepath.Join(root, "perp", "btcusdt_"+epochID)
	if spotDir == perpDir {
		t.Fatal("spot and perp resolved to the same directory")
	}

	for _, dir := range []string{spotDir, perpDir} {
		data, err := os.ReadFile(filepath.Join(dir, "events.gz"))
		if err != nil {
			t.Fatalf("%s: events.gz missing: %v", dir, err)
		}
		if len(data) == 0 {
			t.Errorf("%s: events.gz is empty — it was truncated by the other recorder", dir)
		}
	}
}

// TestFeedRecorderRotationWritesMetadata verifies the prior epoch's bucket gets
// metadata.json when an event from the next epoch rotates the bucket.
func TestFeedRecorderRotationWritesMetadata(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "btcusdt", FeedSourceBinance, "perp", testAnchor)
	defer fr.Close()

	e1, epoch1 := epochTS(t)
	e2 := e1.Add(24 * time.Hour) // same wall-clock time, next epoch
	epoch2 := "2026-08-23"
	if got := libs.EpochIDAt(e2, testAnchor); got != epoch2 {
		t.Fatalf("second timestamp lands in epoch %s, want %s", got, epoch2)
	}

	if !fr.Record(bookTicker(1, e1)) {
		t.Fatal("record e1 failed")
	}
	if !fr.Record(bookTicker(2, e2)) {
		t.Fatal("record e2 failed")
	}
	fr.Close()

	dir1 := filepath.Join(root, "perp", "btcusdt_"+epoch1)
	if _, err := os.Stat(filepath.Join(dir1, "metadata.json")); err != nil {
		t.Fatalf("rotated epoch metadata.json missing: %v", err)
	}
	dir2 := filepath.Join(root, "perp", "btcusdt_"+epoch2)
	if _, err := os.Stat(filepath.Join(dir2, "metadata.json")); err != nil {
		t.Fatalf("closed epoch metadata.json missing: %v", err)
	}
}

// TestFeedRecorderTickWritesMetadataForOpenBucket verifies the periodic tick
// writes metadata.json for the OPEN bucket (so metadata survives a hard kill
// before a clean close).
func TestFeedRecorderTickWritesMetadataForOpenBucket(t *testing.T) {
	root := t.TempDir()
	fr := NewFeedRecorder(root, "btcusdt", FeedSourceBinance, "perp", testAnchor)
	defer fr.Close()

	ts, epochID := epochTS(t)
	if !fr.Record(bookTicker(1, ts)) {
		t.Fatal("record failed")
	}
	// Same epoch, later wall-clock time → tick refreshes open-bucket metadata.
	fr.tick(ts.Add(60 * time.Second))

	dir := filepath.Join(root, "perp", "btcusdt_"+epochID)
	if _, err := os.Stat(filepath.Join(dir, "metadata.json")); err != nil {
		t.Fatalf("tick did not write metadata.json for open bucket: %v", err)
	}
	// Bucket must still be open (events.gz present) after the tick.
	if _, err := os.Stat(filepath.Join(dir, "events.gz")); err != nil {
		t.Fatalf("events.gz missing after tick: %v", err)
	}
}
