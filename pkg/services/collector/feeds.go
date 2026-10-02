package collector

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Feed sources.
const (
	FeedSourceRTDS          = "rtds"           // Polymarket RTDS reference prices (crypto_prices / chainlink)
	FeedSourceChainlinkTWAP = "chainlink_twap" // Polymarket RTDS Chainlink-computed TWAP (the resolution source)
	FeedSourceBinance       = "binance"        // Binance market streams
)

// Feed event type discriminators (stored in RecordedEvent.Type).
const (
	FeedEventCryptoPrice     = "crypto_price"        // connector.CryptoPriceEvent (RTDS binance/chainlink)
	FeedEventChainlinkTWAP   = "chainlink_twap"      // connector.CryptoPriceEvent (Source=chainlink_twap)
	FeedEventBinanceBookTkr  = "binance_book_ticker" // connector.BinanceBookTickerEvent
	FeedEventBinanceAggTrade = "binance_agg_trade"   // connector.BinanceAggTradeEvent
	FeedEventBinanceDepth    = "binance_depth"       // connector.BinanceDepthEvent
	FeedEventBinanceKline    = "binance_kline"       // connector.BinanceKlineEvent (final candles only)
)

// feedFlushEvery is how many records to buffer before flushing the gzip
// stream, so a crash mid-bucket still leaves recoverable data.
const feedFlushEvery = 512

// DefaultFeedPartBytes is how large one bucket part may grow before the recorder
// starts another one.
//
// A single epoch bucket holds a day of aggTrade + top-20 depth, which at full
// load runs to hundreds of MB compressed — an awkward thing to scp, to open, or
// to lose to one write error. Parts are therefore cut at this size and listed in
// the bucket's metadata.json (`parts`); the first keeps the documented name
// events.gz and the rest are events.001.gz, events.002.gz, …, so `events*.gz`
// sorted is the whole stream in order and a reader that opens only events.gz
// still works on any bucket that never rotated.
const DefaultFeedPartBytes = 256 << 20 // 256 MiB compressed

// feedDropLogInterval throttles "queue full, dropping events" warnings.
const feedDropLogInterval = 5 * time.Second

// feedItem carries one feed event from the dispatcher goroutine (Record) to
// the recorder's writer goroutine, where it is marshaled + gzip-encoded.
type feedItem struct {
	ev         any
	typ        string
	ts         time.Time
	receivedAt time.Time
	seqID      int64
}

// FeedRecorder streams a continuous reference-price feed (RTDS or Binance) to
// disk in hourly buckets, decoupled from prediction-market rotation:
//
//	{root}/[{market}/]{prefix}{symbol}_{epochID}/
//	    events.gz       ← gzip JSONL of RecordedEvent (mixed event types)
//	    metadata.json   ← FeedMetadata (bucket window, counts, feed info)
//
// The market directory is what keeps binance spot and perp apart: both can
// record the same symbol (BTCUSDT), and without it they would open the same
// events.gz and truncate each other.
//
// The chainlink_twap source gets a "chainlink_twap_" prefix because it shares
// the RTDS root with FeedSourceRTDS and would otherwise collide with the same
// symbol recorded as a reference price (e.g. btc/usd TWAP vs btc/usd).
//
// Buckets are cut at **epoch** boundaries (libs.EpochAnchor) so a feed file
// lines up with the market day it belongs to and with data/market/<epoch>/
// (see CONTEXT.md §5). Like market sessions, a feed recorder streams events to
// the current bucket as they arrive, so memory stays flat regardless of volume.
type FeedRecorder struct {
	root   string           // e.g. "data/rtds" or "data/binance"
	symbol string           // e.g. "btcusdt" / "BTCUSDT"
	source string           // FeedSourceRTDS or FeedSourceBinance
	market string           // binance market ("spot"/"perp"), empty for rtds
	anchor libs.EpochAnchor // daily epoch boundary rule

	mu          sync.Mutex
	closed      bool
	bucketName  string    // folder name, e.g. "btcusdt_2026-10-02"
	bucketEpoch string    // epoch id of the open bucket (ET date, YYYY-MM-DD)
	bucketStart time.Time // epoch start
	bucketEnd   time.Time // epoch end == settle instant of the epoch
	f           *os.File
	gw          *gzip.Writer
	enc         *json.Encoder

	// Part rotation: the events of one bucket may span several files (see
	// DefaultFeedPartBytes). partNames is what metadata.json lists.
	partLimit int64
	partNames []string

	// recCh buffers events between the dispatcher goroutine (Record) and the
	// writer goroutine, so heavy feed encoding (esp. binance depth) never runs
	// on — or blocks — the WS read/dispatch threads of other clients.
	recCh       chan feedItem
	stopCh      chan struct{} // signals the writer goroutine to stop
	wg          sync.WaitGroup
	recClosed   atomic.Bool  // fast-path gate: true once Close() begins
	drops       atomic.Int64 // events dropped when recCh is full
	lastDropLog atomic.Int64 // unix nanos — throttles drop warnings

	// Per-bucket statistics (reset on bucket open).
	count        int64
	typeCounts   map[string]int64
	firstTs      int64
	lastTs       int64
	prevTs       int64
	latencySum   float64
	latencyCount int64
	maxLatencyMs float64
	gapCount     int64
	maxGapMs     int64
	totalGapMs   int64
	gapWindows   [][2]int64 // broker-ts gaps > 2s within this bucket
	connEvents   []ConnectionEvent
}

// NewFeedRecorder creates a feed recorder rooted at root (e.g. "data/rtds").
// symbol is the asset symbol, source is FeedSourceRTDS or FeedSourceBinance,
// market is the binance market ("spot"/"perp") or empty for rtds, and anchor
// is the daily epoch rule the buckets are cut on.
func NewFeedRecorder(root, symbol, source, market string, anchor libs.EpochAnchor) *FeedRecorder {
	if !anchor.Valid() {
		panic(fmt.Sprintf("collector: NewFeedRecorder needs a daily epoch anchor, got %q", anchor))
	}
	return &FeedRecorder{
		root:       root,
		symbol:     symbol,
		anchor:     anchor,
		source:     source,
		market:     market,
		partLimit:  DefaultFeedPartBytes,
		typeCounts: make(map[string]int64),
		recCh:      make(chan feedItem, 8192),
		stopCh:     make(chan struct{}),
	}
}

// SetPartLimit overrides how large a bucket part may grow before the recorder
// starts another (0 or negative = one file per bucket, the pre-rotation layout).
// Call it before Start: it is not safe to change once events are being written.
func (f *FeedRecorder) SetPartLimit(bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bucketName != "" {
		slog.Warn("feed: ignoring a part-limit change on an open bucket",
			"feed", f.source, "symbol", f.symbol)
		return
	}
	f.partLimit = bytes
}

// Start launches a background ticker that keeps the open bucket's
// metadata.json fresh and cuts hourly buckets even when no events arrive
// (so a bucket with zero events is still closed at the hour boundary).
// Stops when ctx is cancelled or the recorder is closed.
func (f *FeedRecorder) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				f.mu.Lock()
				closed := f.closed
				f.mu.Unlock()
				if closed {
					return
				}
				f.tick(time.Now())
			}
		}
	}()

	// Writer goroutine: the only consumer of recCh. Doing the marshal + gzip
	// encode here (instead of in Record) keeps the WS dispatcher threads fast
	// and prevents one heavy feed (binance depth) from starving the market /
	// RTDS clients.
	f.wg.Add(1)
	go f.writeLoop(ctx)
}

// Close flushes and finalizes the current bucket (writes metadata.json) and
// marks the recorder closed — further Record calls are dropped. It stops the
// writer goroutine and drains any still-queued events so nothing is lost.
func (f *FeedRecorder) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	f.mu.Unlock()

	f.recClosed.Store(true)
	close(f.stopCh)
	f.wg.Wait() // writer drains its share and exits
	f.drain()   // write anything the writer didn't get to

	f.mu.Lock()
	f.closeBucketLocked()
	f.mu.Unlock()
}

// writeLoop is the recorder's writer goroutine: it encodes queued events and
// exits when the context is cancelled or Close() signals stop. The heavy
// json.Marshal + gzip encode runs here, off the WS dispatcher threads.
func (f *FeedRecorder) writeLoop(ctx context.Context) {
	defer f.wg.Done()
	for {
		select {
		case <-ctx.Done():
			f.drain()
			return
		case <-f.stopCh:
			f.drain()
			return
		case item := <-f.recCh:
			f.writeOne(item)
		}
	}
}

// drain writes any events still queued in recCh. Used at Close/stop so queued
// events are flushed before the bucket is finalized. Items are written in
// order, so each lands in its own hour's bucket.
func (f *FeedRecorder) drain() {
	for {
		select {
		case item := <-f.recCh:
			f.writeOne(item)
		default:
			return
		}
	}
}

// ensureBucketOpenLocked opens the bucket if none is open (first event),
// without rotating on an epoch change — rotation is handled by the writer
// goroutine (writeOne) so queued items always land in their own epoch's bucket.
func (f *FeedRecorder) ensureBucketOpenLocked(ts time.Time) {
	if f.bucketStart.IsZero() {
		f.openBucketLocked(ts)
	}
}

// bucketRoot is the directory the bucket folders live under: {root}/{market}
// for binance feeds (so spot and perp cannot collide), {root} otherwise.
func (f *FeedRecorder) bucketRoot() string {
	if f.market == "" {
		return f.root
	}
	return filepath.Join(f.root, f.market)
}

// epochBucket returns the epoch id containing ts and its bounds.
func (f *FeedRecorder) epochBucket(ts time.Time) (id string, start, end time.Time) {
	id = libs.EpochIDAt(ts, f.anchor)
	start, end, _ = libs.EpochBounds(id, f.anchor) // anchor validated at construction
	return id, start, end
}

// writeOne marshals and encodes a single queued feed event, updating bucket
// stats. Runs on the writer goroutine (or during Close's drain).
func (f *FeedRecorder) writeOne(item feedItem) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ensureBucketLocked(item.receivedAt)
	if f.enc == nil {
		return // bucket open failed
	}

	data, err := json.Marshal(item.ev)
	if err != nil {
		slog.Warn("feed: failed to marshal event", "type", item.typ, "err", err)
		return
	}

	rec := RecordedEvent{
		SeqID:      item.seqID,
		Type:       item.typ,
		Timestamp:  item.ts.UnixMilli(),
		ReceivedAt: item.receivedAt.UnixMilli(),
		Data:       data,
	}
	if err := f.enc.Encode(rec); err != nil {
		slog.Warn("feed: encode failed", "type", item.typ, "err", err)
		return
	}

	f.count++
	f.typeCounts[item.typ]++
	if f.firstTs == 0 || rec.Timestamp < f.firstTs {
		f.firstTs = rec.Timestamp
	}
	if rec.Timestamp > f.lastTs {
		f.lastTs = rec.Timestamp
	}

	// Latency (ReceivedAt - Timestamp).
	latencyMs := float64(rec.ReceivedAt - rec.Timestamp)
	f.latencySum += latencyMs
	f.latencyCount++
	if latencyMs > f.maxLatencyMs {
		f.maxLatencyMs = latencyMs
	}

	// Broker-timestamp gap (> 2s) — classified as data-loss/inactivity at
	// bucket close using the connection event log.
	if f.prevTs != 0 {
		gapMs := rec.Timestamp - f.prevTs
		if gapMs > brokerGapThresholdMs {
			f.gapCount++
			f.totalGapMs += gapMs
			if gapMs > f.maxGapMs {
				f.maxGapMs = gapMs
			}
			f.gapWindows = append(f.gapWindows, [2]int64{f.prevTs, rec.Timestamp})
		}
	}
	f.prevTs = rec.Timestamp

	if f.count%feedFlushEvery == 0 && f.gw != nil {
		_ = f.gw.Flush() // durability: recoverable up to this point on crash
	}
	f.rotateIfFullLocked()
}

// partFileName is the file name of part i: the documented events.gz first, then
// events.001.gz, events.002.gz, …
//
// Keeping events.gz for the first part means a bucket that never rotates is
// byte-for-byte the file it always was, so existing readers keep working. It
// also means a plain glob is NOT the read order — "events.001.gz" sorts ahead of
// "events.gz" — so the order is events.gz followed by the numbered parts in
// numeric order, which is exactly what metadata.json's `parts` list records.
func partFileName(i int) string {
	if i <= 0 {
		return "events.gz"
	}
	return fmt.Sprintf("events.%03d.gz", i)
}

// rotateIfFullLocked starts a new part once the open one has reached the size
// limit. Caller holds f.mu.
//
// The size is the file's own offset, i.e. the COMPRESSED bytes already on disk —
// that is what a download and a reader have to cope with. gzip spills its
// internal buffer as it fills, so the offset tracks the stream closely, and the
// periodic flush above marks a hard point every feedFlushEvery records.
func (f *FeedRecorder) rotateIfFullLocked() {
	if f.partLimit <= 0 || f.f == nil {
		return
	}
	off, err := f.f.Seek(0, io.SeekCurrent)
	if err != nil || off < f.partLimit {
		return
	}
	dir := filepath.Join(f.bucketRoot(), f.bucketName)
	name := partFileName(len(f.partNames))
	if err := f.closePartLocked(); err != nil {
		slog.Error("feed: closing a full part failed", "path", filepath.Join(dir, name), "err", err)
	}
	if err := f.openPartLocked(dir); err != nil {
		slog.Error("feed: opening the next part failed", "dir", dir, "limit", f.partLimit, "err", err)
		return
	}
	slog.Info("feed: rotated bucket part",
		"feed", f.source,
		"symbol", f.symbol,
		"bucket", f.bucketName,
		"part", f.partNames[len(f.partNames)-1],
		"bytes", off,
	)
}

// closePartLocked flushes and closes the open part, leaving f.enc nil. Caller
// holds f.mu.
func (f *FeedRecorder) closePartLocked() error {
	if f.enc == nil {
		return nil
	}
	if f.gw != nil {
		if err := f.gw.Close(); err != nil {
			_ = f.f.Close()
			f.gw, f.f, f.enc = nil, nil, nil
			return err
		}
	}
	err := f.f.Close()
	f.gw, f.f, f.enc = nil, nil, nil
	return err
}

// openPartLocked creates the next part file inside dir and starts streaming into
// it, appending its name to the bucket's part list. Caller holds f.mu.
func (f *FeedRecorder) openPartLocked(dir string) error {
	name := partFileName(len(f.partNames))
	path := filepath.Join(dir, name)

	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	gw, err := gzip.NewWriterLevel(fh, gzip.BestSpeed)
	if err != nil {
		_ = fh.Close()
		return err
	}
	enc := json.NewEncoder(gw)
	enc.SetEscapeHTML(false)

	f.f = fh
	f.gw = gw
	f.enc = enc
	f.partNames = append(f.partNames, name)
	return nil
}

// FeedHealth is a snapshot of a feed recorder's liveness, used by the
// collector's health monitor to detect silent disconnects (a feed that stops
// receiving events without a WS status change).
type FeedHealth struct {
	Source      string
	Symbol      string
	Market      string
	EventCount  int64
	LastEventTs int64
	StaleMs     int64 // now - lastEventTs (0 until the first event)
	Drops       int64
}

// Health returns a liveness snapshot for the feed.
func (f *FeedRecorder) Health(now time.Time) FeedHealth {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := FeedHealth{
		Source:      f.source,
		Symbol:      f.symbol,
		Market:      f.market,
		EventCount:  f.count,
		LastEventTs: f.lastTs,
		Drops:       f.drops.Load(),
	}
	if f.lastTs > 0 {
		h.StaleMs = now.UnixMilli() - f.lastTs
	}
	return h
}

// EventCount returns the number of records written to the current bucket.
func (f *FeedRecorder) EventCount() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

// BucketName returns the active bucket folder, or "" when idle.
func (f *FeedRecorder) BucketName() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bucketName
}

// RecordConnectionEvent records a WebSocket connect/disconnect for the feed's
// source, used to classify data-loss gaps in the bucket metadata.
func (f *FeedRecorder) RecordConnectionEvent(status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connEvents = append(f.connEvents, ConnectionEvent{Status: status, Timestamp: time.Now().UnixMilli()})
}

// Record enqueues a supported feed event for the writer goroutine, opening the
// bucket on the first event so the on-disk layout exists immediately. It never
// blocks on the heavy marshal/encode (done on the writer goroutine), so a slow
// feed (e.g. binance depth) can't stall the WS dispatcher threads of the
// market / RTDS clients. Returns false for non-feed event types, or when the
// recorder is closed / its queue is full.
func (f *FeedRecorder) Record(ev any) bool {
	if f.recClosed.Load() {
		return false
	}
	typ, ts, receivedAt, seqID, ok := feedEventMeta(ev)
	if !ok {
		return false
	}

	// Open the bucket if this is the first event (cheap in steady state).
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return false
	}
	f.ensureBucketOpenLocked(receivedAt)
	f.mu.Unlock()

	item := feedItem{ev: ev, typ: typ, ts: ts, receivedAt: receivedAt, seqID: seqID}
	select {
	case f.recCh <- item:
		return true
	default:
		n := f.drops.Add(1)
		if now := time.Now(); now.UnixNano()-f.lastDropLog.Load() >= int64(feedDropLogInterval) {
			f.lastDropLog.Store(now.UnixNano())
			slog.Warn("feed: queue full, dropping events",
				"feed", f.source, "symbol", f.symbol, "dropped_total", n)
		}
		return false
	}
}

// tick runs periodically (from Start). It refreshes the open bucket's
// metadata.json (so metadata survives even a hard kill) and closes the bucket
// once the epoch advances (leaving it closed; the next event reopens the new
// epoch's bucket).
func (f *FeedRecorder) tick(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bucketStart.IsZero() {
		return
	}
	id, _, _ := f.epochBucket(now)
	if id > f.bucketEpoch { // only ever rotate forwards
		f.closeBucketLocked()
		return
	}
	f.writeOpenBucketMetadataLocked()
}

// writeOpenBucketMetadataLocked writes (or refreshes) metadata.json for the
// current open bucket without closing it, so metadata exists even if the
// process is killed before a clean bucket close. Caller holds f.mu.
func (f *FeedRecorder) writeOpenBucketMetadataLocked() {
	if f.enc == nil {
		return
	}
	meta := f.buildFeedMetadataLocked()
	if err := writeMetadata(f.bucketRoot(), f.bucketName, meta); err != nil {
		slog.Error("feed: metadata write failed", "dir", f.bucketRoot(), "bucket", f.bucketName, "err", err)
	}
}

// ensureBucketLocked opens/rotates to the epoch bucket containing ts.
func (f *FeedRecorder) ensureBucketLocked(ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}
	id, _, _ := f.epochBucket(ts)

	if !f.bucketStart.IsZero() {
		// Epoch ids are YYYY-MM-DD, so they compare lexically. A late event from
		// an already-closed epoch stays in the current bucket rather than
		// reopening history.
		if id <= f.bucketEpoch {
			return
		}
		f.closeBucketLocked()
	}
	f.openBucketLocked(ts)
}

// openBucketLocked creates {root}/[{market}/]{prefix}{symbol}_{epochID}/ and
// starts streaming into its first part.
func (f *FeedRecorder) openBucketLocked(ts time.Time) {
	epochID, start, end := f.epochBucket(ts)
	name := fmt.Sprintf("%s%s_%s", bucketPrefix(f.source), bucketSymbol(f.symbol), epochID)
	dir := filepath.Join(f.bucketRoot(), name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("feed: mkdir failed", "dir", dir, "err", err)
		return
	}

	f.bucketName = name
	f.bucketEpoch = epochID
	f.bucketStart = start
	f.bucketEnd = end
	f.partNames = nil
	if err := f.openPartLocked(dir); err != nil {
		slog.Error("feed: create failed", "dir", dir, "err", err)
		f.bucketName = ""
		f.bucketEpoch = ""
		f.bucketStart = time.Time{}
		f.bucketEnd = time.Time{}
		return
	}

	f.count = 0
	f.typeCounts = make(map[string]int64)
	f.firstTs = 0
	f.lastTs = 0
	f.prevTs = 0
	f.latencySum = 0
	f.latencyCount = 0
	f.maxLatencyMs = 0
	f.gapCount = 0
	f.maxGapMs = 0
	f.totalGapMs = 0
	f.gapWindows = nil
	f.connEvents = nil

	slog.Info("feed: opened bucket",
		"feed", f.source,
		"symbol", f.symbol,
		"bucket", name,
		"path", filepath.Join(dir, f.partNames[0]),
	)
}

// closeBucketLocked flushes the current part and writes the bucket's
// metadata.json.
func (f *FeedRecorder) closeBucketLocked() {
	if f.bucketName == "" {
		return
	}

	name := f.bucketName
	if err := f.closePartLocked(); err != nil {
		slog.Error("feed: closing the last part failed", "bucket", name, "err", err)
	}

	meta := f.buildFeedMetadataLocked()
	if err := writeMetadata(f.bucketRoot(), name, meta); err != nil {
		slog.Error("feed: metadata write failed", "dir", f.bucketRoot(), "bucket", name, "err", err)
	}

	slog.Info("feed: closed bucket",
		"feed", f.source,
		"symbol", f.symbol,
		"bucket", name,
		"events", f.count,
		"parts", len(f.partNames),
	)

	f.bucketName = ""
	f.bucketEpoch = ""
	f.bucketStart = time.Time{}
	f.bucketEnd = time.Time{}
	f.partNames = nil
	f.count = 0
	f.typeCounts = make(map[string]int64)
	f.firstTs = 0
	f.lastTs = 0
	f.prevTs = 0
	f.latencySum = 0
	f.latencyCount = 0
	f.maxLatencyMs = 0
	f.gapCount = 0
	f.maxGapMs = 0
	f.totalGapMs = 0
	f.gapWindows = nil
	f.connEvents = nil
}

// buildFeedMetadataLocked computes the FeedMetadata (including latency, gap
// and disconnect stats) for the current open bucket. Caller holds f.mu.
func (f *FeedRecorder) buildFeedMetadataLocked() *FeedMetadata {
	var gi *GapInfo
	var connInfo *ConnectionInfo
	if f.count > 0 {
		disconnectWindows := buildDisconnectWindows(f.connEvents)
		connInfo = computeConnectionInfo(f.connEvents)

		var dataLossGaps, inactivityGaps, dataLossMs int64
		for _, gw := range f.gapWindows {
			if overlapsDisconnect(gw[0], gw[1], disconnectWindows) {
				dataLossGaps++
				dataLossMs += gw[1] - gw[0]
			} else {
				inactivityGaps++
			}
		}
		avgLatency := 0.0
		if f.latencyCount > 0 {
			avgLatency = math.Round(f.latencySum/float64(f.latencyCount)*100) / 100
		}
		gi = &GapInfo{
			BrokerTimestampGaps: int(f.gapCount),
			MaxBrokerGapMs:      f.maxGapMs,
			TotalBrokerGapMs:    f.totalGapMs,
			DataLossGaps:        int(dataLossGaps),
			InactivityGaps:      int(inactivityGaps),
			DataLossMs:          dataLossMs,
			AvgLatencyMs:        avgLatency,
			MaxLatencyMs:        f.maxLatencyMs,
			StartTimestamp:      f.firstTs,
			EndTimestamp:        f.lastTs,
		}
	}
	tailStale := int64(0)
	if f.count > 0 && f.lastTs > 0 && !f.bucketEnd.IsZero() {
		tailStale = f.bucketEnd.UnixMilli() - f.lastTs
		if tailStale < 0 {
			tailStale = 0
		}
	}

	return &FeedMetadata{
		Feed:         f.source,
		Symbol:       f.symbol,
		Market:       f.market,
		Parts:        append([]string(nil), f.partNames...),
		EpochID:      f.bucketEpoch,
		Anchor:       string(f.anchor),
		StartTime:    f.bucketStart.UnixMilli(),
		EndTime:      f.bucketEnd.UnixMilli(),
		EventCount:   f.count,
		EventCounts:  f.typeCounts,
		FirstEventTs: f.firstTs,
		LastEventTs:  f.lastTs,
		TailStaleMs:  tailStale,
		RecordedAt:   time.Now().UnixMilli(),
		DataQuality:  gi,
		Connections:  connInfo,
	}
}

// feedEventMeta extracts the recordable metadata from a feed event.
func feedEventMeta(ev any) (typ string, ts, receivedAt time.Time, seqID int64, ok bool) {
	switch e := ev.(type) {
	case *connector.CryptoPriceEvent:
		typ := FeedEventCryptoPrice
		if e.Source == "chainlink_twap" {
			typ = FeedEventChainlinkTWAP
		}
		return typ, e.Timestamp, e.ReceivedAt, e.SeqID, true
	case *connector.BinanceBookTickerEvent:
		return FeedEventBinanceBookTkr, e.Timestamp, e.ReceivedAt, e.SeqID, true
	case *connector.BinanceAggTradeEvent:
		return FeedEventBinanceAggTrade, e.Timestamp, e.ReceivedAt, e.SeqID, true
	case *connector.BinanceDepthEvent:
		return FeedEventBinanceDepth, e.Timestamp, e.ReceivedAt, e.SeqID, true
	case *connector.BinanceKlineEvent:
		// Binance pushes an update for the in-progress candle every second, all
		// of them redundant with the trade stream. Only the closed candle — the
		// one that can be cross-checked against aggTrade — is worth a row.
		if !e.IsFinal {
			return "", time.Time{}, time.Time{}, 0, false
		}
		return FeedEventBinanceKline, e.Timestamp, e.ReceivedAt, e.SeqID, true
	}
	return "", time.Time{}, time.Time{}, 0, false
}

// feedKey builds the routing key for a feed recorder: source|market|symbol
// (market empty for rtds). Symbol matching is case-insensitive.
func feedKey(source, market, symbol string) string {
	return source + "|" + market + "|" + strings.ToLower(strings.TrimSpace(symbol))
}

// bucketSymbol sanitizes a symbol for use in a folder name: lowercased, with
// "/" flattened (chainlink feeds e.g. "btc/usd" → "btc_usd"; binance symbols
// e.g. "BTCUSDT" → "btcusdt").
func bucketSymbol(symbol string) string {
	return strings.ToLower(strings.ReplaceAll(symbol, "/", "_"))
}

// bucketPrefix returns a folder-name prefix that disambiguates feed sources
// sharing the same root directory. chainlink_twap shares the RTDS root with
// FeedSourceRTDS and can carry the same symbol (e.g. "btc/usd"), so it is
// prefixed; rtds and binance live in separate roots and need no prefix.
func bucketPrefix(source string) string {
	if source == FeedSourceChainlinkTWAP {
		return "chainlink_twap_"
	}
	return ""
}
