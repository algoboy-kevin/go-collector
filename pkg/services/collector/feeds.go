package collector

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
)

// feedFlushEvery is how many records to buffer before flushing the gzip
// stream, so a crash mid-bucket still leaves recoverable data.
const feedFlushEvery = 512

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
//	{root}/[{SOURCE}_]{SYMBOL}_{hourStartUnix}/
//	    events.gz       ← gzip JSONL of RecordedEvent (mixed event types)
//	    metadata.json   ← FeedMetadata (bucket window, counts, feed info)
//
// The chainlink_twap source gets a "chainlink_twap_" prefix because it shares
// the RTDS root with FeedSourceRTDS and would otherwise collide with the same
// symbol recorded as a reference price (e.g. btc/usd TWAP vs btc/usd).
//
// Buckets are cut at wall-clock hour boundaries so folders/files stay small.
// Unlike market sessions (buffered then written once at finalize), a feed
// recorder streams events to the current bucket as they arrive — required
// because Binance feeds are continuous and high-volume (trades + partial
// book depth at up to 10/s) and would be too large to hold in memory.
type FeedRecorder struct {
	root   string // e.g. "data/rtds" or "data/binance"
	symbol string // e.g. "btcusdt" / "BTCUSDT"
	source string // FeedSourceRTDS or FeedSourceBinance
	market string // binance market ("spot"/"perp"), empty for rtds

	mu          sync.Mutex
	closed      bool
	bucketName  string
	bucketStart time.Time
	f           *os.File
	gw          *gzip.Writer
	enc         *json.Encoder

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
// and market is the binance market ("spot"/"perp") or empty for rtds.
func NewFeedRecorder(root, symbol, source, market string) *FeedRecorder {
	return &FeedRecorder{
		root:       root,
		symbol:     symbol,
		source:     source,
		market:     market,
		typeCounts: make(map[string]int64),
		recCh:      make(chan feedItem, 8192),
		stopCh:     make(chan struct{}),
	}
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
// without rotating on hour change — rotation is handled by the writer
// goroutine (writeOne) so queued items always land in their own hour's bucket.
func (f *FeedRecorder) ensureBucketOpenLocked(ts time.Time) {
	if f.bucketStart.IsZero() {
		f.openBucketLocked(ts.Truncate(time.Hour))
	}
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
// metadata.json (so metadata survives even a hard kill) and closes the
// bucket once the wall-clock hour advances (leaving it closed; the next
// event reopens the new hour's bucket).
func (f *FeedRecorder) tick(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bucketStart.IsZero() {
		return
	}
	hour := now.Truncate(time.Hour)
	if hour.After(f.bucketStart) {
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
	if err := writeMetadata(f.root, f.bucketName, meta); err != nil {
		slog.Error("feed: metadata write failed", "dir", f.root, "bucket", f.bucketName, "err", err)
	}
}

// ensureBucketLocked opens/rotates to the bucket containing ts.
func (f *FeedRecorder) ensureBucketLocked(ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}
	hour := ts.Truncate(time.Hour)

	if !f.bucketStart.IsZero() {
		if hour.Before(f.bucketStart) {
			hour = f.bucketStart // out-of-order arrival — stay in current bucket
		}
		if hour.Equal(f.bucketStart) {
			return
		}
		f.closeBucketLocked()
	}
	f.openBucketLocked(hour)
}

// openBucketLocked creates {root}/[{SOURCE}_]{SYMBOL}_{hourStartUnix}/events.gz
// and starts streaming into it.
func (f *FeedRecorder) openBucketLocked(hour time.Time) {
	name := fmt.Sprintf("%s%s_%d", bucketPrefix(f.source), bucketSymbol(f.symbol), hour.Unix())
	dir := filepath.Join(f.root, name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		slog.Error("feed: mkdir failed", "dir", dir, "err", err)
		return
	}

	path := filepath.Join(dir, "events.gz")
	fh, err := os.Create(path)
	if err != nil {
		slog.Error("feed: create failed", "path", path, "err", err)
		return
	}
	gw, err := gzip.NewWriterLevel(fh, gzip.BestSpeed)
	if err != nil {
		slog.Error("feed: gzip failed", "path", path, "err", err)
		fh.Close()
		return
	}

	enc := json.NewEncoder(gw)
	enc.SetEscapeHTML(false)

	f.bucketName = name
	f.bucketStart = hour
	f.f = fh
	f.gw = gw
	f.enc = enc
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
		"path", path,
	)
}

// closeBucketLocked flushes the current bucket and writes its metadata.json.
func (f *FeedRecorder) closeBucketLocked() {
	if f.enc == nil {
		return
	}

	name := f.bucketName
	if f.gw != nil {
		_ = f.gw.Close()
	}
	if f.f != nil {
		_ = f.f.Close()
	}

	meta := f.buildFeedMetadataLocked()
	if err := writeMetadata(f.root, name, meta); err != nil {
		slog.Error("feed: metadata write failed", "dir", f.root, "bucket", name, "err", err)
	}

	slog.Info("feed: closed bucket",
		"feed", f.source,
		"symbol", f.symbol,
		"bucket", name,
		"events", f.count,
	)

	f.bucketName = ""
	f.bucketStart = time.Time{}
	f.f = nil
	f.gw = nil
	f.enc = nil
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
	if f.count > 0 && f.lastTs > 0 {
		tailStale = f.bucketStart.Add(time.Hour).UnixMilli() - f.lastTs
		if tailStale < 0 {
			tailStale = 0
		}
	}

	return &FeedMetadata{
		Feed:         f.source,
		Symbol:       f.symbol,
		Market:       f.market,
		StartTime:    f.bucketStart.UnixMilli(),
		EndTime:      f.bucketStart.Add(time.Hour).UnixMilli(),
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
