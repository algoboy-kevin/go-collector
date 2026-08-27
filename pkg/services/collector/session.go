package collector

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// RecordingSession buffers events for a single market and manages its
// lifecycle from recording through resolution to disk finalization.
//
// Lifecycle:
//
//	Start() → RECORDING → Resolve() → RESOLVED → Finalize() → FINALIZED
//	                     → synthetic timer → synthetic Resolve() → Finalize()
type RecordingSession struct {
	cfg   SessionConfig
	state SessionState
	mu    sync.Mutex

	// Streaming event writer — opened lazily on the first Record. Events are
	// appended to {dir}/{marketID}/events.gz as they arrive instead of being
	// buffered in RAM, so memory stays flat regardless of market activity
	// (a hyperactive btc_5m market can emit 100k+ events per window, which
	// previously buffered ~50-80MB and OOM'd the 458MB collector box).
	f   *os.File
	gz  *gzip.Writer
	enc *json.Encoder

	// Incremental stats, updated per recorded event (mirrors FeedRecorder).
	count        int64
	firstTs      int64
	lastTs       int64
	prevTs       int64
	latencySum   float64
	latencyCount int64
	maxLatencyMs float64
	gapCount     int64
	maxGapMs     int64
	totalGapMs   int64
	gapWindows   [][2]int64

	// Last book snapshot (raw Data) per asset — used for synthetic midprice
	// resolution, replacing the old scan of the full buffered event list.
	lastYesBook []byte
	lastNoBook  []byte

	// parent collector (for asset→market routing and finalize callback)
	collector *EventCollector

	// synthetic resolve timer
	syntheticTimer *time.Timer

	// onFinalized is called after a successful Finalize() to allow the
	// collector to unsubscribe from WS assets and clean up.
	onFinalized func()

	// Progress tracking
	progressCount int64
	startedAt     time.Time

	// Resolution tracking
	winningOutcome        string  // "YES" or "NO" — set by Resolve()
	resolvedSynthetically bool    // true if resolved via synthetic midprice
	lastMidprice          float64 // last computed midprice (for synthetic resolve), -1 if unavailable

	// Underlying asset price at the window open/close (crypto-price API).
	openPrice  float64
	closePrice float64

	// series identifies the rolling market series this session belongs to
	// (empty for standalone recordings). Used at finalize to fetch the
	// window's open/close price via SERIES_CRYPTO_CONFIG.
	series libs.RollingMarketSeries
}

// newRecordingSession creates a new session in RECORDING state and starts
// the synthetic resolve timer.
func newRecordingSession(cfg SessionConfig, collector *EventCollector) *RecordingSession {
	s := &RecordingSession{
		cfg:       cfg,
		state:     SessionRecording,
		collector: collector,
		startedAt: time.Now(),
	}

	// Start synthetic resolve timer.
	delay := cfg.SyntheticResolveAfter
	if delay <= 0 {
		delay = DefaultSyntheticResolveDelay
	}
	closeTime := cfg.MarketEndTime
	if closeTime.IsZero() {
		// Fallback: 5 minutes from start.
		closeTime = cfg.MarketStartTime.Add(5 * time.Minute)
	}

	s.syntheticTimer = time.AfterFunc(time.Until(closeTime.Add(delay)), func() {
		s.handleSyntheticResolve()
	})

	slog.Info("collector: session started",
		"market", cfg.MarketID,
		"slug", cfg.Slug,
		"yes_asset", cfg.YesAssetID,
		"no_asset", cfg.NoAssetID,
		"end_time", closeTime,
		"synthetic_delay", delay,
	)

	return s
}

// Record buffers a single event.  Thread-safe.
// Only records in RECORDING state — returns false if already resolved/finalized.
func (s *RecordingSession) Record(ev RecordedEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != SessionRecording {
		return false
	}

	// Lazily open the per-market events.gz stream on the first event.
	if err := s.ensureWriterLocked(); err != nil {
		slog.Warn("collector: open event stream failed", "market", s.cfg.MarketID, "err", err)
		return false
	}
	if err := s.writeEventLocked(ev); err != nil {
		slog.Warn("collector: write event failed", "market", s.cfg.MarketID, "err", err)
		return false
	}

	// Progress log every 5,000 events.
	s.progressCount++
	if s.progressCount%5000 == 0 {
		slog.Info("collector: recording progress",
			"market", s.cfg.MarketID,
			"slug", s.cfg.Slug,
			"events", s.count,
			"elapsed", time.Since(s.startedAt).Round(time.Second),
		)
	}

	return true
}

// ensureWriterLocked opens the per-market events.gz stream on first use.
// Caller holds s.mu.
func (s *RecordingSession) ensureWriterLocked() error {
	if s.enc != nil {
		return nil
	}
	dir := s.collector.recordingDir()
	marketDir := filepath.Join(dir, s.cfg.MarketID)
	if err := os.MkdirAll(marketDir, 0755); err != nil {
		return fmt.Errorf("ensureWriter: mkdir: %w", err)
	}
	path := filepath.Join(marketDir, "events.gz")
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("ensureWriter: create: %w", err)
	}
	gz, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("ensureWriter: gzip: %w", err)
	}
	s.f = f
	s.gz = gz
	s.enc = json.NewEncoder(gz)
	s.enc.SetEscapeHTML(false)
	return nil
}

// writeEventLocked encodes one event to the stream and updates the rolling
// gap/latency stats (mirrors FeedRecorder.writeOne). Caller holds s.mu.
func (s *RecordingSession) writeEventLocked(ev RecordedEvent) error {
	// Assign a monotonic SeqID in arrival order.
	ev.SeqID = s.count + 1
	if err := s.enc.Encode(ev); err != nil {
		return err
	}
	s.count++
	if s.firstTs == 0 || ev.Timestamp < s.firstTs {
		s.firstTs = ev.Timestamp
	}
	if ev.Timestamp > s.lastTs {
		s.lastTs = ev.Timestamp
	}

	// Latency (ReceivedAt - Timestamp).
	latencyMs := float64(ev.ReceivedAt - ev.Timestamp)
	s.latencySum += latencyMs
	s.latencyCount++
	if latencyMs > s.maxLatencyMs {
		s.maxLatencyMs = latencyMs
	}

	// Broker-timestamp gap (> 2s) — classified against disconnect windows at
	// finalize.
	if s.prevTs != 0 {
		gapMs := ev.Timestamp - s.prevTs
		if gapMs > brokerGapThresholdMs {
			s.gapCount++
			s.totalGapMs += gapMs
			if gapMs > s.maxGapMs {
				s.maxGapMs = gapMs
			}
			s.gapWindows = append(s.gapWindows, [2]int64{s.prevTs, ev.Timestamp})
		}
	}
	s.prevTs = ev.Timestamp

	// Keep the latest book snapshot per asset for synthetic midprice
	// resolution (book_snapshot events are large; only the last one matters).
	if ev.Type == "book_snapshot" {
		var snap connector.BookSnapshotEvent
		if err := json.Unmarshal(ev.Data, &snap); err == nil {
			switch snap.AssetID {
			case s.cfg.YesAssetID:
				s.lastYesBook = append(s.lastYesBook[:0], ev.Data...)
			case s.cfg.NoAssetID:
				s.lastNoBook = append(s.lastNoBook[:0], ev.Data...)
			}
		}
	}
	return nil
}

// closeEventStream flushes/closes the events.gz file, classifies broker gaps
// against disconnect windows, and returns the final data-quality stats.
func (s *RecordingSession) closeEventStream() (*GapInfo, *ConnectionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gz != nil {
		if err := s.gz.Close(); err != nil {
			_ = s.f.Close()
			return nil, nil, fmt.Errorf("gzip close: %w", err)
		}
		s.gz = nil
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			return nil, nil, fmt.Errorf("file close: %w", err)
		}
		s.f = nil
	}
	if s.count == 0 {
		return nil, nil, nil
	}

	var connEvents []ConnectionEvent
	if s.collector != nil {
		connEvents = filterConnectionEvents(s.collector.GetConnectionEvents(), s.cfg.MarketStartTime.UnixMilli(), 0)
	}
	disconnectWindows := buildDisconnectWindows(connEvents)
	connInfo := computeConnectionInfo(connEvents)

	gi := &GapInfo{
		BrokerTimestampGaps: int(s.gapCount),
		MaxBrokerGapMs:      s.maxGapMs,
		TotalBrokerGapMs:    s.totalGapMs,
		MaxLatencyMs:        s.maxLatencyMs,
		StartTimestamp:      s.firstTs,
		EndTimestamp:        s.lastTs,
	}
	if s.latencyCount > 0 {
		gi.AvgLatencyMs = math.Round(s.latencySum/float64(s.latencyCount)*100) / 100
	}
	for _, gw := range s.gapWindows {
		if overlapsDisconnect(gw[0], gw[1], disconnectWindows) {
			gi.DataLossGaps++
			gi.DataLossMs += gw[1] - gw[0]
		} else {
			gi.InactivityGaps++
		}
	}
	return gi, connInfo, nil
}

// EventCount returns the number of recorded events.  Thread-safe.
func (s *RecordingSession) EventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.count)
}

// State returns the current session state.  Thread-safe.
func (s *RecordingSession) State() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SetOpenPrice records the underlying asset price at the market window open.
func (s *RecordingSession) SetOpenPrice(price float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openPrice = price
}

// SetClosePrice records the underlying asset price at the market window close.
func (s *RecordingSession) SetClosePrice(price float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closePrice = price
}

// Resolve transitions the session to RESOLVED state.  Thread-safe.
// Returns an error if already resolved or finalized.
func (s *RecordingSession) Resolve(winningAssetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == SessionFinalized {
		return fmt.Errorf("session %s already finalized", s.cfg.MarketID)
	}
	if s.state == SessionResolved {
		return nil // idempotent
	}

	s.state = SessionResolved

	// Stop the synthetic timer since we got a real resolution.
	if s.syntheticTimer != nil {
		s.syntheticTimer.Stop()
		s.syntheticTimer = nil
	}

	// Determine winning outcome.
	s.winningOutcome = "YES"
	if winningAssetID == s.cfg.NoAssetID {
		s.winningOutcome = "NO"
	}

	slog.Info("collector: session resolved",
		"market", s.cfg.MarketID,
		"outcome", s.winningOutcome,
		"synthetic", s.resolvedSynthetically,
		"total_events", s.count,
	)

	return nil
}

// Finalize flushes all buffered events to disk and transitions to FINALIZED.
// Thread-safe.  Safe to call multiple times (idempotent after first).
func (s *RecordingSession) Finalize() error {
	s.mu.Lock()
	// Idempotent: skip if already finalized.
	if s.state == SessionFinalized {
		s.mu.Unlock()
		return nil
	}
	s.state = SessionFinalized
	s.mu.Unlock()

	// Fetch underlying open/close prices at finalize so both are settled
	// together from a single API call. A market that ended midway (before
	// its end time) records only the open price.
	if s.collector != nil {
		s.collector.fetchWindowPrices(s)
	}

	// Close the streaming events.gz and finalize the gap/latency stats.
	gi, connInfo, err := s.closeEventStream()
	if err != nil {
		return fmt.Errorf("finalize %s: %w", s.cfg.MarketID, err)
	}

	// Build metadata.
	meta := &MarketMetadata{
		MarketID:         s.cfg.MarketID,
		Slug:             s.cfg.Slug,
		YesAssetID:       s.cfg.YesAssetID,
		NoAssetID:        s.cfg.NoAssetID,
		Question:         s.cfg.Question,
		ConditionID:      s.cfg.ConditionID,
		StartTime:        s.cfg.MarketStartTime.UnixMilli(),
		EndTime:          s.cfg.MarketEndTime.UnixMilli(),
		Resolution:       s.winningOutcome,
		WinningOutcome:   s.winningOutcome,
		SyntheticResolve: s.resolvedSynthetically,
		LastMidprice:     s.lastMidprice,
		OpenPrice:        s.openPrice,
		ClosePrice:       s.closePrice,
		EventCount:       int(s.count),
		RecordedAt:       time.Now().UnixMilli(),
	}
	meta.DataQuality = gi
	meta.Connections = connInfo

	// Write metadata.json.
	if err := writeMetadata(s.collector.recordingDir(), s.cfg.MarketID, meta); err != nil {
		return fmt.Errorf("finalize %s: writeMetadata: %w", s.cfg.MarketID, err)
	}

	slog.Info("collector: session finalized",
		"market", s.cfg.MarketID,
		"slug", s.cfg.Slug,
		"events", s.count,
	)

	// Notify collector to unsubscribe from WS assets.
	if s.onFinalized != nil {
		s.onFinalized()
	}

	return nil
}

// ── Helpers ──────────────────────────────────────────────────

// eventType returns the type discriminator string for a raw event.
func eventType(ev interface{}) string {
	switch ev.(type) {
	case *connector.PriceChangeEvent:
		return "price_change"
	case *connector.BookSnapshotEvent:
		return "book_snapshot"
	case *connector.TradeEvent:
		return "trade"
	case *connector.TickChangeEvent:
		return "tick_change"
	case *connector.MarketResolvedEvent:
		return "market_resolved"
	default:
		return "unknown"
	}
}

// eventAssetID extracts the asset ID from a raw event for routing purposes.
// Returns empty string for events without an asset ID (e.g. MarketResolvedEvent).
func eventAssetID(ev interface{}) string {
	switch e := ev.(type) {
	case *connector.PriceChangeEvent:
		if len(e.Changes) > 0 {
			return e.Changes[0].AssetID
		}
		return ""
	case *connector.BookSnapshotEvent:
		return e.AssetID
	case *connector.TradeEvent:
		return e.AssetID
	case *connector.TickChangeEvent:
		return e.AssetID
	default:
		return ""
	}
}

// handleSyntheticResolve computes a synthetic resolution based on the last
// orderbook midprice and resolves the session.
func (s *RecordingSession) handleSyntheticResolve() {
	s.mu.Lock()
	// Only apply synthetic resolve if still recording (not already resolved).
	if s.state != SessionRecording {
		s.mu.Unlock()
		return
	}
	// Mark as synthetic before computing midprice (under lock).
	s.resolvedSynthetically = true
	// Find the last book snapshot for midprice.
	s.lastMidprice = s.computeLastMidprice()
	s.mu.Unlock()

	lastMidprice := s.lastMidprice

	winningAssetID := s.cfg.NoAssetID
	if lastMidprice < 0 {
		slog.Warn("collector: synthetic resolve — no orderbook data, defaulting to NO",
			"market", s.cfg.MarketID)
	} else if lastMidprice >= 0.5 {
		winningAssetID = s.cfg.YesAssetID
	}

	slog.Info("collector: synthetic resolve",
		"market", s.cfg.MarketID,
		"last_midprice", lastMidprice,
		"winner", winningAssetID,
	)

	if err := s.Resolve(winningAssetID); err != nil {
		slog.Error("collector: synthetic resolve failed", "market", s.cfg.MarketID, "err", err)
		return
	}

	if err := s.Finalize(); err != nil {
		slog.Error("collector: synthetic finalize failed", "market", s.cfg.MarketID, "err", err)
	}
}

// computeLastMidprice scans the buffered events for the last book snapshot
// for the YES asset and returns its midprice = (best_bid + best_ask) / 2.
// The YES asset's orderbook directly reflects the market's estimated
// probability of the "Up" outcome. Using the NO asset's book would invert
// the probability (NO_price ≈ 1 - YES_price) and give wrong resolution.
//
// Connector sends book levels in arbitrary order, so we sort before picking:
// bids descending by price (best/highest first), asks ascending (best/lowest first).
// Returns -1 if no YES asset book data is available.
func (s *RecordingSession) computeLastMidprice() float64 {
	// Prefer the YES asset's book — it directly reflects the estimated
	// probability of the "Up" outcome. Falls back to the NO asset's book,
	// inverting (NO_price ≈ 1 - YES_price).
	if len(s.lastYesBook) > 0 {
		if mid, ok := midpriceFromBook(s.lastYesBook, s.cfg.YesAssetID); ok {
			return mid
		}
	}
	if len(s.lastNoBook) > 0 {
		if mid, ok := midpriceFromBook(s.lastNoBook, s.cfg.NoAssetID); ok {
			return libs.R3(1 - mid)
		}
	}
	return -1
}

// midpriceFromBook parses a stored book_snapshot Data and returns the token
// midprice = (best_bid + best_ask) / 2. Connector sends book levels in
// arbitrary order, so bids are sorted descending and asks ascending before
// picking the best levels. Defaults bid=0 / ask=1 for one-sided or empty
// books. Returns ok=false if the snapshot doesn't match wantAssetID.
func midpriceFromBook(data []byte, wantAssetID string) (float64, bool) {
	var snap connector.BookSnapshotEvent
	if err := json.Unmarshal(data, &snap); err != nil {
		return 0, false
	}
	if snap.AssetID != wantAssetID {
		return 0, false
	}

	sort.Slice(snap.Bids, func(i, j int) bool {
		return libs.ParseFloat(snap.Bids[i].Price) > libs.ParseFloat(snap.Bids[j].Price)
	})
	sort.Slice(snap.Asks, func(i, j int) bool {
		return libs.ParseFloat(snap.Asks[i].Price) < libs.ParseFloat(snap.Asks[j].Price)
	})

	bestBid := 0.0
	bestAsk := 1.0
	if len(snap.Bids) > 0 {
		bestBid = libs.ParseFloat(snap.Bids[0].Price)
	}
	if len(snap.Asks) > 0 {
		bestAsk = libs.ParseFloat(snap.Asks[0].Price)
	}
	return libs.R3((bestBid + bestAsk) / 2), true
}
