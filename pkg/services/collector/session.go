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
	resolveBasis          string  // how the outcome was decided (ResolutionBasis*)
	lastMidprice          float64 // last computed midprice (for synthetic resolve), -1 if unavailable

	// Underlying asset price at the window open/close (crypto-price API).
	openPrice  float64
	closePrice float64

	// meta describes where this market sits — epoch, series family, Gamma event
	// and settlement rule — and is written verbatim into metadata.json. The zero
	// value means a standalone recording: no epoch directory, no family blocks.
	meta MarketContext
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
	marketDir := s.dir()
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

// SetMarketContext attaches the discovery-derived context (epoch, family,
// event, settlement rule) to the session. Call it before the market settles;
// it is safe to call concurrently with Record.
func (s *RecordingSession) SetMarketContext(mc MarketContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = mc
}

// MarketContext returns a copy of the session's discovery context. It is the
// zero value for a standalone recording, so callers should check Empty().
func (s *RecordingSession) MarketContext() MarketContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

// MarkTruncated records that the run ended before this market settled, so the
// metadata is written as "unsettled" rather than given a synthesized outcome.
func (s *RecordingSession) MarkTruncated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta.Truncated = true
}

// marketRoot is the directory the market folder lives in:
// {recordingDir}/[{epoch}]. The epoch level groups a settlement day's markets.
func (s *RecordingSession) marketRoot() string {
	base := s.collector.recordingDir()
	if s.meta.EpochID == "" {
		return base
	}
	return filepath.Join(base, s.meta.EpochID)
}

// dir is the session's own folder: {recordingDir}/[{epoch}/]{marketID}.
func (s *RecordingSession) dir() string {
	return filepath.Join(s.marketRoot(), s.cfg.MarketID)
}

// settleAtLocked is the instant this market resolves: the discovered
// settlement anchor when we have one, else the configured window end.
// Caller holds s.mu.
func (s *RecordingSession) settleAtLocked() time.Time {
	if s.meta.Settlement != nil && s.meta.Settlement.At > 0 {
		return time.UnixMilli(s.meta.Settlement.At)
	}
	return s.cfg.MarketEndTime
}

// SettleAt returns the market's settlement instant. Thread-safe.
func (s *RecordingSession) SettleAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settleAtLocked()
}

// SettlementPassed reports whether the market's settlement instant has passed.
// A session still recording past this point settled without us seeing the
// resolution event, so a synthetic resolve is legitimate; before it, stopping
// the run means the recording is simply truncated.
func (s *RecordingSession) SettlementPassed(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := s.settleAtLocked()
	return !at.IsZero() && !now.Before(at)
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
	if !s.resolvedSynthetically {
		// The market's own resolution message: authoritative.
		s.resolveBasis = ResolutionBasisOnchain
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
	mc := s.meta // snapshot under the lock: Truncated is set at shutdown
	s.mu.Unlock()

	// Close the streaming events.gz and finalize the gap/latency stats.
	gi, connInfo, err := s.closeEventStream()
	if err != nil {
		return fmt.Errorf("finalize %s: %w", s.cfg.MarketID, err)
	}

	// Build metadata. StartTime is when this session's recording window opened;
	// EndTime is the market's settle instant — Settlement.At when discovery gave
	// us one, else the configured window end.
	endTime := s.cfg.MarketEndTime
	if mc.Settlement != nil && mc.Settlement.At > 0 {
		endTime = time.UnixMilli(mc.Settlement.At)
	}
	meta := &MarketMetadata{
		MarketID:         s.cfg.MarketID,
		Slug:             s.cfg.Slug,
		YesAssetID:       s.cfg.YesAssetID,
		NoAssetID:        s.cfg.NoAssetID,
		Question:         s.cfg.Question,
		ConditionID:      s.cfg.ConditionID,
		EpochID:          mc.EpochID,
		StartTime:        s.cfg.MarketStartTime.UnixMilli(),
		EndTime:          endTime.UnixMilli(),
		Resolution:       mc.ResolutionFor(s.winningOutcome),
		WinningOutcome:   s.winningOutcome,
		SyntheticResolve: s.resolvedSynthetically,
		ResolutionBasis:  s.writeBasis(mc.Truncated),
		Truncated:        mc.Truncated,
		LastMidprice:     s.lastMidprice,
		OpenPrice:        s.openPrice,
		ClosePrice:       s.closePrice,
		Family:           mc.Family,
		Event:            mc.Event,
		Settlement:       mc.Settlement,
		StrikeFilter:     mc.StrikeFilter,
		EventCount:       int(s.count),
		RecordedAt:       time.Now().UnixMilli(),
	}
	meta.DataQuality = gi
	meta.Connections = connInfo

	// Write metadata.json under {recordingDir}/[{epoch}/]{marketID}/.
	if err := writeMetadata(s.marketRoot(), s.cfg.MarketID, meta); err != nil {
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

// writeBasis is the ResolutionBasis to persist: the basis the resolution used,
// or empty when there is no outcome to qualify (an unsettled, truncated session,
// or one that was never resolved at all).
func (s *RecordingSession) writeBasis(truncated bool) string {
	if truncated || s.winningOutcome == "" {
		return ""
	}
	return s.resolveBasis
}

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
	// A truncated session (the run stopped before settlement) must never get a
	// synthesized outcome — it is written as "unsettled".
	if s.meta.Truncated {
		slog.Info("collector: skipping synthetic resolve for truncated session",
			"market", s.cfg.MarketID)
		s.mu.Unlock()
		return
	}
	// Mark as synthetic before computing midprice (under lock).
	s.resolvedSynthetically = true
	// Find the last book snapshot for midprice.
	s.lastMidprice = s.computeLastMidprice()
	s.mu.Unlock()

	lastMidprice := s.lastMidprice

	winningAssetID, basis := resolveFromMidprice(lastMidprice, s.cfg)

	s.mu.Lock()
	s.resolveBasis = basis
	s.mu.Unlock()

	if basis == ResolutionBasisNoBook {
		slog.Warn("collector: synthetic resolve — no orderbook data, defaulting to NO",
			"market", s.cfg.MarketID)
	}

	slog.Info("collector: synthetic resolve",
		"market", s.cfg.MarketID,
		"last_midprice", lastMidprice,
		"basis", basis,
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

// Midprice bounds a SETTLED market's book collapses into.
//
// A resolved market's book goes one-sided: if YES won, the YES token bids at
// ~0.99 and nobody offers to sell it, so there is no ask — midpriceFromBook
// substitutes the maximum (1.00), and the midprice reads (0.99+1)/2 = 0.995.
// The mirror case (NO won) leaves the bid side empty with the ask at ~0.01,
// reading (0+0.01)/2 = 0.005. So a midprice this far out is the outcome itself,
// not a probability.
const (
	collapsedBookYesMid = 0.99
	collapsedBookNoMid  = 0.01
)

// resolveFromMidprice decides a market's outcome from the last midprice taken a
// minute past the settlement instant, and reports how it decided.
//
// The collapsed-book bounds come first because they are evidence rather than a
// guess: a book that has already gone one-sided *is* the resolution. A midprice
// still between them means the book had not collapsed by the time we looked —
// typically a book frozen since before settlement — so the outcome falls back to
// the 0.5 boundary and is recorded as the weaker `midprice` basis, and a session
// with no book at all (mid < 0) defaults to NO, recorded as `no_book`.
func resolveFromMidprice(mid float64, cfg SessionConfig) (winningAssetID, basis string) {
	switch {
	case mid < 0:
		return cfg.NoAssetID, ResolutionBasisNoBook
	case mid >= collapsedBookYesMid:
		return cfg.YesAssetID, ResolutionBasisCollapsedBook
	case mid <= collapsedBookNoMid:
		return cfg.NoAssetID, ResolutionBasisCollapsedBook
	case mid >= 0.5:
		return cfg.YesAssetID, ResolutionBasisMidprice
	default:
		return cfg.NoAssetID, ResolutionBasisMidprice
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
