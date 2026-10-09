package collector

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
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

	// Live top-of-book per outcome token. A full book_snapshot replaces both
	// sides; a price_change carries the exchange's own BBO for the levels that
	// moved and keeps the book current between the (rare) snapshots. Used for
	// synthetic midprice resolution in place of the last stored snapshot, which
	// could be hours old even while price_change events kept the book current.
	yesBBO topOfBook
	noBBO  topOfBook

	// lastBookAt is the received-at time (unix ms) of the most recent top-of-
	// book update for either side — a book_snapshot OR a price_change carrying
	// that asset's BBO. A synthetic resolve only trusts a book that is still
	// fresh at the settlement instant; 0 means no book data was ever seen. A
	// quiet market on a healthy connection stays fresh because its
	// price_change events refresh this, not just its snapshots.
	lastBookAt int64

	// disconnectWindows are the session's market-WS down intervals, derived
	// from connEvents at finalize. A tail that overlaps one is a real data
	// loss; a tail on a healthy connection is just a quiet market.
	disconnectWindows [][2]int64

	// connEvents records the market-WS connection status changes received while
	// this session was recording. Per-session stats are derived from it rather
	// than from the collector's capped, run-wide log (which gave every market
	// the same disconnect count).
	connEvents []ConnectionEvent

	// unresolvedBasis is the basis recorded when a synthetic resolve declined
	// to decide an outcome (no data / no book / stale book). The session is
	// left unsettled, but the reason it could not be decided is preserved.
	unresolvedBasis string

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

	// Seed the connection log with the state the session was born into. A
	// session created while the market WS is already down would otherwise never
	// see the "disconnected" callback (it fired before the session existed) and
	// would undercount its own downtime.
	s.seedInitialConnectionState()

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

	// Keep the live top-of-book per asset for synthetic midprice resolution:
	// a book_snapshot is a full ladder (authoritative — it replaces both
	// sides), and a price_change carries the exchange's own BBO for the levels
	// it touched. Either refreshes lastBookAt, so a market whose book is kept
	// current only by price_change events is still fresh at settlement.
	if ev.Type == "book_snapshot" {
		var snap connector.BookSnapshotEvent
		if err := json.Unmarshal(ev.Data, &snap); err == nil {
			switch snap.AssetID {
			case s.cfg.YesAssetID:
				s.yesBBO.setFromLevels(snap.Bids, snap.Asks)
				s.noteBookAtLocked(ev.ReceivedAt)
			case s.cfg.NoAssetID:
				s.noBBO.setFromLevels(snap.Bids, snap.Asks)
				s.noteBookAtLocked(ev.ReceivedAt)
			}
		}
	}
	if ev.Type == "price_change" {
		var pc connector.PriceChangeEvent
		if err := json.Unmarshal(ev.Data, &pc); err == nil {
			for _, ch := range pc.Changes {
				switch ch.AssetID {
				case s.cfg.YesAssetID:
					s.yesBBO.updateFromChange(ch.BestBid, ch.BestAsk)
					s.noteBookAtLocked(ev.ReceivedAt)
				case s.cfg.NoAssetID:
					s.noBBO.updateFromChange(ch.BestBid, ch.BestAsk)
					s.noteBookAtLocked(ev.ReceivedAt)
				}
			}
		}
	}
	return nil
}

// noteBookAtLocked records that a usable book snapshot arrived at `at` (unix
// ms), keeping the latest. Caller holds s.mu.
func (s *RecordingSession) noteBookAtLocked(at int64) {
	if at > s.lastBookAt {
		s.lastBookAt = at
	}
}

// closeEventStream flushes/closes the events.gz file, classifies broker gaps
// against disconnect windows, and returns the final data-quality stats plus the
// session's own market-WS down intervals (used to decide the tail-truncation
// flag).
func (s *RecordingSession) closeEventStream() (*GapInfo, *ConnectionInfo, [][2]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gz != nil {
		if err := s.gz.Close(); err != nil {
			_ = s.f.Close()
			return nil, nil, nil, fmt.Errorf("gzip close: %w", err)
		}
		s.gz = nil
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			return nil, nil, nil, fmt.Errorf("file close: %w", err)
		}
		s.f = nil
	}
	if s.count == 0 {
		return nil, nil, nil, nil
	}

	// Per-session disconnect stats: the status callbacks that fell inside this
	// session's own recording window, not the run-wide (capped) log — the cap
	// gave every market the same disconnect count. An open-ended disconnect is
	// closed at the finalize instant so its duration is counted, not lost.
	connEvents := append([]ConnectionEvent(nil), s.connEvents...)
	if n := len(connEvents); n > 0 && connEvents[n-1].Status == "disconnected" {
		connEvents = append(connEvents, ConnectionEvent{Status: "connected", Timestamp: time.Now().UnixMilli()})
	}
	disconnectWindows := buildDisconnectWindows(connEvents)
	connInfo := computeConnectionInfo(connEvents)
	s.disconnectWindows = disconnectWindows

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
	return gi, connInfo, disconnectWindows, nil
}

// RecordConnection accumulates a market-WS connection status change that
// occurred while this session was still recording. Thread-safe.
//
// Per-session rather than run-wide: the collector's connection log is capped
// and shared by every market, so deriving a market's disconnect stats from it
// gave them all the same truncated count. Only callbacks that arrive while the
// session is RECORDING are kept, so the stats describe this market's window.
func (s *RecordingSession) RecordConnection(status string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != SessionRecording {
		return
	}
	s.connEvents = append(s.connEvents, ConnectionEvent{
		Status:    status,
		Timestamp: at.UnixMilli(),
	})
}

// seedInitialConnectionState records the market WS status the session was
// created under. If the WS is already disconnected, that opens a disconnect
// window at the session's start: a later "connected" callback closes it and the
// downtime is counted, instead of the session reporting zero disconnects
// because the disconnect callback fired before it existed.
func (s *RecordingSession) seedInitialConnectionState() {
	status, ok := s.collector.marketStatus()
	if !ok {
		return
	}
	switch status {
	case websocket.StatusConnected, websocket.StatusDisconnected:
		s.connEvents = append(s.connEvents, ConnectionEvent{
			Status:    string(status),
			Timestamp: s.startedAt.UnixMilli(),
		})
	}
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
	winning := s.winningOutcome
	s.mu.Unlock()

	// Close the streaming events.gz and finalize the gap/latency stats.
	gi, connInfo, disconnectWindows, err := s.closeEventStream()
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

	// Tail gap: how much of the window the feed actually covers. It only means
	// something once the window has ended, so it is computed when the settlement
	// instant is in the past. A recording whose last event predates settlement
	// by more than the silence timeout is incomplete — but only when the gap is
	// explained by a real outage (a market-WS disconnect, or a whole-feed
	// silence the watchdog recorded); a quiet wing strike on a healthy
	// connection legitimately goes minutes without an event and must not be
	// flagged. Never at the cost of a real outcome: a resolution that arrived is
	// authoritative, and an early-resolved hit market stops trading long before
	// its anchor.
	if gi != nil && gi.EndTimestamp > 0 && endTime.Before(time.Now()) {
		tailStart := gi.EndTimestamp
		tailEnd := endTime.UnixMilli()
		if tail := tailEnd - tailStart; tail > 0 {
			gi.LastEventTs = tailStart
			gi.TailGapMs = tail
			if winning == "" {
				if limit := s.marketSilenceMs(); limit > 0 && tail > limit && s.tailLostToFeed(tailStart, tailEnd, disconnectWindows) {
					mc.Truncated = true
				}
			}
		}
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
		Resolution:       mc.ResolutionFor(winning),
		WinningOutcome:   winning,
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

// writeBasis is the ResolutionBasis to persist: the reason an attempted
// synthetic resolve could not decide an outcome (no_data / no_book /
// stale_book), the basis the resolution used, or empty when there is no outcome
// and no recorded reason (an unsettled, run-truncated session).
func (s *RecordingSession) writeBasis(truncated bool) string {
	if s.unresolvedBasis != "" {
		return s.unresolvedBasis
	}
	if truncated || s.winningOutcome == "" {
		return ""
	}
	return s.resolveBasis
}

// marketSilenceMs is the configured tail-gap threshold in ms (0 = disabled).
func (s *RecordingSession) marketSilenceMs() int64 {
	if s.collector == nil {
		return DefaultMarketSilenceTimeout.Milliseconds()
	}
	return s.collector.marketSilenceTimeout().Milliseconds()
}

// bookStaleMs is the configured book-staleness bound in ms (0 = disabled).
func (s *RecordingSession) bookStaleMs() int64 {
	if s.collector == nil {
		return DefaultBookStaleAfter.Milliseconds()
	}
	return s.collector.bookStaleAfter().Milliseconds()
}

// feedLostDuring reports whether [start, end] overlaps a window in which the
// market feed was known to be down: a market-WS disconnect window from the
// session's own connection log, or a whole-feed silence interval the
// collector's watchdog recorded (StartMarketWatchdog). Either overlapping the
// interval means the feed really was down across it — the only evidence that a
// missing book, or a missing recording tail, is a real loss rather than a
// market that simply stopped trading.
func (s *RecordingSession) feedLostDuring(start, end int64, disconnectWindows [][2]int64) bool {
	if overlapsDisconnect(start, end, disconnectWindows) {
		return true
	}
	return s.collector != nil && s.collector.silenceCovers(start, end)
}

// tailLostToFeed reports whether a missing recording tail is explained by an
// actual feed outage rather than a market that simply stopped trading.
//
// A quiet wing strike on a healthy connection produces no events for minutes at
// a time, so a large tail gap alone is not evidence of loss; see feedLostDuring
// for the two things that make it one.
func (s *RecordingSession) tailLostToFeed(tailStart, tailEnd int64, disconnectWindows [][2]int64) bool {
	return s.feedLostDuring(tailStart, tailEnd, disconnectWindows)
}

// disconnectWindowsAtLocked builds this session's market-WS down intervals from
// its own connection log, extending an open-ended disconnect — one that never
// reconnected — forward to `end` so a feed still down across the interval is
// caught. Caller holds s.mu. Mirrors closeEventStream's open-ended close, but
// closes at the caller's instant (settlement here) rather than at finalize.
func (s *RecordingSession) disconnectWindowsAtLocked(end int64) [][2]int64 {
	events := append([]ConnectionEvent(nil), s.connEvents...)
	if n := len(events); n > 0 && events[n-1].Status == "disconnected" && end > events[n-1].Timestamp {
		events = append(events, ConnectionEvent{Status: "connected", Timestamp: end})
	}
	return buildDisconnectWindows(events)
}

// hasFeedLivenessEvidence reports whether this session can tell whether the
// market feed was live during a book window: it needs a collector wired to a
// market-WS status source (SetMarketStatus). Without one no disconnect window
// or watchdog silence is ever recorded, so the hard age bound
// (book_stale_timeout_s) is the only remaining guard.
func (s *RecordingSession) hasFeedLivenessEvidence() bool {
	return s.collector != nil && s.collector.hasMarketStatusSource()
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

// handleSyntheticResolve decides a market whose resolution message never
// reached us. It first asks the venue for the official outcome (Gamma via the
// connector) — that is authoritative and beats any read of our own book. Only
// when the venue has not published a resolution does it fall back to the last
// order book seen at the settlement instant.
//
// It never invents an outcome. With nothing recorded it leaves the session
// unsettled (no_data); with market data but no book it leaves it unsettled
// (no_book); and with a book whose window was spanned by a real feed outage — a
// market-WS disconnect or a whole-feed silence — it leaves it unsettled
// (stale_book). Only a book the feed was live across may decide the outcome,
// and even then the collapsed-book bounds are preferred over the 0.5-boundary
// guess.
//
// Book age alone is NOT staleness: a deep-ITM/OTM ladder strike or a far
// hit/dip rung pins its book at 0.001/0.999 and may receive no price_change for
// many minutes on a perfectly healthy feed, yet the book still reflects the
// outcome. So the interval [lastBookAt, settleAt] is judged against feed
// liveness (disconnect/silence windows), not against a per-market age bound.
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
	s.mu.Unlock()

	// Official outcome first. The lookup is bounded (see officialResolution) so
	// a hung HTTP call cannot stall the close-time timer or shutdown, and it is
	// consulted even for a market we recorded nothing from — the venue's
	// resolution stands on its own.
	if res, ok := s.officialResolution(); ok {
		winning := winningToken(s.cfg, "", string(res))
		if winning != "" {
			s.mu.Lock()
			s.resolvedSynthetically = true
			s.resolveBasis = ResolutionBasisGamma
			s.mu.Unlock()

			slog.Info("collector: resolved from the official outcome",
				"market", s.cfg.MarketID, "resolution", string(res), "winner", winning)

			if err := s.Resolve(winning); err != nil {
				slog.Error("collector: official resolve failed", "market", s.cfg.MarketID, "err", err)
				return
			}
			if err := s.Finalize(); err != nil {
				slog.Error("collector: official finalize failed", "market", s.cfg.MarketID, "err", err)
			}
			return
		}
		slog.Warn("collector: official resolution carried no usable outcome, falling back to the book",
			"market", s.cfg.MarketID, "resolution", string(res))
	}

	s.mu.Lock()
	// Re-check: a real resolution event may have finalized the session while
	// the official lookup was in flight.
	if s.state != SessionRecording {
		s.mu.Unlock()
		return
	}
	count := s.count
	lastBookAt := s.lastBookAt
	settleAt := s.settleAtLocked()
	// This session's own market-WS down intervals, with an open-ended disconnect
	// run forward to settlement so a feed still down across the window is caught.
	disconnectWindows := s.disconnectWindowsAtLocked(settleAt.UnixMilli())
	s.mu.Unlock()

	// Decide whether the book can be trusted at all, before reading it.
	switch {
	case count == 0:
		s.declineResolution(ResolutionBasisNoData) // recorded nothing at all
		return
	case lastBookAt == 0:
		s.declineResolution(ResolutionBasisNoBook) // market data, but never a book
		return
	}

	settleMs := settleAt.UnixMilli()
	if s.feedLostDuring(lastBookAt, settleMs, disconnectWindows) {
		// A real outage (market-WS disconnect or whole-feed silence) covers the
		// interval, so the book may predate the loss: refuse it.
		s.declineResolution(ResolutionBasisStaleBook)
		return
	}
	if !s.hasFeedLivenessEvidence() {
		// No way to observe the feed at all (no collector / no status source):
		// fall back to the hard age bound so a book frozen since a feed death is
		// still refused. With a status source present this bound is never used —
		// a quiet-but-healthy market resolves from its last book.
		if staleMs := s.bookStaleMs(); staleMs > 0 && settleMs-lastBookAt > staleMs {
			s.declineResolution(ResolutionBasisStaleBook)
			return
		}
	}

	// The feed was live across the book: mark synthetic and read the midprice.
	s.mu.Lock()
	s.resolvedSynthetically = true
	s.lastMidprice = s.computeLastMidprice()
	lastMidprice := s.lastMidprice
	s.mu.Unlock()

	winningAssetID, basis := resolveFromMidprice(lastMidprice, s.cfg)
	if winningAssetID == "" {
		// A book was recorded but could not be parsed — same as none.
		s.declineResolution(basis)
		return
	}

	s.mu.Lock()
	s.resolveBasis = basis
	s.mu.Unlock()

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

// declineResolution leaves a session unsettled — no winning outcome — while
// recording the reason a synthetic resolve could not decide it. This replaces
// the old behaviour that mapped "no book" to a NO win, which fabricated an
// outcome for a market we simply had no data on.
func (s *RecordingSession) declineResolution(basis string) {
	s.mu.Lock()
	s.unresolvedBasis = basis
	s.meta.Truncated = true
	if s.syntheticTimer != nil {
		s.syntheticTimer.Stop()
		s.syntheticTimer = nil
	}
	s.mu.Unlock()

	slog.Warn("collector: synthetic resolve declined — leaving the market unsettled",
		"market", s.cfg.MarketID, "basis", basis)

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

// resolveFromMidprice decides a market's outcome from a book that is still
// fresh at the settlement instant, and reports how it decided.
//
// The collapsed-book bounds come first because they are evidence rather than a
// guess: a book that has already gone one-sided *is* the resolution. A midprice
// still between them means the book had not collapsed by the time we looked, so
// the outcome falls back to the 0.5 boundary and is recorded as the weaker
// `midprice` basis. A midprice below zero means there is no book to read at all
// (mid < 0): no outcome and no basis beyond `no_book` are returned, and the
// caller must leave the session unsettled rather than map it to a NO win.
func resolveFromMidprice(mid float64, cfg SessionConfig) (winningAssetID, basis string) {
	switch {
	case mid < 0:
		return "", ResolutionBasisNoBook
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

// computeLastMidprice returns the midprice of the live top-of-book at the
// settlement instant, read from the per-asset BBO kept current by both
// book_snapshot and price_change events. The YES asset's book directly reflects
// the market's estimated probability of the "Up" outcome; the NO asset's book
// is the mirror, so it is inverted (NO_price ≈ 1 - YES_price). Returns -1 when
// no top-of-book was ever seen.
func (s *RecordingSession) computeLastMidprice() float64 {
	if mid, ok := s.yesBBO.mid(); ok {
		return mid
	}
	if mid, ok := s.noBBO.mid(); ok {
		return libs.R3(1 - mid)
	}
	return -1
}

// topOfBook is the latest known best bid/ask for one outcome token. A full book
// snapshot is authoritative and replaces both sides; a price_change reports the
// exchange's own BBO for the levels it touched and only updates the sides it
// carries (an empty/unparseable side is left as it was — the change is partial).
type topOfBook struct {
	bid    float64
	ask    float64
	hasBid bool
	hasAsk bool
}

// setFromLevels replaces both sides from a full book snapshot. An empty side is
// recorded as absent — the snapshot is authoritative — and mid() then falls back
// to the collapsed-book default (bid 0 / ask 1) for it.
func (b *topOfBook) setFromLevels(bids, asks []connector.Level) {
	if p, ok := bestLevel(bids, false); ok {
		b.bid, b.hasBid = p, true
	} else {
		b.bid, b.hasBid = 0, false
	}
	if p, ok := bestLevel(asks, true); ok {
		b.ask, b.hasAsk = p, true
	} else {
		b.ask, b.hasAsk = 0, false
	}
}

// updateFromChange applies an exchange-reported BBO update. An empty or
// unparseable side is treated as unknown and keeps its previous value.
func (b *topOfBook) updateFromChange(bestBid, bestAsk string) {
	if p, ok := parsePrice(bestBid); ok {
		b.bid, b.hasBid = p, true
	}
	if p, ok := parsePrice(bestAsk); ok {
		b.ask, b.hasAsk = p, true
	}
}

// mid returns the token midprice = (best_bid + best_ask) / 2, substituting the
// collapsed-book default (bid 0 / ask 1) for a missing side so a one-sided book
// is still readable — the same convention the previous snapshot reader used.
// ok is false when neither side has ever been seen.
func (b topOfBook) mid() (float64, bool) {
	if !b.hasBid && !b.hasAsk {
		return 0, false
	}
	bid, ask := 0.0, 1.0
	if b.hasBid {
		bid = b.bid
	}
	if b.hasAsk {
		ask = b.ask
	}
	return libs.R3((bid + ask) / 2), true
}

// bestLevel picks the best price from a set of levels: the highest bid, or the
// lowest ask. Connector levels arrive in arbitrary order, so this scans rather
// than trusting position. ok is false when no level carries a usable price.
func bestLevel(levels []connector.Level, wantMin bool) (float64, bool) {
	best := 0.0
	found := false
	for _, l := range levels {
		p, ok := parsePrice(l.Price)
		if !ok {
			continue
		}
		if !found || (wantMin && p < best) || (!wantMin && p > best) {
			best, found = p, true
		}
	}
	return best, found
}

// parsePrice parses a decimal price string. An empty or malformed string is
// reported as unknown rather than silently treated as zero.
func parsePrice(s string) (float64, bool) {
	if strings.TrimSpace(s) == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ── Official (Gamma) resolution ─────────────────────────────

const (
	// officialLookupAttempts / officialLookupTimeout bound the post-settlement
	// GetResolution call so a hung HTTP request cannot stall the synthetic
	// resolve (which runs on the close-time timer and at shutdown).
	officialLookupAttempts = 3
	officialLookupTimeout  = 2 * time.Second
)

// officialResolution asks the venue for the market's published resolution via
// the connector's GetResolution (Gamma-backed). Each attempt is bounded, and an
// error or timeout is retried; a nil result means "not resolved yet" and is
// returned immediately. ok is false whenever no usable resolution was obtained.
func (s *RecordingSession) officialResolution() (connector.Resolution, bool) {
	checker := s.resolutionChecker()
	if checker == nil {
		return "", false
	}
	for attempt := 0; attempt < officialLookupAttempts; attempt++ {
		res, err, completed := resolutionWithTimeout(func() (*connector.Resolution, error) {
			return checker.GetResolution(s.cfg.MarketID)
		}, officialLookupTimeout)
		if !completed {
			continue // timed out — retry
		}
		if err != nil {
			slog.Warn("collector: official resolution lookup failed",
				"market", s.cfg.MarketID, "attempt", attempt+1, "err", err)
			continue
		}
		if res == nil || *res == "" {
			return "", false // not resolved (yet) — no point retrying
		}
		return *res, true
	}
	return "", false
}

// resolutionWithTimeout runs fn in a goroutine and waits up to d for it. It
// returns completed=false on timeout; the goroutine and its buffered channel
// are abandoned but cannot block (the send never blocks on a buffered channel).
func resolutionWithTimeout(fn func() (*connector.Resolution, error), d time.Duration) (*connector.Resolution, error, bool) {
	type result struct {
		res *connector.Resolution
		err error
	}
	ch := make(chan result, 1)
	go func() {
		r, e := fn()
		ch <- result{r, e}
	}()
	select {
	case r := <-ch:
		return r.res, r.err, true
	case <-time.After(d):
		return nil, nil, false
	}
}

// resolutionChecker exposes the collector's ResolutionChecker (the connector),
// or nil when it is absent — as in standalone tests.
func (s *RecordingSession) resolutionChecker() ResolutionChecker {
	if s.collector == nil {
		return nil
	}
	return s.collector.resolutionChecker()
}
