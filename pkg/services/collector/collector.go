package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// EventCollector manages multiple parallel RecordingSessions and routes
// incoming raw events from the PolymarketConnector's OnEvent hook to the
// correct session by asset ID or market ID.
//
// Usage:
//
//	collector := collector.NewCollector(collector.CollectorConfig{
//	    RecordingDir: "data/market",
//	})
//	connector.SetOnEvent(collector.HandleEvent)
//
//	sess, _ := collector.StartSession(ctx, connector, sessionConfig)
//	// ... wait for resolution ...
//	sess.Finalize()
type EventCollector struct {
	mu       sync.RWMutex
	sessions map[string]*RecordingSession // marketID → session
	assetMap map[string]string            // assetID → marketID
	// conditionMap and negRiskMap are the other two ways a resolution can name
	// a market: by its Gamma condition id, and by the shared neg-risk group id.
	// The market WS sends `id` and `market` (the condition id), and for a
	// neg-risk event that id can be the *group* id rather than the rung's market
	// id — without these maps such a rung would never resolve and would be
	// truncated at shutdown instead (CONTEXT.md §13.2).
	conditionMap map[string]string   // conditionID → marketID
	negRiskMap   map[string][]string // negRiskMarketID → marketIDs

	cfg  CollectorConfig
	conn connector.ExchangeConnector // for WS subscription management

	// Continuous feed recorders (RTDS / Binance), keyed by feedKey.
	feedRecorders map[string]*FeedRecorder

	// feedHealth tracks the previous stale state per feed (monitor goroutine).
	feedHealth map[string]bool

	// truncatedMarkets remembers markets this run ended before their settlement.
	// StopAll drops finalized sessions from the routing map, so the epoch
	// manifest asks here rather than scanning sessions that no longer exist.
	truncatedMarkets map[string]bool

	// spotBits carries the latest reference price of the underlying asset
	// (math.Float64bits), read by the ladder strike filter.
	spotBits atomic.Uint64

	connEvents   []ConnectionEvent
	connEventsMu sync.Mutex
}

// NewCollector creates a new EventCollector with the given connector. The
// connector is used to subscribe/unsubscribe asset IDs on the WS.
func NewCollector(cfg CollectorConfig, conn connector.ExchangeConnector) *EventCollector {
	if cfg.RecordingDir == "" {
		cfg.RecordingDir = DefaultRecordingDir
	}
	return &EventCollector{
		sessions:         make(map[string]*RecordingSession),
		assetMap:         make(map[string]string),
		conditionMap:     make(map[string]string),
		negRiskMap:       make(map[string][]string),
		feedRecorders:    make(map[string]*FeedRecorder),
		feedHealth:       make(map[string]bool),
		truncatedMarkets: make(map[string]bool),
		cfg:              cfg,
		conn:             conn,
	}
}

// recordingDir returns the configured recording directory (thread-safe).
func (c *EventCollector) recordingDir() string {
	return c.cfg.RecordingDir
}

// RecordConnectionEvent records a WebSocket connection status change.
func (c *EventCollector) RecordConnectionEvent(status string) {
	c.connEventsMu.Lock()
	defer c.connEventsMu.Unlock()
	c.connEvents = append(c.connEvents, ConnectionEvent{
		Status:    status,
		Timestamp: time.Now().UnixMilli(),
	})
	// Bound the log — finalized sessions have already written their stats, so
	// only the recent tail is needed for per-market gap classification.
	const maxConnEvents = 2048
	if len(c.connEvents) > maxConnEvents {
		c.connEvents = append([]ConnectionEvent(nil), c.connEvents[len(c.connEvents)-maxConnEvents:]...)
	}
	slog.Debug("collector: connection event", "status", status)
}

// GetConnectionEvents returns a copy of the connection event log.
func (c *EventCollector) GetConnectionEvents() []ConnectionEvent {
	c.connEventsMu.Lock()
	defer c.connEventsMu.Unlock()
	out := make([]ConnectionEvent, len(c.connEvents))
	copy(out, c.connEvents)
	return out
}

// StartSession creates a new recording session for a market.  It fetches
// market metadata via the connector's Gamma API, subscribes to the YES and
// NO asset IDs, and begins buffering events.
//
// The connector must be started and the OnEvent hook must be set before
// calling this method.
func (c *EventCollector) StartSession(
	conn connector.ExchangeConnector,
	cfg SessionConfig,
) (*RecordingSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.sessions[cfg.MarketID]; exists {
		return nil, fmt.Errorf("collector: session already exists for market %s", cfg.MarketID)
	}

	// Register asset → market mapping for event routing.
	c.assetMap[cfg.YesAssetID] = cfg.MarketID
	c.assetMap[cfg.NoAssetID] = cfg.MarketID
	if cfg.ConditionID != "" {
		c.conditionMap[cfg.ConditionID] = cfg.MarketID
	}
	if cfg.NegRiskMarketID != "" {
		c.negRiskMap[cfg.NegRiskMarketID] = append(c.negRiskMap[cfg.NegRiskMarketID], cfg.MarketID)
	}

	// Subscribe to both asset IDs on the WS.
	conn.Subscribe([]string{cfg.YesAssetID, cfg.NoAssetID})

	// Create and store the session.
	sess := newRecordingSession(cfg, c)
	sess.onFinalized = func() {
		c.unsubscribeSession(sess)
	}
	c.sessions[cfg.MarketID] = sess

	return sess, nil
}

// StartSessionFromSlug is a convenience method that fetches market metadata
// from the Polymarket Gamma API and starts a session. The epoch scheduler does
// not use it: discovery already returns the market's clobTokenIds, so the
// fan-out builds SessionConfigs from the Gamma payload with no extra request.
//
// It remains for standalone/ad-hoc recordings and tests.
func (c *EventCollector) StartSessionFromSlug(
	conn connector.ExchangeConnector,
	slug string,
	startTime time.Time,
	endTime time.Time,
	syntheticDelay time.Duration,
) (*RecordingSession, error) {
	// Fetch market info from Gamma API.
	gamma, err := conn.GetMarket("", slug)
	if err != nil {
		return nil, fmt.Errorf("collector: fetch market %s: %w", slug, err)
	}

	cfg := SessionConfig{
		MarketID:              gamma.ID,
		YesAssetID:            gamma.YesAssetID,
		NoAssetID:             gamma.NoAssetID,
		Slug:                  gamma.Slug,
		Question:              gamma.Question,
		ConditionID:           gamma.ConditionID,
		MarketStartTime:       startTime,
		MarketEndTime:         endTime,
		SyntheticResolveAfter: syntheticDelay,
	}

	sess, err := c.StartSession(conn, cfg)
	if err != nil {
		return nil, err
	}

	slog.Info("collector: session started from slug",
		"market", cfg.MarketID,
		"slug", slug,
		"yes_asset", cfg.YesAssetID,
		"no_asset", cfg.NoAssetID,
	)

	return sess, nil
}

// ── Reference price (ladder strike filter) ──────────────────

// SetSpotPrice records the latest reference price of the family's underlying
// asset. The ladder strike filter picks rungs around it (CONTEXT.md §7).
func (c *EventCollector) SetSpotPrice(price float64) {
	c.spotBits.Store(math.Float64bits(price))
}

// SpotPrice returns the latest reference price, or 0 if none has been seen.
func (c *EventCollector) SpotPrice() float64 {
	return math.Float64frombits(c.spotBits.Load())
}

// HandleEvent is the callback registered with PolymarketConnector.SetOnEvent.
// It serializes the event and routes it to the correct RecordingSession based
// on the event's asset ID or market ID.
//
// Safe to call from any goroutine (the connector's WS handlers).
func (c *EventCollector) HandleEvent(ev any) {
	// ── Continuous feed events (RTDS / Binance) ─────────────
	// These run independently of market rotation and route to the hourly
	// feed recorders instead of a market session.
	switch e := ev.(type) {
	case *connector.CryptoPriceEvent:
		if e.Source == "chainlink_twap" {
			c.routeFeed(FeedSourceChainlinkTWAP, "", e.Symbol, ev)
		} else {
			c.routeFeed(FeedSourceRTDS, "", e.Symbol, ev)
			// The Binance RTDS reference price is the settlement venue's own
			// price, so it is the right spot for the ladder strike filter.
			if e.Source == "binance" {
				if p := libs.ParseFloat(e.Price); p > 0 {
					c.SetSpotPrice(p)
				}
			}
		}
		return
	case *connector.BinanceBookTickerEvent:
		c.routeFeed(FeedSourceBinance, e.Market, e.Symbol, ev)
		return
	case *connector.BinanceAggTradeEvent:
		c.routeFeed(FeedSourceBinance, e.Market, e.Symbol, ev)
		return
	case *connector.BinanceDepthEvent:
		c.routeFeed(FeedSourceBinance, e.Market, e.Symbol, ev)
		return
	case *connector.BinanceKlineEvent:
		// Only final candles are persisted; FeedRecorder drops the rest, so a
		// route here is cheap even at one update per second per candle.
		c.routeFeed(FeedSourceBinance, e.Market, e.Symbol, ev)
		return
	}

	typ := eventType(ev)
	assetID := eventAssetID(ev)

	// Determine the market ID for routing.
	//
	// A resolution is the one event with no asset id of its own, so it is routed
	// by the winning token (an exact join — we subscribed to both sides), then by
	// its condition id, and only then by the market id the WS reported. The
	// last one is unreliable for a neg-risk ladder, where the payload can name
	// the group instead of the rung (CONTEXT.md §13.2).
	var marketID string
	resolved, isResolved := ev.(*connector.MarketResolvedEvent)
	if assetID == "" && isResolved {
		assetID = resolved.WinningAssetID
	}

	c.mu.RLock()
	if assetID != "" {
		marketID = c.assetMap[assetID]
	}
	if marketID == "" && isResolved {
		marketID = c.conditionMap[resolved.ConditionID]
	}
	if marketID == "" && isResolved {
		marketID = resolved.MarketID
	}
	sess, hasSession := c.sessions[marketID]
	groupHeld := false
	if !hasSession && isResolved {
		_, groupHeld = c.negRiskMap[resolved.MarketID]
	}
	c.mu.RUnlock()

	if !hasSession || sess == nil {
		if groupHeld {
			// The payload named a neg-risk group rather than the rung it resolved.
			// Every rung's assets were subscribed, so this is not something we can
			// disambiguate — say so loudly rather than dropping it silently, so a
			// range market settling never looks like a market that simply never
			// resolved.
			slog.Error("collector: resolution names a neg-risk group, not a rung — cannot route",
				"group", resolved.MarketID,
				"condition", resolved.ConditionID,
				"winning_asset", resolved.WinningAssetID,
				"winning_outcome", resolved.WinningOutcome)
			return
		}
		// No session for this event — silently drop.
		return
	}

	// Serialize the event data.
	data, err := json.Marshal(ev)
	if err != nil {
		slog.Warn("collector: failed to marshal event",
			"type", typ, "market", marketID, "err", err)
		return
	}

	// Determine timestamps.
	var ts, receivedAt int64
	switch e := ev.(type) {
	case *connector.PriceChangeEvent:
		ts = e.Timestamp.UnixMilli()
		receivedAt = e.ReceivedAt.UnixMilli()
	case *connector.BookSnapshotEvent:
		ts = e.Timestamp.UnixMilli()
		receivedAt = e.ReceivedAt.UnixMilli()
	case *connector.TradeEvent:
		ts = e.Timestamp.UnixMilli()
		receivedAt = e.ReceivedAt.UnixMilli()
	case *connector.TickChangeEvent:
		ts = e.Timestamp.UnixMilli()
		receivedAt = e.ReceivedAt.UnixMilli()
	case *connector.MarketResolvedEvent:
		ts = e.Timestamp.UnixMilli()
		receivedAt = e.ReceivedAt.UnixMilli()
	default:
		ts = time.Now().UnixMilli()
		receivedAt = ts
	}

	// Create the recorded event.
	// SeqID from the original event for ordered replay.
	var seqID int64
	switch e := ev.(type) {
	case *connector.PriceChangeEvent:
		seqID = e.SeqID
	case *connector.BookSnapshotEvent:
		seqID = e.SeqID
	case *connector.TradeEvent:
		seqID = e.SeqID
	case *connector.TickChangeEvent:
		seqID = e.SeqID
	case *connector.MarketResolvedEvent:
		seqID = e.SeqID
	}

	recorded := RecordedEvent{
		SeqID:      seqID,
		Type:       typ,
		Timestamp:  ts,
		ReceivedAt: receivedAt,
		Data:       data,
	}

	// Record into the session buffer.
	if !sess.Record(recorded) {
		slog.Warn("collector: session not in recording state, dropping event",
			"type", typ, "market", marketID, "state", sess.State())
	}

	// If this is a MarketResolvedEvent, trigger resolution.
	if isResolved {
		winning := winningToken(sess.cfg, resolved.WinningAssetID, resolved.WinningOutcome)
		if err := sess.Resolve(winning); err != nil {
			slog.Error("collector: resolve failed", "market", marketID, "err", err)
			return
		}
		if err := sess.Finalize(); err != nil {
			slog.Error("collector: finalize failed", "market", marketID, "err", err)
		}
		// Note: sess.onFinalized (set in StartSession) handles WS unsubscribe.
	}
}

// winningToken resolves the token that won a market from a resolution payload.
//
// The winning asset id is authoritative; the outcome label is the fallback for
// a payload that omits it, because Resolve records "YES" for anything that is
// not the NO token — an absent id would otherwise be written as a YES win.
// Only a payload with neither is left to that default, and that case is logged.
func winningToken(cfg SessionConfig, winningAssetID, winningOutcome string) string {
	if winningAssetID != "" {
		return winningAssetID
	}
	switch strings.ToLower(strings.TrimSpace(winningOutcome)) {
	case "no", "down":
		return cfg.NoAssetID
	case "yes", "up":
		return cfg.YesAssetID
	default:
		slog.Warn("collector: resolution carries neither a winning asset id nor a usable outcome label",
			"market", cfg.MarketID, "winning_outcome", winningOutcome)
		return ""
	}
}

// ── Resolved-market guard ───────────────────────────────────

// ResolutionChecker is the slice of the connector the fan-out needs to refuse
// claiming a market that has already resolved on-chain. Satisfied by the
// Polymarket connector; absent (or nil) in tests that do not care.
type ResolutionChecker interface {
	GetResolution(marketID string) (*connector.Resolution, error)
}

// resolutionCheckWindow is how close to its settle instant a market must be
// before the fan-out spends an HTTP round trip on GetResolution.
//
// Gamma's own `closed` flag and the settleAt-in-the-past check cover the normal
// case; the gap they leave is a restart in the last minutes before settlement,
// where a market can already be resolved on-chain while Gamma still reports it
// open. Checking every claim would put a network dependency in the claim path
// (11 extra calls per ladder event, on every restart) for no benefit, so the
// check is confined to the window where it can matter (CONTEXT.md §13.3).
const resolutionCheckWindow = 15 * time.Minute

// alreadyResolved reports whether a market about to be claimed has already
// resolved on-chain. It fails open: a lookup error returns false, because a
// transient Gamma failure must never stop us recording a live market.
func (c *EventCollector) alreadyResolved(marketID string, settleAt, now time.Time) bool {
	if settleAt.Sub(now) > resolutionCheckWindow {
		return false
	}
	checker, ok := c.conn.(ResolutionChecker)
	if !ok || checker == nil {
		return false
	}
	res, err := checker.GetResolution(marketID)
	if err != nil {
		slog.Warn("collector: resolution lookup failed, claiming anyway",
			"market", marketID, "err", err)
		return false
	}
	if res == nil || *res == "" {
		return false
	}
	slog.Warn("collector: market already resolved on-chain, not recording it",
		"market", marketID,
		"resolution", string(*res),
		"settle_at", settleAt.UTC().Format(time.RFC3339),
	)
	return true
}

// GetSession returns the recording session for a market ID, or nil.
func (c *EventCollector) GetSession(marketID string) *RecordingSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessions[marketID]
}

// TruncatedMarkets returns the markets the run ended before they settled, as a
// set keyed by market ID. Their recordings are partial and must never be read
// as a settled outcome.
func (c *EventCollector) TruncatedMarkets() map[string]bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]bool, len(c.truncatedMarkets))
	for id := range c.truncatedMarkets {
		out[id] = true
	}
	return out
}

// ActiveSessions returns the number of active (non-finalized) sessions.
func (c *EventCollector) ActiveSessions() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, s := range c.sessions {
		if s.State() != SessionFinalized {
			count++
		}
	}
	return count
}

// AddFeedRecorder registers a continuous feed recorder (RTDS or Binance).
func (c *EventCollector) AddFeedRecorder(fr *FeedRecorder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.feedRecorders == nil {
		c.feedRecorders = make(map[string]*FeedRecorder)
	}
	c.feedRecorders[feedKey(fr.source, fr.market, fr.symbol)] = fr
}

// routeFeed forwards a feed event to the matching recorder by source, market
// and symbol (case-insensitive). No-op when no recorder is registered.
func (c *EventCollector) routeFeed(source, market, symbol string, ev any) {
	c.mu.RLock()
	fr := c.feedRecorders[feedKey(source, market, symbol)]
	c.mu.RUnlock()
	if fr != nil {
		fr.Record(ev)
	}
}

// StopFeeds closes all feed recorders, flushing their final buckets and
// writing metadata.json.
func (c *EventCollector) StopFeeds() {
	c.mu.RLock()
	recs := make([]*FeedRecorder, 0, len(c.feedRecorders))
	for _, fr := range c.feedRecorders {
		recs = append(recs, fr)
	}
	c.mu.RUnlock()
	for _, fr := range recs {
		fr.Close()
	}
}

// feedHealthSummaryEvery is how many health ticks elapse before an Info
// summary of all feeds is logged (at 15s ticks this is every ~75s).
const feedHealthSummaryEvery = 5

// StartHealthMonitor periodically logs the liveness of every continuous feed
// (RTDS / binance) and warns when a feed goes stale — catching the silent
// disconnect seen in production where a WS stops delivering events without
// firing a status-change callback. Stops when ctx is cancelled.
func (c *EventCollector) StartHealthMonitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		tick := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				tick++
				c.checkFeedHealth(tick)
			}
		}
	}()
}

// checkFeedHealth snapshots each feed recorder and logs WARN on stale / INFO
// on recovery, plus a periodic summary so healthy feeds are visible too.
func (c *EventCollector) checkFeedHealth(tick int) {
	c.mu.RLock()
	recs := make([]*FeedRecorder, 0, len(c.feedRecorders))
	for _, fr := range c.feedRecorders {
		recs = append(recs, fr)
	}
	c.mu.RUnlock()
	if len(recs) == 0 {
		return
	}

	now := time.Now()
	for _, fr := range recs {
		h := fr.Health(now)
		key := feedKey(h.Source, h.Market, h.Symbol)
		stale := h.StaleMs >= feedStaleThresholdMs(h.Source)
		switch {
		case stale && !c.feedHealth[key]:
			slog.Warn("feed: went stale",
				"feed", h.Source, "symbol", h.Symbol, "market", h.Market,
				"last_event_age_s", h.StaleMs/1000, "events", h.EventCount, "drops", h.Drops)
		case stale:
			slog.Warn("feed: still stale",
				"feed", h.Source, "symbol", h.Symbol, "market", h.Market,
				"last_event_age_s", h.StaleMs/1000, "events", h.EventCount, "drops", h.Drops)
		case c.feedHealth[key]:
			slog.Info("feed: recovered",
				"feed", h.Source, "symbol", h.Symbol, "market", h.Market,
				"last_event_age_s", h.StaleMs/1000, "events", h.EventCount, "drops", h.Drops)
		}
		c.feedHealth[key] = stale
	}

	if tick%feedHealthSummaryEvery == 0 {
		var b strings.Builder
		for _, fr := range recs {
			h := fr.Health(now)
			fmt.Fprintf(&b, "%s/%s=%ds ", h.Source, h.Symbol, h.StaleMs/1000)
		}
		slog.Info("feed: health", "summary", strings.TrimSpace(b.String()))
	}
}

// feedStaleThresholdMs returns how old (ms) a feed's last event may be before
// the collector warns it is stale. RTDS reference prices arrive ~1/sec and
// binance trades/depth arrive ~10/sec, so a handful of missed updates is suspicious.
func feedStaleThresholdMs(source string) int64 {
	switch source {
	case FeedSourceRTDS, FeedSourceChainlinkTWAP:
		return 10_000
	case FeedSourceBinance:
		return 15_000
	default:
		return 10_000
	}
}

// RecordFeedConnection records a WebSocket connection status change for a feed
// source, routing to all matching feed recorders (binance is additionally
// filtered by market).
func (c *EventCollector) RecordFeedConnection(source, market, status string) {
	c.mu.RLock()
	var recs []*FeedRecorder
	for _, fr := range c.feedRecorders {
		if fr.source == source && (market == "" || fr.market == market) {
			recs = append(recs, fr)
		}
	}
	c.mu.RUnlock()
	for _, fr := range recs {
		fr.RecordConnectionEvent(status)
	}
}

// StopAll finalizes all active sessions, closes feed recorders, and
// unsubscribes from their assets.
func (c *EventCollector) StopAll() {
	c.mu.RLock()
	sessions := make([]*RecordingSession, 0, len(c.sessions))
	for _, sess := range c.sessions {
		sessions = append(sessions, sess)
	}
	c.mu.RUnlock()

	now := time.Now()
	for _, sess := range sessions {
		switch sess.State() {
		case SessionRecording:
			if sess.SettlementPassed(now) {
				// The market settled but its resolution event never arrived, so a
				// synthetic resolve from the last midprice is legitimate.
				slog.Info("collector: force-resolving settled session",
					"market", sess.cfg.MarketID,
					"settle_at", sess.SettleAt().UTC().Format(time.RFC3339),
				)
				sess.handleSyntheticResolve()
				break
			}
			// Stopping before settlement: the recording is partial. Mark it
			// unsettled and finalize — never synthesize an outcome for it.
			slog.Info("collector: truncating unfinished session (run stopping before settlement)",
				"market", sess.cfg.MarketID,
				"slug", sess.cfg.Slug,
				"settle_at", sess.SettleAt().UTC().Format(time.RFC3339),
			)
			sess.MarkTruncated()
			c.mu.Lock()
			c.truncatedMarkets[sess.cfg.MarketID] = true
			c.mu.Unlock()
			if err := sess.Finalize(); err != nil {
				slog.Error("collector: truncate-finalize failed", "market", sess.cfg.MarketID, "err", err)
			}
		case SessionResolved:
			slog.Info("collector: force-finalizing resolved session", "market", sess.cfg.MarketID)
			if err := sess.Finalize(); err != nil {
				slog.Error("collector: force-finalize failed", "market", sess.cfg.MarketID, "err", err)
			}
		}
		c.unsubscribeSession(sess)
	}

	// Close continuous feed recorders (flushes buckets + metadata).
	c.StopFeeds()
}

// unsubscribeSession removes a session's WS subscriptions AND drops it from
// the collector's routing maps, so a long-running collector doesn't accumulate
// one entry per finalized interval (the cause of unbounded RAM growth over
// time). Safe: finalized sessions receive no further events (Record returns
// false once FINALIZED) and ActiveSessions() only counts non-finalized.
func (c *EventCollector) unsubscribeSession(sess *RecordingSession) {
	c.mu.Lock()
	delete(c.sessions, sess.cfg.MarketID)
	delete(c.assetMap, sess.cfg.YesAssetID)
	delete(c.assetMap, sess.cfg.NoAssetID)
	if sess.cfg.ConditionID != "" {
		delete(c.conditionMap, sess.cfg.ConditionID)
	}
	if group := sess.cfg.NegRiskMarketID; group != "" {
		if held := c.negRiskMap[group]; len(held) > 1 {
			kept := held[:0]
			for _, id := range held {
				if id != sess.cfg.MarketID {
					kept = append(kept, id)
				}
			}
			c.negRiskMap[group] = kept
		} else {
			delete(c.negRiskMap, group)
		}
	}
	c.mu.Unlock()

	if c.conn == nil {
		return
	}
	assetIDs := []string{sess.cfg.YesAssetID, sess.cfg.NoAssetID}
	c.conn.Unsubscribe(assetIDs)
	slog.Debug("collector: unsubscribed from assets",
		"market", sess.cfg.MarketID,
		"yes_asset", sess.cfg.YesAssetID,
		"no_asset", sess.cfg.NoAssetID,
	)
}

// ── Rolling market series ───────────────────────────────────

// Discovery is driven by the epoch scheduler (scheduler.go): every market is
// *discovered* from Gamma by matching the event that settles at the epoch's
// target instant. The old StartSeries / newIntervalHandler pair synthesized
// market slugs and truncated time.Now() to the interval, which silently
// targeted the wrong market for every family except 5m/15m — the hourly slug
// was built from the UTC hour while Polymarket labels hourly markets by the ET
// hour. See CONTEXT.md §2/§3.
