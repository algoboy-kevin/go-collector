package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	"github.com/algoboy-kevin/go-collector/pkg/services/runtime"
	"github.com/algoboy-kevin/go-des/pkg/abstract"
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

	cfg  CollectorConfig
	rt   *runtime.RuntimeManager     // for cron routine scheduling
	conn connector.ExchangeConnector // for WS subscription management

	// Continuous feed recorders (RTDS / Binance), keyed by feedKey.
	feedRecorders map[string]*FeedRecorder

	// feedHealth tracks the previous stale state per feed (monitor goroutine).
	feedHealth map[string]bool

	connEvents   []ConnectionEvent
	connEventsMu sync.Mutex
}

// NewCollector creates a new EventCollector with the given RuntimeManager
// and PolymarketConnector. The RuntimeManager is used for cron scheduling;
// the connector is used to subscribe/unsubscribe asset IDs on the WS.
func NewCollector(cfg CollectorConfig, rt *runtime.RuntimeManager, conn connector.ExchangeConnector) *EventCollector {
	if cfg.RecordingDir == "" {
		cfg.RecordingDir = DefaultRecordingDir
	}
	return &EventCollector{
		sessions:      make(map[string]*RecordingSession),
		assetMap:      make(map[string]string),
		feedRecorders: make(map[string]*FeedRecorder),
		feedHealth:    make(map[string]bool),
		cfg:           cfg,
		rt:            rt,
		conn:          conn,
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
// from the Polymarket Gamma API and starts a session.
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

	rawMarket, _ := json.Marshal(gamma)

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
		"raw_market_size", len(rawMarket),
	)

	return sess, nil
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
	}

	typ := eventType(ev)
	assetID := eventAssetID(ev)

	// Determine the market ID for routing.
	var marketID string

	c.mu.RLock()
	if assetID != "" {
		marketID = c.assetMap[assetID]
	} else if resolved, ok := ev.(*connector.MarketResolvedEvent); ok {
		marketID = resolved.MarketID
	}
	sess, hasSession := c.sessions[marketID]
	c.mu.RUnlock()

	if !hasSession || sess == nil {
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
	if _, ok := ev.(*connector.MarketResolvedEvent); ok {
		resolved := ev.(*connector.MarketResolvedEvent)
		if err := sess.Resolve(resolved.WinningAssetID); err != nil {
			slog.Error("collector: resolve failed", "market", marketID, "err", err)
			return
		}
		if err := sess.Finalize(); err != nil {
			slog.Error("collector: finalize failed", "market", marketID, "err", err)
		}
		// Note: sess.onFinalized (set in StartSession) handles WS unsubscribe.
	}
}

// GetSession returns the recording session for a market ID, or nil.
func (c *EventCollector) GetSession(marketID string) *RecordingSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessions[marketID]
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

	for _, sess := range sessions {
		switch sess.State() {
		case SessionRecording:
			// Ctrl+C — apply synthetic resolve using last midprice for
			// any session still recording (regardless of whether the
			// market end time has passed or not).
			slog.Info("collector: force-resolving session (mid-session stop)",
				"market", sess.cfg.MarketID,
				"end_time", sess.cfg.MarketEndTime.Format("15:04:05"),
			)
			sess.handleSyntheticResolve()
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

// cryptoPriceFetcher is implemented by connectors that expose Polymarket's
// crypto-price API (open/close price over a market window).
type cryptoPriceFetcher interface {
	GetCryptoPrice(req connector.CryptoPriceRequest) (*connector.CryptoPrice, error)
}

// cryptoPriceTWAPLookback is the TWAP lookback window (seconds) used when
// fetching a window's open/close price, so the recorded prices match
// Polymarket's displayed TWAP reference price.
const cryptoPriceTWAPLookback = 60

// fetchWindowPrices fetches the underlying asset's open and close price for a
// session's market window via Polymarket's crypto-price API. Called at
// finalize so both prices are settled together from a single API call.
//
// A completed window (one that ran to its end time) records both the open and
// close price: the API's series spans the full window and its last point is
// the close. A market that ended midway (before its end time, e.g. an early
// resolution or a mid-session stop) has no settled close, so only the open
// price is recorded.
func (c *EventCollector) fetchWindowPrices(sess *RecordingSession) {
	if sess.series == "" {
		return // standalone recording — no crypto config
	}
	fetcher, ok := c.conn.(cryptoPriceFetcher)
	if !ok || fetcher == nil {
		return
	}
	cfg, ok := libs.SERIES_CRYPTO_CONFIG[sess.series]
	if !ok {
		slog.Warn("collector: no crypto config for series", "series", sess.series)
		return
	}
	cp, err := fetcher.GetCryptoPrice(connector.CryptoPriceRequest{
		Symbol:              cfg.Symbol,
		Variant:             cfg.Variant,
		EventStartTime:      sess.cfg.MarketStartTime,
		EndDate:             sess.cfg.MarketEndTime,
		TWAPEnabled:         true,
		TWAPLookbackSeconds: cryptoPriceTWAPLookback,
	})
	if err != nil {
		slog.Warn("collector: fetch window prices failed",
			"series", sess.series, "slug", sess.cfg.Slug, "err", err)
		return
	}
	sess.SetOpenPrice(cp.OpenPrice)
	closeVal := 0.0
	if cp.ClosePrice != nil {
		closeVal = *cp.ClosePrice
		sess.SetClosePrice(closeVal)
	}
	slog.Info("collector: window prices (finalize)",
		"series", sess.series,
		"slug", sess.cfg.Slug,
		"open", cp.OpenPrice,
		"close", closeVal,
	)
}

// StartSeries activates a rolling market series and begins continuous
// recording on the given RuntimeManager.  It starts a session for the
// current interval immediately, then registers a cron routine that fires
// at each wall-clock-aligned interval boundary (e.g. every 5 min on the
// :00) to start the next session.
//
// If sc.Count > 0, the cron unregisters after that many intervals.
// If sc.Count == 0, it runs indefinitely.
//
// Visual: for btc_5m starting at 14:00:05
//
//	14:00:05  ── start session for [14:00, 14:05)
//	14:05:00  ── cron → start session for [14:05, 14:10)
//	14:10:00  ── cron → start session for [14:10, 14:15)
//	...
func (c *EventCollector) StartSeries(conn connector.ExchangeConnector, sc SeriesConfig) error {
	series := libs.RollingMarketSeries(sc.Series)
	intervalSec, err := libs.GetIntervalSeconds(series)
	if err != nil {
		return fmt.Errorf("invalid series %q: %w", sc.Series, err)
	}

	now := time.Now()
	interval := time.Duration(intervalSec) * time.Second

	// ── 1. Start current interval immediately ───────────────
	alignedStart := now.Truncate(interval)
	alignedEnd := alignedStart.Add(interval)

	slug := libs.GenerateMarketSlug(series, alignedStart.Unix())
	sess, err := c.StartSessionFromSlug(conn, slug, alignedStart, alignedEnd, 0)
	if err != nil {
		slog.Warn("collector: failed to start current interval",
			"slug", slug, "err", err)
	} else {
		sess.series = series
		slog.Info("collector: started current interval",
			"series", sc.Series,
			"slug", slug,
			"window", fmt.Sprintf("%s → %s", alignedStart.Format("15:04:05"), alignedEnd.Format("15:04:05")),
			"session", sess,
		)
	}

	// ── 2. Register a cron routine for future intervals ─────
	routineKey := abstract.RoutineKey(fmt.Sprintf("collector.series.%s", sc.Series))
	remaining := sc.Count - 1 // current already consumed

	// nextStart tracks the start time of the next interval.
	// We advance it by interval each tick rather than computing from
	// time.Now().Truncate(), which can produce wrong results near
	// boundary crossings due to scheduling jitter.
	nextStart := alignedEnd

	c.rt.Register(abstract.RoutineRegistration{
		Key:      routineKey,
		Schedule: abstract.NewCronSchedule(interval, 0),
		Priority: abstract.PrioritySystemRotation,
		Handler:  c.newIntervalHandler(conn, series, interval, sc.Series, sc.Count, &remaining, routineKey, &nextStart),
	})

	slog.Info("collector: registered cron for series",
		"series", sc.Series,
		"interval", interval,
		"count", sc.Count,
		"next_boundary", alignedEnd.Format("15:04:05"),
	)

	return nil
}

// newIntervalHandler returns the cron handler that starts a new recording
// session at each interval boundary and unregisters when the count limit
// is reached.
func (c *EventCollector) newIntervalHandler(
	conn connector.ExchangeConnector,
	series libs.RollingMarketSeries,
	interval time.Duration,
	seriesName string,
	count int,
	remaining *int,
	key abstract.RoutineKey,
	nextStart *time.Time,
) func() {
	return func() {
		start := *nextStart
		end := start.Add(interval)
		slug := libs.GenerateMarketSlug(series, start.Unix())

		sess, err := c.StartSessionFromSlug(conn, slug, start, end, 0)
		if err != nil {
			slog.Warn("collector: cron failed to start interval",
				"series", seriesName, "slug", slug, "err", err)
			return
		}

		sess.series = series
		slog.Info("collector: cron started new interval",
			"series", seriesName,
			"slug", slug,
			"window", fmt.Sprintf("%s → %s", start.Format("15:04:05"), end.Format("15:04:05")),
			"session", sess,
		)

		// Advance nextStart by one interval.  If the cron was delayed
		// (scheduling jitter), skip ahead to the current boundary so we
		// don't create overlapping sessions.
		*nextStart = end
		for nextStart.Before(time.Now()) {
			*nextStart = nextStart.Add(interval)
		}

		if count > 0 {
			*remaining--
			if *remaining <= 0 {
				c.rt.Unregister(key)
				slog.Info("collector: reached interval limit, unregistered cron",
					"series", seriesName, "count", count)
			}
		}
	}
}
