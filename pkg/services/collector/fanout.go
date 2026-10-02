package collector

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// EventTarget is one Gamma event scheduled for recording: the series it belongs
// to, the event itself, and the instant every market inside it settles.
type EventTarget struct {
	Spec  libs.SeriesSpec
	Event connector.GammaEvent

	// SettleAt is the event's EndDate — the instant its markets settle.
	//
	// For the families we record this coincides with the end of the event's
	// [StartDate, EndDate) window, but the window is not the market window:
	// intraday events open ~2 days early and ladder events trade ~7 days, while
	// settlement is a single candle at the anchor. EndDate is the one field that
	// identifies the event unambiguously (CONTEXT.md §3).
	SettleAt time.Time

	// SeriesID is the Gamma series id the event was listed from, and Recurrence
	// its Gamma recurrence ("hourly", "weekly", ...).
	SeriesID   string
	Recurrence string

	// EpochID is the ET settlement date the markets are filed under, i.e. the
	// directory {recordingDir}/{EpochID}/{marketID}/.
	EpochID string

	// OffsetDays is how many settlement periods ahead of the running epoch the
	// event was picked up: 0 for the epoch's own settlement event, 1 for the
	// "+1" ladder event carried so the next epoch's ladder accumulates a full
	// pre-settlement history (CONTEXT.md §4).
	OffsetDays int

	// StrikeLimit keeps at most this many ladder rungs on each side of the spot
	// price (0 = keep every rung). Ignored by non-ladder families.
	StrikeLimit int
}

// StartedMarket is one market a fan-out claimed, in the shape the epoch
// manifest needs.
type StartedMarket struct {
	MarketID   string
	Slug       string
	Geometry   StrikeRange
	OffsetDays int
	Session    *RecordingSession
}

// manifestMarket renders the market as a manifest entry.
func (m StartedMarket) manifestMarket() ManifestMarket {
	return ManifestMarket{
		MarketID:    m.MarketID,
		Slug:        m.Slug,
		StrikeLabel: m.Geometry.Label,
		Strike:      m.Geometry.Strike,
		RangeLow:    m.Geometry.RangeLow,
		RangeHigh:   m.Geometry.RangeHigh,
		OffsetDays:  m.OffsetDays,
	}
}

// rungFilter describes how a ladder fan-out picked its rungs: the configured
// limit, what it centred on, and how many rungs of the event's full ladder it
// kept. It is stamped into every claimed market's metadata (see
// StrikeFilterInfo) so a reader can tell a spot-centred selection from the
// median-rung fallback the first claim of a run uses.
type rungFilter struct {
	limit int
	basis string
	spot  float64
	total int
}

// info renders the filter as the metadata block.
func (f rungFilter) info(kept int) *StrikeFilterInfo {
	return &StrikeFilterInfo{
		Limit: f.limit,
		Basis: f.basis,
		Spot:  f.spot,
		Kept:  kept,
		Total: f.total,
	}
}

// rung is one market of an event plus its parsed ladder geometry.
type rung struct {
	market   connector.GammaMarket
	geo      StrikeRange
	value    float64 // representative value used for ordering and filtering
	hasValue bool
}

// ladderRungs orders an event's markets by ladder position: strike ascending,
// range buckets by their representative value.
//
// The same ordering is used for the metadata's ladder_index, computed over the
// event's FULL market list rather than the filtered subset, so a rung keeps its
// index no matter which rungs the strike filter kept.
func ladderRungs(event connector.GammaEvent) []rung {
	rs := make([]rung, 0, len(event.Markets))
	for _, m := range event.Markets {
		r := rung{market: m}
		geo, err := ParseStrikeLabel(m.GroupItemTitle)
		if err != nil {
			// Keep the raw label — it is written verbatim — but the rung has no
			// value, so it cannot be ordered or strike-filtered.
			slog.Warn("collector: unparseable ladder label",
				"event", event.Slug, "market", m.ID, "label", m.GroupItemTitle, "err", err)
			geo = StrikeRange{Label: strings.TrimSpace(m.GroupItemTitle)}
		}
		r.geo = geo
		r.value, r.hasValue = rungValue(geo)
		rs = append(rs, r)
	}
	sort.SliceStable(rs, func(i, j int) bool {
		if rs[i].hasValue != rs[j].hasValue {
			return rs[i].hasValue // valued rungs first
		}
		if !rs[i].hasValue {
			return false // keep Gamma's order for unvalued rungs
		}
		return rs[i].value < rs[j].value
	})
	return rs
}

// rungValue is the scalar a rung is ordered and filtered by: the strike for a
// point rung, the midpoint of an explicit interval, otherwise the single bound
// the rung carries.
func rungValue(geo StrikeRange) (float64, bool) {
	switch {
	case geo.Strike != nil:
		return *geo.Strike, true
	case geo.RangeLow != nil && geo.RangeHigh != nil:
		return (*geo.RangeLow + *geo.RangeHigh) / 2, true
	case geo.RangeLow != nil:
		return *geo.RangeLow, true
	case geo.RangeHigh != nil:
		return *geo.RangeHigh, true
	default:
		return 0, false
	}
}

// ladderIndex returns the rung's index within the ladder, or -1 when the event
// holds a single market.
func ladderIndex(rs []rung, marketID string) int {
	if len(rs) < 2 {
		return -1
	}
	for i, r := range rs {
		if r.market.ID == marketID {
			return i
		}
	}
	return -1
}

// selectRungs keeps at most limit rungs on each side of spot. limit <= 0 keeps
// every rung. The returned rungFilter records what it centred on, which the
// caller stamps into the market metadata.
//
// When no reference price is known yet — the scheduler claims its first ladder
// immediately at startup, before the RTDS feed has delivered anything — the
// filter centres on the ladder's median rung instead of being dropped. The
// strikes are re-struck around spot weekly, so the middle rung is a good proxy:
// the filter is a volume control, never a correctness one, and silently
// ignoring it would record 11 rungs where the config asked for 6.
func selectRungs(rs []rung, limit int, spot float64) ([]rung, rungFilter) {
	total := len(rs)
	if limit <= 0 {
		return rs, rungFilter{limit: limit, basis: StrikeFilterAll, spot: spot, total: total}
	}
	valued := 0
	for _, r := range rs {
		if r.hasValue {
			valued++
		}
	}
	if dropped := len(rs) - valued; dropped > 0 {
		slog.Warn("collector: dropping rungs with unparseable labels from a filtered ladder",
			"dropped", dropped, "total", len(rs))
	}
	rs = rs[:valued]
	if len(rs) == 0 {
		return rs, rungFilter{limit: limit, basis: StrikeFilterSpot, spot: spot, total: total}
	}

	basis := StrikeFilterSpot
	if spot <= 0 {
		spot = rs[len(rs)/2].value
		basis = StrikeFilterMedian
		slog.Warn("collector: no spot price yet, filtering around the ladder's middle rung",
			"requested_per_side", limit, "middle", spot, "rungs", len(rs))
	}

	// i is the first rung at or above spot.
	i := sort.Search(len(rs), func(k int) bool { return rs[k].value >= spot })
	lo := max(0, i-limit)
	hi := min(len(rs), i+limit)
	return rs[lo:hi], rungFilter{limit: limit, basis: basis, spot: spot, total: total}
}

// StartEventSessions claims a recording session for every market of a
// discovered event — one session for an up/down event, one per (filtered) rung
// for a ladder — and returns the markets it started.
//
// It is idempotent: a market that already has a session is skipped, so calling
// it once per reconcile tick never double-subscribes.
//
// The second result is how many markets were refused because they had already
// resolved on-chain (see alreadyResolved). A caller that gets no sessions back
// but a non-zero refusal count should treat the target as done rather than
// retry it forever.
func (c *EventCollector) StartEventSessions(t EventTarget) ([]StartedMarket, int, error) {
	if len(t.Event.Markets) == 0 {
		return nil, 0, fmt.Errorf("collector: event %s has no markets", t.Event.Slug)
	}
	now := time.Now()
	if !t.SettleAt.After(now) {
		// Never claim a market that has already settled: its events are over and
		// a session would only record the resolution.
		return nil, 0, fmt.Errorf("collector: event %s settled at %s, not claiming it",
			t.Event.Slug, t.SettleAt.UTC().Format(time.RFC3339))
	}

	venue, err := venueFor(t.Spec)
	if err != nil {
		return nil, 0, err
	}
	// libs.EpochID of the settle instant returns the bucket that contains it,
	// for daily and intraday anchors alike.
	bucketEpoch := libs.EpochIDAt(t.SettleAt, c.feedAnchor())
	derivedFrom := c.FeedRef(venue, bucketEpoch)

	// all is the event's full ladder, used for the recorded ladder geometry;
	// selected is what the strike filter actually keeps.
	all := ladderRungs(t.Event)
	filter := rungFilter{basis: StrikeFilterAll, total: len(all)}
	var selected []rung
	if !t.Spec.Ladder {
		if len(all) != 1 {
			return nil, 0, fmt.Errorf("collector: up/down event %s carries %d markets, want 1", t.Event.Slug, len(all))
		}
		selected = all
	} else {
		selected, filter = selectRungs(all, t.StrikeLimit, c.SpotPrice())
	}
	if len(selected) == 0 {
		return nil, 0, fmt.Errorf("collector: event %s: no rungs selected (strike filter %d)",
			t.Event.Slug, t.StrikeLimit)
	}

	started := make([]StartedMarket, 0, len(selected))
	skippedResolved := 0
	for _, r := range selected {
		m := r.market
		if c.GetSession(m.ID) != nil {
			continue // already recording this rung
		}
		if m.Closed {
			slog.Warn("collector: skipping closed market", "event", t.Event.Slug, "market", m.ID, "slug", m.Slug)
			continue
		}
		if c.alreadyResolved(m.ID, t.SettleAt, now) {
			skippedResolved++
			continue
		}
		yes, no, err := assetIDs(m)
		if err != nil {
			slog.Error("collector: market has no usable asset ids",
				"event", t.Event.Slug, "market", m.ID, "slug", m.Slug, "err", err)
			continue
		}

		cfg := SessionConfig{
			MarketID:        m.ID,
			YesAssetID:      yes,
			NoAssetID:       no,
			Slug:            m.Slug,
			Question:        m.Question,
			ConditionID:     m.ConditionID,
			NegRiskMarketID: m.NegRiskMarketID,
			MarketStartTime: now,
			MarketEndTime:   t.SettleAt,
		}
		ctx := MarketContext{
			EpochID:      t.EpochID,
			Family:       familyInfo(t),
			Event:        eventInfo(t, all, m, r.geo),
			Settlement:   settlementInfo(t, m, r.geo, venue, derivedFrom),
			StrikeFilter: filter.info(len(selected)),
		}

		sess, err := c.StartSession(c.conn, cfg)
		if err != nil {
			slog.Error("collector: failed to start session",
				"event", t.Event.Slug, "market", m.ID, "err", err)
			continue
		}
		sess.SetMarketContext(ctx)

		slog.Info("collector: market recording started",
			"market", m.ID,
			"slug", m.Slug,
			"family", t.Spec.Name,
			"epoch", t.EpochID,
			"settle_at", t.SettleAt.UTC().Format(time.RFC3339),
			"offset_days", t.OffsetDays,
			"ladder_index", ctx.Event.LadderIndex,
			"ladder_size", ctx.Event.LadderSize,
			"strike", r.geo.Label,
			"strike_filter", filter.basis,
			"derived_from", derivedFrom,
		)
		started = append(started, StartedMarket{
			MarketID:   m.ID,
			Slug:       m.Slug,
			Geometry:   r.geo,
			OffsetDays: t.OffsetDays,
			Session:    sess,
		})
	}
	return started, skippedResolved, nil
}

// familyInfo describes the series a target belongs to.
func familyInfo(t EventTarget) *FamilyInfo {
	return &FamilyInfo{
		Name:       string(t.Spec.Key.Family),
		Interval:   libs.IntervalLabel(t.Spec.Key.Interval),
		Asset:      t.Spec.Key.Asset,
		SeriesSlug: t.Spec.GammaSeries,
		SeriesID:   t.SeriesID,
		Recurrence: t.Recurrence,
	}
}

// eventInfo describes the Gamma event, including the rung geometry. ladderSize
// and ladderIndex are measured over the event's full market list (all), so a
// filtered fan-out still reports true ladder positions.
func eventInfo(t EventTarget, all []rung, m connector.GammaMarket, geo StrikeRange) *EventInfo {
	return &EventInfo{
		Slug:   t.Event.Slug,
		Ticker: t.Event.Ticker,
		Title:  t.Event.Title,
		// The date in the market title, which is not always the epoch directory
		// the recording is filed under (see libs.SettleDateET).
		SettleDateET:       libs.SettleDateET(t.SettleAt),
		TradingWindowStart: t.Event.StartDate.UnixMilli(),
		TradingWindowEnd:   t.Event.EndDate.UnixMilli(),
		LadderSize:         max(1, len(all)),
		LadderIndex:        ladderIndex(all, m.ID),
		Strike:             geo.Strike,
		StrikeLabel:        geo.Label,
		RangeLow:           geo.RangeLow,
		RangeHigh:          geo.RangeHigh,
		Outcomes:           outcomeList(m),
		NegRisk:            m.NegRisk,
		NegRiskMarketID:    m.NegRiskMarketID,
		OffsetDays:         t.OffsetDays,
		// Relative to the epoch the market was picked up in, not the one it is
		// filed under: a ladder event recorded from its settle epoch onwards is
		// picked up as the previous epoch's "+1" and only ever reaches us that
		// way (CONTEXT.md §4).
		IsSettlementEpoch: t.OffsetDays == 0,
	}
}

// settlementInfo records the anchor, the window the settlement compares, the
// resolution rule and the feed bucket the outcome can be recomputed from.
func settlementInfo(t EventTarget, m connector.GammaMarket, geo StrikeRange, v settleVenue, derivedFrom string) *SettlementInfo {
	info := &SettlementInfo{
		At:          t.SettleAt.UnixMilli(),
		Anchor:      string(t.Spec.Anchor),
		Source:      string(t.Spec.Source),
		Symbol:      v.symbol,
		Market:      v.market,
		Comparison:  ComparisonForSeries(t.Spec, geo),
		RuleText:    m.Description,
		DerivedFrom: derivedFrom,
	}
	// The window the settlement compares: the family's own interval back from
	// the anchor. For an up/down market that is the candle it is decided by; for
	// the daily ladders it is the previous noon, which is the candle the market
	// compares its own close against (CONTEXT.md §13.3).
	if iv := t.Spec.Key.Interval; iv > 0 {
		info.WindowStart = t.SettleAt.Add(-iv).UnixMilli()
	}
	return info
}

// outcomeList decodes a Gamma market's outcomes, tolerating malformed JSON.
func outcomeList(m connector.GammaMarket) []string {
	out, err := m.OutcomeList()
	if err != nil {
		return nil
	}
	return out
}

// assetIDs maps a Gamma market's outcomes onto the YES/NO asset ids the WS
// subscribes to.
//
// clobTokenIds is index-aligned with outcomes, and the first outcome is the
// "Up"/"Yes" side on every family we record — but we match on the label first
// and only fall back to position, so a reordered payload cannot silently swap
// the sides and record the mirror image of the market.
func assetIDs(m connector.GammaMarket) (yes, no string, err error) {
	outcomes, err := m.OutcomeList()
	if err != nil {
		return "", "", fmt.Errorf("outcomes: %w", err)
	}
	tokens, err := m.TokenIDList()
	if err != nil {
		return "", "", fmt.Errorf("clobTokenIds: %w", err)
	}
	if len(tokens) != len(outcomes) {
		return "", "", fmt.Errorf("%d outcomes but %d clob token ids", len(outcomes), len(tokens))
	}
	for i, o := range outcomes {
		switch strings.ToLower(strings.TrimSpace(o)) {
		case "yes", "up":
			yes = tokens[i]
		case "no", "down":
			no = tokens[i]
		}
	}
	if (yes == "" || no == "") && len(tokens) == 2 {
		yes, no = tokens[0], tokens[1]
	}
	if yes == "" || no == "" || yes == no {
		return "", "", fmt.Errorf("cannot resolve YES/NO assets from outcomes %v", outcomes)
	}
	return yes, no, nil
}

// ── Settlement venue ────────────────────────────────────────

// settleVenue is the feed that carries a family's settlement source.
type settleVenue struct {
	symbol string // BTCUSDT | btc/usd
	market string // spot | perp | "" (rtds)
	source string // FeedSourceBinance | FeedSourceChainlinkTWAP
}

// venueFor maps a series' resolution rule onto the feed that can reproduce it.
// The rule text names the venue ("the Binance 1 minute candle for BTC/USDT"),
// so this is a table lookup, not a guess.
func venueFor(spec libs.SeriesSpec) (settleVenue, error) {
	switch spec.Source {
	case libs.SettleBinanceCandle, libs.SettleBinance1mClose, libs.SettleBinance1mHigh:
		symbol, ok := binanceSymbol(spec.Key.Asset)
		if !ok {
			return settleVenue{}, fmt.Errorf("collector: no Binance symbol for asset %s", spec.Key.Asset)
		}
		return settleVenue{symbol: symbol, market: "spot", source: FeedSourceBinance}, nil
	case libs.SettleChainlinkTWAP:
		feed, ok := chainlinkFeed(spec.Key.Asset)
		if !ok {
			return settleVenue{}, fmt.Errorf("collector: no Chainlink feed for asset %s", spec.Key.Asset)
		}
		return settleVenue{symbol: feed, source: FeedSourceChainlinkTWAP}, nil
	default:
		return settleVenue{}, fmt.Errorf("collector: series %s has unknown settle source %q", spec.Name, spec.Source)
	}
}

// binanceSymbol maps a family asset onto its Binance spot symbol.
func binanceSymbol(asset string) (string, bool) {
	switch strings.ToUpper(asset) {
	case "BTC":
		return "BTCUSDT", true
	case "ETH":
		return "ETHUSDT", true
	case "SOL":
		return "SOLUSDT", true
	case "XRP":
		return "XRPUSDT", true
	default:
		return "", false
	}
}

// chainlinkFeed maps a family asset onto its Polymarket Chainlink feed id.
func chainlinkFeed(asset string) (string, bool) {
	switch strings.ToUpper(asset) {
	case "BTC":
		return "btc/usd", true
	case "ETH":
		return "eth/usd", true
	case "SOL":
		return "sol/usd", true
	case "XRP":
		return "xrp/usd", true
	default:
		return "", false
	}
}

// feedAnchor is the daily epoch rule the feed buckets are cut on, set on the
// collector at construction. The default is noon ET, the rule every
// settlement-bearing BTC family settles against.
func (c *EventCollector) feedAnchor() libs.EpochAnchor {
	if c.cfg.EpochAnchor.Valid() {
		return c.cfg.EpochAnchor
	}
	return libs.AnchorNoonET
}

// FeedRef returns the data-root-relative path of the feed bucket holding an
// epoch's data, e.g. "binance/spot/btcusdt_2026-10-03".
//
// It is built from the registered feed recorders, so it always names a file the
// run actually created. An empty result means the settlement source was not
// recorded at all, and the outcome cannot be recomputed from our own data —
// which is worth knowing, so it is left empty rather than invented.
func (c *EventCollector) FeedRef(v settleVenue, epochID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, fr := range c.feedRecorders {
		if fr.source != v.source || fr.market != v.market || !strings.EqualFold(fr.symbol, v.symbol) {
			continue
		}
		bucket := bucketPrefix(fr.source) + bucketSymbol(fr.symbol) + "_" + epochID
		return filepath.Join(filepath.Base(fr.root), fr.market, bucket)
	}
	return ""
}

// FeedRefs lists every registered feed bucket for an epoch, grouped by feed
// source ("binance", "rtds"), for the epoch manifest.
func (c *EventCollector) FeedRefs(epochID string) map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	refs := make(map[string][]string)
	for _, fr := range c.feedRecorders {
		bucket := bucketPrefix(fr.source) + bucketSymbol(fr.symbol) + "_" + epochID
		group, rel := "rtds", bucket
		if fr.source == FeedSourceBinance {
			group, rel = "binance", filepath.Join(fr.market, bucket)
		}
		refs[group] = append(refs[group], rel)
	}
	for _, list := range refs {
		sort.Strings(list)
	}
	return refs
}
