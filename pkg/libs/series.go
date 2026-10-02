// Package libs — market series registry.
//
// The collector no longer synthesizes market slugs. A series is identified by
// (asset, family, interval), and resolved at runtime through Gamma:
//
//	series slug ──/series?slug=──▶ series.id
//	            ──/events?series_id=<id>&closed=false──▶ events (with nested markets)
//
// so the registry below only has to hold the *stable* facts about a family:
// its Gamma series slug, how its markets settle, and roughly how many markets
// an event carries. See CONTEXT.md §3.
//
// Slug synthesis was deleted because it silently targeted the wrong market:
// GenerateMarketSlug built the hourly slug from the UTC hour while Polymarket
// labels hourly markets by the ET hour. Verified 2026-10-02 05:19Z: it produced
// "bitcoin-up-or-down-october-2-2026-5am-et" (window 09:00–10:00Z) instead of
// the live "bitcoin-up-or-down-october-2-2026-1am-et" (05:00–06:00Z).
package libs

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// MarketFamily is the kind of market a series records.
type MarketFamily string

const (
	// FamilyUpDown: "Up or Down" — one market per event, two outcomes.
	FamilyUpDown MarketFamily = "updown"
	// FamilyAbove: "Bitcoin above ___ on <date>?" — a strike ladder
	// (1 event = N markets), one rung per strike.
	FamilyAbove MarketFamily = "above"
	// FamilyRange: "Bitcoin price on <date>?" — a price-range ladder
	// (1 event = N markets), neg-risk.
	FamilyRange MarketFamily = "range"
	// FamilyHit: "What price will Bitcoin hit on <date>?" — a strike ladder
	// (1 event = N markets) observed across a whole ET calendar day.
	FamilyHit MarketFamily = "hit"
)

// IsLadder reports whether the family puts several markets inside one event
// (a strike or range ladder), which means the event must fan out to N
// recording sessions rather than one.
func (f MarketFamily) IsLadder() bool {
	switch f {
	case FamilyAbove, FamilyRange, FamilyHit:
		return true
	default:
		return false
	}
}

// SettleSource is the rule a market resolves on, as stated in its Gamma
// description. Stored verbatim in per-market metadata so a replay can recompute
// the outcome from the recorded feeds (CONTEXT.md §6).
type SettleSource string

const (
	// SettleBinanceCandle: the Binance BTC/USDT candle matching the series
	// interval settles the market (close compared against open). Intraday
	// up/down families.
	SettleBinanceCandle SettleSource = "binance_candle"
	// SettleBinance1mClose: the final Close of the Binance BTC/USDT 1-minute
	// candle at 12:00 ET on the title date. up/down-daily, above, range.
	SettleBinance1mClose SettleSource = "binance_1m_close"
	// SettleBinance1mHigh: any Binance BTC/USDT 1-minute candle High within the
	// observed ET day reaches the strike. Hit price.
	SettleBinance1mHigh SettleSource = "binance_1m_high"
	// SettleChainlinkTWAP: Chainlink-generated TWAP over the window compared
	// against the price at the window start. up/down 4h.
	SettleChainlinkTWAP SettleSource = "chainlink_twap"
)

// SeriesKey identifies a series independently of its Gamma slug.
type SeriesKey struct {
	Asset    string        // upper-case, e.g. "BTC"
	Family   MarketFamily  // updown | above | range | hit
	Interval time.Duration // market window length (24h for the daily ladders)
}

// SeriesSpec is the registry entry for one series.
type SeriesSpec struct {
	Key SeriesKey

	// Name is the config shorthand, e.g. "btc_1h", "btc_above".
	Name string

	// GammaSeries is the Gamma series slug used for discovery
	// (GET /series?slug=<GammaSeries>).
	GammaSeries string

	// Anchor is how the settlement instant is derived from the ET date in the
	// market title.
	Anchor EpochAnchor

	// Source is the resolution rule (see SettleSource).
	Source SettleSource

	// MarketsPerEventHint is the expected ladder size. It is a *hint* only — the
	// authoritative count always comes from Gamma, and ladders are re-struck
	// weekly so the size can change.
	MarketsPerEventHint int

	// Ladder is true when one event contains several markets (a strike or range
	// ladder) and therefore fans out to N recording sessions.
	Ladder bool

	// Verified is true when this row was checked against a live Gamma event
	// description. Rows that are not verified should not be enabled.
	Verified bool

	// Note carries the verified evidence / caveats.
	Note string
}

// Name returns the config shorthand for a key (e.g. "btc_1h", "btc_above").
func (k SeriesKey) Name() string {
	asset := strings.ToLower(k.Asset)
	if k.Family != FamilyUpDown {
		return fmt.Sprintf("%s_%s", asset, k.Family)
	}
	return fmt.Sprintf("%s_%s", asset, IntervalLabel(k.Interval))
}

// IntervalLabel renders a duration as the short label used in config and paths.
func IntervalLabel(d time.Duration) string {
	switch d {
	case 5 * time.Minute:
		return "5m"
	case 15 * time.Minute:
		return "15m"
	case time.Hour:
		return "1h"
	case 4 * time.Hour:
		return "4h"
	case 24 * time.Hour:
		return "1d"
	default:
		return strings.TrimSuffix(d.String(), "0s")
	}
}

// ParseInterval parses an interval label ("5m", "15m", "1h", "4h", "1d").
func ParseInterval(s string) (time.Duration, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "5m", "5min":
		return 5 * time.Minute, nil
	case "15m", "15min":
		return 15 * time.Minute, nil
	case "1h", "hourly":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d", "24h", "daily":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("libs: unknown interval %q (want 5m, 15m, 1h, 4h or 1d)", s)
	}
}

// ParseSeriesName parses a config shorthand into a SeriesKey.
//
//	"btc_1h"    → {BTC, updown, 1h}
//	"btc_above" → {BTC, above,  24h}
func ParseSeriesName(name string) (SeriesKey, error) {
	parts := strings.SplitN(strings.ToLower(strings.TrimSpace(name)), "_", 2)
	if len(parts) != 2 {
		return SeriesKey{}, fmt.Errorf("libs: malformed series name %q (want e.g. btc_1h, btc_above)", name)
	}
	asset := strings.ToUpper(parts[0])
	tail := parts[1]

	switch MarketFamily(tail) {
	case FamilyAbove, FamilyRange, FamilyHit:
		return SeriesKey{Asset: asset, Family: MarketFamily(tail), Interval: 24 * time.Hour}, nil
	}

	interval, err := ParseInterval(tail)
	if err != nil {
		return SeriesKey{}, fmt.Errorf("libs: malformed series name %q: %w", name, err)
	}
	return SeriesKey{Asset: asset, Family: FamilyUpDown, Interval: interval}, nil
}

// seriesRegistry holds the verified BTC series. Add assets/families here only
// after checking a live Gamma event description — guessing a family slug makes
// the collector record the wrong market silently.
var seriesRegistry = map[SeriesKey]SeriesSpec{
	// ── Up/Down, intraday ────────────────────────────────────────
	{Asset: "BTC", Family: FamilyUpDown, Interval: 5 * time.Minute}: {
		Name: "btc_5m", GammaSeries: "btc-up-or-down-5m",
		Anchor: AnchorETBoundary, Source: SettleBinanceCandle,
		MarketsPerEventHint: 1, Verified: false,
		Note: "slug shape verified on Gamma; resolution description not yet checked.",
	},
	{Asset: "BTC", Family: FamilyUpDown, Interval: 15 * time.Minute}: {
		Name: "btc_15m", GammaSeries: "btc-up-or-down-15m",
		Anchor: AnchorETBoundary, Source: SettleBinanceCandle,
		MarketsPerEventHint: 1, Verified: false,
		Note: "slug shape verified on Gamma; resolution description not yet checked.",
	},
	{Asset: "BTC", Family: FamilyUpDown, Interval: time.Hour}: {
		Name: "btc_1h", GammaSeries: "btc-up-or-down-hourly",
		Anchor: AnchorETBoundary, Source: SettleBinanceCandle,
		MarketsPerEventHint: 1, Verified: true,
		Note: `"resolve to Up if the close price is greater than or equal to the open price for the BTC/USDT 1 hour candle that begins on the time and date specified in the title"`,
	},

	// ── Up/Down, 4h — the only Chainlink-settled family ──────────
	{Asset: "BTC", Family: FamilyUpDown, Interval: 4 * time.Hour}: {
		Name: "btc_4h", GammaSeries: "btc-up-or-down-4h",
		Anchor: AnchorETBoundary, Source: SettleChainlinkTWAP,
		MarketsPerEventHint: 1, Verified: true,
		Note: `"resolve to Up if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range"; event slug btc-updown-4h-<unixETboundary>`,
	},

	// ── Up/Down, daily ───────────────────────────────────────────
	{Asset: "BTC", Family: FamilyUpDown, Interval: 24 * time.Hour}: {
		Name: "btc_1d", GammaSeries: "btc-up-or-down-daily",
		Anchor: AnchorNoonET, Source: SettleBinance1mClose,
		MarketsPerEventHint: 1, Verified: true,
		Note: `compares the Binance 1m candle closing at 12:00 ET on the previous day against the one closing at 12:00 ET on the title date; event slug bitcoin-up-or-down-on-<month>-<d>-<year>`,
	},

	// ── Ladders (one event = N markets) ──────────────────────────
	{Asset: "BTC", Family: FamilyAbove, Interval: 24 * time.Hour}: {
		Name: "btc_above", GammaSeries: "btc-multi-strikes-weekly",
		Anchor: AnchorNoonET, Source: SettleBinance1mClose,
		MarketsPerEventHint: 11, Ladder: true, Verified: true,
		Note: "11 strike markets per event (74,000..94,000 on 2026-10-02); 7 dated events open concurrently; strikes re-struck weekly; event slug bitcoin-above-on-<month>-<d>-<year>; trading window is ~7 days but settlement is the 12:00 ET candle on the title date",
	},
	{Asset: "BTC", Family: FamilyRange, Interval: 24 * time.Hour}: {
		Name: "btc_range", GammaSeries: "bitcoin-neg-risk-weekly",
		Anchor: AnchorNoonET, Source: SettleBinance1mClose,
		MarketsPerEventHint: 11, Ladder: true, Verified: true,
		Note: "11 range markets per event, neg-risk; event slug bitcoin-price-on-<month>-<d>-<year>; same weekly+dated pattern as above/below",
	},
	{Asset: "BTC", Family: FamilyHit, Interval: 24 * time.Hour}: {
		Name: "btc_hit", GammaSeries: "bitcoin-hit-price-daily",
		Anchor: AnchorMidnightET, Source: SettleBinance1mHigh,
		MarketsPerEventHint: 16, Ladder: true, Verified: true,
		Note: `"immediately resolve to Yes if any Binance 1-minute candle for Bitcoin (BTC/USDT) on the date specified in the title, between 12:00 AM ET and 11:59 PM ET has a final High price equal to or greater than the price specified"; event slug what-price-will-bitcoin-hit-on-<month>-<d>-<year>; NOTE: settles at midnight ET, so its epoch id is the day AFTER the title date`,
	},
}

// init keeps each spec's Key in sync with the map key it is registered under,
// so callers can rely on spec.Key being populated (Name()/IntervalLabel()
// depend on it). Adding a registry row therefore only needs the key + slugs.
func init() {
	keys := make([]SeriesKey, 0, len(seriesRegistry))
	for key := range seriesRegistry {
		keys = append(keys, key)
	}
	for _, key := range keys {
		spec := seriesRegistry[key]
		spec.Key = key
		seriesRegistry[key] = spec
	}
}

// TargetSettle returns the settlement instant of the market of this series
// that is live at now — the instant discovery matches an event's EndDate on.
//
//	AnchorETBoundary (5m/15m/1h/4h) → end of the ET-aligned window containing now
//	AnchorNoonET / AnchorMidnightET → end of the epoch containing now
//
// Nothing here truncates a UTC time: the ET grid is applied through
// IntervalWindow / EpochBounds, so the result is correct across a DST change.
func (s SeriesSpec) TargetSettle(now time.Time) (time.Time, error) {
	if s.Anchor == AnchorETBoundary {
		if s.Key.Interval <= 0 {
			return time.Time{}, fmt.Errorf("libs: series %s has no interval", s.Name)
		}
		_, end := IntervalWindow(now, s.Key.Interval)
		return end, nil
	}
	id, err := EpochID(now, s.Anchor)
	if err != nil {
		return time.Time{}, err
	}
	_, end, err := EpochBounds(id, s.Anchor)
	return end, err
}

// EpochIDFor returns the recording epoch a market of this series is filed
// under: the ET calendar date named in its title.
//
//	up/down 1h, window [13:00, 14:00) ET Oct 2   → "2026-10-02" (window date)
//	up/down 1h, window [23:00 Oct 2, 00:00 Oct 3) → "2026-10-02" (window date)
//	above ...on-october-2-2026, settles noon Oct 2 → "2026-10-02" (settle date)
//
// The two families name themselves differently — daily/ladder markets after the
// date they settle on, intraday markets after the date their window opens —
// which is why the instant is shifted back a nanosecond before taking its ET
// date in the daily case: the settle instant *is* the anchor boundary and
// belongs to the day it ends.
//
// NOTE: this is the market's *title* date, which is not necessarily the epoch
// directory it is recorded in — see EpochForSettle, which is what the
// scheduler files by.
func (s SeriesSpec) EpochIDFor(settleAt time.Time) string {
	at := settleAt.Add(-time.Nanosecond)
	if s.Anchor == AnchorETBoundary && s.Key.Interval > 0 {
		start, _ := IntervalWindow(at, s.Key.Interval)
		at = start
	}
	return at.In(EpochLocation).Format(EpochDateLayout)
}

// EpochForSettle returns the epoch a market settling at `at` is recorded in.
//
// It is the epoch that ends at `at` when `at` is an anchor instant (the daily
// and ladder families all settle at noon ET), and the epoch containing `at`
// otherwise (the intraday families settle at the end of their own window). In
// both cases the shift is what makes it well defined: an instant sitting exactly
// on a boundary belongs to the epoch it ends, not the one it starts.
//
// This is what makes the epoch directory equal to one settlement cycle, and it
// also makes an epoch's manifest complete the moment the epoch rolls: every
// market claimed while epoch D was running is filed under D.
//
//	noon ET anchor, settle 12:00 ET Oct 2  → "2026-10-02" (epoch ends there)
//	noon ET anchor, settle 15:00 ET Oct 2  → "2026-10-03" (epoch [noon Oct 2, …))
//	noon ET anchor, settle 00:00 ET Oct 3  → "2026-10-03"
//	noon ET anchor, settle 11:00 ET Oct 2  → "2026-10-02"
func EpochForSettle(at time.Time, a EpochAnchor) (string, error) {
	return EpochID(at.Add(-time.Nanosecond), a)
}

// SettleDateET is the ET calendar date the settlement instant falls on: the
// date a market title names. It differs from the recording epoch for intraday
// markets that settle after the epoch boundary — a market titled "october-2"
// whose window runs 14:00–15:00 ET settles on the 2nd but is recorded during,
// and filed under, the epoch that closes at noon on the 3rd.
func SettleDateET(at time.Time) string {
	return at.In(EpochLocation).Format(EpochDateLayout)
}

// LookupSeries resolves a key to its spec.
func LookupSeries(key SeriesKey) (SeriesSpec, error) {
	if spec, ok := seriesRegistry[key]; ok {
		return spec, nil
	}
	return SeriesSpec{}, fmt.Errorf("libs: unknown series %s (%s/%s/%s); known: %s",
		key.Name(), key.Asset, key.Family, IntervalLabel(key.Interval), strings.Join(KnownSeriesNames(), ", "))
}

// LookupSeriesByName resolves a config shorthand (e.g. "btc_1h") to its spec.
func LookupSeriesByName(name string) (SeriesSpec, error) {
	key, err := ParseSeriesName(name)
	if err != nil {
		return SeriesSpec{}, err
	}
	return LookupSeries(key)
}

// KnownSeriesNames lists the registered config shorthands, sorted.
func KnownSeriesNames() []string {
	names := make([]string, 0, len(seriesRegistry))
	for _, spec := range seriesRegistry {
		names = append(names, spec.Name)
	}
	sort.Strings(names)
	return names
}

// AllSeriesSpecs returns every registered spec, sorted by name.
func AllSeriesSpecs() []SeriesSpec {
	specs := make([]SeriesSpec, 0, len(seriesRegistry))
	for _, spec := range seriesRegistry {
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

// DefaultAnchor returns the epoch anchor implied by a set of series: the daily
// noon-ET rule unless every daily family in the set is midnight-anchored.
//
// The collector records one epoch grid for all families, so the anchor must be
// a single choice; this helper documents the natural default.
func DefaultAnchor(specs []SeriesSpec) EpochAnchor {
	noon, midnight := 0, 0
	for _, s := range specs {
		switch s.Anchor {
		case AnchorNoonET:
			noon++
		case AnchorMidnightET:
			midnight++
		}
	}
	if noon == 0 && midnight > 0 {
		return AnchorMidnightET
	}
	return AnchorNoonET
}
