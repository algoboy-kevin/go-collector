// Package collector records raw Polymarket market events to disk for DES
// (Discrete Event Simulation) backtest replay.  Events are captured via the
// PolymarketConnector's OnEvent hook, buffered by RecordingSession, and
// flushed as gzip-compressed JSONL files on market resolution.
//
// Each market produces:
//
//	data/market/{marketID}/
//	    events.gz       ← gzip-compressed JSONL, sorted by SeqID
//	    metadata.json   ← market info, resolution, event count
//
// The recorded events are raw typed Go structs (PriceChangeEvent,
// BookSnapshotEvent, TradeEvent, TickChangeEvent, MarketResolvedEvent)
// serialized as JSON.  They include SeqID for deterministic ordering and
// ReceivedAt for latency measurement during replay.
package collector

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	"gopkg.in/yaml.v3"
)

// RecordedEvent is a single market event persisted to disk for DES replay.
// The Data field contains the raw JSON of the original Go event struct
// (e.g. *events.PriceChangeEvent, *events.BookSnapshotEvent).
type RecordedEvent struct {
	SeqID      int64           `json:"seq_id"`      // monotonic sequence for ordered replay
	Type       string          `json:"type"`        // event type discriminator
	Timestamp  int64           `json:"timestamp"`   // exchange timestamp (unix ms)
	ReceivedAt int64           `json:"received_at"` // local arrival (unix ms)
	Data       json.RawMessage `json:"data"`        // raw JSON of the original event
}

// SessionState tracks the lifecycle of a single market recording session.
type SessionState string

const (
	SessionRecording SessionState = "recording" // actively buffering events
	SessionResolved  SessionState = "resolved"  // resolution received, pending finalize
	SessionFinalized SessionState = "finalized" // written to disk, cleanup done
)

// SessionConfig configures a recording session for one market.
type SessionConfig struct {
	MarketID    string
	YesAssetID  string
	NoAssetID   string
	Slug        string
	Question    string
	ConditionID string

	// NegRiskMarketID is the shared group id of a neg-risk ladder market, used
	// only as a fallback routing key for a resolution event that names the group
	// rather than the rung. Empty for every non-neg-risk market.
	NegRiskMarketID string

	MarketStartTime time.Time
	MarketEndTime   time.Time

	// SyntheticResolveAfter is how long after MarketEndTime to wait before
	// applying a synthetic resolution based on last midprice.  Default: 60s.
	SyntheticResolveAfter time.Duration
}

// ConnectionEvent records a WebSocket connection status change.
type ConnectionEvent struct {
	Status    string `json:"status"`    // "connected" or "disconnected"
	Timestamp int64  `json:"timestamp"` // unix ms
}

// ConnectionInfo summarises WebSocket connectivity during the session.
type ConnectionInfo struct {
	DisconnectCount     int   `json:"disconnect_count"`
	TotalDisconnectedMs int64 `json:"total_disconnected_ms"`
	MaxDisconnectedMs   int64 `json:"max_disconnected_ms"`
}

// GapInfo captures data-quality metrics computed during post-processing.
type GapInfo struct {
	BrokerTimestampGaps int     `json:"broker_timestamp_gaps"` // count of gaps > 2s
	MaxBrokerGapMs      int64   `json:"max_broker_gap_ms"`     // largest broker-timestamp gap
	TotalBrokerGapMs    int64   `json:"total_broker_gap_ms"`   // sum of all gaps
	DataLossGaps        int     `json:"data_loss_gaps"`        // gaps caused by WS disconnect
	InactivityGaps      int     `json:"inactivity_gaps"`       // gaps while WS was connected
	DataLossMs          int64   `json:"data_loss_ms"`          // total ms lost to disconnects
	AvgLatencyMs        float64 `json:"avg_latency_ms"`        // avg(ReceivedAt - Timestamp)
	MaxLatencyMs        float64 `json:"max_latency_ms"`        // max latency
	StartTimestamp      int64   `json:"start_timestamp"`       // first event broker ts
	EndTimestamp        int64   `json:"end_timestamp"`         // last event broker ts
}

// Resolution values written to MarketMetadata.Resolution.
//
// Up/down markets resolve "up"/"down"; ladder markets (above / range / hit)
// resolve "yes"/"no". A session the run ended before settlement is
// "unsettled" and must never be given a synthesized outcome.
const (
	ResolutionUp        = "up"
	ResolutionDown      = "down"
	ResolutionYes       = "yes"
	ResolutionNo        = "no"
	ResolutionUnsettled = "unsettled"
)

// ResolutionBasis* record HOW a session's outcome was decided, so a reader can
// weigh it: `onchain` is the market's own resolution message (authoritative),
// `collapsed_book` is an order book that had already collapsed to one side by
// the time we looked, `midprice` is the 0.5-boundary guess used when the book
// was still two-sided, and `no_book` means no book was ever recorded.
const (
	ResolutionBasisOnchain       = "onchain"
	ResolutionBasisCollapsedBook = "collapsed_book"
	ResolutionBasisMidprice      = "midprice"
	ResolutionBasisNoBook        = "no_book"
)

// ResolutionLabel maps a raw on-chain winning outcome ("YES"/"NO") onto the
// family-appropriate Resolution value. family is a libs.MarketFamily string;
// an empty family (standalone recording) keeps the raw outcome unchanged.
func ResolutionLabel(family, winningOutcome string) string {
	if winningOutcome == "" {
		return ""
	}
	if family == "" {
		return winningOutcome
	}
	switch strings.ToUpper(winningOutcome) {
	case "YES":
		if family == string(libs.FamilyUpDown) {
			return ResolutionUp
		}
		return ResolutionYes
	case "NO":
		if family == string(libs.FamilyUpDown) {
			return ResolutionDown
		}
		return ResolutionNo
	default:
		return winningOutcome
	}
}

// Settle source values written to SettlementInfo.Source. They mirror
// libs.SettleSource; kept as strings so metadata.json stays self-describing.
const (
	SettleSourceBinanceCandle  = string(libs.SettleBinanceCandle)
	SettleSourceBinance1mClose = string(libs.SettleBinance1mClose)
	SettleSourceBinance1mHigh  = string(libs.SettleBinance1mHigh)
	SettleSourceChainlinkTWAP  = string(libs.SettleChainlinkTWAP)
)

// FamilyInfo identifies the market series a recorded market belongs to, so a
// reader can group markets across epochs.
type FamilyInfo struct {
	Name       string `json:"name"`        // updown | above | range | hit
	Interval   string `json:"interval"`    // 5m | 15m | 1h | 4h | 1d
	Asset      string `json:"asset"`       // BTC
	SeriesSlug string `json:"series_slug"` // Gamma series slug
	SeriesID   string `json:"series_id"`   // Gamma series id
	Recurrence string `json:"recurrence,omitempty"`
}

// EventInfo describes the Gamma event a market belongs to. For the ladder
// families one event holds several markets, one per rung.
type EventInfo struct {
	Slug   string `json:"slug"`
	Ticker string `json:"ticker,omitempty"`
	Title  string `json:"title,omitempty"`

	// SettleDateET is the ET calendar date the settlement instant falls on, i.e.
	// the date named in the market title. It is not always the epoch directory
	// the recording lives in: an intraday market that settles after the epoch
	// boundary is titled with its window date but filed under the epoch that
	// contains its settlement.
	SettleDateET string `json:"settle_date_et"`

	// TradingWindowStart/End are the event's own [startDate, endDate). This is
	// the window during which the market TRADES, which is not the market
	// window: intraday events open about two days early, and ladder events
	// trade for ~7 days. Settlement is SettlementInfo.At.
	TradingWindowStart int64 `json:"trading_window_start"`
	TradingWindowEnd   int64 `json:"trading_window_end"`

	// Ladder geometry. LadderSize is 1 for up/down markets; LadderIndex is -1
	// for non-ladder markets. Strike is set for strike ladders, RangeLow/High
	// for range ladders. StrikeLabel is the verbatim GroupItemTitle, kept even
	// when parsing fails so nothing is lost.
	LadderSize  int      `json:"ladder_size"`
	LadderIndex int      `json:"ladder_index"`
	Strike      *float64 `json:"strike,omitempty"`
	StrikeLabel string   `json:"strike_label,omitempty"`
	RangeLow    *float64 `json:"range_low,omitempty"`
	RangeHigh   *float64 `json:"range_high,omitempty"`

	Outcomes        []string `json:"outcomes,omitempty"`
	NegRisk         bool     `json:"neg_risk,omitempty"`
	NegRiskMarketID string   `json:"neg_risk_market_id,omitempty"`

	// OffsetDays is how many settlement days ahead this event was when it was
	// picked up. 0 is the epoch's own settlement event; 1 is the "+1" event.
	OffsetDays int `json:"offset_days"`
	// IsSettlementEpoch is true when the event settles inside this epoch, as
	// opposed to being carried for the next one.
	IsSettlementEpoch bool `json:"is_settlement_epoch"`
}

// SettlementInfo records how a market's outcome is decided and — crucially —
// which recorded feed it can be recomputed from.
type SettlementInfo struct {
	At     int64  `json:"at"`     // anchor instant, unix ms
	Anchor string `json:"anchor"` // noon_et | midnight_et | et_boundary
	Source string `json:"source"` // see SettleSource* constants

	// WindowStart is the instant the window the settlement compares opens:
	// settlement.At − the family's interval. For the intraday up/down families
	// that is the start of the candle whose close decides the market; for the
	// noon-anchored daily families it is the previous noon — the candle the
	// market compares its own close against — and for hit price, midnight ET of
	// the observed day. With At it bounds the window open_price / close_price
	// are derived over, so post-processing does not have to re-derive the
	// convention from family.interval (CONTEXT.md §13.3).
	WindowStart int64 `json:"window_start,omitempty"`

	Symbol string `json:"symbol,omitempty"` // BTCUSDT
	Market string `json:"market,omitempty"` // spot | perp
	// Comparison is the geometric test applied to the settle candle:
	// above_strike | above_level | below_level | between | up_vs_open |
	// twap_vs_start | any_high_at_or_above.
	Comparison string `json:"comparison,omitempty"`

	// RuleText is the verbatim Gamma description, so a reader can audit the
	// parse without re-fetching the market.
	RuleText string `json:"rule_text,omitempty"`

	// DerivedFrom names the feed bucket the outcome can be recomputed from,
	// relative to the data root (e.g. "binance/spot/BTCUSDT_2026-10-02").
	DerivedFrom string `json:"derived_from,omitempty"`
	// DerivedVerified is set by post-processing once the trade stream has been
	// cross-checked against the recorded kline. Nil means "not checked yet".
	DerivedVerified *bool `json:"derived_verified,omitempty"`
}

// StrikeFilterInfo records how a ladder fan-out selected its rungs.
//
// The filter is a volume control, never a correctness one — but the first claim
// of a run happens before the RTDS feed has delivered a price, so it centres on
// the ladder's median rung rather than on spot. Two runs can therefore
// legitimately hold different rung sets, and this block is what says which. It
// is stamped once per claim, when the ladder was first picked up (CONTEXT.md
// §13.3).
type StrikeFilterInfo struct {
	Limit int     `json:"limit"`          // configured rungs per side (0 = every rung)
	Basis string  `json:"basis"`          // all | spot | median_rung
	Spot  float64 `json:"spot,omitempty"` // reference price the filter used
	Kept  int     `json:"kept"`           // rungs recorded
	Total int     `json:"total"`          // rungs in the event's full ladder
}

// Strike-filter bases, recorded in StrikeFilterInfo.Basis.
const (
	// StrikeFilterAll: no filter configured, so every rung was kept.
	StrikeFilterAll = "all"
	// StrikeFilterSpot: centred on the reference price the feed had delivered.
	StrikeFilterSpot = "spot"
	// StrikeFilterMedian: no reference price yet, so the ladder's middle rung
	// was used as the centre (the filter is re-centred weekly with the strikes,
	// so the middle rung is a good proxy).
	StrikeFilterMedian = "median_rung"
)

// MarketMetadata is the metadata persisted alongside recorded events.
type MarketMetadata struct {
	MarketID    string `json:"market_id"`
	Slug        string `json:"slug"`
	YesAssetID  string `json:"yes_asset_id"`
	NoAssetID   string `json:"no_asset_id"`
	Question    string `json:"question"`
	ConditionID string `json:"condition_id"`

	// EpochID is the ET settlement date this market belongs to, i.e. the
	// directory it is written under (libs.EpochDateLayout). Empty for
	// standalone recordings.
	EpochID string `json:"epoch_id,omitempty"`

	StartTime int64 `json:"start_time"` // recording window start, unix ms
	EndTime   int64 `json:"end_time"`   // recording window end == Settlement.At, unix ms

	Resolution       string `json:"resolution,omitempty"` // see Resolution* constants
	WinningOutcome   string `json:"winning_outcome,omitempty"`
	SyntheticResolve bool   `json:"synthetic_resolve"`
	// ResolutionBasis says how the outcome was decided — see ResolutionBasis*.
	// It separates a confident read (a collapsed book) from the 0.5 guess, so a
	// synthesized outcome is never taken at face value.
	ResolutionBasis string `json:"resolution_basis,omitempty"`
	// Truncated is true when the run ended before this market settled, so the
	// recording is partial. Such a market must never get a synthesized outcome.
	Truncated    bool    `json:"truncated,omitempty"`
	LastMidprice float64 `json:"last_midprice,omitempty"`

	// OpenPrice/ClosePrice are the underlying price at the market window's
	// open/close, taken from the feeds recorded alongside the market (the
	// settlement source itself), not from an external price API.
	OpenPrice  float64 `json:"open_price,omitempty"`
	ClosePrice float64 `json:"close_price,omitempty"`

	// Family/Event/Settlement describe where the market sits and how it
	// resolves. Nil for standalone (non-series) recordings.
	Family     *FamilyInfo     `json:"family,omitempty"`
	Event      *EventInfo      `json:"event,omitempty"`
	Settlement *SettlementInfo `json:"settlement,omitempty"`
	// StrikeFilter records how the ladder fan-out picked this market's rung.
	// Nil for the non-ladder families.
	StrikeFilter *StrikeFilterInfo `json:"strike_filter,omitempty"`

	EventCount  int             `json:"event_count"`
	RecordedAt  int64           `json:"recorded_at"` // unix ms
	RawMarket   json.RawMessage `json:"raw_market,omitempty"`
	DataQuality *GapInfo        `json:"data_quality,omitempty"`
	Connections *ConnectionInfo `json:"connections,omitempty"`
}

// ── Epoch manifest ──────────────────────────────────────────

// EpochManifest is written to data/market/<epoch>/manifest.json. It is an
// index of what the epoch contains, grouped by series, so a reader never has
// to scan directories.
type EpochManifest struct {
	EpochID    string   `json:"epoch_id"`
	EpochStart int64    `json:"epoch_start"` // unix ms
	EpochEnd   int64    `json:"epoch_end"`   // unix ms
	Anchor     string   `json:"anchor"`      // noon_et | midnight_et
	Run        *RunInfo `json:"run,omitempty"`

	// Feeds maps a feed name ("rtds", "binance") to the bucket folder names
	// recorded for this epoch.
	Feeds map[string][]string `json:"feeds,omitempty"`

	Series []SeriesManifest `json:"series,omitempty"`

	// OffsetEvents lists the event slugs carried for the *next* settlement day
	// (the "+1" events), which will be filed under the following epoch.
	OffsetEvents []string `json:"offset_events,omitempty"`

	RecordedAt int64 `json:"recorded_at"`
}

// RunInfo describes the recording run that produced an epoch.
type RunInfo struct {
	Days             int    `json:"days"`
	LadderOffsetDays int    `json:"ladder_offset_days"`
	Anchor           string `json:"anchor"`
	StartedAt        int64  `json:"started_at"`
}

// SeriesManifest is one series' contribution to an epoch.
type SeriesManifest struct {
	Series      string `json:"series"`   // registry shorthand, e.g. "btc_1h", "btc_above"
	Family      string `json:"family"`   // updown | above | range | hit
	Interval    string `json:"interval"` // 5m | 15m | 1h | 4h | 1d
	Asset       string `json:"asset"`    // BTC
	GammaSeries string `json:"gamma_series"`
	EventSlug   string `json:"event_slug,omitempty"`
	LadderSize  int    `json:"ladder_size,omitempty"`
	SettleAt    int64  `json:"settle_at,omitempty"`
	// StrikeFilter records the rung filter this event was claimed under, so a
	// reader can tell a spot-centred claim from the median-rung fallback.
	StrikeFilter *StrikeFilterInfo `json:"strike_filter,omitempty"`
	Markets      []ManifestMarket  `json:"markets"`
}

// ManifestMarket is one recorded market inside a SeriesManifest.
type ManifestMarket struct {
	MarketID    string   `json:"market_id"`
	Slug        string   `json:"slug,omitempty"`
	StrikeLabel string   `json:"strike_label,omitempty"`
	Strike      *float64 `json:"strike,omitempty"`
	RangeLow    *float64 `json:"range_low,omitempty"`
	RangeHigh   *float64 `json:"range_high,omitempty"`
	OffsetDays  int      `json:"offset_days,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
}

// RecordingConfig configures a daily-epoch recording run. It is the `recording`
// block of collector.yaml; the zero value means "noon-ET epochs, run until
// stopped".
//
// The epoch is the day boundary every feed bucket and every market recording is
// keyed to: epoch `D` covers [anchor on D-1, anchor on D). See CONTEXT.md §2.
type RecordingConfig struct {
	// Days is the number of daily epochs to record (0 = run indefinitely).
	Days int `yaml:"days"`
	// Epoch is the daily boundary rule: "noon_et" (default) or "midnight_et".
	Epoch string `yaml:"epoch"`
	// LadderOffsetDays is how many extra settlement days ahead the ladder
	// families are recorded, so every ladder event accumulates a full
	// pre-settlement history. Default 1.
	LadderOffsetDays int `yaml:"ladder_offset_days"`
}

// Anchor resolves the configured epoch rule, defaulting to noon ET.
func (c RecordingConfig) Anchor() (libs.EpochAnchor, error) {
	return libs.ParseEpochAnchor(c.Epoch)
}

// OffsetDays resolves the ladder offset, defaulting to 1.
func (c RecordingConfig) OffsetDays() int {
	if c.LadderOffsetDays < 0 {
		return 0
	}
	if c.LadderOffsetDays == 0 {
		return 1
	}
	return c.LadderOffsetDays
}

// MarketContext carries the discovery-derived descriptors for a recording
// session: which epoch it belongs to, which series family and Gamma event it
// came from, and how it settles. The scheduler builds it from Gamma and the
// session writes it verbatim into metadata.json. The zero value means a
// standalone recording — no epoch directory and no family/event blocks.
type MarketContext struct {
	// EpochID is the ET settlement date the market is filed under. Empty for
	// standalone recordings, which keep the flat {recordingDir}/{marketID} path.
	EpochID      string
	Family       *FamilyInfo
	Event        *EventInfo
	Settlement   *SettlementInfo
	StrikeFilter *StrikeFilterInfo
	// Truncated marks a session the run ended before its settlement. It is set
	// at shutdown and forces Resolution to "unsettled".
	Truncated bool
}

// Empty reports whether the context carries no discovery information.
func (mc MarketContext) Empty() bool {
	return mc.EpochID == "" && mc.Family == nil && mc.Event == nil &&
		mc.Settlement == nil && mc.StrikeFilter == nil
}

// ResolutionFor returns the Resolution value for a raw on-chain outcome under
// this context: "unsettled" when the run truncated the session, otherwise the
// family-appropriate label (up/down vs yes/no).
func (mc MarketContext) ResolutionFor(winningOutcome string) string {
	if mc.Truncated {
		return ResolutionUnsettled
	}
	family := ""
	if mc.Family != nil {
		family = mc.Family.Name
	}
	return ResolutionLabel(family, winningOutcome)
}

// CollectorConfig configures the EventCollector.
type CollectorConfig struct {
	// RecordingDir is the root directory for recorded market data.
	// Default: "data/market".
	RecordingDir string
	// EpochAnchor is the daily epoch rule every recording is keyed to. It names
	// the directory a market lands in, and the feed bucket a settlement can be
	// recomputed from. Default: noon_et.
	EpochAnchor libs.EpochAnchor
}

// SeriesConfig selects one market family to record, by its registry shorthand:
//
//	series:
//	  - series: btc_1h
//	  - series: btc_above
//	    strikes: 3
//
// The shorthand is resolved through libs.LookupSeriesByName, so the series
// table — not the config — decides the Gamma slug, the settlement anchor and
// the resolution rule. Those were verified against live Gamma, and guessing one
// makes the collector record the wrong market silently.
//
// A series no longer carries a count: the whole schedule is daily now, so the
// run length is `recording.days`.
type SeriesConfig struct {
	Series string `yaml:"series"`

	// Strikes keeps at most this many ladder rungs on each side of the spot
	// price at session start (0 = every rung). Ladder families only — ignored by
	// up/down, which have a single market per event.
	Strikes int `yaml:"strikes"`
}

// DefaultSyntheticResolveDelay is how long to wait after a market's end time
// before applying a synthetic resolution based on the last midprice.
const DefaultSyntheticResolveDelay = 60 * time.Second

// DefaultRecordingDir is the default root directory for recorded market data.
const DefaultRecordingDir = "data/market"

// DefaultDataDir is the default base data directory, containing the market/,
// rtds/ and binance/ subdirectories.
const DefaultDataDir = "data"

// ── Continuous feed recording (RTDS / Binance) ──────────────

// RTDSConfig configures recording of Polymarket RTDS reference prices.
// RTDS streams the underlying crypto asset price (~1/sec) and runs
// continuously, independent of prediction-market rotation.
type RTDSConfig struct {
	// CryptoPrices records real-time reference prices from the RTDS
	// crypto_prices / chainlink topics. Symbols containing "/" are treated as
	// Chainlink feeds (e.g. "btc/usd"); all others as crypto_prices symbols
	// (e.g. "btcusdt").
	CryptoPrices CryptoPricesConfig `yaml:"crypto_prices"`
	// ChainlinkTWAP records the Chainlink-computed TWAP prices that Polymarket
	// resolves each rolling market on. Nil disables it.
	ChainlinkTWAP *ChainlinkTWAPConfig `yaml:"chainlink_twap"`
}

// CryptoPricesConfig configures recording of RTDS reference prices.
type CryptoPricesConfig struct {
	// Enabled turns on reference-price recording.
	Enabled bool `yaml:"enabled"`
	// Symbols are the RTDS symbols to record, e.g. ["btcusdt", "btc/usd"].
	Symbols []string `yaml:"symbols"`
}

// ChainlinkTWAPConfig configures recording of Chainlink TWAP reference prices
// from Polymarket's RTDS stream (the actual price each market settles on).
type ChainlinkTWAPConfig struct {
	// Enabled turns on Chainlink TWAP recording.
	Enabled bool `yaml:"enabled"`
	// Feeds are the Chainlink feeds to record, e.g. ["btc/usd"].
	Feeds []string `yaml:"feeds"`
	// Window is the TWAP lookback in seconds: 30 or 60 (default 60).
	Window int `yaml:"window"`
}

// BinanceConfig configures recording of Binance market data per market
// (spot / USDⓈ-M perpetual) for building TWAP forecasts in Python.
type BinanceConfig struct {
	// Perp is the USDⓈ-M perpetual market config.
	Perp BinanceMarketConfig `yaml:"perp"`
	// Spot is the spot market config.
	Spot BinanceMarketConfig `yaml:"spot"`
}

// BinanceMarketConfig configures recording for one Binance market.
type BinanceMarketConfig struct {
	// Symbols are the Binance symbols to record, e.g. ["BTCUSDT"].
	Symbols []string `yaml:"symbols"`
	// Streams selects which Binance streams to subscribe: "aggTrade", "depth"
	// (partial book depth, @depth20@100ms). Empty = both.
	Streams []string `yaml:"streams"`
}

// UnmarshalYAML accepts either a YAML list of symbols or a scalar such as an
// empty string (`symbols: ""`) for convenience when disabling a market.
func (m *BinanceMarketConfig) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Symbols any      `yaml:"symbols"`
		Streams []string `yaml:"streams"`
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	switch v := raw.Symbols.(type) {
	case nil:
		m.Symbols = nil
	case string:
		if strings.TrimSpace(v) == "" {
			m.Symbols = nil
		} else {
			m.Symbols = []string{v}
		}
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				m.Symbols = append(m.Symbols, s)
			}
		}
	default:
		return fmt.Errorf("binance market: symbols must be a list of strings or an empty string")
	}
	m.Streams = raw.Streams
	return nil
}

// FeedMetadata is the metadata.json written per epoch feed bucket.
type FeedMetadata struct {
	Feed   string `json:"feed"`             // "rtds" | "binance"
	Symbol string `json:"symbol"`           // e.g. "btcusdt" / "BTCUSDT"
	Market string `json:"market,omitempty"` // "spot"|"perp" (binance only)

	// Parts lists this bucket's event files in the order they must be read:
	// events.gz first, then events.001.gz, events.002.gz, … A bucket whose volume
	// passes the part limit is split, because one gzip file per epoch of aggTrade
	// + depth reaches hundreds of MB and every reader — including download.sh,
	// which pulls the bucket as one scp — would rather handle bounded files. The
	// first part keeps the documented name, so a bucket that never rotated is
	// unchanged and a reader that only opens events.gz still works on it (note
	// that a plain glob sorts the numbered parts ahead of events.gz, so this list,
	// not a glob, is the order).
	Parts []string `json:"parts,omitempty"`

	EpochID      string           `json:"epoch_id,omitempty"` // ET settlement date, YYYY-MM-DD
	Anchor       string           `json:"anchor,omitempty"`   // epoch boundary rule
	StartTime    int64            `json:"start_time"`         // bucket window start (unix ms)
	EndTime      int64            `json:"end_time"`           // bucket window end (unix ms)
	EventCount   int64            `json:"event_count"`
	EventCounts  map[string]int64 `json:"event_counts,omitempty"` // per event type
	FirstEventTs int64            `json:"first_event_ts,omitempty"`
	LastEventTs  int64            `json:"last_event_ts,omitempty"`
	// TailStaleMs is the gap between the last recorded event and the bucket's
	// window end — a large value flags a feed that silently stopped receiving
	// events mid-bucket (e.g. a WS death with no disconnect event).
	TailStaleMs int64 `json:"tail_stale_ms,omitempty"`
	RecordedAt  int64 `json:"recorded_at"`
	// DataQuality holds latency / broker-timestamp-gap statistics (same schema
	// as market metadata, so Python can treat them identically).
	DataQuality *GapInfo `json:"data_quality,omitempty"`
	// Connections summarises WebSocket connectivity during the bucket.
	Connections *ConnectionInfo `json:"connections,omitempty"`
}
