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

// MarketMetadata is the metadata persisted alongside recorded events.
type MarketMetadata struct {
	MarketID         string  `json:"market_id"`
	Slug             string  `json:"slug"`
	YesAssetID       string  `json:"yes_asset_id"`
	NoAssetID        string  `json:"no_asset_id"`
	Question         string  `json:"question"`
	ConditionID      string  `json:"condition_id"`
	StartTime        int64   `json:"start_time"` // unix ms
	EndTime          int64   `json:"end_time"`   // unix ms
	Resolution       string  `json:"resolution,omitempty"`
	WinningOutcome   string  `json:"winning_outcome,omitempty"`
	SyntheticResolve bool    `json:"synthetic_resolve"`
	LastMidprice     float64 `json:"last_midprice,omitempty"`
	// OpenPrice/ClosePrice are the underlying asset price at the market
	// window's open/close from Polymarket's crypto-price API, both fetched
	// together at finalize. A market that ended midway (before its end time)
	// records only OpenPrice.
	OpenPrice   float64         `json:"open_price,omitempty"`
	ClosePrice  float64         `json:"close_price,omitempty"`
	EventCount  int             `json:"event_count"`
	RecordedAt  int64           `json:"recorded_at"` // unix ms
	RawMarket   json.RawMessage `json:"raw_market,omitempty"`
	DataQuality *GapInfo        `json:"data_quality,omitempty"`
	Connections *ConnectionInfo `json:"connections,omitempty"`
}

// CollectorConfig configures the EventCollector.
type CollectorConfig struct {
	// RecordingDir is the root directory for recorded market data.
	// Default: "data/market".
	RecordingDir string
}

// SeriesConfig describes a rolling market series to record via EventCollector.
// Series must be one of the libs.RollingMarketSeries constants
// (e.g. "btc_5m", "eth_15m", "sol_1h").
// Count is the number of intervals to record (0 = run indefinitely).
type SeriesConfig struct {
	Series string `yaml:"series"`
	Count  int    `yaml:"count"`
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

// FeedMetadata is the metadata.json written per hourly feed bucket.
type FeedMetadata struct {
	Feed         string           `json:"feed"`             // "rtds" | "binance"
	Symbol       string           `json:"symbol"`           // e.g. "btcusdt" / "BTCUSDT"
	Market       string           `json:"market,omitempty"` // "spot"|"perp" (binance only)
	StartTime    int64            `json:"start_time"`       // bucket window start (unix ms)
	EndTime      int64            `json:"end_time"`         // bucket window end (unix ms)
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
