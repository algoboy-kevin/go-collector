package collector

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"gopkg.in/yaml.v3"
)

// TestRTDSConfigUnmarshal ensures the restructured RTDS config block
// (crypto_prices + chainlink_twap) decodes as expected.
func TestRTDSConfigUnmarshal(t *testing.T) {
	var raw struct {
		RTDS RTDSConfig `yaml:"rtds"`
	}
	doc := []byte(`
rtds:
  crypto_prices:
    enabled: true
    symbols:
      - btcusdt
      - btc/usd
  chainlink_twap:
    enabled: true
    feeds:
      - btc/usd
    window: 60
`)
	if err := yaml.Unmarshal(doc, &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !raw.RTDS.CryptoPrices.Enabled {
		t.Fatalf("crypto_prices.enabled = false, want true")
	}
	if len(raw.RTDS.CryptoPrices.Symbols) != 2 ||
		raw.RTDS.CryptoPrices.Symbols[0] != "btcusdt" ||
		raw.RTDS.CryptoPrices.Symbols[1] != "btc/usd" {
		t.Fatalf("crypto_prices.symbols = %v, want [btcusdt btc/usd]", raw.RTDS.CryptoPrices.Symbols)
	}
	if raw.RTDS.ChainlinkTWAP == nil || !raw.RTDS.ChainlinkTWAP.Enabled {
		t.Fatalf("chainlink_twap = %+v, want enabled", raw.RTDS.ChainlinkTWAP)
	}
	if len(raw.RTDS.ChainlinkTWAP.Feeds) != 1 || raw.RTDS.ChainlinkTWAP.Feeds[0] != "btc/usd" {
		t.Fatalf("chainlink_twap.feeds = %v, want [btc/usd]", raw.RTDS.ChainlinkTWAP.Feeds)
	}
	if raw.RTDS.ChainlinkTWAP.Window != 60 {
		t.Fatalf("chainlink_twap.window = %d, want 60", raw.RTDS.ChainlinkTWAP.Window)
	}
}

// TestBinanceMarketConfigUnmarshal ensures the per-market Binance block
// decodes, including the `symbols: ""` scalar form used to disable a market.
func TestBinanceMarketConfigUnmarshal(t *testing.T) {
	var raw struct {
		Binance BinanceConfig `yaml:"binance"`
	}
	doc := []byte(`
binance:
  perp:
    symbols:
      - BTCUSDT
    streams:
      - aggTrade
      - depth
  spot:
    symbols: ""
    streams: []
`)
	if err := yaml.Unmarshal(doc, &raw); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(raw.Binance.Perp.Symbols) != 1 || raw.Binance.Perp.Symbols[0] != "BTCUSDT" {
		t.Fatalf("perp symbols = %v, want [BTCUSDT]", raw.Binance.Perp.Symbols)
	}
	if len(raw.Binance.Perp.Streams) != 2 {
		t.Fatalf("perp streams = %v, want 2", raw.Binance.Perp.Streams)
	}
	if len(raw.Binance.Spot.Symbols) != 0 {
		t.Fatalf("spot symbols = %v, want empty (nil)", raw.Binance.Spot.Symbols)
	}
}

// TestRecordingConfigDefaults pins the epoch defaults: noon ET, offset 1, and
// `days: 0` meaning "run until stopped".
func TestRecordingConfigDefaults(t *testing.T) {
	var zero RecordingConfig
	anchor, err := zero.Anchor()
	if err != nil {
		t.Fatalf("zero Anchor(): %v", err)
	}
	if anchor != libs.AnchorNoonET {
		t.Errorf("default anchor = %q, want %q", anchor, libs.AnchorNoonET)
	}
	if got := zero.OffsetDays(); got != 1 {
		t.Errorf("default ladder offset = %d, want 1", got)
	}
	if got := (RecordingConfig{LadderOffsetDays: 2}).OffsetDays(); got != 2 {
		t.Errorf("ladder offset = %d, want 2", got)
	}

	bad := RecordingConfig{Epoch: "utc_midnight"}
	if _, err := bad.Anchor(); err == nil {
		t.Error("an unknown epoch rule should be rejected, not silently defaulted")
	}
}

// TestShippedConfigsDecode validates the configs in the repo root against the
// live schema: every key must exist — a stale `count:` or a renamed series is a
// startup failure on the server, so it should fail here — and every series must
// be a registry row.
func TestShippedConfigsDecode(t *testing.T) {
	type rootConfig struct {
		DataDir    string          `yaml:"data_dir"`
		RTDSDir    string          `yaml:"rtds_dir"`
		BinanceDir string          `yaml:"binance_dir"`
		Connector  yaml.Node       `yaml:"connector"`
		Series     []SeriesConfig  `yaml:"series"`
		Recording  RecordingConfig `yaml:"recording"`
		Collector  HealthConfig    `yaml:"collector"`
		RTDS       RTDSConfig      `yaml:"rtds"`
		Binance    BinanceConfig   `yaml:"binance"`
	}

	for _, name := range []string{"collector.yaml", "collector_daily.yaml"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "..", name))
			if err != nil {
				t.Skipf("config not readable from the test dir: %v", err)
			}

			var cfg rootConfig
			dec := yaml.NewDecoder(bytes.NewReader(raw))
			dec.KnownFields(true) // an unknown key is an error, not a silent no-op
			if err := dec.Decode(&cfg); err != nil {
				t.Fatalf("decode: %v", err)
			}

			if len(cfg.Series) == 0 {
				t.Fatal("no series configured")
			}
			if anchor, err := cfg.Recording.Anchor(); err != nil {
				t.Fatalf("recording.epoch: %v", err)
			} else if anchor != libs.AnchorNoonET {
				t.Errorf("anchor = %q, want noon_et", anchor)
			}

			// A scheduler built from this config must accept every series.
			ec, _ := newTestCollector(t)
			if _, err := NewEpochScheduler(ec, &stubDiscovery{},
				SchedulerConfig{Recording: cfg.Recording, Series: cfg.Series}); err != nil {
				t.Fatalf("NewEpochScheduler: %v", err)
			}

			// The settlement venue has to be recorded, or no market's outcome can
			// be recomputed from our own data: every BTC family except 4h settles
			// on a Binance BTC/USDT candle.
			if len(cfg.Binance.Spot.Symbols) == 0 {
				t.Error("binance spot is the settlement venue but is not recorded")
			}

			// ...and the Chainlink TWAP feed is the only settlement source the 4h
			// family has.
			for _, sc := range cfg.Series {
				spec, err := libs.LookupSeriesByName(sc.Series)
				if err != nil {
					t.Errorf("series %q: %v", sc.Series, err)
					continue
				}
				if spec.Source != libs.SettleChainlinkTWAP {
					continue
				}
				if cfg.RTDS.ChainlinkTWAP == nil || !cfg.RTDS.ChainlinkTWAP.Enabled ||
					len(cfg.RTDS.ChainlinkTWAP.Feeds) == 0 {
					t.Errorf("%s resolves on the Chainlink TWAP but that feed is not enabled", spec.Name)
				}
			}
		})
	}
}

// TestShippedDailyConfigReadLimit pins the market-WS read limit the research
// config ships. After ANY reconnect the connector re-subscribes the whole asset
// set in one handshake and the server answers with a book snapshot per asset,
// which overruns the 1 MiB default once the full ladder set is being recorded —
// the oversized reply fails the read, closes the socket, and starts the
// reconnect loop the read limit exists to prevent.
func TestShippedDailyConfigReadLimit(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "collector_daily.yaml"))
	if err != nil {
		t.Skipf("config not readable from the test dir: %v", err)
	}

	var cfg struct {
		DataDir    string            `yaml:"data_dir"`
		RTDSDir    string            `yaml:"rtds_dir"`
		BinanceDir string            `yaml:"binance_dir"`
		Connector  polymarket.Config `yaml:"connector"`
		Series     []SeriesConfig    `yaml:"series"`
		Recording  RecordingConfig   `yaml:"recording"`
		Collector  HealthConfig      `yaml:"collector"`
		RTDS       RTDSConfig        `yaml:"rtds"`
		Binance    BinanceConfig     `yaml:"binance"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // an unknown key is an error, not a silent no-op
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("decode collector_daily.yaml: %v", err)
	}

	const want = int64(8 << 20) // 8 MiB
	if cfg.Connector.ReadLimitBytes != want {
		t.Errorf("connector.read_limit_bytes = %d, want %d", cfg.Connector.ReadLimitBytes, want)
	}
}

// TestPriceChangeCoalesceConfigValidation pins the opt-in coalescing setting:
// absent/0 keeps the existing raw behaviour, a positive value is the window
// width in milliseconds, and a negative or duration-overflowing value is
// rejected at startup rather than silently discarded or wrapped.
func TestPriceChangeCoalesceConfigValidation(t *testing.T) {
	for _, ms := range []int{0, 100, 250, 1000} {
		if err := (HealthConfig{PriceChangeCoalesceMs: ms}).Validate(); err != nil {
			t.Errorf("PriceChangeCoalesceMs=%d should be valid, got %v", ms, err)
		}
	}
	if err := (HealthConfig{PriceChangeCoalesceMs: -1}).Validate(); err == nil {
		t.Error("a negative window must be rejected, not silently treated as raw")
	}
	if err := (HealthConfig{PriceChangeCoalesceMs: math.MaxInt}).Validate(); err == nil {
		t.Error("a window that overflows time.Duration must be rejected")
	}

	// Apply carries the value into the collector config as a duration.
	if got := (HealthConfig{PriceChangeCoalesceMs: 100}).Apply(CollectorConfig{}).PriceChangeCoalesce; got != 100*time.Millisecond {
		t.Errorf("Apply(100) = %v, want 100ms", got)
	}
	if got := (HealthConfig{}).Apply(CollectorConfig{}).PriceChangeCoalesce; got != 0 {
		t.Errorf("Apply(absurd) = %v, want 0 (raw)", got)
	}
}

// TestShippedConfigsKeepCoalescingOff pins that the tracked configs ship the
// coalescing window OFF: merely upgrading the binary must never start
// aggregating data on its own, so enabling it stays an explicit edit.
func TestShippedConfigsKeepCoalescingOff(t *testing.T) {
	for _, name := range []string{"collector.yaml", "collector_daily.yaml"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "..", name))
			if err != nil {
				t.Skipf("config not readable from the test dir: %v", err)
			}
			var cfg struct {
				DataDir    string          `yaml:"data_dir"`
				RTDSDir    string          `yaml:"rtds_dir"`
				BinanceDir string          `yaml:"binance_dir"`
				Connector  yaml.Node       `yaml:"connector"`
				Series     []SeriesConfig  `yaml:"series"`
				Recording  RecordingConfig `yaml:"recording"`
				Collector  HealthConfig    `yaml:"collector"`
				RTDS       RTDSConfig      `yaml:"rtds"`
				Binance    BinanceConfig   `yaml:"binance"`
			}
			dec := yaml.NewDecoder(bytes.NewReader(raw))
			dec.KnownFields(true) // the new key must exist in the schema
			if err := dec.Decode(&cfg); err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}
			if cfg.Collector.PriceChangeCoalesceMs != 0 {
				t.Errorf("%s: price_change_coalesce_ms = %d, want 0", name, cfg.Collector.PriceChangeCoalesceMs)
			}
		})
	}
}
