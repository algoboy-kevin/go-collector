package collector

import (
	"testing"

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
