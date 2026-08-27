// Package libs provides the small set of shared utilities the collector needs
// from the trading engine: rolling market series metadata, slug generation,
// and number helpers.
//
// These are inlined (not imported) so the collector module has no dependency
// on go-trading-core. They mirror the equivalent symbols in
// go-trading-core/pkg/libs; keep them in sync if the engine's series table
// changes.
package libs

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// RollingMarketSeries identifies a rolling prediction market series.
type RollingMarketSeries string

const (
	// BTC
	RollingBTC5M  RollingMarketSeries = "btc_5m"
	RollingBTC15M RollingMarketSeries = "btc_15m"
	RollingBTC1H  RollingMarketSeries = "btc_1h"
	// ETH
	RollingETH5M  RollingMarketSeries = "eth_5m"
	RollingETH15M RollingMarketSeries = "eth_15m"
	RollingETH1H  RollingMarketSeries = "eth_1h"
	// SOL
	RollingSOL5M  RollingMarketSeries = "sol_5m"
	RollingSOL15M RollingMarketSeries = "sol_15m"
	RollingSOL1H  RollingMarketSeries = "sol_1h"
	// XRP
	RollingXRP5M  RollingMarketSeries = "xrp_5m"
	RollingXRP15M RollingMarketSeries = "xrp_15m"
	RollingXRP1H  RollingMarketSeries = "xrp_1h"
)

// RollingCryptoConfig holds the crypto symbol and interval variant for a series.
type RollingCryptoConfig struct {
	Symbol  string
	Variant string // "fiveminute", "fifteen", "hourly", "daily"
}

// SERIES_CRYPTO_CONFIG maps each rolling series to its crypto API config.
var SERIES_CRYPTO_CONFIG = map[RollingMarketSeries]RollingCryptoConfig{
	RollingBTC5M:  {Symbol: "BTC", Variant: "fiveminute"},
	RollingBTC15M: {Symbol: "BTC", Variant: "fifteen"},
	RollingBTC1H:  {Symbol: "BTC", Variant: "hourly"},
	RollingETH5M:  {Symbol: "ETH", Variant: "fiveminute"},
	RollingETH15M: {Symbol: "ETH", Variant: "fifteen"},
	RollingETH1H:  {Symbol: "ETH", Variant: "hourly"},
	RollingSOL5M:  {Symbol: "SOL", Variant: "fiveminute"},
	RollingSOL15M: {Symbol: "SOL", Variant: "fifteen"},
	RollingSOL1H:  {Symbol: "SOL", Variant: "hourly"},
	RollingXRP5M:  {Symbol: "XRP", Variant: "fiveminute"},
	RollingXRP15M: {Symbol: "XRP", Variant: "fifteen"},
	RollingXRP1H:  {Symbol: "XRP", Variant: "hourly"},
}

// GetIntervalSeconds returns the interval in seconds for a rolling series.
func GetIntervalSeconds(series RollingMarketSeries) (int64, error) {
	s := string(series)
	if strings.Contains(s, "5m") {
		return 5 * 60, nil
	}
	if strings.Contains(s, "15m") {
		return 15 * 60, nil
	}
	if strings.Contains(s, "1h") {
		return 60 * 60, nil
	}
	return 0, fmt.Errorf("unknown interval for series: %s", series)
}

// GenerateMarketSlug creates a market slug from a series and start time.
//
// Examples:
//
//	5m/15m: "btc-updown-5m-1773555000"
//	1h:     "bitcoin-up-or-down-march-15-2026-2pm-et"
func GenerateMarketSlug(series RollingMarketSeries, startTime int64) string {
	parts := strings.SplitN(string(series), "_", 2)
	if len(parts) != 2 {
		return ""
	}
	asset := parts[0]
	interval := parts[1]

	if interval == "1h" {
		assetName := asset
		if asset == "btc" {
			assetName = "bitcoin"
		}
		date := time.Unix(startTime, 0).UTC()
		dateStr := formatDateForSlug(date)
		return fmt.Sprintf("%s-up-or-down-%s-et", assetName, dateStr)
	}

	return fmt.Sprintf("%s-updown-%s-%d", asset, interval, startTime)
}

// formatDateForSlug renders a UTC time as "march-15-2026-2pm".
func formatDateForSlug(date time.Time) string {
	months := []string{
		"january", "february", "march", "april", "may", "june",
		"july", "august", "september", "october", "november", "december",
	}
	month := months[date.Month()-1]
	day := date.Day()
	year := date.Year()
	hour := date.Hour()
	ampm := "am"
	if hour >= 12 {
		ampm = "pm"
	}
	if hour > 12 {
		hour -= 12
	} else if hour == 0 {
		hour = 12
	}
	return fmt.Sprintf("%s-%d-%d-%d%s", month, day, year, hour, ampm)
}

// ParseFloat converts a string to float64. Returns 0 on failure.
func ParseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// Round rounds f to the given number of decimal places.
func Round(f float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(f*pow) / pow
}

// R3 rounds to 3 decimal places.
func R3(x float64) float64 { return Round(x, 3) }
