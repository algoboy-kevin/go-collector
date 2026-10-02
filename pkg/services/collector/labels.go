package collector

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
)

// Settlement comparison values written to SettlementInfo.Comparison.
const (
	CompareAboveStrike  = "above_strike"         // settle price > strike
	CompareAboveLevel   = "above_level"          // settle price > level (">94,000")
	CompareBelowLevel   = "below_level"          // settle price < level ("<74,000")
	CompareBetween      = "between"              // range_low <= settle price < range_high
	CompareUpVsOpen     = "up_vs_open"           // close vs open of the interval candle
	CompareTWAPVsStart  = "twap_vs_start"        // Chainlink TWAP vs range-start price
	CompareAnyHighAbove = "any_high_at_or_above" // any 1m candle high in the ET day >= strike
)

// StrikeRange is the ladder geometry parsed from a Gamma market's
// GroupItemTitle.
//
// Real labels observed on Gamma (2026-10-02):
//
//	"74,000"     strike ladder rung      → Strike
//	"<74,000"    lowest range bucket     → RangeHigh
//	">94,000"    highest range bucket    → RangeLow
//	"↑ 93,000"   hit-price rung          → RangeLow
//	"↓ 70,000"   hit-price rung          → RangeHigh
//
// Up/down markets have an empty label (no strike) — that is not an error.
type StrikeRange struct {
	Label     string   // verbatim GroupItemTitle, always preserved
	Strike    *float64 // exact strike, when the rung is a single point
	RangeLow  *float64 // lower bound of a range rung
	RangeHigh *float64 // upper bound of a range rung
}

// Empty reports whether the label described no ladder rung at all.
func (r StrikeRange) Empty() bool {
	return r.Strike == nil && r.RangeLow == nil && r.RangeHigh == nil
}

// labelNumberRE matches a number with optional thousands separators and
// decimals, e.g. "74,000" or "82,500.50".
var labelNumberRE = regexp.MustCompile(`\d[\d,]*(?:\.\d+)?`)

// ParseStrikeLabel extracts the ladder geometry from a Gamma GroupItemTitle.
//
// The direction markers are what distinguish a point strike from a one-sided
// range rung: "<74,000" is "below 74,000" (an upper bound), ">94,000" is
// "above 94,000" (a lower bound), while a bare "74,000" is an exact strike.
// An unparseable non-empty label is an error so the caller can keep the raw
// label and flag it rather than silently record wrong geometry.
func ParseStrikeLabel(label string) (StrikeRange, error) {
	out := StrikeRange{Label: strings.TrimSpace(label)}
	if out.Label == "" {
		return out, nil // up/down market: no rung
	}

	raw := labelNumberRE.FindAllString(out.Label, -1)
	if len(raw) == 0 {
		return out, fmt.Errorf("collector: no number in strike label %q", label)
	}
	values := make([]float64, 0, len(raw))
	for _, s := range raw {
		v, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
		if err != nil {
			return out, fmt.Errorf("collector: parse %q in strike label %q: %w", s, label, err)
		}
		values = append(values, v)
	}

	// Two numbers is an explicit interval ("$74,000 - $76,000").
	if len(values) >= 2 {
		low, high := values[0], values[1]
		if low > high {
			low, high = high, low
		}
		out.RangeLow, out.RangeHigh = &low, &high
		return out, nil
	}

	v := values[0]
	switch {
	case strings.ContainsAny(out.Label, "<\u2264\u2193"): // < ≤ ↓
		out.RangeHigh = &v
	case strings.ContainsAny(out.Label, ">\u2265\u2191"): // > ≥ ↑
		out.RangeLow = &v
	default:
		out.Strike = &v
	}
	return out, nil
}

// ComparisonFor derives SettlementInfo.Comparison from the settle source and
// the parsed ladder geometry. Up/down markets resolve on direction, so their
// geometry is empty and the comparison follows from the settlement source.
func ComparisonFor(source string, r StrikeRange) string {
	switch source {
	case SettleSourceChainlinkTWAP:
		return CompareTWAPVsStart
	case SettleSourceBinanceCandle:
		return CompareUpVsOpen
	case SettleSourceBinance1mHigh:
		return CompareAnyHighAbove
	}
	// Binance 1m close: up/down daily resolves on direction; ladders compare
	// the settle price against the rung.
	switch {
	case r.RangeLow != nil && r.RangeHigh != nil:
		return CompareBetween
	case r.RangeLow != nil:
		return CompareAboveLevel
	case r.RangeHigh != nil:
		return CompareBelowLevel
	case r.Strike != nil:
		return CompareAboveStrike
	default:
		return CompareUpVsOpen
	}
}

// ComparisonForSeries is ComparisonFor with the source taken from the series
// registry.
func ComparisonForSeries(spec libs.SeriesSpec, r StrikeRange) string {
	return ComparisonFor(string(spec.Source), r)
}
