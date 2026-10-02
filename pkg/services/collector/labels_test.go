package collector

import (
	"fmt"
	"strings"
	"testing"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
)

func TestParseStrikeLabel(t *testing.T) {
	f := func(v float64) *float64 { return &v }

	tests := []struct {
		label string
		want  StrikeRange
	}{
		// Up/down markets carry no rung: empty, and not an error.
		{"", StrikeRange{}},
		{"   ", StrikeRange{}},

		// Strike ladders (above/below markets).
		{"74,000", StrikeRange{Strike: f(74000)}},
		{"80,000", StrikeRange{Strike: f(80000)}},
		// Hit-price rungs use arrows.
		{"↑ 93,000", StrikeRange{RangeLow: f(93000)}},
		{"↓ 70,000", StrikeRange{RangeHigh: f(70000)}},

		// Range ladders (price-range markets) use one-sided buckets.
		{"<74,000", StrikeRange{RangeHigh: f(74000)}},
		{">94,000", StrikeRange{RangeLow: f(94000)}},
		{"$74,000 - $76,000", StrikeRange{RangeLow: f(74000), RangeHigh: f(76000)}},
		{"$76,000 - $74,000", StrikeRange{RangeLow: f(74000), RangeHigh: f(76000)}}, // order-insensitive

		// Decimals and ≥/≤.
		{"82,500.50", StrikeRange{Strike: f(82500.5)}},
		{"≤ 70,000", StrikeRange{RangeHigh: f(70000)}},
		{"≥ 95,000", StrikeRange{RangeLow: f(95000)}},
	}

	for _, tc := range tests {
		got, err := ParseStrikeLabel(tc.label)
		if err != nil {
			t.Errorf("ParseStrikeLabel(%q): unexpected error %v", tc.label, err)
			continue
		}
		if got.Label != trimLabel(tc.label) {
			t.Errorf("ParseStrikeLabel(%q).Label = %q, want the verbatim trimmed label",
				tc.label, got.Label)
		}
		if !samePtr(got.Strike, tc.want.Strike) ||
			!samePtr(got.RangeLow, tc.want.RangeLow) ||
			!samePtr(got.RangeHigh, tc.want.RangeHigh) {
			t.Errorf("ParseStrikeLabel(%q) = strike=%s low=%s high=%s, want strike=%s low=%s high=%s",
				tc.label, ptr(got.Strike), ptr(got.RangeLow), ptr(got.RangeHigh),
				ptr(tc.want.Strike), ptr(tc.want.RangeLow), ptr(tc.want.RangeHigh))
		}
		if got.Empty() != tc.want.Empty() {
			t.Errorf("ParseStrikeLabel(%q).Empty() = %v, want %v", tc.label, got.Empty(), tc.want.Empty())
		}
	}
}

func TestParseStrikeLabelRejectsGarbage(t *testing.T) {
	for _, label := range []string{"above", "unknown rung", "$"} {
		if _, err := ParseStrikeLabel(label); err == nil {
			t.Errorf("ParseStrikeLabel(%q) should error so the caller keeps the raw label", label)
		}
	}
}

func TestComparisonFor(t *testing.T) {
	tests := []struct {
		source string
		r      StrikeRange
		want   string
	}{
		{SettleSourceChainlinkTWAP, StrikeRange{}, CompareTWAPVsStart},  // up/down 4h
		{SettleSourceBinanceCandle, StrikeRange{}, CompareUpVsOpen},     // up/down 1h
		{SettleSourceBinance1mHigh, StrikeRange{}, CompareAnyHighAbove}, // hit price
		{SettleSourceBinance1mClose, StrikeRange{}, CompareUpVsOpen},    // up/down daily
		{SettleSourceBinance1mClose, StrikeRange{Strike: ptrF(80000)}, CompareAboveStrike},
		{SettleSourceBinance1mClose, StrikeRange{RangeHigh: ptrF(74000)}, CompareBelowLevel},
		{SettleSourceBinance1mClose, StrikeRange{RangeLow: ptrF(94000)}, CompareAboveLevel},
		{SettleSourceBinance1mClose, StrikeRange{RangeLow: ptrF(74000), RangeHigh: ptrF(76000)}, CompareBetween},
	}
	for _, tc := range tests {
		if got := ComparisonFor(tc.source, tc.r); got != tc.want {
			t.Errorf("ComparisonFor(%s, …) = %s, want %s", tc.source, got, tc.want)
		}
	}
}

func TestComparisonForSeriesUsesRegistrySource(t *testing.T) {
	cases := map[string]string{
		"btc_1h":    CompareUpVsOpen,
		"btc_4h":    CompareTWAPVsStart,
		"btc_1d":    CompareUpVsOpen,
		"btc_above": CompareAboveStrike,
		"btc_hit":   CompareAnyHighAbove,
	}
	for name, want := range cases {
		spec, err := libs.LookupSeriesByName(name)
		if err != nil {
			t.Fatalf("LookupSeriesByName(%s): %v", name, err)
		}
		r := StrikeRange{}
		if spec.Ladder && spec.Key.Family == libs.FamilyAbove {
			r.Strike = ptrF(80000)
		}
		if got := ComparisonForSeries(spec, r); got != want {
			t.Errorf("ComparisonForSeries(%s) = %s, want %s", name, got, want)
		}
	}
}

func TestResolutionLabel(t *testing.T) {
	tests := []struct {
		family, outcome, want string
	}{
		{"updown", "YES", ResolutionUp},
		{"updown", "NO", ResolutionDown},
		{"above", "YES", ResolutionYes},
		{"range", "NO", ResolutionNo},
		{"hit", "YES", ResolutionYes},
		{"updown", "yes", ResolutionUp}, // case-insensitive on the raw outcome
		{"updown", "", ""},              // unresolved
		{"", "YES", "YES"},              // standalone: raw outcome preserved
	}
	for _, tc := range tests {
		if got := ResolutionLabel(tc.family, tc.outcome); got != tc.want {
			t.Errorf("ResolutionLabel(%q, %q) = %q, want %q", tc.family, tc.outcome, got, tc.want)
		}
	}
}

func TestUnsettledIsNotAWinOutcome(t *testing.T) {
	// A truncated session must be marked unsettled, never given an outcome.
	if got := ResolutionLabel("above", ""); got != "" {
		t.Errorf("unresolved market = %q, want empty", got)
	}
	if ResolutionUnsettled == ResolutionYes || ResolutionUnsettled == ResolutionNo {
		t.Fatal("ResolutionUnsettled must not collide with a real outcome")
	}
}

func trimLabel(s string) string { return strings.TrimSpace(s) }

func ptrF(v float64) *float64 { return &v }

func ptr(p *float64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%g", *p)
}

func samePtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
