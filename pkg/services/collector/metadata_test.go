package collector

import (
	"encoding/json"
	"testing"
	"time"
)

// TestMarketMetadataJSONKeys guards the doc/code contract in CONTEXT.md §6: a
// reader must be able to find every documented field by name.
func TestMarketMetadataJSONKeys(t *testing.T) {
	strike := 80000.0
	low, high := 74000.0, 76000.0
	verified := false
	md := MarketMetadata{
		MarketID:   "5169514",
		Slug:       "bitcoin-above-80k-on-october-2-2026",
		EpochID:    "2026-10-02",
		StartTime:  time.Date(2026, 10, 1, 16, 0, 0, 0, time.UTC).UnixMilli(),
		EndTime:    time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC).UnixMilli(),
		Resolution: ResolutionYes,
		Truncated:  false,
		Family: &FamilyInfo{
			Name: "above", Interval: "1d", Asset: "BTC",
			SeriesSlug: "btc-multi-strikes-weekly", SeriesID: "45", Recurrence: "weekly",
		},
		Event: &EventInfo{
			Slug: "bitcoin-above-on-october-2-2026", Title: "Bitcoin above ___ on October 2?",
			SettleDateET:       "2026-10-02",
			TradingWindowStart: time.Date(2026, 9, 25, 16, 0, 0, 0, time.UTC).UnixMilli(),
			TradingWindowEnd:   time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC).UnixMilli(),
			LadderSize:         11, LadderIndex: 3,
			Strike: &strike, StrikeLabel: "80,000",
			RangeLow: &low, RangeHigh: &high,
			Outcomes: []string{"Yes", "No"}, NegRisk: true, NegRiskMarketID: "0xabc",
			OffsetDays: 1, IsSettlementEpoch: false,
		},
		Settlement: &SettlementInfo{
			At:     time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC).UnixMilli(),
			Anchor: stringAnchor(), Source: SettleSourceBinance1mClose,
			Symbol: "BTCUSDT", Market: "spot", Comparison: CompareAboveStrike,
			RuleText:        `resolve to "Yes" if the Binance 1 minute candle …`,
			DerivedFrom:     "binance/spot/BTCUSDT_2026-10-02",
			DerivedVerified: &verified,
		},
	}

	root := marshalToMap(t, md)
	requireKeys(t, root, "market_id", "slug", "epoch_id", "start_time", "end_time",
		"resolution", "event_count", "recorded_at")
	if _, ok := root["truncated"]; ok {
		t.Error("truncated should be omitted when false")
	}

	family := requireChild(t, root, "family")
	requireKeys(t, family, "name", "interval", "asset", "series_slug", "series_id", "recurrence")
	if family["name"] != "above" || family["interval"] != "1d" {
		t.Errorf("family = %v", family)
	}

	event := requireChild(t, root, "event")
	requireKeys(t, event, "slug", "settle_date_et", "trading_window_start", "trading_window_end",
		"ladder_size", "ladder_index", "strike", "strike_label", "range_low", "range_high",
		"offset_days", "is_settlement_epoch", "neg_risk", "neg_risk_market_id")
	if event["strike"] != strike {
		t.Errorf("event.strike = %v, want %v", event["strike"], strike)
	}

	settlement := requireChild(t, root, "settlement")
	requireKeys(t, settlement, "at", "anchor", "source", "symbol", "market",
		"comparison", "rule_text", "derived_from", "derived_verified")
	if settlement["at"] != float64(md.Settlement.At) {
		t.Errorf("settlement.at = %v, want %v", settlement["at"], md.Settlement.At)
	}
}

// TestMarketMetadataLegacyShapeUnchanged ensures a standalone recording (no
// family/event/settlement) still serialises to the old shape, so existing
// consumers are unaffected.
func TestMarketMetadataLegacyShapeUnchanged(t *testing.T) {
	root := marshalToMap(t, MarketMetadata{MarketID: "123", Slug: "old", EventCount: 7})
	for _, key := range []string{"family", "event", "settlement", "epoch_id", "truncated"} {
		if _, ok := root[key]; ok {
			t.Errorf("standalone recording should not emit %q", key)
		}
	}
	requireKeys(t, root, "market_id", "slug", "event_count", "recorded_at")
}

func TestEpochManifestJSONKeys(t *testing.T) {
	strike := 74000.0
	m := EpochManifest{
		EpochID:    "2026-10-02",
		EpochStart: time.Date(2026, 10, 1, 16, 0, 0, 0, time.UTC).UnixMilli(),
		EpochEnd:   time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC).UnixMilli(),
		Anchor:     stringAnchor(),
		Run:        &RunInfo{Days: 3, LadderOffsetDays: 1, Anchor: stringAnchor(), StartedAt: 1},
		Feeds:      map[string][]string{"binance": {"spot/BTCUSDT_2026-10-02"}},
		Series: []SeriesManifest{{
			Series: "btc_above", Family: "above", Interval: "1d", Asset: "BTC",
			GammaSeries: "btc-multi-strikes-weekly",
			EventSlug:   "bitcoin-above-on-october-2-2026",
			LadderSize:  11, SettleAt: 1,
			Markets: []ManifestMarket{{
				MarketID: "5169514", Slug: "bitcoin-above-74k-on-october-2-2026",
				StrikeLabel: "74,000", Strike: &strike, OffsetDays: 1,
			}},
		}},
		OffsetEvents: []string{"bitcoin-above-on-october-3-2026"},
	}

	root := marshalToMap(t, m)
	requireKeys(t, root, "epoch_id", "epoch_start", "epoch_end", "anchor",
		"run", "feeds", "series", "offset_events")

	series, ok := root["series"].([]any)
	if !ok || len(series) != 1 {
		t.Fatalf("series = %v", root["series"])
	}
	first := series[0].(map[string]any)
	requireKeys(t, first, "series", "family", "interval", "asset", "gamma_series",
		"event_slug", "ladder_size", "settle_at", "markets")

	markets := first["markets"].([]any)
	firstMarket := markets[0].(map[string]any)
	requireKeys(t, firstMarket, "market_id", "slug", "strike_label", "strike", "offset_days")
}

func stringAnchor() string { return "noon_et" }

func marshalToMap(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func requireKeys(t *testing.T, m map[string]any, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			t.Errorf("missing json key %q", key)
		}
	}
}

func requireChild(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	child, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("missing or non-object json key %q", key)
	}
	return child
}
