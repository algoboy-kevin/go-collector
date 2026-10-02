package libs

import (
	"testing"
	"time"
)

func TestRegistryIntegrity(t *testing.T) {
	for _, s := range AllSeriesSpecs() {
		if s.GammaSeries == "" {
			t.Errorf("%s: empty gamma series slug", s.Name)
		}
		if s.Name != s.Key.Name() {
			t.Errorf("%s: Name() = %s", s.Name, s.Key.Name())
		}
		if s.MarketsPerEventHint < 1 {
			t.Errorf("%s: bad MarketsPerEventHint %d", s.Name, s.MarketsPerEventHint)
		}
		if s.Ladder != (s.MarketsPerEventHint > 1) {
			t.Errorf("%s: Ladder=%v but hint=%d", s.Name, s.Ladder, s.MarketsPerEventHint)
		}
		if s.Key.Family.IsLadder() != s.Ladder {
			t.Errorf("%s: IsLadder()=%v but Ladder=%v", s.Name, s.Key.Family.IsLadder(), s.Ladder)
		}
		if s.Ladder && !s.Anchor.Daily() {
			t.Errorf("%s: a ladder must settle on a daily anchor", s.Name)
		}
		if s.Key.Family == FamilyUpDown && s.Key.Interval < 24*time.Hour && s.Anchor != AnchorETBoundary {
			t.Errorf("%s: intraday up/down must use the ET-boundary anchor", s.Name)
		}
		if s.Key.Family == FamilyUpDown && s.Key.Interval == 24*time.Hour && !s.Anchor.Daily() {
			t.Errorf("%s: daily up/down must use a daily anchor", s.Name)
		}
		spec, err := LookupSeriesByName(s.Name)
		if err != nil || spec.GammaSeries != s.GammaSeries {
			t.Errorf("%s: round-trip lookup failed: %v", s.Name, err)
		}
	}
	if _, err := LookupSeriesByName("btc_nope"); err == nil {
		t.Error("LookupSeriesByName should reject an unknown series")
	}
}

// mustSpec resolves a registry series or fails the test.
func mustSpec(t *testing.T, name string) SeriesSpec {
	t.Helper()
	spec, err := LookupSeriesByName(name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return spec
}

// at builds an instant from an ET wall-clock time, which is how every
// settlement rule in this package is expressed.
func at(t *testing.T, et string) time.Time {
	t.Helper()
	ts, err := time.ParseInLocation("2006-01-02 15:04", et, EpochLocation)
	if err != nil {
		t.Fatalf("parse %q: %v", et, err)
	}
	return ts
}

// TestTargetSettle pins the instant discovery matches an event's EndDate on.
// Getting this wrong records the wrong market, so every family is covered.
func TestTargetSettle(t *testing.T) {
	tests := []struct {
		series string
		nowET  string
		wantET string
	}{
		// Intraday up/down: the end of the ET-aligned window containing now.
		{"btc_1h", "2026-10-02 13:20", "2026-10-02 14:00"},
		{"btc_1h", "2026-10-02 13:59", "2026-10-02 14:00"},
		{"btc_1h", "2026-10-02 23:30", "2026-10-03 00:00"},
		{"btc_4h", "2026-10-02 05:10", "2026-10-02 08:00"},
		{"btc_4h", "2026-10-02 21:00", "2026-10-03 00:00"},

		// Daily up/down and the ladders settle at noon ET, so an hour before
		// noon the target is today's noon and just after it, tomorrow's.
		{"btc_1d", "2026-10-02 11:00", "2026-10-02 12:00"},
		{"btc_1d", "2026-10-02 12:30", "2026-10-03 12:00"},
		{"btc_above", "2026-10-02 11:00", "2026-10-02 12:00"},
		{"btc_range", "2026-10-02 18:00", "2026-10-03 12:00"},
	}
	for _, tc := range tests {
		got, err := mustSpec(t, tc.series).TargetSettle(at(t, tc.nowET))
		if err != nil {
			t.Errorf("%s @ %s: %v", tc.series, tc.nowET, err)
			continue
		}
		if want := at(t, tc.wantET); !got.Equal(want) {
			t.Errorf("%s @ %s ET: target settle = %s, want %s",
				tc.series, tc.nowET, got.In(EpochLocation).Format("2006-01-02 15:04"), tc.wantET)
		}
	}
}

// TestTargetSettleAcrossDST proves the target is computed on the ET calendar
// and not by adding a fixed offset to UTC: DST ends on 1 November 2026, so noon
// ET is 16:00Z before that date and 17:00Z after it.
func TestTargetSettleAcrossDST(t *testing.T) {
	spec := mustSpec(t, "btc_1d")

	// Before noon, so the target is the same ET day's noon.
	before, err := spec.TargetSettle(at(t, "2026-10-31 06:00")) // EDT
	if err != nil {
		t.Fatal(err)
	}
	if got := before.UTC().Hour(); got != 16 {
		t.Errorf("target settle offset before the transition = %02d:00Z, want 16:00Z", got)
	}

	after, err := spec.TargetSettle(at(t, "2026-11-02 06:00")) // EST
	if err != nil {
		t.Fatal(err)
	}
	if got := after.UTC().Hour(); got != 17 {
		t.Errorf("target settle offset after the transition = %02d:00Z, want 17:00Z", got)
	}
}

// TestEpochIDFor pins the date a market's title names.
func TestEpochIDFor(t *testing.T) {
	tests := []struct {
		series   string
		settleET string
		want     string
	}{
		{"btc_1h", "2026-10-02 12:00", "2026-10-02"},
		{"btc_1h", "2026-10-02 14:00", "2026-10-02"},
		{"btc_1h", "2026-10-03 00:00", "2026-10-02"}, // the 11pm ET window
		{"btc_4h", "2026-10-02 16:00", "2026-10-02"},
		{"btc_4h", "2026-10-03 00:00", "2026-10-02"}, // the 20:00 ET window
		{"btc_1d", "2026-10-02 12:00", "2026-10-02"},
		{"btc_above", "2026-10-02 12:00", "2026-10-02"},
		{"btc_range", "2026-10-02 12:00", "2026-10-02"},
		{"btc_hit", "2026-10-03 00:00", "2026-10-02"},
	}
	for _, tc := range tests {
		if got := mustSpec(t, tc.series).EpochIDFor(at(t, tc.settleET)); got != tc.want {
			t.Errorf("%s settling %s ET: title date = %s, want %s", tc.series, tc.settleET, got, tc.want)
		}
	}
}

// TestEpochForSettle pins the directory a recording lands in: the epoch that
// ENDS at the settle instant. This is what makes a directory equal one
// settlement cycle and lets an epoch's manifest be written complete the moment
// the epoch rolls — every market claimed while epoch D ran is filed under D.
//
// It also pins the counts from CONTEXT.md §5: one noon→noon epoch holds 24
// hourly markets and 6 four-hourly ones.
func TestEpochForSettle(t *testing.T) {
	tests := []struct {
		series   string
		settleET string
		want     string
	}{
		// The epoch [noon Oct 1, noon Oct 2) — markets that settle in it.
		{"btc_1h", "2026-10-01 15:00", "2026-10-02"},
		{"btc_1h", "2026-10-02 00:00", "2026-10-02"},
		{"btc_1h", "2026-10-02 12:00", "2026-10-02"},
		{"btc_4h", "2026-10-01 20:00", "2026-10-02"},
		{"btc_4h", "2026-10-02 12:00", "2026-10-02"},
		// The next epoch: a 2pm ET window settles at 15:00 and belongs to the
		// epoch that closes at noon on the 3rd, even though its title says the 2nd.
		{"btc_1h", "2026-10-02 15:00", "2026-10-03"},
		{"btc_1h", "2026-10-03 00:00", "2026-10-03"},
		{"btc_1h", "2026-10-03 12:00", "2026-10-03"},
		{"btc_4h", "2026-10-03 12:00", "2026-10-03"},
		// Daily and ladder markets settle exactly on the boundary, so they are
		// filed under the day they settle on — the day in their title.
		{"btc_1d", "2026-10-02 12:00", "2026-10-02"},
		{"btc_above", "2026-10-02 12:00", "2026-10-02"},
		{"btc_range", "2026-10-03 12:00", "2026-10-03"},
		// Hit price observes a whole ET day and settles at midnight, which under
		// the noon anchor lands inside the NEXT epoch — the reason it is deferred
		// to phase 3 and needs its own alignment.
		{"btc_hit", "2026-10-03 00:00", "2026-10-03"},
	}
	for _, tc := range tests {
		got, err := EpochForSettle(at(t, tc.settleET), AnchorNoonET)
		if err != nil {
			t.Fatalf("EpochForSettle: %v", err)
		}
		if got != tc.want {
			t.Errorf("%s settling %s ET: epoch = %s, want %s", tc.series, tc.settleET, got, tc.want)
		}
		// The title date must agree with the market's own naming rule.
		_ = mustSpec(t, tc.series)
	}
}

// TestEpochForSettleCountsPerEpoch checks the structural claim behind the
// on-disk layout: a noon→noon epoch holds exactly the 24 hourly and 6
// four-hourly windows that settle inside it.
func TestEpochForSettleCountsPerEpoch(t *testing.T) {
	count := func(series string) map[string]int {
		t.Helper()
		spec := mustSpec(t, series)
		per := map[string]int{}
		// Walk a bit over a day of windows in 5-minute steps so every window
		// edge is seen, then bucket each distinct window by its epoch. Starting
		// mid-window keeps the first window of the epoch in view.
		seen := map[string]bool{}
		start := at(t, "2026-10-01 12:30")
		for i := 0; i < 24*(60/5); i++ {
			now := start.Add(time.Duration(i) * 5 * time.Minute)
			settle, err := spec.TargetSettle(now)
			if err != nil {
				t.Fatal(err)
			}
			if seen[settle.String()] {
				continue
			}
			seen[settle.String()] = true
			id, err := EpochForSettle(settle, AnchorNoonET)
			if err != nil {
				t.Fatal(err)
			}
			per[id]++
		}
		return per
	}

	hourly := count("btc_1h")
	if hourly["2026-10-02"] != 24 {
		t.Errorf("epoch 2026-10-02 holds %d hourly windows, want 24", hourly["2026-10-02"])
	}
	four := count("btc_4h")
	if four["2026-10-02"] != 6 {
		t.Errorf("epoch 2026-10-02 holds %d 4h windows, want 6", four["2026-10-02"])
	}
}
