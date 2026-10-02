package libs

import (
	"testing"
	"time"
)

// et builds a time from an ET wall-clock string, so the tests read the way the
// market rules do ("12:00 in the ET timezone").
func et(t *testing.T, wall string) time.Time {
	t.Helper()
	ts, err := time.ParseInLocation("2006-01-02 15:04", wall, EpochLocation)
	if err != nil {
		t.Fatalf("parse ET wall time %q: %v", wall, err)
	}
	return ts
}

func TestEpochIDNoonET(t *testing.T) {
	tests := []struct {
		wall string
		want string
	}{
		// Before noon: still inside the epoch that ends at noon today.
		{"2026-10-02 09:00", "2026-10-02"},
		{"2026-10-01 12:00", "2026-10-02"}, // exactly on the start boundary
		// At/after noon: the noon anchor has passed, so this epoch ends tomorrow.
		{"2026-10-02 12:00", "2026-10-03"},
		{"2026-10-02 15:00", "2026-10-03"},
		{"2026-10-02 23:59", "2026-10-03"},
		// DST: US clocks fall back on 2026-11-01, so noon ET is 17:00Z that day.
		{"2026-11-01 11:00", "2026-11-01"},
		{"2026-11-01 13:00", "2026-11-02"},
	}
	for _, tc := range tests {
		got, err := EpochID(et(t, tc.wall), AnchorNoonET)
		if err != nil {
			t.Fatalf("EpochID(%s): %v", tc.wall, err)
		}
		if got != tc.want {
			t.Errorf("EpochID(%s ET) = %s, want %s", tc.wall, got, tc.want)
		}
	}
}

func TestEpochIDMidnightET(t *testing.T) {
	// The epoch is identified by the ET date the anchor lands on, which for a
	// midnight anchor is the day *after* the observed day.
	tests := []struct {
		wall string
		want string
	}{
		{"2026-10-02 00:00", "2026-10-03"}, // exactly on the start boundary
		{"2026-10-02 09:00", "2026-10-03"},
		{"2026-10-02 23:59", "2026-10-03"},
		{"2026-10-03 00:00", "2026-10-04"},
	}
	for _, tc := range tests {
		got, err := EpochID(et(t, tc.wall), AnchorMidnightET)
		if err != nil {
			t.Fatalf("EpochID(%s): %v", tc.wall, err)
		}
		if got != tc.want {
			t.Errorf("EpochID(%s ET, midnight) = %s, want %s", tc.wall, got, tc.want)
		}
	}
}

func TestEpochBoundsNoonET(t *testing.T) {
	start, end, err := EpochBounds("2026-10-03", AnchorNoonET)
	if err != nil {
		t.Fatal(err)
	}
	// Noon ET under EDT is 16:00Z.
	wantStart := time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Errorf("start = %s, want %s", start.UTC(), wantStart)
	}
	if !end.Equal(wantEnd) {
		t.Errorf("end = %s, want %s", end.UTC(), wantEnd)
	}

	// The fall-back day is 25 hours long: the wall-clock hour is preserved
	// across the DST transition (16:00Z → 17:00Z), not the UTC hour.
	start, end, err = EpochBounds("2026-11-01", AnchorNoonET)
	if err != nil {
		t.Fatal(err)
	}
	if got := end.Sub(start); got != 25*time.Hour {
		t.Errorf("fall-back epoch duration = %s, want 25h", got)
	}
	if h := end.UTC().Hour(); h != 17 {
		t.Errorf("noon ET on 2026-11-01 = %02d:00Z, want 17:00Z (EST)", h)
	}
}

func TestEpochContains(t *testing.T) {
	inside, err := EpochContains("2026-10-02", AnchorNoonET, et(t, "2026-10-01 12:00"))
	if err != nil {
		t.Fatal(err)
	}
	if !inside {
		t.Error("noon Oct 1 should be the start boundary of epoch 2026-10-02")
	}
	inside, err = EpochContains("2026-10-02", AnchorNoonET, et(t, "2026-10-02 12:00"))
	if err != nil {
		t.Fatal(err)
	}
	if inside {
		t.Error("noon Oct 2 is the exclusive end of epoch 2026-10-02")
	}
}

func TestShiftAndNextEpochID(t *testing.T) {
	got, err := ShiftEpochID("2026-10-02", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-10-03" {
		t.Errorf("ShiftEpochID(+1) = %s, want 2026-10-03", got)
	}
	got, err = ShiftEpochID("2026-11-01", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "2026-11-02" {
		t.Errorf("ShiftEpochID across DST = %s, want 2026-11-02", got)
	}
	next, err := NextEpochID(et(t, "2026-10-02 15:00"), AnchorNoonET)
	if err != nil {
		t.Fatal(err)
	}
	if next != "2026-10-04" {
		t.Errorf("NextEpochID = %s, want 2026-10-04", next)
	}
}

func TestIntervalWindowAlignsToETGrid(t *testing.T) {
	tests := []struct {
		wall      string
		interval  time.Duration
		wantStart string
	}{
		{"2026-10-02 13:37", 4 * time.Hour, "2026-10-02 12:00"}, // 4h windows: 12–16, 16–20, ...
		{"2026-10-02 00:30", 4 * time.Hour, "2026-10-02 00:00"},
		{"2026-10-02 05:15", time.Hour, "2026-10-02 05:00"},
		{"2026-10-02 05:15", 5 * time.Minute, "2026-10-02 05:15"},
		{"2026-10-02 05:14", 15 * time.Minute, "2026-10-02 05:00"},
	}
	for _, tc := range tests {
		start, end := IntervalWindow(et(t, tc.wall), tc.interval)
		if got := start.In(EpochLocation).Format("2006-01-02 15:04"); got != tc.wantStart {
			t.Errorf("IntervalWindow(%s, %s) start = %s, want %s",
				tc.wall, tc.interval, got, tc.wantStart)
		}
		if got := end.Sub(start); got != tc.interval {
			t.Errorf("IntervalWindow(%s, %s) span = %s, want %s",
				tc.wall, tc.interval, got, tc.interval)
		}
	}
}

func TestIntervalWindowsTileContiguously(t *testing.T) {
	// Walk a fall-back day (2026-11-01): midnight-to-midnight ET is 25 absolute
	// hours there, so an hourly grid yields 25 windows. The property that
	// matters is that each window starts exactly where the previous one ended —
	// the scheduler relies on it to avoid gaps or overlaps when bucketing.
	cursor := et(t, "2026-11-01 00:00")
	dayEnd := et(t, "2026-11-02 00:00")
	firstStart := cursor

	windows := 0
	for cursor.Before(dayEnd) {
		start, end := IntervalWindow(cursor, time.Hour)
		if !start.Equal(cursor) {
			t.Fatalf("window %d starts at %s, want %s", windows, start, cursor)
		}
		if end.After(dayEnd) {
			t.Fatalf("window %d ends at %s, past the ET midnight %s", windows, end, dayEnd)
		}
		cursor = end
		windows++
	}

	if windows != 25 {
		t.Errorf("hourly windows on the fall-back day = %d, want 25", windows)
	}
	if got := cursor.Sub(firstStart); got != 25*time.Hour {
		t.Errorf("windows cover %s, want 25h", got)
	}
}

// TestIntervalWindowMatchesTheLiveDSTGrid pins the ET window grid against the
// boundaries Polymarket actually published across a DST transition, read back
// from Gamma (series `btc-up-or-down-4h`, id 10331) on 2026-10-02.
//
// On 2026-03-08 US clocks jump 02:00→03:00 EST. From local midnight EST the
// series ran the 4h grid 05:00Z, 09:00Z, 13:00Z, 17:00Z, 21:00Z, 01:00Z — so the
// window [05:00Z, 09:00Z) is labelled "12:00AM-5:00AM ET": five wall-clock hours
// but four absolute ones. That is exactly what IntervalWindow derives (local
// midnight plus whole intervals), and it is the property this test protects: the
// grid is anchored to ET midnight by wall clock, never to a UTC truncation.
//
// The same day also carried a parallel wall-clock grid (04:00Z, 08:00Z, 12:00Z,
// 16:00Z, 20:00Z, 00:00Z …), so a transition day offers two overlapping families.
// The recorder follows whichever family its grid lands on; both are real markets,
// and the grid is re-anchored at the next ET midnight either way.
func TestIntervalWindowMatchesTheLiveDSTGrid(t *testing.T) {
	const interval = 4 * time.Hour

	// slug boundary == window start, event endDate == window end.
	observed := []struct {
		start string
		end   string
		title string
	}{
		{"2026-03-08T05:00:00Z", "2026-03-08T09:00:00Z", "March 8, 12:00AM-5:00AM ET"},
		{"2026-03-08T09:00:00Z", "2026-03-08T13:00:00Z", "March 8, 5:00AM-9:00AM ET"},
		{"2026-03-08T13:00:00Z", "2026-03-08T17:00:00Z", "March 8, 9:00AM-1:00PM ET"},
		{"2026-03-08T17:00:00Z", "2026-03-08T21:00:00Z", "March 8, 1:00PM-5:00PM ET"},
		{"2026-03-08T21:00:00Z", "2026-03-09T01:00:00Z", "March 8, 5:00PM-9:00PM ET"},
		{"2026-03-09T01:00:00Z", "2026-03-09T05:00:00Z", "March 8, 9:00PM-1:00AM ET"},
	}
	for _, tc := range observed {
		start, err := time.Parse(time.RFC3339, tc.start)
		if err != nil {
			t.Fatal(err)
		}
		end, err := time.Parse(time.RFC3339, tc.end)
		if err != nil {
			t.Fatal(err)
		}
		// A window contains every instant inside it, so ask about its midpoint.
		mid := start.Add(end.Sub(start) / 2)
		gotStart, gotEnd := IntervalWindow(mid, interval)
		if !gotStart.Equal(start) || !gotEnd.Equal(end) {
			t.Errorf("%s: IntervalWindow(%s) = [%s, %s), want [%s, %s)",
				tc.title, mid.Format(time.RFC3339),
				gotStart.UTC().Format(time.RFC3339), gotEnd.UTC().Format(time.RFC3339),
				tc.start, tc.end)
		}
	}

	// The preceding day is still on the aligned grid (09:00Z = 04:00 EST).
	before, err := time.Parse(time.RFC3339, "2026-03-07T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	s, e := IntervalWindow(before, interval)
	if got := s.UTC().Format(time.RFC3339); got != "2026-03-07T09:00:00Z" {
		t.Errorf("window containing %s starts at %s, want 09:00Z", before.Format(time.RFC3339), got)
	}
	if got := e.UTC().Format(time.RFC3339); got != "2026-03-07T13:00:00Z" {
		t.Errorf("window containing %s ends at %s, want 13:00Z", before.Format(time.RFC3339), got)
	}

	// And the day after is back on the wall-clock grid: local midnight is now
	// 04:00Z, so the grid shifted by an hour in UTC and the local labels realign.
	after, err := time.Parse(time.RFC3339, "2026-03-09T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	s, e = IntervalWindow(after, interval)
	if got := s.UTC().Format(time.RFC3339); got != "2026-03-09T08:00:00Z" {
		t.Errorf("window containing %s starts at %s, want 08:00Z (04:00 ET)",
			after.Format(time.RFC3339), got)
	}
	if got := e.UTC().Format(time.RFC3339); got != "2026-03-09T12:00:00Z" {
		t.Errorf("window containing %s ends at %s, want 12:00Z", after.Format(time.RFC3339), got)
	}
}

// TestHourlyGridAcrossTheFallBack pins the other half of the DST edge. On
// 2025-11-02 the ET clock falls back, so the local 01:00–02:00 hour happens
// twice and an hour-anchored grid must produce two windows for it: 05:00Z–06:00Z
// (01:00 EDT) and 06:00Z–07:00Z (01:00 EST).
//
// Those two windows are exactly why the collector warns when a target has no
// event: Gamma published NEITHER of them that day — the series jumps from the
// 12:00AM event ending 05:00Z to the 2:00AM one ending 08:00Z (verified against
// Gamma on 2026-10-02) — so the honest outcome is a recorded hole, not a
// synthesized market.
func TestHourlyGridAcrossTheFallBack(t *testing.T) {
	day, err := time.Parse(time.RFC3339, "2025-11-02T04:00:00Z") // 00:00 EDT
	if err != nil {
		t.Fatal(err)
	}
	wantEnds := []string{
		"2025-11-02T05:00:00Z", "2025-11-02T06:00:00Z", "2025-11-02T07:00:00Z",
		"2025-11-02T08:00:00Z", "2025-11-02T09:00:00Z", "2025-11-02T10:00:00Z",
	}
	cursor := day
	for i, want := range wantEnds {
		start, end := IntervalWindow(cursor, time.Hour)
		if !start.Equal(cursor) {
			t.Fatalf("window %d starts at %s, want %s", i,
				start.UTC().Format(time.RFC3339), cursor.UTC().Format(time.RFC3339))
		}
		if got := end.UTC().Format(time.RFC3339); got != want {
			t.Errorf("window %d ends at %s, want %s", i, got, want)
		}
		cursor = end
	}

	// The repeated hour, labelled: two windows, one hour apart in UTC, both
	// starting at local 01:00 — the first under EDT, the second under EST.
	first, _ := IntervalWindow(time.Date(2025, 11, 2, 5, 30, 0, 0, time.UTC), time.Hour)
	second, _ := IntervalWindow(time.Date(2025, 11, 2, 6, 30, 0, 0, time.UTC), time.Hour)
	if got := first.In(EpochLocation).Format("15:04 MST"); got != "01:00 EDT" {
		t.Errorf("first pass of the repeated hour starts at %s, want 01:00 EDT", got)
	}
	if got := second.In(EpochLocation).Format("15:04 MST"); got != "01:00 EST" {
		t.Errorf("second pass of the repeated hour starts at %s, want 01:00 EST", got)
	}
}

func TestSettlementInstant(t *testing.T) {
	noon, err := SettlementInstant(AnchorNoonET, "2026-10-03")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC); !noon.Equal(want) {
		t.Errorf("noon settlement = %s, want %s", noon.UTC(), want)
	}

	// Hit price observed on Oct 2 settles when that ET day ends.
	midnight, err := SettlementInstant(AnchorMidnightET, "2026-10-02")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 3, 4, 0, 0, 0, time.UTC); !midnight.Equal(want) {
		t.Errorf("midnight settlement = %s, want %s", midnight.UTC(), want)
	}
}

func TestParseEpochAnchor(t *testing.T) {
	if a, err := ParseEpochAnchor(""); err != nil || a != AnchorNoonET {
		t.Errorf(`ParseEpochAnchor("") = %q, %v; want %q, nil`, a, err, AnchorNoonET)
	}
	if _, err := ParseEpochAnchor("nonsense"); err == nil {
		t.Error("ParseEpochAnchor should reject an unknown anchor")
	}
	if AnchorETBoundary.Daily() {
		t.Error("AnchorETBoundary must not report itself as a daily anchor")
	}
	if _, err := EpochID(time.Now(), AnchorETBoundary); err == nil {
		t.Error("EpochID must reject a non-daily anchor")
	}
}
