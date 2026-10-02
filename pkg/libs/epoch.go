// Package libs — recording epochs.
//
// Polymarket's crypto markets are defined against **ET wall-clock** times
// ("the Binance 1 minute candle for BTC/USDT 12:00 in the ET timezone (noon)").
// Every recording epoch boundary must therefore be computed in
// America/New_York, **never** by truncating a UTC time:
//
//	time.Now().Truncate(24 * time.Hour)  // WRONG — that is UTC midnight = 20:00 ET
//
// An epoch is identified by the ET calendar date on which its anchor instant
// falls, i.e. the day the markets settle. See CONTEXT.md §2.
package libs

import (
	"fmt"
	"time"

	// Embed the IANA timezone database. The collector ships as a
	// CGO_ENABLED=0 Linux binary; without this, time.LoadLocation("America/New_York")
	// depends on /usr/share/zoneinfo existing on the target host.
	_ "time/tzdata"
)

// EpochLocation is the timezone every epoch boundary is anchored to.
var EpochLocation = mustLoadLocation("America/New_York")

// EpochDateLayout is the on-disk / on-wire format of an epoch id.
const EpochDateLayout = "2006-01-02"

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(fmt.Sprintf("libs: cannot load timezone %q: %v", name, err))
	}
	return loc
}

// EpochAnchor identifies the daily boundary rule an epoch is cut on.
type EpochAnchor string

const (
	// AnchorETBoundary is used by the intraday families (5m/15m/1h/4h), whose
	// windows are ET-aligned and settle at the end of their own window. There is
	// no single "daily" hour for these; the window start is derived from the ET
	// grid via IntervalWindow.
	AnchorETBoundary EpochAnchor = "et_boundary"

	// AnchorNoonET cuts the day at 12:00 ET: epoch `D` covers
	// [12:00 ET on D-1, 12:00 ET on D). This is the default because up/down-daily
	// compares the 12:00 ET candle of the previous day against the title day's,
	// and above/below + price-range settle on the 12:00 ET candle of their title
	// date — so one epoch == one settlement cycle.
	AnchorNoonET EpochAnchor = "noon_et"

	// AnchorMidnightET cuts the day at 00:00 ET: used by hit-price, whose events
	// observe an entire ET calendar day and settle at its end.
	AnchorMidnightET EpochAnchor = "midnight_et"
)

// epochHour returns the ET wall-clock hour of the anchor, and whether the
// anchor is a daily rule at all.
func (a EpochAnchor) epochHour() (int, bool) {
	switch a {
	case AnchorNoonET:
		return 12, true
	case AnchorMidnightET:
		return 0, true
	default:
		return 0, false
	}
}

// Valid reports whether the anchor is a recognised daily epoch rule.
func (a EpochAnchor) Valid() bool {
	_, ok := a.epochHour()
	return ok
}

// Daily reports whether the anchor cuts whole days (as opposed to
// AnchorETBoundary, which follows the series interval).
func (a EpochAnchor) Daily() bool { return a.Valid() }

// ParseEpochAnchor parses a config string, defaulting to AnchorNoonET for "".
func ParseEpochAnchor(s string) (EpochAnchor, error) {
	switch s {
	case "":
		return AnchorNoonET, nil
	case string(AnchorNoonET):
		return AnchorNoonET, nil
	case string(AnchorMidnightET):
		return AnchorMidnightET, nil
	case string(AnchorETBoundary):
		return AnchorETBoundary, nil
	default:
		return "", fmt.Errorf("libs: unknown epoch anchor %q (want %q, %q or %q)",
			s, AnchorNoonET, AnchorMidnightET, AnchorETBoundary)
	}
}

// anchorAt returns the anchor instant on the ET calendar date of day.
func anchorAt(day time.Time, hour int) time.Time {
	y, m, d := day.In(EpochLocation).Date()
	return time.Date(y, m, d, hour, 0, 0, 0, EpochLocation)
}

// EpochID returns the id of the epoch containing t: the ET calendar date on
// which that epoch's anchor instant falls.
//
//	anchor = noon ET, t = Oct 2 09:00 ET → "2026-10-02"  (epoch [noon Oct 1, noon Oct 2))
//	anchor = noon ET, t = Oct 2 15:00 ET → "2026-10-03"  (epoch [noon Oct 2, noon Oct 3))
//	anchor = midnight ET, t = Oct 2 09:00 ET → "2026-10-03" (epoch [00:00 Oct 2, 00:00 Oct 3))
func EpochID(t time.Time, a EpochAnchor) (string, error) {
	hour, ok := a.epochHour()
	if !ok {
		return "", fmt.Errorf("libs: EpochID requires a daily anchor, got %q", a)
	}
	tl := t.In(EpochLocation)
	if anchorAt(tl, hour).After(tl) {
		return tl.Format(EpochDateLayout), nil // anchor later today → epoch ends today
	}
	return tl.AddDate(0, 0, 1).Format(EpochDateLayout), nil // anchor already passed → ends tomorrow
}

// EpochIDAt is EpochID, panicking on an invalid anchor. Convenience for call
// sites that already validated the anchor at startup.
func EpochIDAt(t time.Time, a EpochAnchor) string {
	id, err := EpochID(t, a)
	if err != nil {
		panic(err)
	}
	return id
}

// EpochBounds returns the half-open window [start, end) of an epoch.
//
// The previous day's boundary is computed with AddDate on the ET calendar date
// so the wall-clock hour is preserved across a DST transition (only the UTC
// offset changes).
func EpochBounds(id string, a EpochAnchor) (start, end time.Time, err error) {
	hour, ok := a.epochHour()
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("libs: EpochBounds requires a daily anchor, got %q", a)
	}
	day, err := time.ParseInLocation(EpochDateLayout, id, EpochLocation)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("libs: bad epoch id %q (want YYYY-MM-DD): %w", id, err)
	}
	end = anchorAt(day, hour)
	prev := day.AddDate(0, 0, -1)
	start = anchorAt(prev, hour)
	return start, end, nil
}

// EpochContains reports whether t falls inside the given epoch.
func EpochContains(id string, a EpochAnchor, t time.Time) (bool, error) {
	start, end, err := EpochBounds(id, a)
	if err != nil {
		return false, err
	}
	return !t.Before(start) && t.Before(end), nil
}

// NextEpochID returns the epoch that follows the one containing t.
func NextEpochID(t time.Time, a EpochAnchor) (string, error) {
	id, err := EpochID(t, a)
	if err != nil {
		return "", err
	}
	day, err := time.ParseInLocation(EpochDateLayout, id, EpochLocation)
	if err != nil {
		return "", err
	}
	return day.AddDate(0, 0, 1).Format(EpochDateLayout), nil
}

// ShiftEpochID moves an epoch id by n ET calendar days.
func ShiftEpochID(id string, n int) (string, error) {
	day, err := time.ParseInLocation(EpochDateLayout, id, EpochLocation)
	if err != nil {
		return "", fmt.Errorf("libs: bad epoch id %q: %w", id, err)
	}
	return day.AddDate(0, 0, n).Format(EpochDateLayout), nil
}

// IntervalWindow returns the ET-aligned window [start, end) of the given
// interval that contains t.
//
// ET-aligned means the window boundaries lie on ET multiples of interval.
// Because the ET offset is a whole number of hours, hourly and 4-hourly
// windows coincide with the corresponding UTC grid; the daily window does
// not, which is why daily epochs go through EpochBounds instead.
func IntervalWindow(t time.Time, interval time.Duration) (start, end time.Time) {
	if interval <= 0 {
		panic("libs: IntervalWindow requires a positive interval")
	}
	tl := t.In(EpochLocation)
	// Midnight ET on t's ET calendar day, then floor the elapsed time.
	midnight := time.Date(tl.Year(), tl.Month(), tl.Day(), 0, 0, 0, 0, EpochLocation)
	elapsed := tl.Sub(midnight)
	steps := elapsed / interval
	start = midnight.Add(steps * interval)
	return start, start.Add(interval)
}

// SettlementInstant returns the instant at which a market settles, derived from
// its settlement anchor and the ET date in its title.
//
//	AnchorNoonET     → 12:00 ET on the title date
//	AnchorMidnightET → 00:00 ET the day *after* the title date (the end of the
//	                   observed ET calendar day)
//	AnchorETBoundary → the end of the ET-aligned window starting at titleTime
func SettlementInstant(anchor EpochAnchor, titleDateET string) (time.Time, error) {
	day, err := time.ParseInLocation(EpochDateLayout, titleDateET, EpochLocation)
	if err != nil {
		return time.Time{}, fmt.Errorf("libs: bad ET title date %q: %w", titleDateET, err)
	}
	switch anchor {
	case AnchorNoonET:
		return anchorAt(day, 12), nil
	case AnchorMidnightET:
		return anchorAt(day.AddDate(0, 0, 1), 0), nil
	default:
		return time.Time{}, fmt.Errorf("libs: SettlementInstant needs a daily anchor, got %q", anchor)
	}
}
