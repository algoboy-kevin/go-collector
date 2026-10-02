package collector

import (
	"errors"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Resolution routing and the resolved-market guard. These pin two things the
// collector cannot get wrong silently:
//
//  1. A market_resolved event names its market three different ways depending on
//     the payload (`winning_asset_id`, `market` = the condition id, and `id`),
//     and for a neg-risk ladder the last one can be the shared GROUP id rather
//     than the rung's — routing on it alone left the rung unresolved, to be
//     truncated at shutdown instead (CONTEXT.md §13.2).
//  2. A restart in the last minutes before settlement can claim a market that
//     already resolved on-chain (CONTEXT.md §13.3).

// negRiskMarket is a one-market event whose market carries a neg-risk group id —
// the shape a price-range ladder has in the Gamma payload.
func negRiskEvent(slug string, settleAt time.Time, marketID string) connector.GammaEvent {
	m := gammaMarket(marketID, slug, "<74,000", "Yes", "No")
	m.NegRisk = true
	m.NegRiskMarketID = "0xgroup"
	ev := upDownEvent(slug, settleAt)
	ev.Markets = []connector.GammaMarket{m}
	return ev
}

// startRangeSession claims one neg-risk range market and returns it with the
// collector it was started on.
func startRangeSession(t *testing.T, marketID string) (*EventCollector, *stubConn, *RecordingSession) {
	t.Helper()
	ec, conn := newTestCollector(t)
	settleAt := time.Now().Add(6 * time.Hour)
	started, skipped, err := ec.StartEventSessions(EventTarget{
		Spec:     ladderSpec(t),
		Event:    negRiskEvent("bitcoin-price-on-october-2-2026", settleAt, marketID),
		SettleAt: settleAt,
		EpochID:  "2026-10-02",
	})
	if err != nil {
		t.Fatalf("StartEventSessions: %v", err)
	}
	if len(started) != 1 || skipped != 0 {
		t.Fatalf("started %d markets (skipped %d), want 1 (0)", len(started), skipped)
	}
	return ec, conn, started[0].Session
}

func TestHandleEventRoutesResolutionByWinningAssetConditionThenMarketID(t *testing.T) {
	// The three joins the collector holds, each in isolation: the keys the tested
	// one could fall back on are blanked out, so a payload only that key can route
	// still resolves.
	tests := []struct {
		name           string
		winning        string
		condition      string
		market         string
		winningOutcome string
		wantOutcome    string
	}{
		{
			// The token wins over the label — the rung is the NO leg even though the
			// payload's label says Yes.
			name:    "winning asset id",
			winning: "m-range-no",
			// The neg-risk group id, which is NOT the market id: routing on it alone
			// would leave this rung unresolved (CONTEXT.md §13.2).
			market:         "0xgroup",
			winningOutcome: "Yes",
			wantOutcome:    "NO",
		},
		{
			name:           "condition id",
			condition:      "0xm-range",
			winningOutcome: "Yes",
			wantOutcome:    "YES",
		},
		{
			name:           "market id",
			market:         "m-range",
			winningOutcome: "Yes",
			wantOutcome:    "YES",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ec, _, sess := startRangeSession(t, "m-range")
			ec.HandleEvent(&connector.MarketResolvedEvent{
				MarketID:       tc.market,
				ConditionID:    tc.condition,
				WinningAssetID: tc.winning,
				WinningOutcome: tc.winningOutcome,
			})
			if got := sess.State(); got != SessionFinalized {
				t.Fatalf("session state = %s, want %s", got, SessionFinalized)
			}
			if sess.winningOutcome != tc.wantOutcome {
				t.Errorf("winning outcome = %q, want %q", sess.winningOutcome, tc.wantOutcome)
			}
			want := ResolutionYes
			if tc.wantOutcome == "NO" {
				want = ResolutionNo
			}
			if got := sess.meta.ResolutionFor(sess.winningOutcome); got != want {
				t.Errorf("resolution = %q, want %q", got, want)
			}
		})
	}
}

func TestHandleEventFallsBackToTheOutcomeLabel(t *testing.T) {
	// A payload with no winning_asset_id would otherwise be recorded as a YES win
	// (Resolve treats every non-NO token as YES), so the label decides.
	ec, _, sess := startRangeSession(t, "m-label")
	ec.HandleEvent(&connector.MarketResolvedEvent{
		ConditionID:    "0xm-label",
		WinningOutcome: "No",
	})
	if sess.winningOutcome != "NO" {
		t.Errorf("winning outcome = %q, want NO from the outcome label", sess.winningOutcome)
	}

	ec, _, sess = startRangeSession(t, "m-label-up")
	ec.HandleEvent(&connector.MarketResolvedEvent{
		ConditionID:    "0xm-label-up",
		WinningOutcome: "Up",
	})
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES from the outcome label", sess.winningOutcome)
	}
}

func TestHandleEventWarnsOnNegRiskGroupResolution(t *testing.T) {
	// The payload named the neg-risk GROUP, and carried no winning asset id, so
	// there is no way to tell which rung resolved: the session must stay
	// recording (to be truncated) rather than be resolved as a guess.
	ec, _, sess := startRangeSession(t, "m-group")
	ec.HandleEvent(&connector.MarketResolvedEvent{
		MarketID:       "0xgroup",
		WinningOutcome: "Yes",
	})
	if got := sess.State(); got != SessionRecording {
		t.Errorf("session state = %s, want %s (unroutable resolution must not resolve)", got, SessionRecording)
	}
	if sess.EventCount() != 0 {
		t.Errorf("recorded %d events, want 0 (an unroutable resolution is not the market's data)", sess.EventCount())
	}
}

func TestStartEventSessionsRefusesAlreadyResolvedMarket(t *testing.T) {
	const marketID = "m-guard"

	tests := []struct {
		name        string
		settleIn    time.Duration
		resolution  connector.Resolution
		lookupErr   error
		wantStarted int
		wantSkipped int
		wantLookups int
	}{
		{
			name:     "resolved inside the check window",
			settleIn: 5 * time.Minute, resolution: connector.ResYes,
			wantLookups: 1, wantSkipped: 1,
		},
		{
			name:        "unresolved inside the check window",
			settleIn:    5 * time.Minute,
			wantLookups: 1, wantStarted: 1,
		},
		{
			// Far from settlement the guard is skipped entirely: the closed flag
			// and the settleAt check cover the normal case, and an HTTP round trip
			// per claim is not worth paying for it.
			name:     "resolved but far from settlement: not even checked",
			settleIn: 2 * time.Hour, resolution: connector.ResYes,
			wantStarted: 1,
		},
		{
			// A Gamma failure must never stop us recording a live market.
			name:     "lookup fails: claim anyway",
			settleIn: 5 * time.Minute, resolution: connector.ResYes, lookupErr: errors.New("gamma down"),
			wantLookups: 1, wantStarted: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ec, conn := newTestCollector(t)
			if tc.resolution != "" {
				conn.resolved[marketID] = tc.resolution
			}
			conn.resolveErr = tc.lookupErr

			settleAt := time.Now().Add(tc.settleIn)
			started, skipped, err := ec.StartEventSessions(EventTarget{
				Spec:     ladderSpec(t),
				Event:    negRiskEvent("bitcoin-price-on-october-2-2026", settleAt, marketID),
				SettleAt: settleAt,
				EpochID:  "2026-10-02",
			})
			if err != nil {
				t.Fatalf("StartEventSessions: %v", err)
			}
			if len(started) != tc.wantStarted {
				t.Errorf("started %d markets, want %d", len(started), tc.wantStarted)
			}
			if skipped != tc.wantSkipped {
				t.Errorf("skipped %d markets, want %d", skipped, tc.wantSkipped)
			}
			if conn.resolutionCalls != tc.wantLookups {
				t.Errorf("resolution lookups = %d, want %d", conn.resolutionCalls, tc.wantLookups)
			}
		})
	}
}

// TestSettlementWindowStart pins the window the settlement compares: the
// family's own interval back from the anchor. It is what post-processing
// derives open_price/close_price over, and deriving it from the family name
// instead would put the convention in the reader rather than in the file.
func TestSettlementWindowStart(t *testing.T) {
	settleAt := time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC) // noon ET

	tests := []struct {
		series   string
		wantBack time.Duration
	}{
		{"btc_1h", time.Hour},
		{"btc_4h", 4 * time.Hour},
		{"btc_1d", 24 * time.Hour}, // the previous noon — the candle it compares
		{"btc_above", 24 * time.Hour},
		{"btc_hit", 24 * time.Hour}, // midnight ET: the whole observed ET day
	}
	for _, tc := range tests {
		spec := mustSpecByName(t, tc.series)
		venue, err := venueFor(spec)
		if err != nil {
			t.Fatalf("%s: venueFor: %v", tc.series, err)
		}
		info := settlementInfo(EventTarget{Spec: spec, SettleAt: settleAt},
			connector.GammaMarket{}, StrikeRange{}, venue, "ref")
		if got, want := info.WindowStart, settleAt.Add(-tc.wantBack).UnixMilli(); got != want {
			t.Errorf("%s: window_start = %s, want %s (%s before the anchor)",
				tc.series, time.UnixMilli(got).UTC(), time.UnixMilli(want).UTC(), tc.wantBack)
		}
		if info.WindowStart >= info.At {
			t.Errorf("%s: window_start %s must precede the anchor %s",
				tc.series, time.UnixMilli(info.WindowStart).UTC(), time.UnixMilli(info.At).UTC())
		}
	}

	// The hit-price day is the ET calendar day named in the title: its window
	// opens at midnight ET on the title date and closes at the next midnight.
	hit := mustSpecByName(t, "btc_hit")
	_, end, err := libs.EpochBounds("2026-10-03", libs.AnchorMidnightET)
	if err != nil {
		t.Fatal(err)
	}
	venue, err := venueFor(hit)
	if err != nil {
		t.Fatal(err)
	}
	info := settlementInfo(EventTarget{Spec: hit, SettleAt: end}, connector.GammaMarket{}, StrikeRange{}, venue, "ref")
	if want := end.Add(-24 * time.Hour); info.WindowStart != want.UnixMilli() {
		t.Errorf("hit: window_start = %s, want midnight ET of the title date (%s)",
			time.UnixMilli(info.WindowStart).UTC(), want.UTC())
	}
}
