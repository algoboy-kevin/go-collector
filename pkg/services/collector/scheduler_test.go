package collector

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Epoch scheduler tests. These pin the contract from CONTEXT.md §3/§4/§6:
// a market is *discovered* by matching the event that settles at the target
// instant (never by window containment), a ladder event fans out to one session
// per rung, and the epoch manifest records what was claimed — including the
// "+1" ladder event that is recorded now but settles next epoch.

// ── Test doubles ────────────────────────────────────────────

// stubConn records WS subscriptions and canned on-chain resolutions. The
// embedded nil interface means any *other* connector method a test unexpectedly
// reaches panics loudly.
type stubConn struct {
	connector.ExchangeConnector
	subscribed []string
	resolveErr error
	// resolutionCalls counts GetResolution calls, so a test can prove the guard
	// does not put the claim path on the network when it need not.
	resolutionCalls int
	// resolved maps a market id to the outcome the chain reports for it; an
	// absent market is unresolved (nil, nil — the connector's own convention).
	resolved map[string]connector.Resolution
}

func (s *stubConn) Subscribe(assetIDs []string)   { s.subscribed = append(s.subscribed, assetIDs...) }
func (s *stubConn) Unsubscribe(assetIDs []string) {}

func (s *stubConn) GetResolution(marketID string) (*connector.Resolution, error) {
	s.resolutionCalls++
	if s.resolveErr != nil {
		return nil, s.resolveErr
	}
	res, ok := s.resolved[marketID]
	if !ok {
		return nil, nil
	}
	return &res, nil
}

// stubDiscovery serves canned Gamma responses and counts the calls, so a test
// can prove that reconciling repeatedly does not hammer the API.
type stubDiscovery struct {
	series      map[string][]connector.GammaEvent // gamma slug → open events
	seriesCalls int
	eventCalls  int
}

func (s *stubDiscovery) GetSeries(_ context.Context, slug string) ([]connector.GammaSeries, error) {
	s.seriesCalls++
	if _, ok := s.series[slug]; !ok {
		return nil, nil
	}
	return []connector.GammaSeries{{ID: "id-" + slug, Slug: slug, Recurrence: "hourly"}}, nil
}

func (s *stubDiscovery) ListSeriesEvents(_ context.Context, q connector.EventQuery) ([]connector.GammaEvent, error) {
	s.eventCalls++
	slug := strings.TrimPrefix(q.SeriesID, "id-")
	return s.series[slug], nil
}

// ── Fixtures ────────────────────────────────────────────────

// gammaMarket builds a Gamma market the way the API returns one: outcomes and
// clob token ids arrive as JSON-encoded, index-aligned strings.
func gammaMarket(id, slug, label string, outcomes ...string) connector.GammaMarket {
	tokens := make([]string, len(outcomes))
	for i, o := range outcomes {
		tokens[i] = id + "-" + strings.ToLower(o)
	}
	oj, _ := json.Marshal(outcomes)
	tj, _ := json.Marshal(tokens)
	return connector.GammaMarket{
		ID:             id,
		Slug:           slug,
		Question:       "q " + slug,
		ConditionID:    "0x" + id,
		GroupItemTitle: label,
		Outcomes:       string(oj),
		ClobTokenIDs:   string(tj),
		Description:    "resolution rule for " + slug,
	}
}

// upDownEvent builds a single-market event settling at settleAt.
func upDownEvent(slug string, settleAt time.Time) connector.GammaEvent {
	return connector.GammaEvent{
		ID:        "ev-" + slug,
		Slug:      slug,
		Ticker:    slug,
		Title:     slug,
		StartDate: settleAt.Add(-2 * time.Hour), // trading opens ~2 days early in practice
		EndDate:   settleAt,
		Markets:   []connector.GammaMarket{gammaMarket("m-"+slug, slug, "", "Up", "Down")},
	}
}

// ladderEvent builds an 11-rung strike ladder settling at settleAt, with rung
// ids namespaced by the slug so two dated events never share a market id.
func ladderEvent(slug string, settleAt time.Time, idPrefix string) connector.GammaEvent {
	ev := connector.GammaEvent{
		ID:        "ev-" + slug,
		Slug:      slug,
		Ticker:    slug,
		Title:     slug,
		StartDate: settleAt.AddDate(0, 0, -7),
		EndDate:   settleAt,
	}
	for i, strike := range []int{74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94} {
		label := itoa(strike) + ",000"
		ev.Markets = append(ev.Markets,
			gammaMarket(idPrefix+"-"+itoa(i), slug+"-"+itoa(strike), label, "Yes", "No"))
	}
	return ev
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

// newTestCollector builds a collector on a temp dir with a subscription stub.
func newTestCollector(t *testing.T) (*EventCollector, *stubConn) {
	t.Helper()
	conn := &stubConn{resolved: map[string]connector.Resolution{}}
	ec := NewCollector(CollectorConfig{
		RecordingDir: t.TempDir(),
		EpochAnchor:  libs.AnchorNoonET,
	}, conn)

	// Sessions arm a synthetic-resolve timer; stop them so the test process
	// does not carry live timers, and leave the recordings unfinalized so no
	// test's files depend on another's cleanup.
	t.Cleanup(func() {
		ec.mu.Lock()
		defer ec.mu.Unlock()
		for _, s := range ec.sessions {
			if s.syntheticTimer != nil {
				s.syntheticTimer.Stop()
			}
		}
	})
	return ec, conn
}

// hourlySpec / ladderSpec resolve registry rows used throughout.
func hourlySpec(t *testing.T) libs.SeriesSpec { return mustSpecByName(t, "btc_1h") }
func ladderSpec(t *testing.T) libs.SeriesSpec { return mustSpecByName(t, "btc_above") }

func mustSpecByName(t *testing.T, name string) libs.SeriesSpec {
	t.Helper()
	spec, err := libs.LookupSeriesByName(name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return spec
}

// ── Event selection ─────────────────────────────────────────

func TestSelectEventsMatchesOnEndDateOnly(t *testing.T) {
	target := time.Date(2026, 10, 2, 14, 0, 0, 0, time.UTC)

	// Two events whose TRADING windows contain the target, plus the one that
	// actually settles then. Picking by containment would record the wrong
	// market — that is the bug this rule exists to prevent.
	early := upDownEvent("bitcoin-up-or-down-october-2-2026-1pm-et", target.Add(-time.Hour))
	early.StartDate = target.Add(-48 * time.Hour)
	early.EndDate = target.Add(-time.Hour)

	late := upDownEvent("bitcoin-up-or-down-october-2-2026-3pm-et", target.Add(time.Hour))
	late.StartDate = target.Add(-48 * time.Hour)
	late.EndDate = target.Add(time.Hour)

	right := upDownEvent("bitcoin-up-or-down-october-2-2026-2pm-et", target)

	got, err := SelectEvents([]connector.GammaEvent{early, late, right}, target)
	if err != nil {
		t.Fatalf("SelectEvents: %v", err)
	}
	if len(got) != 1 || got[0].Slug != right.Slug {
		t.Fatalf("selected %d events (%v), want just %s", len(got), slugsOf(got), right.Slug)
	}

	// Several events settling at the same instant are ALL returned, in slug
	// order: on a DST transition day Polymarket's generator emits two markets
	// ending at the same instant (2026-03-08: "…-12am-et" and "…-1am-et", both
	// ending 06:00Z — verified live). Picking one would be a guess; dropping or
	// aborting would silently lose a real market.
	dup := upDownEvent("bitcoin-up-or-down-october-2-2026-1pm-et", target)
	got, err = SelectEvents([]connector.GammaEvent{right, dup}, target)
	if err != nil {
		t.Fatalf("duplicate settle instant: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("selected %d events, want both at the same instant", len(got))
	}
	if got[0].Slug >= got[1].Slug {
		t.Errorf("events out of order: %v, want slug order", slugsOf(got))
	}

	// Nothing settles then.
	if _, err := SelectEvents([]connector.GammaEvent{early, late}, target); !errors.Is(err, ErrNoEventForSettle) {
		t.Errorf("no match: err = %v, want ErrNoEventForSettle", err)
	}

	// An event with no endDate can never be matched.
	zero := upDownEvent("no-end-date", target)
	zero.EndDate = time.Time{}
	if _, err := SelectEvents([]connector.GammaEvent{zero}, target); !errors.Is(err, ErrNoEventForSettle) {
		t.Errorf("zero endDate: err = %v, want ErrNoEventForSettle", err)
	}
}

// ── Fan-out ─────────────────────────────────────────────────

func TestStartEventSessionsUpDown(t *testing.T) {
	ec, conn := newTestCollector(t)
	settleAt := time.Now().Add(time.Hour).Truncate(time.Hour)
	spec := hourlySpec(t)

	target := EventTarget{
		Spec:       spec,
		Event:      upDownEvent("bitcoin-up-or-down-october-2-2026-2pm-et", settleAt),
		SettleAt:   settleAt,
		SeriesID:   "id-" + spec.GammaSeries,
		EpochID:    spec.EpochIDFor(settleAt),
		Recurrence: "hourly",
	}

	started, skipped, err := ec.StartEventSessions(target)
	if err != nil {
		t.Fatalf("StartEventSessions: %v", err)
	}
	if len(started) != 1 {
		t.Fatalf("started %d markets, want 1", len(started))
	}
	if skipped != 0 {
		t.Errorf("skipped %d markets, want 0", skipped)
	}
	sess := started[0].Session

	// Both outcomes are subscribed, and the Up side is the YES asset even though
	// it is not first in the payload — matching on the label, not on position.
	wantYes := "m-bitcoin-up-or-down-october-2-2026-2pm-et-up"
	wantNo := "m-bitcoin-up-or-down-october-2-2026-2pm-et-down"
	if sess.cfg.YesAssetID != wantYes || sess.cfg.NoAssetID != wantNo {
		t.Errorf("assets = (%s, %s), want (%s, %s)",
			sess.cfg.YesAssetID, sess.cfg.NoAssetID, wantYes, wantNo)
	}
	if len(conn.subscribed) != 2 {
		t.Errorf("subscribed %v, want both outcome tokens", conn.subscribed)
	}

	// The session carries the discovery context, and the settle instant comes
	// from the event rather than the configured window.
	if !sess.SettleAt().Equal(settleAt) {
		t.Errorf("SettleAt = %s, want %s", sess.SettleAt(), settleAt)
	}
	if mc := sess.meta; mc.EpochID != target.EpochID ||
		mc.Family == nil || mc.Family.Name != "updown" ||
		mc.Event == nil || mc.Event.LadderIndex != -1 ||
		mc.Settlement == nil || mc.Settlement.Source != string(libs.SettleBinanceCandle) {
		t.Errorf("market context = %+v", mc)
	}

	// Idempotent: reconciling again must not double-subscribe.
	again, _, err := ec.StartEventSessions(target)
	if err != nil {
		t.Fatalf("second StartEventSessions: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second call started %d markets, want 0", len(again))
	}
	if len(conn.subscribed) != 2 {
		t.Errorf("second call re-subscribed: %v", conn.subscribed)
	}

	// A market that already settled is never claimed.
	settled := target
	settled.Event = upDownEvent("already-settled", time.Now().Add(-time.Minute))
	settled.SettleAt = settled.Event.EndDate
	if _, _, err := ec.StartEventSessions(settled); err == nil {
		t.Error("claiming an already-settled event should fail")
	}
}

func TestStartEventSessionsOutcomeOrderIsLabelDriven(t *testing.T) {
	ec, _ := newTestCollector(t)
	settleAt := time.Now().Add(time.Hour)

	ev := upDownEvent("swapped", settleAt)
	// A payload that lists Down first must still map YES to Down's complement.
	m := gammaMarket("swapped-m", "swapped", "", "Down", "Up")
	ev.Markets = []connector.GammaMarket{m}

	started, _, err := ec.StartEventSessions(EventTarget{
		Spec: hourlySpec(t), Event: ev, SettleAt: settleAt, EpochID: "2026-10-02",
	})
	if err != nil {
		t.Fatalf("StartEventSessions: %v", err)
	}
	sess := started[0].Session
	if want := "swapped-m-up"; sess.cfg.YesAssetID != want {
		t.Errorf("YesAssetID = %s, want %s (mapped by outcome label)", sess.cfg.YesAssetID, want)
	}
	if want := "swapped-m-down"; sess.cfg.NoAssetID != want {
		t.Errorf("NoAssetID = %s, want %s", sess.cfg.NoAssetID, want)
	}
}

func TestStartEventSessionsLadderFansOutAndFilters(t *testing.T) {
	ec, _ := newTestCollector(t)
	settleAt := time.Now().Add(6 * time.Hour)

	// 80,000 is rung index 3 of 74,000…94,000; the filter keeps two rungs each
	// side of the spot, so 76,000 … 82,000.
	ec.SetSpotPrice(80000)

	target := EventTarget{
		Spec:        ladderSpec(t),
		Event:       ladderEvent("bitcoin-above-on-october-2-2026", settleAt, "oct2"),
		SettleAt:    settleAt,
		EpochID:     "2026-10-02",
		OffsetDays:  0,
		StrikeLimit: 2,
	}

	started, _, err := ec.StartEventSessions(target)
	if err != nil {
		t.Fatalf("StartEventSessions: %v", err)
	}
	if len(started) != 4 {
		t.Fatalf("started %d rungs, want 4 (2 each side of spot)", len(started))
	}

	var labels []string
	for _, st := range started {
		labels = append(labels, st.Geometry.Label)
	}
	if got := strings.Join(labels, " "); got != "76,000 78,000 80,000 82,000" {
		t.Errorf("kept rungs = %q, want %q", got, "76,000 78,000 80,000 82,000")
	}

	// The rung geometry is reported against the event's FULL ladder, so a
	// filtered recording still says where each rung sits.
	md := started[2].Session.meta
	if md.Event.LadderSize != 11 {
		t.Errorf("ladder_size = %d, want 11", md.Event.LadderSize)
	}
	if md.Event.LadderIndex != 3 {
		t.Errorf("ladder_index = %d, want 3", md.Event.LadderIndex)
	}
	if md.Event.Strike == nil || *md.Event.Strike != 80000 {
		t.Errorf("strike = %v, want 80000", md.Event.Strike)
	}
	if md.Settlement.Comparison != CompareAboveStrike {
		t.Errorf("comparison = %s, want %s", md.Settlement.Comparison, CompareAboveStrike)
	}
	if md.Event.IsSettlementEpoch != true || md.Event.OffsetDays != 0 {
		t.Errorf("offset flags = (%v, %d), want (true, 0)",
			md.Event.IsSettlementEpoch, md.Event.OffsetDays)
	}
}

func TestParseStrikeLabelGeometry(t *testing.T) {
	tests := []struct {
		label          string
		strike         float64
		low, high      float64
		wantErr, empty bool
	}{
		{label: "", empty: true},
		{label: "74,000", strike: 74000},
		{label: "<74,000", high: 74000},
		{label: ">94,000", low: 94000},
		{label: "$76,000 - $78,000", low: 76000, high: 78000},
		{label: "no digits here", wantErr: true},
	}
	for _, tc := range tests {
		geo, err := ParseStrikeLabel(tc.label)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: want error", tc.label)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.label, err)
			continue
		}
		if geo.Empty() != tc.empty {
			t.Errorf("%q: empty = %v, want %v", tc.label, geo.Empty(), tc.empty)
		}
		if tc.strike > 0 && (geo.Strike == nil || *geo.Strike != tc.strike) {
			t.Errorf("%q: strike = %v, want %v", tc.label, geo.Strike, tc.strike)
		}
		if tc.low > 0 && (geo.RangeLow == nil || *geo.RangeLow != tc.low) {
			t.Errorf("%q: low = %v, want %v", tc.label, geo.RangeLow, tc.low)
		}
		if tc.high > 0 && (geo.RangeHigh == nil || *geo.RangeHigh != tc.high) {
			t.Errorf("%q: high = %v, want %v", tc.label, geo.RangeHigh, tc.high)
		}
	}
}

// TestSelectRungsCentresOnSpot covers the filter directly, including the
// startup case where no reference price has arrived yet.
func TestSelectRungsCentresOnSpot(t *testing.T) {
	labels := []string{"74,000", "76,000", "78,000", "80,000", "82,000", "84,000", "86,000", "88,000", "90,000", "92,000", "94,000"}
	rs := make([]rung, 0, len(labels))
	ev := ladderEvent("x", time.Now().Add(time.Hour), "x")
	for i, m := range ev.Markets {
		geo, err := ParseStrikeLabel(labels[i])
		if err != nil {
			t.Fatal(err)
		}
		v, ok := rungValue(geo)
		rs = append(rs, rung{market: m, geo: geo, value: v, hasValue: ok})
	}

	kept := func(out []rung) []string {
		var got []string
		for _, r := range out {
			got = append(got, r.geo.Label)
		}
		return got
	}
	join := func(out []rung) string { return strings.Join(kept(out), " ") }
	apply := func(limit int, spot float64) ([]rung, rungFilter) {
		out, f := selectRungs(rs, limit, spot)
		return out, f
	}

	if out, f := apply(0, 84000); join(out) != strings.Join(labels, " ") {
		t.Errorf("limit 0 = %q, want every rung", join(out))
	} else if f.basis != StrikeFilterAll || f.total != len(labels) {
		t.Errorf("limit 0 filter = %+v, want basis %q over %d rungs", f, StrikeFilterAll, len(labels))
	}
	if out, f := apply(2, 80000); join(out) != "76,000 78,000 80,000 82,000" {
		t.Errorf("2 per side of 80,000 = %q, want %q", join(out), "76,000 78,000 80,000 82,000")
	} else if f.basis != StrikeFilterSpot || f.spot != 80000 || f.total != len(labels) {
		t.Errorf("filter = %+v, want a spot-centred filter at 80,000 over %d rungs", f, len(labels))
	}
	// A spot below the ladder keeps the lowest rungs, above it the highest.
	if out, _ := apply(2, 10000); join(out) != "74,000 76,000" {
		t.Errorf("2 per side of 10,000 = %q, want %q", join(out), "74,000 76,000")
	}
	if out, _ := apply(2, 999999); join(out) != "92,000 94,000" {
		t.Errorf("2 per side of 999999 = %q, want %q", join(out), "92,000 94,000")
	}
	// No spot yet: centre on the ladder's middle rung rather than record all 11,
	// and say so — this is the one case where two runs of the same config hold
	// different rung sets (CONTEXT.md §13.3).
	if out, f := apply(2, 0); join(out) != "80,000 82,000 84,000 86,000" {
		t.Errorf("no spot = %q, want %q", join(out), "80,000 82,000 84,000 86,000")
	} else if f.basis != StrikeFilterMedian {
		t.Errorf("no spot: basis = %q, want %q", f.basis, StrikeFilterMedian)
	}
}

// ── Settlement feed reference ───────────────────────────────

// TestDerivedFromPointsAtTheSettleBucket pins the edge documented in CONTEXT.md
// §5: a market settling at noon ET on D is decided by the first candle of the
// feed bucket that OPENS at noon on D, not by the one that closes there.
func TestDerivedFromPointsAtTheSettleBucket(t *testing.T) {
	ec, _ := newTestCollector(t)
	fr := NewFeedRecorder(filepath.Join(t.TempDir(), "binance"), "BTCUSDT", FeedSourceBinance, "spot", libs.AnchorNoonET)
	ec.AddFeedRecorder(fr)

	settleAt := time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC) // 12:00 ET Oct 2
	venue, err := venueFor(ladderSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	bucketEpoch := libs.EpochIDAt(settleAt, libs.AnchorNoonET)
	if bucketEpoch != "2026-10-03" {
		t.Fatalf("bucket epoch = %s, want 2026-10-03", bucketEpoch)
	}
	if got := ec.FeedRef(venue, bucketEpoch); got != "binance/spot/btcusdt_2026-10-03" {
		t.Errorf("FeedRef = %q, want %q", got, "binance/spot/btcusdt_2026-10-03")
	}

	// Chainlink is a different root and a different bucket name.
	twap, err := venueFor(mustSpecByName(t, "btc_4h"))
	if err != nil {
		t.Fatal(err)
	}
	ec.AddFeedRecorder(NewFeedRecorder(filepath.Join(t.TempDir(), "rtds"), "btc/usd", FeedSourceChainlinkTWAP, "", libs.AnchorNoonET))
	if got := ec.FeedRef(twap, "2026-10-03"); got != "rtds/chainlink_twap_btc_usd_2026-10-03" {
		t.Errorf("FeedRef(chainlink) = %q", got)
	}

	// An unrecorded settlement source is reported as empty rather than invented.
	other := venue
	other.symbol = "ETHUSDT"
	if got := ec.FeedRef(other, "2026-10-03"); got != "" {
		t.Errorf("FeedRef(absent feed) = %q, want empty", got)
	}

	refs := ec.FeedRefs("2026-10-03")
	if len(refs["binance"]) != 1 || refs["binance"][0] != "spot/btcusdt_2026-10-03" {
		t.Errorf("FeedRefs[binance] = %v", refs["binance"])
	}
	if len(refs["rtds"]) != 1 || refs["rtds"][0] != "chainlink_twap_btc_usd_2026-10-03" {
		t.Errorf("FeedRefs[rtds] = %v", refs["rtds"])
	}
}

// ── Scheduler ───────────────────────────────────────────────

func TestSchedulerReconcileClaimsSettlementsAndOffset(t *testing.T) {
	ec, _ := newTestCollector(t)
	hourly := hourlySpec(t)
	above := ladderSpec(t)

	// The targets are recomputed from the real clock inside reconcile, so the
	// stub serves the events for both the current and the next period: exactly
	// one of each can match, whichever side of a boundary the test runs on.
	now := time.Now()
	hourTarget, err := hourly.TargetSettle(now)
	if err != nil {
		t.Fatal(err)
	}
	aboveTarget, err := above.TargetSettle(now)
	if err != nil {
		t.Fatal(err)
	}
	aboveNext, err := above.TargetSettle(now.AddDate(0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}

	disc := &stubDiscovery{series: map[string][]connector.GammaEvent{
		hourly.GammaSeries: {
			upDownEvent("hour-now", hourTarget),
			upDownEvent("hour-next", hourTarget.Add(time.Hour)),
		},
		above.GammaSeries: {
			ladderEvent("above-now", aboveTarget, "now"),
			ladderEvent("above-next", aboveNext, "next"),
		},
	}}

	sched, err := NewEpochScheduler(ec, disc, SchedulerConfig{
		Recording:    RecordingConfig{Days: 3, Epoch: string(libs.AnchorNoonET), LadderOffsetDays: 1},
		Series:       []SeriesConfig{{Series: "btc_1h"}, {Series: "btc_above", Strikes: 0}},
		PollInterval: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewEpochScheduler: %v", err)
	}

	if err := sched.reconcile(context.Background(), now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	sessions := snapshotSessions(ec)
	if len(sessions) != 23 {
		t.Fatalf("started %d sessions, want 23 (1 hourly + 2 dated ladder events × 11 rungs)",
			len(sessions))
	}

	// Every session must settle at its own event's endDate, and be filed under
	// the epoch that settle instant belongs to.
	epochs := map[string]int{}
	for _, s := range sessions {
		if got := s.SettleAt().UnixMilli(); got != s.meta.Event.TradingWindowEnd {
			t.Errorf("%s: settle = %d, event end = %d", s.cfg.Slug, got, s.meta.Event.TradingWindowEnd)
		}
		epochs[s.meta.EpochID]++
	}

	// The running epoch holds everything claimed now — the hourly market plus the
	// ladder event settling at its end — because a market is filed by the epoch
	// its settlement falls in. The "+1" ladder event is the only thing filed
	// ahead, so exactly two epochs are touched.
	active := sched.ActiveEpoch()
	if epochs[active] != 12 {
		t.Errorf("epoch %s holds %d sessions, want 12 (1 hourly + 11 rungs)", active, epochs[active])
	}
	if len(epochs) != 2 {
		t.Errorf("sessions spread over %d epochs, want 2 (the +1 event files next epoch)", len(epochs))
	}
	nextEpoch, err := libs.ShiftEpochID(active, 1)
	if err != nil {
		t.Fatal(err)
	}
	if epochs[nextEpoch] != 11 {
		t.Errorf("epoch %s holds %d sessions, want the 11 offset rungs", nextEpoch, epochs[nextEpoch])
	}

	// The +1 event is a day ahead of the epoch's own settlement, and is flagged
	// as an offset pickup rather than the epoch's own event.
	for _, s := range sessions {
		if s.meta.EpochID == nextEpoch && (s.meta.Event.OffsetDays != 1 || s.meta.Event.IsSettlementEpoch) {
			t.Errorf("%s: offset flags = (%d, %v), want (1, false)",
				s.cfg.Slug, s.meta.Event.OffsetDays, s.meta.Event.IsSettlementEpoch)
		}
		if s.meta.EpochID == active && s.meta.Event.OffsetDays != 0 {
			t.Errorf("%s: offset_days = %d, want 0", s.cfg.Slug, s.meta.Event.OffsetDays)
		}
	}

	// Re-reconciling must be free: sessions are already claimed, so discovery is
	// not even called.
	before := disc.eventCalls
	if err := sched.reconcile(context.Background(), now); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if len(snapshotSessions(ec)) != len(sessions) {
		t.Error("second reconcile claimed more sessions")
	}
	if disc.eventCalls != before {
		t.Errorf("second reconcile made %d extra discovery calls, want 0", disc.eventCalls-before)
	}

	// The manifest indexes the claims: the running epoch lists the +1 event as
	// an offset carry, and the next epoch's file holds that event's markets.
	root := ec.recordingDir()
	running := readManifest(t, filepath.Join(root, active, manifestFile))
	if !contains(running.OffsetEvents, "above-next") {
		t.Errorf("offset_events = %v, want to contain above-next", running.OffsetEvents)
	}
	next := readManifest(t, filepath.Join(root, nextEpoch, manifestFile))
	if len(next.Series) != 1 {
		t.Fatalf("next epoch series = %d, want 1", len(next.Series))
	}
	if next.Series[0].EventSlug != "above-next" || len(next.Series[0].Markets) != 11 {
		t.Errorf("next epoch ladder = %s with %d markets, want above-next with 11",
			next.Series[0].EventSlug, len(next.Series[0].Markets))
	}
	if next.EpochStart == 0 || next.EpochEnd <= next.EpochStart {
		t.Errorf("next epoch bounds = (%d, %d)", next.EpochStart, next.EpochEnd)
	}
	if next.Run == nil || next.Run.Days != 3 || next.Run.LadderOffsetDays != 1 {
		t.Errorf("run block = %+v", next.Run)
	}
}

// TestSchedulerRecordsEveryEventAtTheSameInstant pins what happens when a series
// carries two events that settle at the same instant — which is not a bug in the
// series but the DST transition: on 2026-03-08 the hourly series ran both
// "…-12am-et" and "…-1am-et" to the same 06:00Z boundary (verified live against
// Gamma), because the local 01:00 hour never happened.
//
// Both are real markets settling on the same candle, so both are recorded. The
// alternative — aborting the run, as the pre-DST version did — would cost the
// entire recording over a calendar artefact.
func TestSchedulerRecordsEveryEventAtTheSameInstant(t *testing.T) {
	ec, _ := newTestCollector(t)
	hourly := hourlySpec(t)
	now := time.Now()
	target, err := hourly.TargetSettle(now)
	if err != nil {
		t.Fatal(err)
	}

	disc := &stubDiscovery{series: map[string][]connector.GammaEvent{
		hourly.GammaSeries: {
			upDownEvent("mark-8-12am-et", target),
			upDownEvent("mark-8-1am-et", target),
		},
	}}
	sched, err := NewEpochScheduler(ec, disc, SchedulerConfig{
		Recording: RecordingConfig{Days: 1, Epoch: string(libs.AnchorNoonET)},
		Series:    []SeriesConfig{{Series: "btc_1h"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	planned, err := sched.Plan(context.Background(), now)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(planned) != 2 {
		t.Fatalf("planned %d targets, want both events settling at %s", len(planned), target)
	}

	if err := sched.reconcile(context.Background(), now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := len(snapshotSessions(ec)); got != 2 {
		t.Fatalf("started %d sessions, want one per event (2)", got)
	}

	// Each market is filed under the epoch its settlement falls in, and the
	// manifest keeps the two events apart by slug.
	m := readManifest(t, filepath.Join(ec.recordingDir(), sched.ActiveEpoch(), manifestFile))
	if len(m.Series) != 2 {
		t.Fatalf("manifest holds %d series entries, want 2", len(m.Series))
	}
	var slugs []string
	for _, s := range m.Series {
		if s.SettleAt != target.UnixMilli() {
			t.Errorf("%s: settle_at = %d, want %d", s.Series, s.SettleAt, target.UnixMilli())
		}
		if s.EventSlug == "" {
			t.Error("manifest entry has no event slug, so two same-instant events are indistinguishable")
		}
		slugs = append(slugs, s.EventSlug)
	}
	sort.Strings(slugs)
	if strings.Join(slugs, " ") != "mark-8-12am-et mark-8-1am-et" {
		t.Errorf("manifest slugs = %v, want both events", slugs)
	}
}

// TestSchedulerStopsAfterConfiguredEpochs pins the run-length rule: `days` daily
// epochs are recorded, and the run ends at the boundary that would start the
// next one (so the final ladder event is truncated, never half-claimed).
func TestSchedulerStopsAfterConfiguredEpochs(t *testing.T) {
	ec, _ := newTestCollector(t)
	disc := &stubDiscovery{series: map[string][]connector.GammaEvent{}}
	sched, err := NewEpochScheduler(ec, disc, SchedulerConfig{
		Recording: RecordingConfig{Days: 2, Epoch: string(libs.AnchorNoonET), LadderOffsetDays: 1},
		Series:    []SeriesConfig{{Series: "btc_1h"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 14:00 ET on the 2nd, 3rd and 4th — each is past noon, so each instant
	// belongs to the epoch that closes at noon the following day.
	day1 := time.Date(2026, 10, 2, 18, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	day3 := day1.AddDate(0, 0, 2)

	for _, now := range []time.Time{day1, day2} {
		if err := sched.rollEpoch(now); err != nil {
			t.Fatalf("rollEpoch: %v", err)
		}
	}
	if sched.Finished() {
		t.Fatal("run finished one epoch too early")
	}
	if got, want := sched.ActiveEpoch(), "2026-10-04"; got != want {
		t.Errorf("active epoch = %s, want %s", got, want)
	}

	if err := sched.rollEpoch(day3); err != nil {
		t.Fatalf("rollEpoch: %v", err)
	}
	if !sched.Finished() {
		t.Error("run did not finish after the configured number of epochs")
	}
}

// TestUntilNextWakeWakesAtBoundaries proves the scheduler does not sleep a full
// poll interval through a window boundary, which would lose the open of every
// window, while still bounding how long a failed lookup waits.
func TestUntilNextWakeWakesAtBoundaries(t *testing.T) {
	ec, _ := newTestCollector(t)
	sched, err := NewEpochScheduler(ec, &stubDiscovery{series: map[string][]connector.GammaEvent{}}, SchedulerConfig{
		Recording:    RecordingConfig{Days: 1, Epoch: string(libs.AnchorNoonET)},
		Series:       []SeriesConfig{{Series: "btc_1h"}, {Series: "btc_1d"}},
		PollInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, now := range []time.Time{
		time.Now(),
		time.Date(2026, 10, 2, 13, 59, 30, 0, time.UTC), // 30s before a boundary
		time.Date(2026, 10, 2, 16, 30, 0, 0, time.UTC),  // just past the anchor
	} {
		d := sched.untilNextWake(now)
		if d <= 0 || d > time.Minute {
			t.Errorf("untilNextWake(%s) = %s, want (0, 1m]", now.Format(time.RFC3339), d)
		}
	}

	// Ten seconds out from an hourly boundary the wake is that boundary plus the
	// epsilon — not the poll interval, which would lose the open of every window.
	near := time.Date(2026, 10, 2, 9, 59, 50, 0, libs.EpochLocation)
	if d := sched.untilNextWake(near); d < 10*time.Second || d > 11*time.Second {
		t.Errorf("wake = %s, want ~%s", d, 10*time.Second+boundaryEpsilon)
	}
}

// TestSchedulerRejectsUnknownSeries makes a config typo a startup failure rather
// than a silent no-op.
func TestSchedulerRejectsUnknownSeries(t *testing.T) {
	ec, _ := newTestCollector(t)
	disc := &stubDiscovery{series: map[string][]connector.GammaEvent{}}
	for _, sc := range []SeriesConfig{{Series: "btc_nope"}, {Series: "btc_above", Strikes: -1}} {
		if _, err := NewEpochScheduler(ec, disc, SchedulerConfig{
			Recording: RecordingConfig{Days: 1},
			Series:    []SeriesConfig{sc},
		}); err == nil {
			t.Errorf("%+v: want an error", sc)
		}
	}
	if _, err := NewEpochScheduler(ec, disc, SchedulerConfig{Recording: RecordingConfig{Days: 1}}); err == nil {
		t.Error("an empty series list should be rejected")
	}
}

// ── Helpers ─────────────────────────────────────────────────

// snapshotSessions returns the collector's live sessions.
func snapshotSessions(ec *EventCollector) []*RecordingSession {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	out := make([]*RecordingSession, 0, len(ec.sessions))
	for _, s := range ec.sessions {
		out = append(out, s)
	}
	return out
}

func readManifest(t *testing.T, path string) EpochManifest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	var m EpochManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse manifest %s: %v", path, err)
	}
	return m
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// slugsOf lists the slugs of a selection, for failure messages.
func slugsOf(events []connector.GammaEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Slug)
	}
	return out
}
