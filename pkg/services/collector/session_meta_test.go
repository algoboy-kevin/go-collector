package collector

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Session metadata/path tests. These pin the layout contract from CONTEXT.md
// §5/§6: a market is filed under its settlement epoch, its metadata is
// self-describing, and a run-truncated recording is `unsettled` rather than
// being given a synthesized outcome.

var (
	// 2026-10-02T06:30Z = 02:30 ET — before the noon anchor, so this instant
	// belongs to the epoch that closes at noon on the 2nd.
	sessNow = time.Date(2026, 10, 2, 6, 30, 0, 0, time.UTC)
	// 2026-10-02T16:00Z = 12:00 ET = the noon-ET epoch end.
	settleAt = time.Date(2026, 10, 2, 16, 0, 0, 0, time.UTC)
)

func newTestSession(t *testing.T, dir string, mc MarketContext) *RecordingSession {
	t.Helper()
	ec := NewCollector(CollectorConfig{RecordingDir: dir}, nil)
	sess := newRecordingSession(SessionConfig{
		MarketID:        "5169514",
		Slug:            "bitcoin-above-80k-on-october-2-2026",
		YesAssetID:      "yes-token",
		NoAssetID:       "no-token",
		Question:        "Bitcoin above ___ on October 2?",
		ConditionID:     "0xabc",
		MarketStartTime: sessNow,
		// Measured from the real clock, NOT from sessNow. The synthetic-resolve timer
		// is scheduled at MarketEndTime + syntheticDelay, so anchoring this to the
		// frozen fixture date made the suite expire: once real time passed
		// 2026-10-04T06:31Z (sessNow + 48h + 1m) the timer fired immediately, resolving
		// and finalizing sessions mid-test and racing every assertion below.
		MarketEndTime: time.Now().Add(48 * time.Hour),
	}, ec)
	sess.SetMarketContext(mc)
	// Finalize stops and nils the timer, so guard the cleanup.
	t.Cleanup(func() {
		if sess.syntheticTimer != nil {
			sess.syntheticTimer.Stop()
		}
	})
	return sess
}

func aboveContext() MarketContext {
	strike := 80000.0
	return MarketContext{
		EpochID: "2026-10-02",
		Family: &FamilyInfo{
			Name: "above", Interval: "1d", Asset: "BTC",
			SeriesSlug: "btc-multi-strikes-weekly", SeriesID: "45", Recurrence: "weekly",
		},
		Event: &EventInfo{
			Slug:         "bitcoin-above-on-october-2-2026",
			Title:        "Bitcoin above ___ on October 2?",
			SettleDateET: "2026-10-02", LadderSize: 11, LadderIndex: 3,
			Strike: &strike, StrikeLabel: "80,000",
			Outcomes:   []string{"Yes", "No"},
			OffsetDays: 1,
		},
		Settlement: &SettlementInfo{
			At: settleAt.UnixMilli(), Anchor: "noon_et", Source: SettleSourceBinance1mClose,
			Symbol: "BTCUSDT", Market: "spot", Comparison: CompareAboveStrike,
			DerivedFrom: "binance/spot/btcusdt_2026-10-02",
		},
	}
}

func recordOne(t *testing.T, sess *RecordingSession) {
	t.Helper()
	if !sess.Record(RecordedEvent{Type: "price_change", Timestamp: sessNow.UnixMilli(), ReceivedAt: sessNow.UnixMilli()}) {
		t.Fatal("record failed")
	}
}

func readMarketMeta(t *testing.T, path string) MarketMetadata {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	var md MarketMetadata
	if err := json.Unmarshal(raw, &md); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	return md
}

func TestSessionWritesUnderEpochDirWithSettlement(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)

	if err := sess.Resolve(sess.cfg.YesAssetID); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	dir := filepath.Join(root, "2026-10-02", "5169514")
	if _, err := os.Stat(filepath.Join(dir, "events.gz")); err != nil {
		t.Fatalf("events.gz not under the epoch directory: %v", err)
	}
	md := readMarketMeta(t, filepath.Join(dir, "metadata.json"))

	if md.EpochID != "2026-10-02" {
		t.Errorf("epoch_id = %q, want 2026-10-02", md.EpochID)
	}
	if md.Truncated {
		t.Error("a settled market must not be marked truncated")
	}
	if md.Resolution != ResolutionYes {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionYes)
	}
	if md.WinningOutcome != "YES" {
		t.Errorf("winning_outcome = %q, want the raw on-chain outcome YES", md.WinningOutcome)
	}
	// end_time is the settle instant, not the configured window end (48h out).
	if md.EndTime != settleAt.UnixMilli() {
		t.Errorf("end_time = %d, want the settle instant %d", md.EndTime, settleAt.UnixMilli())
	}

	if md.Family == nil || md.Family.Name != "above" || md.Family.Interval != "1d" {
		t.Fatalf("family block = %+v", md.Family)
	}
	if md.Family.SeriesSlug != "btc-multi-strikes-weekly" {
		t.Errorf("family.series_slug = %q", md.Family.SeriesSlug)
	}
	if md.Event == nil || md.Event.Strike == nil || *md.Event.Strike != 80000 {
		t.Fatalf("event block = %+v", md.Event)
	}
	if md.Event.StrikeLabel != "80,000" || md.Event.LadderSize != 11 || md.Event.LadderIndex != 3 {
		t.Errorf("ladder geometry lost: %+v", md.Event)
	}
	if md.Settlement == nil {
		t.Fatal("settlement block missing")
	}
	if md.Settlement.Source != SettleSourceBinance1mClose {
		t.Errorf("settlement.source = %q", md.Settlement.Source)
	}
	if md.Settlement.Comparison != CompareAboveStrike {
		t.Errorf("settlement.comparison = %q", md.Settlement.Comparison)
	}
	if md.Settlement.DerivedFrom != "binance/spot/btcusdt_2026-10-02" {
		t.Errorf("settlement.derived_from = %q", md.Settlement.DerivedFrom)
	}
}

func TestSessionTruncatedIsUnsettled(t *testing.T) {
	root := t.TempDir()
	mc := aboveContext()
	sess := newTestSession(t, root, mc)
	recordOne(t, sess)

	// The run stops before the market settles: no outcome, no synthesis.
	sess.MarkTruncated()
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
	if !md.Truncated {
		t.Error("truncated must be true so a reader knows the recording is partial")
	}
	if md.WinningOutcome != "" {
		t.Errorf("winning_outcome = %q, want empty for an unsettled market", md.WinningOutcome)
	}
	if md.SyntheticResolve {
		t.Error("a truncated session must not be marked as synthetically resolved")
	}
}

func TestSyntheticResolveRefusesTruncatedSession(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)
	sess.MarkTruncated()

	// Even if something forces the synthetic path (e.g. the close-time timer),
	// a truncated session must stay recording rather than invent an outcome.
	sess.handleSyntheticResolve()

	if got := sess.State(); got != SessionRecording {
		t.Errorf("state = %q, want %q — synthetic resolve must not resolve a truncated session",
			got, SessionRecording)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
}

func TestStandaloneSessionKeepsFlatPath(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, MarketContext{}) // no discovery context
	recordOne(t, sess)
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// Compatible with the pre-epoch layout: {root}/{marketID}/.
	dir := filepath.Join(root, "5169514")
	if _, err := os.Stat(filepath.Join(dir, "metadata.json")); err != nil {
		t.Fatalf("standalone metadata.json missing at the flat path: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"epoch_id", "family", "event", "settlement", "truncated"} {
		if _, ok := generic[key]; ok {
			t.Errorf("standalone recording should not emit %q", key)
		}
	}
}

func TestSessionSettlementPassed(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())

	if sess.SettlementPassed(settleAt.Add(-time.Second)) {
		t.Error("settlement must not be considered passed before the anchor")
	}
	if !sess.SettlementPassed(settleAt) {
		t.Error("settlement is passed exactly at the anchor")
	}
	if got := sess.SettleAt(); !got.Equal(settleAt) {
		t.Errorf("SettleAt() = %s, want %s", got, settleAt)
	}
}
