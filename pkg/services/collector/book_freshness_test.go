package collector

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Book-freshness tests. The reviewer's finding: lastBookAt used to be updated
// only on book_snapshot, and computeLastMidprice read the last stored SNAPSHOT —
// so a quiet market whose book was kept current by thousands of price_change
// events still looked stale (stale_book) at settlement, because snapshots only
// arrive on subscribe/reconnect. The fix keeps a live top-of-book per asset,
// refreshed by BOTH snapshots and price_change BBO fields.

// recordPriceChangeAt records one price_change for an asset carrying the
// exchange-reported BBO at that instant.
func recordPriceChangeAt(t *testing.T, sess *RecordingSession, assetID string, at time.Time, bestBid, bestAsk string) {
	t.Helper()
	ev := connector.PriceChangeEvent{
		Market:    sess.cfg.MarketID,
		Timestamp: at,
		Changes: []connector.PriceChangeItem{{
			AssetID: assetID,
			Price:   bestAsk,
			Size:    "10",
			Side:    "BUY",
			BestBid: bestBid,
			BestAsk: bestAsk,
		}},
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal price change: %v", err)
	}
	if !sess.Record(RecordedEvent{
		Type:       "price_change",
		Timestamp:  at.UnixMilli(),
		ReceivedAt: at.UnixMilli(),
		Data:       data,
	}) {
		t.Fatal("record price change failed")
	}
}

// TestSyntheticResolveUsesFreshBooFromPriceChange is the core regression: an
// hours-old snapshot is irrelevant once recent price_change events have kept
// the book current, so the market resolves from the fresh BBO and NOT as
// stale_book.
func TestSyntheticResolveUsesFreshBooFromPriceChange(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())

	// A stale snapshot hours before settlement — its collapsed YES book would
	// say YES 0.995 if it were (wrongly) trusted.
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-6*time.Hour),
		[][2]string{{"0.99", "500"}}, nil)
	// Recent price_change BBO: two-sided 0.70/0.72, so the midprice is 0.71.
	recordPriceChangeAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-5*time.Second), "0.70", "0.72")

	sess.handleSyntheticResolve()

	if got := sess.State(); got != SessionFinalized {
		t.Fatalf("state = %s, want %s", got, SessionFinalized)
	}
	if !sess.resolvedSynthetically {
		t.Fatal("expected a synthetic resolve from the fresh BBO")
	}
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES", sess.winningOutcome)
	}
	if sess.lastMidprice != 0.71 {
		t.Errorf("last midprice = %v, want 0.71 (from the price_change BBO)", sess.lastMidprice)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisMidprice {
		t.Errorf("resolution_basis = %q, want %q (not stale_book)", md.ResolutionBasis, ResolutionBasisMidprice)
	}
	if md.LastMidprice != 0.71 {
		t.Errorf("metadata last_midprice = %v, want 0.71", md.LastMidprice)
	}
}

// TestSyntheticResolveStaleWhenOnlyOldEvents proves the fallback hard bound
// still works: with NO feed-liveness evidence (the test collector has no
// market-WS status source) an hours-old snapshot is refused. The age bound is
// only the fallback — with a status source wired, staleness is judged by
// disconnect/silence windows instead (see the liveness tests).
func TestSyntheticResolveStaleWhenOnlyOldEvents(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-6*time.Hour),
		[][2]string{{"0.99", "500"}}, nil)

	sess.handleSyntheticResolve()

	if sess.winningOutcome != "" {
		t.Errorf("winning outcome = %q, want empty (stale book must not decide)", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisStaleBook {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisStaleBook)
	}
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
}

// TestSyntheticResolveNoEventsIsNoData re-checks the empty case.
func TestSyntheticResolveNoEventsIsNoData(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())

	sess.handleSyntheticResolve()

	if sess.winningOutcome != "" {
		t.Errorf("winning outcome = %q, want empty", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisNoData {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisNoData)
	}
}

// TestSyntheticResolveUsesNoBooInverted proves the NO-only fallback: with no
// YES top-of-book, the NO mid is inverted (NO_price ≈ 1 - YES_price).
func TestSyntheticResolveUsesNoBooInverted(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	// Only the NO asset reports a BBO: 0.28/0.30 → mid 0.29 → YES 0.71.
	recordPriceChangeAt(t, sess, sess.cfg.NoAssetID, settleAt.Add(-5*time.Second), "0.28", "0.30")

	sess.handleSyntheticResolve()

	if sess.lastMidprice != 0.71 {
		t.Errorf("last midprice = %v, want 0.71 (1 - NO mid 0.29)", sess.lastMidprice)
	}
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisMidprice {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisMidprice)
	}
}

// TestTopOfBookPartialPriceChangeKeepsPreviousSide pins the "treat '' as
// unknown, keep previous" rule: a price_change that carries only one side must
// not erase the other.
func TestTopOfBookPartialPriceChangeKeepsPreviousSide(t *testing.T) {
	var b topOfBook
	b.setFromLevels([]connector.Level{{Price: "0.40"}}, []connector.Level{{Price: "0.42"}})
	b.updateFromChange("", "0.60")

	if !b.hasBid || b.bid != 0.40 {
		t.Errorf("bid = (%v, has=%v), want (0.40, true) — the empty side keeps its previous value", b.bid, b.hasBid)
	}
	if !b.hasAsk || b.ask != 0.60 {
		t.Errorf("ask = (%v, has=%v), want (0.60, true)", b.ask, b.hasAsk)
	}
	if mid, ok := b.mid(); !ok || mid != 0.5 {
		t.Errorf("mid = (%v, ok=%v), want (0.5, true)", mid, ok)
	}
}

// TestTopOfBookSnapshotIsAuthoritative proves a snapshot replaces both sides,
// including clearing a side the ladder no longer carries.
func TestTopOfBookSnapshotIsAuthoritative(t *testing.T) {
	var b topOfBook
	b.setFromLevels([]connector.Level{{Price: "0.40"}}, []connector.Level{{Price: "0.42"}})
	// A later one-sided snapshot: no bids, ask at 0.01.
	b.setFromLevels(nil, []connector.Level{{Price: "0.01"}})

	if b.hasBid {
		t.Errorf("bid should be cleared by the authoritative snapshot, got has=%v", b.hasBid)
	}
	if mid, ok := b.mid(); !ok || mid != 0.005 {
		t.Errorf("mid = (%v, ok=%v), want (0.005, true)", mid, ok)
	}
}
