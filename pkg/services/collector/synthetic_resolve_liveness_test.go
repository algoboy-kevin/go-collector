package collector

import (
	"path/filepath"
	"testing"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// Synthetic-resolve liveness tests. Book age is NOT staleness: a deep-ITM/OTM
// ladder strike or a far hit/dip rung pins its book at 0.001/0.999 and may get
// no price_change for many minutes on a perfectly healthy feed — the book still
// reflects the outcome and must resolve. The book is refused only when a real
// outage (this session's own market-WS disconnect, or a whole-feed silence the
// watchdog recorded) overlaps [lastBookAt, settleAt]. When no liveness evidence
// exists at all, the hard age bound (book_stale_timeout_s) is the fallback.

// liveFeedSession builds a session whose collector is wired to a market-WS
// status source reporting the feed connected, so the synthetic resolve judges
// the book by feed liveness (disconnect/silence windows) rather than the
// no-evidence age fallback.
func liveFeedSession(t *testing.T, dir string) *RecordingSession {
	t.Helper()
	sess := newTestSession(t, dir, aboveContext())
	sess.collector.SetMarketStatus(func() ws.ConnectionStatus { return ws.StatusConnected })
	return sess
}

// TestSyntheticResolveQuietHealthyMarketResolves is the core regression: a
// strike whose book has not moved for half an hour (pinned deep ITM at 0.999)
// on a healthy feed still resolves from that book. Under the old per-market age
// rule it was mislabeled stale_book and left unsettled at every settlement.
func TestSyntheticResolveQuietHealthyMarketResolves(t *testing.T) {
	root := t.TempDir()
	sess := liveFeedSession(t, root)
	// No price_change or snapshot for 30 minutes before settlement — far past
	// the old 120s bound — on a feed with no disconnect and no silence.
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-30*time.Minute),
		[][2]string{{"0.999", "500"}}, nil)

	sess.handleSyntheticResolve()

	if got := sess.State(); got != SessionFinalized {
		t.Fatalf("state = %s, want %s", got, SessionFinalized)
	}
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES (a quiet healthy book must resolve)", sess.winningOutcome)
	}
	if !sess.resolvedSynthetically {
		t.Error("expected a synthetic resolve from the quiet book")
	}
	if sess.lastMidprice < 0.99 {
		t.Errorf("last midprice = %v, want a collapsed-book midprice ≥ 0.99", sess.lastMidprice)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisCollapsedBook {
		t.Errorf("resolution_basis = %q, want %q (not stale_book)", md.ResolutionBasis, ResolutionBasisCollapsedBook)
	}
	if md.Truncated {
		t.Error("a quiet-but-healthy market must not be left unsettled")
	}
}

// TestSyntheticResolveDeclinesWhenDisconnectCoversTheBook: the same quiet book,
// but a market-WS disconnect window from this session's own connection log
// overlaps [lastBookAt, settleAt]. The book may predate the outage, so it is
// refused (stale_book).
func TestSyntheticResolveDeclinesWhenDisconnectCoversTheBook(t *testing.T) {
	root := t.TempDir()
	sess := liveFeedSession(t, root)
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-30*time.Minute),
		[][2]string{{"0.999", "500"}}, nil)

	// The WS dropped 25 min before settlement and only came back 5 min after —
	// a real outage spanning the last 25 min of the book's quiet window.
	sess.RecordConnection("disconnected", settleAt.Add(-25*time.Minute))
	sess.RecordConnection("connected", settleAt.Add(5*time.Minute))

	sess.handleSyntheticResolve()

	assertStaleBook(t, sess, root)
}

// TestSyntheticResolveDeclinesWhenFeedSilenceCoversTheBook: no disconnect
// callback, but the watchdog recorded that the whole feed was silent across the
// interval. That silence is the evidence the book is stale.
func TestSyntheticResolveDeclinesWhenFeedSilenceCoversTheBook(t *testing.T) {
	root := t.TempDir()
	sess := liveFeedSession(t, root)
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-30*time.Minute),
		[][2]string{{"0.999", "500"}}, nil)

	sess.collector.recordFeedSilence(
		settleAt.Add(-20*time.Minute).UnixMilli(),
		settleAt.Add(10*time.Minute).UnixMilli(),
	)

	sess.handleSyntheticResolve()

	assertStaleBook(t, sess, root)
}

// assertStaleBook checks the session was left unsettled with the stale_book
// basis and written as truncated/unsettled.
func assertStaleBook(t *testing.T, sess *RecordingSession, root string) {
	t.Helper()
	if got := sess.State(); got != SessionFinalized {
		t.Fatalf("state = %s, want %s", got, SessionFinalized)
	}
	if sess.winningOutcome != "" {
		t.Errorf("winning outcome = %q, want empty (an outage-covered book must not decide)", sess.winningOutcome)
	}
	if sess.resolvedSynthetically {
		t.Error("a stale book must not be marked synthetic")
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisStaleBook {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisStaleBook)
	}
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
}

// TestSyntheticResolveDeclinesWhenSeededDisconnectedNeverReconnects builds a
// session born into a down market WS (so it seeds its own initial disconnect)
// that never reconnects: the open-ended window runs to settlement and covers
// the book, so the resolve is declined. Settlement is anchored on the live
// clock so the seeded window lands around the book.
func TestSyntheticResolveDeclinesWhenSeededDisconnectedNeverReconnects(t *testing.T) {
	root := t.TempDir()
	ec := NewCollector(CollectorConfig{RecordingDir: root}, nil)
	ec.SetMarketStatus(func() ws.ConnectionStatus { return ws.StatusDisconnected })

	settle := time.Now().Add(time.Hour)
	sess := newRecordingSession(SessionConfig{
		MarketID:        "5169514",
		Slug:            "bitcoin-above-80k-on-october-2-2026",
		YesAssetID:      "yes-token",
		NoAssetID:       "no-token",
		ConditionID:     "0xabc",
		MarketStartTime: time.Now(),
		MarketEndTime:   settle,
	}, ec)
	mc := aboveContext()
	mc.Settlement.At = settle.UnixMilli()
	sess.SetMarketContext(mc)
	t.Cleanup(func() {
		if sess.syntheticTimer != nil {
			sess.syntheticTimer.Stop()
		}
	})

	// Born into a down WS: the seed opened a disconnect window at session start.
	if len(sess.connEvents) != 1 || sess.connEvents[0].Status != "disconnected" {
		t.Fatalf("seeded connEvents = %+v, want a single initial 'disconnected'", sess.connEvents)
	}
	// A book arrives shortly after start, then the feed never comes back.
	recordBookAt(t, sess, sess.cfg.YesAssetID, time.Now().Add(time.Minute),
		[][2]string{{"0.999", "500"}}, nil)

	sess.handleSyntheticResolve()

	if sess.winningOutcome != "" {
		t.Errorf("winning outcome = %q, want empty (a still-down feed must not decide)", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisStaleBook {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisStaleBook)
	}
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
}
