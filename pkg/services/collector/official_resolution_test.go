package collector

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Official-resolution tests. The reviewer's finding: the post-settlement
// official outcome (the connector's Gamma-backed GetResolution) was never used,
// so a market whose resolution message was missed fell back to a guess from our
// own book even when the venue had already published the answer. Now the
// official outcome is tried first and wins; the book is only a fallback.

// startStubSession starts a recording session on a collector backed by the test
// stub connector, with the above-strike discovery context so metadata lands
// under the epoch directory.
func startStubSession(t *testing.T, ec *EventCollector, conn *stubConn, marketID string) *RecordingSession {
	t.Helper()
	sess, err := ec.StartSession(conn, SessionConfig{
		MarketID:        marketID,
		YesAssetID:      marketID + "-yes",
		NoAssetID:       marketID + "-no",
		ConditionID:     "0x" + marketID,
		MarketStartTime: time.Now(),
		// Far in the future so the synthetic timer never races the test.
		MarketEndTime: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("StartSession(%s): %v", marketID, err)
	}
	sess.SetMarketContext(aboveContext())
	t.Cleanup(func() {
		if sess.syntheticTimer != nil {
			sess.syntheticTimer.Stop()
		}
	})
	return sess
}

// TestOfficialOutcomeBeatsTheBook proves the venue's published resolution wins
// over a book that would have said the opposite.
func TestOfficialOutcomeBeatsTheBook(t *testing.T) {
	ec, conn := newTestCollector(t)
	sess := startStubSession(t, ec, conn, "m-official")
	recordOne(t, sess)
	// A NO-collapsed book (ask at 0.01, no bid) that would resolve NO 0.005 if
	// the official lookup were ignored.
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt, nil, [][2]string{{"0.01", "500"}})

	conn.resolved["m-official"] = connector.ResYes

	sess.handleSyntheticResolve()

	if got := sess.State(); got != SessionFinalized {
		t.Fatalf("state = %s, want %s", got, SessionFinalized)
	}
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES (official beats the book)", sess.winningOutcome)
	}
	if !sess.resolvedSynthetically {
		t.Error("an official resolve must not be marked as an on-chain event")
	}
	if conn.resolutionCalls != 1 {
		t.Errorf("resolution lookups = %d, want 1", conn.resolutionCalls)
	}

	md := readMarketMeta(t, filepath.Join(ec.recordingDir(), "2026-10-02", "m-official", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisGamma {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisGamma)
	}
	if md.Resolution != ResolutionYes || md.WinningOutcome != "YES" {
		t.Errorf("resolution/outcome = %q/%q, want yes/YES", md.Resolution, md.WinningOutcome)
	}
}

// TestOfficialOutcomeNilFallsBackToTheBook proves an unresolved lookup (nil,
// nil — the connector's own convention) leaves the book in charge.
func TestOfficialOutcomeNilFallsBackToTheBook(t *testing.T) {
	ec, conn := newTestCollector(t)
	sess := startStubSession(t, ec, conn, "m-nil")
	recordOne(t, sess)
	// Fresh collapsed YES book: 0.99 bids, no ask.
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt, [][2]string{{"0.99", "500"}}, nil)
	// No conn.resolved entry: GetResolution returns (nil, nil).

	sess.handleSyntheticResolve()

	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES from the book", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(ec.recordingDir(), "2026-10-02", "m-nil", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisCollapsedBook {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisCollapsedBook)
	}
}

// TestOfficialOutcomeErrorFallsBackAfterBoundedRetries proves a failing lookup
// is retried a bounded number of times and then the book decides — the resolve
// path must not hang or give up on the first transient error.
func TestOfficialOutcomeErrorFallsBackAfterBoundedRetries(t *testing.T) {
	ec, conn := newTestCollector(t)
	sess := startStubSession(t, ec, conn, "m-err")
	recordOne(t, sess)
	recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt, [][2]string{{"0.99", "500"}}, nil)
	conn.resolveErr = errors.New("gamma down")

	sess.handleSyntheticResolve()

	if conn.resolutionCalls != officialLookupAttempts {
		t.Errorf("resolution lookups = %d, want %d bounded attempts", conn.resolutionCalls, officialLookupAttempts)
	}
	if sess.winningOutcome != "YES" {
		t.Errorf("winning outcome = %q, want YES from the book fallback", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(ec.recordingDir(), "2026-10-02", "m-err", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisCollapsedBook {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisCollapsedBook)
	}
}

// TestOfficialOutcomeUsedWithNoRecordedEvents proves the official lookup is
// consulted even for a market we recorded nothing from: the venue's resolution
// stands on its own, and the emptiness of the recording is separately visible
// (event_count 0).
func TestOfficialOutcomeUsedWithNoRecordedEvents(t *testing.T) {
	ec, conn := newTestCollector(t)
	sess := startStubSession(t, ec, conn, "m-empty")
	conn.resolved["m-empty"] = connector.ResNo

	sess.handleSyntheticResolve()

	if sess.winningOutcome != "NO" {
		t.Errorf("winning outcome = %q, want NO from the official outcome", sess.winningOutcome)
	}
	md := readMarketMeta(t, filepath.Join(ec.recordingDir(), "2026-10-02", "m-empty", "metadata.json"))
	if md.ResolutionBasis != ResolutionBasisGamma {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisGamma)
	}
	if md.Resolution != ResolutionNo {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionNo)
	}
	if md.EventCount != 0 {
		t.Errorf("event_count = %d, want 0 (nothing was recorded)", md.EventCount)
	}
}
