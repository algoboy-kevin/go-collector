package collector

import (
	"path/filepath"
	"testing"
	"time"
)

// Per-session connection stats and the tail-gap completeness flag. Both exist
// because the old per-market stats were derived from the collector's capped,
// run-wide connection log, which gave every market the same disconnect count,
// and because a recording whose feed died mid-window used to be written as if
// it were complete.

// TestSessionConnectionStatsSurviveTheGlobalCap proves a session keeps its own
// disconnect stats rather than reading them from the capped global log: with
// 3000 alternating disconnect/connect callbacks the count is 1500, not the
// 1024-ish tail the global 2048-entry cap would have left behind.
func TestSessionConnectionStatsSurviveTheGlobalCap(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)

	at := sessNow
	for i := 0; i < 3000; i++ {
		status := "disconnected"
		if i%2 == 1 {
			status = "connected"
		}
		at = at.Add(time.Millisecond)
		sess.RecordConnection(status, at)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.Connections == nil {
		t.Fatal("metadata has no connections block")
	}
	if got := md.Connections.DisconnectCount; got != 1500 {
		t.Errorf("disconnect_count = %d, want 1500 (no 2048 cap artifact)", got)
	}
}

// TestSessionsAccumulateTheirOwnConnectionStats proves two sessions with
// different start times see different disconnect counts: a session only counts
// the callbacks that arrive while it is recording.
func TestSessionsAccumulateTheirOwnConnectionStats(t *testing.T) {
	ec, conn := newTestCollector(t)

	mkSession := func(id string) *RecordingSession {
		t.Helper()
		sess, err := ec.StartSession(conn, SessionConfig{
			MarketID:        id,
			YesAssetID:      id + "-yes",
			NoAssetID:       id + "-no",
			ConditionID:     "0x" + id,
			MarketStartTime: time.Now(),
			MarketEndTime:   time.Now().Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("StartSession(%s): %v", id, err)
		}
		return sess
	}
	pairs := func(n int) {
		for i := 0; i < n; i++ {
			ec.RecordConnectionEvent("disconnected")
			ec.RecordConnectionEvent("connected")
		}
	}

	a := mkSession("m-a")
	recordOne(t, a)
	pairs(5) // A sees 5 disconnects
	b := mkSession("m-b")
	recordOne(t, b)
	pairs(3) // B sees 3; A, still recording, sees them too

	for _, sess := range []*RecordingSession{a, b} {
		if err := sess.Finalize(); err != nil {
			t.Fatalf("finalize %s: %v", sess.cfg.MarketID, err)
		}
	}

	got := func(id string) int {
		md := readMarketMeta(t, filepath.Join(ec.recordingDir(), id, "metadata.json"))
		if md.Connections == nil {
			t.Fatalf("%s has no connections block", id)
		}
		return md.Connections.DisconnectCount
	}
	if n := got("m-a"); n != 8 {
		t.Errorf("session A disconnect_count = %d, want 8 (5 before B + 3 after)", n)
	}
	if n := got("m-b"); n != 3 {
		t.Errorf("session B disconnect_count = %d, want 3", n)
	}
}

// TestTailGapFlagsTruncatedWhenTheFeedDies proves a recording whose last event
// predates settlement by more than the silence timeout is flagged truncated and
// carries the gap, so a stalled feed is visible in the metadata rather than
// reading as a complete recording.
func TestTailGapFlagsTruncatedWhenTheFeedDies(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess) // last event at sessNow, hours before the settle instant

	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if !md.Truncated {
		t.Error("a recording whose last event predates settlement by hours must be flagged truncated")
	}
	if md.Resolution != ResolutionUnsettled {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
	if md.DataQuality == nil {
		t.Fatal("metadata has no data_quality block")
	}
	if md.DataQuality.LastEventTs != sessNow.UnixMilli() {
		t.Errorf("last_event_ts = %d, want %d", md.DataQuality.LastEventTs, sessNow.UnixMilli())
	}
	if want := settleAt.Sub(sessNow).Milliseconds(); md.DataQuality.TailGapMs != want {
		t.Errorf("tail_gap_ms = %d, want %d", md.DataQuality.TailGapMs, want)
	}
}

// TestResolvedSessionIsNeverTruncatedByItsTailGap proves the completeness flag
// never overrides a real outcome: a resolved market whose recording tail is
// short is left as resolved, not rewritten as unsettled.
func TestResolvedSessionIsNeverTruncatedByItsTailGap(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)
	if err := sess.Resolve(sess.cfg.YesAssetID); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.Truncated {
		t.Error("a resolved market must not be flagged truncated by its tail gap")
	}
	if md.Resolution != ResolutionYes {
		t.Errorf("resolution = %q, want %q", md.Resolution, ResolutionYes)
	}
}
