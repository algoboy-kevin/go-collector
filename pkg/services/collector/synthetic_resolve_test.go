package collector

import (
	"encoding/json"
	"path/filepath"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Synthetic-resolution tests. A market whose resolution message never reaches us
// is decided from the last order book seen a minute past the settlement instant
// (CONTEXT.md §6): a book that has collapsed to one side IS the outcome, and the
// 0.5 boundary is only the fallback for a book that never moved.
//
//	YES won → bids at ~0.99, no ask   → mid = (0.99 + 1.00)/2 = 0.995
//	NO won  → ask at ~0.01, no bid    → mid = (0.00 + 0.01)/2 = 0.005
//
// The substituted 1.00/0.00 are midpriceFromBook's defaults for a missing side,
// which is what makes a one-sided book readable at all.

// recordBook records one book snapshot for an asset, one level per pair given as
// {price, size}.
func recordBook(t *testing.T, sess *RecordingSession, assetID string, bids, asks [][2]string) {
	t.Helper()
	levels := func(pairs [][2]string) []connector.Level {
		out := make([]connector.Level, 0, len(pairs))
		for _, p := range pairs {
			out = append(out, connector.Level{Price: p[0], Size: p[1]})
		}
		return out
	}
	snap := connector.BookSnapshotEvent{
		Market:    sess.cfg.MarketID,
		AssetID:   assetID,
		Timestamp: sessNow,
		Bids:      levels(bids),
		Asks:      levels(asks),
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if !sess.Record(RecordedEvent{
		Type:       "book_snapshot",
		Timestamp:  sessNow.UnixMilli(),
		ReceivedAt: sessNow.UnixMilli(),
		Data:       data,
	}) {
		t.Fatal("record book snapshot failed")
	}
}

func TestSyntheticResolveFromTheOrderBook(t *testing.T) {
	tests := []struct {
		name        string
		bids        [][2]string
		asks        [][2]string
		wantOutcome string
		wantBasis   string
		wantMid     float64
	}{
		{
			// YES won: the YES token bids at 0.99 and nobody sells it, so the ask
			// side is empty and the midprice is (0.99 + 1.00)/2.
			name:        "collapsed book, YES wins",
			bids:        [][2]string{{"0.99", "500"}, {"0.98", "1200"}},
			wantOutcome: "YES",
			wantBasis:   ResolutionBasisCollapsedBook,
			wantMid:     0.995,
		},
		{
			// NO won: the mirror image — no bids, ask at 0.01.
			name:        "collapsed book, NO wins",
			asks:        [][2]string{{"0.01", "500"}, {"0.02", "900"}},
			wantOutcome: "NO",
			wantBasis:   ResolutionBasisCollapsedBook,
			wantMid:     0.005,
		},
		{
			// A book that never collapsed: a guess at the 0.5 boundary, recorded as
			// such so nobody reads it as evidence.
			name:        "two-sided book, YES leads",
			bids:        [][2]string{{"0.62", "100"}},
			asks:        [][2]string{{"0.64", "100"}},
			wantOutcome: "YES",
			wantBasis:   ResolutionBasisMidprice,
			wantMid:     0.63,
		},
		{
			name:        "two-sided book, NO leads",
			bids:        [][2]string{{"0.36", "100"}},
			asks:        [][2]string{{"0.38", "100"}},
			wantOutcome: "NO",
			wantBasis:   ResolutionBasisMidprice,
			wantMid:     0.37,
		},
		{
			// Nothing recorded at all: NO by default, and the basis says so.
			name:        "no book",
			wantOutcome: "NO",
			wantBasis:   ResolutionBasisNoBook,
			wantMid:     -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			sess := newTestSession(t, root, aboveContext())
			recordOne(t, sess)
			if tc.bids != nil || tc.asks != nil {
				recordBook(t, sess, sess.cfg.YesAssetID, tc.bids, tc.asks)
			}

			// The close-time timer is what normally calls this, at
			// settlement + SyntheticResolveAfter (60s, see DefaultSyntheticResolveDelay).
			sess.handleSyntheticResolve()

			if got := sess.State(); got != SessionFinalized {
				t.Fatalf("state = %s, want %s", got, SessionFinalized)
			}
			if !sess.resolvedSynthetically {
				t.Error("a session resolved from the book must be flagged synthetic")
			}
			if sess.winningOutcome != tc.wantOutcome {
				t.Errorf("winning outcome = %q, want %q", sess.winningOutcome, tc.wantOutcome)
			}
			if sess.resolveBasis != tc.wantBasis {
				t.Errorf("basis = %q, want %q", sess.resolveBasis, tc.wantBasis)
			}
			if sess.lastMidprice != tc.wantMid {
				t.Errorf("last midprice = %v, want %v", sess.lastMidprice, tc.wantMid)
			}

			md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
			if !md.SyntheticResolve {
				t.Error("metadata must flag a synthesized outcome")
			}
			if md.ResolutionBasis != tc.wantBasis {
				t.Errorf("metadata resolution_basis = %q, want %q", md.ResolutionBasis, tc.wantBasis)
			}
			if md.LastMidprice != tc.wantMid {
				t.Errorf("metadata last_midprice = %v, want %v", md.LastMidprice, tc.wantMid)
			}
			want := ResolutionYes
			if tc.wantOutcome == "NO" {
				want = ResolutionNo
			}
			if md.Resolution != want {
				t.Errorf("metadata resolution = %q, want %q", md.Resolution, want)
			}
		})
	}
}

func TestResolveBasisIsOnchainForARealResolution(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)

	// A resolution message from the market itself: no synthesis, no guess.
	if err := sess.Resolve(sess.cfg.NoAssetID); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.SyntheticResolve {
		t.Error("an on-chain resolution must not be marked synthetic")
	}
	if md.ResolutionBasis != ResolutionBasisOnchain {
		t.Errorf("resolution_basis = %q, want %q", md.ResolutionBasis, ResolutionBasisOnchain)
	}
}

func TestTruncatedSessionCarriesNoResolutionBasis(t *testing.T) {
	root := t.TempDir()
	sess := newTestSession(t, root, aboveContext())
	recordOne(t, sess)
	sess.MarkTruncated()
	if err := sess.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
	if md.Resolution != ResolutionUnsettled {
		t.Fatalf("resolution = %q, want %q", md.Resolution, ResolutionUnsettled)
	}
	if md.ResolutionBasis != "" {
		t.Errorf("resolution_basis = %q, want empty for an unsettled market", md.ResolutionBasis)
	}
}
