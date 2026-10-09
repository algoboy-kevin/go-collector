package collector

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Synthetic-resolution tests. A market whose resolution message never reaches us
// is decided from the last order book seen at the settlement instant — but only
// when that book is still fresh, and never by inventing a default:
//
//	YES won  → bids at ~0.99, no ask  → mid = (0.99 + 1.00)/2 = 0.995
//	NO won   → ask at ~0.01, no bid   → mid = (0.00 + 0.01)/2 = 0.005
//	nothing recorded            → no outcome, basis "no_data"
//	data but never a book       → no outcome, basis "no_book"
//	a book already stale at close → no outcome, basis "stale_book"
//
// The substituted 1.00/0.00 are midpriceFromBook's defaults for a missing side,
// which is what makes a one-sided book readable at all.

// recordBookAt records one book snapshot for an asset at `at`, one level per
// pair given as {price, size}.
func recordBookAt(t *testing.T, sess *RecordingSession, assetID string, at time.Time, bids, asks [][2]string) {
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
		Timestamp: at,
		Bids:      levels(bids),
		Asks:      levels(asks),
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if !sess.Record(RecordedEvent{
		Type:       "book_snapshot",
		Timestamp:  at.UnixMilli(),
		ReceivedAt: at.UnixMilli(),
		Data:       data,
	}) {
		t.Fatal("record book snapshot failed")
	}
}

func TestSyntheticResolveFromTheOrderBook(t *testing.T) {
	tests := []struct {
		name string
		// record is the market data recorded before settlement.
		record func(t *testing.T, sess *RecordingSession)
		want   string // "YES" | "NO" | "" (unresolved)
		basis  string
		mid    float64
	}{
		{
			// YES won: the YES token bids at 0.99 and nobody sells it, so the ask
			// side is empty and the midprice is (0.99 + 1.00)/2.
			name: "collapsed book, YES wins",
			record: func(t *testing.T, sess *RecordingSession) {
				recordOne(t, sess)
				recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt,
					[][2]string{{"0.99", "500"}, {"0.98", "1200"}}, nil)
			},
			want: "YES", basis: ResolutionBasisCollapsedBook, mid: 0.995,
		},
		{
			// NO won: the mirror image — no bids, ask at 0.01.
			name: "collapsed book, NO wins",
			record: func(t *testing.T, sess *RecordingSession) {
				recordOne(t, sess)
				recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt,
					nil, [][2]string{{"0.01", "500"}, {"0.02", "900"}})
			},
			want: "NO", basis: ResolutionBasisCollapsedBook, mid: 0.005,
		},
		{
			// A book that never collapsed: a guess at the 0.5 boundary, recorded
			// as such so nobody reads it as evidence.
			name: "two-sided book, YES leads",
			record: func(t *testing.T, sess *RecordingSession) {
				recordOne(t, sess)
				recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-30*time.Second),
					[][2]string{{"0.62", "100"}}, [][2]string{{"0.64", "100"}})
			},
			want: "YES", basis: ResolutionBasisMidprice, mid: 0.63,
		},
		{
			name: "two-sided book, NO leads",
			record: func(t *testing.T, sess *RecordingSession) {
				recordOne(t, sess)
				recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt,
					[][2]string{{"0.36", "100"}}, [][2]string{{"0.38", "100"}})
			},
			want: "NO", basis: ResolutionBasisMidprice, mid: 0.37,
		},
		{
			// Nothing recorded at all: the outcome is left empty and the basis
			// says why — an invented outcome here is the bug being fixed.
			name:   "zero events",
			record: func(t *testing.T, sess *RecordingSession) {},
			want:   "", basis: ResolutionBasisNoData, mid: 0,
		},
		{
			// Market data but never a book: there is nothing to read, so the
			// outcome is left empty rather than defaulted to NO.
			name:   "no book",
			record: func(t *testing.T, sess *RecordingSession) { recordOne(t, sess) },
			want:   "", basis: ResolutionBasisNoBook, mid: 0,
		},
		{
			// The book is hours old at the settlement instant (a feed that died):
			// it must not decide the outcome.
			name: "stale book",
			record: func(t *testing.T, sess *RecordingSession) {
				recordOne(t, sess)
				recordBookAt(t, sess, sess.cfg.YesAssetID, settleAt.Add(-6*time.Hour),
					[][2]string{{"0.99", "500"}}, nil)
			},
			want: "", basis: ResolutionBasisStaleBook, mid: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			sess := newTestSession(t, root, aboveContext())
			tc.record(t, sess)

			// The close-time timer is what normally calls this, at
			// settlement + SyntheticResolveAfter (60s, see DefaultSyntheticResolveDelay).
			sess.handleSyntheticResolve()

			if got := sess.State(); got != SessionFinalized {
				t.Fatalf("state = %s, want %s", got, SessionFinalized)
			}
			wantSynthetic := tc.want != ""
			if sess.resolvedSynthetically != wantSynthetic {
				t.Errorf("resolvedSynthetically = %v, want %v", sess.resolvedSynthetically, wantSynthetic)
			}
			if sess.winningOutcome != tc.want {
				t.Errorf("winning outcome = %q, want %q", sess.winningOutcome, tc.want)
			}
			if sess.lastMidprice != tc.mid {
				t.Errorf("last midprice = %v, want %v", sess.lastMidprice, tc.mid)
			}

			md := readMarketMeta(t, filepath.Join(root, "2026-10-02", "5169514", "metadata.json"))
			if md.SyntheticResolve != wantSynthetic {
				t.Errorf("metadata.synthetic_resolve = %v, want %v", md.SyntheticResolve, wantSynthetic)
			}
			if md.ResolutionBasis != tc.basis {
				t.Errorf("metadata resolution_basis = %q, want %q", md.ResolutionBasis, tc.basis)
			}
			if md.LastMidprice != tc.mid {
				t.Errorf("metadata last_midprice = %v, want %v", md.LastMidprice, tc.mid)
			}
			if md.WinningOutcome != tc.want {
				t.Errorf("metadata winning_outcome = %q, want %q", md.WinningOutcome, tc.want)
			}
			var want string
			switch tc.want {
			case "YES":
				want = ResolutionYes
			case "NO":
				want = ResolutionNo
			default:
				want = ResolutionUnsettled
			}
			if md.Resolution != want {
				t.Errorf("metadata resolution = %q, want %q", md.Resolution, want)
			}
			if unresolved := tc.want == ""; md.Truncated != unresolved {
				t.Errorf("metadata truncated = %v, want %v", md.Truncated, unresolved)
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
