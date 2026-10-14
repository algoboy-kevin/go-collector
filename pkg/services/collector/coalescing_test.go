package collector

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// ── helpers ─────────────────────────────────────────────────

// newCoalesceSession builds a session whose price_change coalescing window is
// windowMs (0 = the existing raw behaviour). MarketEndTime is far in the future
// so the synthetic-resolve timer never fires during a test.
//
// Bucket timers are captured rather than armed for real: the fixtures use fixed
// historical timestamps, whose window boundary is already in the past, so a real
// timer would fire mid-assertion. Tests that need the real timer use
// newCoalesceSessionRealTimer.
func newCoalesceSession(t *testing.T, windowMs int) *RecordingSession {
	t.Helper()
	sess := newCoalesceSessionRealTimer(t, windowMs)
	sess.mu.Lock()
	sess.newCoalesceTimer = func(time.Duration, func()) *time.Timer { return time.NewTimer(time.Hour) }
	sess.mu.Unlock()
	return sess
}

// newCoalesceSessionRealTimer is newCoalesceSession with no timer seam: bucket
// flushes go through the real time.AfterFunc.
func newCoalesceSessionRealTimer(t *testing.T, windowMs int) *RecordingSession {
	t.Helper()
	ec := NewCollector(CollectorConfig{
		RecordingDir:        t.TempDir(),
		PriceChangeCoalesce: time.Duration(windowMs) * time.Millisecond,
	}, nil)
	sess := newRecordingSession(SessionConfig{
		MarketID:        "5169514",
		Slug:            "bitcoin-above-80k",
		YesAssetID:      "yes",
		NoAssetID:       "no",
		ConditionID:     "0xabc",
		MarketStartTime: time.Now(),
		MarketEndTime:   time.Now().Add(48 * time.Hour),
	}, ec)
	t.Cleanup(func() {
		if sess.syntheticTimer != nil {
			sess.syntheticTimer.Stop()
		}
		sess.mu.Lock()
		sess.stopCoalesceTimerLocked()
		sess.mu.Unlock()
	})
	return sess
}

const testMarket = "0xmarketcondition"

func ch(asset, side, price, size string) connector.PriceChangeItem {
	return connector.PriceChangeItem{
		AssetID: asset, Side: side, Price: price, Size: size,
		Hash: "h", BestBid: "0.9", BestAsk: "0.95",
	}
}

func pcEvent(seq, recvMs int64, changes ...connector.PriceChangeItem) RecordedEvent {
	ts := time.UnixMilli(recvMs).UTC()
	data, _ := json.Marshal(connector.PriceChangeEvent{
		SeqID: seq, ReceivedAt: ts, Market: testMarket, Timestamp: ts, Changes: changes,
	})
	return RecordedEvent{Type: "price_change", Timestamp: recvMs, ReceivedAt: recvMs, Data: data}
}

func snapEvent(seq, recvMs int64, asset string, bids, asks [][2]string) RecordedEvent {
	ts := time.UnixMilli(recvMs).UTC()
	lv := func(in [][2]string) []connector.Level {
		out := make([]connector.Level, 0, len(in))
		for _, p := range in {
			out = append(out, connector.Level{Price: p[0], Size: p[1]})
		}
		return out
	}
	data, _ := json.Marshal(connector.BookSnapshotEvent{
		SeqID: seq, ReceivedAt: ts, Market: testMarket, AssetID: asset, Timestamp: ts,
		Bids: lv(bids), Asks: lv(asks),
	})
	return RecordedEvent{Type: "book_snapshot", Timestamp: recvMs, ReceivedAt: recvMs, Data: data}
}

func tradeEvent(seq, recvMs int64, asset, side, price, size string) RecordedEvent {
	ts := time.UnixMilli(recvMs).UTC()
	data, _ := json.Marshal(connector.TradeEvent{
		SeqID: seq, ReceivedAt: ts, AssetID: asset, Side: side, Price: price, Size: size, Timestamp: ts,
	})
	return RecordedEvent{Type: "trade", Timestamp: recvMs, ReceivedAt: recvMs, Data: data}
}

type recLine struct {
	SeqID      int64           `json:"seq_id"`
	Type       string          `json:"type"`
	Timestamp  int64           `json:"timestamp"`
	ReceivedAt int64           `json:"received_at"`
	Data       json.RawMessage `json:"data"`
}

type coalWindow struct {
	FormatVersion        int    `json:"format_version"`
	WindowMs             int64  `json:"window_ms"`
	BucketStartMs        int64  `json:"bucket_start_ms"`
	EmissionCutoffMs     int64  `json:"emission_cutoff_ms"`
	LastSourceReceivedMs int64  `json:"last_source_received_ms"`
	FlushReason          string `json:"flush_reason"`
	SourceRecords        int    `json:"source_price_change_records"`
	SourceItems          int    `json:"source_change_items"`
	Synthetic            bool   `json:"synthetic"`
	RightCensored        bool   `json:"right_censored"`
}

type coalescedData struct {
	SeqID           int64                       `json:"seq_id"`
	ReceivedAt      string                      `json:"received_at"`
	Market          string                      `json:"market"`
	Timestamp       string                      `json:"timestamp"`
	Changes         []connector.PriceChangeItem `json:"changes"`
	CoalescedWindow *coalWindow                 `json:"coalesced_window"`
}

// readSession reads the session's events.gz back: the raw gzip bytes (for
// header/level checks) and the decoded JSONL records.
func readSession(t *testing.T, sess *RecordingSession) ([]byte, []recLine) {
	t.Helper()
	path := filepath.Join(sess.dir(), "events.gz")
	gz, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	var lines []recLine
	for _, ln := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		if len(ln) == 0 {
			continue
		}
		var r recLine
		if err := json.Unmarshal(ln, &r); err != nil {
			t.Fatalf("unmarshal %q: %v", ln, err)
		}
		lines = append(lines, r)
	}
	return gz, lines
}

// xflFor returns the gzip XFL header byte Go writes for a compression level, so
// a level assertion is calibrated against this Go version rather than a
// hard-coded magic number.
func xflFor(t *testing.T, level int) byte {
	t.Helper()
	var b bytes.Buffer
	w, err := gzip.NewWriterLevel(&b, level)
	if err != nil {
		t.Fatalf("gzip level %d: %v", level, err)
	}
	if _, err := w.Write([]byte("calibration")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()[8]
}

func coalOf(t *testing.T, ln recLine) coalescedData {
	t.Helper()
	var d coalescedData
	if err := json.Unmarshal(ln.Data, &d); err != nil {
		t.Fatalf("decode coalesced data: %v", err)
	}
	if d.CoalescedWindow == nil {
		t.Fatalf("price_change record has no coalesced_window metadata: %s", ln.Data)
	}
	return d
}

func findChange(chs []connector.PriceChangeItem, asset, side, price string) (connector.PriceChangeItem, bool) {
	for _, c := range chs {
		if c.AssetID == asset && c.Side == side && c.Price == price {
			return c, true
		}
	}
	return connector.PriceChangeItem{}, false
}

// ── behaviour: raw mode is untouched ────────────────────────

func TestRawModeOutputUnchanged(t *testing.T) {
	sess := newCoalesceSession(t, 0)
	base := int64(1_700_000_000_000)

	pc := pcEvent(1, base, ch("yes", "BUY", "0.5", "5"))
	snap := snapEvent(2, base+1, "yes", [][2]string{{"0.5", "5"}}, nil)
	tr := tradeEvent(3, base+2, "yes", "SELL", "0.6", "3")
	for _, ev := range []RecordedEvent{pc, snap, tr} {
		if !sess.Record(ev) {
			t.Fatalf("Record(%s) = false, want true", ev.Type)
		}
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	gz, lines := readSession(t, sess)
	if len(lines) != 3 {
		t.Fatalf("raw mode persisted %d records, want 3 (one per event)", len(lines))
	}
	wantTypes := []string{"price_change", "book_snapshot", "trade"}
	for i, want := range wantTypes {
		if lines[i].Type != want {
			t.Errorf("record %d type = %q, want %q", i, lines[i].Type, want)
		}
		if lines[i].SeqID != int64(i+1) {
			t.Errorf("record %d seq_id = %d, want %d", i, lines[i].SeqID, i+1)
		}
	}
	if got := string(lines[0].Data); got != string(pc.Data) {
		t.Errorf("raw mode must preserve the original data JSON:\n got %s\nwant %s", got, pc.Data)
	}
	if lines[0].ReceivedAt != base || lines[0].Timestamp != base {
		t.Errorf("raw mode must keep the source timestamps, got %d/%d", lines[0].Timestamp, lines[0].ReceivedAt)
	}
	if got, want := gz[8], xflFor(t, gzip.BestSpeed); got != want {
		t.Errorf("raw mode gzip XFL = %d, want %d (gzip.BestSpeed)", got, want)
	}
}

// ── behaviour: last absolute size per level, window metadata ─

func TestCoalesceKeepsLastValueAndEmitsWindowMetadata(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000) // multiple of 100 → bucket start == base

	mustRecord(t, sess, pcEvent(1, base+10,
		withBBO(ch("yes", "BUY", "0.5", "5"), "0.90", "0.95"),
		withBBO(ch("no", "SELL", "0.6", "7"), "0.30", "0.35"),
	))
	mustRecord(t, sess, pcEvent(2, base+70, withBBO(ch("yes", "BUY", "0.5", "9"), "0.91", "0.96")))
	mustRecord(t, sess, pcEvent(3, base+80, withBBO(ch("yes", "BUY", "0.6", "4"), "0.91", "0.96")))
	mustRecord(t, sess, pcEvent(4, base+120, withBBO(ch("no", "SELL", "0.6", "2"), "0.30", "0.35")))
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	gz, lines := readSession(t, sess)
	if len(lines) != 2 {
		t.Fatalf("persisted %d coalesced records, want 2 (one per bucket)", len(lines))
	}

	d1 := coalOf(t, lines[0])
	w := d1.CoalescedWindow
	if w.FormatVersion != 1 || w.WindowMs != 100 || !w.Synthetic {
		t.Errorf("window metadata = %+v, want version 1 / 100ms / synthetic", w)
	}
	if w.BucketStartMs != base {
		t.Errorf("bucket_start_ms = %d, want %d", w.BucketStartMs, base)
	}
	if w.EmissionCutoffMs != base+100 {
		t.Errorf("window_end flush cutoff = %d, want the window end %d (never the bucket start)", w.EmissionCutoffMs, base+100)
	}
	if lines[0].ReceivedAt != base+100 || lines[0].Timestamp != base+100 {
		t.Errorf("synthetic outer timestamps = %d/%d, want the window end %d",
			lines[0].Timestamp, lines[0].ReceivedAt, base+100)
	}
	if w.FlushReason != "window_end" {
		t.Errorf("flush_reason = %q, want window_end", w.FlushReason)
	}
	if w.SourceRecords != 3 || w.SourceItems != 4 {
		t.Errorf("source counts = %d records / %d items, want 3/4", w.SourceRecords, w.SourceItems)
	}

	if got, ok := findChange(d1.Changes, "yes", "BUY", "0.5"); !ok {
		t.Fatal("the changed 0.5 level is missing from the aggregated record")
	} else if got.Size != "9" {
		t.Errorf("level 0.5 size = %q, want the LAST value \"9\" (a SUM would give 14)", got.Size)
	}
	if got, ok := findChange(d1.Changes, "yes", "BUY", "0.6"); !ok || got.Size != "4" {
		t.Errorf("second changed price lost: %+v", got)
	}
	if got, ok := findChange(d1.Changes, "no", "SELL", "0.6"); !ok || got.Size != "7" {
		t.Errorf("the other token's level lost: %+v", got)
	}
	if len(d1.Changes) != 3 {
		t.Errorf("emitted %d distinct levels, want 3", len(d1.Changes))
	}
	// Final token BBO must be the last raw-order BBO, not an earlier level's.
	for _, c := range d1.Changes {
		if c.AssetID == "yes" && (c.BestBid != "0.91" || c.BestAsk != "0.96") {
			t.Errorf("stale BBO on yes level %s: %s/%s, want 0.91/0.96", c.Price, c.BestBid, c.BestAsk)
		}
	}

	d2 := coalOf(t, lines[1])
	if d2.CoalescedWindow.FlushReason != "finalize" || !d2.CoalescedWindow.RightCensored {
		t.Errorf("second bucket = %+v, want a right-censored finalize flush", d2.CoalescedWindow)
	}
	if d2.CoalescedWindow.EmissionCutoffMs != base+120 {
		t.Errorf("finalize cutoff = %d, want the last source received_at %d", d2.CoalescedWindow.EmissionCutoffMs, base+120)
	}
	if got, ok := findChange(d2.Changes, "no", "SELL", "0.6"); !ok || got.Size != "2" {
		t.Errorf("second bucket change lost: %+v", got)
	}

	if got, want := gz[8], xflFor(t, gzip.DefaultCompression); got != want {
		t.Errorf("coalesced gzip XFL = %d, want %d (gzip.DefaultCompression / L6)", got, want)
	}
}

// ── behaviour: tombstones and canonical decimal identity ────

func TestCoalesceTombstonesAndCanonicalDecimals(t *testing.T) {
	t.Run("zero_size_is_kept_as_a_tombstone", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		base := int64(1_700_000_000_000)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, pcEvent(2, base+20, ch("yes", "BUY", "0.5", "0")))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 1 {
			t.Fatalf("records = %d, want 1", len(lines))
		}
		d := coalOf(t, lines[0])
		got, ok := findChange(d.Changes, "yes", "BUY", "0.5")
		if !ok {
			t.Fatal("a deleted level must still be emitted as a zero-size tombstone")
		}
		if got.Size != "0" {
			t.Errorf("tombstone size = %q, want \"0\"", got.Size)
		}
	})

	t.Run("equivalent_decimals_are_one_level", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		base := int64(1_700_000_000_000)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.50", "5")))
		mustRecord(t, sess, pcEvent(2, base+20, ch("yes", "BUY", "0.5", "0")))
		mustRecord(t, sess, pcEvent(3, base+30, ch("yes", "BUY", "0.500", "3")))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		d := coalOf(t, lines[0])
		if len(d.Changes) != 1 {
			t.Fatalf("equivalent decimals produced %d levels, want 1: %+v", len(d.Changes), d.Changes)
		}
		if d.Changes[0].Price != "0.5" {
			t.Errorf("emitted price = %q, want the canonical \"0.5\"", d.Changes[0].Price)
		}
		if d.Changes[0].Size != "3" {
			t.Errorf("size = %q, want the last value \"3\" (add→delete→re-add survives)", d.Changes[0].Size)
		}
	})

	t.Run("both_tokens_sides_and_prices_survive", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		base := int64(1_700_000_000_000)
		mustRecord(t, sess, pcEvent(1, base+10,
			ch("yes", "BUY", "0.40", "1"),
			ch("yes", "SELL", "0.60", "2"),
			ch("no", "BUY", "0.40", "3"),
			ch("no", "SELL", "0.60", "4"),
		))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		d := coalOf(t, lines[0])
		want := [][3]string{{"yes", "BUY", "1"}, {"yes", "SELL", "2"}, {"no", "BUY", "3"}, {"no", "SELL", "4"}}
		if len(d.Changes) != len(want) {
			t.Fatalf("emitted %d levels, want %d: %+v", len(d.Changes), len(want), d.Changes)
		}
		for _, w := range want {
			// the emitted prices are the canonical forms of 0.40 / 0.60
			price := "0.4"
			if w[1] == "SELL" {
				price = "0.6"
			}
			got, ok := findChange(d.Changes, w[0], w[1], price)
			if !ok || got.Size != w[2] {
				t.Errorf("level %s/%s@%s = %+v, want size %s", w[0], w[1], price, got, w[2])
			}
		}
	})
}

func withBBO(c connector.PriceChangeItem, bid, ask string) connector.PriceChangeItem {
	c.BestBid, c.BestAsk = bid, ask
	return c
}

func mustRecord(t *testing.T, sess *RecordingSession, ev RecordedEvent) {
	t.Helper()
	if !sess.Record(ev) {
		t.Fatalf("Record(%s) = false, want true", ev.Type)
	}
}

// ── independent replay model ────────────────────────────────
//
// bookState is a deliberately separate implementation of the recorded book
// (price-level state + per-token BBO). It is driven twice: from the raw inputs
// (the reference) and from the bytes the production session actually wrote to
// events.gz (the replay). The two are compared at EVERY output boundary, so a
// wrong aggregate, a dropped tombstone, a stale level BBO or a misordered flush
// all fail — comparing the emitted dictionary to itself would not.

type bookState struct {
	levels map[string]string    // "asset|side|price" -> size
	bbo    map[string][2]string // asset -> {best_bid, best_ask}
}

func newBookState() *bookState {
	return &bookState{levels: map[string]string{}, bbo: map[string][2]string{}}
}

func lvlKey(asset, side, price string) string { return asset + "|" + side + "|" + price }

func (b *bookState) setLevel(asset, side, price, size string) {
	p, ok := canonicalDecimal(price)
	if !ok {
		panic("test model: malformed price " + price)
	}
	s, ok := canonicalDecimal(size)
	if !ok {
		panic("test model: malformed size " + size)
	}
	k := lvlKey(asset, side, p)
	if s == "0" {
		delete(b.levels, k)
		return
	}
	b.levels[k] = s
}

func (b *bookState) mergeBBO(asset, bid, ask string) {
	if bid == "" && ask == "" {
		return
	}
	prev := b.bbo[asset]
	if bid != "" {
		prev[0] = bid
	}
	if ask != "" {
		prev[1] = ask
	}
	b.bbo[asset] = prev
}

func (b *bookState) replace(asset string, bids, asks [][2]string) {
	for k := range b.levels {
		if len(k) >= len(asset)+1 && k[:len(asset)+1] == asset+"|" {
			delete(b.levels, k)
		}
	}
	for _, l := range bids {
		b.setLevel(asset, "BUY", l[0], l[1])
	}
	for _, l := range asks {
		b.setLevel(asset, "SELL", l[0], l[1])
	}
}

// key is a deterministic serialization used for equality.
func (b *bookState) key() string {
	keys := make([]string, 0, len(b.levels))
	for k := range b.levels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(b.levels[k])
		sb.WriteByte(';')
	}
	assets := make([]string, 0, len(b.bbo))
	for a := range b.bbo {
		assets = append(assets, a)
	}
	sort.Strings(assets)
	for _, a := range assets {
		v := b.bbo[a]
		sb.WriteString("BBO:")
		sb.WriteString(a)
		sb.WriteString("=")
		sb.WriteString(v[0])
		sb.WriteByte('/')
		sb.WriteString(v[1])
		sb.WriteByte(';')
	}
	return sb.String()
}

// rawStep is one raw input as fed to Record, with what the reference model needs.
type rawStep struct {
	typ     string
	bucket  int64 // price_change only: effective window start
	changes []connector.PriceChangeItem
	asset   string
	bids    [][2]string
	asks    [][2]string
}

func (s rawStep) applyTo(b *bookState) {
	switch s.typ {
	case "price_change":
		for _, c := range s.changes {
			b.setLevel(c.AssetID, c.Side, c.Price, c.Size)
			b.mergeBBO(c.AssetID, c.BestBid, c.BestAsk)
		}
	case "book_snapshot":
		b.replace(s.asset, s.bids, s.asks)
	}
}

// verifyReplay walks the emitted tape and the raw inputs together and asserts
// full price-level + per-token BBO equality at every output boundary.
func verifyReplay(t *testing.T, lines []recLine, raw []rawStep) int {
	t.Helper()
	ref := newBookState()
	replay := newBookState()
	cursor := 0
	checks := 0

	consumeLeadingPriceChanges := func() {
		for cursor < len(raw) && raw[cursor].typ == "price_change" {
			raw[cursor].applyTo(ref)
			cursor++
		}
	}

	for i, ln := range lines {
		switch ln.Type {
		case "price_change":
			var d coalescedData
			if err := json.Unmarshal(ln.Data, &d); err != nil {
				t.Fatalf("line %d: decode: %v", i+1, err)
			}
			if d.CoalescedWindow == nil {
				t.Fatalf("line %d: coalesced record without window metadata", i+1)
			}
			// A coalesced record summarizes every price_change input in this
			// bucket or an earlier one; controls are never folded in.
			for cursor < len(raw) && raw[cursor].typ == "price_change" &&
				raw[cursor].bucket <= d.CoalescedWindow.BucketStartMs {
				raw[cursor].applyTo(ref)
				cursor++
			}
			for _, c := range d.Changes {
				replay.setLevel(c.AssetID, c.Side, c.Price, c.Size)
				replay.mergeBBO(c.AssetID, c.BestBid, c.BestAsk)
			}
		case "book_snapshot":
			consumeLeadingPriceChanges()
			if cursor >= len(raw) || raw[cursor].typ != "book_snapshot" {
				t.Fatalf("line %d: book_snapshot emitted out of order (raw cursor at %d)", i+1, cursor)
			}
			raw[cursor].applyTo(ref)
			cursor++
			var snap connector.BookSnapshotEvent
			if err := json.Unmarshal(ln.Data, &snap); err != nil {
				t.Fatalf("line %d: decode snapshot: %v", i+1, err)
			}
			bids := make([][2]string, 0, len(snap.Bids))
			for _, l := range snap.Bids {
				bids = append(bids, [2]string{l.Price, l.Size})
			}
			asks := make([][2]string, 0, len(snap.Asks))
			for _, l := range snap.Asks {
				asks = append(asks, [2]string{l.Price, l.Size})
			}
			replay.replace(snap.AssetID, bids, asks)
		default:
			if len(raw) > 0 {
				if cursor >= len(raw) || raw[cursor].typ != ln.Type {
					t.Fatalf("line %d: %s emitted out of order (raw cursor at %d, next raw is %q)",
						i+1, ln.Type, cursor, raw[cursor].typ)
				}
			}
			cursor++
		}
		checks++
		if ref.key() != replay.key() {
			t.Fatalf("line %d (%s): replayed book != the raw reference book\n replay: %s\n raw   : %s",
				i+1, ln.Type, replay.key(), ref.key())
		}
	}
	return checks
}

// ── behaviour: full-book + token-BBO equality at every boundary ─

func TestCoalesceReplayMatchesRawReferenceBook(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)

	snapBids := [][2]string{{"0.5", "50"}, {"0.55", "60"}}
	snapAsks := [][2]string{{"0.8", "10"}}

	raw := []rawStep{
		{typ: "price_change", bucket: base, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.5", "5"), "0.90", "0.95"),
			withBBO(ch("no", "SELL", "0.6", "7"), "0.30", "0.35"),
		}},
		{typ: "price_change", bucket: base, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.5", "9"), "0.91", "0.96"),
		}},
		{typ: "price_change", bucket: base, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.5", "0"), "0.91", "0.96"),
		}},
		{typ: "price_change", bucket: base + 100, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.5", "3"), "0.91", "0.96"),
			withBBO(ch("no", "SELL", "0.6", "0"), "0.30", "0.35"),
		}},
		{typ: "price_change", bucket: base + 100, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.7", "2"), "0.92", "0.97"),
		}},
		{typ: "trade"},
		{typ: "price_change", bucket: base + 200, changes: []connector.PriceChangeItem{
			withBBO(ch("no", "BUY", "0.4", "1"), "0.29", "0.34"),
		}},
		{typ: "book_snapshot", asset: "yes", bids: snapBids, asks: snapAsks},
		{typ: "price_change", bucket: base + 300, changes: []connector.PriceChangeItem{
			withBBO(ch("yes", "BUY", "0.52", "1"), "0.50", "0.80"),
		}},
	}

	times := []int64{base + 10, base + 20, base + 30, base + 110, base + 120, base + 150, base + 210, base + 250, base + 310}
	for i, s := range raw {
		recv := times[i]
		switch s.typ {
		case "price_change":
			sess.Record(pcEvent(int64(i+1), recv, s.changes...))
		case "trade":
			sess.Record(tradeEvent(int64(i+1), recv, "yes", "SELL", "0.6", "3"))
		case "book_snapshot":
			sess.Record(snapEvent(int64(i+1), recv, s.asset, s.bids, s.asks))
		}
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	_, lines := readSession(t, sess)
	if len(lines) != 6 {
		t.Fatalf("persisted %d records, want 6: %v", len(lines), typesOf(lines))
	}
	if checks := verifyReplay(t, lines, raw); checks != 6 {
		t.Fatalf("boundary checks = %d, want 6", checks)
	}
}

func typesOf(lines []recLine) []string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.Type)
	}
	return out
}

// ── behaviour: flush ordering around controls ───────────────

func TestCoalesceFlushOrderingAroundControls(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)

	tr := tradeEvent(2, base+50, "yes", "SELL", "0.6", "3")
	snap := snapEvent(4, base+70, "yes", [][2]string{{"0.5", "50"}}, [][2]string{{"0.8", "10"}})

	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, tr)
	mustRecord(t, sess, pcEvent(3, base+60, ch("yes", "BUY", "0.5", "6")))
	mustRecord(t, sess, snap)
	mustRecord(t, sess, pcEvent(5, base+250, ch("yes", "BUY", "0.5", "7")))
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	_, lines := readSession(t, sess)
	wantSeq := []string{"price_change", "trade", "price_change", "book_snapshot", "price_change"}
	if len(lines) != len(wantSeq) {
		t.Fatalf("persisted %v, want %v", typesOf(lines), wantSeq)
	}
	for i, want := range wantSeq {
		if lines[i].Type != want {
			t.Fatalf("record %d = %q, want %q (a pending bucket must flush BEFORE the control)", i, lines[i].Type, want)
		}
	}

	// The flush before the trade carries the trade's own received_at as cutoff.
	d1 := coalOf(t, lines[0])
	if d1.CoalescedWindow.FlushReason != "control" || d1.CoalescedWindow.EmissionCutoffMs != base+50 {
		t.Errorf("pre-trade flush = %+v, want reason=control cutoff=%d", d1.CoalescedWindow, base+50)
	}
	// The trade itself is preserved byte-for-byte and keeps its own timestamp.
	if !bytes.Equal(lines[1].Data, tr.Data) || lines[1].ReceivedAt != base+50 {
		t.Errorf("the trade record was not preserved verbatim")
	}
	d3 := coalOf(t, lines[2])
	if d3.CoalescedWindow.FlushReason != "snapshot" || d3.CoalescedWindow.EmissionCutoffMs != base+70 {
		t.Errorf("pre-snapshot flush = %+v, want reason=snapshot cutoff=%d", d3.CoalescedWindow, base+70)
	}
	if lines[2].SeqID >= lines[3].SeqID {
		t.Errorf("the coalesced record must precede the snapshot: seq %d >= %d", lines[2].SeqID, lines[3].SeqID)
	}
	if !bytes.Equal(lines[3].Data, snap.Data) {
		t.Errorf("the snapshot record was not preserved verbatim")
	}
	// The same window start can be emitted twice (the trade split it), and the
	// second emission must only carry what came after the boundary.
	if d3.CoalescedWindow.BucketStartMs != base || d1.CoalescedWindow.BucketStartMs != base {
		t.Errorf("both pre-boundary flushes belong to window %d", base)
	}
	if d3.CoalescedWindow.SourceRecords != 1 {
		t.Errorf("the post-trade flush merged %d records, want 1", d3.CoalescedWindow.SourceRecords)
	}
}

func TestCoalesceLateControlAndExactBoundary(t *testing.T) {
	t.Run("late_control_after_an_expired_bucket", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		base := int64(1_700_000_000_000)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		// The control arrives long after the window closed, with no event in
		// between: the expired bucket must be emitted at its OWN window end
		// first — NOT stretched forward to the late control's arrival time.
		late := base + 5000
		mustRecord(t, sess, tradeEvent(2, late, "yes", "SELL", "0.6", "3"))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 || lines[0].Type != "price_change" || lines[1].Type != "trade" {
			t.Fatalf("records = %v, want [price_change trade]", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "window_end" || w.EmissionCutoffMs != base+100 {
			t.Errorf("flush = %+v, want reason=window_end cutoff=%d (the window's own end, not the late control %d)", w, base+100, late)
		}
	})

	t.Run("update_exactly_on_the_boundary_opens_a_new_bucket", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		base := int64(1_700_000_000_000)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, pcEvent(2, base+100, ch("yes", "BUY", "0.5", "6")))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 {
			t.Fatalf("records = %v, want 2 buckets", typesOf(lines))
		}
		if got := coalOf(t, lines[0]).CoalescedWindow.BucketStartMs; got != base {
			t.Errorf("first bucket start = %d, want %d", got, base)
		}
		w2 := coalOf(t, lines[1]).CoalescedWindow
		if w2.BucketStartMs != base+100 {
			t.Errorf("an update exactly on the boundary starts the next bucket, got start %d", w2.BucketStartMs)
		}
		if w2.FlushReason != "finalize" {
			t.Errorf("second bucket flush reason = %q, want finalize", w2.FlushReason)
		}
	})
}

// ── behaviour: the real quiet-feed timer ────────────────────

// TestCoalesceTimerFlushesQuietFeed proves a bucket is flushed by a REAL
// wall-clock timer when the feed goes quiet: no further event is recorded, yet
// the window still reaches disk.
func TestCoalesceTimerFlushesQuietFeed(t *testing.T) {
	sess := newCoalesceSessionRealTimer(t, 40)
	base := time.Now().UnixMilli()
	bucket := (base / 40) * 40

	mustRecord(t, sess, pcEvent(1, bucket+5, ch("yes", "BUY", "0.5", "5")))

	deadline := time.Now().Add(3 * time.Second)
	for {
		sess.mu.Lock()
		n := sess.count
		sess.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a quiet feed never flushed its pending bucket: no real timer is armed to the window boundary")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	_, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want one timer-flushed record", typesOf(lines))
	}
	w := coalOf(t, lines[0]).CoalescedWindow
	if w.FlushReason != "timer" {
		t.Errorf("flush reason = %q, want timer", w.FlushReason)
	}
	if w.EmissionCutoffMs != bucket+40 {
		t.Errorf("cutoff = %d, want the window end %d", w.EmissionCutoffMs, bucket+40)
	}
}

// TestCoalesceStaleTimerCallbackIsHarmless proves a callback that is already in
// flight when its bucket is superseded (or when the stream closes) cannot write
// a duplicate, empty or mis-bucketed record.
func TestCoalesceStaleTimerCallbackIsHarmless(t *testing.T) {
	sess := newCoalesceSession(t, 1000)
	var captured []func()
	sess.mu.Lock()
	sess.newCoalesceTimer = func(d time.Duration, f func()) *time.Timer {
		captured = append(captured, f)
		return time.NewTimer(time.Hour) // never fires on its own
	}
	sess.mu.Unlock()

	base := int64(1_700_000_000_000) // multiple of 1000
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, pcEvent(2, base+1010, ch("yes", "BUY", "0.5", "6")))
	if len(captured) != 2 {
		t.Fatalf("armed %d timers, want 2 (one per bucket)", len(captured))
	}

	captured[0]() // stale: bucket `base` was already flushed by the rollover
	if got := sess.EventCount(); got != 1 {
		t.Errorf("a stale timer callback wrote a record: event count = %d, want 1", got)
	}

	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	captured[0]() // after close: still harmless
	captured[1]() // after close: still harmless
	if got := sess.EventCount(); got != 2 {
		t.Errorf("event count after close = %d, want 2 (no writes after close)", got)
	}

	_, lines := readSession(t, sess)
	if len(lines) != 2 {
		t.Fatalf("records = %v, want 2", typesOf(lines))
	}
	if w := coalOf(t, lines[1]).CoalescedWindow; w.FlushReason != "finalize" {
		t.Errorf("second bucket reason = %q, want finalize", w.FlushReason)
	}
}

// TestCoalesceConcurrentRecordTimerFinalize exercises Record / the bucket timer
// / Finalize concurrently under -race, and pins that a finalized session refuses
// further events.
func TestCoalesceConcurrentRecordTimerFinalize(t *testing.T) {
	for i := 0; i < 20; i++ {
		sess := newCoalesceSessionRealTimer(t, 5)
		base := time.Now().UnixMilli()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 60; j++ {
				sess.Record(pcEvent(int64(j+1), base+int64(j), ch("yes", "BUY", "0.5", "5")))
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			if err := sess.Finalize(); err != nil {
				t.Errorf("Finalize: %v", err)
			}
		}()
		wg.Wait()
		if err := sess.Finalize(); err != nil { // repeated finalization
			t.Fatalf("repeated Finalize: %v", err)
		}
		if sess.Record(pcEvent(999, base+5, ch("yes", "BUY", "0.5", "5"))) {
			t.Fatal("a finalized session must refuse further events")
		}
		if _, lines := readSession(t, sess); len(lines) == 0 {
			t.Fatal("no records persisted")
		}
	}
}

// ── behaviour: lifecycle flushing + metadata + source stats ─

func readMeta(t *testing.T, sess *RecordingSession) map[string]any {
	t.Helper()
	path := filepath.Join(sess.marketRoot(), sess.cfg.MarketID, "metadata.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return m
}

// TestCoalesceResolveFlushesPendingWithTombstone proves Resolve flushes the
// pending bucket (a resolution ends the market, so nothing may stay pending),
// that the final tombstone survives, and that the file is a valid gzip stream.
func TestCoalesceResolveFlushesPendingWithTombstone(t *testing.T) {
	sess := newCoalesceSession(t, 100_000) // effectively no automatic window end
	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, pcEvent(2, base+20, ch("yes", "BUY", "0.5", "0")))

	if err := sess.Resolve("yes"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	gz, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want the single resolved bucket", typesOf(lines))
	}
	d := coalOf(t, lines[0])
	if d.CoalescedWindow.FlushReason != "resolve" {
		t.Errorf("flush reason = %q, want resolve (Resolve must flush, not Finalize)", d.CoalescedWindow.FlushReason)
	}
	got, ok := findChange(d.Changes, "yes", "BUY", "0.5")
	if !ok || got.Size != "0" {
		t.Errorf("the final tombstone did not survive resolution: %+v", got)
	}
	// gzip trailer: the ISIZE field must equal the uncompressed length.
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip body/trailer invalid: %v", err)
	}
	zr.Close()
	want := uint32(len(raw))
	gotSize := uint32(gz[len(gz)-4]) | uint32(gz[len(gz)-3])<<8 | uint32(gz[len(gz)-2])<<16 | uint32(gz[len(gz)-1])<<24
	if gotSize != want {
		t.Errorf("gzip trailer ISIZE = %d, want %d", gotSize, want)
	}
}

// TestCoalesceMetadataStatesModeAndCounts pins the metadata.json contract:
// enabled sessions describe the mode, the interval, the compression and the
// input/emitted counts; raw recordings keep the existing schema untouched.
func TestCoalesceMetadataStatesModeAndCounts(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, pcEvent(2, base+20, ch("yes", "BUY", "0.5", "6")))
	mustRecord(t, sess, pcEvent(3, base+110, ch("yes", "BUY", "0.5", "7")))
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	meta := readMeta(t, sess)
	block, ok := meta["coalescing"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.json has no coalescing block: %v", meta["coalescing"])
	}
	checks := map[string]any{
		"enabled":                    true,
		"window_ms":                  float64(100),
		"compression":                "gzip6",
		"input_price_change_records": float64(3),
		"emitted_coalesced_records":  float64(2),
	}
	for k, want := range checks {
		if got := block[k]; got != want {
			t.Errorf("metadata coalescing.%s = %v, want %v", k, got, want)
		}
	}

	rawSess := newCoalesceSession(t, 0)
	mustRecord(t, rawSess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	if err := rawSess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if _, present := readMeta(t, rawSess)["coalescing"]; present {
		t.Error("a raw recording must not gain a coalescing block (schema must stay unchanged)")
	}
}

// TestCoalesceLeavesSourceStatsIdenticalToRaw proves the aggregation and the
// timer never feed the data-quality statistics: the same raw inputs produce the
// same latency/gap/timestamp numbers whether or not coalescing is on.
func TestCoalesceLeavesSourceStatsIdenticalToRaw(t *testing.T) {
	build := func(t *testing.T, windowMs int) map[string]any {
		sess := newCoalesceSession(t, windowMs)
		base := int64(1_700_000_000_000)
		for i := 0; i < 6; i++ {
			recv := base + int64(i)*100
			ev := pcEvent(int64(i+1), recv, withBBO(ch("yes", "BUY", "0.5", strconv.Itoa(i+1)), "0.9", "0.95"))
			ev.Timestamp = recv - 7 // constant 7ms latency
			mustRecord(t, sess, ev)
		}
		gap := pcEvent(99, base+3007, ch("yes", "BUY", "0.5", "42"))
		gap.Timestamp = base + 3000 // >2s broker gap after the last event
		mustRecord(t, sess, gap)
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		return readMeta(t, sess)
	}
	raw := build(t, 0)["data_quality"]
	coal := build(t, 100)["data_quality"]
	if !reflect.DeepEqual(raw, coal) {
		t.Errorf("data_quality differs between raw and coalesced runs\n raw  : %v\n coal : %v", raw, coal)
	}
	if dq, ok := coal.(map[string]any); ok {
		if got := dq["avg_latency_ms"]; got != float64(7) {
			t.Errorf("avg_latency_ms = %v, want 7 (raw latency, not synthetic)", got)
		}
		if got := dq["broker_timestamp_gaps"]; got != float64(1) {
			t.Errorf("broker_timestamp_gaps = %v, want 1", got)
		}
	} else {
		t.Fatalf("data_quality missing from metadata: %v", coal)
	}
}

// TestCoalescePendingInputCountsAsRecordedForNoData pins that the synthetic
// resolve's no_data check looks at ACCEPTED RAW INPUT, not at how many records
// happen to have been persisted: a market whose only price_change is still
// pending has been recorded, so it must not be classified no_data.
func TestCoalescePendingInputCountsAsRecordedForNoData(t *testing.T) {
	sess := newCoalesceSession(t, 60_000) // long window: the update stays pending
	// A market-WS status source means staleness is judged by feed liveness, not
	// by a hard age bound, so the book decides the outcome.
	sess.collector.SetMarketStatus(func() ws.ConnectionStatus { return ws.StatusConnected })

	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, withBBO(ch("yes", "BUY", "0.5", "5"), "0.90", "0.95")))

	sess.mu.Lock()
	persisted, accepted := sess.count, sess.rawEvents
	sess.mu.Unlock()
	if persisted != 0 || accepted != 1 {
		t.Fatalf("fixture: persisted=%d accepted=%d, want 0/1 (the update must still be pending)", persisted, accepted)
	}

	sess.handleSyntheticResolve()

	sess.mu.Lock()
	declined, winning, basis := sess.unresolvedBasis, sess.winningOutcome, sess.resolveBasis
	sess.mu.Unlock()
	if declined == ResolutionBasisNoData || basis == ResolutionBasisNoData {
		t.Error("a session with accepted raw input was classified no_data")
	}
	if winning == "" {
		t.Errorf("a session whose only update was still pending must not be left unsettled (declined=%q)", declined)
	}
	if basis != ResolutionBasisMidprice {
		t.Errorf("basis = %q, want %q", basis, ResolutionBasisMidprice)
	}
	// The pending bucket was flushed by Resolve, not dropped.
	_, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want the resolved bucket", typesOf(lines))
	}
	reason := coalOf(t, lines[0]).CoalescedWindow.FlushReason
	if reason != "resolve" {
		t.Errorf("flush reason = %q, want resolve", reason)
	}
}

// ── behaviour: malformed input, clock regression, write errors ─

// failingWriter makes every encode fail, deterministically, without breaking
// the gzip stream underneath.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

// TestCoalesceMalformedFallsBackToRaw proves a price_change whose levels are not
// valid decimals is never half-aggregated: the pending bucket is flushed first
// and the raw record is preserved byte-for-byte (and counted).
func TestCoalesceMalformedFallsBackToRaw(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)

	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))

	bad := pcEvent(2, base+20, connector.PriceChangeItem{
		AssetID: "yes", Side: "BUY", Price: "not-a-decimal", Size: "5",
	})
	mustRecord(t, sess, bad)

	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	_, lines := readSession(t, sess)
	if len(lines) != 2 {
		t.Fatalf("records = %v, want [price_change(aggregated) price_change(raw fallback)]", typesOf(lines))
	}
	if !bytes.Equal(lines[1].Data, bad.Data) {
		t.Errorf("a malformed price_change must be preserved unchanged\n got %s\nwant %s", lines[1].Data, bad.Data)
	}
	if got := coalOf(t, lines[0]).CoalescedWindow.FlushReason; got != "fallback" {
		t.Errorf("flush before the malformed record = %q, want fallback", got)
	}
	block, _ := readMeta(t, sess)["coalescing"].(map[string]any)
	if got := block["malformed_fallbacks"]; got != float64(1) {
		t.Errorf("metadata malformed_fallbacks = %v, want 1", got)
	}
}

// TestCoalesceClockRegressionUsesArrivalWatermark proves a source clock that
// steps backwards is counted and bucketed against the monotonic arrival
// watermark instead of silently reordering (or splitting) the window.
func TestCoalesceClockRegressionUsesArrivalWatermark(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)

	mustRecord(t, sess, pcEvent(1, base+200, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, pcEvent(2, base+50, ch("yes", "BUY", "0.5", "6"))) // steps back 150ms
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	_, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want ONE bucket (a regression must not open an earlier one)", typesOf(lines))
	}
	w := coalOf(t, lines[0]).CoalescedWindow
	if w.BucketStartMs != base+200 {
		t.Errorf("bucket start = %d, want %d (the watermark bucket)", w.BucketStartMs, base+200)
	}
	if w.SourceRecords != 2 {
		t.Errorf("merged %d records, want 2", w.SourceRecords)
	}
	if w.EmissionCutoffMs < base+200 {
		t.Errorf("cutoff %d must not precede the latest contributing received_at %d", w.EmissionCutoffMs, base+200)
	}
	block, _ := readMeta(t, sess)["coalescing"].(map[string]any)
	if got := block["receipt_regressions"]; got != float64(1) {
		t.Errorf("metadata receipt_regressions = %v, want 1", got)
	}
	if got, ok := findChange(coalOf(t, lines[0]).Changes, "yes", "BUY", "0.5"); !ok || got.Size != "6" {
		t.Errorf("source order not preserved through the regression: %+v", got)
	}
}

// TestCoalesceWriteErrorIsStickyAndSurfaced proves a failed flush is never
// silent: acceptance stops, the failure is recorded in metadata, and Finalize
// reports it instead of claiming success.
func TestCoalesceWriteErrorIsStickyAndSurfaced(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)

	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))

	// Break the encoder underneath the session, then force a flush.
	sess.mu.Lock()
	sess.enc = json.NewEncoder(failingWriter{})
	sess.mu.Unlock()

	if sess.Record(tradeEvent(2, base+20, "yes", "SELL", "0.6", "3")) {
		t.Error("a record whose flush failed must not report success")
	}
	if sess.Record(pcEvent(3, base+30, ch("yes", "BUY", "0.5", "6"))) {
		t.Error("after a write failure the session must refuse further acceptance (sticky error)")
	}

	err := sess.Finalize()
	if err == nil {
		t.Fatal("Finalize must surface the sticky write failure, not silently discard pending data")
	}
	block, ok := readMeta(t, sess)["coalescing"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.json lost its coalescing block after a write failure")
	}
	if got, _ := block["write_error"].(string); got == "" {
		t.Error("metadata must record the sticky write failure")
	}
}

// TestCoalesceStopAllFlushesPendingAndWritesTombstone exercises the shutdown
// path (StopAll → truncate → Finalize): a pending window must still reach disk,
// with its final tombstone, in a valid gzip stream.
func TestCoalesceStopAllFlushesPendingAndWritesTombstone(t *testing.T) {
	ec := NewCollector(CollectorConfig{
		RecordingDir:        t.TempDir(),
		PriceChangeCoalesce: 60 * time.Second, // never flushes on its own
	}, nil)
	sess := newRecordingSession(SessionConfig{
		MarketID: "5169514", YesAssetID: "yes", NoAssetID: "no",
		MarketStartTime: time.Now(), MarketEndTime: time.Now().Add(48 * time.Hour),
	}, ec)
	t.Cleanup(func() {
		if sess.syntheticTimer != nil {
			sess.syntheticTimer.Stop()
		}
	})
	sess.mu.Lock()
	sess.newCoalesceTimer = func(time.Duration, func()) *time.Timer { return time.NewTimer(time.Hour) }
	sess.mu.Unlock()
	ec.mu.Lock()
	ec.sessions[sess.cfg.MarketID] = sess
	ec.mu.Unlock()

	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	mustRecord(t, sess, pcEvent(2, base+20, ch("yes", "BUY", "0.5", "0")))
	if sess.EventCount() != 0 {
		t.Fatal("fixture: the window must still be pending")
	}

	ec.StopAll()

	gz, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want the flush forced by StopAll", typesOf(lines))
	}
	d := coalOf(t, lines[0])
	if d.CoalescedWindow.FlushReason != "finalize" || !d.CoalescedWindow.RightCensored {
		t.Errorf("shutdown flush = %+v, want a right-censored finalize", d.CoalescedWindow)
	}
	if got, ok := findChange(d.Changes, "yes", "BUY", "0.5"); !ok || got.Size != "0" {
		t.Errorf("the pending tombstone was lost at shutdown: %+v", got)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	if _, err := io.ReadAll(zr); err != nil {
		t.Fatalf("gzip body/trailer invalid after StopAll: %v", err)
	}
	zr.Close()
}

// ── corrections: expired-bucket expiry, zero-duration early boundary,
//    connection transitions, rejected batches, idempotent finalize ────────────

// pcRawEvent builds a raw price_change RecordedEvent with an explicit payload,
// so a test can inject a batch the aggregator must reject (or re-encode).
func pcRawEvent(recvMs int64, data []byte) RecordedEvent {
	return RecordedEvent{Type: "price_change", Timestamp: recvMs, ReceivedAt: recvMs, Data: data}
}

// pcEventWithMarket builds a price_change event whose inner market field is
// whatever the test wants (including empty).
func pcEventWithMarket(seq, recvMs int64, market string, changes ...connector.PriceChangeItem) RecordedEvent {
	ts := time.UnixMilli(recvMs).UTC()
	data, _ := json.Marshal(connector.PriceChangeEvent{
		SeqID: seq, ReceivedAt: ts, Market: market, Timestamp: ts, Changes: changes,
	})
	return RecordedEvent{Type: "price_change", Timestamp: recvMs, ReceivedAt: recvMs, Data: data}
}

// TestCoalesceExpiredBucketEmitsAtItsOwnBoundaryBeforeControl pins correction A:
// a bucket whose window already elapsed is emitted at its ORIGINAL deadline
// (reason window_end) before a later control/snapshot is processed, instead of
// being stretched forward to that record's arrival time.
func TestCoalesceExpiredBucketEmitsAtItsOwnBoundaryBeforeControl(t *testing.T) {
	base := int64(1_700_000_000_000)

	t.Run("trade_after_expiry", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, tradeEvent(2, base+1500, "yes", "SELL", "0.6", "3"))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 || lines[0].Type != "price_change" || lines[1].Type != "trade" {
			t.Fatalf("records = %v, want [price_change trade]", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "window_end" || w.EmissionCutoffMs != base+100 {
			t.Errorf("flush = %+v, want reason=window_end cutoff=%d (the window's own end, not the control %d)", w, base+100, base+1500)
		}
	})

	t.Run("snapshot_after_expiry", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, snapEvent(2, base+1500, "yes", [][2]string{{"0.5", "50"}}, [][2]string{{"0.8", "10"}}))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 || lines[0].Type != "price_change" || lines[1].Type != "book_snapshot" {
			t.Fatalf("records = %v, want [price_change book_snapshot]", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "window_end" || w.EmissionCutoffMs != base+100 {
			t.Errorf("flush = %+v, want reason=window_end cutoff=%d (not the snapshot %d)", w, base+100, base+1500)
		}
	})
}

// TestCoalesceDelayedTimerCallbackKeepsLogicalCutoff pins that a wall-clock
// callback delayed by scheduler contention still records the bucket's LOGICAL
// deadline as the emission cutoff (no wall-clock leak into the tape).
func TestCoalesceDelayedTimerCallbackKeepsLogicalCutoff(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	var fire func()
	sess.mu.Lock()
	sess.newCoalesceTimer = func(_ time.Duration, f func()) *time.Timer {
		fire = f
		return time.NewTimer(time.Hour)
	}
	sess.mu.Unlock()

	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	if fire == nil {
		t.Fatal("no bucket timer was armed")
	}
	// Invoke the captured callback as a late/delayed fire; the cutoff it writes
	// must be the window end, independent of when it actually ran.
	fire()

	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	_, lines := readSession(t, sess)
	if len(lines) != 1 {
		t.Fatalf("records = %v, want 1", typesOf(lines))
	}
	w := coalOf(t, lines[0]).CoalescedWindow
	if w.FlushReason != "timer" || w.EmissionCutoffMs != base+100 {
		t.Errorf("delayed timer flush = %+v, want reason=timer cutoff=%d", w, base+100)
	}
}

// TestCoalesceZeroDurationEarlyBoundaryHasNoLookahead pins correction C: an
// early forced flush at the SAME millisecond as the source/control records the
// ACTUAL cutoff, which may EQUAL the bucket start — never bucket start + 1,
// which would stamp the synthetic record after the record that forced it.
func TestCoalesceZeroDurationEarlyBoundaryHasNoLookahead(t *testing.T) {
	base := int64(1_700_000_000_000) // a multiple of the 100 ms window

	t.Run("trade_same_ms", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, tradeEvent(2, base, "yes", "SELL", "0.6", "3"))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 {
			t.Fatalf("records = %v, want [price_change trade]", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "control" || w.EmissionCutoffMs != base {
			t.Errorf("flush = %+v, want reason=control cutoff=%d (equal to the bucket start)", w, base)
		}
		if lines[0].Timestamp > lines[1].Timestamp {
			t.Errorf("synthetic record stamped %d, after the trade it must precede %d (lookahead)", lines[0].Timestamp, lines[1].Timestamp)
		}
	})

	t.Run("snapshot_same_ms", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base, ch("yes", "BUY", "0.5", "5")))
		mustRecord(t, sess, snapEvent(2, base, "yes", [][2]string{{"0.5", "50"}}, [][2]string{{"0.8", "10"}}))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 {
			t.Fatalf("records = %v, want [price_change book_snapshot]", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.EmissionCutoffMs != base {
			t.Errorf("snapshot boundary cutoff = %d, want %d (the bucket start)", w.EmissionCutoffMs, base)
		}
		if lines[0].Timestamp > lines[1].Timestamp {
			t.Errorf("synthetic record stamped %d, after the snapshot %d (lookahead)", lines[0].Timestamp, lines[1].Timestamp)
		}
	})

	t.Run("shutdown_at_bucket_start", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base, ch("yes", "BUY", "0.5", "5")))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 1 {
			t.Fatalf("records = %v, want 1", typesOf(lines))
		}
		if got := coalOf(t, lines[0]).CoalescedWindow.EmissionCutoffMs; got != base {
			t.Errorf("shutdown cutoff = %d, want %d (a zero-duration early boundary may equal the bucket start)", got, base)
		}
	})
}

// TestCoalesceConnectionTransitionFlushesPending pins correction B: a pending
// window may not straddle a market-WS connection transition. It is flushed
// under the session lock before the status entry, at the transition time (or at
// its own window end if that already elapsed).
func TestCoalesceConnectionTransitionFlushesPending(t *testing.T) {
	base := int64(1_700_000_000_000)

	t.Run("flush_before_the_status_entry", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		if got := sess.EventCount(); got != 0 {
			t.Fatalf("fixture: window must still be pending, have %d records", got)
		}
		sess.RecordConnection("disconnected", time.UnixMilli(base+40))
		if got := sess.EventCount(); got != 1 {
			t.Fatalf("event count after the transition = %d, want 1 (pending bucket must be flushed)", got)
		}
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 1 {
			t.Fatalf("records = %v, want the flushed bucket", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "connection" || w.EmissionCutoffMs != base+40 {
			t.Errorf("flush = %+v, want reason=connection cutoff=%d", w, base+40)
		}
	})

	t.Run("expired_bucket_emitted_at_own_boundary", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		sess.RecordConnection("connected", time.UnixMilli(base+900))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 1 {
			t.Fatalf("records = %v, want the flushed bucket", typesOf(lines))
		}
		w := coalOf(t, lines[0]).CoalescedWindow
		if w.FlushReason != "window_end" || w.EmissionCutoffMs != base+100 {
			t.Errorf("expired-bucket flush = %+v, want reason=window_end cutoff=%d", w, base+100)
		}
	})

	t.Run("pending_does_not_cross_a_transition", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		sess.RecordConnection("disconnected", time.UnixMilli(base+30))
		mustRecord(t, sess, pcEvent(2, base+40, ch("yes", "BUY", "0.5", "6")))
		if err := sess.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
		_, lines := readSession(t, sess)
		if len(lines) != 2 {
			t.Fatalf("records = %v, want a pre-transition bucket and a post-transition one", typesOf(lines))
		}
		if w := coalOf(t, lines[0]).CoalescedWindow; w.FlushReason != "connection" {
			t.Errorf("first bucket reason = %q, want connection", w.FlushReason)
		}
		// The post-transition bucket must retain ONLY updates after the split.
		if got, ok := findChange(coalOf(t, lines[1]).Changes, "yes", "BUY", "0.5"); !ok || got.Size != "6" {
			t.Errorf("post-transition bucket = %+v, want the split at the transition", got)
		}
	})

	t.Run("flush_failure_is_sticky", func(t *testing.T) {
		sess := newCoalesceSession(t, 100)
		mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
		sess.mu.Lock()
		sess.enc = json.NewEncoder(failingWriter{})
		sess.mu.Unlock()
		sess.RecordConnection("disconnected", time.UnixMilli(base+40))
		sess.mu.Lock()
		sticky := sess.coalesceErr
		sess.mu.Unlock()
		if sticky == nil {
			t.Error("a failed flush on a connection transition must record the sticky error")
		}
	})
}

// TestCoalesceRejectedBatchFallsBackToRaw pins correction D: a price_change
// batch with a missing/empty market, an empty asset_id, a non-BUY/SELL side, no
// changes at all, or unparseable JSON is never silently accepted, dropped or
// half-aggregated — the pending window is flushed and the record is persisted
// raw and counted as an input.
func TestCoalesceRejectedBatchFallsBackToRaw(t *testing.T) {
	base := int64(1_700_000_000_000)

	cases := []struct {
		name  string
		ev    RecordedEvent
		items int // parseable change items in the rejected batch
	}{
		{"null_json", pcRawEvent(base+20, []byte("null")), 0},
		{"empty_object", pcRawEvent(base+20, []byte("{}")), 0},
		{"no_changes", pcEventWithMarket(2, base+20, testMarket), 0},
		{"missing_market", pcEventWithMarket(2, base+20, "", ch("yes", "BUY", "0.5", "5")), 1},
		{"empty_asset_id", pcEventWithMarket(2, base+20, testMarket, ch("", "BUY", "0.5", "5")), 1},
		{"invalid_side", pcEventWithMarket(2, base+20, testMarket, ch("yes", "HOLD", "0.5", "5")), 1},
		{"not_a_decimal", pcEvent(2, base+20, connector.PriceChangeItem{AssetID: "yes", Side: "BUY", Price: "x", Size: "5"}), 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sess := newCoalesceSession(t, 100)
			mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
			if !sess.Record(c.ev) {
				t.Fatalf("Record(%s) = false: a rejected batch must be persisted raw, not dropped", c.name)
			}
			if err := sess.Finalize(); err != nil {
				t.Fatalf("Finalize: %v", err)
			}
			_, lines := readSession(t, sess)
			if len(lines) != 2 {
				t.Fatalf("records = %v, want [aggregated, raw fallback]", typesOf(lines))
			}
			if !bytes.Equal(lines[1].Data, c.ev.Data) {
				t.Errorf("fallback must preserve the raw Data exactly\n got %s\nwant %s", lines[1].Data, c.ev.Data)
			}
			if got := coalOf(t, lines[0]).CoalescedWindow.FlushReason; got != "fallback" {
				t.Errorf("flush before the fallback = %q, want fallback", got)
			}
			block, ok := readMeta(t, sess)["coalescing"].(map[string]any)
			if !ok {
				t.Fatal("metadata.json lost its coalescing block")
			}
			if got := block["malformed_fallbacks"]; got != float64(1) {
				t.Errorf("malformed_fallbacks = %v, want 1", got)
			}
			// The raw fallback still counts as an input record, and its items
			// count as input items: the counts describe what ARRIVED.
			if got := block["input_price_change_records"]; got != float64(2) {
				t.Errorf("input_price_change_records = %v, want 2 (the aggregated one AND the fallback)", got)
			}
			if got := block["input_price_change_items"]; got != float64(1+c.items) {
				t.Errorf("input_price_change_items = %v, want %d", got, 1+c.items)
			}
		})
	}
}

// TestCoalesceUnencodableFallbackSurfacesError pins that a price_change whose
// raw payload is not valid JSON cannot be silently counted as success: the
// failure is surfaced (Record returns false and Finalize reports it) instead of
// pretending the batch was persisted.
func TestCoalesceUnencodableFallbackSurfacesError(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	if sess.Record(pcRawEvent(base+20, []byte("{not-json"))) {
		t.Error("a price_change payload that is not valid JSON must not report success")
	}
	if err := sess.Finalize(); err == nil {
		t.Error("Finalize must surface the coalescing write failure, not report success")
	}
}

// TestCoalesceCountsInputItemsThroughAFallback pins that input counts describe
// every parseable change item even when a non-decimal level forces the batch to
// be persisted raw, and that records are never double-counted.
func TestCoalesceCountsInputItemsThroughAFallback(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	bad := pcEvent(2, base+20,
		ch("yes", "BUY", "0.6", "1"),
		connector.PriceChangeItem{AssetID: "yes", Side: "BUY", Price: "nan", Size: "1"},
	)
	if !sess.Record(bad) {
		t.Fatal("the malformed batch must still be persisted raw")
	}
	if err := sess.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	block, _ := readMeta(t, sess)["coalescing"].(map[string]any)
	if got := block["input_price_change_records"]; got != float64(2) {
		t.Errorf("input records = %v, want 2 (no double count)", got)
	}
	if got := block["input_price_change_items"]; got != float64(3) {
		t.Errorf("input items = %v, want 3 (1 aggregated + 2 in the fallback)", got)
	}
}

// TestCoalesceRepeatedFinalizeSurfacesStickyError pins correction E: once a
// flush has failed, a repeated Finalize keeps surfacing the same failure instead
// of reporting success, and does not re-run the finalization side effects.
func TestCoalesceRepeatedFinalizeSurfacesStickyError(t *testing.T) {
	sess := newCoalesceSession(t, 100)
	base := int64(1_700_000_000_000)
	mustRecord(t, sess, pcEvent(1, base+10, ch("yes", "BUY", "0.5", "5")))
	sess.mu.Lock()
	sess.enc = json.NewEncoder(failingWriter{})
	sess.mu.Unlock()
	if sess.Record(tradeEvent(2, base+20, "yes", "SELL", "0.6", "3")) {
		t.Fatal("fixture: the forced flush must fail")
	}
	if err := sess.Finalize(); err == nil {
		t.Fatal("the first Finalize must surface the sticky write failure")
	}
	countAfterFirst := sess.EventCount()
	if err := sess.Finalize(); err == nil {
		t.Fatal("a repeated Finalize must keep surfacing the sticky failure, not report success")
	}
	if got := sess.EventCount(); got != countAfterFirst {
		t.Errorf("repeated Finalize wrote records: event count %d -> %d", countAfterFirst, got)
	}
}
