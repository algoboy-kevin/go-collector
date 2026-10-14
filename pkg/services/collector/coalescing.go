package collector

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Price_change coalescing (opt-in, collector.price_change_coalesce_ms).
//
// An enabled session keeps at most ONE current bucket. Changed levels are keyed
// by (asset_id, side, exact canonical decimal price) and the LAST absolute size
// wins — sizes are not summed — with a zero size kept as a tombstone so a
// deleted level is still emitted. A bucket is flushed at its fixed received_at
// window end, or early (before a control/snapshot/connection transition, or
// when the session closes) at the forced cutoff. See
// docs/operators/price-change-coalescing.md.
//
// What this format gives up: the intra-window ordering and timing of the
// individual updates, and their per-update outer sequence numbers. A synthetic
// timestamp/received_at is the flush cutoff — the window end, or the actual
// forcing cutoff for an early flush (which MAY EQUAL the bucket start when the
// source update and the forcing record share one millisecond) — never a
// timestamp nudged forward past the record that forced the flush, and never
// earlier than any contributing update's arrival time. Latency/data-loss
// statistics must always be computed from the raw inputs (see
// RecordingSession.observeRawLocked) and never from these records.

// coalesceFormatVersion tags the emitted coalesced price_change layout so a
// reader can tell the synthetic records from raw ones.
const coalesceFormatVersion = 1

// Flush reasons recorded in coalesced_window.flush_reason.
const (
	flushReasonWindowEnd  = "window_end" // the bucket's own window end elapsed
	flushReasonTimer      = "timer"      // a real timer fired on feed silence
	flushReasonControl    = "control"    // a trade / tick_change / unknown control
	flushReasonSnapshot   = "snapshot"   // a book_snapshot boundary
	flushReasonResolve    = "resolve"    // the market resolved
	flushReasonFinalize   = "finalize"   // the session is closing (right-censored)
	flushReasonFallback   = "fallback"   // an unaggregatable record forced a flush
	flushReasonConnection = "connection" // a market-WS status transition
)

// coalesceStats tracks raw input vs coalesced output so metadata.json can state
// the reduction explicitly instead of implying it.
type coalesceStats struct {
	inputRecords   int64 // accepted raw price_change records (INCLUDING fallbacks)
	inputItems     int64 // accepted raw price_change change items
	emittedRecords int64 // persisted coalesced price_change records
	emittedItems   int64 // persisted coalesced change items
	regressions    int64 // raw inputs whose received_at went backwards
	malformed      int64 // price_change batches persisted raw as a fallback
}

type coalesceKey struct {
	asset string
	side  string
	price string // canonical decimal
}

// coalescePending is the session's one current bucket.
type coalescePending struct {
	bucketStart int64 // unix ms, a multiple of the window width
	lastSrcMs   int64 // latest contributing raw received_at (unix ms)
	srcRecords  int   // raw price_change records merged
	srcItems    int   // raw change items merged
	market      string
	lastSeq     int64 // final source seq_id (provenance only)

	order []coalesceKey // first-seen order, so emission is deterministic
	idx   map[coalesceKey]int
	size  map[coalesceKey]string // canonical decimal, LAST value wins
	hash  map[coalesceKey]string // last source-update hash for that level
	bbo   map[string][2]string   // asset -> final {best_bid, best_ask}
}

func newCoalescePending(bucketStart int64) *coalescePending {
	return &coalescePending{
		bucketStart: bucketStart,
		idx:         map[coalesceKey]int{},
		size:        map[coalesceKey]string{},
		hash:        map[coalesceKey]string{},
		bbo:         map[string][2]string{},
	}
}

// canonicalDecimal returns the canonical decimal string for a non-negative
// decimal number: no leading integer zeros, no trailing fractional zeros, and a
// bare "0" for zero. ok is false for anything that is not a plain decimal
// (empty, spaces, signs, exponents, NaN/Inf), so a malformed level is reported
// rather than silently accepted as a distinct price key.
func canonicalDecimal(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	intPart, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i+1:]
		if strings.IndexByte(frac, '.') >= 0 {
			return "", false
		}
	}
	if intPart == "" && frac == "" {
		return "", false
	}
	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return "", false
		}
	}
	for i := 0; i < len(frac); i++ {
		if frac[i] < '0' || frac[i] > '9' {
			return "", false
		}
	}
	intPart = strings.TrimLeft(intPart, "0")
	frac = strings.TrimRight(frac, "0")
	if intPart == "" {
		intPart = "0"
	}
	if frac == "" {
		return intPart, true
	}
	return intPart + "." + frac, true
}

// coalescedWindowInfo is the explicit window metadata block: it is what stops an
// aggregated record from masquerading as a raw one.
type coalescedWindowInfo struct {
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

// coalescedPriceChangeData mirrors the connector's PriceChangeEvent JSON (so a
// reader that already parses price_change needs only to ignore the extra field)
// plus the coalesced_window block. seq_id is the final source record's id and
// the hashes are last-update hashes: provenance, not an aggregate book digest.
type coalescedPriceChangeData struct {
	SeqID           int64                       `json:"seq_id"`
	ReceivedAt      time.Time                   `json:"received_at"`
	Market          string                      `json:"market"`
	Timestamp       time.Time                   `json:"timestamp"`
	Changes         []connector.PriceChangeItem `json:"changes"`
	CoalescedWindow *coalescedWindowInfo        `json:"coalesced_window"`
}

// expirePendingLocked emits the current bucket at its OWN window end if that
// deadline has already elapsed as of nowMs, WITHOUT waiting for the wall-clock
// timer. Callers run it before processing a later raw record (control, snapshot,
// connection transition, unaggregatable fallback) so a bucket that already
// expired is attributed to its boundary — reason window_end, cutoff
// bucketStart+window — rather than being stretched forward to the forcing
// record's arrival time. nowMs is a logical (record) time, never the wall
// clock, so a callback delayed by scheduler contention cannot move the recorded
// cutoff. Caller holds s.mu.
func (s *RecordingSession) expirePendingLocked(nowMs int64) error {
	if s.pending == nil {
		return nil
	}
	deadline := s.pending.bucketStart + s.coalesceMs
	if nowMs < deadline {
		return nil
	}
	return s.emitPendingLocked(flushReasonWindowEnd, deadline, false)
}

// flushPendingLocked emits the current bucket, if any. triggerMs is the
// received_at of the event forcing the flush (0 when there is none), used as
// the emission cutoff.
func (s *RecordingSession) flushPendingLocked(reason string, triggerMs int64, censored bool) error {
	if s.pending == nil {
		return nil
	}
	return s.emitPendingLocked(reason, triggerMs, censored)
}

// emitPendingLocked writes the current bucket out and clears it. Caller holds
// s.mu.
func (s *RecordingSession) emitPendingLocked(reason string, cutoffMs int64, censored bool) error {
	p := s.pending
	s.pending = nil
	s.stopCoalesceTimerLocked()
	if p == nil || len(p.order) == 0 {
		return nil
	}

	// The synthetic timestamp is the flush cutoff. It is never earlier than a
	// contributing update, and never before the bucket start — but an early
	// forced flush (control / snapshot / connection / fallback) uses the ACTUAL
	// cutoff and MAY EQUAL the bucket start when the source update and the
	// forcing record share one millisecond. It must never be nudged forward:
	// bucketStart+1 would stamp the synthetic record after the control that
	// forced it (future/lookahead) and after the boundary it claims to precede.
	// A normal window end is bucketStart+window, always after the start.
	if cutoffMs < p.lastSrcMs {
		cutoffMs = p.lastSrcMs
	}
	if cutoffMs < p.bucketStart {
		cutoffMs = p.bucketStart
	}

	changes := make([]connector.PriceChangeItem, 0, len(p.order))
	for _, k := range p.order {
		bb := p.bbo[k.asset]
		changes = append(changes, connector.PriceChangeItem{
			AssetID: k.asset,
			Price:   k.price,
			Size:    p.size[k],
			Side:    k.side,
			Hash:    p.hash[k],
			// Every emitted change for a token carries that token's FINAL
			// raw-order BBO, so applying them in any order cannot leave an
			// earlier level's stale BBO behind.
			BestBid: bb[0],
			BestAsk: bb[1],
		})
	}

	cut := time.UnixMilli(cutoffMs).UTC()
	data, err := json.Marshal(coalescedPriceChangeData{
		SeqID:      p.lastSeq,
		ReceivedAt: cut,
		Market:     p.market,
		Timestamp:  cut,
		Changes:    changes,
		CoalescedWindow: &coalescedWindowInfo{
			FormatVersion:        coalesceFormatVersion,
			WindowMs:             s.coalesceMs,
			BucketStartMs:        p.bucketStart,
			EmissionCutoffMs:     cutoffMs,
			LastSourceReceivedMs: p.lastSrcMs,
			FlushReason:          reason,
			SourceRecords:        p.srcRecords,
			SourceItems:          p.srcItems,
			Synthetic:            true,
			RightCensored:        censored,
		},
	})
	if err != nil {
		return err
	}

	if err := s.writeEventLocked(RecordedEvent{
		Type:       "price_change",
		Timestamp:  cutoffMs,
		ReceivedAt: cutoffMs,
		Data:       data,
	}); err != nil {
		return err
	}
	s.coalesceStats.emittedRecords++
	s.coalesceStats.emittedItems += int64(len(changes))
	return nil
}

// stopCoalesceTimerLocked cancels the pending-bucket timer and invalidates any
// callback already in flight, so a stale callback can never flush a newer
// bucket. Caller holds s.mu.
func (s *RecordingSession) stopCoalesceTimerLocked() {
	s.coalesceGen++
	if s.coalesceTimer != nil {
		s.coalesceTimer.Stop()
		s.coalesceTimer = nil
	}
}

// armCoalesceTimerLocked schedules a flush for the bucket's own window end.
// The generation counter is the deadline identity: only the newest armed
// callback may act, so a superseded (stale) one is a no-op. Caller holds s.mu.
func (s *RecordingSession) armCoalesceTimerLocked(bucketStart int64) {
	s.coalesceGen++
	gen := s.coalesceGen
	deadline := bucketStart + s.coalesceMs
	d := time.Until(time.UnixMilli(deadline))
	if d < 0 {
		d = 0 // the window already ended: flush promptly
	}
	fire := func() { s.coalesceTimerFire(gen, bucketStart) }
	if s.newCoalesceTimer != nil {
		s.coalesceTimer = s.newCoalesceTimer(d, fire)
	} else {
		s.coalesceTimer = time.AfterFunc(d, fire)
	}
}

// coalesceTimerFire is the wall-clock bucket-boundary callback. It re-checks the
// generation and the bucket identity under s.mu, so a callback whose bucket has
// already been flushed (or whose session has closed) does nothing. The cutoff it
// records is the bucket's LOGICAL deadline, not the time the callback ran, so a
// delayed or contended fire cannot make the tape claim a later boundary.
func (s *RecordingSession) coalesceTimerFire(gen uint64, bucketStart int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if gen != s.coalesceGen || s.state != SessionRecording {
		return
	}
	if s.pending == nil || s.pending.bucketStart != bucketStart {
		return
	}
	if err := s.emitPendingLocked(flushReasonTimer, bucketStart+s.coalesceMs, false); err != nil {
		slog.Error("collector: coalesced timer flush failed", "market", s.cfg.MarketID, "err", err)
		s.failLocked(err)
	}
}

// coalescingInfoLocked builds the metadata block describing this recording's
// coalescing, or nil for a raw recording (so the schema stays unchanged).
// Caller holds s.mu.
func (s *RecordingSession) coalescingInfoLocked() *CoalescingInfo {
	if s.coalesceMs <= 0 {
		return nil
	}
	info := &CoalescingInfo{
		Enabled:                 true,
		WindowMs:                s.coalesceMs,
		Compression:             "gzip6",
		InputPriceChangeRecords: s.coalesceStats.inputRecords,
		InputPriceChangeItems:   s.coalesceStats.inputItems,
		EmittedCoalescedRecords: s.coalesceStats.emittedRecords,
		EmittedChangeItems:      s.coalesceStats.emittedItems,
		ReceiptRegressions:      s.coalesceStats.regressions,
		MalformedFallbacks:      s.coalesceStats.malformed,
	}
	if s.coalesceErr != nil {
		info.WriteError = s.coalesceErr.Error()
	}
	return info
}

// coalesceFallbackLocked persists a price_change that cannot be aggregated
// (unparseable JSON, a missing/empty market, an empty asset_id, a side that is
// not BUY/SELL, a level that is not a valid decimal, or no changes at all) RAW
// and unmodified. The pending bucket is flushed first — at its own window end if
// it already expired — so source order survives, and the fallback is counted: a
// batch that cannot be aggregated is never silently half-aggregated or dropped.
// Caller holds s.mu.
func (s *RecordingSession) coalesceFallbackLocked(ev RecordedEvent) error {
	s.coalesceStats.malformed++
	slog.Warn("collector: price_change could not be aggregated, persisting it raw",
		"market", s.cfg.MarketID)
	if err := s.expirePendingLocked(ev.ReceivedAt); err != nil {
		return err
	}
	if err := s.flushPendingLocked(flushReasonFallback, ev.ReceivedAt, false); err != nil {
		return err
	}
	return s.writeEventLocked(ev)
}

// failLocked records the first coalescing write failure. It is sticky: the
// session refuses further acceptance, and Finalize reports it, so a failed
// flush can never be mistaken for success. Caller holds s.mu.
func (s *RecordingSession) failLocked(err error) {
	if err == nil || s.coalesceMs <= 0 {
		return
	}
	if s.coalesceErr == nil {
		s.coalesceErr = err
		slog.Error("collector: coalescing write failed; refusing further events",
			"market", s.cfg.MarketID, "err", err)
	}
}

// coalescePriceChangeLocked folds one raw price_change record into the current
// bucket, flushing an expired bucket first. Caller holds s.mu.
func (s *RecordingSession) coalescePriceChangeLocked(ev RecordedEvent) error {
	// Every raw price_change record is an accepted input, INCLUDING one that
	// turns out to be malformed and is persisted raw as a fallback: the input
	// count must describe what ARRIVED, not only what got aggregated. Counted
	// here exactly once, so a fallback is never double-counted.
	s.coalesceStats.inputRecords++

	var pc connector.PriceChangeEvent
	if err := json.Unmarshal(ev.Data, &pc); err != nil {
		return s.coalesceFallbackLocked(ev)
	}
	// Parseable change items count as input items even when a non-decimal level
	// forces the whole batch to fall back (null/{} have none to count).
	s.coalesceStats.inputItems += int64(len(pc.Changes))

	// Validate the ENTIRE batch before touching the pending bucket: a
	// price_change with no market, no changes, an empty asset_id, a side that
	// is not BUY/SELL, or a non-decimal price/size is not a well-formed batch
	// and must be persisted raw rather than silently accepted, dropped, or
	// half-aggregated.
	if pc.Market == "" || len(pc.Changes) == 0 {
		return s.coalesceFallbackLocked(ev)
	}
	type level struct {
		key  coalesceKey
		size string
		hash string
	}
	levels := make([]level, 0, len(pc.Changes))
	for _, c := range pc.Changes {
		if c.AssetID == "" || (c.Side != "BUY" && c.Side != "SELL") {
			return s.coalesceFallbackLocked(ev)
		}
		price, ok := canonicalDecimal(c.Price)
		if !ok {
			return s.coalesceFallbackLocked(ev)
		}
		size, ok := canonicalDecimal(c.Size)
		if !ok {
			return s.coalesceFallbackLocked(ev)
		}
		levels = append(levels, level{coalesceKey{c.AssetID, c.Side, price}, size, c.Hash})
	}

	// Bucket from the documented monotonic arrival watermark so a regressing
	// source clock can never reorder or split a window.
	eff := s.srcWatermark
	bucket := (eff / s.coalesceMs) * s.coalesceMs
	if s.pending != nil && s.pending.bucketStart != bucket {
		if err := s.emitPendingLocked(flushReasonWindowEnd, s.pending.bucketStart+s.coalesceMs, false); err != nil {
			return err
		}
	}
	if s.pending == nil {
		s.pending = newCoalescePending(bucket)
		// A REAL wall-clock timer to the bucket's own window end, so a feed that
		// goes quiet mid-window still gets its bucket on disk. This is a window
		// flush, not fsync/durability: it bounds how long data stays pending,
		// not when the OS sees it.
		s.armCoalesceTimerLocked(bucket)
	}

	p := s.pending
	p.market = pc.Market
	p.lastSeq = pc.SeqID
	p.srcRecords++
	p.srcItems += len(pc.Changes)
	if ev.ReceivedAt > p.lastSrcMs {
		p.lastSrcMs = ev.ReceivedAt
	}
	for i, c := range pc.Changes {
		k := levels[i].key
		if _, seen := p.idx[k]; !seen {
			p.idx[k] = len(p.order)
			p.order = append(p.order, k)
		}
		p.size[k] = levels[i].size // LAST absolute size, never summed
		p.hash[k] = levels[i].hash
		// An empty side is unknown, not "no liquidity": keep the previous value
		// exactly as topOfBook.updateFromChange does.
		if c.BestBid != "" || c.BestAsk != "" {
			prev := p.bbo[c.AssetID]
			if c.BestBid != "" {
				prev[0] = c.BestBid
			}
			if c.BestAsk != "" {
				prev[1] = c.BestAsk
			}
			p.bbo[c.AssetID] = prev
		}
	}
	return nil
}
