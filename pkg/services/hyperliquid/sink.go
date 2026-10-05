package hyperliquid

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultQueueSize bounds the per-channel queue. At ~454 B/frame that is ~4 MB of
	// frames per channel in the worst case, which is what makes the process's memory
	// flat in message count rather than in message rate (PERP_COLLECTOR_SPEC.md §6.3).
	DefaultQueueSize = 8192

	// DefaultFlushEvery is how often buffered frames reach the disk. A shorter
	// interval costs more syscalls; a longer one loses more to `kill -9`.
	DefaultFlushEvery = time.Second

	// dropWarnEvery throttles the queue-full warning so a stalled disk cannot turn
	// into a log flood that competes for the same I/O.
	dropWarnEvery = time.Second
)

// SinkConfig configures one channel's sink and the writer behind it.
type SinkConfig struct {
	// Channel names the file directory and the log prefix, e.g. "l2Book". Required.
	Channel string
	// Root is the capture root, e.g. "data/hyperliquid". Required.
	Root string
	// Compression is the gzip level; see WriterConfig.Compression.
	Compression int
	// BufSize is the bufio buffer per channel. Zero selects 64 KiB.
	BufSize int
	// QueueSize bounds the pending-frame queue. Zero selects 8192.
	QueueSize int
	// FlushEvery is the flush timer interval. Zero selects 1s.
	FlushEvery time.Duration
	// RunID identifies the recorder process, stamped into the marks this sink writes
	// itself (`dropped`, `write_error`). Empty in tests that do not care.
	RunID string
}

// Sink owns one channel's write path: a bounded queue, a single writer goroutine, a
// flush ticker and the counters a capture needs to describe itself honestly.
//
// It exists because the venue's raw-frame hook runs on the read-loop goroutine, and
// doing gzip or disk I/O there would let disk latency throttle frame reads — which
// makes the venue drop the connection. So Enqueue never blocks: it either hands the
// frame to the queue or counts a drop.
//
// One goroutine per channel owns the HourlyWriter; nothing else may touch it.
type Sink struct {
	name  string
	items chan item
	w     *HourlyWriter

	flushEvery time.Duration
	stop       chan struct{}
	done       chan struct{}
	stopOnce   sync.Once

	// mu makes "nothing can enqueue after Stop" a guarantee rather than a
	// convention. Producers must still be stopped first (see Stop); this closes the
	// window where a producer that is mid-Enqueue could slip a frame in after the
	// final drain, where it would sit in the channel and be lost uncounted.
	mu      sync.RWMutex
	stopped bool

	frames           atomic.Int64
	firstFrameNS     atomic.Int64 // first venue frame accepted, unix nanos
	lastFrameNS      atomic.Int64 // most recent venue frame accepted, unix nanos
	maxGapNS         atomic.Int64 // largest gap between consecutive frames, monotonic
	sentinels        atomic.Int64
	dropped          atomic.Int64
	droppedSentinels atomic.Int64
	afterStop        atomic.Int64
	writeErrors      atomic.Int64
	bytes            atomic.Int64
	lastDropWarn     atomic.Int64 // unix nanos

	// Written only by the writer goroutine.
	//
	// prevFrameRx is the previous frame's receive time. Its monotonic reading is what
	// maxGapNS is measured from, so a wall-clock step cannot invent or hide a stall.
	prevFrameRx       time.Time
	reportedDrops     int64
	reportedWriteErrs int64

	// runID stamps the marks this sink writes itself.
	runID string

	errMu sync.Mutex
	err   error
}

// item is one line to append: a venue frame or a sentinel. Both travel the same
// queue so their order in the file is arrival order.
type item struct {
	rx       time.Time
	frame    []byte
	sentinel bool
}

// NewSink creates the sink and opens its writer, so a bad root or channel fails at
// startup rather than on the first frame that arrives. It starts the writer
// goroutine; call Stop to drain and finish the file.
func NewSink(cfg SinkConfig) (*Sink, error) {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = DefaultFlushEvery
	}

	w, err := NewHourlyWriter(WriterConfig{
		Root:        cfg.Root,
		Channel:     cfg.Channel,
		Compression: cfg.Compression,
		BufSize:     cfg.BufSize,
		RunID:       cfg.RunID,
	})
	if err != nil {
		return nil, err
	}

	s := &Sink{
		name:       cfg.Channel,
		runID:      cfg.RunID,
		items:      make(chan item, cfg.QueueSize),
		w:          w,
		flushEvery: cfg.FlushEvery,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// Name returns the channel this sink records.
func (s *Sink) Name() string { return s.name }

// Dir returns the directory the sink writes into.
func (s *Sink) Dir() string { return s.w.Dir() }

// Enqueue hands one received frame to the write path, returning false if it was
// dropped because the queue was full.
//
// rx must be sampled by the caller on the read-loop goroutine, so that queueing
// delay cannot pollute the timestamp the ingest uses for `ts_init`.
func (s *Sink) Enqueue(rx time.Time, frame []byte) bool {
	return s.enqueue(item{rx: rx, frame: frame})
}

// EnqueueSentinel hands one in-band `_meta` line to the write path. A sentinel is
// written exactly like a frame — `<rx_ns>\t<json>\n` — because the ingest reads the
// same file; it is counted separately only so a capture can report what it contains.
//
// A dropped sentinel is a hole in the *marking* of a hole, so it is counted apart
// from ordinary drops and reported.
func (s *Sink) EnqueueSentinel(rx time.Time, frame []byte) bool {
	return s.enqueue(item{rx: rx, frame: frame, sentinel: true})
}

func (s *Sink) enqueue(it item) bool {
	s.mu.RLock()
	if s.stopped {
		s.mu.RUnlock()
		// Producers are supposed to have been stopped first. Counted, not ignored:
		// this would mean the shutdown order in main is wrong.
		s.afterStop.Add(1)
		return false
	}

	select {
	case s.items <- it:
		if it.sentinel {
			s.sentinels.Add(1)
		} else {
			s.frames.Add(1)
			ns := it.rx.UnixNano()
			// Frames only: a channel's own marks must not extend its observed window.
			s.firstFrameNS.CompareAndSwap(0, ns)
			s.lastFrameNS.Store(ns)
		}
		s.mu.RUnlock()
		return true
	default:
		s.mu.RUnlock()
		if it.sentinel {
			s.droppedSentinels.Add(1)
		} else {
			s.dropped.Add(1)
		}
		s.warnDrop()
		return false
	}
}

// warnDrop logs a queue-full drop at most once per dropWarnEvery.
func (s *Sink) warnDrop() {
	now := time.Now().UnixNano()
	last := s.lastDropWarn.Load()
	if now-last < int64(dropWarnEvery) {
		return
	}
	if !s.lastDropWarn.CompareAndSwap(last, now) {
		return
	}
	// If this is ever needed on a long run the disk has stalled, and the capture is
	// already not clean — which is why the count goes into the manifest too.
	slog.Error("hyperliquid: write queue full, dropping frames",
		"channel", s.name,
		"queue", cap(s.items),
		"dropped", s.dropped.Load(),
		"dropped_sentinels", s.droppedSentinels.Load(),
	)
}

// Stop closes the queue, drains it, flushes, and finishes the hour file (gzip
// trailer, fsync, `.open` → final rename). It is idempotent and returns the first
// error the write path hit, if any.
//
// Callers must stop the producing connection first — then no frame can be enqueued
// during the drain.
func (s *Sink) Stop() error {
	s.stopOnce.Do(func() {
		s.mu.Lock() // excludes Enqueue for the rest of the shutdown
		s.stopped = true
		close(s.stop)
		<-s.done
		s.mu.Unlock()
	})
	return s.Err()
}

// run is the writer goroutine: the only thing that touches the HourlyWriter.
func (s *Sink) run() {
	defer close(s.done)

	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case it := <-s.items:
			s.writeOne(it)
		case <-ticker.C:
			s.reportLosses()
			s.flush()
		case <-s.stop:
			// Drain before finishing: frames already accepted must reach the file.
			s.drain()
			// The last chance to record a loss in the stream itself.
			s.reportLosses()
			s.flush()
			s.finish()
			return
		}
	}
}

func (s *Sink) drain() {
	for {
		select {
		case it := <-s.items:
			s.writeOne(it)
		default:
			return
		}
	}
}

// reportLosses writes the frame-loss marks into the channel's own stream.
//
// These are written by THIS goroutine straight to the writer, never enqueued: the queue
// is exactly what is full when frames are being dropped, so a mark sent through it would
// be dropped too — leaving the hole unmarked, which is the one outcome this exists to
// prevent.
//
// It exists because the consumer reads the stream and not the manifest: drops, write
// errors and clock anomalies are already counted there, but a hole in the book had no
// attributable cause in the data itself.
func (s *Sink) reportLosses() {
	if dropped := s.dropped.Load(); dropped > s.reportedDrops {
		s.reportedDrops = dropped
		s.writeSentinelNow(Sentinel{Event: EventDropped, Count: dropped})
	}
	if err := s.Err(); err != nil && s.reportedWriteErrs == 0 {
		s.reportedWriteErrs = 1
		// Best effort, and knowingly so: the thing that just failed is the write path,
		// so this may fail as well. It is still worth attempting, because the fallback
		// signal — a stream that simply ends without a `stopped` mark — is weaker, and
		// says nothing about which file or which error.
		s.writeSentinelNow(Sentinel{Event: EventWriteError, File: s.w.CurrentName(), Err: err.Error()})
	}
}

// markTime is the receive time a mark is filed under: the last frame's, so a mark always
// lands in the same hour bucket as the data it annotates.
//
// Using the wall clock here instead would let a clock step scatter marks into a different
// hour from the hole they describe — the one thing a mark must not do — and would file a
// burst of drops away from the frames that caused them.
func (s *Sink) markTime() time.Time {
	if !s.prevFrameRx.IsZero() {
		return s.prevFrameRx
	}
	return time.Now()
}

// writeSentinelNow appends one mark directly, bypassing the queue. Only the writer
// goroutine may call it.
func (s *Sink) writeSentinelNow(sc Sentinel) {
	sc.RunID = s.runID
	now := s.markTime()
	line, err := SentinelLine(sc, now)
	if err != nil {
		slog.Error("hyperliquid: build sentinel", "channel", s.name, "event", sc.Event, "err", err)
		return
	}
	if err := s.w.Write(now, line); err != nil {
		slog.Error("hyperliquid: cannot write sentinel", "channel", s.name, "event", sc.Event, "err", err)
		return
	}
	s.sentinels.Add(1)
}

func (s *Sink) writeOne(it item) {
	if s.Err() != nil {
		// The writer already failed. Keep the queue moving so producers never block,
		// but do not attempt more writes: on ENOSPC they would all fail anyway.
		return
	}
	if err := s.w.Write(it.rx, it.frame); err != nil {
		s.writeErrors.Add(1)
		s.setErr(err)
		return
	}
	if !it.sentinel {
		// Measured on rx, which is sampled on the read-loop goroutine and therefore
		// carries a monotonic reading: queueing delay cannot pollute it, and a
		// wall-clock step cannot manufacture a gap.
		if !s.prevFrameRx.IsZero() {
			if gap := it.rx.Sub(s.prevFrameRx); gap > 0 && int64(gap) > s.maxGapNS.Load() {
				s.maxGapNS.Store(int64(gap))
			}
		}
		s.prevFrameRx = it.rx
	}
	// Payload bytes only, matching HourlyWriter.Bytes: this is the number that
	// predicts disk usage, so framing must not inflate it.
	s.bytes.Add(int64(len(it.frame)))
}

func (s *Sink) flush() {
	if s.Err() != nil {
		return
	}
	if err := s.w.Flush(); err != nil {
		s.setErr(err)
	}
}

func (s *Sink) finish() {
	// Always attempt the close, even after a write error: if the file can be
	// completed the rename still happens, and if it cannot, Close skips the rename
	// and leaves the file named `.open`, which is the honest outcome.
	if err := s.w.Close(); err != nil {
		s.setErr(err)
	}
}

func (s *Sink) setErr(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err != nil {
		return
	}
	s.err = err
	slog.Error("hyperliquid: write path failed", "channel", s.name, "err", err)
}

// Err returns the first write-path error, or nil. A non-nil error means the capture
// is not clean and the caller should stop the run.
func (s *Sink) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// ─────────────────────────────────────────────────────────────
// Counters
//
// Frames + Drops is the total offered, so a gap in the file is always
// accounted for. A capture whose Drops, DroppedSentinels, WriteErrors or
// ClockRegressions are non-zero is visibly not clean.
// ─────────────────────────────────────────────────────────────

// Frames returns the venue frames accepted by the queue.
func (s *Sink) Frames() int64 { return s.frames.Load() }

// Sentinels returns the `_meta` sentinel lines accepted by the queue.
func (s *Sink) Sentinels() int64 { return s.sentinels.Load() }

// Drops returns the frames rejected because the queue was full.
func (s *Sink) Drops() int64 { return s.dropped.Load() }

// DroppedSentinels returns the sentinel lines rejected because the queue was full.
// A hole with no marker is worse than a hole, so this is reported separately.
func (s *Sink) DroppedSentinels() int64 { return s.droppedSentinels.Load() }

// AfterStop returns how many items were offered after Stop. Anything above zero
// means the shutdown order was violated, which is a bug rather than data loss.
func (s *Sink) AfterStop() int64 { return s.afterStop.Load() }

// WriteErrors returns how many writes failed. Each one is also the reason Err is
// non-nil.
func (s *Sink) WriteErrors() int64 { return s.writeErrors.Load() }

// Bytes returns the payload bytes successfully written, excluding framing.
func (s *Sink) Bytes() int64 { return s.bytes.Load() }

// LastFrame returns the receive time of the most recently accepted venue frame, or the
// zero time if none has been accepted. This is the input to quiet-channel detection.
// Sentinels are excluded deliberately: a channel that only ever produced marks has not
// received anything from the venue.
func (s *Sink) LastFrame() time.Time {
	ns := s.lastFrameNS.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// FirstFrame returns the receive time of the first venue frame accepted, or the zero
// time. The manifest records it so a run's window is readable without opening files.
func (s *Sink) FirstFrame() time.Time {
	ns := s.firstFrameNS.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// MaxGap returns the largest interval between two consecutive accepted venue frames.
//
// Measured from monotonic clock readings, so it is a real stall rather than a wall-clock
// step. It is what distinguishes "the venue paused" from "this channel is dead" on a
// paced feed like l2Book, where a missing second is invisible in the counts.
func (s *Sink) MaxGap() time.Duration { return time.Duration(s.maxGapNS.Load()) }

// ─────────────────────────────────────────────────────────────
// Writer passthroughs
//
// These read the HourlyWriter's counters, which only its single goroutine touches.
// Safe to call once Stop has returned; before that they race with the writer
// goroutine and are for tests and shutdown-time reporting only.
// ─────────────────────────────────────────────────────────────

// FilesOpened returns how many hour files this channel opened.
func (s *Sink) FilesOpened() int { return s.w.FilesOpened() }

// StaleFiles returns how many `.open` files from an earlier process were preserved.
func (s *Sink) StaleFiles() int { return s.w.StaleFiles() }

// ReplacedHours returns how many completed hours were preserved rather than
// overwritten because a new run opened the same bucket.
func (s *Sink) ReplacedHours() int { return s.w.ReplacedHours() }

// Files returns a record of every completed hour file this channel wrote.
func (s *Sink) Files() []FileRecord { return s.w.Files() }

// Quarantined returns a record of every file preserved rather than overwritten.
func (s *Sink) Quarantined() []QuarantineRecord { return s.w.Quarantined() }

// ClockRegressions returns how many frames arrived for an hour earlier than the open
// file's. Non-zero means the receive clock stepped backwards.
func (s *Sink) ClockRegressions() int { return s.w.ClockRegressions() }

// ClockRegressionSamples returns the first few rewinds with their magnitude, so a
// non-zero count can be told apart from a rewind that actually mattered.
func (s *Sink) ClockRegressionSamples() []ClockRegression {
	return s.w.ClockRegressionSamples()
}

// FramesContainingNewline returns how many frames carried a raw newline, which would
// break the line format. Should always be zero.
func (s *Sink) FramesContainingNewline() int { return s.w.FramesContainingNewline() }

// Bucket returns the UTC-hour bucket currently open, or "" if no frame arrived yet.
func (s *Sink) Bucket() string { return s.w.Bucket() }
