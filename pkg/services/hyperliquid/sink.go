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
	sentinels        atomic.Int64
	dropped          atomic.Int64
	droppedSentinels atomic.Int64
	afterStop        atomic.Int64
	writeErrors      atomic.Int64
	bytes            atomic.Int64
	lastAccepted     atomic.Int64 // unix nanos
	lastDropWarn     atomic.Int64 // unix nanos

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
	})
	if err != nil {
		return nil, err
	}

	s := &Sink{
		name:       cfg.Channel,
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
		}
		s.lastAccepted.Store(it.rx.UnixNano())
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
			s.flush()
		case <-s.stop:
			// Drain before finishing: frames already accepted must reach the file.
			s.drain()
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

// LastFrame returns the receive time of the most recently accepted frame, or the
// zero time if none has been accepted. This is the input to quiet-channel detection.
func (s *Sink) LastFrame() time.Time {
	ns := s.lastAccepted.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

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

// ClockRegressions returns how many frames arrived for an hour earlier than the open
// file's. Non-zero means the receive clock stepped backwards.
func (s *Sink) ClockRegressions() int { return s.w.ClockRegressions() }

// FramesContainingNewline returns how many frames carried a raw newline, which would
// break the line format. Should always be zero.
func (s *Sink) FramesContainingNewline() int { return s.w.FramesContainingNewline() }

// Bucket returns the UTC-hour bucket currently open, or "" if no frame arrived yet.
func (s *Sink) Bucket() string { return s.w.Bucket() }
