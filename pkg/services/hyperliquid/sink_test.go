package hyperliquid

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestSink(t *testing.T, channel string, queueSize int, flushEvery time.Duration) (*Sink, string) {
	t.Helper()
	root := t.TempDir()
	s, err := NewSink(SinkConfig{
		Channel:     channel,
		Root:        root,
		Compression: gzip.DefaultCompression,
		QueueSize:   queueSize,
		FlushEvery:  flushEvery,
		RunID:       "2026-10-04T140000Z",
	})
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s, root
}

// ─────────────────────────────────────────────────────────────
// The happy path
// ─────────────────────────────────────────────────────────────

func TestSinkWritesEveryFrameAndCountsThem(t *testing.T) {
	s, root := newTestSink(t, "bbo", 0, 0)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	frames := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, f := range frames {
		if !s.Enqueue(rx, f) {
			t.Fatalf("Enqueue(%q) was dropped", f)
		}
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := s.Frames(); got != 3 {
		t.Errorf("Frames = %d, want 3", got)
	}
	if got := s.Drops(); got != 0 {
		t.Errorf("Drops = %d, want 0", got)
	}
	if got := s.Bytes(); got != 11 {
		t.Errorf("Bytes = %d, want 11 (payload only: 3+3+5)", got)
	}
	if got := s.WriteErrors(); got != 0 {
		t.Errorf("WriteErrors = %d, want 0", got)
	}
	if got := s.AfterStop(); got != 0 {
		t.Errorf("AfterStop = %d, want 0", got)
	}

	lines := readLines(t, filepath.Join(root, "bbo", FileName("2026-10-04T14")))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3", len(lines))
	}
	for i, want := range []string{"one", "two", "three"} {
		if !strings.HasSuffix(lines[i], "\t"+want) {
			t.Errorf("line %d = %q, want it to end with %q", i, lines[i], want)
		}
	}
	// The hour is complete, so the .open name must be gone.
	assertNotExists(t, filepath.Join(root, "bbo", FileName("2026-10-04T14")+OpenSuffix))
}

func TestSinkStopFinishesTheFileAndIsIdempotent(t *testing.T) {
	s, root := newTestSink(t, "trades", 0, 0)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	if !s.Enqueue(rx, []byte("x")) {
		t.Fatal("Enqueue dropped")
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	final := filepath.Join(root, "trades", FileName("2026-10-04T14"))
	assertExists(t, final)
	assertNotExists(t, final+OpenSuffix)

	if err := s.Stop(); err != nil {
		t.Fatalf("second Stop = %v, want nil", err)
	}
	if got := s.FilesOpened(); got != 1 {
		t.Errorf("FilesOpened = %d, want 1", got)
	}
	if got := s.StaleFiles(); got != 0 {
		t.Errorf("StaleFiles = %d, want 0", got)
	}
	if got := s.ClockRegressions(); got != 0 {
		t.Errorf("ClockRegressions = %d, want 0", got)
	}
	if got := s.FramesContainingNewline(); got != 0 {
		t.Errorf("FramesContainingNewline = %d, want 0", got)
	}
}

// ─────────────────────────────────────────────────────────────
// Sentinels share the queue, so their order is frame order
// ─────────────────────────────────────────────────────────────

func TestSinkSentinelsAreInlineAndCountedApart(t *testing.T) {
	s, root := newTestSink(t, "bbo", 0, 0)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	sentinel := []byte(`{"channel":"_meta","event":"resubscribed"}`)

	s.Enqueue(rx, []byte("before"))
	s.EnqueueSentinel(rx, sentinel)
	s.Enqueue(rx, []byte("after"))
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := s.Frames(); got != 2 {
		t.Errorf("Frames = %d, want 2 (sentinels are not frames)", got)
	}
	if got := s.Sentinels(); got != 1 {
		t.Errorf("Sentinels = %d, want 1", got)
	}

	// Order is the whole point: a sentinel that lands after the frames it is meant to
	// delimit would mark the wrong part of the stream.
	lines := readLines(t, filepath.Join(root, "bbo", FileName("2026-10-04T14")))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3", len(lines))
	}
	if !strings.HasSuffix(lines[0], "\tbefore") {
		t.Errorf("line 0 = %q", lines[0])
	}
	if !strings.HasSuffix(lines[1], "\t"+string(sentinel)) {
		t.Errorf("line 1 = %q, want the sentinel", lines[1])
	}
	if !strings.HasSuffix(lines[2], "\tafter") {
		t.Errorf("line 2 = %q", lines[2])
	}
}

// ─────────────────────────────────────────────────────────────
// Backpressure
// ─────────────────────────────────────────────────────────────

func TestEnqueueDropsInsteadOfBlockingWhenTheQueueIsFull(t *testing.T) {
	// White-box: with no writer goroutine draining, a queue of 1 has to reject the
	// second item. This is the property the read-loop goroutine depends on — a
	// blocking enqueue would throttle frame reads and make the venue drop the socket.
	s := &Sink{
		name:  "bbo",
		items: make(chan item, 1),
		stop:  make(chan struct{}),
	}
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)

	if !s.Enqueue(rx, []byte("first")) {
		t.Fatal("first Enqueue must be accepted")
	}
	if s.Enqueue(rx, []byte("second")) {
		t.Fatal("second Enqueue must be dropped, not blocked")
	}
	if got, want := s.Frames(), int64(1); got != want {
		t.Errorf("Frames = %d, want %d", got, want)
	}
	if got, want := s.Drops(), int64(1); got != want {
		t.Errorf("Drops = %d, want %d", got, want)
	}
	// Every offered frame is either accepted or counted: no silent loss.
	if got := s.Frames() + s.Drops(); got != 2 {
		t.Errorf("accepted + dropped = %d, want 2", got)
	}
}

func TestDroppedSentinelsAreCountedSeparately(t *testing.T) {
	s := &Sink{
		name:  "bbo",
		items: make(chan item, 1),
		stop:  make(chan struct{}),
	}
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	s.Enqueue(rx, []byte("fills the queue"))
	if s.EnqueueSentinel(rx, []byte("sentinel")) {
		t.Fatal("sentinel should have been dropped")
	}
	if got := s.DroppedSentinels(); got != 1 {
		t.Errorf("DroppedSentinels = %d, want 1", got)
	}
	if got := s.Drops(); got != 0 {
		t.Errorf("Drops = %d, want 0 — a dropped sentinel is not a dropped frame", got)
	}
}

func TestBurstNeverLosesAFrameSilently(t *testing.T) {
	s, _ := newTestSink(t, "bbo", 64, 10*time.Millisecond)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)

	const burst = 5000
	for i := 0; i < burst; i++ {
		s.Enqueue(rx, []byte("frame"))
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Whether the drainer kept up or not, the ledger must balance.
	if got := s.Frames() + s.Drops(); got != burst {
		t.Fatalf("accepted + dropped = %d, want %d", got, burst)
	}
}

func TestEnqueueAfterStopIsCountedNotSilentlyLost(t *testing.T) {
	s, _ := newTestSink(t, "bbo", 0, 0)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.Enqueue(rx, []byte("too late")) {
		t.Fatal("Enqueue after Stop must be refused")
	}
	if got := s.AfterStop(); got != 1 {
		t.Errorf("AfterStop = %d, want 1", got)
	}
	if got := s.Drops(); got != 0 {
		t.Errorf("Drops = %d, want 0 — an after-stop enqueue is a shutdown-order bug, not a lost frame", got)
	}
}

// ─────────────────────────────────────────────────────────────
// Durability and failure
// ─────────────────────────────────────────────────────────────

func TestDroppedFramesLeaveAMarkInTheStream(t *testing.T) {
	// The manifest counts drops, but the consumer reads the stream and never opens the
	// manifest — so a hole in the book had no cause in the data at all. The mark has to be
	// written by the writer goroutine, because the queue being full is exactly the
	// condition under which a mark sent through that queue would be dropped too.
	s, root := newTestSink(t, "bbo", 4, 10*time.Millisecond)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)

	const burst = 400
	for i := 0; i < burst; i++ {
		s.Enqueue(rx, []byte("frame"))
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.Drops() == 0 {
		t.Fatal("a 400-frame burst cannot have fit in a 4-deep queue")
	}

	var markedCount int64
	var markedRun string
	for _, line := range readLines(t, singleHourFile(t, filepath.Join(root, "bbo"))) {
		if !strings.Contains(line, `"event":"`+EventDropped+`"`) {
			continue
		}
		tab := strings.IndexByte(line, '\t')
		if tab <= 0 {
			t.Fatalf("mark is not <rx_ns>\\t<json>: %q", line)
		}
		var body struct {
			Count int64  `json:"count"`
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal([]byte(line[tab+1:]), &body); err != nil {
			t.Fatalf("dropped mark is not JSON: %v (%s)", err, line)
		}
		if body.Count < markedCount {
			t.Errorf("count went backwards: %d after %d", body.Count, markedCount)
		}
		markedCount, markedRun = body.Count, body.RunID
	}
	if markedCount != s.Drops() {
		t.Errorf("the stream reports %d dropped frames, the sink counted %d — the hole and "+
			"the count must agree", markedCount, s.Drops())
	}
	if markedRun != "2026-10-04T140000Z" {
		t.Errorf("the mark's run_id = %q, want the sink's run", markedRun)
	}
}

func TestFlushTimerMakesTheCurrentHourReadableMidWrite(t *testing.T) {
	// The property: `kill -9` costs at most one flush interval, so the hour being
	// written is already on disk and gunzip-readable.
	s, root := newTestSink(t, "bbo", 0, 10*time.Millisecond)
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	s.Enqueue(rx, []byte("already durable"))

	open := filepath.Join(root, "bbo", FileName("2026-10-04T14")+OpenSuffix)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if lines, ok := tryReadLines(open); ok && len(lines) == 1 {
			return // flushed and readable without Stop
		}
		if time.Now().After(deadline) {
			t.Fatal("frame was never flushed to the .open file")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// tryReadLines reads a file that may not exist yet, or may be mid-append by the
// writer goroutine. It reports ok=false instead of failing, so a poll loop can
// simply retry; a half-written sync block is an expected transient here.
func tryReadLines(path string) ([]string, bool) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer func() { _ = f.Close() }()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, false
	}
	raw, err := io.ReadAll(zr)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, false
	}
	text := string(raw)
	if text == "" {
		return nil, true
	}
	if !strings.HasSuffix(text, "\n") {
		return nil, false
	}
	return strings.Split(strings.TrimSuffix(text, "\n"), "\n"), true
}

func TestWriteErrorIsReportedAndDoesNotPanic(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "bbo")
	if err := os.MkdirAll(dir, 0o500); err != nil { // no write permission
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) }) // let TempDir clean up

	if os.Geteuid() == 0 {
		t.Skip("running as root: directory permissions are not enforced")
	}
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	s, err := NewSink(SinkConfig{Channel: "bbo", Root: root, Compression: gzip.DefaultCompression})
	if err == nil {
		// The writer opens lazily on the first frame, so the failure arrives via
		// Enqueue/Stop rather than New. Either way it must surface, not panic.
		s.Enqueue(time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC), []byte("x"))
		if stopErr := s.Stop(); stopErr == nil {
			t.Fatal("expected a write failure to surface")
		}
		if s.Err() == nil {
			t.Fatal("Err() must report the write failure")
		}
		if got := s.WriteErrors(); got == 0 {
			t.Error("WriteErrors must be non-zero")
		}
		return
	}
	if !strings.Contains(err.Error(), "hyperliquid:") {
		t.Errorf("New error should be prefixed, got %v", err)
	}
}
