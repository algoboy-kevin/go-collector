package hyperliquid

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newTestWriter returns a writer over a temp root, plus that root.
func newTestWriter(t *testing.T, channel string) (*HourlyWriter, string) {
	t.Helper()
	root := t.TempDir()
	w, err := NewHourlyWriter(WriterConfig{
		Root:        root,
		Channel:     channel,
		Compression: gzip.DefaultCompression,
	})
	if err != nil {
		t.Fatalf("NewHourlyWriter: %v", err)
	}
	return w, root
}

// readLines decompresses path and returns its lines.
//
// A truncated gzip member is tolerated, because that is exactly what a crashed
// `.open` file is: gz.Flush() emits a sync block inside the member but only Close
// writes the trailer, so a crash leaves readable frames and a missing trailer. The
// ingest side needs the same tolerance, which is the point of the `.open` suffix.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader(%s): %v", path, err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("read %s: %v", path, err)
	}

	text := string(raw)
	if text == "" {
		return nil
	}
	if !strings.HasSuffix(text, "\n") {
		t.Fatalf("%s: content is not newline-terminated: %q", path, text)
	}
	return strings.Split(strings.TrimSuffix(text, "\n"), "\n")
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", filepath.Base(path), err)
	}
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("expected %s not to exist", filepath.Base(path))
	}
}

// ─────────────────────────────────────────────────────────────
// The line format — the whole contract
// ─────────────────────────────────────────────────────────────

func TestLineIsRxNsTabFrameNewline(t *testing.T) {
	w, root := newTestWriter(t, "bbo")
	rx := time.Date(2026, 10, 4, 14, 3, 5, 123456789, time.UTC)
	frames := [][]byte{
		[]byte(`{"channel":"bbo","data":{"coin":"BTC"}}`),
		[]byte(`{"channel":"bbo","data":{"coin":"ETH"}}`),
	}
	for _, f := range frames {
		if err := w.Write(rx, f); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(root, "bbo", FileName("2026-10-04T14"))
	lines := readLines(t, path)
	if len(lines) != len(frames) {
		t.Fatalf("got %d lines, want %d", len(lines), len(frames))
	}

	prefix := strconv.FormatInt(rx.UnixNano(), 10) + "\t"
	for i, line := range lines {
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("line %d: want prefix %q, got %q", i, prefix, line)
		}
		if got := line[len(prefix):]; got != string(frames[i]) {
			t.Fatalf("line %d: frame was not written verbatim:\n got %q\nwant %q", i, got, frames[i])
		}
	}

	// rx_ns must be the receive time in *nanoseconds*; a millisecond value here would
	// silently destroy the ingest-latency measurement.
	if got, want := prefix, "1791122585123456789\t"; got != want {
		t.Fatalf("rx_ns prefix = %q, want %q", got, want)
	}
}

func TestFrameBytesAreUnmodified(t *testing.T) {
	// A frame containing everything that could tempt a "helpful" rewrite.
	frame := []byte(`{"a":  "double  space","b":"\u00e9","c":"tab\there","d":"}","e":"{\"x\":1}"}`)
	w, root := newTestWriter(t, "trades")
	rx := time.Date(2026, 10, 4, 14, 0, 0, 1, time.UTC)
	if err := w.Write(rx, frame); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lines := readLines(t, filepath.Join(root, "trades", FileName("2026-10-04T14")))
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	if got := lines[0][strings.IndexByte(lines[0], '\t')+1:]; got != string(frame) {
		t.Fatalf("frame altered:\n got %q\nwant %q", got, frame)
	}
}

// ─────────────────────────────────────────────────────────────
// Rotation
// ─────────────────────────────────────────────────────────────

func TestRotationFollowsRxNotTheWallClock(t *testing.T) {
	w, root := newTestWriter(t, "bbo")

	// One nanosecond apart, straddling the hour boundary. Real time does not move at
	// all during this test, so any rotation can only have come from rx.
	before := time.Date(2026, 10, 4, 13, 59, 59, 999999999, time.UTC)
	after := before.Add(time.Nanosecond)
	if err := w.Write(before, []byte("A")); err != nil {
		t.Fatalf("Write before: %v", err)
	}
	if err := w.Write(after, []byte("B")); err != nil {
		t.Fatalf("Write after: %v", err)
	}

	dir := filepath.Join(root, "bbo")
	first := filepath.Join(dir, FileName("2026-10-04T13"))
	second := filepath.Join(dir, FileName("2026-10-04T14"))

	// Rolling over completes the previous hour immediately, without waiting for Close.
	assertExists(t, first)
	assertNotExists(t, first+OpenSuffix)
	assertExists(t, second+OpenSuffix)
	assertNotExists(t, second)

	if got := w.FilesOpened(); got != 2 {
		t.Fatalf("FilesOpened = %d, want 2", got)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertExists(t, second)
	assertNotExists(t, second+OpenSuffix)

	if lines := readLines(t, first); len(lines) != 1 || !strings.HasSuffix(lines[0], "\tA") {
		t.Fatalf("first hour should hold only A, got %q", lines)
	}
	if lines := readLines(t, second); len(lines) != 1 || !strings.HasSuffix(lines[0], "\tB") {
		t.Fatalf("second hour should hold only B, got %q", lines)
	}
}

func TestBucketNameIsUTC(t *testing.T) {
	// 23:30 local in a zone far from UTC must still bucket by UTC hour.
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	at := time.Date(2026, 10, 4, 23, 30, 0, 0, ny) // 03:30Z on the 5th
	if got, want := BucketName(at), "2026-10-05T03"; got != want {
		t.Fatalf("BucketName = %q, want %q", got, want)
	}
	if got, want := FileName("2026-10-05T03"), "2026-10-05T03.jsonl.gz"; got != want {
		t.Fatalf("FileName = %q, want %q", got, want)
	}
}

func TestClockRegressionNeverOverwritesAFinishedHour(t *testing.T) {
	w, root := newTestWriter(t, "bbo")
	dir := filepath.Join(root, "bbo")

	// A complete hour from an earlier run, already renamed to its final name.
	finished := filepath.Join(dir, FileName("2026-10-04T13"))
	const earlierRun = "a completed hour from an earlier run"
	if err := os.WriteFile(finished, []byte(earlierRun), 0o644); err != nil {
		t.Fatalf("seed finished hour: %v", err)
	}

	hour14 := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	hour13 := time.Date(2026, 10, 4, 13, 30, 0, 0, time.UTC) // earlier hour, later frame

	if err := w.Write(hour14, []byte("A")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Write(hour13, []byte("B")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if got := w.ClockRegressions(); got != 1 {
		t.Errorf("ClockRegressions = %d, want 1", got)
	}
	// The count alone cannot tell a millisecond blip from a real rewind, so the magnitude
	// is recorded: hour13's frame is 30 minutes before hour14's start, so it falls short by
	// exactly that much and the delta is negative.
	samples := w.ClockRegressionSamples()
	if len(samples) != 1 {
		t.Fatalf("samples = %+v, want 1", samples)
	}
	if want := int64(-30 * time.Minute); samples[0].DeltaNS != want {
		t.Errorf("DeltaNS = %d, want %d (negative: the clock was behind the open file)",
			samples[0].DeltaNS, want)
	}
	if samples[0].AtNS != hour13.UnixNano() {
		t.Errorf("AtNS = %d, want the frame's own receive time %d", samples[0].AtNS, hour13.UnixNano())
	}
	if got := w.FilesOpened(); got != 1 {
		t.Errorf("FilesOpened = %d, want 1 — must not reopen an earlier bucket", got)
	}
	assertNotExists(t, finished+OpenSuffix)

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The pre-existing hour is byte-for-byte intact. Rotating back would have renamed
	// the new file over it and destroyed it.
	got, err := os.ReadFile(finished)
	if err != nil {
		t.Fatalf("finished hour was destroyed: %v", err)
	}
	if string(got) != earlierRun {
		t.Fatalf("finished hour was overwritten: %q", got)
	}

	// Both frames survive in the open hour, and the regressed one keeps its true rx_ns
	// rather than being relabelled to the file's hour.
	lines := readLines(t, filepath.Join(dir, FileName("2026-10-04T14")))
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if !strings.HasPrefix(lines[1], strconv.FormatInt(hour13.UnixNano(), 10)+"\t") {
		t.Fatalf("regressed frame lost its receive time: %q", lines[1])
	}
}

// ─────────────────────────────────────────────────────────────
// The crash contract
// ─────────────────────────────────────────────────────────────

func TestOpenSuffixPersistsUntilClose(t *testing.T) {
	w, root := newTestWriter(t, "l2Book")
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	if err := w.Write(rx, []byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	final := filepath.Join(root, "l2Book", FileName("2026-10-04T14"))

	// Flushed but not closed: not readable as a complete hour, yet the bytes are
	// already on disk (this is the "kill -9 costs at most one second" guarantee).
	assertNotExists(t, final)
	assertExists(t, final+OpenSuffix)
	if lines := readLines(t, final+OpenSuffix); len(lines) != 1 {
		t.Fatalf("flushed .open should hold 1 line, got %q", lines)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertExists(t, final)
	assertNotExists(t, final+OpenSuffix)
}

func TestStaleOpenFileIsPreservedNotTruncated(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "bbo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// A previous process died mid-hour and left its `.open` behind.
	final := filepath.Join(dir, FileName("2026-10-04T14"))
	if err := os.WriteFile(final+OpenSuffix, []byte("pre-crash bytes"), 0o644); err != nil {
		t.Fatalf("seed stale file: %v", err)
	}

	rx := time.Date(2026, 10, 4, 14, 30, 0, 0, time.UTC)
	w, err := NewHourlyWriter(WriterConfig{
		Root: root, Channel: "bbo", Compression: gzip.DefaultCompression,
		RunID: "2026-10-04T143000Z",
	})
	if err != nil {
		t.Fatalf("NewHourlyWriter: %v", err)
	}
	if err := w.Write(rx, []byte("X")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := w.StaleFiles(); got != 1 {
		t.Fatalf("StaleFiles = %d, want 1", got)
	}

	// The irreplaceable bytes still exist — but under quarantine/, not in the channel
	// directory, because a preserved file sitting among real hour files reads as data to
	// anyone doing `ls` while being invisible to anything that globs the channel.
	aside := filepath.Join(root, QuarantineDir, "bbo", "2026-10-04T14."+QuarantinedTruncated+"-2026-10-04T143000Z"+BucketSuffix)
	got, err := os.ReadFile(aside)
	if err != nil {
		t.Fatalf("stale .open bytes were destroyed: %v", err)
	}
	if string(got) != "pre-crash bytes" {
		t.Fatalf("stale bytes altered: %q", got)
	}
	// The stale bytes must exist in exactly one place. The channel directory keeps only
	// this run's own hour file, so nothing listed among the real hours is a leftover.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read channel dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != FileName("2026-10-04T14") {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("channel dir holds %v, want only the current hour file — a leftover here "+
			"reads as real data to anyone listing it", names)
	}

	// The record is what makes the orphan findable: which hour, where it went, and that
	// it was a truncation rather than a displacement.
	recs := w.Quarantined()
	if len(recs) != 1 {
		t.Fatalf("quarantine records = %d, want 1", len(recs))
	}
	if recs[0].Kind != QuarantinedTruncated || recs[0].Hour != "2026-10-04T14" {
		t.Errorf("record = %+v, want kind=truncated hour=2026-10-04T14", recs[0])
	}
	if want := filepath.Join(QuarantineDir, "bbo", filepath.Base(aside)); recs[0].PreservedAs != want {
		t.Errorf("PreservedAs = %q, want %q", recs[0].PreservedAs, want)
	}

	// And the file that *is* claimed to be complete contains only this run's frame.
	if lines := readLines(t, filepath.Join(dir, FileName("2026-10-04T14"))); len(lines) != 1 || !strings.HasSuffix(lines[0], "\tX") {
		t.Fatalf("new hour should hold only X, got %q", lines)
	}
}

// ─────────────────────────────────────────────────────────────
// Counters and tripwires
// ─────────────────────────────────────────────────────────────

func TestCountersCountFramesAndPayloadBytesOnly(t *testing.T) {
	w, _ := newTestWriter(t, "bbo")
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	for _, f := range [][]byte{[]byte("abc"), []byte("defgh")} {
		if err := w.Write(rx, f); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if got := w.Frames(); got != 2 {
		t.Fatalf("Frames = %d, want 2", got)
	}
	// 3 + 5 payload bytes: the rx_ns prefix, the tab and the newline are framing, not
	// capture volume, so they must not inflate the number that predicts disk usage.
	if got := w.Bytes(); got != 8 {
		t.Fatalf("Bytes = %d, want 8", got)
	}
	if got := w.FilesOpened(); got != 1 {
		t.Fatalf("FilesOpened = %d, want 1", got)
	}
}

func TestNewlineInFrameIsCountedNeverFiltered(t *testing.T) {
	w, root := newTestWriter(t, "bbo")
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	// A frame with a raw newline would end the line early and break every line after
	// it. The recorder must not "fix" it — it counts it and reports it.
	frame := []byte("{\"a\":1}\n{\"b\":2}")
	if err := w.Write(rx, frame); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := w.FramesContainingNewline(); got != 1 {
		t.Fatalf("FramesContainingNewline = %d, want 1", got)
	}
	lines := readLines(t, filepath.Join(root, "bbo", FileName("2026-10-04T14")))
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 — the frame must be written verbatim", len(lines))
	}
	if !strings.HasSuffix(lines[0], "{\"a\":1}") {
		t.Fatalf("line 0 = %q", lines[0])
	}
}

// ─────────────────────────────────────────────────────────────
// Failure modes
// ─────────────────────────────────────────────────────────────

func TestNewHourlyWriterValidatesConfig(t *testing.T) {
	root := t.TempDir()
	cases := []struct {
		name string
		cfg  WriterConfig
	}{
		{"empty root", WriterConfig{Channel: "bbo", Compression: gzip.DefaultCompression}},
		{"blank root", WriterConfig{Root: "   ", Channel: "bbo", Compression: gzip.DefaultCompression}},
		{"empty channel", WriterConfig{Root: root, Compression: gzip.DefaultCompression}},
		{"dot channel", WriterConfig{Root: root, Channel: ".", Compression: gzip.DefaultCompression}},
		{"dotdot channel", WriterConfig{Root: root, Channel: "..", Compression: gzip.DefaultCompression}},
		{"traversal channel", WriterConfig{Root: root, Channel: "../../etc", Compression: gzip.DefaultCompression}},
		{"slash channel", WriterConfig{Root: root, Channel: "a/b", Compression: gzip.DefaultCompression}},
		{"backslash channel", WriterConfig{Root: root, Channel: `a\b`, Compression: gzip.DefaultCompression}},
		{"level too high", WriterConfig{Root: root, Channel: "bbo", Compression: 10}},
		{"level too low", WriterConfig{Root: root, Channel: "bbo", Compression: -3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewHourlyWriter(tc.cfg); err == nil {
				t.Fatalf("NewHourlyWriter(%+v) = nil error, want an error", tc.cfg)
			}
		})
	}
}

func TestUncompressedLevelIsRefused(t *testing.T) {
	// 0 is gzip.NoCompression. It would silently store frames at ~1:1 and fill a disk,
	// so it is refused rather than reinterpreted as "unset, use the default".
	_, err := NewHourlyWriter(WriterConfig{Root: t.TempDir(), Channel: "bbo"})
	if !errors.Is(err, ErrNoCompression) {
		t.Fatalf("err = %v, want ErrNoCompression", err)
	}
}

func TestZeroReceiveTimeIsRejected(t *testing.T) {
	w, root := newTestWriter(t, "bbo")
	if err := w.Write(time.Time{}, []byte("x")); !errors.Is(err, ErrZeroTime) {
		t.Fatalf("err = %v, want ErrZeroTime", err)
	}
	// A rejected frame must not leave an empty hour file behind.
	if got := w.Frames(); got != 0 {
		t.Fatalf("Frames = %d, want 0", got)
	}
	if got := w.FilesOpened(); got != 0 {
		t.Fatalf("FilesOpened = %d, want 0", got)
	}
	if entries, err := os.ReadDir(filepath.Join(root, "bbo")); err == nil && len(entries) != 0 {
		t.Fatalf("expected no files, got %v", entries)
	}
}

func TestWriteAndFlushAfterCloseFail(t *testing.T) {
	w, _ := newTestWriter(t, "bbo")
	rx := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	if err := w.Write(rx, []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Write(rx, []byte("y")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Write after Close = %v, want ErrWriterClosed", err)
	}
	if err := w.Flush(); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Flush after Close = %v, want ErrWriterClosed", err)
	}
	// Close is idempotent — the shutdown path may call it after a failed write.
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

func TestCloseWithoutFramesWritesNothing(t *testing.T) {
	w, root := newTestWriter(t, "bbo")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "bbo"))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	// A channel that never received a frame must not create an hour file: an empty
	// `2026-10-04T14.jsonl.gz` would look like a recorded hour with no data.
	if len(entries) != 0 {
		t.Fatalf("expected no files, got %v", entries)
	}
}

func TestFlushWithNoFileIsNoop(t *testing.T) {
	w, _ := newTestWriter(t, "bbo")
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush before any frame = %v, want nil", err)
	}
}
