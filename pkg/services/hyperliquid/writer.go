// Package hyperliquid implements the raw Hyperliquid recorder: it archives every
// WebSocket frame the venue sends, byte for byte, and interprets nothing.
//
// The design rule is that a capture is irreplaceable. A parser bug, a venue schema
// change, or a field this code has never heard of must not be able to lose data, so
// frames are written as `<rx_ns>\t<frame>\n` and never decoded. Everything in this
// package exists to hold that line format still under crashes, rotation and
// backpressure.
//
// File layout, one file per channel per UTC hour:
//
//	{root}/{channel}/{YYYY-MM-DDTHH}.jsonl.gz
//
// The wire and file contract is RECORDER_SPEC.md; the implementation rationale is
// PERP_COLLECTOR_SPEC.md.
//
// NOTE ON PACKAGE NAME: this package is called `hyperliquid` because it sits in
// pkg/services/hyperliquid, matching pkg/services/collector. It is unrelated to the
// connector's pkg/hyperliquid, which this package consumes (aliased where imported).
package hyperliquid

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The on-disk contract. Changing any of these breaks the ingest side.
const (
	// BucketLayout is the UTC-hour bucket format: one file per channel per hour.
	BucketLayout = "2006-01-02T15"
	// BucketSuffix is the final (readable, complete) file extension.
	BucketSuffix = ".jsonl.gz"
	// OpenSuffix marks a file still being written. The rename to the final name is
	// what tells the converter "this hour is complete" — see Close.
	OpenSuffix = ".open"
	// TruncatedMarker appears in the name of an `.open` file left behind by an earlier
	// process, when a new run needs the same bucket name.
	TruncatedMarker = ".open.truncated-"

	// ReplacedMarker appears in the name of a *completed* hour that a new run would
	// otherwise have renamed over.
	ReplacedMarker = ".replaced-"

	// QuarantineDir is the capture subdirectory holding files the recorder refused to
	// overwrite. It is deliberately NOT part of the time series: nothing under it is a
	// current hour, and a reader that walks the capture must exclude it by name.
	//
	// It exists because "preserved" used to mean "left in the channel directory under a
	// name that does not look like data" — which made the artifact invisible to a
	// consumer while looking like data to a human reading `ls`. Naming it by its own hour
	// and tagging it with the run that displaced it makes both the range and the cause
	// recoverable.
	QuarantineDir = "quarantine"

	defaultBufSize  = 64 << 10 // 64 KiB, per PERP_COLLECTOR_SPEC.md §6.3
	defaultFileMode = 0o644
)

// Errors that callers are expected to distinguish.
var (
	// ErrWriterClosed is returned by Write and Flush after Close.
	ErrWriterClosed = errors.New("hyperliquid: writer is closed")
	// ErrZeroTime is returned by Write when rx is the zero time. rx is the recorder's
	// only clock, so a zero value is a caller bug, not a frame to file under the year 1.
	ErrZeroTime = errors.New("hyperliquid: zero receive time")
	// ErrNoCompression is returned by NewHourlyWriter for compression level 0. This
	// recorder exists to conserve disk, so storing frames uncompressed is never
	// intended; a zero here is a config bug and is refused rather than silently
	// reinterpreted as any other level.
	ErrNoCompression = errors.New("hyperliquid: compression level 0 (uncompressed) is not allowed")
)

// BucketName returns the UTC-hour bucket for a receive time, e.g. "2026-10-04T14".
//
// Rotation follows the *local receive clock*, never a venue timestamp, so a venue
// clock skew cannot scatter a capture across files. rx is therefore the only clock
// this package has, and it is injected per frame rather than read from the system —
// which is also what makes rotation testable without waiting for an hour to pass.
func BucketName(rx time.Time) string {
	return rx.UTC().Format(BucketLayout)
}

// FileName returns the final file name for a bucket, e.g. "2026-10-04T14.jsonl.gz".
func FileName(bucket string) string {
	return bucket + BucketSuffix
}

// WriterConfig configures one channel's writer.
type WriterConfig struct {
	// Root is the capture root, e.g. "data/hyperliquid". Files are written under
	// Root/<Channel>/. Required.
	Root string

	// Channel is the directory name, e.g. "l2Book". Required, and must be a single
	// safe path component: it comes from config, but it becomes a path.
	Channel string

	// Compression is the gzip level. Exactly one of 1 (best speed), 6 (default) or 9
	// (best) in practice; 0 is rejected. See PERP_COLLECTOR_SPEC.md §6.2 — the default
	// here is deliberately the middle of the trade, not gzip.BestSpeed.
	Compression int

	// BufSize is the bufio buffer between flate and the file. Zero selects 64 KiB.
	BufSize int

	// FileMode is the permission for newly created files. Zero selects 0644.
	FileMode os.FileMode

	// RunID tags any file this writer preserves, so a quarantined hour names the run that
	// displaced it. Empty in tests that do not care.
	RunID string
}

// QuarantineKind distinguishes why a file was preserved.
const (
	// QuarantinedReplaced is a *complete* hour that a new run would have renamed over.
	QuarantinedReplaced = "replaced"
	// QuarantinedTruncated is an `.open` file from a process that died inside the hour.
	QuarantinedTruncated = "truncated"
)

// maxClockRegressionSamples bounds the recorded samples. The COUNT is always exact; the
// samples are evidence, and a pathological clock could produce millions of them, which
// would turn a diagnostic into the largest structure in the process.
const maxClockRegressionSamples = 32

// ClockRegression is one frame whose receive time was earlier than the hour of the file
// that was open. The count says it happened; the magnitude says whether it mattered.
type ClockRegression struct {
	AtNS int64 `json:"at_ns"`
	// DeltaNS is how far the frame fell short of the open file's hour start, so it is
	// negative: -50ms is a blip, -40s is a rewind worth investigating.
	DeltaNS int64 `json:"delta_ns"`
}

// QuarantineRecord describes one preserved file.
//
// The point of the record — rather than the bare count this replaced — is that a hole in
// the timeline can be attributed: which hour, which file holds the surviving bytes, and
// what range they cover. A count says an hour was displaced but not where it went.
type QuarantineRecord struct {
	Kind string `json:"kind"`
	// Hour is the bucket the preserved file covers, e.g. "2026-10-05T08". It is the name
	// the file earned when it was written, NOT the instant it was displaced.
	Hour string `json:"hour"`
	// PreservedAs is the path relative to the capture root.
	PreservedAs string `json:"preserved_as"`
	// Lines, FirstNS and LastNS describe the surviving bytes. They are read back from the
	// quarantined file once, at quarantine time: without them the reader has to guess
	// whether the orphan covers seconds or most of an hour.
	Lines   int   `json:"lines,omitempty"`
	FirstNS int64 `json:"first_ns,omitempty"`
	LastNS  int64 `json:"last_ns,omitempty"`
}

// FileRecord describes one completed hour file, so the manifest can answer "which hours
// exist, are they empty, and are they the bytes I have" without opening any of them.
type FileRecord struct {
	Name    string `json:"name"`
	Lines   int    `json:"lines"`
	Bytes   int64  `json:"bytes"`
	FirstNS int64  `json:"first_ns,omitempty"`
	LastNS  int64  `json:"last_ns,omitempty"`
	// SHA256 is the digest of the file's bytes as written. The capture is irreplaceable and
	// lives on one box; without this, verifying a copy means reading both the original and
	// the copy.
	SHA256 string `json:"sha256,omitempty"`
}

// HourlyWriter appends raw frames to one gzip file per UTC hour.
//
// Write path, three tiers — the layering order matters:
//
//	bufio.Writer (BufSize) > gzip.Writer (Compression) > *os.File
//
// so a per-frame Write is CPU-only (no syscall), a flush emits one write, and a
// close emits the gzip trailer and performs the rename.
//
// A writer is not safe for concurrent use: the recorder gives each channel exactly
// one goroutine that owns it.
type HourlyWriter struct {
	cfg WriterConfig
	dir string
	// quarantineDir holds files this writer refuses to overwrite. One per channel, so a
	// quarantined `trades` file can never be confused with a quarantined `l2Book` one.
	quarantineDir string

	bucket string // current bucket, "" when no file is open
	file   *os.File
	buf    *bufio.Writer
	gz     *gzip.Writer

	closed bool

	frames           int
	bytes            int64
	filesOpened      int
	staleFiles       int
	replacedHours    int
	newlineFrames    int
	clockRegressions int
	// clockRegressionSamples holds the first few rewinds with their magnitude.
	clockRegressionSamples []ClockRegression

	// Per-file accounting, reset on every rotate and turned into a FileRecord on close.
	curLines   int
	curFirstNS int64
	curLastNS  int64

	files      []FileRecord
	quarantine []QuarantineRecord
}

// NewHourlyWriter creates a writer for one channel and ensures its directory exists.
func NewHourlyWriter(cfg WriterConfig) (*HourlyWriter, error) {
	if strings.TrimSpace(cfg.Root) == "" {
		return nil, errors.New("hyperliquid: writer root is required")
	}
	if err := checkPathComponent(cfg.Channel); err != nil {
		return nil, err
	}
	if cfg.Compression == gzip.NoCompression {
		return nil, ErrNoCompression
	}
	if cfg.Compression < gzip.HuffmanOnly || cfg.Compression > gzip.BestCompression {
		return nil, fmt.Errorf("hyperliquid: invalid compression level %d", cfg.Compression)
	}
	if cfg.BufSize <= 0 {
		cfg.BufSize = defaultBufSize
	}
	if cfg.FileMode == 0 {
		cfg.FileMode = defaultFileMode
	}

	dir := filepath.Join(cfg.Root, cfg.Channel)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("hyperliquid: create %s: %w", dir, err)
	}
	// Created up front so quarantining can never be the thing that fails: preserving an
	// existing file must not depend on a directory that is only needed when it happens.
	qdir := filepath.Join(cfg.Root, QuarantineDir, cfg.Channel)
	if err := os.MkdirAll(qdir, 0o755); err != nil {
		return nil, fmt.Errorf("hyperliquid: create %s: %w", qdir, err)
	}
	return &HourlyWriter{cfg: cfg, dir: dir, quarantineDir: qdir}, nil
}

// Files returns a record of every completed hour file, oldest first. Safe to read once
// the writer is closed; before that it covers only the files already rotated out.
func (w *HourlyWriter) Files() []FileRecord { return append([]FileRecord(nil), w.files...) }

// Quarantined returns a record of every file preserved rather than overwritten.
func (w *HourlyWriter) Quarantined() []QuarantineRecord {
	return append([]QuarantineRecord(nil), w.quarantine...)
}

// Dir returns the directory this writer writes into.
func (w *HourlyWriter) Dir() string { return w.dir }

// QuarantineDir returns the directory preserved files are moved into.
func (w *HourlyWriter) QuarantineDir() string { return w.quarantineDir }

// CurrentName returns the final name of the hour file being written, or "" if none is
// open. It is used to say *which* file a write error happened on, in the error mark
// written into the stream.
func (w *HourlyWriter) CurrentName() string {
	if w.bucket == "" {
		return ""
	}
	return FileName(w.bucket)
}

// Bucket returns the current UTC-hour bucket, or "" before the first frame.
func (w *HourlyWriter) Bucket() string { return w.bucket }

// Frames returns the number of frames written since the writer was created.
func (w *HourlyWriter) Frames() int { return w.frames }

// Bytes returns the number of *payload* bytes written (excluding the rx_ns prefix,
// the tab and the newline), so it matches the raw frame volume the venue sent.
func (w *HourlyWriter) Bytes() int64 { return w.bytes }

// FilesOpened returns how many hour files the writer has opened: 1 for the first,
// plus 1 per hour rollover.
func (w *HourlyWriter) FilesOpened() int { return w.filesOpened }

// ClockRegressions returns how many frames arrived for an hour *earlier* than the
// open file's. Non-zero means the receive clock stepped backwards; the frames were
// written into the current file rather than risking an overwrite, so their rx_ns
// prefixes will be outside the file's hour. Rare, and worth reporting rather than
// hiding.
func (w *HourlyWriter) ClockRegressions() int { return w.clockRegressions }

// ClockRegressionSamples returns the first few rewinds with their magnitude. The count
// from ClockRegressions is exact even when more than maxClockRegressionSamples happened.
func (w *HourlyWriter) ClockRegressionSamples() []ClockRegression {
	return append([]ClockRegression(nil), w.clockRegressionSamples...)
}

// StaleFiles returns how many `.open` files left behind by an earlier process this
// writer had to move aside. Non-zero means a previous run died mid-hour.
func (w *HourlyWriter) StaleFiles() int { return w.staleFiles }

// ReplacedHours returns how many completed hours this writer preserved rather than
// overwrote. Non-zero means a new run started inside an hour a previous run had
// already finished, so that hour's bytes are on disk under a `.replaced-` name.
func (w *HourlyWriter) ReplacedHours() int { return w.replacedHours }

// FramesContainingNewline returns how many frames contained a raw newline byte.
//
// This is a format-integrity tripwire, not a filter: the line format is
// `<rx_ns>\t<frame>\n`, so a frame carrying a literal newline would end the line
// early and quietly break every line after it. The venue sends compact single-line
// JSON, so this should always be zero — and because the recorder never modifies a
// frame, the only honest response to a non-zero count is to report it.
func (w *HourlyWriter) FramesContainingNewline() int { return w.newlineFrames }

// Write appends one received frame, rotating the output file if rx falls in a new
// UTC hour.
//
// The frame is written verbatim. Nothing about it is inspected, trimmed, re-encoded
// or validated — only scanned for a newline, and only to count (see
// FramesContainingNewline).
func (w *HourlyWriter) Write(rx time.Time, frame []byte) error {
	if w.closed {
		return ErrWriterClosed
	}
	if rx.IsZero() {
		return ErrZeroTime
	}

	bucket := BucketName(rx)
	if bucket != w.bucket {
		switch {
		case w.bucket == "":
			// First frame: open this hour's file.
			if err := w.rotate(rx, bucket); err != nil {
				return err
			}
		case bucket > w.bucket:
			// BucketLayout is zero-padded and big-endian, so string order is time
			// order; a greater bucket means the hour advanced.
			if err := w.rotate(rx, bucket); err != nil {
				return err
			}
		default:
			// The receive clock stepped *backwards* across an hour boundary. Rotating
			// here would reopen a bucket that has already been renamed to its final
			// name, and the rename at close would overwrite a completed hour — silent,
			// permanent loss of exactly the thing this package exists to protect. So
			// write into the current file instead: the frame survives, its rx_ns prefix
			// still records the true receive time, and the anomaly is counted.
			w.clockRegressions++
			if len(w.clockRegressionSamples) < maxClockRegressionSamples {
				var delta int64
				// Measured against the START of the hour the open file covers, so the number
				// reads as "the clock was this far behind where the file already was".
				if start, perr := time.ParseInLocation(BucketLayout, w.bucket, time.UTC); perr == nil {
					delta = rx.UnixNano() - start.UnixNano()
				}
				w.clockRegressionSamples = append(w.clockRegressionSamples, ClockRegression{
					AtNS:    rx.UnixNano(),
					DeltaNS: delta,
				})
			}
		}
	}

	// Both writes are into flate's internal buffer, not the file: no syscall, and the
	// header buffer is on the stack, so a frame costs no allocation.
	var hdr [24]byte
	prefix := strconv.AppendInt(hdr[:0], rx.UnixNano(), 10)
	if _, err := w.gz.Write(prefix); err != nil {
		return err
	}
	if _, err := w.gz.Write(tabByte); err != nil {
		return err
	}
	if _, err := w.gz.Write(frame); err != nil {
		return err
	}
	if _, err := w.gz.Write(newlineByte); err != nil {
		return err
	}

	w.frames++
	w.bytes += int64(len(frame))
	if bytes.IndexByte(frame, '\n') >= 0 {
		w.newlineFrames++
	}

	// Per-file accounting for the manifest's files[]. Every line counts, sentinels
	// included: they are bytes in the file like any other, and a reader comparing line
	// counts must not be surprised by them.
	w.curLines++
	if w.curFirstNS == 0 {
		w.curFirstNS = rx.UnixNano()
	}
	w.curLastNS = rx.UnixNano()
	return nil
}

var (
	tabByte     = []byte{'\t'}
	newlineByte = []byte{'\n'}
)

// Flush pushes buffered frames out to the file without closing it.
//
// Order is load-bearing: gz.Flush emits a byte-aligned sync block, then buf.Flush
// pushes it through one write syscall. Reversing them would leave a partial block in
// the gzip buffer and the file would appear frozen.
func (w *HourlyWriter) Flush() error {
	if w.closed {
		return ErrWriterClosed
	}
	if w.gz == nil {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return err
	}
	return w.buf.Flush()
}

// Close finishes the current file: gzip trailer, buffer, fsync, close, then the
// `.open` → final rename that marks the hour complete. It is idempotent.
//
// If any step fails the rename is skipped, so the file stays named `.open` and the
// converter is told exactly what happened. That is the whole point of the suffix.
func (w *HourlyWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.closeCurrent()
}

// rotate closes the current file (if any) and opens the new bucket.
func (w *HourlyWriter) rotate(rx time.Time, bucket string) error {
	if w.file != nil {
		if err := w.closeCurrent(); err != nil {
			return err
		}
	}

	final := filepath.Join(w.dir, FileName(bucket))
	tmp := final + OpenSuffix

	// A leftover `.open` means a previous process died inside this same hour. Its
	// bytes are the only record of that time, and opening with O_TRUNC would destroy
	// them, so move it aside instead: the partial hour survives, the new `.open` is
	// clean, and the name says why. Appending would be wrong — a crash always leaves
	// an unterminated gzip member, so a second member behind it would corrupt the
	// first.
	if _, err := os.Lstat(tmp); err == nil {
		if err := w.quarantineFile(tmp, bucket, QuarantinedTruncated, w.cfg.RunID); err != nil {
			return err
		}
		w.staleFiles++
	}

	// A *complete* hour already at the destination: a previous run finished this hour,
	// and this one has started inside the same hour. Renaming at close would replace
	// those bytes with this run's, silently destroying them — the same failure as a
	// backwards clock step, reached by a different route. Preserve and record instead.
	if _, err := os.Lstat(final); err == nil {
		if err := w.quarantineFile(final, bucket, QuarantinedReplaced, w.cfg.RunID); err != nil {
			return err
		}
		w.replacedHours++
	}

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, w.cfg.FileMode)
	if err != nil {
		return fmt.Errorf("hyperliquid: open %s: %w", tmp, err)
	}
	buf := bufio.NewWriterSize(f, w.cfg.BufSize)
	gz, err := gzip.NewWriterLevel(buf, w.cfg.Compression)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("hyperliquid: gzip writer for %s: %w", tmp, err)
	}

	w.bucket = bucket
	w.file = f
	w.buf = buf
	w.gz = gz
	w.filesOpened++
	w.curLines, w.curFirstNS, w.curLastNS = 0, 0, 0
	return nil
}

// quarantineFile moves a file out of the channel directory into quarantine, naming it by
// the hour it covers and the run that displaced it, and records what it contains.
//
// The name it gets encodes the OLD file's hour on purpose. The previous scheme appended
// the instant of the move — which reads as information about the displaced file but is
// actually the first receive time of the file that replaced it. The thing a reader needs
// when reconciling a hole is the orphan's own range, so that is what the record carries.
func (w *HourlyWriter) quarantineFile(src, hour, kind, runID string) error {
	if runID == "" {
		runID = "unknown"
	}
	name := hour + "." + kind + "-" + runID + BucketSuffix
	dst := filepath.Join(w.quarantineDir, name)

	// Record before the move is attempted: if the rename then fails the caller aborts the
	// hour, and a manifest that claims a file which was not actually preserved would be
	// worse than one that omits it.
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("hyperliquid: quarantine %s: %w", src, err)
	}

	rec := QuarantineRecord{
		Kind:        kind,
		Hour:        hour,
		PreservedAs: filepath.Join(QuarantineDir, w.cfg.Channel, name),
	}
	// Best effort: the file is preserved either way, and a truncated gzip member is
	// exactly the case this tolerates. Failing the run here would lose the very data the
	// quarantine exists to keep.
	lines, first, last, err := inspectGzip(dst)
	if err != nil {
		slog.Warn("hyperliquid: quarantine contents not measured",
			"file", dst, "err", err)
	} else {
		rec.Lines, rec.FirstNS, rec.LastNS = lines, first, last
	}
	w.quarantine = append(w.quarantine, rec)
	return nil
}

// inspectGzip reports how many lines a file holds and the first and last receive times.
//
// It tolerates a truncated member, because that is what a crashed `.open` file is, and
// the whole point is to describe it rather than to refuse it.
func inspectGzip(path string) (lines int, firstNS, lastNS int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = f.Close() }()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = zr.Close() }()

	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if i := bytes.IndexByte(line, '\t'); i > 0 {
			if ns, convErr := strconv.ParseInt(string(line[:i]), 10, 64); convErr == nil {
				if firstNS == 0 {
					firstNS = ns
				}
				lastNS = ns
			}
		}
		lines++
	}
	// A truncated member surfaces here as an unexpected EOF. The lines already counted
	// are real, so only a genuine read failure is reported.
	if err := sc.Err(); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return lines, firstNS, lastNS, err
	}
	return lines, firstNS, lastNS, nil
}

// closeCurrent finishes and renames the open file, if there is one.
func (w *HourlyWriter) closeCurrent() error {
	if w.file == nil {
		return nil
	}

	// Every step is attempted before returning, so a failing fsync cannot leak the
	// file handle.
	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	keep(w.gz.Close()) // writes the CRC trailer; the file is complete after this
	keep(w.buf.Flush())
	keep(w.file.Sync())
	keep(w.file.Close())

	if firstErr == nil {
		final := filepath.Join(w.dir, FileName(w.bucket))
		tmp := final + OpenSuffix
		if err := os.Rename(tmp, final); err != nil {
			firstErr = fmt.Errorf("hyperliquid: rename %s: %w", tmp, err)
		} else {
			rec := FileRecord{
				Name:    FileName(w.bucket),
				Lines:   w.curLines,
				FirstNS: w.curFirstNS,
				LastNS:  w.curLastNS,
			}
			if info, statErr := os.Stat(final); statErr == nil {
				rec.Bytes = info.Size()
			}
			// Hashed after the rename, so the digest describes the finished, readable file
			// — the one a copy is compared against. Best effort: a capture is not made
			// less valid by a missing checksum, and failing the close here would be a
			// worse trade than an absent field.
			if sum, hashErr := hashFile(final); hashErr != nil {
				slog.Warn("hyperliquid: could not hash closed hour", "file", final, "err", hashErr)
			} else {
				rec.SHA256 = sum
			}
			w.files = append(w.files, rec)
		}
	}

	w.file, w.buf, w.gz = nil, nil, nil
	w.bucket = ""
	return firstErr
}

// hashFile returns the hex sha256 of a file's bytes. The manifest records it so a copy
// can be verified by reading the copy alone.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// checkPathComponent rejects anything that would let a config value escape its
// directory once it is joined into a path.
func checkPathComponent(name string) error {
	switch {
	case name == "":
		return errors.New("hyperliquid: channel name is empty")
	case name == "." || name == "..":
		return fmt.Errorf("hyperliquid: channel name %q is not a directory name", name)
	case strings.Contains(name, ".."):
		return fmt.Errorf("hyperliquid: channel name %q contains %q", name, "..")
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("hyperliquid: channel name %q contains a path separator", name)
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("hyperliquid: channel name %q contains a NUL", name)
	}
	return nil
}
