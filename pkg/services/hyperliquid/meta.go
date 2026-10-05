package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// File names inside the capture root.
const (
	MetaFile     = "meta.json"
	PerpDexsFile = "perpDexs.json"
	// ManifestFile is a copy of the NEWEST run's manifest, kept for convenience so an
	// existing reader that looks for manifest.json keeps working. It is not the record of
	// anything: the authoritative per-run records are in ManifestsDir, and a reader that
	// cares which run it is looking at must read one of those.
	ManifestFile = "manifest.json"
	// ManifestsDir holds one manifest per run, named <run_id>.json.
	//
	// A single manifest.json was written at startup and rewritten at shutdown, so the
	// second run on a capture destroyed the first run's record — its window, its counts,
	// and why it stopped — while the first run's frames survived in quarantine. The
	// provenance was lost in the one place that is supposed to hold it.
	ManifestsDir = "manifests"
)

// InfoClient is the slice of the connector's /info client this package needs.
// *hl.InfoClient satisfies it; tests substitute a stub.
type InfoClient interface {
	MetaRaw(ctx context.Context) ([]byte, error)
	PerpDexsRaw(ctx context.Context) ([]byte, error)
	// Post is the escape hatch for anything the typed wrappers do not cover — in
	// practice a HIP-3 dex's own universe, via {"type":"meta","dex":"<dex>"}.
	Post(ctx context.Context, payload any) ([]byte, error)
}

// MetaOptions configures the startup metadata dump.
type MetaOptions struct {
	// Attempts is how many times to try before giving up. Nothing is written until a
	// fetch succeeds, so a failed attempt leaves no partial file behind.
	Attempts int
	// Delay is the pause between attempts.
	Delay time.Duration
}

// DefaultMetaOptions is the shipped policy: a handful of tries, well under a second
// of total delay, because a failure here aborts the run.
func DefaultMetaOptions() MetaOptions {
	return MetaOptions{Attempts: 4, Delay: 500 * time.Millisecond}
}

// FetchMeta writes {root}/meta.json from the venue's {"type":"meta"} response.
//
// The body is stored **verbatim** — the connector has already checked it is JSON, but
// nothing parses it, here or anywhere. The ingest owns interpretation, and the
// per-asset szDecimals and universe order in this file are what let it build
// instruments at all: a catalog without them resolves no symbol and builds no engine.
//
// Because of that, failing here fails the run. Recording hours of frames into a
// capture with no instrument definitions produces an unusable catalog, and the
// problem would otherwise surface much later, when the data can no longer be refetched.
func FetchMeta(ctx context.Context, c InfoClient, root string, opts MetaOptions) error {
	return fetchRaw(ctx, c.MetaRaw, root, MetaFile, opts, "meta")
}

// FetchPerpDexs writes {root}/perpDexs.json. Only needed for HIP-3 markets: asset ids
// there are 100000 + dexIndex*10000 + indexInMeta, which cannot be derived from
// meta.json alone.
func FetchPerpDexs(ctx context.Context, c InfoClient, root string, opts MetaOptions) error {
	return fetchRaw(ctx, c.PerpDexsRaw, root, PerpDexsFile, opts, "perpDexs")
}

func fetchRaw(
	ctx context.Context,
	get func(context.Context) ([]byte, error),
	root, name string,
	opts MetaOptions,
	what string,
) error {
	body, err := fetchWithRetry(ctx, get, opts, what)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(root, name), body, 0o644); err != nil {
		return fmt.Errorf("hyperliquid: write %s: %w", name, err)
	}
	return nil
}

// fetchWithRetry runs get until it succeeds or the attempts run out, so every startup
// dump gets the same tolerance regardless of which endpoint it came from.
func fetchWithRetry(
	ctx context.Context,
	get func(context.Context) ([]byte, error),
	opts MetaOptions,
	what string,
) ([]byte, error) {
	if opts.Attempts <= 0 {
		opts.Attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= opts.Attempts; attempt++ {
		body, err := get(ctx)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if attempt == opts.Attempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("hyperliquid: fetch %s: %w", what, ctx.Err())
		case <-time.After(opts.Delay):
		}
	}
	return nil, fmt.Errorf("hyperliquid: fetch %s after %d attempts: %w", what, opts.Attempts, lastErr)
}

// ─────────────────────────────────────────────────────────────
// manifest.json
// ─────────────────────────────────────────────────────────────

// Manifest describes a capture from the inside, so a directory of gzip files can be
// read months later without guessing what produced it.
//
// It is written at startup and rewritten at shutdown. The rewrite is the point: the
// started copy carries the intent, the final copy carries what actually happened,
// including per-channel counts and why the run ended.
type Manifest struct {
	// RunID identifies this run: its start instant, UTC, to the second. It is the name of
	// this file (manifests/<run_id>.json) and the tag on every quarantine record, so any
	// artifact in the capture can be traced back to the run that produced it.
	RunID string `json:"run_id"`

	Venue            string `json:"venue"`
	Environment      string `json:"environment"`
	CollectorVersion string `json:"collector_version"`
	ConnectorVersion string `json:"connector_version"`

	StartedNS int64 `json:"started_ns"`
	// EndedNS is absent until the run has stopped cleanly.
	EndedNS int64 `json:"ended_ns,omitempty"`

	Coins    []CoinRef `json:"coins"`
	Channels []string  `json:"channels"`

	// CoinsSkipped names every configured coin that was NOT recorded, and why. A skip is a
	// decision, and a decision that is not written down is later indistinguishable from a
	// coin that was never configured.
	CoinsSkipped []CoinSkip `json:"coins_skipped,omitempty"`

	// UniverseFingerprint digests the coins, channels, l2Book thinning and meta bytes. A
	// client compares it against earlier runs to answer "is this the same experiment?".
	UniverseFingerprint string `json:"universe_fingerprint,omitempty"`

	// MetaSHA256 is the digest of each metadata dump, so a copy of a capture can be
	// verified by reading the copy rather than the original.
	MetaSHA256 map[string]string `json:"meta_sha256,omitempty"`

	// L2BookParams is omitted entirely when nothing was set, so a reader can tell
	// "we recorded the venue default" from "we asked for 5 levels".
	L2BookParams *L2BookParams `json:"l2book_params,omitempty"`

	// Hip3Dexes names each HIP-3 dex whose universe was dumped alongside meta.json.
	// A reader needs this to find the instrument definitions for any `dex:COIN` in
	// `coins`: meta.json holds the default dex only.
	Hip3Dexes []DexDump `json:"hip3_dexes,omitempty"`

	Recording RecordingManifest `json:"recording"`

	// StopReason is "signal", "duration", "disk_low", "write_error" or "channel_stopped".
	StopReason string `json:"stop_reason,omitempty"`

	// Error is set when the run ended because something failed, so an unclean capture
	// says so in its own file.
	Error string `json:"error,omitempty"`

	// Channels_ is one entry per recorded channel, in configuration order.
	ChannelReports []ChannelReport `json:"channel_reports"`
}

// l2Params renders the l2Book thinning as the manifest records it, or nil when nothing was
// set. Nil and an all-zero struct are not the same statement: one says "the venue's own
// default", the other "we asked for no thinning", and the fingerprint covers them
// distinctly too.
func l2Params(c *Config) *L2BookParams {
	if !c.L2Book.Fast && c.L2Book.NSigFigs == 0 && c.L2Book.Mantissa == 0 {
		return nil
	}
	return &L2BookParams{
		Fast:     c.L2Book.Fast,
		NSigFigs: c.L2Book.NSigFigs,
		Mantissa: c.L2Book.Mantissa,
	}
}

// Frames returns the total venue frames recorded across every channel, so two runs can
// be compared without walking channel_reports by hand.
func (m Manifest) Frames() int64 {
	var n int64
	for _, r := range m.ChannelReports {
		n += r.Frames
	}
	return n
}

// L2BookParams records the l2Book thinning that was requested.
type L2BookParams struct {
	Fast     bool `json:"fast"`
	NSigFigs int  `json:"nSigFigs,omitempty"`
	Mantissa int  `json:"mantissa,omitempty"`
}

// RecordingManifest records the write-path settings, so the compressed volume in the
// capture can be compared against the settings that produced it.
type RecordingManifest struct {
	Compression      int   `json:"compression_level"`
	FlushIntervalMS  int64 `json:"flush_interval_ms"`
	QueueSize        int   `json:"queue_size"`
	ReadLimitBytes   int64 `json:"read_limit_bytes,omitempty"`
	AppPingMS        int64 `json:"app_ping_interval_ms,omitempty"`
	WSPingMS         int64 `json:"ws_ping_interval_ms,omitempty"`
	ReconnectMS      int64 `json:"reconnect_interval_ms,omitempty"`
	MinFreeBytes     int64 `json:"min_free_bytes"`
	FreeBytesAtStart int64 `json:"free_bytes_at_start,omitempty"`
	FreeBytesAtEnd   int64 `json:"free_bytes_at_end,omitempty"`
}

// ChannelReport is one channel's accounting.
//
// Frames + Drops is everything offered, so a gap in the file is always explained by a
// number in this struct. A capture whose Drops, DroppedSentinels, WriteErrors,
// ClockRegressions or StaleFiles are non-zero is visibly not clean — which is the
// point: it must not be able to *look* clean.
type ChannelReport struct {
	Channel string `json:"channel"`

	// Subscriptions is how many subscribe frames were sent; Acks is how many the venue
	// acknowledged. Acks < Subscriptions is the one direct signal that a subscription
	// was refused, and it arrives within a second of startup.
	Subscriptions int   `json:"subscriptions"`
	Acks          int64 `json:"acks"`

	// Reconnects counts successful dials after the first. A healthy channel stays at 1
	// for the whole run; a large number here means the connection kept being dropped and
	// remade, which is worth seeing even when the capture looks otherwise fine.
	Reconnects int64 `json:"reconnects"`

	// ConnectAttempts counts dials that SUCCEEDED, which is to say Reconnects+1.
	//
	// It is deliberately not "dials attempted": the connector exposes no callback for a
	// dial that fails, so a failed dial is invisible here and a run where every dial failed
	// reports zero attempts. The number is kept under its own name because it is what a
	// reader will look for, and because it becomes honest the moment the connector reports
	// failures; until then the guard against a dial-failure loop is behavioural (see
	// checkFlap).
	ConnectAttempts int64 `json:"connect_attempts"`

	Frames    int64 `json:"frames"`
	Sentinels int64 `json:"sentinels"`
	Drops     int64 `json:"drops"`
	// DroppedSentinels counts missing gap markers, which is worse than a missing
	// frame: an unmarked hole cannot be detected downstream.
	DroppedSentinels int64 `json:"dropped_sentinels"`
	AfterStop        int64 `json:"after_stop,omitempty"`
	WriteErrors      int64 `json:"write_errors"`
	Bytes            int64 `json:"bytes"`

	FilesOpened       int `json:"files_opened"`
	StaleFiles        int `json:"stale_files,omitempty"`
	ReplacedHours     int `json:"replaced_hours,omitempty"`
	ClockRegressions  int `json:"clock_regressions,omitempty"`
	FramesWithNewline int `json:"frames_with_newline,omitempty"`

	// ClockRegressionSamples carries the magnitude of the first few rewinds, capped at 32.
	// ClockRegressions stays the exact count; this is the evidence for whether the step was
	// a millisecond blip or a genuine rewind, which the count alone cannot distinguish.
	ClockRegressionSamples []ClockRegression `json:"clock_regression_samples,omitempty"`

	// Files describes every completed hour this channel wrote: name, line count, byte
	// size, the receive-time window it covers, and its sha256. `files_opened: 3` says how
	// many there are; this says which, whether they are empty, and whether the copy in
	// front of you is the one that was written.
	Files []FileRecord `json:"files,omitempty"`

	// Quarantined lists files this run preserved instead of overwriting, each naming its
	// own hour, where it went, and the range it covers. It replaces a bare
	// `replaced_hours: 1`, which said an hour was displaced but not which file, where it
	// went, or what it contained.
	Quarantined []QuarantineRecord `json:"quarantined,omitempty"`

	// LargestFrame is the biggest single frame seen, for comparison against the read
	// limit: a capture that approaches it is one burst away from a dropped connection.
	LargestFrame int64 `json:"largest_frame_bytes,omitempty"`

	// TradeSides tallies trades[].side values. The ingest must accept the venue's
	// encoding, and this records which one actually appeared.
	TradeSides map[string]int64 `json:"trade_sides,omitempty"`

	// LastFrameNS is when the last frame was accepted, so a reader can see whether a
	// quiet channel was quiet for its whole run.
	LastFrameNS int64 `json:"last_frame_ns,omitempty"`

	// FirstFrameNS is when the first frame was accepted. With LastFrameNS it brackets the
	// window this channel actually observed, which otherwise has to be recovered by
	// opening files and reading their first and last lines.
	FirstFrameNS int64 `json:"first_frame_ns,omitempty"`

	// MaxGapMS is the largest interval between two consecutive frames, measured from the
	// monotonic clock so a wall-clock step cannot invent or hide a stall. On a paced feed
	// (`l2Book` is ~1.8 frames/s/coin) it is what distinguishes a pause from a dead
	// channel, which the frame counts alone cannot show. Zero means fewer than two frames
	// arrived, so no interval exists.
	MaxGapMS int64 `json:"max_gap_ms,omitempty"`

	// LastDisconnect explains the most recent unexpected drop in the venue's own words
	// — the WebSocket close status and the peer's reason, as the connector reported
	// them, e.g. `failed to read: status = policy violation and reason = "Cannot open
	// more than 15 connections."`. Empty when nothing dropped.
	//
	// It exists because Reconnects alone is a number without a cause: a capture that
	// flapped can say *why* here, next to the count, instead of leaving the reader to
	// infer it. The same text is in the `disconnected` sentinel of that channel's file.
	LastDisconnect string `json:"last_disconnect,omitempty"`

	Error string `json:"error,omitempty"`
}

// WriteManifest writes the run's record to {root}/manifests/<run_id>.json, atomically,
// and refreshes {root}/manifest.json as a copy of it.
//
// The per-run file is written twice — once at startup, so a run that is killed still says
// what it was trying to do, and once at shutdown with the outcome — and never touched by
// any other run. That is the whole point: `manifest.json` alone meant the second run
// erased the first run's explanation for the hole its frames left behind.
func WriteManifest(root string, m Manifest) error {
	if strings.TrimSpace(m.RunID) == "" {
		return errors.New("hyperliquid: manifest has no run_id")
	}
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("hyperliquid: marshal manifest: %w", err)
	}
	body = append(body, '\n')

	dir := filepath.Join(root, ManifestsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("hyperliquid: create %s: %w", dir, err)
	}
	if err := writeFileAtomic(filepath.Join(dir, m.RunID+".json"), body, 0o644); err != nil {
		return err
	}
	// Best effort, and deliberately last: the convenience copy is not worth failing a
	// shutdown over once the real record is on disk.
	if err := writeFileAtomic(filepath.Join(root, ManifestFile), body, 0o644); err != nil {
		slog.Warn("hyperliquid: manifest.json convenience copy not refreshed", "err", err)
	}
	return nil
}

// ManifestPath returns the authoritative path for a run's manifest.
func ManifestPath(root, runID string) string {
	return filepath.Join(root, ManifestsDir, runID+".json")
}

// uniqueRunID returns base, or base with a "-2"/"-3" suffix when a manifest in this root
// already claims base.
//
// Second resolution is not sufficient on its own. "One recorder per root" — enforced by
// deploy-hl.sh — rules out CONCURRENT runs, but not a sequential restart: a run that dies
// at startup and is restarted inside the same second would otherwise write over the
// previous run's manifest, which is exactly the loss the per-run file exists to prevent.
// A disambiguated id stays a single identity: it names the manifest, tags the quarantined
// files and stamps every sentinel of that run.
func uniqueRunID(root, base string) string {
	free := func(id string) bool {
		_, err := os.Stat(ManifestPath(root, id))
		return errors.Is(err, os.ErrNotExist)
	}
	if free(base) {
		return base
	}
	// Bounded: a directory with thousands of runs in one second is not a case worth
	// scanning for, and falling back to nanoseconds keeps the id unique anyway.
	for n := 2; n < 100; n++ {
		candidate := fmt.Sprintf("%s-%d", base, n)
		if free(candidate) {
			return candidate
		}
	}
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// ReadManifests returns every run record in a capture root, sorted by run id (which is
// to say, chronologically). A capture with none returns an empty slice and no error.
func ReadManifests(root string) ([]Manifest, error) {
	entries, err := os.ReadDir(filepath.Join(root, ManifestsDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("hyperliquid: read %s: %w", ManifestsDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	// Read in a deterministic order, then order the results by what the runs actually say.
	sort.Strings(names)

	out := make([]Manifest, 0, len(names))
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(root, ManifestsDir, name))
		if err != nil {
			return nil, fmt.Errorf("hyperliquid: read %s: %w", name, err)
		}
		var m Manifest
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, fmt.Errorf("hyperliquid: parse %s: %w", name, err)
		}
		out = append(out, m)
	}
	sortManifests(out)
	return out, nil
}

// sortManifests orders runs chronologically by their recorded start, falling back to the
// run id.
//
// Deliberately NOT by file name. A run id is normally its start instant, so name order is
// time order — but a same-second restart is disambiguated as `<id>-2`, and `-` sorts
// before `.`, so `2026-10-05T084829Z-2.json` precedes `2026-10-05T084829Z.json` in a
// listing. That would silently reverse the two, and "the newest run" is read off the end
// of this slice.
func sortManifests(runs []Manifest) {
	sort.SliceStable(runs, func(i, j int) bool {
		if runs[i].StartedNS != runs[j].StartedNS {
			return runs[i].StartedNS < runs[j].StartedNS
		}
		return runs[i].RunID < runs[j].RunID
	})
}

// Environment names the venue environment for a websocket URL, so a capture records
// which network produced it.
func Environment(wsURL string) string {
	if strings.Contains(strings.ToLower(wsURL), "testnet") {
		return "testnet"
	}
	return "mainnet"
}

// CollectorVersion is stamped into every manifest. Bumped by hand; the point is that a
// capture can name the code that wrote it.
const CollectorVersion = "0.1.0"

// ManifestHeader fills in the fields that describe the run rather than its outcome.
func (c *Config) ManifestHeader(started time.Time, runID string, compression int) Manifest {
	m := Manifest{
		RunID:            runID,
		Venue:            "hyperliquid",
		Environment:      Environment(c.Connector.WSURL),
		CollectorVersion: CollectorVersion,
		ConnectorVersion: connector.Version,
		StartedNS:        started.UnixNano(),
		Coins:            CoinRefsFor(c.Coins),
		Recording: RecordingManifest{
			Compression:     compression,
			FlushIntervalMS: c.FlushEvery().Milliseconds(),
			QueueSize:       c.QueueSize(),
			// The EFFECTIVE values, not the raw keys: a manifest that reported 0 for an
			// unset read_limit would not say what was actually in force, which is the only
			// thing a reader needs in order to reproduce or trust the capture.
			ReadLimitBytes: c.ReadLimit(),
			AppPingMS:      c.AppPingIntervalMS(),
			WSPingMS:       c.Recording.WSPingIntervalMS,
			ReconnectMS:    c.Recording.ReconnectIntervalMS,
			MinFreeBytes:   c.MinFree(),
		},
	}
	if l2 := l2Params(c); l2 != nil {
		m.L2BookParams = l2
	}
	for _, name := range c.Channels {
		m.Channels = append(m.Channels, strings.TrimSpace(name))
	}
	return m
}

// writeFileAtomic writes data to a temporary file in the same directory and renames it
// into place, so a crash mid-write cannot leave a truncated JSON file that parses as
// valid but incomplete.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
