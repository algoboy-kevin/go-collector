package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// File names inside the capture root.
const (
	MetaFile     = "meta.json"
	PerpDexsFile = "perpDexs.json"
	ManifestFile = "manifest.json"
)

// InfoClient is the slice of the connector's /info client this package needs.
// *hl.InfoClient satisfies it; tests substitute a stub.
type InfoClient interface {
	MetaRaw(ctx context.Context) ([]byte, error)
	PerpDexsRaw(ctx context.Context) ([]byte, error)
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
	if opts.Attempts <= 0 {
		opts.Attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= opts.Attempts; attempt++ {
		body, err := get(ctx)
		if err == nil {
			if err := writeFileAtomic(filepath.Join(root, name), body, 0o644); err != nil {
				return fmt.Errorf("hyperliquid: write %s: %w", name, err)
			}
			return nil
		}
		lastErr = err
		if attempt == opts.Attempts {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("hyperliquid: fetch %s: %w", what, ctx.Err())
		case <-time.After(opts.Delay):
		}
	}
	return fmt.Errorf("hyperliquid: fetch %s after %d attempts: %w", what, opts.Attempts, lastErr)
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
	Venue            string `json:"venue"`
	Environment      string `json:"environment"`
	CollectorVersion string `json:"collector_version"`
	ConnectorVersion string `json:"connector_version"`

	StartedNS int64 `json:"started_ns"`
	// EndedNS is absent until the run has stopped cleanly.
	EndedNS int64 `json:"ended_ns,omitempty"`

	Coins    []string `json:"coins"`
	Channels []string `json:"channels"`

	// L2BookParams is omitted entirely when nothing was set, so a reader can tell
	// "we recorded the venue default" from "we asked for 5 levels".
	L2BookParams *L2BookParams `json:"l2book_params,omitempty"`

	Recording RecordingManifest `json:"recording"`

	// StopReason is "signal", "duration", "disk_low", "write_error" or "channel_stopped".
	StopReason string `json:"stop_reason,omitempty"`

	// Error is set when the run ended because something failed, so an unclean capture
	// says so in its own file.
	Error string `json:"error,omitempty"`

	// Channels_ is one entry per recorded channel, in configuration order.
	ChannelReports []ChannelReport `json:"channel_reports"`
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

	// LargestFrame is the biggest single frame seen, for comparison against the read
	// limit: a capture that approaches it is one burst away from a dropped connection.
	LargestFrame int64 `json:"largest_frame_bytes,omitempty"`

	// TradeSides tallies trades[].side values. The ingest must accept the venue's
	// encoding, and this records which one actually appeared.
	TradeSides map[string]int64 `json:"trade_sides,omitempty"`

	// LastFrameNS is when the last frame was accepted, so a reader can see whether a
	// quiet channel was quiet for its whole run.
	LastFrameNS int64 `json:"last_frame_ns,omitempty"`

	Error string `json:"error,omitempty"`
}

// WriteManifest writes {root}/manifest.json atomically.
func WriteManifest(root string, m Manifest) error {
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("hyperliquid: marshal manifest: %w", err)
	}
	body = append(body, '\n')
	return writeFileAtomic(filepath.Join(root, ManifestFile), body, 0o644)
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
func (c *Config) ManifestHeader(started time.Time, compression int) Manifest {
	m := Manifest{
		Venue:            "hyperliquid",
		Environment:      Environment(c.Connector.WSURL),
		CollectorVersion: CollectorVersion,
		ConnectorVersion: connector.Version,
		StartedNS:        started.UnixNano(),
		Coins:            append([]string(nil), c.Coins...),
		Recording: RecordingManifest{
			Compression:     compression,
			FlushIntervalMS: c.FlushEvery().Milliseconds(),
			QueueSize:       c.QueueSize(),
			ReadLimitBytes:  c.Recording.ReadLimitBytes,
			AppPingMS:       c.Recording.PingIntervalMS,
			WSPingMS:        c.Recording.WSPingIntervalMS,
			ReconnectMS:     c.Recording.ReconnectIntervalMS,
			MinFreeBytes:    c.MinFree(),
		},
	}
	if c.L2Book.Fast || c.L2Book.NSigFigs != 0 || c.L2Book.Mantissa != 0 {
		m.L2BookParams = &L2BookParams{
			Fast:     c.L2Book.Fast,
			NSigFigs: c.L2Book.NSigFigs,
			Mantissa: c.L2Book.Mantissa,
		}
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
