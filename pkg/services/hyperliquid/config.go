package hyperliquid

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	"gopkg.in/yaml.v3"
)

// Defaults for the recording knobs. They are applied when the corresponding key is
// absent, so a minimal config file still produces a well-formed recorder.
const (
	DefaultDataDir      = "data/hyperliquid"
	DefaultMinFreeBytes = 524288000 // 500 MiB

	// DefaultReadLimitBytes caps a single WebSocket message at 16 MiB.
	//
	// The number that matters is the measured worst case, not a probe. A 2h capture on
	// five mid-cap coins produced a single 126,590-byte `trades` frame, against 9,003
	// bytes in the 60s probe that set the original 1 MiB default — a 14x growth in three
	// hours of run time, on the channel PERP_COLLECTOR_SPEC.md itself flagged as the
	// bursty one. At 1 MiB that is 8.3x of headroom, not the 116x the spec claimed, and one
	// oversized frame kills the connection.
	//
	// 16 MiB is ~132x the worst frame seen and costs nothing but buffer: with the
	// connector's read-limit failure handled as a per-connection reconnect, too high is
	// cheap and too low is a hole in the capture. Treat
	// `channel_reports[].largest_frame_bytes` as something to RE-MEASURE on every long
	// run — the 14x growth is the evidence that this is not a settled venue property.
	DefaultReadLimitBytes = 16 << 20

	// DefaultAppPingMS is the application-level {"method":"ping"} interval.
	//
	// 25s, not the connector's 50s default. The venue closes an idle socket at ~60s, and
	// the 2h capture measured the 50s interval as *exactly* 50.0s (72 pongs in an hour):
	// 10s of margin against a 60s timer is not margin, it is one delayed write away from
	// an avoidable reconnect. The WS-level control ping is not documented to reset the
	// venue's application idle timer, so it does not make the longer interval safe.
	DefaultAppPingMS = 25000
)

// Config is the YAML configuration for the hlrecorder binary.
//
// Optional channel and coin parameters are validated by the connector's own
// BuildSubscriptions, not restated here: the library already rejects an unknown
// channel, a duplicate coin, a coin on `allMids`, a bad candle interval,
// `nSigFigs` outside 2..5 and `mantissa` without `nSigFigs=5`. Re-checking those
// would create a second source of truth that can drift from the venue.
type Config struct {
	// DataDir is the capture root. Files land in DataDir/<channel>/<UTC hour>.jsonl.gz.
	DataDir string `yaml:"data_dir"`

	Coins []string `yaml:"coins"`

	// DelistedPolicy decides what a configured coin that the venue lists but no longer
	// trades does: "skip" (leave it out and record why), "record" (subscribe anyway) or
	// "fail" (refuse to start). Empty selects "skip".
	//
	// A policy rather than a hard rule because both answers are legitimate — `skip` is
	// right for a live capture, `record` is right for a study of the venue's silence on a
	// dead market — and because the decision is recorded in the manifest either way.
	DelistedPolicy string `yaml:"delisted_policy"`

	// OnUniverseChange decides what happens when the capture root already holds a session
	// with a DIFFERENT instrument set: "fail" (default) or "allow".
	//
	// It defaults to failing because a consumer derives one instrument set, and one price
	// precision per coin, from a whole directory. Two universes in one directory does not
	// raise an error — it produces a capture that looks complete and backtests at wrong
	// prices. The remedy is a sibling root, which is a one-line change at deploy time.
	OnUniverseChange string `yaml:"on_universe_change"`

	// Channels is deliberately narrower than the venue's subscribable set: `candle`
	// is subscribable but must not be recorded (RECORDER_SPEC.md §1 — bars are
	// useless for market making and waste disk).
	Channels []string `yaml:"channels"`

	L2Book    L2BookConfig    `yaml:"l2Book"`
	Connector ConnectorConfig `yaml:"connector"`
	Recording RecordingConfig `yaml:"recording"`
}

// L2BookConfig carries the optional l2Book thinning parameters.
//
// These are plain values, not pointers: 0 is not a legal venue value for any of them
// (nSigFigs is 2..5, mantissa is 1/2/5), so "unset" and "zero" cannot collide. False
// is likewise identical on the wire to an absent `fast`.
type L2BookConfig struct {
	// Fast requests 5 book levels instead of 20 and roughly halves l2Book bytes.
	Fast bool `yaml:"fast"`
	// NSigFigs thins the book to N significant figures. Leave unset: it rounds prices
	// as well as levels, so it destroys raw precision the capture cannot get back.
	NSigFigs int `yaml:"nSigFigs"`
	// Mantissa is only legal together with NSigFigs=5.
	Mantissa int `yaml:"mantissa"`
}

// ConnectorConfig points the recorder at the venue.
type ConnectorConfig struct {
	WSURL          string `yaml:"ws_url"`
	InfoURL        string `yaml:"info_url"`
	RecordPerpDexs bool   `yaml:"record_perp_dexs"`
}

// RecordingConfig tunes the write path and the connection.
type RecordingConfig struct {
	// Compression is one of default | best_speed | best.
	Compression string `yaml:"compression"`

	FlushIntervalMS int64 `yaml:"flush_interval_ms"`

	// PingIntervalMS is the application-level {"method":"ping"} interval. The
	// connector's convention, which this follows deliberately rather than inventing
	// its own: a negative value disables the ping, and 0 selects the recorder's default
	// (DefaultAppPingMS, 25s) rather than the library's 50s.
	//
	// 0 does NOT mean "disabled": an idle socket with no keepalive is exactly what the
	// venue closes at ~60s. The default was lowered from the library's 50s because 10s of
	// margin on a 60s timer is not margin — see DefaultAppPingMS.
	PingIntervalMS int64 `yaml:"ping_interval_ms"`

	// WSPingIntervalMS is the WebSocket control-ping interval, whose pong watchdog
	// (2x this) is what catches a half-open socket. 0 selects the library default of
	// 20s; negative disables it.
	WSPingIntervalMS int64 `yaml:"ws_ping_interval_ms"`

	// QueueSize bounds the per-channel pending-frame queue.
	QueueSize int `yaml:"queue_size"`

	// ReadLimitBytes caps a single WebSocket message. 0 selects
	// DefaultReadLimitBytes (16 MiB).
	//
	// Do not lower this without re-measuring: an oversized frame is not a dropped frame,
	// it is a killed connection, and the venue's worst case grew 14x in three hours of
	// run time. See DefaultReadLimitBytes.
	ReadLimitBytes int64 `yaml:"read_limit_bytes"`

	// MinFreeBytes is the free-space floor for DataDir. Below it the recorder stops
	// cleanly, so the hour in progress is finished and renamed rather than left
	// truncated.
	//
	// A pointer, unlike every other knob here, because BOTH values are meaningful:
	// 0 disables the guard, while an absent key means "use the default". With a plain
	// int64 the safe default would be indistinguishable from "off".
	MinFreeBytes *int64 `yaml:"min_free_bytes"`

	// ReconnectIntervalMS is the base reconnect delay; it backs off to the library's
	// 30s ceiling. 0 selects the library default of 500ms.
	//
	// There is deliberately no max-interval key: the connector pins that to the
	// websocket default and exposes no override, so a key here would be a no-op.
	ReconnectIntervalMS int64 `yaml:"reconnect_interval_ms"`
}

// Load reads and strictly decodes a config file. An unknown key is an error, so a
// typo cannot silently do nothing (the TestShippedConfigsDecode precedent).
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("hyperliquid: decode %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("hyperliquid: %s: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks the config as a whole, reporting every problem it finds rather
// than only the first.
func (c *Config) Validate() error {
	var errs []error

	if strings.TrimSpace(c.DataDir) == "" {
		c.DataDir = DefaultDataDir
	}
	if c.Connector.WSURL == "" {
		c.Connector.WSURL = hl.DefaultWSURL
	}
	if c.Connector.InfoURL == "" {
		c.Connector.InfoURL = hl.DefaultInfoURL
	}

	// Coins: normalized and injection-checked by the connector's validator, which is
	// the authoritative one because the coin ends up inside a JSON frame.
	seen := map[string]bool{}
	for i, raw := range c.Coins {
		coin, err := hl.NormalizeCoin(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("coins[%d]: %w", i, err))
			continue
		}
		if seen[coin] {
			errs = append(errs, fmt.Errorf("coins[%d]: duplicate coin %q", i, coin))
			continue
		}
		seen[coin] = true
		c.Coins[i] = coin
	}
	if len(c.Coins) == 0 {
		errs = append(errs, errors.New("coins: at least one coin is required"))
	}

	switch c.DelistedPolicyOrDefault() {
	case DelistedSkip, DelistedRecord, DelistedFail:
	default:
		errs = append(errs, fmt.Errorf("delisted_policy: %q is not one of %s, %s, %s",
			c.DelistedPolicy, DelistedSkip, DelistedRecord, DelistedFail))
	}
	switch c.OnUniverseChangeOrDefault() {
	case UniverseChangeFail, UniverseChangeAllow:
	default:
		errs = append(errs, fmt.Errorf("on_universe_change: %q is not one of %s, %s",
			c.OnUniverseChange, UniverseChangeFail, UniverseChangeAllow))
	}

	// Channels.
	chans, channelsErr := c.channels()
	if channelsErr != nil {
		errs = append(errs, channelsErr)
	} else if len(chans) == 0 {
		errs = append(errs, errors.New("channels: at least one channel is required"))
	}

	// l2Book params are only meaningful when l2Book is recorded. Checked only when
	// something is actually set, so leaving the block in the shipped config while
	// removing l2Book from `channels` is caught, but an absent block is not.
	if c.L2Book.Fast || c.L2Book.NSigFigs != 0 || c.L2Book.Mantissa != 0 {
		recordsL2 := false
		for _, ch := range chans {
			if ch == hl.ChannelL2Book {
				recordsL2 = true
			}
		}
		if !recordsL2 {
			errs = append(errs, errors.New("l2Book: parameters set but l2Book is not in `channels`"))
		}
	}

	// Compression.
	if _, err := c.CompressionLevel(); err != nil {
		errs = append(errs, err)
	}

	// Flush interval: a zero interval would spin the ticker. Zero means "use the
	// default", which FlushEvery resolves.
	if ms := c.Recording.FlushIntervalMS; ms != 0 && (ms < 0 || ms > 60_000) {
		errs = append(errs, fmt.Errorf("recording.flush_interval_ms: got %d, want 1..60000", ms))
	}

	if c.Recording.QueueSize < 0 {
		errs = append(errs, fmt.Errorf("recording.queue_size: got %d, want >= 0", c.Recording.QueueSize))
	}
	if c.Recording.ReadLimitBytes < 0 {
		errs = append(errs, fmt.Errorf("recording.read_limit_bytes: got %d, want >= 0", c.Recording.ReadLimitBytes))
	}
	if c.Recording.ReconnectIntervalMS < 0 {
		errs = append(errs, fmt.Errorf("recording.reconnect_interval_ms: got %d, want >= 0", c.Recording.ReconnectIntervalMS))
	}
	if c.Recording.MinFreeBytes != nil && *c.Recording.MinFreeBytes < 0 {
		errs = append(errs, fmt.Errorf("recording.min_free_bytes: got %d, want >= 0 (0 disables the guard)",
			*c.Recording.MinFreeBytes))
	}

	// Finally, let the connector validate the subscriptions themselves. This is the
	// check that matters most — it is the same code that will build the frames.
	if _, err := c.Plan(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// recordedChannels is the set this recorder will record, deliberately narrower than
// the venue's subscribable set.
var recordedChannels = []hl.Channel{
	hl.ChannelL2Book,
	hl.ChannelTrades,
	hl.ChannelBBO,
	hl.ChannelActiveAssetCtx,
	hl.ChannelAllMids,
}

// RecordedChannels returns the channels this recorder is willing to record.
func RecordedChannels() []hl.Channel {
	out := make([]hl.Channel, len(recordedChannels))
	copy(out, recordedChannels)
	return out
}

// channels resolves the configured channel names, rejecting duplicates and anything
// outside recordedChannels.
func (c *Config) channels() ([]hl.Channel, error) {
	var errs []error
	var out []hl.Channel
	seen := map[hl.Channel]bool{}

	for i, name := range c.Channels {
		trimmed := strings.TrimSpace(name)
		ch := hl.Channel(trimmed)
		if !isRecorded(ch) {
			errs = append(errs, fmt.Errorf("channels[%d]: %q is not a recordable channel (recordable: %s)",
				i, name, channelNames(recordedChannels)))
			continue
		}
		if seen[ch] {
			errs = append(errs, fmt.Errorf("channels[%d]: duplicate channel %q", i, name))
			continue
		}
		seen[ch] = true
		out = append(out, ch)
	}
	return out, errors.Join(errs...)
}

func isRecorded(ch hl.Channel) bool {
	for _, allowed := range recordedChannels {
		if ch == allowed {
			return true
		}
	}
	return false
}

func channelNames(chs []hl.Channel) string {
	names := make([]string, len(chs))
	for i, ch := range chs {
		names[i] = string(ch)
	}
	return strings.Join(names, ", ")
}

// ChannelPlan is one connection's worth of work: everything to subscribe on a single
// socket dedicated to one channel.
type ChannelPlan struct {
	Channel hl.Channel

	// Subscriptions is one entry per coin, or exactly one for the coin-less allMids.
	Subscriptions []hl.Subscription

	// Frames is the subscription frames, built by the connector. Precomputed so a
	// malformed frame is impossible at dial time.
	Frames [][]byte
}

// Plan builds and validates one connection plan per configured channel.
//
// This is where the config becomes subscriptions: the connector's BuildSubscriptions
// applies the channel, coin and parameter rules, and SubscribeFrame builds the exact
// bytes that go on the wire. A config that cannot produce a valid frame cannot start.
func (c *Config) Plan() ([]ChannelPlan, error) {
	return c.PlanFor(c.Coins)
}

// PlanFor is Plan against an explicit coin list.
//
// It exists because the policy decision happens later than the config is read: the
// delisted-coin check needs the venue's universe, which needs a network call. So the
// plans built at construction time may cover coins that are then dropped, and this is
// how they are rebuilt. The subscription frame must match the coins actually recorded —
// subscribing to a coin and refusing to write its frames is the one outcome worse than
// not subscribing at all.
func (c *Config) PlanFor(coins []string) ([]ChannelPlan, error) {
	chans, err := c.channels()
	if err != nil {
		return nil, err
	}
	if len(chans) == 0 {
		return nil, errors.New("channels: at least one channel is required")
	}
	if len(coins) == 0 {
		return nil, errors.New("coins: at least one coin is required")
	}

	plans := make([]ChannelPlan, 0, len(chans))
	for _, ch := range chans {
		channelCoins := coins
		if ch == hl.ChannelAllMids {
			// allMids is global and takes no coin.
			channelCoins = nil
		}

		subs, err := hl.BuildSubscriptions(ch, channelCoins, c.SubParamsFor(ch))
		if err != nil {
			return nil, fmt.Errorf("channel %s: %w", ch, err)
		}
		plan := ChannelPlan{Channel: ch, Subscriptions: subs}
		for _, sub := range subs {
			frame, err := hl.SubscribeFrame(sub)
			if err != nil {
				return nil, fmt.Errorf("channel %s: subscribe frame for %s: %w", ch, sub.Key(), err)
			}
			plan.Frames = append(plan.Frames, frame)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// SubParamsFor returns the subscription parameters for a channel.
func (c *Config) SubParamsFor(ch hl.Channel) hl.SubParams {
	if ch == hl.ChannelL2Book {
		return hl.SubParams{
			Fast:     c.L2Book.Fast,
			NSigFigs: c.L2Book.NSigFigs,
			Mantissa: c.L2Book.Mantissa,
		}
	}
	return hl.SubParams{}
}

// Universe-change policies, for `on_universe_change` in the config.
const (
	// UniverseChangeFail refuses to record into a root whose existing capture describes
	// different instruments. The default.
	UniverseChangeFail = "fail"
	// UniverseChangeAllow records anyway, for a deliberate merge the operator owns.
	UniverseChangeAllow = "allow"
)

// DelistedPolicyOrDefault returns the effective delisted-coin policy.
func (c *Config) DelistedPolicyOrDefault() string {
	if v := strings.ToLower(strings.TrimSpace(c.DelistedPolicy)); v != "" {
		return v
	}
	return DelistedSkip
}

// OnUniverseChangeOrDefault returns the effective universe-change policy.
func (c *Config) OnUniverseChangeOrDefault() string {
	if v := strings.ToLower(strings.TrimSpace(c.OnUniverseChange)); v != "" {
		return v
	}
	return UniverseChangeFail
}

// ReadLimit returns the effective per-message read limit. An unset key selects
// DefaultReadLimitBytes rather than the connector's 1 MiB default.
func (c *Config) ReadLimit() int64 {
	if c.Recording.ReadLimitBytes == 0 {
		return DefaultReadLimitBytes
	}
	return c.Recording.ReadLimitBytes
}

// AppPingIntervalMS returns the effective application ping interval. An unset key
// selects DefaultAppPingMS rather than the connector's 50s default; a negative value
// disables the ping.
func (c *Config) AppPingIntervalMS() int64 {
	if c.Recording.PingIntervalMS == 0 {
		return DefaultAppPingMS
	}
	return c.Recording.PingIntervalMS
}

// CompressionLevel maps the configured name onto a gzip level.
//
// The default is the middle of the trade, not gzip.BestSpeed: CPU is ~99% idle on the
// target box while the disk is the scarce resource, and level 6 gives ~30% more
// retention than level 1 for ~0.27% of one core (PERP_COLLECTOR_SPEC.md §6.2).
func (c *Config) CompressionLevel() (int, error) {
	switch strings.ToLower(strings.TrimSpace(c.Recording.Compression)) {
	case "", "default":
		return 6, nil // flate's default level, stated explicitly
	case "best_speed":
		return 1, nil
	case "best":
		return 9, nil
	default:
		return 0, fmt.Errorf("recording.compression: %q is not one of default, best_speed, best",
			c.Recording.Compression)
	}
}

// FlushEvery returns the effective flush interval.
func (c *Config) FlushEvery() time.Duration {
	ms := c.Recording.FlushIntervalMS
	if ms == 0 {
		return DefaultFlushEvery
	}
	return time.Duration(ms) * time.Millisecond
}

// QueueSize returns the effective per-channel queue size.
func (c *Config) QueueSize() int {
	if c.Recording.QueueSize <= 0 {
		return DefaultQueueSize
	}
	return c.Recording.QueueSize
}

// Options returns the connector transport options this config implies.
func (c *Config) Options() hl.Options {
	opts := hl.DefaultOptions()
	if c.Connector.WSURL != "" {
		opts.URL = c.Connector.WSURL
	}
	if c.Recording.ReconnectIntervalMS != 0 {
		opts.ReconnectIntervalMs = c.Recording.ReconnectIntervalMS
	}
	// 0 means "library default" for each of these, so only non-zero values are
	// forwarded. The library treats a negative value as "disabled".
	opts.ReadLimitBytes = c.ReadLimit()
	opts.AppPingIntervalMs = c.AppPingIntervalMS()
	opts.WSPingIntervalMs = c.Recording.WSPingIntervalMS
	return opts
}

// MinFree returns the free-space floor, using the default when the key is absent and
// 0 when the guard is explicitly disabled.
func (c *Config) MinFree() int64 {
	if c.Recording.MinFreeBytes == nil {
		return DefaultMinFreeBytes
	}
	return *c.Recording.MinFreeBytes
}

// SinkConfigFor returns the sink configuration for one channel.
func (c *Config) SinkConfigFor(ch hl.Channel, compression int) SinkConfig {
	return SinkConfig{
		Channel:     string(ch),
		Root:        c.DataDir,
		Compression: compression,
		QueueSize:   c.QueueSize(),
		FlushEvery:  c.FlushEvery(),
	}
}
