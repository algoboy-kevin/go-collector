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
	// its own: 0 selects the library default of 50s, and a negative value disables
	// the ping. A wrong guess here is not symmetric — 0 meaning "disabled" would
	// leave an idle socket to be closed by the venue.
	PingIntervalMS int64 `yaml:"ping_interval_ms"`

	// WSPingIntervalMS is the WebSocket control-ping interval, whose pong watchdog
	// (2x this) is what catches a half-open socket. 0 selects the library default of
	// 20s; negative disables it.
	WSPingIntervalMS int64 `yaml:"ws_ping_interval_ms"`

	// QueueSize bounds the per-channel pending-frame queue.
	QueueSize int `yaml:"queue_size"`

	// ReadLimitBytes caps a single WebSocket message. 0 selects the library default
	// of 1 MiB.
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
	chans, err := c.channels()
	if err != nil {
		return nil, err
	}
	if len(chans) == 0 {
		return nil, errors.New("channels: at least one channel is required")
	}
	if len(c.Coins) == 0 {
		return nil, errors.New("coins: at least one coin is required")
	}

	plans := make([]ChannelPlan, 0, len(chans))
	for _, ch := range chans {
		coins := c.Coins
		if ch == hl.ChannelAllMids {
			// allMids is global and takes no coin.
			coins = nil
		}

		subs, err := hl.BuildSubscriptions(ch, coins, c.SubParamsFor(ch))
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
	opts.ReadLimitBytes = c.Recording.ReadLimitBytes
	opts.AppPingIntervalMs = c.Recording.PingIntervalMS
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
