package hyperliquid

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
)

const baseConfig = `
data_dir: /tmp/hl-capture
coins:
  - BTC
  - ETH
channels:
  - l2Book
  - trades
`

// loadYAML writes body to a temp file and loads it.
func loadYAML(t *testing.T, body string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hl.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return Load(path)
}

func mustLoad(t *testing.T, body string) *Config {
	t.Helper()
	cfg, err := loadYAML(t, body)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// ─────────────────────────────────────────────────────────────
// Decoding
// ─────────────────────────────────────────────────────────────

func TestUnknownKeyIsRejected(t *testing.T) {
	// The TestShippedConfigsDecode precedent: a typo'd key must be an error, not a
	// silent no-op. `queue_sise` would otherwise leave the operator believing they
	// had changed something.
	_, err := loadYAML(t, baseConfig+"\nrecording:\n  queue_sise: 4096\n")
	if err == nil {
		t.Fatal("unknown key must be rejected")
	}
	if !strings.Contains(err.Error(), "queue_sise") {
		t.Fatalf("error should name the offending key, got: %v", err)
	}
}

func TestDefaultsAreApplied(t *testing.T) {
	cfg := mustLoad(t, `
coins: [BTC]
channels: [bbo]
`)
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("data_dir = %q, want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.Connector.WSURL != hl.DefaultWSURL {
		t.Errorf("ws_url = %q, want the venue default", cfg.Connector.WSURL)
	}
	if cfg.Connector.InfoURL != hl.DefaultInfoURL {
		t.Errorf("info_url = %q, want the venue default", cfg.Connector.InfoURL)
	}
	if got := cfg.FlushEvery().Milliseconds(); got != DefaultFlushEvery.Milliseconds() {
		t.Errorf("flush interval = %dms, want %dms", got, DefaultFlushEvery.Milliseconds())
	}
	if got := cfg.MinFree(); got != DefaultMinFreeBytes {
		t.Errorf("MinFree = %d, want the %d default", got, DefaultMinFreeBytes)
	}
}

func TestMinFreeDistinguishesAbsentFromZero(t *testing.T) {
	// This is the one knob where both 0 and "absent" are meaningful, so it is the one
	// knob that is a pointer. An absent key must mean "guard on with the default";
	// an explicit 0 must mean "guard off".
	absent := mustLoad(t, baseConfig)
	if got := absent.MinFree(); got != DefaultMinFreeBytes {
		t.Errorf("absent MinFree = %d, want the default %d", got, DefaultMinFreeBytes)
	}

	explicit := mustLoad(t, baseConfig+"\nrecording:\n  min_free_bytes: 0\n")
	if got := explicit.MinFree(); got != 0 {
		t.Errorf("explicit 0 MinFree = %d, want 0 (disabled)", got)
	}

	negative, err := loadYAML(t, baseConfig+"\nrecording:\n  min_free_bytes: -1\n")
	if err == nil {
		t.Errorf("a negative min_free_bytes must be rejected, got %+v", negative)
	}
}

// ─────────────────────────────────────────────────────────────
// Validation — each rule guards a real footgun
// ─────────────────────────────────────────────────────────────

func TestValidationRejectsBadConfigs(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string // substring the error must contain
	}{
		{
			name: "no coins",
			body: "channels: [l2Book]\n",
			want: "at least one coin",
		},
		{
			name: "no channels",
			body: "coins: [BTC]\n",
			want: "at least one channel",
		},
		{
			name: "unknown channel",
			body: "coins: [BTC]\nchannels: [l2book]\n", // wrong case: the venue is exact
			want: "not a recordable channel",
		},
		{
			name: "duplicate channel",
			body: "coins: [BTC]\nchannels: [trades, trades]\n",
			want: "duplicate channel",
		},
		{
			name: "duplicate coin",
			body: "coins: [BTC, BTC]\nchannels: [trades]\n",
			want: "duplicate coin",
		},
		{
			name: "coin with whitespace inside",
			body: "coins: [\"BT C\"]\nchannels: [trades]\n",
			want: "whitespace",
		},
		{
			name: "coin with a quote",
			body: "coins: ['BT\"C']\nchannels: [trades]\n",
			want: "contains",
		},
		{
			name: "l2Book params without the l2Book channel",
			body: "coins: [BTC]\nchannels: [trades]\nl2Book:\n  fast: true\n",
			want: "l2Book is not in `channels`",
		},
		{
			name: "mantissa without nSigFigs=5",
			body: "coins: [BTC]\nchannels: [l2Book]\nl2Book:\n  nSigFigs: 3\n  mantissa: 2\n",
			want: "mantissa",
		},
		{
			name: "nSigFigs out of range",
			body: "coins: [BTC]\nchannels: [l2Book]\nl2Book:\n  nSigFigs: 7\n",
			want: "nSigFigs",
		},
		{
			name: "unknown compression",
			body: baseConfig + "\nrecording:\n  compression: fastest\n",
			want: "compression",
		},
		{
			name: "flush interval of zero-ish",
			body: baseConfig + "\nrecording:\n  flush_interval_ms: -5\n",
			want: "flush_interval_ms",
		},
		{
			name: "flush interval above the ceiling",
			body: baseConfig + "\nrecording:\n  flush_interval_ms: 60001\n",
			want: "flush_interval_ms",
		},
		{
			name: "negative queue size",
			body: baseConfig + "\nrecording:\n  queue_size: -1\n",
			want: "queue_size",
		},
		{
			name: "negative reconnect interval",
			body: baseConfig + "\nrecording:\n  reconnect_interval_ms: -1\n",
			want: "reconnect_interval_ms",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadYAML(t, tc.body)
			if err == nil {
				t.Fatalf("expected an error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestCandleIsSubscribableButNotRecordable(t *testing.T) {
	// The reason our recorded set is a separate list rather than the venue's: candle
	// is a legal subscription and a useless capture (RECORDER_SPEC.md §1).
	if !hl.IsSubscribable(hl.ChannelCandle) {
		t.Fatal("premise changed: candle is no longer subscribable at the venue")
	}
	if isRecorded(hl.ChannelCandle) {
		t.Fatal("candle must not be recordable")
	}
	if _, err := loadYAML(t, "coins: [BTC]\nchannels: [candle]\n"); err == nil {
		t.Fatal("a candle channel must be rejected")
	}
}

func TestValidationReportsEveryProblemAtOnce(t *testing.T) {
	// One error at a time makes config work a guessing game.
	_, err := loadYAML(t, `
coins: [BTC, BTC]
channels: [nope]
recording:
  compression: turbo
  flush_interval_ms: 999999
`)
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"duplicate coin", "not a recordable channel", "compression", "flush_interval_ms"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("combined error is missing %q:\n%v", want, err)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Plan — config becomes subscriptions
// ─────────────────────────────────────────────────────────────

func TestPlanGivesOneConnectionPerChannel(t *testing.T) {
	cfg := mustLoad(t, `
coins: [BTC, ETH, SOL]
channels: [l2Book, trades, bbo, activeAssetCtx]
l2Book:
  fast: true
`)
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plans) != 4 {
		t.Fatalf("got %d plans, want 4 (one connection per channel)", len(plans))
	}
	for _, p := range plans {
		if got := len(p.Subscriptions); got != 3 {
			t.Errorf("%s: %d subscriptions, want 3 (one per coin)", p.Channel, got)
		}
		if got := len(p.Frames); got != 3 {
			t.Errorf("%s: %d frames, want 3", p.Channel, got)
		}
	}
}

func TestPlanFramesUseTheMethodSubscriptionShape(t *testing.T) {
	// The shape the bare-form spec originally got wrong, pinned by decoding the
	// bytes the recorder will actually put on the wire.
	cfg := mustLoad(t, "coins: [BTC]\nchannels: [l2Book]\nl2Book:\n  fast: true\n")
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	var frame struct {
		Method       string `json:"method"`
		Subscription struct {
			Type string `json:"type"`
			Coin string `json:"coin"`
			Fast bool   `json:"fast"`
		} `json:"subscription"`
	}
	if err := json.Unmarshal(plans[0].Frames[0], &frame); err != nil {
		t.Fatalf("subscribe frame is not JSON: %v (%s)", err, plans[0].Frames[0])
	}
	if frame.Method != "subscribe" {
		t.Errorf("method = %q, want %q", frame.Method, "subscribe")
	}
	if frame.Subscription.Type != "l2Book" || frame.Subscription.Coin != "BTC" {
		t.Errorf("subscription = %+v, want l2Book/BTC", frame.Subscription)
	}
	if !frame.Subscription.Fast {
		t.Error("fast should be carried through")
	}
}

func TestPlanOmitsParamsWeWereToldNotToSet(t *testing.T) {
	// nSigFigs thins prices as well as levels, so the default config must not send it.
	cfg := mustLoad(t, "coins: [BTC]\nchannels: [l2Book]\n")
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	for _, needle := range []string{"nSigFigs", "mantissa", "fast"} {
		if strings.Contains(string(plans[0].Frames[0]), needle) {
			t.Errorf("frame should omit %s: %s", needle, plans[0].Frames[0])
		}
	}
}

func TestPlanGivesAllMidsExactlyOneCoinlessSubscription(t *testing.T) {
	cfg := mustLoad(t, "coins: [BTC, ETH]\nchannels: [allMids]\n")
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if got := len(plans[0].Subscriptions); got != 1 {
		t.Fatalf("allMids has %d subscriptions, want 1 (it is global)", got)
	}
	if got := plans[0].Subscriptions[0].Key(); got != "allMids" {
		t.Errorf("subscription key = %q, want %q", got, "allMids")
	}
}

func TestPlanSubscriptionsMatchTheRecordedChannelSet(t *testing.T) {
	cfg := mustLoad(t, `
coins: [BTC]
channels: [l2Book, trades, bbo, activeAssetCtx, allMids]
`)
	plans, err := cfg.Plan()
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	for _, p := range plans {
		for _, sub := range p.Subscriptions {
			if sub.Type != p.Channel {
				t.Fatalf("%s plan holds a %s subscription", p.Channel, sub.Type)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Derived settings
// ─────────────────────────────────────────────────────────────

func TestCompressionLevelMapping(t *testing.T) {
	cases := map[string]int{"": 6, "default": 6, "best_speed": 1, "best": 9, "DEFAULT": 6}
	for name, want := range cases {
		cfg := mustLoad(t, baseConfig+"\nrecording:\n  compression: "+name+"\n")
		got, err := cfg.CompressionLevel()
		if err != nil {
			t.Fatalf("compression %q: %v", name, err)
		}
		if got != want {
			t.Errorf("compression %q -> %d, want %d", name, got, want)
		}
	}
}

func TestOptionsForwardOnlyNonZeroKnobs(t *testing.T) {
	// 0 means "library default" for each of these, so forwarding a literal 0 is
	// correct and forwarding a negative (which the library reads as "disabled") must
	// survive.
	cfg := mustLoad(t, baseConfig+`
recording:
  ping_interval_ms: -1
  ws_ping_interval_ms: 30000
  read_limit_bytes: 2097152
  reconnect_interval_ms: 250
`)
	opts := cfg.Options()
	if opts.AppPingIntervalMs != -1 {
		t.Errorf("AppPingIntervalMs = %d, want -1 (disabled)", opts.AppPingIntervalMs)
	}
	if opts.WSPingIntervalMs != 30000 {
		t.Errorf("WSPingIntervalMs = %d, want 30000", opts.WSPingIntervalMs)
	}
	if opts.ReadLimitBytes != 2097152 {
		t.Errorf("ReadLimitBytes = %d, want 2097152", opts.ReadLimitBytes)
	}
	if opts.ReconnectIntervalMs != 250 {
		t.Errorf("ReconnectIntervalMs = %d, want 250", opts.ReconnectIntervalMs)
	}
}

func TestSinkConfigUsesTheDataDirAndChannel(t *testing.T) {
	cfg := mustLoad(t, baseConfig)
	sc := cfg.SinkConfigFor(hl.ChannelBBO, 1)
	if sc.Root != cfg.DataDir {
		t.Errorf("Root = %q, want %q", sc.Root, cfg.DataDir)
	}
	if sc.Channel != "bbo" {
		t.Errorf("Channel = %q, want bbo", sc.Channel)
	}
	if sc.Compression != 1 {
		t.Errorf("Compression = %d, want 1", sc.Compression)
	}
	if sc.QueueSize != DefaultQueueSize {
		t.Errorf("QueueSize = %d, want %d", sc.QueueSize, DefaultQueueSize)
	}
}
