package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// anchorInfo is stubInfo plus an l2Book-aware Post: it counts calls per coin, can fail a
// chosen coin, and serves a recognisably non-canonical body so "verbatim" is testable.
type anchorInfo struct {
	*stubInfo
	mu       sync.Mutex
	calls    map[string]int
	failCoin string
	body     func(coin string) string
}

func newAnchorInfo() *anchorInfo {
	return &anchorInfo{
		stubInfo: newStubInfo("BTC", "ETH"),
		calls:    map[string]int{},
		// Trailing zeros and key order are deliberately not what a re-marshal would emit.
		body: func(coin string) string {
			return fmt.Sprintf(`{"coin":%q,"time":1700000000000,"levels":[[{"px":"100.0","sz":"2.50","n":1}],[{"px":"100.5","sz":"1.00","n":2}]]}`, coin)
		},
	}
}

func (a *anchorInfo) Post(ctx context.Context, payload any) ([]byte, error) {
	m, ok := payload.(map[string]any)
	if !ok || m["type"] != "l2Book" {
		return a.stubInfo.Post(ctx, payload)
	}
	coin, _ := m["coin"].(string)
	a.mu.Lock()
	a.calls[coin]++
	a.mu.Unlock()
	if coin == a.failCoin {
		return nil, fmt.Errorf("stub: l2Book %s unavailable", coin)
	}
	return []byte(a.body(coin)), nil
}

func (a *anchorInfo) total() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, v := range a.calls {
		n += v
	}
	return n
}

func (a *anchorInfo) count(coin string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[coin]
}

const anchorYAML = "book_anchors:\n  enabled: true\n  interval_secs: 300\n"

func anchorRecorder(t *testing.T, venue *fakeVenue, info InfoClient, extraYAML string, tick <-chan time.Time) (*Recorder, string) {
	t.Helper()
	dataDir := t.TempDir()
	r := recorderFor(t, venue, venueConfig(dataDir, venue.URL(), "trades", extraYAML),
		func(o *RecorderConfig) {
			o.Info = info
			o.AnchorDelay = time.Nanosecond // no real pacing in tests
			o.AnchorTick = tick
		})
	return r, dataDir
}

func startRun(r *Recorder) (cancel func() error) {
	ctx, c := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() error { c(); return <-done }
}

func reportFor(t *testing.T, m Manifest, ch string) (ChannelReport, bool) {
	t.Helper()
	for _, rep := range m.ChannelReports {
		if rep.Channel == ch {
			return rep, true
		}
	}
	return ChannelReport{}, false
}

// ─── config ──────────────────────────────────────────────────

func TestBookAnchorsConfig(t *testing.T) {
	base := "data_dir: x\ncoins: [BTC, ETH]\nchannels: [trades]\n"

	cfg := mustLoad(t, base)
	if cfg.BookAnchors.Enabled {
		t.Fatal("book_anchors must be off when the block is absent")
	}
	cfg = mustLoad(t, base+anchorYAML+"  coins: [BTC]\n")
	if got := cfg.BookAnchors.Coins; len(got) != 1 || got[0] != "BTC" {
		t.Errorf("coins = %v, want [BTC]", got)
	}
	if cfg.BookAnchors.Interval() != 300*time.Second {
		t.Errorf("interval = %v", cfg.BookAnchors.Interval())
	}

	for name, yaml := range map[string]string{
		"unknown key":       base + "book_anchors:\n  enabled: true\n  interval: 5\n",
		"negative":          base + "book_anchors:\n  enabled: true\n  interval_secs: -1\n",
		"coin not in coins": base + "book_anchors:\n  enabled: true\n  coins: [SOL]\n",
		"duplicate coin":    base + "book_anchors:\n  enabled: true\n  coins: [BTC, BTC]\n",
	} {
		if _, err := loadYAML(t, yaml); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestAnchorLineRefusesBodiesThatBreakTheFormat(t *testing.T) {
	for name, body := range map[string]string{
		"empty": "", "not json": "oops", "multi-line": "{\"a\":\n1}",
	} {
		if _, err := anchorLine([]byte(body)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	line, err := anchorLine([]byte("  {\"a\":1.50}\n"))
	if err != nil || string(line) != `{"channel":"bookAnchors","data":{"a":1.50}}` {
		t.Errorf("line = %q, err = %v", line, err)
	}
}

// ─── disabled ────────────────────────────────────────────────

func TestBookAnchorsOffWritesNothingAndFetchesNothing(t *testing.T) {
	venue := newFakeVenue(t, 2, true)
	info := newAnchorInfo()
	r, dataDir := anchorRecorder(t, venue, info, "", nil)

	stop := startRun(r)
	waitUntil(t, 10*time.Second, "frames", func() bool { return r.sinks["trades"].Frames() >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := info.total(); n != 0 {
		t.Errorf("anchors off, but %d l2Book fetches happened", n)
	}
	if _, err := os.Stat(filepath.Join(dataDir, BookAnchorsChannel)); !os.IsNotExist(err) {
		t.Errorf("anchors off, but %s/ exists (err=%v)", BookAnchorsChannel, err)
	}
	m := r.Manifest()
	if m.BookAnchors != nil {
		t.Errorf("manifest must not mention anchors when off: %+v", m.BookAnchors)
	}
	if _, ok := reportFor(t, m, BookAnchorsChannel); ok {
		t.Error("no bookAnchors channel report when off")
	}
	raw, _ := os.ReadFile(filepath.Join(dataDir, "manifests", m.RunID+".json"))
	if strings.Contains(string(raw), "book_anchors") || strings.Contains(string(raw), BookAnchorsChannel) {
		t.Error("the manifest file must be unchanged when anchors are off")
	}
}

// ─── startup, periodic, accounting ───────────────────────────

func TestBookAnchorsStartupPeriodicAndManifest(t *testing.T) {
	venue := newFakeVenue(t, 2, true)
	info := newAnchorInfo()
	tick := make(chan time.Time)
	r, dataDir := anchorRecorder(t, venue, info, anchorYAML, tick)

	stop := startRun(r)
	// Startup round: one fetch per coin, taken before the connections exist.
	waitUntil(t, 10*time.Second, "startup anchors", func() bool { return info.total() >= 2 })
	waitUntil(t, 10*time.Second, "frames", func() bool { return r.sinks["trades"].Frames() >= 2 })

	// Two deterministic periodic rounds; the unbuffered send returns once the loop took it.
	tick <- time.Time{}
	tick <- time.Time{}
	waitUntil(t, 10*time.Second, "periodic anchors", func() bool { return info.total() >= 6 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	lines := readLines(t, singleHourFile(t, filepath.Join(dataDir, BookAnchorsChannel)))
	if len(lines) != 6 {
		t.Fatalf("got %d anchor lines, want 6 (3 rounds x 2 coins): %v", len(lines), lines)
	}
	var lastRx int64
	for i, line := range lines {
		tab := strings.IndexByte(line, '\t')
		rx, err := strconv.ParseInt(line[:tab], 10, 64)
		if tab <= 0 || err != nil {
			t.Fatalf("line %d is not <rx_ns>\\t<json>: %q", i, line)
		}
		if rx < lastRx {
			t.Errorf("line %d: rx_ns went backwards", i)
		}
		lastRx = rx
		var env struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line[tab+1:]), &env); err != nil || env.Channel != BookAnchorsChannel {
			t.Fatalf("line %d: bad envelope %q (%v)", i, line, err)
		}
		coin := []string{"BTC", "ETH"}[i%2]
		if string(env.Data) != info.body(coin) {
			t.Errorf("line %d: body not verbatim:\n got  %s\n want %s", i, env.Data, info.body(coin))
		}
	}

	// Ordering vs the first WS frame is deliberately NOT asserted: the startup round
	// runs concurrently with the connections (it must never delay them — see
	// TestStartupRoundDoesNotBlockTheStream). What matters is that the round happened.

	m := r.Manifest()
	rep, ok := reportFor(t, m, BookAnchorsChannel)
	if !ok {
		t.Fatalf("no bookAnchors channel report: %+v", m.ChannelReports)
	}
	if rep.Frames != 6 || rep.Drops != 0 || rep.WriteErrors != 0 || rep.FirstFrameNS == 0 ||
		rep.LastFrameNS < rep.FirstFrameNS || rep.LargestFrame != int64(len(info.body("BTC"))) {
		t.Errorf("bookAnchors report: %+v", rep)
	}
	ba := m.BookAnchors
	if ba == nil || !ba.Enabled || ba.IntervalSecs != 300 || len(ba.Coins) != 2 ||
		ba.Fetches != 6 || ba.Failures != 0 || ba.Rounds[AnchorStartup] != 1 || ba.Rounds[AnchorPeriodic] != 2 {
		t.Errorf("manifest book_anchors: %+v", ba)
	}
}

// ─── failure and subset ──────────────────────────────────────

func TestBookAnchorFailureLogsAndSkips(t *testing.T) {
	venue := newFakeVenue(t, 2, true)
	info := newAnchorInfo()
	info.failCoin = "ETH"
	r, dataDir := anchorRecorder(t, venue, info, anchorYAML, make(chan time.Time))

	stop := startRun(r)
	waitUntil(t, 10*time.Second, "startup anchors", func() bool { return info.count("ETH") >= 1 && info.count("BTC") >= 1 })
	waitUntil(t, 10*time.Second, "frames", func() bool { return r.sinks["trades"].Frames() >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("an anchor failure must not fail the run: %v", err)
	}

	lines := readLines(t, singleHourFile(t, filepath.Join(dataDir, BookAnchorsChannel)))
	if len(lines) != 1 || !strings.Contains(lines[0], `"coin":"BTC"`) {
		t.Errorf("want only the BTC anchor, got %v", lines)
	}
	if ba := r.Manifest().BookAnchors; ba == nil || ba.Failures != 1 || !strings.Contains(ba.LastError, "ETH") {
		t.Errorf("manifest book_anchors: %+v", ba)
	}
	if r.Manifest().StopReason != StopSignal {
		t.Errorf("StopReason = %q", r.Manifest().StopReason)
	}
}

func TestBookAnchorsCoinSubset(t *testing.T) {
	venue := newFakeVenue(t, 1, true)
	info := newAnchorInfo()
	r, _ := anchorRecorder(t, venue, info, anchorYAML+"  coins: [ETH]\n", make(chan time.Time))

	stop := startRun(r)
	waitUntil(t, 10*time.Second, "startup anchors", func() bool { return info.total() >= 1 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if info.count("BTC") != 0 || info.count("ETH") != 1 {
		t.Errorf("calls = %v, want ETH only", info.calls)
	}
}

// ─── the dumper alone: reconnect trigger, coalescing, stop ───

func TestBookAnchorerReconnectRequestsAreCoalescedAndStopIsClean(t *testing.T) {
	info := newAnchorInfo()
	sink, err := NewSink(SinkConfig{Channel: BookAnchorsChannel, Root: t.TempDir(), Compression: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := newBookAnchorer(info, sink, []string{"BTC"}, 0, time.Nanosecond, time.Now, nil)

	a.Start(context.Background()) // startup round runs in the loop (non-blocking)
	waitUntil(t, 10*time.Second, "startup round", func() bool { return info.total() >= 1 })
	a.Request(AnchorReconnect)
	waitUntil(t, 10*time.Second, "reconnect round", func() bool { return info.total() >= 2 })

	a.Stop()
	a.Stop()                   // idempotent
	a.Request(AnchorReconnect) // must not block after Stop
	if err := sink.Stop(); err != nil {
		t.Fatal(err)
	}
	m := a.manifest(BookAnchorsConfig{Enabled: true})
	if m.Rounds[AnchorStartup] != 1 || m.Rounds[AnchorReconnect] != 1 || m.Fetches != 2 {
		t.Errorf("manifest: %+v", m)
	}
	if sink.AfterStop() != 0 {
		t.Error("nothing may be enqueued after Stop")
	}
}

// ─── capture first, anchors second ───────────────────────────

// gatedInfo blocks every l2Book Post until released, so the startup round gets stuck
// mid-fetch while the venue keeps serving.
type gatedInfo struct {
	*anchorInfo
	gate chan struct{}
}

func (g *gatedInfo) Post(ctx context.Context, payload any) ([]byte, error) {
	if m, ok := payload.(map[string]any); ok && m["type"] == "l2Book" {
		select {
		case <-g.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.anchorInfo.Post(ctx, payload)
}

// TestStartupRoundDoesNotBlockTheStream pins the capture-first contract: a stuck (or
// simply slow) anchor round must never delay the WS connections. With the old
// synchronous Start this test FAILS by timing out in "frames while anchors stuck" —
// the connections were not even dialled until the round finished.
func TestStartupRoundDoesNotBlockTheStream(t *testing.T) {
	venue := newFakeVenue(t, 2, true)
	info := &gatedInfo{anchorInfo: newAnchorInfo(), gate: make(chan struct{})}
	r, _ := anchorRecorder(t, venue, info, anchorYAML, nil)

	stop := startRun(r)
	// The stream flows while the startup round is still stuck on its first fetch.
	waitUntil(t, 10*time.Second, "frames while anchors stuck", func() bool {
		return r.sinks["trades"].Frames() >= 2
	})
	if info.total() != 0 {
		t.Fatalf("a fetch completed despite the gate: %d", info.total())
	}
	close(info.gate)
	waitUntil(t, 10*time.Second, "startup round completes", func() bool { return info.total() >= 2 })
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
