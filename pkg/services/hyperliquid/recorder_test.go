package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	coderws "github.com/coder/websocket"
)

// ptr is used for the one config field that is a pointer.
func ptr[T any](v T) *T { return &v }

// ─────────────────────────────────────────────────────────────
// A local stand-in for the venue
// ─────────────────────────────────────────────────────────────

// fakeVenue is a local WebSocket server that does enough of what Hyperliquid does to
// exercise the recorder for real: accept, read a subscribe frame, optionally
// acknowledge it, then stream a fixed number of frames for that channel.
//
// The recorder talks to it through the same connector code that talks to the venue, so
// the subscription frames, the connect hook, the raw-frame hook and the shutdown
// sequence are all genuinely exercised rather than stubbed.
type fakeVenue struct {
	srv          *httptest.Server
	framesPerSub int
	ack          bool

	// rejectImmediately reproduces the venue's behaviour for a refused subscription or
	// an IP over its connection limit: complete the handshake, then close. The dial
	// *succeeds*, which is what makes it invisible to connector-level backoff.
	rejectImmediately bool

	mu    sync.Mutex
	subs  []seenSub
	conns int
}

type seenSub struct {
	Type string
	Coin string
}

func newFakeVenue(t *testing.T, framesPerSub int, ack bool) *fakeVenue {
	t.Helper()
	v := &fakeVenue{framesPerSub: framesPerSub, ack: ack}
	v.srv = httptest.NewServer(http.HandlerFunc(v.handle))
	t.Cleanup(v.srv.Close)
	return v
}

// newRejectingVenue accepts every connection and immediately closes it, which is how the
// real venue reports a refused subscription or an exhausted connection budget.
func newRejectingVenue(t *testing.T) *fakeVenue {
	t.Helper()
	v := &fakeVenue{rejectImmediately: true}
	v.srv = httptest.NewServer(http.HandlerFunc(v.handle))
	t.Cleanup(v.srv.Close)
	return v
}

func (v *fakeVenue) URL() string { return "ws" + strings.TrimPrefix(v.srv.URL, "http") }

func (v *fakeVenue) subsSeen() []seenSub {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]seenSub(nil), v.subs...)
}

func (v *fakeVenue) connections() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.conns
}

func (v *fakeVenue) handle(w http.ResponseWriter, r *http.Request) {
	c, err := coderws.Accept(w, r, &coderws.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()

	v.mu.Lock()
	v.conns++
	v.mu.Unlock()

	if v.rejectImmediately {
		_ = c.Close(coderws.StatusPolicyViolation, "Cannot open more than 15 connections.")
		return
	}

	ctx := context.Background()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}

		var req struct {
			Method       string `json:"method"`
			Subscription struct {
				Type string `json:"type"`
				Coin string `json:"coin"`
			} `json:"subscription"`
		}
		if err := json.Unmarshal(data, &req); err != nil {
			continue
		}
		if req.Method == "ping" {
			_ = c.Write(ctx, coderws.MessageText, []byte(`{"channel":"pong"}`))
			continue
		}
		if req.Method != "subscribe" {
			continue
		}

		ch, coin := req.Subscription.Type, req.Subscription.Coin
		v.mu.Lock()
		v.subs = append(v.subs, seenSub{Type: ch, Coin: coin})
		v.mu.Unlock()

		if v.ack {
			ack := fmt.Sprintf(`{"channel":"subscriptionResponse","data":%s}`, data)
			if err := c.Write(ctx, coderws.MessageText, []byte(ack)); err != nil {
				return
			}
		}
		for i := 0; i < v.framesPerSub; i++ {
			if err := c.Write(ctx, coderws.MessageText, venueFrame(ch, coin, i)); err != nil {
				return
			}
		}
	}
}

// venueFrame builds a frame in the shape the venue uses for the channel, so both the
// connector's decoder and the recorder's telemetry see something realistic. Every
// frame is wrapped in the `{"channel":...,"data":...}` envelope, and a `trades`
// payload is an array of prints — which is also the largest frame the venue produces.
func venueFrame(channel, coin string, i int) []byte {
	var payload string
	if channel == string(hl.ChannelTrades) {
		payload = fmt.Sprintf(
			`[{"coin":%q,"side":"B","px":"100.5","sz":"1.25","time":%d,"hash":"0xabc","tid":%d,"users":[]}]`,
			coin, time.Now().UnixMilli(), 1000+i)
	} else {
		payload = fmt.Sprintf(`{"coin":%q,"time":%d,"seq":%d}`, coin, time.Now().UnixMilli(), i)
	}
	return []byte(fmt.Sprintf(`{"channel":%q,"data":%s}`, channel, payload))
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// singleHourFile returns the one complete hour file in dir, and asserts there is
// exactly one and that no `.open` file was left behind.
func singleHourFile(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var complete, open []string
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), BucketSuffix):
			complete = append(complete, e.Name())
		case strings.HasSuffix(e.Name(), OpenSuffix):
			open = append(open, e.Name())
		}
	}
	if len(complete) != 1 {
		t.Fatalf("want exactly one complete hour file in %s, got %v", dir, complete)
	}
	if len(open) != 0 {
		t.Fatalf("a clean shutdown must leave no .open file, got %v", open)
	}
	return filepath.Join(dir, complete[0])
}

func recorderFor(t *testing.T, venue *fakeVenue, body string, extra ...func(*RecorderConfig)) *Recorder {
	t.Helper()
	cfg := mustLoad(t, body)
	opts := RecorderConfig{
		Config:      cfg,
		Info:        newStubInfo("BTC", "ETH"),
		AckWait:     500 * time.Millisecond,
		MetaOptions: MetaOptions{Attempts: 1},
	}
	for _, fn := range extra {
		fn(&opts)
	}
	r, err := NewRecorder(opts)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	return r
}

func venueConfig(dataDir, wsURL, channels string, extra string) string {
	return fmt.Sprintf(`
data_dir: %q
coins: [BTC, ETH]
channels: [%s]
connector:
  ws_url: %q
recording:
  min_free_bytes: 0
%s`, dataDir, channels, wsURL, extra)
}

// ─────────────────────────────────────────────────────────────
// The happy path, end to end
// ─────────────────────────────────────────────────────────────

func TestRecorderCapturesFramesAndMarksTheStream(t *testing.T) {
	venue := newFakeVenue(t, 3, true)
	dataDir := t.TempDir()
	r := recorderFor(t, venue, venueConfig(dataDir, venue.URL(), "trades, bbo", ""))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	const wantFrames = int64(3 * 2) // framesPerSub * coins
	waitUntil(t, 10*time.Second, "every channel to capture its frames", func() bool {
		for _, sink := range r.sinks {
			if sink.Frames() < wantFrames {
				return false
			}
		}
		return true
	})
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// One connection per channel, each carrying every coin.
	if got := venue.connections(); got != 2 {
		t.Errorf("venue saw %d connections, want 2 (one per channel)", got)
	}
	for _, sub := range venue.subsSeen() {
		if sub.Coin == "" {
			t.Errorf("a subscription arrived with no coin: %+v", sub)
		}
	}

	// Everything the connection received is archived, handshakes included: one
	// acknowledgement per subscription lands in the same file as the data. The acks are
	// not filtered out, because filtering would mean parsing, and a frame the recorder
	// *did* understand is exactly the kind it must not drop.
	const acksPerChannel = int64(2)

	for _, ch := range []string{"trades", "bbo"} {
		path := singleHourFile(t, filepath.Join(dataDir, ch))
		lines := readLines(t, path)

		if want := int(wantFrames+acksPerChannel) + 2; len(lines) != want {
			t.Fatalf("%s: got %d lines, want %d (frames + acks + 2 sentinels): %v", ch, len(lines), want, lines)
		}
		if !strings.Contains(lines[0], `"event":"subscribed"`) {
			t.Errorf("%s: first line should be the *subscribed* sentinel — the initial connect "+
				"must not read as a reconnect — got %q", ch, lines[0])
		}
		if strings.Contains(lines[0], `"event":"resubscribed"`) {
			t.Errorf("%s: the first connect must never say resubscribed, got %q", ch, lines[0])
		}
		if !strings.Contains(lines[0], `"run_id":"`) {
			t.Errorf("%s: every mark must name its run, got %q", ch, lines[0])
		}
		if !strings.Contains(lines[len(lines)-1], `"event":"stopped"`) {
			t.Errorf("%s: last line should be the stopped sentinel, got %q", ch, lines[len(lines)-1])
		}
		// No connection dropped during this run, so a `resubscribed` or `disconnected` mark
		// anywhere would be a lie about the capture.
		for _, line := range lines {
			for _, lie := range []string{`"event":"resubscribed"`, `"event":"disconnected"`} {
				if strings.Contains(line, lie) {
					t.Errorf("%s: nothing dropped, so %s must not appear: %q", ch, lie, line)
				}
			}
		}

		for i, line := range lines[1 : len(lines)-1] {
			tab := strings.IndexByte(line, '\t')
			if tab <= 0 {
				t.Fatalf("%s line %d is not <rx_ns>\\t<frame>: %q", ch, i, line)
			}
			if _, err := strconv.ParseInt(line[:tab], 10, 64); err != nil {
				t.Errorf("%s line %d: rx_ns is not an integer: %q", ch, i, line[:tab])
			}
			// Routing by connection identity: only this channel's frames and the venue's
			// own handshake traffic may appear in this channel's file.
			frame := line[tab+1:]
			mine := strings.Contains(frame, fmt.Sprintf(`"channel":%q`, ch))
			ack := strings.Contains(frame, `"channel":"subscriptionResponse"`)
			if !mine && !ack {
				t.Errorf("%s line %d holds a frame for another channel: %s", ch, i, frame)
			}
		}
	}

	m := r.Manifest()
	if m.StopReason != StopSignal {
		t.Errorf("StopReason = %q, want %q", m.StopReason, StopSignal)
	}
	if m.Error != "" {
		t.Errorf("a clean run must not record an error, got %q", m.Error)
	}
	if m.EndedNS == 0 || m.EndedNS <= m.StartedNS {
		t.Errorf("EndedNS = %d, StartedNS = %d", m.EndedNS, m.StartedNS)
	}
	if m.Recording.FreeBytesAtStart == 0 {
		t.Error("FreeBytesAtStart should have been sampled at startup")
	}
	if len(m.ChannelReports) != 2 {
		t.Fatalf("got %d channel reports, want 2", len(m.ChannelReports))
	}
	for _, rep := range m.ChannelReports {
		// Frames is every venue frame written, which includes the handshake acks: the
		// number has to match the file's line count minus the recorder's own sentinels.
		if want := wantFrames + acksPerChannel; rep.Frames != want {
			t.Errorf("%s: Frames = %d, want %d (data frames + acks)", rep.Channel, rep.Frames, want)
		}
		if rep.Sentinels != 2 {
			t.Errorf("%s: Sentinels = %d, want 2", rep.Channel, rep.Sentinels)
		}
		if rep.Drops != 0 || rep.DroppedSentinels != 0 || rep.WriteErrors != 0 {
			t.Errorf("%s: a clean capture must not drop or fail: %+v", rep.Channel, rep)
		}
		if rep.Subscriptions != 2 || rep.Acks != 2 {
			t.Errorf("%s: subscriptions/acks = %d/%d, want 2/2", rep.Channel, rep.Subscriptions, rep.Acks)
		}
		if rep.LargestFrame == 0 || rep.LastFrameNS == 0 {
			t.Errorf("%s: telemetry missing: %+v", rep.Channel, rep)
		}
		if rep.Channel == "trades" {
			// Telemetry from the typed path: the venue's side encoding, recorded so a
			// reader does not have to guess what the ingest must accept.
			if got := rep.TradeSides["B"]; got != wantFrames {
				t.Errorf("trades: TradeSides = %v, want B=%d", rep.TradeSides, wantFrames)
			}
		}
	}

	// meta.json is what makes the catalog buildable, and it is written before any
	// frame is recorded.
	assertExists(t, filepath.Join(dataDir, MetaFile))
}

// ─────────────────────────────────────────────────────────────
// Failure paths
// ─────────────────────────────────────────────────────────────

func TestRecorderFailsTheRunWhenMetaIsUnavailable(t *testing.T) {
	dataDir := t.TempDir()
	cfg := mustLoad(t, venueConfig(dataDir, "ws://127.0.0.1:1/ws", "bbo", ""))

	r, err := NewRecorder(RecorderConfig{
		Config:      cfg,
		Info:        &stubInfo{metaErr: errors.New("network down")},
		MetaOptions: MetaOptions{Attempts: 2, Delay: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	err = r.Run(context.Background())
	if err == nil {
		t.Fatal("a capture with no instrument definitions must not start")
	}
	if got := r.StopReason(); got != StopStartupFailed {
		t.Errorf("StopReason = %q, want %q", got, StopStartupFailed)
	}
	assertNotExists(t, filepath.Join(dataDir, MetaFile))

	// Neither the opening nor the final manifest should claim the capture succeeded.
	m := r.Manifest()
	if m.Error == "" {
		t.Error("the manifest must say why the run failed")
	}
	if m.StopReason != StopStartupFailed {
		t.Errorf("manifest StopReason = %q, want %q", m.StopReason, StopStartupFailed)
	}

	// No frames were written, so there must be no hour file at all.
	entries, err := os.ReadDir(filepath.Join(dataDir, "bbo"))
	if err != nil {
		t.Fatalf("read channel dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), BucketSuffix) || strings.HasSuffix(e.Name(), OpenSuffix) {
			t.Errorf("no data should have been written, found %s", e.Name())
		}
	}
}

func TestRecorderStopsCleanlyWhenFreeSpaceRunsOut(t *testing.T) {
	venue := newFakeVenue(t, 2, true)
	dataDir := t.TempDir()
	// The guard has to be on for this test, so the floor is raised above zero.
	cfg := mustLoad(t, venueConfig(dataDir, venue.URL(), "bbo", "")+
		"")
	cfg.Recording.MinFreeBytes = ptr(int64(1_000_000))

	var free atomic.Int64
	free.Store(50 << 30) // 50 GiB: plenty

	r, err := NewRecorder(RecorderConfig{
		Config:         cfg,
		Info:           newStubInfo("BTC", "ETH"),
		FreeBytes:      func(string) (int64, error) { return free.Load(), nil },
		DiskCheckEvery: 5 * time.Millisecond,
		AckWait:        time.Hour, // the ack report must not fire mid-test
		MetaOptions:    MetaOptions{Attempts: 1},
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	// Deliberately not cancellable: the guard itself has to end the run.
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()

	waitUntil(t, 10*time.Second, "the first frame to land", func() bool {
		return r.sinks[hl.ChannelBBO].Frames() > 0
	})
	free.Store(1) // the disk has filled up

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the disk guard never stopped the run")
	}
	if runErr == nil {
		t.Fatal("a low-disk stop must be reported so the operator sees a non-zero exit")
	}
	if got := r.StopReason(); got != StopDiskLow {
		t.Fatalf("StopReason = %q, want %q", got, StopDiskLow)
	}

	// This is the whole point of stopping cleanly rather than dying: the hour in
	// progress is finished, fsynced and renamed, so the capture is valid and complete
	// rather than a truncated file that looks whole.
	path := singleHourFile(t, filepath.Join(dataDir, "bbo"))
	lines := readLines(t, path)
	if len(lines) == 0 {
		t.Fatal("no lines recorded")
	}
	if !strings.Contains(lines[len(lines)-1], `"event":"stopped"`) {
		t.Errorf("the stopped sentinel must close the stream, got %q", lines[len(lines)-1])
	}

	m := r.Manifest()
	if m.StopReason != StopDiskLow {
		t.Errorf("manifest StopReason = %q, want %q", m.StopReason, StopDiskLow)
	}
	if m.Error == "" {
		t.Error("the manifest must record why the capture stopped early")
	}
	if m.Recording.FreeBytesAtEnd != 1 {
		t.Errorf("FreeBytesAtEnd = %d, want the value that tripped the guard", m.Recording.FreeBytesAtEnd)
	}
}

func TestRecorderDistinguishesADurationStopFromASignal(t *testing.T) {
	// A capture should say whether it ended because someone stopped it or because it
	// was asked to run for a fixed length — the two mean different things to a
	// consumer deciding whether the capture is complete.
	venue := newFakeVenue(t, 1, true)
	dataDir := t.TempDir()
	r := recorderFor(t, venue, venueConfig(dataDir, venue.URL(), "bbo", ""))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := r.StopReason(); got != StopDuration {
		t.Errorf("StopReason = %q, want %q", got, StopDuration)
	}
	if got := r.Manifest().StopReason; got != StopDuration {
		t.Errorf("manifest StopReason = %q, want %q", got, StopDuration)
	}
	if r.Manifest().Error != "" {
		t.Errorf("a deadline stop is not an error, got %q", r.Manifest().Error)
	}
	// The data still has to be complete.
	if got := r.sinks[hl.ChannelBBO].Frames(); got == 0 {
		t.Error("no frames were captured")
	}
}

func TestRecorderReportsUnacknowledgedSubscriptions(t *testing.T) {
	// A venue that accepts subscriptions and never acknowledges them. The recorder must
	// still run, and the shortfall must be visible in the capture rather than silent.
	venue := newFakeVenue(t, 1, false)
	dataDir := t.TempDir()
	r := recorderFor(t, venue, venueConfig(dataDir, venue.URL(), "bbo", ""))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitUntil(t, 10*time.Second, "frames to arrive", func() bool {
		return r.sinks[hl.ChannelBBO].Frames() > 0
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	rep := r.Manifest().ChannelReports[0]
	if rep.Subscriptions != 2 {
		t.Errorf("Subscriptions = %d, want 2", rep.Subscriptions)
	}
	if rep.Acks != 0 {
		t.Errorf("Acks = %d, want 0 — the venue acked nothing", rep.Acks)
	}
}

func TestRecorderStopsWhenAChannelFlapsWithoutDeliveringFrames(t *testing.T) {
	// The behavioural guard against a rejection the recorder cannot diagnose: a venue that
	// accepts the handshake, then closes, over and over, with nothing to show for it.
	// Measured on the real thing before the connector capped the backoff: 719 connections
	// in three minutes, straight through the venue's per-IP limit, having captured nothing.
	venue := newRejectingVenue(t)
	dataDir := t.TempDir()

	r, err := NewRecorder(RecorderConfig{
		Config:         mustLoad(t, venueConfig(dataDir, venue.URL(), "bbo", "")),
		Info:           newStubInfo("BTC", "ETH"),
		DiskCheckEvery: 5 * time.Millisecond,
		FlapThreshold:  3,
		AckWait:        time.Hour, // the ack report must not fire mid-test
		MetaOptions:    MetaOptions{Attempts: 1},
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the flap detector never fired — this is the reconnect-storm bug")
	}

	if runErr == nil {
		t.Fatal("a flap loop must be reported so the process exits non-zero")
	}
	if got := r.StopReason(); got != StopConnectionFlap {
		t.Fatalf("StopReason = %q, want %q", got, StopConnectionFlap)
	}

	m := r.Manifest()
	if m.StopReason != StopConnectionFlap {
		t.Errorf("manifest StopReason = %q, want %q", m.StopReason, StopConnectionFlap)
	}
	if !strings.Contains(m.Error, "without receiving a frame") {
		t.Errorf("the capture should say what happened, got %q", m.Error)
	}
	if m.ChannelReports[0].Reconnects == 0 {
		t.Error("Reconnects must be reported, or a flap is invisible in the manifest")
	}

	// The venue closed with a reason (newRejectingVenue sends the real one), and
	// SetOnDisconnect now carries it. It has to reach both the manifest and the channel's
	// own file: the point is that a capture explains its own gaps without the log.
	const why = "Cannot open more than 15 connections."
	if got := m.ChannelReports[0].LastDisconnect; !strings.Contains(got, why) {
		t.Errorf("ChannelReport.LastDisconnect = %q, want it to contain %q", got, why)
	}
	if !strings.Contains(m.Error, why) {
		t.Errorf("the stop error should name the venue's reason, got %q", m.Error)
	}

	hours, err := filepath.Glob(filepath.Join(dataDir, "bbo", "*.jsonl.gz"))
	if err != nil {
		t.Fatalf("glob hour files: %v", err)
	}
	if len(hours) != 1 {
		t.Fatalf("got %d hour files in the capture, want 1: %v", len(hours), hours)
	}
	var sawDisconnect bool
	var sawResubscribed bool
	for _, line := range readLines(t, hours[0]) {
		if strings.Contains(line, `"event":"`+EventResubscribed+`"`) {
			sawResubscribed = true
		}
		if !strings.Contains(line, `"event":"`+EventDisconnected+`"`) {
			continue
		}
		sawDisconnect = true
		if !strings.Contains(line, why) {
			t.Errorf("the disconnected sentinel must carry the reason, got %s", line)
		}
	}
	if !sawDisconnect {
		t.Error("a channel that was dropped must leave a disconnected sentinel in its file")
	}
	// The other half of the subscribed/resubscribed split: a connection that really did
	// come back says `resubscribed`. Only the FIRST connect says `subscribed`.
	if !sawResubscribed {
		t.Error("a channel that reconnected must mark `resubscribed`, or the split loses its meaning")
	}
	if strings.Count(strings.Join(readLines(t, hours[0]), "\n"), `"event":"`+EventSubscribed+`"`) != 1 {
		t.Error("exactly one `subscribed` mark is expected — the first connect and no other")
	}

	// Nothing was captured, but stopping cleanly means there must be no dangling `.open`
	// file either.
	entries, err := os.ReadDir(filepath.Join(dataDir, "bbo"))
	if err != nil {
		t.Fatalf("read channel dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), OpenSuffix) {
			t.Errorf("a clean stop must leave no .open file, found %s", e.Name())
		}
	}
}

func TestRecorderSecondRunDoesNotClobberTheFirstHour(t *testing.T) {
	// A restart inside the same UTC hour: the finished file from the first run must
	// survive, and the new run must not silently overwrite it.
	venue := newFakeVenue(t, 1, true)
	dataDir := t.TempDir()

	runOnce := func() *Recorder {
		r := recorderFor(t, venue, venueConfig(dataDir, venue.URL(), "bbo", ""))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		waitUntil(t, 10*time.Second, "frames", func() bool {
			return r.sinks[hl.ChannelBBO].Frames() > 0
		})
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		return r
	}

	runOnce()
	firstPath := singleHourFile(t, filepath.Join(dataDir, "bbo"))
	firstBytes, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("read first hour: %v", err)
	}

	r := runOnce()

	// The second run opened the same hour bucket, so its lines are what the plain hour
	// file now holds. The first run's hour must still exist, byte for byte, and it must be
	// in quarantine/ rather than loose in the channel directory: a restart inside an hour
	// must not be able to destroy it, and must not be able to hide it either.
	second := readLines(t, singleHourFile(t, filepath.Join(dataDir, "bbo")))
	if len(second) == 0 {
		t.Fatal("second run wrote nothing")
	}
	for i, line := range second {
		ok := strings.Contains(line, `"channel":"_meta"`) ||
			strings.Contains(line, `"channel":"bbo"`) ||
			strings.Contains(line, `"channel":"subscriptionResponse"`)
		if !ok {
			t.Fatalf("line %d of the second run does not belong to bbo: %s", i, line)
		}
	}

	entries, err := os.ReadDir(filepath.Join(dataDir, QuarantineDir, "bbo"))
	if err != nil {
		t.Fatalf("read quarantine dir: %v", err)
	}
	var preserved []string
	for _, e := range entries {
		preserved = append(preserved, e.Name())
	}
	if len(preserved) != 1 {
		t.Fatalf("want exactly one preserved hour, got %v", preserved)
	}
	// The name must identify its own hour and the run that displaced it — not the instant
	// of the displacement, which is what the old `.replaced-<rx_ns>` suffix encoded.
	if !strings.Contains(preserved[0], "2026-10-04T") && !strings.Contains(preserved[0], BucketLayout[:2]) {
		t.Errorf("quarantined name %q should be keyed by its own hour", preserved[0])
	}
	if !strings.Contains(preserved[0], QuarantinedReplaced+"-") {
		t.Errorf("quarantined name %q should say why and by which run", preserved[0])
	}
	got, err := os.ReadFile(filepath.Join(dataDir, QuarantineDir, "bbo", preserved[0]))
	if err != nil {
		t.Fatalf("read preserved hour: %v", err)
	}
	if !bytes.Equal(got, firstBytes) {
		t.Fatal("the first run's hour was altered when it was preserved")
	}

	// The record must describe the ORPHAN's range. Describing the displacing run's would
	// be the bug this replaced: the old suffix was the new file's first rx_ns, so the name
	// told you when the move happened and nothing about what had been moved.
	rep := r.Manifest().ChannelReports[0]
	if len(rep.Quarantined) != 1 {
		t.Fatalf("quarantined records = %+v, want 1", rep.Quarantined)
	}
	q := rep.Quarantined[0]
	if q.Kind != QuarantinedReplaced {
		t.Errorf("Kind = %q, want %q", q.Kind, QuarantinedReplaced)
	}
	if q.Lines == 0 || q.FirstNS == 0 || q.LastNS == 0 {
		t.Errorf("record = %+v, want the orphan's lines and range measured", q)
	}
	// Guards the exact confusion the review found: the range must be the orphan's, so it
	// has to end before this run's first frame.
	if q.LastNS >= rep.FirstFrameNS {
		t.Errorf("orphan range ends at %d, at or after this run's first frame %d — the record "+
			"describes the displacing file, not the displaced one", q.LastNS, rep.FirstFrameNS)
	}
}

func TestRecorderSkipsDelistedCoinsAndRecordsTheDecision(t *testing.T) {
	// Nothing about a delisted market's feed says "dead": the venue accepts the
	// subscription, and `l2Book` is paced so its frame counts match every live coin's.
	// The capture that prompted this spent two hours subscribed to `vntl:OPENAI` for 30
	// trades and ZERO bbo frames. Reading isDelisted at startup is the only chance to
	// notice, and the decision has to be written down or it is indistinguishable later
	// from a coin that was never configured.
	venue := newFakeVenue(t, 1, true)
	dataDir := t.TempDir()

	info := newStubInfo("BTC")
	info.dexMeta = map[string][]byte{
		"vntl": []byte(`{"universe":[{"name":"vntl:OPENAI","szDecimals":3,"isDelisted":true}]}`),
	}

	r, err := NewRecorder(RecorderConfig{
		Config: mustLoad(t, fmt.Sprintf(`
data_dir: %q
coins: [BTC, vntl:OPENAI]
channels: [bbo]
connector:
  ws_url: %q
recording:
  min_free_bytes: 0
`, dataDir, venue.URL())),
		Info:        info,
		AckWait:     time.Hour,
		MetaOptions: MetaOptions{Attempts: 1},
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitUntil(t, 10*time.Second, "frames", func() bool {
		return r.sinks[hl.ChannelBBO].Frames() > 0
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The subscription frame must match the coins whose frames we keep. Subscribing and
	// then refusing to write is the one outcome worse than not subscribing.
	subs := venue.subsSeen()
	if len(subs) != 1 || subs[0].Coin != "BTC" {
		t.Errorf("venue saw %+v, want exactly one subscription for BTC", subs)
	}

	m := r.Manifest()
	if len(m.Coins) != 1 || m.Coins[0].Coin != "BTC" {
		t.Errorf("manifest coins = %+v, want just BTC", m.Coins)
	}
	if m.Coins[0].MetaFile != MetaFile {
		t.Errorf("BTC meta_file = %q, want %q", m.Coins[0].MetaFile, MetaFile)
	}
	if len(m.CoinsSkipped) != 1 || m.CoinsSkipped[0].Coin != "vntl:OPENAI" ||
		m.CoinsSkipped[0].Reason != "isDelisted" {
		t.Errorf("coins_skipped = %+v, want vntl:OPENAI with a reason", m.CoinsSkipped)
	}
	// The skipped dex's dump must still be in the capture: that keeps the skip a
	// reversible decision read off the manifest rather than a re-fetch.
	if _, err := os.Stat(filepath.Join(dataDir, "meta-vntl.json")); err != nil {
		t.Errorf("the skipped dex's meta dump must still be written: %v", err)
	}
	if m.UniverseFingerprint == "" || len(m.MetaSHA256) == 0 {
		t.Errorf("fingerprint=%q meta_sha256=%v, both must be recorded",
			m.UniverseFingerprint, m.MetaSHA256)
	}
}

func TestRecorderRefusesToBlendTwoUniversesIntoOneRoot(t *testing.T) {
	// A consumer derives ONE instrument set and ONE price precision per coin from a whole
	// directory. Two universes in one directory does not error — it produces a capture
	// that looks complete and backtests at the wrong prices. So the second experiment has
	// to be refused, in words that say what to do about it.
	venue := newFakeVenue(t, 1, true)
	dataDir := t.TempDir()

	runOnce := func(coins string) error {
		r, err := NewRecorder(RecorderConfig{
			Config: mustLoad(t, fmt.Sprintf(`
data_dir: %q
coins: [%s]
channels: [bbo]
connector:
  ws_url: %q
recording:
  min_free_bytes: 0
`, dataDir, coins, venue.URL())),
			Info:        newStubInfo("BTC", "ETH", "SOL"),
			AckWait:     time.Hour,
			MetaOptions: MetaOptions{Attempts: 1},
		})
		if err != nil {
			t.Fatalf("NewRecorder(%s): %v", coins, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()

		// Either a frame arrives (Start succeeded), or Run returns first (refused).
		for {
			if r.sinks[hl.ChannelBBO].Frames() > 0 {
				cancel()
				return <-done
			}
			select {
			case err := <-done:
				return err
			case <-time.After(10 * time.Millisecond):
			case <-time.After(15 * time.Second):
				t.Fatal("neither a frame nor an error")
			}
		}
	}

	if err := runOnce("BTC, ETH"); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Same universe, different order: continuing a capture is the normal case.
	if err := runOnce("ETH, BTC"); err != nil {
		t.Fatalf("re-running the same universe must be allowed: %v", err)
	}

	err := runOnce("BTC, SOL")
	if err == nil {
		t.Fatal("a different universe in the same root must be refused")
	}
	for _, want := range []string{"different instruments", "sibling root"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error must contain %q so the operator knows what to do, got: %v", want, err)
		}
	}

	// And a refused run must not itself become the reference the next one matches: if it
	// did, the second attempt would sail through against a directory that holds other
	// data. This is the property that makes the guard safe to keep retrying.
	if again := runOnce("BTC, SOL"); again == nil {
		t.Error("a refused run must not seed the guard — the next attempt must be refused too")
	}

	// The refusal is recorded, so an operator can see the attempt rather than wonder.
	runs, rerr := ReadManifests(dataDir)
	if rerr != nil {
		t.Fatalf("ReadManifests: %v", rerr)
	}
	var sawRefusal bool
	for _, run := range runs {
		if run.StopReason != StopStartupFailed {
			continue
		}
		sawRefusal = true
		if run.UniverseFingerprint != "" {
			t.Errorf("a run that recorded nothing must not claim a universe: %q", run.UniverseFingerprint)
		}
	}
	if !sawRefusal {
		t.Error("a refused startup must leave a record of the attempt")
	}
}
