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
		Info:        &stubInfo{meta: []byte(`{"universe":[]}`)},
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
		if !strings.Contains(lines[0], `"event":"resubscribed"`) {
			t.Errorf("%s: first line should be the resubscribed sentinel, got %q", ch, lines[0])
		}
		if !strings.Contains(lines[len(lines)-1], `"event":"stopped"`) {
			t.Errorf("%s: last line should be the stopped sentinel, got %q", ch, lines[len(lines)-1])
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
		Info:           &stubInfo{meta: []byte(`{}`)},
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

func TestRecorderSecondRunDoesNotClobberTheFirstHour(t *testing.T) {
	// A restart inside the same UTC hour: the finished file from the first run must
	// survive, and the new run must not silently overwrite it.
	venue := newFakeVenue(t, 1, true)
	dataDir := t.TempDir()

	runOnce := func() {
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
	}

	runOnce()
	firstPath := singleHourFile(t, filepath.Join(dataDir, "bbo"))
	firstBytes, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("read first hour: %v", err)
	}

	runOnce()

	// The second run opened the same hour bucket, so its lines are what the plain hour
	// file now holds. The first run's hour must still exist, under the `.replaced-`
	// name, byte for byte: a restart inside an hour must not be able to destroy it.
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

	entries, err := os.ReadDir(filepath.Join(dataDir, "bbo"))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var replaced []string
	for _, e := range entries {
		if strings.Contains(e.Name(), ReplacedMarker) {
			replaced = append(replaced, e.Name())
		}
	}
	if len(replaced) != 1 {
		t.Fatalf("want exactly one preserved hour, got %v", replaced)
	}
	preserved, err := os.ReadFile(filepath.Join(dataDir, "bbo", replaced[0]))
	if err != nil {
		t.Fatalf("read preserved hour: %v", err)
	}
	if !bytes.Equal(preserved, firstBytes) {
		t.Fatal("the first run's hour was altered when it was preserved")
	}
}
