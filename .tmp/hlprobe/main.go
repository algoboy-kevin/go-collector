// Command hlprobe is a throwaway live probe for the Hyperliquid raw recorder.
//
// It exists to settle PERP_COLLECTOR_SPEC.md §8 items 1–2 in the layout the recorder
// will actually use — **one connection per channel, every coin on each** — because the
// connector's own live capture (v0.7.1) was taken on a single multiplexed connection.
// It also measures the per-channel frame counts and frame sizes that decide droplet
// size (open questions #3–#5).
//
//	go run ./.tmp/hlprobe
//	go run ./.tmp/hlprobe -d 30s -coins BTC,ETH -channels l2Book,trades
//	go run ./.tmp/hlprobe -v            # one line per frame
//
// It reads public market data only and writes nothing to disk. Connector logs go to
// stderr; the report goes to stdout. Exit code is non-zero if any probed channel
// received no data, which is the whole point of the exercise.
//
// This is a dev tool, not a test: it needs the network and it talks to a live venue.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

func main() {
	dur := flag.Duration("d", 75*time.Second, "how long to watch each channel")
	coinsFlag := flag.String("coins", "BTC,ETH,SOL,HYPE,DOGE", "comma-separated coins")
	chansFlag := flag.String("channels", "l2Book,trades,bbo,activeAssetCtx", "comma-separated channels")
	fast := flag.Bool("fast", true, "l2Book fast=true (5 levels instead of 20)")
	appPing := flag.Duration("appping", 20*time.Second, "app-level {\"method\":\"ping\"} interval (0 = connector default 50s, negative disables)")
	wsPing := flag.Duration("wspping", 0, "WS control ping interval (0 = connector default 20s, negative disables)")
	verbose := flag.Bool("v", false, "print a line per frame (noisy)")
	flag.Parse()

	// Connector logs (connect/disconnect, venue errors) to stderr so the report on
	// stdout stays readable and greppable.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	coins := splitList(*coinsFlag)
	if len(coins) == 0 {
		fatal("-coins is empty")
	}
	for _, c := range coins {
		// Same validator the recorder will use: this is the injection surface.
		if err := hyperliquid.ValidateCoin(c); err != nil {
			fatal("%v", err)
		}
	}

	channels, err := parseChannels(splitList(*chansFlag))
	if err != nil {
		fatal("%v", err)
	}

	opts := hyperliquid.DefaultOptions()
	opts.AppPingIntervalMs = msOrZero(*appPing)
	opts.WSPingIntervalMs = msOrZero(*wsPing)

	fmt.Printf("hlprobe: %d connections (one per channel)\n", len(channels))
	fmt.Printf("  url       %s\n", opts.URL)
	fmt.Printf("  coins     %s\n", strings.Join(coins, ","))
	fmt.Printf("  channels  %s\n", strings.Join(chanNames(channels), ","))
	fmt.Printf("  duration  %s\n", *dur)
	fmt.Printf("  readlimit %d bytes\n", opts.ReadLimitBytes)
	fmt.Printf("  app ping  %s (0 in config = connector default %dms)\n",
		pingDesc(opts.AppPingIntervalMs), defaultAppPingMs)
	fmt.Printf("  ws  ping  %s (0 in config = connector default %dms)\n",
		pingDesc(opts.WSPingIntervalMs), defaultWSPingMs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr, "\nhlprobe: interrupted, stopping early")
		cancel()
	}()

	var probes []*probe
	for _, ch := range channels {
		p, err := start(ctx, ch, coins, *fast, opts, *verbose)
		if err != nil {
			// Clean up whatever already dialled, then fail loudly.
			for _, done := range probes {
				done.ws.Stop()
			}
			fatal("%v", err)
		}
		probes = append(probes, p)
	}

	fmt.Printf("\nwatching for %s...\n\n", *dur)
	select {
	case <-time.After(*dur):
	case <-ctx.Done():
	}

	// Stop in reverse so the first channel we reported on is the last to close.
	for i := len(probes) - 1; i >= 0; i-- {
		probes[i].ws.Stop()
	}

	if !report(probes, *dur) {
		os.Exit(1)
	}
}

// ─────────────────────────────────────────────────────────────
// One connection per channel
// ─────────────────────────────────────────────────────────────

type probe struct {
	ch  hyperliquid.Channel
	ws  *hyperliquid.WSHyperliquid
	st  *stats
	sub []hyperliquid.Subscription
}

func start(ctx context.Context, ch hyperliquid.Channel, coins []string, fast bool, opts hyperliquid.Options, verbose bool) (*probe, error) {
	params := hyperliquid.SubParams{}
	if ch == hyperliquid.ChannelL2Book && fast {
		params.Fast = true
	}

	// Exactly what the recorder will send: the library builds and validates the
	// frames, so any protocol mistake here is the library's, not a hand-rolled
	// string in this tool.
	subs, err := hyperliquid.BuildSubscriptions(ch, coins, params)
	if err != nil {
		return nil, fmt.Errorf("build subscriptions for %s: %w", ch, err)
	}
	if len(subs) == 0 {
		return nil, fmt.Errorf("channel %s produced no subscriptions", ch)
	}

	st := newStats(string(ch), verbose)
	for i, sub := range subs {
		frame, err := hyperliquid.SubscribeFrame(sub)
		if err != nil {
			return nil, fmt.Errorf("subscribe frame for %s: %w", sub.Key(), err)
		}
		if verbose || i < 2 {
			fmt.Printf("  %-15s send %s\n", ch, frame)
		}
	}

	// Each probe gets its own base: the dispatcher lives on the base, so sharing one
	// would make the four channels fight over it. The recorder does the same.
	base := connector.New(false, nil)
	h := hyperliquid.NewWithOptions(base, opts)
	h.SetRawFrameHandler(st.observeFrame)
	h.SetDispatcher(st.dispatch)
	h.SetOnStatusChange(st.onStatus)

	if err := h.Start(ctx, 0); err != nil {
		return nil, fmt.Errorf("start %s: %w", ch, err)
	}
	if err := h.Subscribe(ctx, subs...); err != nil {
		h.Stop()
		return nil, fmt.Errorf("subscribe %s: %w", ch, err)
	}
	return &probe{ch: ch, ws: h, st: st, sub: subs}, nil
}

// ─────────────────────────────────────────────────────────────
// Observation
// ─────────────────────────────────────────────────────────────

// envelope is the venue frame, classified only. The probe parses because it is allowed
// to; the recorder it validates never does.
type envelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type stats struct {
	name    string
	verbose bool

	mu        sync.Mutex
	total     int
	data      int
	acks      int
	pongs     int
	errFrames int
	unparsed  int
	other     map[string]int
	bytes     int64
	maxFrame  int
	firstData time.Time
	lastData  time.Time
	sides     map[string]int
	statuses  []string
}

func newStats(name string, verbose bool) *stats {
	return &stats{
		name:    name,
		verbose: verbose,
		other:   map[string]int{},
		sides:   map[string]int{},
	}
}

// observeFrame runs on the read-loop goroutine, so it takes one lock and returns.
func (s *stats) observeFrame(frame []byte, rx time.Time) {
	s.mu.Lock()
	s.total++
	n := len(frame)
	s.bytes += int64(n)
	if n > s.maxFrame {
		s.maxFrame = n
	}

	var env envelope
	if err := json.Unmarshal(frame, &env); err != nil {
		s.unparsed++
		s.mu.Unlock()
		s.note("UNPARSEABLE frame (%d bytes): %s", n, truncate(string(frame), 160))
		return
	}

	switch hyperliquid.Channel(env.Channel) {
	case hyperliquid.ServerChannelSubscriptionResponse:
		s.acks++
		s.mu.Unlock()
		s.trace("ack: %s", truncate(string(env.Data), 200))

	case hyperliquid.ServerChannelPong:
		s.pongs++
		s.mu.Unlock()
		s.note("pong — the app-level ping was answered")

	case hyperliquid.ServerChannelError:
		s.errFrames++
		s.mu.Unlock()
		s.note("VENUE ERROR FRAME: %s", truncate(string(env.Data), 200))

	default:
		if env.Channel != s.name {
			s.other[env.Channel]++
			s.mu.Unlock()
			s.note("UNEXPECTED channel %q on the %s connection: %s",
				env.Channel, s.name, truncate(string(frame), 160))
			return
		}
		s.data++
		first := s.firstData.IsZero()
		if first {
			s.firstData = rx
		}
		s.lastData = rx
		s.mu.Unlock()

		if first {
			s.note("FIRST DATA at %s (%d bytes): %s",
				rx.UTC().Format(time.RFC3339Nano), n, truncate(string(frame), 200))
		} else {
			s.trace("frame %d bytes", n)
		}
	}
}

// dispatch is the telemetry path. In the recorder this is where counters and
// distinct-value checks live; here it doubles as free coverage of §8 item 6, since the
// connector decodes trades whether or not anyone is listening.
func (s *stats) dispatch(ev any) {
	switch e := ev.(type) {
	case *connector.HyperliquidErrorEvent:
		s.note("venue error event: %s", e.Message)
	case *connector.HyperliquidTradeEvent:
		s.mu.Lock()
		s.sides[e.Side]++
		s.mu.Unlock()
	}
}

func (s *stats) onStatus(st ws.ConnectionStatus) {
	s.mu.Lock()
	s.statuses = append(s.statuses, fmt.Sprintf("%s@%s", st, time.Now().UTC().Format(time.RFC3339Nano)))
	s.mu.Unlock()
	s.note("status: %s", st)
}

// note prints an event worth seeing on every run.
func (s *stats) note(format string, args ...any) {
	fmt.Printf("[%-15s] %s\n", s.name, fmt.Sprintf(format, args...))
}

// trace prints per-frame detail, only with -v.
func (s *stats) trace(format string, args ...any) {
	if !s.verbose {
		return
	}
	s.note(format, args...)
}

// snap is a lock-free copy of the counters, safe to read and print once the mutex has
// been released. It exists as its own type (rather than a stats value) so no copy of a
// live stats ever drags the mutex along with it.
type snap struct {
	name      string
	total     int
	data      int
	acks      int
	pongs     int
	errFrames int
	unparsed  int
	other     map[string]int
	bytes     int64
	maxFrame  int
	firstData time.Time
	lastData  time.Time
	sides     map[string]int
	statuses  []string
}

func (s *stats) snapshot() snap {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := snap{
		name:      s.name,
		total:     s.total,
		data:      s.data,
		acks:      s.acks,
		pongs:     s.pongs,
		errFrames: s.errFrames,
		unparsed:  s.unparsed,
		bytes:     s.bytes,
		maxFrame:  s.maxFrame,
		firstData: s.firstData,
		lastData:  s.lastData,
		other:     make(map[string]int, len(s.other)),
		sides:     make(map[string]int, len(s.sides)),
		statuses:  append([]string(nil), s.statuses...),
	}
	for k, v := range s.other {
		cp.other[k] = v
	}
	for k, v := range s.sides {
		cp.sides[k] = v
	}
	return cp
}

// ─────────────────────────────────────────────────────────────
// Report
// ─────────────────────────────────────────────────────────────

// report prints the summary and returns false if any channel saw no data.
func report(probes []*probe, dur time.Duration) bool {
	secs := dur.Seconds()
	if secs <= 0 {
		secs = 1
	}

	fmt.Printf("\n%s\n", strings.Repeat("─", 100))
	fmt.Printf("%-15s %7s %6s %6s %5s %5s %11s %10s %9s %9s\n",
		"CHANNEL", "DATA", "ACK", "PONG", "ERR", "BAD", "MAX FRAME", "RAW MB/H", "DATA/s", "B/s")
	fmt.Printf("%s\n", strings.Repeat("─", 100))

	ok := true
	for _, p := range probes {
		s := p.st.snapshot()
		if s.data == 0 {
			ok = false
		}
		fmt.Printf("%-15s %7d %6d %6d %5d %5d %11d %10.2f %9.2f %9.0f\n",
			s.name, s.data, s.acks, s.pongs, s.errFrames, s.unparsed,
			s.maxFrame,
			float64(s.bytes)/secs*3600/1e6,
			float64(s.data)/secs,
			float64(s.bytes)/secs,
		)
	}

	// Extrapolated raw volume decides droplet size (§8 item 3); compression on top of
	// that is the recorder's measurement, not the probe's.
	var rawTotal float64
	for _, p := range probes {
		rawTotal += float64(p.st.snapshot().bytes)
	}
	fmt.Printf("%s\n", strings.Repeat("─", 100))
	fmt.Printf("raw total %.1f MB over %s  =>  %.0f MB/hour  (before compression)\n",
		rawTotal/1e6, dur, rawTotal/secs*3600/1e6)

	for _, p := range probes {
		s := p.st.snapshot()
		fmt.Printf("\n%s\n", s.name)

		switch {
		case s.data == 0:
			fmt.Printf("  ✗ NO DATA — subscription accepted but silent %s\n", verdictHint(s))
		case len(s.statuses) > 1:
			fmt.Printf("  ~ data flowing, but the connection changed state %d times: %v\n",
				len(s.statuses), s.statuses)
		default:
			fmt.Printf("  ✓ %d data frames, one clean connect, no state change\n", s.data)
		}
		if s.errFrames > 0 {
			fmt.Printf("  ✗ %d venue error frame(s) — a refused subscription\n", s.errFrames)
		}
		if s.unparsed > 0 {
			fmt.Printf("  ✗ %d unparseable frame(s)\n", s.unparsed)
		}
		if len(s.other) > 0 {
			keys := make([]string, 0, len(s.other))
			for k := range s.other {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("  ~ %d frame(s) for channel %q on this connection\n", s.other[k], k)
			}
		}
		if len(s.sides) > 0 {
			keys := make([]string, 0, len(s.sides))
			for k := range s.sides {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			fmt.Printf("  trade side values seen (RECORDER_SPEC.md §4 question):")
			for _, k := range keys {
				fmt.Printf("  %q=%d", k, s.sides[k])
			}
			fmt.Println()
		}
		if s.acks == 0 {
			fmt.Printf("  ~ no subscriptionResponse seen (venue may ack only in some modes)\n")
		}
		if s.pongs == 0 {
			fmt.Printf("  ~ no pong seen — app-level ping unanswered in this window\n")
		}
		if s.maxFrame > 0 {
			fmt.Printf("  largest frame %d bytes = %.1f%% of the 1 MiB read limit\n",
				s.maxFrame, float64(s.maxFrame)/(1<<20)*100)
		}
	}

	fmt.Println()
	if !ok {
		fmt.Println("RESULT: FAILED — at least one channel received no data")
		return false
	}
	fmt.Println("RESULT: OK — every probed channel delivered data on its own connection")
	return true
}

func verdictHint(s snap) string {
	if s.acks == 0 {
		return "(and no subscriptionResponse either — check the subscribe frame)"
	}
	if s.errFrames > 0 {
		return "(venue sent an error frame)"
	}
	return "(subscribed and acked, but nothing arrived)"
}

// ─────────────────────────────────────────────────────────────
// Flags and small helpers
// ─────────────────────────────────────────────────────────────

const (
	defaultAppPingMs = 50_000
	defaultWSPingMs  = 20_000
)

func parseChannels(names []string) ([]hyperliquid.Channel, error) {
	valid := map[string]hyperliquid.Channel{}
	for _, c := range hyperliquid.SubscribableChannels() {
		valid[string(c)] = c
	}
	var out []hyperliquid.Channel
	seen := map[hyperliquid.Channel]bool{}
	for _, n := range names {
		ch, ok := valid[n]
		if !ok {
			return nil, fmt.Errorf("unknown channel %q (subscribable: %s)",
				n, strings.Join(chanNames(hyperliquid.SubscribableChannels()), ", "))
		}
		if seen[ch] {
			return nil, fmt.Errorf("duplicate channel %q", n)
		}
		seen[ch] = true
		out = append(out, ch)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-channels is empty")
	}
	return out, nil
}

// chanNames renders channels as their wire names.
func chanNames(chs []hyperliquid.Channel) []string {
	out := make([]string, len(chs))
	for i, c := range chs {
		out[i] = string(c)
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func msOrZero(d time.Duration) int64 {
	if d == 0 {
		return 0 // zero means "use the connector default"
	}
	return d.Milliseconds()
}

func pingDesc(ms int64) string {
	switch {
	case ms == 0:
		return "default"
	case ms < 0:
		return "disabled"
	default:
		return (time.Duration(ms) * time.Millisecond).String()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "hlprobe: "+format+"\n", args...)
	os.Exit(2)
}
