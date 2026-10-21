package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// BookAnchorsChannel is the capture directory (and manifest channel name) for anchors.
const BookAnchorsChannel = "bookAnchors"

const (
	// DefaultAnchorDelay spaces the per-coin requests of one round, so a many-coin
	// round does not burst against the venue's /info request budget.
	DefaultAnchorDelay = 100 * time.Millisecond
	// anchorFetchTimeout bounds one REST call, so a hung request costs one anchor,
	// not the round or the shutdown.
	anchorFetchTimeout = 10 * time.Second
)

// Anchor round triggers, as recorded in the manifest.
const (
	AnchorStartup   = "startup"
	AnchorReconnect = "reconnect"
	AnchorPeriodic  = "periodic"
)

// anchorEnvelopePrefix / Suffix wrap the verbatim REST body into one line:
//
//	<rx_ns>\t{"channel":"bookAnchors","data":<verbatim body>}\n
//
// The shape mirrors the venue's own WebSocket frames (`{"channel":...,"data":...}`), so a
// reader that handles the other channels handles this one. The body is embedded as
// received; only surrounding whitespace is trimmed.
var (
	anchorEnvelopePrefix = []byte(`{"channel":"` + BookAnchorsChannel + `","data":`)
	anchorEnvelopeSuffix = []byte(`}`)
)

// BookAnchorsManifest is the anchor dumper's entry in the run manifest. The per-file
// line counts and sizes are the `bookAnchors` entry in channel_reports; this carries the
// settings and the REST-side accounting that a sink cannot see.
type BookAnchorsManifest struct {
	Enabled      bool     `json:"enabled"`
	IntervalSecs int      `json:"interval_secs"`
	Coins        []string `json:"coins"`
	// Rounds counts anchor rounds by trigger (startup, reconnect, periodic).
	Rounds map[string]int64 `json:"rounds,omitempty"`
	// Fetches counts REST calls attempted; Failures those that errored or returned a
	// body that cannot be put on one line. Fetches - Failures anchors were enqueued.
	Fetches   int64  `json:"fetches"`
	Failures  int64  `json:"failures"`
	LastError string `json:"last_error,omitempty"`
}

// bookAnchorer takes REST l2Book snapshots and enqueues them on its own sink.
type bookAnchorer struct {
	info     InfoClient
	sink     *Sink
	coins    []string
	interval time.Duration
	delay    time.Duration
	now      func() time.Time
	// tick overrides the periodic ticker (tests). nil selects a real ticker.
	tick <-chan time.Time

	trigger chan string // reconnect requests, coalesced (cap 1)

	cancel context.CancelFunc
	wg     sync.WaitGroup

	fetches  atomic.Int64
	failures atomic.Int64
	largest  atomic.Int64

	mu        sync.Mutex
	rounds    map[string]int64
	lastError string
}

func newBookAnchorer(info InfoClient, sink *Sink, coins []string, interval, delay time.Duration,
	now func() time.Time, tick <-chan time.Time) *bookAnchorer {
	if delay <= 0 {
		delay = DefaultAnchorDelay
	}
	return &bookAnchorer{
		info: info, sink: sink, coins: coins, interval: interval, delay: delay,
		now: now, tick: tick,
		trigger: make(chan string, 1),
		rounds:  map[string]int64{},
	}
}

// Request asks for a round after a reconnect. It never blocks: if one is already
// pending the request is coalesced into it. Safe to call from a connection hook.
func (a *bookAnchorer) Request(reason string) {
	select {
	case a.trigger <- reason:
	default:
	}
}

// Start launches the anchor loop: the startup round runs INSIDE it and must never
// delay the WS connections. At dataset scale (100+ coins) a synchronous startup round
// would push the first stream frames out by tens of seconds — the capture comes first,
// anchors are a bonus on top of it. Ordering before the first WS frame is best-effort
// and not load-bearing: the stream is self-anchoring (every l2Book frame is a full
// top-N snapshot), so what matters is that the round happens, not that it goes first.
func (a *bookAnchorer) Start(ctx context.Context) {
	ctx, a.cancel = context.WithCancel(ctx)
	a.wg.Add(1)
	go a.loop(ctx)
}

// Stop ends the loop and waits for any in-flight round, so nothing can enqueue onto
// the sink after the recorder starts closing it. Idempotent.
func (a *bookAnchorer) Stop() {
	if a.cancel != nil {
		a.cancel()
	}
	a.wg.Wait()
}

func (a *bookAnchorer) loop(ctx context.Context) {
	defer a.wg.Done()

	a.round(ctx, AnchorStartup)
	tick := a.tick
	if tick == nil && a.interval > 0 {
		t := time.NewTicker(a.interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case reason := <-a.trigger:
			a.round(ctx, reason)
		case <-tick:
			a.round(ctx, AnchorPeriodic)
		}
	}
}

// round fetches every coin once. A failure skips that coin and the round continues:
// an anchor is a bonus on top of the capture and must never be able to stop it.
func (a *bookAnchorer) round(ctx context.Context, reason string) {
	a.mu.Lock()
	a.rounds[reason]++
	a.mu.Unlock()

	for i, coin := range a.coins {
		if ctx.Err() != nil {
			return
		}
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(a.delay):
			}
		}
		if err := a.fetchOne(ctx, coin); err != nil {
			a.failures.Add(1)
			a.mu.Lock()
			a.lastError = fmt.Sprintf("%s: %v", coin, err)
			a.mu.Unlock()
			slog.Warn("hyperliquid: book anchor skipped", "coin", coin, "trigger", reason, "err", err)
		}
	}
}

func (a *bookAnchorer) fetchOne(ctx context.Context, coin string) error {
	a.fetches.Add(1)
	cctx, cancel := context.WithTimeout(ctx, anchorFetchTimeout)
	defer cancel()

	// rx is taken as the request returns: it is the instant the body was in hand.
	body, err := a.info.Post(cctx, map[string]any{"type": "l2Book", "coin": coin})
	rx := a.now()
	if err != nil {
		return err
	}
	line, err := anchorLine(body)
	if err != nil {
		return err
	}
	if n := int64(len(body)); n > a.largest.Load() {
		a.largest.Store(n)
	}
	if !a.sink.Enqueue(rx, line) {
		// The sink counts the drop in its own report; nothing more to do here.
		return fmt.Errorf("anchor queue full or closed, anchor dropped")
	}
	return nil
}

// anchorLine wraps a REST body in the envelope. The body must be a single line of valid
// JSON, because the file format is one record per line: a body that is not is refused
// (and counted) rather than written in a way that would corrupt its neighbours.
func anchorLine(body []byte) ([]byte, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	if bytes.ContainsAny(body, "\r\n") {
		return nil, fmt.Errorf("body contains a newline")
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("body is not valid JSON")
	}
	line := make([]byte, 0, len(anchorEnvelopePrefix)+len(body)+len(anchorEnvelopeSuffix))
	line = append(line, anchorEnvelopePrefix...)
	line = append(line, body...)
	line = append(line, anchorEnvelopeSuffix...)
	return line, nil
}

func (a *bookAnchorer) manifest(cfg BookAnchorsConfig) *BookAnchorsManifest {
	a.mu.Lock()
	defer a.mu.Unlock()
	m := &BookAnchorsManifest{
		Enabled:      true,
		IntervalSecs: cfg.IntervalSecs,
		Coins:        append([]string(nil), a.coins...),
		Fetches:      a.fetches.Load(),
		Failures:     a.failures.Load(),
		LastError:    a.lastError,
	}
	if len(a.rounds) > 0 {
		m.Rounds = make(map[string]int64, len(a.rounds))
		for k, v := range a.rounds {
			m.Rounds[k] = v
		}
	}
	return m
}

// anchorCoins resolves the coins to anchor: the configured subset (or all) intersected
// with the coins actually recorded, so a skipped/overridden coin is never fetched.
func anchorCoins(cfg BookAnchorsConfig, recorded []string) []string {
	if len(cfg.Coins) == 0 {
		return append([]string(nil), recorded...)
	}
	rec := make(map[string]bool, len(recorded))
	for _, c := range recorded {
		rec[c] = true
	}
	var out []string
	for _, c := range cfg.Coins {
		if rec[c] {
			out = append(out, c)
		} else {
			slog.Warn("hyperliquid: book_anchors coin is not being recorded, ignored", "coin", c)
		}
	}
	return out
}
