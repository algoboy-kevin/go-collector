package hyperliquid

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// Stop reasons recorded in the manifest.
const (
	StopSignal        = "signal"
	StopDuration      = "duration"
	StopDiskLow       = "disk_low"
	StopWriteError    = "write_error"
	StopStartupFailed = "startup_failed"
)

const (
	// DefaultQuietAfter is how long a channel may receive nothing before the recorder
	// warns. It is a warning, never a stop: `bbo` and `trades` are event-driven, so
	// silence can be legitimate, and the ack count is the signal that actually
	// distinguishes "nothing happening" from "subscription refused".
	DefaultQuietAfter = 5 * time.Minute
	// DefaultAckWait is how long to wait for subscription acknowledgements before
	// reporting a shortfall. The venue acks within a second in practice.
	DefaultAckWait = 10 * time.Second
)

// ackPrefix identifies a subscription acknowledgement frame.
//
// This is a telemetry-only heuristic, not parsing: the venue sends compact JSON, so
// the ack frame begins with these bytes exactly, and no data frame can. If the venue
// ever reformatted its JSON this would read zero acks and warn — a false alarm, never
// a silent pass. Nothing in the capture depends on it; the authoritative signal for a
// refused subscription is the connector's HyperliquidErrorEvent.
var ackPrefix = []byte(`{"channel":"subscriptionResponse"`)

// RecorderConfig supplies the recorder's inputs and its injectable seams.
type RecorderConfig struct {
	// Config is required.
	Config *Config

	// Info is the /info client used for the startup dumps. Required.
	Info InfoClient

	// FreeBytes reports free space for the disk guard. Defaults to the platform
	// implementation; injected in tests so the guard can be exercised without filling
	// a disk.
	FreeBytes func(path string) (int64, error)

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time

	// QuietAfter overrides DefaultQuietAfter.
	QuietAfter time.Duration

	// AckWait overrides DefaultAckWait.
	AckWait time.Duration

	// DiskCheckEvery overrides how often free space is checked. Defaults to the flush
	// interval, floored at one second: at the measured ~46 KB/s a second of exposure is
	// a few hundred kilobytes, and a rotation cannot go unnoticed for longer than one
	// tick.
	DiskCheckEvery time.Duration

	// MetaOptions overrides DefaultMetaOptions.
	MetaOptions MetaOptions
}

// Recorder captures every frame the venue sends for the configured channels.
//
// One connection per channel is not an optimisation: because the recorder never parses
// a frame, it cannot decide where a frame belongs by looking at it. The destination
// has to be a property of the connection, so each connection carries exactly one
// channel and every frame arriving on it is that channel's.
type Recorder struct {
	cfg       *Config
	opts      RecorderConfig
	compress  int
	plans     []ChannelPlan
	conns     []*channelConn
	sinks     map[hl.Channel]*Sink
	startedAt time.Time

	startFree atomic.Int64
	endFree   atomic.Int64

	mu         sync.Mutex
	stopReason string
	runErr     error
	manifest   Manifest
}

// channelConn is one channel's connection: the connector manager, its sink, and the
// telemetry the manifest reports.
type channelConn struct {
	ch   hl.Channel
	sink *Sink
	ws   *hl.WSHyperliquid
	plan ChannelPlan

	acks    atomic.Int64
	largest atomic.Int64
	lastRx  atomic.Int64

	sidesMu sync.Mutex
	sides   map[string]int64

	errMu  sync.Mutex
	errors []string
}

// NewRecorder validates the configuration and builds the sinks, so a bad data
// directory fails here rather than after the first frame arrives.
func NewRecorder(opts RecorderConfig) (*Recorder, error) {
	if opts.Config == nil {
		return nil, errors.New("hyperliquid: recorder config is required")
	}
	if opts.Info == nil {
		return nil, errors.New("hyperliquid: an /info client is required")
	}
	cfg := opts.Config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	compression, err := cfg.CompressionLevel()
	if err != nil {
		return nil, err
	}
	plans, err := cfg.Plan()
	if err != nil {
		return nil, err
	}

	if opts.FreeBytes == nil {
		opts.FreeBytes = freeBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.QuietAfter <= 0 {
		opts.QuietAfter = DefaultQuietAfter
	}
	if opts.AckWait <= 0 {
		opts.AckWait = DefaultAckWait
	}
	if opts.MetaOptions.Attempts <= 0 {
		opts.MetaOptions = DefaultMetaOptions()
	}

	r := &Recorder{
		cfg:      cfg,
		opts:     opts,
		compress: compression,
		plans:    plans,
		sinks:    make(map[hl.Channel]*Sink, len(plans)),
	}

	// Sinks first: connections must have somewhere to put a frame the instant they
	// deliver one.
	for _, plan := range plans {
		sink, err := NewSink(cfg.SinkConfigFor(plan.Channel, compression))
		if err != nil {
			r.closeSinks()
			return nil, err
		}
		r.sinks[plan.Channel] = sink
	}
	return r, nil
}

// Config returns the validated configuration.
func (r *Recorder) Config() *Config { return r.cfg }

// Manifest returns the current manifest. After Run has returned it is the final copy.
func (r *Recorder) Manifest() Manifest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.manifest
}

// StopReason returns why the run ended, or "" while it is still running.
func (r *Recorder) StopReason() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopReason
}

// Err returns the error that ended the run, if any.
func (r *Recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runErr
}

// ─────────────────────────────────────────────────────────────
// Start
// ─────────────────────────────────────────────────────────────

// Start fetches the metadata dumps, writes the opening manifest, and brings up one
// connection per channel.
//
// Run calls it; call it directly only if you intend to drive the shutdown sequence
// yourself.
func (r *Recorder) Start(ctx context.Context) error {
	r.startedAt = r.opts.Now()

	// Free space before anything is written, for the manifest's before/after pair.
	r.startFree.Store(r.freeSpace())

	// meta.json before any frame: a capture with no instrument definitions is
	// unusable, and this is irreplaceable data rather than a nicety.
	if err := FetchMeta(ctx, r.opts.Info, r.cfg.DataDir, r.opts.MetaOptions); err != nil {
		return err
	}
	if r.cfg.Connector.RecordPerpDexs {
		// Best effort: only HIP-3 markets need it, so it must not fail the run.
		if err := FetchPerpDexs(ctx, r.opts.Info, r.cfg.DataDir, r.opts.MetaOptions); err != nil {
			slog.Warn("hyperliquid: perpDexs dump failed", "err", err)
		}
	}

	// The manifest is written now and rewritten at shutdown, so a capture that is
	// killed mid-run still says what it was trying to do.
	m := r.cfg.ManifestHeader(r.startedAt, r.compress)
	m.Recording.FreeBytesAtStart = r.startFree.Load()
	if err := WriteManifest(r.cfg.DataDir, m); err != nil {
		return err
	}
	r.mu.Lock()
	r.manifest = m
	r.mu.Unlock()

	opts := r.cfg.Options()
	for _, plan := range r.plans {
		cc := &channelConn{
			ch:    plan.Channel,
			sink:  r.sinks[plan.Channel],
			plan:  plan,
			sides: map[string]int64{},
		}

		// Each manager needs its own base: the dispatcher lives on the base, so a
		// shared one would have the channels overwrite each other's handler.
		base := connector.New(false, nil)
		cc.ws = hl.NewWithOptions(base, opts)
		cc.ws.SetRawFrameHandler(cc.onRawFrame)
		cc.ws.SetDispatcher(cc.onEvent)
		cc.ws.SetOnStatusChange(cc.onStatus)

		// Subscribe before dialling so that the first connect already has the
		// subscriptions registered. The manager re-sends the registry on every
		// successful dial, and ShouldConnect only redials while the registry is
		// non-empty — so registering first removes a window in which a drop would not
		// be retried.
		if err := cc.ws.Subscribe(ctx, plan.Subscriptions...); err != nil {
			r.stopConnections()
			return fmt.Errorf("hyperliquid: subscribe %s: %w", plan.Channel, err)
		}
		if err := cc.ws.Start(ctx, 0); err != nil {
			r.stopConnections()
			return fmt.Errorf("hyperliquid: connect %s: %w", plan.Channel, err)
		}
		r.conns = append(r.conns, cc)
		slog.Info("hyperliquid: channel up",
			"channel", plan.Channel,
			"subscriptions", len(plan.Subscriptions),
			"dir", cc.sink.Dir(),
		)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Run
// ─────────────────────────────────────────────────────────────

// Run starts the recorder, supervises it, and performs the shutdown sequence.
//
// It returns an error when the run did not end the way it was asked to (a failed
// start, a low disk, a write error) and nil when the caller's context ended it.
func (r *Recorder) Run(ctx context.Context) error {
	if err := r.Start(ctx); err != nil {
		r.setStop(StopStartupFailed, err)
		r.closeSinks()
		r.writeFinalManifest()
		return err
	}

	// Wait for context cancellation, or for the supervisor to decide the run must
	// end. Whatever the reason, the same sequence follows.
	runErr := r.supervise(ctx)

	// 1. Stop the connections first: after this nothing can enqueue a frame.
	r.stopConnections()

	// 2. Mark the end of the stream in each channel's own file. Written after the
	// connections stop so it is genuinely last, and before the sinks stop so it is
	// flushed like any other line.
	r.writeSentinel(EventStopped)

	// 3. Drain the queues, finish the gzip streams, fsync, and rename `.open` to the
	// final name. This is what turns a partial hour into a readable one.
	writeErr := r.closeSinks()

	// 4. Record the outcome in the capture itself.
	if runErr == nil {
		runErr = writeErr
	}
	if r.StopReason() == "" {
		r.setStop(stopReasonForContext(ctx), nil)
	}
	if runErr != nil {
		r.setRunErr(runErr)
	}
	r.writeFinalManifest()
	return runErr
}

// stopReasonForContext distinguishes a -duration deadline from a signal, so a capture
// records why it ended rather than lumping both under "signal".
func stopReasonForContext(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return StopDuration
	}
	return StopSignal
}

// supervise watches the disk, the channels, and the caller's context.
func (r *Recorder) supervise(ctx context.Context) error {
	diskEvery := r.opts.DiskCheckEvery
	if diskEvery <= 0 {
		diskEvery = r.cfg.FlushEvery()
		if diskEvery < time.Second {
			diskEvery = time.Second
		}
	}
	diskTick := time.NewTicker(diskEvery)
	defer diskTick.Stop()

	// Quiet-channel checks run far less often than the disk check: they are a warning
	// about a slow-moving condition, not a safety interlock.
	healthEvery := r.opts.QuietAfter / 4
	if healthEvery < time.Second {
		healthEvery = time.Second
	}
	healthTick := time.NewTicker(healthEvery)
	defer healthTick.Stop()

	ackDeadline := time.After(r.opts.AckWait)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ackDeadline:
			r.reportAcks()

		case <-diskTick.C:
			if err := r.checkDisk(); err != nil {
				slog.Error("hyperliquid: stopping, free space is below the floor",
					"dir", r.cfg.DataDir,
					"min_free_bytes", r.cfg.MinFree(),
					"err", err,
				)
				r.setStop(StopDiskLow, err)
				return err
			}
			if err := r.writeError(); err != nil {
				slog.Error("hyperliquid: stopping, the write path failed", "err", err)
				r.setStop(StopWriteError, err)
				return err
			}

		case <-healthTick.C:
			r.checkQuiet()
		}
	}
}

// reportAcks compares the subscriptions sent against the acknowledgements received.
//
// The venue acknowledges each subscription individually, so a shortfall is the most
// direct evidence available that something was refused — available within seconds of
// startup, rather than inferred from silence minutes later.
func (r *Recorder) reportAcks() {
	for _, cc := range r.conns {
		want := int64(len(cc.plan.Subscriptions))
		got := cc.acks.Load()
		switch {
		case got == want:
			slog.Info("hyperliquid: subscriptions acknowledged",
				"channel", cc.ch, "acked", got, "sent", want)
		case got < want:
			slog.Error("hyperliquid: subscriptions NOT fully acknowledged — check for refused coins",
				"channel", cc.ch, "acked", got, "sent", want,
				"sent_coins", subscriptionCoins(cc.plan))
		default:
			// More acks than subscriptions: either a reconnect re-sent them or the
			// heuristic matched something unexpected. Not a problem, worth knowing.
			slog.Info("hyperliquid: more acknowledgements than subscriptions (reconnect?)",
				"channel", cc.ch, "acked", got, "sent", want)
		}
	}
}

func subscriptionCoins(plan ChannelPlan) string {
	coins := make([]string, 0, len(plan.Subscriptions))
	for _, sub := range plan.Subscriptions {
		if sub.Coin == "" {
			coins = append(coins, "(global)")
			continue
		}
		coins = append(coins, sub.Coin)
	}
	if len(coins) == 0 {
		return "none"
	}
	return strings.Join(coins, ",")
}

// checkQuiet warns about a channel that has gone silent for longer than expected.
//
// It warns rather than stops because `bbo` and `trades` only fire on change, so
// silence is ambiguous — it means either "nothing is happening" or "the subscription
// died". The ack count and the frame counts together are what let a reader decide.
func (r *Recorder) checkQuiet() {
	cutoff := r.opts.Now().Add(-r.opts.QuietAfter).UnixNano()
	for _, cc := range r.conns {
		last := cc.lastRx.Load()
		if last == 0 {
			// Never received anything. Only worth a warning once enough time has
			// passed to be sure, so this is checked against the run's age.
			if r.opts.Now().Sub(r.startedAt) < r.opts.QuietAfter {
				continue
			}
			slog.Warn("hyperliquid: no frames at all on this channel",
				"channel", cc.ch, "acked", cc.acks.Load(), "sent", len(cc.plan.Subscriptions))
			continue
		}
		if last < cutoff {
			slog.Warn("hyperliquid: channel has been silent",
				"channel", cc.ch,
				"silent_for", r.opts.Now().Sub(time.Unix(0, last)).Round(time.Second),
				"frames", cc.sink.Frames(),
			)
		}
	}
}

// checkDisk reports an error when free space in the capture directory has fallen below
// the configured floor.
//
// A statfs failure is deliberately not fatal: losing the ability to *measure* the disk
// is not a reason to stop recording irreplaceable data. It is logged loudly instead.
func (r *Recorder) checkDisk() error {
	minFree := r.cfg.MinFree()
	if minFree <= 0 {
		return nil
	}
	free, err := r.opts.FreeBytes(r.cfg.DataDir)
	if err != nil {
		slog.Warn("hyperliquid: cannot read free space, disk guard inactive this tick",
			"dir", r.cfg.DataDir, "err", err)
		return nil
	}
	r.endFree.Store(free)
	if free < minFree {
		return fmt.Errorf("free space %d bytes is below the %d byte floor", free, minFree)
	}
	return nil
}

// writeError returns the first write-path failure across the channels.
func (r *Recorder) writeError() error {
	for _, cc := range r.conns {
		if err := cc.sink.Err(); err != nil {
			return fmt.Errorf("%s: %w", cc.ch, err)
		}
	}
	return nil
}

func (r *Recorder) freeSpace() int64 {
	free, err := r.opts.FreeBytes(r.cfg.DataDir)
	if err != nil {
		return 0
	}
	return free
}

// ─────────────────────────────────────────────────────────────
// Shutdown helpers
// ─────────────────────────────────────────────────────────────

// stopConnections closes every connection. Nothing can enqueue a frame afterwards.
func (r *Recorder) stopConnections() {
	for _, cc := range r.conns {
		cc.ws.Stop()
	}
}

// closeSinks drains and finishes every channel file. The first error is returned; all
// channels are still attempted, because one bad channel must not cost the others their
// rename.
func (r *Recorder) closeSinks() error {
	var first error
	for _, plan := range r.plans {
		sink, ok := r.sinks[plan.Channel]
		if !ok {
			continue
		}
		if err := sink.Stop(); err != nil && first == nil {
			first = fmt.Errorf("%s: %w", plan.Channel, err)
		}
	}
	return first
}

// writeSentinel appends one `_meta` line to every channel's file.
//
// A sentinel belongs to its own channel's file only: connections are independent, so a
// drop on `trades` is a hole in `trades` and nothing else.
func (r *Recorder) writeSentinel(event string) {
	now := r.opts.Now()
	for _, plan := range r.plans {
		sink, ok := r.sinks[plan.Channel]
		if !ok {
			continue
		}
		line, err := SentinelLine(event, now)
		if err != nil {
			slog.Error("hyperliquid: build sentinel", "event", event, "err", err)
			continue
		}
		if !sink.EnqueueSentinel(now, line) {
			slog.Error("hyperliquid: sentinel dropped", "channel", plan.Channel, "event", event)
		}
	}
}

// writeFinalManifest rewrites manifest.json with the outcome. Failure to write it is
// logged, not returned: it is the last thing that happens, and the capture's data is
// already on disk.
func (r *Recorder) writeFinalManifest() {
	m := r.Manifest()
	m.EndedNS = r.opts.Now().UnixNano()
	m.StopReason = r.StopReason()
	if err := r.Err(); err != nil {
		m.Error = err.Error()
	}
	if v := r.endFree.Load(); v > 0 {
		m.Recording.FreeBytesAtEnd = v
	}
	m.ChannelReports = r.channelReports()

	if err := WriteManifest(r.cfg.DataDir, m); err != nil {
		slog.Error("hyperliquid: rewrite manifest failed", "err", err)
		return
	}
	r.mu.Lock()
	r.manifest = m
	r.mu.Unlock()
}

// channelReports builds one report per channel, in configuration order.
func (r *Recorder) channelReports() []ChannelReport {
	byChannel := make(map[hl.Channel]*channelConn, len(r.conns))
	for _, cc := range r.conns {
		byChannel[cc.ch] = cc
	}

	out := make([]ChannelReport, 0, len(r.plans))
	for _, plan := range r.plans {
		rep := ChannelReport{
			Channel:       string(plan.Channel),
			Subscriptions: len(plan.Subscriptions),
		}
		sink, ok := r.sinks[plan.Channel]
		if !ok {
			out = append(out, rep)
			continue
		}
		rep.Frames = sink.Frames()
		rep.Sentinels = sink.Sentinels()
		rep.Drops = sink.Drops()
		rep.DroppedSentinels = sink.DroppedSentinels()
		rep.AfterStop = sink.AfterStop()
		rep.WriteErrors = sink.WriteErrors()
		rep.Bytes = sink.Bytes()
		rep.FilesOpened = sink.FilesOpened()
		rep.StaleFiles = sink.StaleFiles()
		rep.ReplacedHours = sink.ReplacedHours()
		rep.ClockRegressions = sink.ClockRegressions()
		rep.FramesWithNewline = sink.FramesContainingNewline()
		if err := sink.Err(); err != nil {
			rep.Error = err.Error()
		}
		if last := sink.LastFrame(); !last.IsZero() {
			rep.LastFrameNS = last.UnixNano()
		}

		if cc, ok := byChannel[plan.Channel]; ok {
			rep.Acks = cc.acks.Load()
			rep.LargestFrame = cc.largest.Load()
			cc.sidesMu.Lock()
			if len(cc.sides) > 0 {
				rep.TradeSides = make(map[string]int64, len(cc.sides))
				for k, v := range cc.sides {
					rep.TradeSides[k] = v
				}
			}
			cc.sidesMu.Unlock()
			cc.errMu.Lock()
			if len(cc.errors) > 0 && rep.Error == "" {
				rep.Error = cc.errors[0]
			}
			cc.errMu.Unlock()
		}
		out = append(out, rep)
	}
	return out
}

func (r *Recorder) setStop(reason string, err error) {
	r.mu.Lock()
	if r.stopReason == "" {
		r.stopReason = reason
	}
	if err != nil && r.runErr == nil {
		r.runErr = err
	}
	r.mu.Unlock()
}

func (r *Recorder) setRunErr(err error) {
	r.mu.Lock()
	if r.runErr == nil {
		r.runErr = err
	}
	r.mu.Unlock()
}

// ─────────────────────────────────────────────────────────────
// Connection hooks
//
// These run on the read-loop goroutine, so they do the minimum and return. Anything
// slow here throttles frame reads, which makes the venue drop the connection.
// ─────────────────────────────────────────────────────────────

// onRawFrame is the capture path: the frame goes to the queue exactly as received.
//
// The enqueue happens first and unconditionally. No counter below it may ever be able
// to suppress a frame — the counters are for describing the capture, never for
// deciding what goes into it.
func (c *channelConn) onRawFrame(frame []byte, rx time.Time) {
	c.sink.Enqueue(rx, frame)

	if bytes.HasPrefix(frame, ackPrefix) {
		c.acks.Add(1)
	}

	// Largest-frame tracking, for comparison against the read limit: a capture that
	// approaches it is one burst away from a dropped connection.
	n := int64(len(frame))
	for {
		cur := c.largest.Load()
		if n <= cur || c.largest.CompareAndSwap(cur, n) {
			break
		}
	}
	c.lastRx.Store(rx.UnixNano())
}

// onEvent consumes the connector's typed events. They are telemetry only: the capture
// is the raw bytes above, so a bug in this function can misreport but cannot corrupt.
func (c *channelConn) onEvent(ev any) {
	switch e := ev.(type) {
	case *connector.HyperliquidErrorEvent:
		// The one direct signal that the venue refused something, so it is recorded
		// rather than only logged.
		slog.Error("hyperliquid: venue error on channel", "channel", c.ch, "error", e.Message)
		c.errMu.Lock()
		if len(c.errors) < 8 {
			c.errors = append(c.errors, e.Message)
		}
		c.errMu.Unlock()

	case *connector.HyperliquidTradeEvent:
		c.sidesMu.Lock()
		c.sides[e.Side]++
		c.sidesMu.Unlock()
	}
}

// onStatus writes the connection sentinel into this channel's file.
//
// The connector fires this from inside OnConnect — after the resubscribe frames and
// before the read loop starts — so a `resubscribed` line is always enqueued before any
// frame from that connection can be. And a deliberate Stop suppresses the hook, so a
// `disconnected` line means an unexpected drop and nothing else.
func (c *channelConn) onStatus(st ws.ConnectionStatus) {
	now := time.Now()
	switch st {
	case ws.StatusConnected:
		c.enqueueSentinel(EventResubscribed, now)
	case ws.StatusDisconnected:
		c.enqueueSentinel(EventDisconnected, now)
	}
}

func (c *channelConn) enqueueSentinel(event string, now time.Time) {
	line, err := SentinelLine(event, now)
	if err != nil {
		slog.Error("hyperliquid: build sentinel", "channel", c.ch, "event", event, "err", err)
		return
	}
	if !c.sink.EnqueueSentinel(now, line) {
		slog.Error("hyperliquid: sentinel dropped", "channel", c.ch, "event", event)
	}
}
