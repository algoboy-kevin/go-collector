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
	// StopConnectionFlap means the recorder gave up on a channel that kept reconnecting
	// without ever delivering a frame. The connector now backs such a connection off to
	// its 30s ceiling, so this is no longer about protecting the venue's connection
	// budget — it is the decision the backoff deliberately never makes: give up rather
	// than reconnect politely forever.
	StopConnectionFlap = "connection_flap"
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
	// DefaultFlapThreshold is how many reconnects a channel may make without receiving a
	// single frame before the recorder stops.
	//
	// A dial that *succeeds* and is then closed by the venue — a refused subscription, an
	// IP over its connection limit, a policy close — used to reset the connector's
	// reconnect backoff, so it retried at the base interval forever: measured at 719
	// connections in three minutes, straight through the venue's per-IP limit, while
	// capturing nothing. go-exchange-connector v0.7.2 fixed that storm itself (a
	// connection that dies inside MinStableConnectionMs now escalates the backoff instead
	// of resetting it), so the venue's budget is no longer what this guards. What is left
	// is the decision a backoff never makes: with a 30s ceiling the loop would reconnect
	// politely forever on a channel that has *never* produced a frame, and a capture that
	// will never contain anything is worth ending rather than keeping alive.
	DefaultFlapThreshold = 15
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

	// FlapThreshold overrides DefaultFlapThreshold.
	FlapThreshold int64
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
	// runID is the run's identity, stamped into every manifest and every `_meta` line.
	runID string

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
	// runID is the owning run's identity, stamped into this channel's sentinels.
	runID string

	acks    atomic.Int64
	largest atomic.Int64
	lastRx  atomic.Int64

	// connects counts every successful dial, including reconnects. A healthy channel
	// stays at 1 for the whole run.
	connects atomic.Int64
	// connectsSinceProgress counts reconnects since the channel last received a frame,
	// and is what the flap detector watches.
	connectsSinceProgress atomic.Int64
	framesAtLastCheck     atomic.Int64

	sidesMu sync.Mutex
	sides   map[string]int64

	errMu  sync.Mutex
	errors []string

	// disconnectMu guards lastDisconnect: the reason for the most recent unexpected
	// drop, exactly as the connector reported it. onDisconnect sets it immediately
	// before onStatus writes the `disconnected` sentinel — the connector invokes the
	// disconnect hook before the status callback, on the same goroutine — so the
	// sentinel and the manifest always agree.
	disconnectMu   sync.Mutex
	lastDisconnect string
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
	if opts.FlapThreshold <= 0 {
		opts.FlapThreshold = DefaultFlapThreshold
	}

	r := &Recorder{
		cfg:      cfg,
		opts:     opts,
		compress: compression,
		plans:    plans,
		sinks:    make(map[hl.Channel]*Sink, len(plans)),
	}

	// The run's identity is fixed here, at construction, not in Start: the sinks need it
	// when they are built, and they are built next. cmd/hlrecorder constructs and starts
	// in one breath, so the difference is microseconds — and the alternative, deriving it
	// twice, could straddle a second boundary and produce two ids for one run.
	r.startedAt = r.opts.Now()
	r.runID = uniqueRunID(cfg.DataDir, RunID(r.startedAt))

	// Sinks first: connections must have somewhere to put a frame the instant they
	// deliver one.
	for _, plan := range plans {
		sinkCfg := cfg.SinkConfigFor(plan.Channel, compression)
		// The sink stamps this into the `dropped`/`write_error` marks it writes itself,
		// so those lines identify their run like every other sentinel.
		sinkCfg.RunID = r.runID
		sink, err := NewSink(sinkCfg)
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
	// runID and startedAt were fixed at construction (see NewRecorder); the sinks already
	// carry this run's identity in the marks they write themselves.
	// Free space before anything is written, for the manifest's before/after pair.
	r.startFree.Store(r.freeSpace())

	// Instrument metadata before any frame, and the configured coins checked against
	// the venue's own universe. A capture with no instrument definitions is unusable,
	// and a coin the venue does not know is not an error it reports — it closes the
	// connection, which without this check becomes a reconnect storm that captures
	// nothing.
	dumps, err := FetchStartupMeta(ctx, r.opts.Info, r.cfg.DataDir, r.cfg.Coins, r.opts.MetaOptions)
	if err != nil {
		return err
	}
	slog.Info("hyperliquid: instrument metadata captured",
		"default_assets", dumps.DefaultAssets,
		"hip3_dexes", len(dumps.Dexes),
		"detail", dumps.Describe(),
	)
	if r.cfg.Connector.RecordPerpDexs {
		// Best effort: the dex *list* is informational, and only some consumers need it.
		// The per-dex universes above are not optional, and are already written.
		if err := FetchPerpDexs(ctx, r.opts.Info, r.cfg.DataDir, r.opts.MetaOptions); err != nil {
			slog.Warn("hyperliquid: perpDexs dump failed", "err", err)
		}
	}

	// Narrow the coin list to what will actually be recorded. The venue keeps a delisted
	// market in its universe and still accepts the subscription, so this is not a safety
	// check — it is a decision, and it has to happen here because it needs the universe,
	// which needs a network call. The meta dumps above are deliberately NOT narrowed with
	// it: a skipped coin's instrument definition stays in the capture, so the decision is
	// reversible from the manifest rather than from a re-fetch.
	coins, skipped, err := ApplyDelistedPolicy(r.cfg.Coins, dumps.Universes, r.cfg.DelistedPolicyOrDefault())
	if err != nil {
		return err
	}
	if len(coins) == 0 {
		return fmt.Errorf("hyperliquid: every configured coin was skipped (%s); nothing to record", r.cfg.DelistedPolicyOrDefault())
	}
	if len(coins) != len(r.cfg.Coins) {
		// Rebuild the subscriptions: the frames on the wire must match the coins whose
		// frames we intend to keep.
		plans, perr := r.cfg.PlanFor(coins)
		if perr != nil {
			return perr
		}
		r.plans = plans
	}

	// What decides the instrument set, digested. Compared against earlier runs so two
	// experiments cannot silently share a directory — see checkUniverseChange.
	fingerprint := UniverseFingerprint(coins, r.cfg.Channels, l2Params(r.cfg), dumps.Metas)
	if err := r.checkUniverseChange(fingerprint); err != nil {
		return err
	}

	// The manifest is written now and rewritten at shutdown, so a capture that is
	// killed mid-run still says what it was trying to do.
	m := r.cfg.ManifestHeader(r.startedAt, r.runID, r.compress)
	m.Recording.FreeBytesAtStart = r.startFree.Load()
	m.Hip3Dexes = dumps.Dexes
	m.Coins = CoinRefsFor(coins)
	m.CoinsSkipped = skipped
	m.UniverseFingerprint = fingerprint
	m.MetaSHA256 = dumps.MetaSHA256
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
			runID: r.runID,
			sides: map[string]int64{},
		}

		// Each manager needs its own base: the dispatcher lives on the base, so a
		// shared one would have the channels overwrite each other's handler.
		base := connector.New(false, nil)
		cc.ws = hl.NewWithOptions(base, opts)
		cc.ws.SetRawFrameHandler(cc.onRawFrame)
		cc.ws.SetDispatcher(cc.onEvent)
		cc.ws.SetOnStatusChange(cc.onStatus)
		// SetOnDisconnect carries WHY the connection dropped; SetOnStatusChange reports
		// only *that* it did. The connector calls the disconnect hook first, so the
		// reason is in place before onStatus writes the `disconnected` sentinel.
		cc.ws.SetOnDisconnect(cc.onDisconnect)

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
	r.writeSentinel(Sentinel{Event: EventStopped})

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
			if err := r.checkFlap(); err != nil {
				slog.Error("hyperliquid: stopping, a channel keeps reconnecting without delivering frames",
					"err", err,
				)
				r.setStop(StopConnectionFlap, err)
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

// checkUniverseChange refuses to record into a capture root whose earlier runs describe
// different instruments.
//
// A consumer builds ONE instrument set, and one price precision per coin, from a whole
// directory. Two universes in one directory does not raise an error — it produces a
// capture that looks complete and backtests at the wrong prices, because the precision
// derived from one session's prices is applied to the other's records.
//
// The comparison is against the NEWEST run that recorded a fingerprint. Runs from before
// the field existed, and runs that died before writing one, are skipped rather than read
// as a mismatch: an absent fingerprint is not evidence of a different universe.
func (r *Recorder) checkUniverseChange(fingerprint string) error {
	if r.cfg.OnUniverseChangeOrDefault() == UniverseChangeAllow {
		slog.Warn("hyperliquid: on_universe_change is allow; earlier runs are not checked",
			"dir", r.cfg.DataDir)
		return nil
	}

	existing, err := ReadManifests(r.cfg.DataDir)
	if err != nil {
		// Fail open, deliberately. An unreadable *historical* manifest must not stop a
		// recording run: blocking would lose data that cannot be refetched, whereas the
		// contamination risk this guards against is visible in the manifest this run writes
		// and in the warning below.
		slog.Warn("hyperliquid: cannot read earlier manifests, universe not checked",
			"dir", r.cfg.DataDir, "err", err)
		return nil
	}

	for i := len(existing) - 1; i >= 0; i-- {
		prev := existing[i]
		if prev.UniverseFingerprint == "" {
			continue
		}
		if prev.UniverseFingerprint == fingerprint {
			slog.Info("hyperliquid: continuing an existing capture",
				"dir", r.cfg.DataDir,
				"first_run", existing[0].RunID,
				"previous_run", prev.RunID,
				"runs", len(existing))
			return nil
		}
		return fmt.Errorf(
			"hyperliquid: %s already holds a capture with different instruments "+
				"(run %s, %s; this run is %s). Recording both into one directory would blend "+
				"two universes, and a consumer derives one instrument set and one price "+
				"precision per coin from the whole directory — so the result backtests at the "+
				"wrong prices without erroring. Use a sibling root for the new universe "+
				"(e.g. %s-<label>), or set on_universe_change: allow if the merge is deliberate",
			r.cfg.DataDir, prev.RunID, prev.UniverseFingerprint, fingerprint, r.cfg.DataDir)
	}
	return nil
}

// checkFlap stops the run when a channel has reconnected repeatedly without ever
// receiving a frame.
//
// This is a behavioural guard, not a diagnosis: the recorder cannot tell a refused
// subscription from a network path that drops anything long-lived, so it counts
// reconnects-since-the-last-frame and treats a channel that has never produced one as
// not going to. The connector's own backoff (v0.7.2, MinStableConnectionMs) already
// stops such a connection from being retried at the base interval forever, so the
// failure this prevents is no longer a storm but a permanently empty capture.
//
// The reason the connector reports for the most recent drop is included in the error
// when there is one, so the usual suspects (a coin that is not in the dex's universe, an
// IP over its connection limit, a refused subscription) are named rather than guessed.
//
// The threshold counts reconnects *since the last frame*, so a channel that worked for
// hours and then hit a network blip is not punished for it.
func (r *Recorder) checkFlap() error {
	for _, cc := range r.conns {
		frames := cc.sink.Frames()
		if frames > cc.framesAtLastCheck.Load() {
			cc.framesAtLastCheck.Store(frames)
			cc.connectsSinceProgress.Store(0)
		}
		if n := cc.connectsSinceProgress.Load(); n >= r.opts.FlapThreshold {
			msg := fmt.Sprintf("channel %s reconnected %d times without receiving a frame "+
				"(largest frame seen: %d bytes, acks: %d of %d)",
				cc.ch, n, cc.largest.Load(), cc.acks.Load(), len(cc.plan.Subscriptions))
			if reason := cc.lastDisconnectReason(); reason != "" {
				msg += "; the venue said: " + reason
			} else {
				msg += " — check the coins against the venue's universe, or whether this IP " +
					"is over its connection limit"
			}
			return errors.New(msg)
		}
	}
	return nil
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
func (r *Recorder) writeSentinel(s Sentinel) {
	s.RunID = r.runID
	now := r.opts.Now()
	for _, plan := range r.plans {
		sink, ok := r.sinks[plan.Channel]
		if !ok {
			continue
		}
		line, err := SentinelLine(s, now)
		if err != nil {
			slog.Error("hyperliquid: build sentinel", "event", s.Event, "err", err)
			continue
		}
		if !sink.EnqueueSentinel(now, line) {
			slog.Error("hyperliquid: sentinel dropped", "channel", plan.Channel, "event", s.Event)
		}
	}
}

// writeFinalManifest rewrites manifest.json with the outcome. Failure to write it is
// logged, not returned: it is the last thing that happens, and the capture's data is
// already on disk.
func (r *Recorder) writeFinalManifest() {
	m := r.Manifest()
	if m.RunID == "" {
		// A run that died before the opening manifest was written still has to leave a
		// record: a failed startup is exactly the case where "what was it even trying to
		// do" matters, and the header costs nothing.
		m = r.cfg.ManifestHeader(r.startedAt, r.runID, r.compress)
	}
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
		rep.ClockRegressionSamples = sink.ClockRegressionSamples()
		rep.FramesWithNewline = sink.FramesContainingNewline()
		rep.Files = sink.Files()
		rep.Quarantined = sink.Quarantined()
		if err := sink.Err(); err != nil {
			rep.Error = err.Error()
		}
		if first := sink.FirstFrame(); !first.IsZero() {
			rep.FirstFrameNS = first.UnixNano()
		}
		if last := sink.LastFrame(); !last.IsZero() {
			rep.LastFrameNS = last.UnixNano()
		}
		rep.MaxGapMS = sink.MaxGap().Milliseconds()

		if cc, ok := byChannel[plan.Channel]; ok {
			rep.Acks = cc.acks.Load()
			rep.Reconnects = cc.connects.Load() - 1 // the first dial is not a reconnect
			rep.ConnectAttempts = cc.connects.Load()
			rep.LargestFrame = cc.largest.Load()
			rep.LastDisconnect = cc.lastDisconnectReason()
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
// before the read loop starts — so a `subscribed`/`resubscribed` line is always enqueued
// before any frame from that connection can be. And a deliberate Stop suppresses the
// hook, so a `disconnected` line means an unexpected drop and nothing else.
//
// The FIRST connect emits `subscribed`, not `resubscribed`. A consumer counting
// `resubscribed` as "this channel reconnected" is then correct without special-casing the
// first connect — which is exactly what the 2h capture got wrong: `reconnects: 0` in the
// manifest, but one `resubscribed` per channel for the ingest to count.
func (c *channelConn) onStatus(st ws.ConnectionStatus) {
	now := time.Now()
	switch st {
	case ws.StatusConnected:
		if n := c.connects.Add(1); n > 1 {
			// A reconnect. Counted until a frame arrives, so the flap detector can tell
			// "this connection is being refused" from "this market is quiet".
			c.connectsSinceProgress.Add(1)
			c.enqueueSentinel(Sentinel{Event: EventResubscribed}, now)
		} else {
			c.enqueueSentinel(Sentinel{Event: EventSubscribed}, now)
		}
	case ws.StatusDisconnected:
		// onDisconnect ran first, so the reason is the one for this drop.
		c.enqueueSentinel(Sentinel{Event: EventDisconnected, Reason: c.lastDisconnectReason()}, now)
	}
}

// onDisconnect records why a connection dropped.
//
// The connector invokes this immediately before the status callback, so the reason is
// already in place when onStatus writes the `disconnected` sentinel: the channel's file
// and the manifest carry the same words. SetOnStatusChange alone cannot do this — it
// reports only *that* the connection dropped, never why (that gap was
// PERP_COLLECTOR_SPEC.md §8 item 8, closed by go-exchange-connector v0.7.2).
//
// The reason is logged as well, because a log is what a live operator reads; but the
// capture has to stand on its own, since the log is gone by the time anyone asks.
func (c *channelConn) onDisconnect(err error) {
	reason := DisconnectReason(err)
	c.setLastDisconnect(reason)
	if reason == "" {
		// A deliberate shutdown suppresses this path, so an unexplained drop is rare:
		// the socket went away without the venue saying anything.
		slog.Warn("hyperliquid: channel disconnected", "channel", c.ch, "reason", "unreported")
		return
	}
	slog.Warn("hyperliquid: channel disconnected", "channel", c.ch, "reason", reason)
}

// setLastDisconnect records the reason for the most recent unexpected drop.
func (c *channelConn) setLastDisconnect(reason string) {
	c.disconnectMu.Lock()
	c.lastDisconnect = reason
	c.disconnectMu.Unlock()
}

// lastDisconnectReason returns the most recent drop's reason, or "" if nothing has
// dropped unexpectedly.
func (c *channelConn) lastDisconnectReason() string {
	c.disconnectMu.Lock()
	defer c.disconnectMu.Unlock()
	return c.lastDisconnect
}

func (c *channelConn) enqueueSentinel(s Sentinel, now time.Time) {
	s.RunID = c.runID
	line, err := SentinelLine(s, now)
	if err != nil {
		slog.Error("hyperliquid: build sentinel", "channel", c.ch, "event", s.Event, "err", err)
		return
	}
	if !c.sink.EnqueueSentinel(now, line) {
		slog.Error("hyperliquid: sentinel dropped", "channel", c.ch, "event", s.Event)
	}
}
