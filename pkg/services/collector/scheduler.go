package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Discoverer is the slice of the Polymarket connector the epoch scheduler uses.
// It is an interface so the scheduler can be driven from a stub in tests.
type Discoverer interface {
	GetSeries(ctx context.Context, slug string) ([]connector.GammaSeries, error)
	ListSeriesEvents(ctx context.Context, q connector.EventQuery) ([]connector.GammaEvent, error)
}

// seriesInvalidator is implemented by connectors that cache series lookups, so
// the scheduler can force a re-resolve after a failed listing.
type seriesInvalidator interface {
	InvalidateSeries(slug string)
}

// Errors SelectEvents distinguishes.
var (
	// ErrNoEventForSettle means the series has no event settling at the target
	// instant — normal right after a window opens, or for a market that has not
	// been created yet.
	ErrNoEventForSettle = errors.New("collector: no event settles at the target instant")
)

// settleMatchTolerance is how far an event's EndDate may differ from the target
// settle instant and still count as a match. Gamma publishes endDate at second
// precision, so the tolerance absorbs sub-second representation noise while
// staying far below the smallest interval recorded (1h) — a neighbouring window
// can never be matched by accident.
const settleMatchTolerance = time.Second

// missingMarketWarnWindow is how close to its settle instant a target must be
// before "no event settles there" is logged as a warning rather than debug.
//
// Past this window the family's live market should exist: a gap here means we
// are not recording a market that is about to settle, which is exactly the
// signal a DST-grid mismatch (or a Gamma outage) produces. Further out the
// absence is ordinary — a target may be a window that has not been published
// yet.
const missingMarketWarnWindow = 5 * time.Minute

// SelectEvents returns every open event that settles at settleAt, ordered by
// slug.
//
// Normally that is exactly one. On a DST transition day it can be two — and can
// be none — because Polymarket's own generator works on the ET wall clock:
//
//	2026-03-08 (US clocks jump 02:00→03:00 EST): the hourly series carried BOTH
//	"…-12am-et" and "…-1am-et" ending at 06:00Z, the local 01:00 hour that never
//	happened. Verified against Gamma 2026-10-02.
//	2025-11-02 (US clocks fall back): the local 01:00–02:00 hour occurs twice and
//	the hourly series carried NEITHER — a two hour hole.
//
// Both are real markets settling on the same candle, so all of them are
// recorded. Returning the one the generator "meant" would be a guess, and
// aborting the run over an artefact the calendar creates would cost the whole
// recording. Matching is on EndDate — the settlement instant — and never on
// "does the event's window contain now": intraday events open roughly two days
// early, so dozens of events contain now at any moment (47 hourly events did
// when this was verified on 2026-10-02) and containment silently records the
// wrong market. See CONTEXT.md §3.
func SelectEvents(events []connector.GammaEvent, settleAt time.Time) ([]connector.GammaEvent, error) {
	var matches []connector.GammaEvent
	for _, ev := range events {
		if ev.EndDate.IsZero() {
			continue
		}
		if d := ev.EndDate.Sub(settleAt); d <= settleMatchTolerance && d >= -settleMatchTolerance {
			matches = append(matches, ev)
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("%w: %s (of %d open events)",
			ErrNoEventForSettle, settleAt.UTC().Format(time.RFC3339), len(events))
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Slug < matches[j].Slug })
	return matches, nil
}

// SeriesSelection is one configured series plus its ladder filter.
type SeriesSelection struct {
	Spec        libs.SeriesSpec
	StrikeLimit int
}

// SchedulerConfig configures the daily-epoch scheduler.
type SchedulerConfig struct {
	// Recording is the `recording` block: how many epochs to record, the epoch
	// anchor, and the ladder offset.
	Recording RecordingConfig
	// Series is the `series` block: which families to discover and record.
	Series []SeriesConfig
	// PollInterval caps how long the scheduler goes without reconciling, so a
	// market created late (or a discovery call that failed) is retried even when
	// no boundary is near. Default 30s.
	PollInterval time.Duration
}

// defaultPollInterval is the reconcile retry floor.
const defaultPollInterval = 30 * time.Second

// boundaryEpsilon is how far past a boundary the scheduler wakes.
//
// TargetSettle is computed from time.Now(), so waking a hair early would resolve
// the window that is *ending* rather than the one starting. Claiming the old
// window again is a no-op (discovery is idempotent), but the new window would
// then wait a whole poll interval to be claimed, losing the open of every
// window.
const boundaryEpsilon = 500 * time.Millisecond

// seriesRef is a cached Gamma series identity.
type seriesRef struct {
	id         string
	recurrence string
}

// EpochScheduler records a rolling set of market families on a daily epoch.
//
// Each reconcile tick derives, per family, the settlement instant the family's
// live market has, discovers the Gamma event that settles exactly then, and
// fans it out to one recording session per market. Nothing is synthesized: no
// slug is ever constructed locally, so the collector cannot drift onto the
// wrong market.
//
// The epoch itself is what every recording is keyed to: a market is filed under
// the epoch its settlement falls in (libs.EpochForSettle), and feed buckets are
// cut on the same boundary (CONTEXT.md §2/§5).
type EpochScheduler struct {
	ec   *EventCollector
	disc Discoverer
	cfg  SchedulerConfig
	sel  []SeriesSelection

	anchor libs.EpochAnchor
	index  *epochIndex

	mu          sync.Mutex
	seriesIDs   map[string]seriesRef
	claimed     map[string]bool
	warned      map[string]bool
	activeEpoch string
	epochs      int
	done        bool
}

// NewEpochScheduler resolves the configured series through the registry and
// returns a scheduler ready to run.
//
// Every series must be a registry row, because the registry — not the config —
// holds the verified Gamma slug, settlement anchor and resolution rule.
func NewEpochScheduler(ec *EventCollector, disc Discoverer, cfg SchedulerConfig) (*EpochScheduler, error) {
	if ec == nil {
		return nil, errors.New("collector: scheduler needs a collector")
	}
	if disc == nil {
		return nil, errors.New("collector: scheduler needs a discoverer")
	}
	anchor, err := cfg.Recording.Anchor()
	if err != nil {
		return nil, err
	}
	if len(cfg.Series) == 0 {
		return nil, errors.New("collector: no series configured")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	sel := make([]SeriesSelection, 0, len(cfg.Series))
	for _, sc := range cfg.Series {
		spec, err := libs.LookupSeriesByName(sc.Series)
		if err != nil {
			return nil, err
		}
		if !spec.Verified {
			slog.Warn("scheduler: series is not verified against live Gamma",
				"series", spec.Name, "note", spec.Note)
		}
		if sc.Strikes < 0 {
			return nil, fmt.Errorf("collector: series %s: strikes cannot be negative (%d)", sc.Series, sc.Strikes)
		}
		if sc.Strikes > 0 && !spec.Ladder {
			slog.Warn("scheduler: strikes ignored for a non-ladder series", "series", spec.Name)
		}
		sel = append(sel, SeriesSelection{Spec: spec, StrikeLimit: sc.Strikes})
	}

	return &EpochScheduler{
		ec:        ec,
		disc:      disc,
		cfg:       cfg,
		sel:       sel,
		anchor:    anchor,
		seriesIDs: make(map[string]seriesRef),
		claimed:   make(map[string]bool),
		warned:    make(map[string]bool),
	}, nil
}

// Series returns the resolved selections, for logging and tests.
func (s *EpochScheduler) Series() []SeriesSelection { return s.sel }

// Run reconciles until ctx is cancelled or the configured number of epochs has
// been recorded, then writes the epoch manifests.
//
// An ambiguous series (two events settling at the same instant) aborts the run;
// every other failure — Gamma unreachable, a market not created yet — is logged
// and retried on the next tick, because the recorded data is still valid.
func (s *EpochScheduler) Run(ctx context.Context) error {
	names := make([]string, 0, len(s.sel))
	for _, sel := range s.sel {
		names = append(names, sel.Spec.Name)
	}
	slog.Info("scheduler: starting",
		"series", strings.Join(names, ","),
		"anchor", string(s.anchor),
		"days", s.cfg.Recording.Days,
		"ladder_offset_days", s.cfg.Recording.OffsetDays(),
	)

	for {
		now := time.Now()
		if err := s.reconcile(ctx, now); err != nil {
			slog.Error("scheduler: reconcile failed", "err", err)
		}
		if s.Finished() {
			slog.Info("scheduler: recorded the configured number of epochs, stopping",
				"days", s.cfg.Recording.Days, "epochs", s.epochs)
			return s.Finalize()
		}

		select {
		case <-ctx.Done():
			// Shutdown: the last epoch is partial, but its manifest is written
			// with whatever was claimed so the index is never missing.
			return s.Finalize()
		case <-time.After(s.untilNextWake(now)):
		}
	}
}

// Finished reports whether the configured epoch count has been reached.
func (s *EpochScheduler) Finished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// PlannedTarget is what a reconcile would claim for one (series, offset).
type PlannedTarget struct {
	EventTarget
	// Markets is how many markets the fan-out would claim: 1 for an up/down
	// event, the filtered rung count for a ladder.
	Markets int
}

// Plan resolves, for every configured series and offset, the event(s) that
// settle at the target instant and what the fan-out would claim — without
// rolling the epoch, starting a session, subscribing to anything or touching
// the disk.
//
// It runs the same discovery and selection the live scheduler does, which is
// what makes it a real dry run rather than a lookalike: a config that would
// record the wrong market fails here first. Targets this scheduler has already
// claimed are skipped. More than one entry for a series can legitimately appear
// when several events settle at the same instant (a DST transition day — see
// SelectEvents).
func (s *EpochScheduler) Plan(ctx context.Context, now time.Time) ([]PlannedTarget, error) {
	var out []PlannedTarget
	offsetDays := s.cfg.Recording.OffsetDays()
	for _, sel := range s.sel {
		last := 0
		if sel.Spec.Ladder {
			last = offsetDays
		}
		for off := 0; off <= last; off++ {
			targets, err := s.resolveTargets(ctx, sel, off, now)
			if err != nil {
				return out, err
			}
			for i := range targets {
				t := targets[i]
				out = append(out, PlannedTarget{
					EventTarget: t,
					Markets:     plannedMarkets(t, s.ec.SpotPrice()),
				})
			}
		}
	}
	return out, nil
}

// plannedMarkets counts the markets a fan-out would claim, including the effect
// of the strike filter.
func plannedMarkets(t EventTarget, spot float64) int {
	all := ladderRungs(t.Event)
	if !t.Spec.Ladder {
		return len(all)
	}
	selected, _ := selectRungs(all, t.StrikeLimit, spot)
	return len(selected)
}

// ActiveEpoch returns the epoch currently being recorded ("" before the first
// reconcile).
func (s *EpochScheduler) ActiveEpoch() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeEpoch
}

// reconcile brings the running set of sessions in line with the epoch and the
// series configured: roll the epoch if the boundary has passed, then make sure
// every family's live market — and the ladder families' next settlement day —
// has a session.
func (s *EpochScheduler) reconcile(ctx context.Context, now time.Time) error {
	if err := s.rollEpoch(now); err != nil {
		return err
	}
	if s.Finished() {
		return nil
	}

	offsetDays := s.cfg.Recording.OffsetDays()
	for _, sel := range s.sel {
		// Only the ladder families carry a "+1" event: an up/down market's
		// trading window *is* its epoch, so there is nothing to carry.
		last := 0
		if sel.Spec.Ladder {
			last = offsetDays
		}
		for off := 0; off <= last; off++ {
			if err := s.ensure(ctx, sel, off, now); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensure claims the sessions for one (series, offset) target.
func (s *EpochScheduler) ensure(ctx context.Context, sel SeriesSelection, offset int, now time.Time) error {
	targets, err := s.resolveTargets(ctx, sel, offset, now)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}

	failed := false
	for i := range targets {
		target := targets[i]
		started, skippedResolved, err := s.ec.StartEventSessions(target)
		if err != nil {
			// A transient failure must not settle the claim: leaving the memo unset
			// means the next tick retries this target.
			slog.Warn("scheduler: fan-out failed", "series", sel.Spec.Name, "event", target.Event.Slug, "err", err)
			failed = true
			continue
		}
		if len(started) == 0 && skippedResolved == 0 {
			continue // every market already had a session
		}
		if skippedResolved > 0 {
			// A market that resolved on-chain before we could claim it is not going
			// to become claimable, so the target counts as handled even when it
			// produced no sessions.
			slog.Warn("scheduler: markets already resolved on-chain, target closed out",
				"series", sel.Spec.Name,
				"event", target.Event.Slug,
				"resolved", skippedResolved,
				"claimed", len(started))
		}
		if len(started) == 0 {
			continue
		}

		s.mu.Lock()
		running := s.activeEpoch
		s.mu.Unlock()
		s.index.Record(running, target, started, targetStrikeFilter(started))

		slog.Info("scheduler: event claimed",
			"series", sel.Spec.Name,
			"event", target.Event.Slug,
			"epoch", target.EpochID,
			"settle_at", target.SettleAt.UTC().Format(time.RFC3339),
			"offset_days", offset,
			"markets", len(started),
			"of_ladder", len(target.Event.Markets),
		)
	}

	if !failed {
		s.mu.Lock()
		s.claimed[claimKey(sel.Spec.Name, offset, targets[0].SettleAt)] = true
		s.mu.Unlock()
	}

	// Persist the manifests now, not only at the epoch roll, so a hard kill still
	// leaves an index of what was claimed. Every touched epoch is written: an
	// offset claim records the carry in the running epoch AND the markets in the
	// next one, so persisting only the target would leave the carry on disk
	// stale.
	return s.persistAll()
}

// resolveTargets finds the event(s) a (series, offset) resolves to at `now`, and
// derives the epoch each belongs to. An empty result with a nil error means
// there is nothing to do — the instant has passed, no event is published for it
// yet, the lookup failed, or this scheduler already claimed it.
//
// More than one target is normal on a DST transition day, where Polymarket's
// generator emits two markets ending at the same instant (see SelectEvents).
func (s *EpochScheduler) resolveTargets(ctx context.Context, sel SeriesSelection, offset int, now time.Time) ([]EventTarget, error) {
	// The target settle instant of the "+n" ladder event is what the same series
	// resolves to n days from now, so the offset falls out of the existing rule
	// instead of needing its own arithmetic.
	at := now
	if offset > 0 {
		at = now.AddDate(0, 0, offset)
	}
	settleAt, err := sel.Spec.TargetSettle(at)
	if err != nil {
		return nil, err
	}
	if !settleAt.After(now) {
		return nil, nil // already settled, or settling: nothing left to record
	}

	// Steady state is one claim per (series, offset, settle instant): an hour for
	// the hourly family, a day for the rest. This memo is checked BEFORE any HTTP
	// because that is what makes the 30s poll free — without it every tick would
	// re-run discovery for every target all day. A target that was never claimed
	// has no memo, so it is retried on every tick.
	if s.claimedTarget(sel.Spec.Name, offset, settleAt) {
		return nil, nil
	}

	ref, err := s.series(ctx, sel.Spec)
	if err != nil {
		slog.Warn("scheduler: series lookup failed", "series", sel.Spec.Name, "err", err)
		return nil, nil
	}

	events, err := s.disc.ListSeriesEvents(ctx, connector.EventQuery{
		SeriesID: ref.id,
		Closed:   boolPtr(false),
		Limit:    200,
	})
	if err != nil {
		// The cached series id may be stale — force a re-resolve next tick.
		if inv, ok := s.disc.(seriesInvalidator); ok {
			inv.InvalidateSeries(sel.Spec.GammaSeries)
		}
		s.mu.Lock()
		delete(s.seriesIDs, sel.Spec.GammaSeries)
		s.mu.Unlock()

		slog.Warn("scheduler: listing events failed",
			"series", sel.Spec.Name, "series_id", ref.id, "err", err)
		return nil, nil
	}

	matched, err := SelectEvents(events, settleAt)
	if errors.Is(err, ErrNoEventForSettle) {
		// A target this close to settling that has no market means we are not
		// recording a live market — the signature of a DST-grid mismatch or a Gamma
		// gap — so it is worth a warning rather than a debug line nobody reads.
		// Warn once per target: the poll keeps retrying until the instant passes.
		args := []any{
			"series", sel.Spec.Name,
			"settle_at", settleAt.UTC().Format(time.RFC3339),
			"open_events", len(events),
		}
		key := claimKey(sel.Spec.Name, offset, settleAt)
		if settleAt.Sub(now) <= missingMarketWarnWindow {
			s.mu.Lock()
			first := !s.warned[key]
			s.warned[key] = true
			s.mu.Unlock()
			if first {
				slog.Warn("scheduler: no event for an imminent target — nothing will be recorded for it", args...)
			} else {
				slog.Debug("scheduler: no event for target", args...)
			}
		} else {
			slog.Debug("scheduler: no event for target", args...)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	epochID, err := libs.EpochForSettle(settleAt, s.anchor)
	if err != nil {
		return nil, err
	}

	// Every event that settles at the instant is claimed: one normally, two on a
	// DST transition day (see SelectEvents).
	out := make([]EventTarget, 0, len(matched))
	for _, event := range matched {
		out = append(out, EventTarget{
			Spec:        sel.Spec,
			Event:       event,
			SettleAt:    settleAt,
			SeriesID:    ref.id,
			Recurrence:  ref.recurrence,
			EpochID:     epochID,
			OffsetDays:  offset,
			StrikeLimit: sel.StrikeLimit,
		})
	}

	if len(out) > 1 {
		slugs := make([]string, 0, len(out))
		for _, t := range out {
			slugs = append(slugs, t.Event.Slug)
		}
		slog.Warn("scheduler: several events settle at the same instant, recording all of them",
			"series", sel.Spec.Name,
			"settle_at", settleAt.UTC().Format(time.RFC3339),
			"events", strings.Join(slugs, ", "),
		)
	}
	return out, nil
}

// series resolves a series' Gamma id, caching it.
func (s *EpochScheduler) series(ctx context.Context, spec libs.SeriesSpec) (seriesRef, error) {
	s.mu.Lock()
	ref, ok := s.seriesIDs[spec.GammaSeries]
	s.mu.Unlock()
	if ok {
		return ref, nil
	}

	list, err := s.disc.GetSeries(ctx, spec.GammaSeries)
	if err != nil {
		return seriesRef{}, fmt.Errorf("get series %s: %w", spec.GammaSeries, err)
	}
	for _, ser := range list {
		if ser.Slug != spec.GammaSeries {
			continue
		}
		ref = seriesRef{id: ser.ID, recurrence: ser.Recurrence}
		s.mu.Lock()
		s.seriesIDs[spec.GammaSeries] = ref
		s.mu.Unlock()
		return ref, nil
	}
	return seriesRef{}, fmt.Errorf("series %s not in Gamma response (%d entries)", spec.GammaSeries, len(list))
}

// targetStrikeFilter reads back the filter a fan-out stamped on its sessions,
// so the epoch manifest can record it once per series entry.
func targetStrikeFilter(started []StartedMarket) *StrikeFilterInfo {
	for _, st := range started {
		if st.Session == nil {
			continue
		}
		if f := st.Session.MarketContext().StrikeFilter; f != nil {
			return f
		}
	}
	return nil
}

// rollEpoch advances the epoch when the anchor boundary has passed, writes the
// manifest of the epoch that just ended, and marks the run finished once the
// configured number of epochs has been recorded.
func (s *EpochScheduler) rollEpoch(now time.Time) error {
	id := libs.EpochIDAt(now, s.anchor)

	s.mu.Lock()
	if s.activeEpoch == "" {
		s.activeEpoch = id
		s.epochs = 1
		s.index = newEpochIndex(s.anchor, RunInfo{
			Days:             s.cfg.Recording.Days,
			LadderOffsetDays: s.cfg.Recording.OffsetDays(),
			Anchor:           string(s.anchor),
			StartedAt:        now.UnixMilli(),
		})
		s.mu.Unlock()
		slog.Info("scheduler: epoch started",
			"epoch", id, "anchor", string(s.anchor), "poll_interval", s.cfg.PollInterval)
		return nil
	}
	if id == s.activeEpoch {
		s.mu.Unlock()
		return nil
	}

	ended := s.activeEpoch
	s.activeEpoch = id
	s.epochs++
	epochs := s.epochs
	if days := s.cfg.Recording.Days; days > 0 && epochs > days {
		s.done = true
	}
	s.mu.Unlock()

	slog.Info("scheduler: epoch rolled",
		"ended", ended, "current", id, "epochs_recorded", epochs, "done", s.Finished())

	// The epoch that just ended is complete: its own settlement event has
	// resolved, and the ladder event it carried settles now too.
	return s.persist(ended)
}

// persist writes a single epoch's manifest.
func (s *EpochScheduler) persist(epochID string) error {
	root := s.ec.recordingDir()
	s.mu.Lock()
	ix := s.index
	s.mu.Unlock()
	if ix == nil {
		return nil
	}
	return ix.writeEpoch(root, epochID, s.ec.FeedRefs(epochID), s.ec.TruncatedMarkets())
}

// persistAll writes the manifest of every epoch the run has touched.
func (s *EpochScheduler) persistAll() error {
	root := s.ec.recordingDir()
	s.mu.Lock()
	ix := s.index
	s.mu.Unlock()
	if ix == nil {
		return nil
	}
	return ix.WriteAll(root, s.ec.FeedRefs, s.ec.TruncatedMarkets())
}

// Finalize writes the manifest of every epoch the run touched. Called at
// shutdown, after StopAll has marked the sessions the run outlived as
// unsettled, so the partial recordings are flagged in the index too.
func (s *EpochScheduler) Finalize() error {
	s.mu.Lock()
	ix := s.index
	s.mu.Unlock()
	if ix == nil {
		return nil
	}
	return ix.WriteAll(s.ec.recordingDir(), s.ec.FeedRefs, s.ec.TruncatedMarkets())
}

// untilNextWake is how long to sleep before the next reconcile: the earliest
// boundary across the configured series and the epoch anchor, capped by the
// poll interval so late-created markets and failed lookups are retried.
func (s *EpochScheduler) untilNextWake(now time.Time) time.Duration {
	next := now.Add(s.cfg.PollInterval)

	if _, end, err := libs.EpochBounds(libs.EpochIDAt(now, s.anchor), s.anchor); err == nil && end.After(now) {
		next = earlier(next, end)
	}
	for _, sel := range s.sel {
		if sel.Spec.Anchor != libs.AnchorETBoundary || sel.Spec.Key.Interval <= 0 {
			continue
		}
		if _, end := libs.IntervalWindow(now, sel.Spec.Key.Interval); end.After(now) {
			next = earlier(next, end)
		}
	}

	d := next.Sub(now) + boundaryEpsilon
	if d > s.cfg.PollInterval {
		d = s.cfg.PollInterval
	}
	if d < time.Second {
		d = time.Second
	}
	return d
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// claimedTarget reports whether a (series, offset, settle instant) target has
// already been claimed. Safe to call without holding s.mu.
func (s *EpochScheduler) claimedTarget(series string, offset int, settleAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed[claimKey(series, offset, settleAt)]
}

// claimKey identifies a (series, offset, settle instant) target across ticks.
func claimKey(series string, offset int, settleAt time.Time) string {
	return series + "|" + strconv.Itoa(offset) + "|" + strconv.FormatInt(settleAt.UnixMilli(), 10)
}

func boolPtr(b bool) *bool { return &b }
