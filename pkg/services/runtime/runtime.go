// Package runtime provides a minimal live RuntimeManager for the collector.
//
// The collector is a single-purpose real-time recorder: it subscribes to a
// market WebSocket, buffers events, and writes them to disk on resolution.
// It needs wall-clock time and cron-style scheduling for rolling market
// rotation — no DES loop, no Hollywood actor engine, no backtest clock.
//
// This is intentionally a slimmed-down copy of the RuntimeManager used by
// the trading engine. It exposes the same surface the collector's
// EventCollector depends on (New, RootContext, Now, Start, Stop,
// Register/Unregister, RoutineRunner, Clock, Mode) so the collector package
// compiles without importing go-trading-core.
package runtime

import (
	"context"
	"time"

	"github.com/algoboy-kevin/go-des/pkg/abstract"
)

// Mode represents the runtime operating mode. The collector only ever uses
// ModeCollector (real wall-clock time + live routine runner).
type Mode string

const (
	ModeCollector Mode = "COLLECTOR"
)

// RuntimeManager wires together a Clock and a RoutineRunner for the collector.
//
// It is deliberately minimal: no actor engine, no backtest clock, no logger.
// The collector only needs a context, wall-clock time, and cron scheduling.
type RuntimeManager struct {
	mode  Mode
	clock abstract.Clock
	rt    abstract.RoutineRunner

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a RuntimeManager for the given mode. Only ModeCollector is
// supported; it uses a LiveClock (time.Now) and a LiveRoutineRunner.
func New(mode Mode) (*RuntimeManager, error) {
	m := &RuntimeManager{mode: mode}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.clock = abstract.LiveClock{}
	m.rt = abstract.NewLiveRoutineRunner(m.clock)
	return m, nil
}

// Mode returns the operating mode.
func (m *RuntimeManager) Mode() Mode { return m.mode }

// Clock returns the clock used for scheduling.
func (m *RuntimeManager) Clock() abstract.Clock { return m.clock }

// RoutineRunner returns the routine runner used for cron scheduling.
func (m *RuntimeManager) RoutineRunner() abstract.RoutineRunner { return m.rt }

// RootContext returns the root context, cancelled on Stop.
func (m *RuntimeManager) RootContext() context.Context { return m.ctx }

// Now returns the current wall-clock time.
func (m *RuntimeManager) Now() time.Time { return m.clock.Now() }

// Start begins routine scheduling.
func (m *RuntimeManager) Start() { m.rt.Start() }

// Stop cancels the root context and stops the routine runner.
func (m *RuntimeManager) Stop() {
	m.cancel()
	m.rt.Stop()
}

// Register schedules a routine (cron/interval/one-shot).
func (m *RuntimeManager) Register(reg abstract.RoutineRegistration) {
	m.rt.Register(reg)
}

// Unregister removes a scheduled routine by key.
func (m *RuntimeManager) Unregister(key abstract.RoutineKey) {
	m.rt.Unregister(key)
}

// ClearRoutines removes all scheduled routines.
func (m *RuntimeManager) ClearRoutines() { m.rt.ClearRoutine() }
