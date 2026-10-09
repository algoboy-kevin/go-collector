package collector

import (
	"context"
	"testing"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// Market-WS silence watchdog tests. The reviewer's finding: nothing forced a
// market reconnect when the socket still answered pings but stopped delivering
// events. The watchdog fills that gap; these tests pin its state machine with a
// controlled clock and injected status/reconnect hooks.

func newWatchdogCollector(t *testing.T, timeout time.Duration) *EventCollector {
	t.Helper()
	return NewCollector(CollectorConfig{
		RecordingDir:         t.TempDir(),
		MarketSilenceTimeout: timeout,
	}, nil)
}

// watchdogDeps builds injectable deps around a mutable status and a fire
// counter.
func watchdogDeps(status *ws.ConnectionStatus, sessions *int, fires *int) MarketWatchdogDeps {
	return MarketWatchdogDeps{
		Status:         func() ws.ConnectionStatus { return *status },
		Reconnect:      func() { *fires++ },
		ActiveSessions: func() int { return *sessions },
	}
}

// TestWatchdogFiresOnceThenSuppressesUntilFullWindow proves the core rule: with
// sessions active and the feed connected, a silence longer than the timeout
// triggers one reconnect, and further ticks are suppressed until another full
// window elapses.
func TestWatchdogFiresOnceThenSuppressesUntilFullWindow(t *testing.T) {
	timeout := 30 * time.Second
	ec := newWatchdogCollector(t, timeout)
	status, sessions, fires := ws.StatusConnected, 1, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, timeout) // connect transition seeds the clock
	if fires != 0 {
		t.Fatalf("fired before any silence: %d", fires)
	}

	t1 := t0.Add(timeout + time.Second)
	ec.marketWatchdogTick(t1, deps, timeout)
	if fires != 1 {
		t.Fatalf("fires after the first silence window = %d, want 1", fires)
	}
	// The recorded silence interval is what the finalize tail check reads.
	if !ec.silenceCovers(t0.UnixMilli(), t1.UnixMilli()) {
		t.Error("the triggered silence interval was not recorded")
	}

	// A tick inside the suppression window must not fire again.
	ec.marketWatchdogTick(t1.Add(time.Second), deps, timeout)
	if fires != 1 {
		t.Errorf("fires after a suppressed tick = %d, want 1", fires)
	}

	// After another full window it fires again.
	ec.marketWatchdogTick(t1.Add(timeout+time.Second), deps, timeout)
	if fires != 2 {
		t.Errorf("fires after a second full window = %d, want 2", fires)
	}
}

// TestWatchdogDoesNotFireWithoutSessions proves the watchdog is inert when
// nothing is recording — a quiet gap between markets is not a stuck feed.
func TestWatchdogDoesNotFireWithoutSessions(t *testing.T) {
	timeout := 30 * time.Second
	ec := newWatchdogCollector(t, timeout)
	status, sessions, fires := ws.StatusConnected, 0, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, timeout)
	ec.marketWatchdogTick(t0.Add(time.Hour), deps, timeout)
	if fires != 0 {
		t.Errorf("fired with no active sessions: %d", fires)
	}
}

// TestWatchdogDoesNotFireWhileDisconnected proves a disconnect is the
// connector's own reconnect loop's job, not the silence watchdog's.
func TestWatchdogDoesNotFireWhileDisconnected(t *testing.T) {
	timeout := 30 * time.Second
	ec := newWatchdogCollector(t, timeout)
	status, sessions, fires := ws.StatusDisconnected, 1, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, timeout)
	ec.marketWatchdogTick(t0.Add(time.Hour), deps, timeout)
	if fires != 0 {
		t.Errorf("fired while disconnected: %d", fires)
	}
}

// TestWatchdogDoesNotFireWhileEventsArrive proves a busy feed never triggers,
// even across a long span — only silence does.
func TestWatchdogDoesNotFireWhileEventsArrive(t *testing.T) {
	timeout := 30 * time.Second
	ec := newWatchdogCollector(t, timeout)
	status, sessions, fires := ws.StatusConnected, 1, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, timeout)
	// An event every 10s keeps the feed under the timeout for an hour.
	for i := 1; i <= 360; i++ {
		at := t0.Add(time.Duration(i) * 10 * time.Second)
		ec.lastMarketEventAt.Store(at.UnixMilli())
		ec.marketWatchdogTick(at, deps, timeout)
	}
	if fires != 0 {
		t.Errorf("fired on a feed delivering events: %d", fires)
	}
}

// TestWatchdogResetsWindowOnReconnect proves a fresh connection gets a full
// silence window before it is judged: the disconnected -> connected transition
// resets the clock and clears the suppression.
func TestWatchdogResetsWindowOnReconnect(t *testing.T) {
	timeout := 30 * time.Second
	ec := newWatchdogCollector(t, timeout)
	status, sessions, fires := ws.StatusConnected, 1, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, timeout)

	// A silence fires once.
	t1 := t0.Add(timeout + time.Second)
	ec.marketWatchdogTick(t1, deps, timeout)
	if fires != 1 {
		t.Fatalf("fires = %d, want 1", fires)
	}

	// Drop and come back: the reconnect loop owns the down interval.
	status = ws.StatusDisconnected
	ec.marketWatchdogTick(t1.Add(time.Second), deps, timeout)
	status = ws.StatusConnected
	t2 := t1.Add(2 * time.Second)
	ec.marketWatchdogTick(t2, deps, timeout) // transition resets the clock
	ec.marketWatchdogTick(t2.Add(time.Second), deps, timeout)
	if fires != 1 {
		t.Errorf("fired within a full window of the reset: %d", fires)
	}

	// A full window of silence AFTER the reconnect fires again.
	ec.marketWatchdogTick(t2.Add(timeout+time.Second), deps, timeout)
	if fires != 2 {
		t.Errorf("fires after a full window post-reconnect = %d, want 2", fires)
	}
}

// TestWatchdogDisabledByNonPositiveTimeout proves the timeout <= 0 disable path.
func TestWatchdogDisabledByNonPositiveTimeout(t *testing.T) {
	ec := newWatchdogCollector(t, -time.Second)
	status, sessions, fires := ws.StatusConnected, 1, 0
	deps := watchdogDeps(&status, &sessions, &fires)

	t0 := time.Now()
	ec.marketWatchdogTick(t0, deps, 0)
	ec.marketWatchdogTick(t0.Add(time.Hour), deps, 0)
	if fires != 0 {
		t.Errorf("fired with the watchdog disabled: %d", fires)
	}
}

// TestStartMarketWatchdogFires proves the goroutine wiring with a real clock.
func TestStartMarketWatchdogFires(t *testing.T) {
	ec := newWatchdogCollector(t, 30*time.Millisecond)
	fired := make(chan struct{}, 1)
	deps := MarketWatchdogDeps{
		Now:            time.Now,
		Status:         func() ws.ConnectionStatus { return ws.StatusConnected },
		Reconnect:      func() { select { case fired <- struct{}{}: default: } },
		ActiveSessions: func() int { return 1 },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ec.StartMarketWatchdog(ctx, 5*time.Millisecond, deps)

	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("StartMarketWatchdog did not fire within 3s")
	}
}
