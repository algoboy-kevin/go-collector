package collector

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// manualReconnectClock is an injected, monotonic test clock. It also counts
// Now() calls, which is exactly one per signal the reconnect loop consumes —
// the barrier the rate-limit test uses to know a signal has been processed.
type manualReconnectClock struct {
	mu    sync.Mutex
	now   time.Time
	calls atomic.Int64
}

func (c *manualReconnectClock) Now() time.Time {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualReconnectClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// startManualReconnect runs RunManualReconnect in a goroutine with a fake clock
// and a counting reconnect, returning the handles the tests drive it with.
func startManualReconnect(t *testing.T) (
	sigs chan os.Signal,
	fired *atomic.Int64,
	clk *manualReconnectClock,
	done chan struct{},
	cancel context.CancelFunc,
) {
	t.Helper()
	sigs = make(chan os.Signal, 8)
	fired = &atomic.Int64{}
	clk = &manualReconnectClock{now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	done = make(chan struct{})

	ctx, cancelFn := context.WithCancel(context.Background())
	cancel = cancelFn
	go func() {
		defer close(done)
		RunManualReconnect(ctx, sigs,
			func() { fired.Add(1) },
			func() websocket.ConnectionStatus { return websocket.StatusConnected },
			clk.Now,
		)
	}()
	t.Cleanup(cancelFn)
	return sigs, fired, clk, done, cancel
}

// waitManualFired blocks until the reconnect has fired at least `want` times.
func waitManualFired(t *testing.T, fired *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fired.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("reconnect fired %d times, want >= %d", fired.Load(), want)
}

// waitManualConsumed blocks until `want` signals have been consumed (Now() is
// called exactly once per consumed signal).
func waitManualConsumed(t *testing.T, clk *manualReconnectClock, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if clk.calls.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("clock Now() calls = %d, want >= %d (signals not consumed)", clk.calls.Load(), want)
}

// TestManualReconnectOneSignalFiresOnce: a single signal forces exactly one
// reconnect.
func TestManualReconnectOneSignalFiresOnce(t *testing.T) {
	sigs, fired, _, _, _ := startManualReconnect(t)

	sigs <- syscall.SIGUSR1

	waitManualFired(t, fired, 1)
	if got := fired.Load(); got != 1 {
		t.Errorf("reconnect fired %d times, want 1", got)
	}
}

// TestManualReconnectSecondSignalWithinWindowIsIgnored: two signals inside the
// 5 s window deliver one reconnect; the second is rate-limited (logged and
// ignored), so a signal storm cannot flap the socket.
func TestManualReconnectSecondSignalWithinWindowIsIgnored(t *testing.T) {
	sigs, fired, clk, _, _ := startManualReconnect(t)

	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGUSR1

	// Both signals are inside the window (the fake clock does not advance), so
	// once both have been consumed only the first may have reconnected.
	waitManualConsumed(t, clk, 2)
	if got := fired.Load(); got != 1 {
		t.Errorf("reconnect fired %d times, want 1 (the second signal is inside the 5s window)", got)
	}
}

// TestManualReconnectSignalAfterWindowFiresAgain: the rate limit is a window,
// not a one-shot — a signal a full interval later reconnects again.
func TestManualReconnectSignalAfterWindowFiresAgain(t *testing.T) {
	sigs, fired, clk, _, _ := startManualReconnect(t)

	sigs <- syscall.SIGUSR1
	waitManualFired(t, fired, 1)

	clk.Advance(manualReconnectMinInterval)
	sigs <- syscall.SIGUSR1
	waitManualFired(t, fired, 2)
}

// TestManualReconnectReturnsOnContextCancel: cancelling the context ends the
// goroutine (no leak).
func TestManualReconnectReturnsOnContextCancel(t *testing.T) {
	_, _, _, done, cancel := startManualReconnect(t)

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunManualReconnect did not return after ctx cancel (goroutine leak)")
	}
}
