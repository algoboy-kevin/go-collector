package collector

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// manualReconnectMinInterval bounds how often a manual reconnect signal may
// fire. A storm of SIGUSR1s (or a misconfigured `kill` loop) must not flap the
// market WS, so extra signals inside the window are logged and ignored.
const manualReconnectMinInterval = 5 * time.Second

// RunManualReconnect forwards manual "force a market-WS reconnect" signals to
// reconnect, so the recovery path can be exercised on a live run without
// waiting for the silence watchdog or a transport failure. cmd/collector wires
// it to its own SIGUSR1 channel — deliberately separate from the shutdown
// signals, because a manual reconnect must never stop the process.
//
// Each accepted signal logs at WARN with the current market-WS status and then
// calls reconnect. A signal arriving less than manualReconnectMinInterval after
// the previous accepted one is logged and ignored, so a signal storm cannot
// flap the socket. The loop returns when ctx is cancelled or sigs is closed;
// the caller is responsible for signal.Stop.
//
// now defaults to time.Now. status may be nil, in which case the logged status
// is empty. reconnect may be nil, in which case the call is a no-op.
func RunManualReconnect(
	ctx context.Context,
	sigs <-chan os.Signal,
	reconnect func(),
	status func() websocket.ConnectionStatus,
	now func() time.Time,
) {
	if reconnect == nil {
		return
	}
	if now == nil {
		now = time.Now
	}

	var last time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sigs:
			if !ok {
				return
			}
			t := now()
			if !last.IsZero() && t.Sub(last) < manualReconnectMinInterval {
				slog.Warn("collector: SIGUSR1 ignored — rate-limited",
					"since_last_s", t.Sub(last).Seconds(),
					"min_interval_s", manualReconnectMinInterval.Seconds(),
				)
				continue
			}
			last = t

			st := websocket.ConnectionStatus("")
			if status != nil {
				st = status()
			}
			slog.Warn("collector: SIGUSR1 — forcing market WS reconnect", "status", string(st))
			reconnect()
		}
	}
}
