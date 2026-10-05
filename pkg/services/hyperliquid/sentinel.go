package hyperliquid

import (
	"encoding/json"
	"fmt"
	"time"
)

// The `_meta` channel carries the recorder's own marks, never venue data.
const (
	// MetaChannel is the synthetic channel name for sentinel lines.
	MetaChannel = "_meta"

	// EventResubscribed marks the start of a connection's frames, written after the
	// subscribe frames are sent on every successful connect.
	EventResubscribed = "resubscribed"
	// EventDisconnected marks an unexpected drop.
	EventDisconnected = "disconnected"
	// EventStopped is a terminal mark written only on a clean shutdown. It is an
	// addition beyond RECORDER_SPEC.md, so a reader can tell "recording ended" from
	// "recording was cut off" without inspecting the filesystem.
	EventStopped = "stopped"
)

// sentinel is the JSON body of a `_meta` line.
type sentinel struct {
	Channel string `json:"channel"`
	Event   string `json:"event"`
	TSNS    int64  `json:"ts_ns"`
	Reason  string `json:"reason,omitempty"`
}

// SentinelLine builds one in-band `_meta` line for a channel's frame stream.
//
// It is written with exactly the same framing as a venue frame — `<rx_ns>\t<json>\n`
// — so the ingest reads the whole file with one parser, and a reconnect or a shutdown
// cannot be mistaken for a hole in the data.
//
// KNOWN GAP: `disconnected` carries no `reason`. RECORDER_SPEC.md §3 asks for one, but
// the connector's SetOnStatusChange callback passes only the connection status, so the
// error never reaches us; the library logs it, so the diagnosis is not lost, it is just
// not in the capture. Filling it needs a change in the connector. See
// PERP_COLLECTOR_SPEC.md §8 item 8.
func SentinelLine(event string, rx time.Time) ([]byte, error) {
	body, err := json.Marshal(sentinel{
		Channel: MetaChannel,
		Event:   event,
		TSNS:    rx.UnixNano(),
	})
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: marshal sentinel %q: %w", event, err)
	}
	return body, nil
}
