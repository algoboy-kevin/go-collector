package hyperliquid

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// The `_meta` channel carries the recorder's own marks, never venue data.
const (
	// MetaChannel is the synthetic channel name for sentinel lines.
	MetaChannel = "_meta"

	// EventSubscribed marks the FIRST connection's frames, written after the subscribe
	// frames are sent. It is distinct from EventResubscribed on purpose: a consumer that
	// counts "how many times did this channel reconnect" must not count the initial
	// connect as one. A capture before this existed emitted `resubscribed` first, which
	// read as one reconnect per channel on every run.
	EventSubscribed = "subscribed"
	// EventResubscribed marks a REAL reconnect — a connection that came back after a drop.
	EventResubscribed = "resubscribed"
	// EventDisconnected marks an unexpected drop.
	EventDisconnected = "disconnected"
	// EventDropped marks frames lost to a full write queue. It exists because the queue
	// is bounded and dropping is deliberate: a hole in the book must be attributable from
	// the stream alone, since the consumer does not read the manifest.
	EventDropped = "dropped"
	// EventWriteError marks a failed write. Same reasoning as EventDropped: a hole has to
	// live in the data, not only in a manifest the ingest never opens.
	EventWriteError = "write_error"
	// EventStopped is a terminal mark written only on a clean shutdown. It is an
	// addition beyond RECORDER_SPEC.md, so a reader can tell "recording ended" from
	// "recording was cut off" without inspecting the filesystem.
	EventStopped = "stopped"
)

// Sentinel is one in-band `_meta` mark. Only the fields relevant to Event are set; the
// rest are omitted from the JSON.
type Sentinel struct {
	// Event is one of the Event* constants above.
	Event string
	// RunID identifies the recorder process, so a mark lifted out of its directory still
	// says which capture it belongs to.
	RunID string
	// Reason is the venue's own words for a drop (see DisconnectReason).
	Reason string
	// Count is the cumulative number of lost frames, for EventDropped.
	Count int64
	// File is the hour file a write failed on, for EventWriteError.
	File string
	// Err is the write failure, for EventWriteError.
	Err string
}

// sentinelBody is the wire shape of a `_meta` line.
type sentinelBody struct {
	Channel string `json:"channel"`
	Event   string `json:"event"`
	TSNS    int64  `json:"ts_ns"`
	RunID   string `json:"run_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Count   int64  `json:"count,omitempty"`
	File    string `json:"file,omitempty"`
	Err     string `json:"err,omitempty"`
}

// SentinelLine builds one in-band `_meta` line for a channel's frame stream.
//
// It is written with exactly the same framing as a venue frame — `<rx_ns>\t<json>\n`
// — so the ingest reads the whole file with one parser, and a reconnect or a shutdown
// cannot be mistaken for a hole in the data.
//
// reason is filled for `disconnected` only; the other marks pass "" and the field is
// omitted. It is the venue's own words for the drop — see DisconnectReason — which is
// what RECORDER_SPEC.md §3 asks for. A capture written by a connector older than
// go-exchange-connector v0.7.2 has no reason field at all, so a converter must not
// require it.
func SentinelLine(s Sentinel, rx time.Time) ([]byte, error) {
	body, err := json.Marshal(sentinelBody{
		Channel: MetaChannel,
		Event:   s.Event,
		TSNS:    rx.UnixNano(),
		RunID:   s.RunID,
		Reason:  s.Reason,
		Count:   s.Count,
		File:    s.File,
		Err:     s.Err,
	})
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: marshal sentinel %q: %w", s.Event, err)
	}
	return body, nil
}

// DisconnectReason renders the error that ended a connection as the single-line
// `reason` recorded in a `disconnected` sentinel and in ChannelReport.LastDisconnect.
//
// It comes from the connector's SetOnDisconnect callback (go-exchange-connector
// v0.7.2+). For a peer close frame coder/websocket renders the error as
// `failed to read: status = policy violation and reason = "Cannot open more than 15
// connections."` — the close code followed by the venue's own words. That is the whole
// difference between "the socket blipped" and "the venue refused us", and it is why
// the reason belongs in the capture: a run that flapped at 03:00 has to be diagnosable
// from its own files, not from a log nobody kept.
//
// A nil error (a deliberate Disconnect(), which the connector does not route here
// anyway) and an error with no text both yield "", and the field is omitted.
func DisconnectReason(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}
