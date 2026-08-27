package collector

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

const brokerGapThresholdMs int64 = 2000 // 2 seconds

// buildDisconnectWindows converts a connection event log into a list of
// [disconnectStart, reconnectEnd] ms timestamps.
func buildDisconnectWindows(connEvents []ConnectionEvent) [][2]int64 {
	var windows [][2]int64
	var disconnectStart int64
	for _, e := range connEvents {
		if e.Status == "disconnected" {
			disconnectStart = e.Timestamp
		} else if e.Status == "connected" && disconnectStart != 0 {
			windows = append(windows, [2]int64{disconnectStart, e.Timestamp})
			disconnectStart = 0
		}
	}
	// If still disconnected at end of log, close at last event time.
	if disconnectStart != 0 {
		windows = append(windows, [2]int64{disconnectStart, disconnectStart})
	}
	return windows
}

// overlapsDisconnect returns true if the broker gap [gapStart, gapEnd]
// overlaps any disconnect window.
func overlapsDisconnect(gapStart, gapEnd int64, windows [][2]int64) bool {
	for _, w := range windows {
		if gapStart < w[1] && gapEnd > w[0] {
			return true
		}
	}
	return false
}

// computeConnectionInfo summarises the connection event log.
func computeConnectionInfo(connEvents []ConnectionEvent) *ConnectionInfo {
	ci := &ConnectionInfo{}
	var disconnectStart int64
	for _, e := range connEvents {
		if e.Status == "disconnected" {
			disconnectStart = e.Timestamp
		} else if e.Status == "connected" && disconnectStart != 0 {
			dur := e.Timestamp - disconnectStart
			ci.DisconnectCount++
			ci.TotalDisconnectedMs += dur
			if dur > ci.MaxDisconnectedMs {
				ci.MaxDisconnectedMs = dur
			}
			disconnectStart = 0
		}
	}
	// Count open-ended disconnect at end of log.
	if disconnectStart != 0 {
		ci.DisconnectCount++
	}
	return ci
}

// filterConnectionEvents returns only connection events with Timestamp in
// [startMs, endMs] (endMs 0 = unbounded). Used to scope per-market disconnect
// stats and gap classification to that market's window instead of the full
// cumulative connection log across market rotation.
func filterConnectionEvents(connEvents []ConnectionEvent, startMs, endMs int64) []ConnectionEvent {
	var out []ConnectionEvent
	for _, e := range connEvents {
		if e.Timestamp < startMs {
			continue
		}
		if endMs != 0 && e.Timestamp > endMs {
			continue
		}
		out = append(out, e)
	}
	return out
}

// writeMetadata writes metadata to {dir}/{name}/metadata.json. meta may be a
// *MarketMetadata (market sessions) or *FeedMetadata (feed buckets).
func writeMetadata(dir, name string, meta any) error {
	bucketDir := filepath.Join(dir, name)
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		return fmt.Errorf("writeMetadata: mkdir: %w", err)
	}

	path := filepath.Join(bucketDir, "metadata.json")
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("writeMetadata: marshal: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writeMetadata: write: %w", err)
	}

	slog.Debug("collector: wrote metadata file", "name", name, "path", path)
	return nil
}
