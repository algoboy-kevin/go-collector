package collector

import "testing"

// TestComputeConnectionInfoScoped verifies that, given only the connection
// events inside a window, disconnect stats reflect just that window. Connection
// stats are per-session now (see RecordingSession.RecordConnection); this pins
// the summariser they share with the feed recorders.
func TestComputeConnectionInfoScoped(t *testing.T) {
	// Events before the window (1000, 2000) are excluded by the caller — here
	// the slice already starts inside the window, as a session's own log does.
	events := []ConnectionEvent{
		{Status: "disconnected", Timestamp: 3000},
		{Status: "connected", Timestamp: 3500}, // 500ms
		{Status: "disconnected", Timestamp: 4000},
		{Status: "connected", Timestamp: 4600}, // 600ms
	}
	ci := computeConnectionInfo(events)
	if ci.DisconnectCount != 2 {
		t.Fatalf("disconnect_count = %d, want 2", ci.DisconnectCount)
	}
	if ci.TotalDisconnectedMs != 1100 {
		t.Fatalf("total_disconnected_ms = %d, want 1100", ci.TotalDisconnectedMs)
	}
	if ci.MaxDisconnectedMs != 600 {
		t.Fatalf("max_disconnected_ms = %d, want 600", ci.MaxDisconnectedMs)
	}
}

// TestComputeConnectionInfoOpenEndedDisconnectIsCounted pins that a log ending
// on a disconnect still counts it: the per-session path appends a synthetic
// reconnect at finalize so an outage in progress is not silently dropped.
func TestComputeConnectionInfoOpenEndedDisconnectIsCounted(t *testing.T) {
	ci := computeConnectionInfo([]ConnectionEvent{
		{Status: "disconnected", Timestamp: 3000},
	})
	if ci.DisconnectCount != 1 {
		t.Errorf("disconnect_count = %d, want 1 for an open-ended disconnect", ci.DisconnectCount)
	}
}
