package collector

import "testing"

// TestFilterConnectionEventsScopesToWindow ensures connection events before the
// market's start are excluded so disconnect stats are per-window, not
// cumulative across market rotation.
func TestFilterConnectionEventsScopesToWindow(t *testing.T) {
	events := []ConnectionEvent{
		{Status: "disconnected", Timestamp: 1000},
		{Status: "connected", Timestamp: 2000},
		{Status: "disconnected", Timestamp: 3000},
		{Status: "connected", Timestamp: 4000},
	}
	filtered := filterConnectionEvents(events, 2500, 0)
	if len(filtered) != 2 {
		t.Fatalf("filtered = %d events, want 2", len(filtered))
	}
	if filtered[0].Timestamp != 3000 || filtered[1].Timestamp != 4000 {
		t.Fatalf("filtered = %+v, want [3000, 4000]", filtered)
	}
}

// TestComputeConnectionInfoScoped verifies that after filtering, disconnect
// stats only reflect disconnects within the window.
func TestComputeConnectionInfoScoped(t *testing.T) {
	events := []ConnectionEvent{
		// Before the window (excluded by filter).
		{Status: "disconnected", Timestamp: 1000},
		{Status: "connected", Timestamp: 2000},
		// Inside the window.
		{Status: "disconnected", Timestamp: 3000},
		{Status: "connected", Timestamp: 3500}, // 500ms
		{Status: "disconnected", Timestamp: 4000},
		{Status: "connected", Timestamp: 4600}, // 600ms
	}
	filtered := filterConnectionEvents(events, 2500, 0)
	ci := computeConnectionInfo(filtered)
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
