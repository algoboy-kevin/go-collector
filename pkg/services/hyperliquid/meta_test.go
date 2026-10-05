package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stubInfo stands in for the connector's /info client.
type stubInfo struct {
	meta      []byte
	metaErr   error
	metaCalls int

	perpDexs      []byte
	perpDexsCalls int
}

func (s *stubInfo) MetaRaw(context.Context) ([]byte, error) {
	s.metaCalls++
	return s.meta, s.metaErr
}

func (s *stubInfo) PerpDexsRaw(context.Context) ([]byte, error) {
	s.perpDexsCalls++
	return s.perpDexs, nil
}

// ─────────────────────────────────────────────────────────────
// meta.json
// ─────────────────────────────────────────────────────────────

func TestFetchMetaWritesTheBodyVerbatim(t *testing.T) {
	root := t.TempDir()
	body := []byte(`{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50}]}`)
	info := &stubInfo{meta: body}

	opts := MetaOptions{Attempts: 1}
	if err := FetchMeta(context.Background(), info, root, opts); err != nil {
		t.Fatalf("FetchMeta: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(root, MetaFile))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	// Byte-for-byte: the ingest builds instruments from this, so any reformatting here
	// would be a silent reinterpretation of venue data.
	if string(got) != string(body) {
		t.Errorf("meta.json was modified:\n got %s\nwant %s", got, body)
	}
	assertNotExists(t, filepath.Join(root, MetaFile+".tmp"))
}

func TestFetchMetaRetriesTransientFailures(t *testing.T) {
	root := t.TempDir()
	info := &flakyInfo{failures: 2, body: []byte(`{"ok":true}`)}

	opts := MetaOptions{Attempts: 3, Delay: time.Millisecond}
	if err := FetchMeta(context.Background(), info, root, opts); err != nil {
		t.Fatalf("FetchMeta: %v", err)
	}
	if info.calls != 3 {
		t.Errorf("calls = %d, want 3 (two failures then success)", info.calls)
	}
	if _, err := os.Stat(filepath.Join(root, MetaFile)); err != nil {
		t.Errorf("meta.json should exist: %v", err)
	}
}

type flakyInfo struct {
	failures int
	calls    int
	body     []byte
}

func (f *flakyInfo) MetaRaw(context.Context) ([]byte, error) {
	f.calls++
	if f.calls <= f.failures {
		return nil, errors.New("connection reset")
	}
	return f.body, nil
}

func (f *flakyInfo) PerpDexsRaw(context.Context) ([]byte, error) { return nil, nil }

func TestFetchMetaFailureLeavesNoPartialFile(t *testing.T) {
	root := t.TempDir()
	info := &stubInfo{metaErr: errors.New("503 from a proxy")}

	opts := MetaOptions{Attempts: 3, Delay: time.Millisecond}
	err := FetchMeta(context.Background(), info, root, opts)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error should mention the attempt count, got: %v", err)
	}
	// A half-written meta.json that parses as JSON would be worse than none: the run
	// must fail loudly instead.
	assertNotExists(t, filepath.Join(root, MetaFile))
	assertNotExists(t, filepath.Join(root, MetaFile+".tmp"))
}

func TestFetchMetaStopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := FetchMeta(ctx, &stubInfo{metaErr: errors.New("nope")}, t.TempDir(),
		MetaOptions{Attempts: 100, Delay: time.Hour})
	if err == nil {
		t.Fatal("expected an error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancel should abort the retry loop immediately, took %s", elapsed)
	}
}

// ─────────────────────────────────────────────────────────────
// manifest.json
// ─────────────────────────────────────────────────────────────

func TestWriteManifestIsAtomicAndRewritable(t *testing.T) {
	root := t.TempDir()

	first := Manifest{Venue: "hyperliquid", StartedNS: 1, Coins: []string{"BTC"}}
	if err := WriteManifest(root, first); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	assertNotExists(t, filepath.Join(root, ManifestFile+".tmp"))

	// The shutdown rewrite is the point: the same file gains the outcome.
	second := first
	second.EndedNS = 2
	second.StopReason = StopSignal
	second.ChannelReports = []ChannelReport{{Channel: "trades", Frames: 7}}
	if err := WriteManifest(root, second); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	var got Manifest
	raw, err := os.ReadFile(filepath.Join(root, ManifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("manifest is not JSON: %v", err)
	}
	if got.EndedNS != 2 || got.StopReason != StopSignal {
		t.Errorf("rewrite did not take: %+v", got)
	}
	if len(got.ChannelReports) != 1 || got.ChannelReports[0].Frames != 7 {
		t.Errorf("channel reports = %+v", got.ChannelReports)
	}
}

func TestManifestHeaderOmitsUnsetL2BookParams(t *testing.T) {
	// Absent params must be distinguishable from "we asked for the venue default",
	// which is why the field is a pointer.
	unset := mustLoad(t, baseConfig)
	m := unset.ManifestHeader(time.Unix(0, 1), 6)
	if m.L2BookParams != nil {
		t.Errorf("L2BookParams = %+v, want omitted", m.L2BookParams)
	}
	if m.Environment != "mainnet" {
		t.Errorf("Environment = %q", m.Environment)
	}
	if m.CollectorVersion == "" || m.ConnectorVersion == "" {
		t.Error("versions must be stamped so a capture can name the code that wrote it")
	}

	set := mustLoad(t, "coins: [BTC]\nchannels: [l2Book]\nl2Book:\n  fast: true\n")
	if got := set.ManifestHeader(time.Unix(0, 1), 6); got.L2BookParams == nil || !got.L2BookParams.Fast {
		t.Errorf("L2BookParams = %+v, want fast recorded", got.L2BookParams)
	}
}

func TestEnvironmentDetectsTestnet(t *testing.T) {
	cases := map[string]string{
		"wss://api.hyperliquid.xyz/ws":         "mainnet",
		"wss://api.hyperliquid-testnet.xyz/ws": "testnet",
		"":                                     "mainnet",
	}
	for url, want := range cases {
		if got := Environment(url); got != want {
			t.Errorf("Environment(%q) = %q, want %q", url, got, want)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Sentinels
// ─────────────────────────────────────────────────────────────

func TestSentinelLineShape(t *testing.T) {
	rx := time.Date(2026, 10, 4, 14, 0, 0, 123456789, time.UTC)
	line, err := SentinelLine(EventResubscribed, rx)
	if err != nil {
		t.Fatalf("SentinelLine: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("sentinel is not JSON: %v (%s)", err, line)
	}
	if got["channel"] != MetaChannel {
		t.Errorf("channel = %v, want %q", got["channel"], MetaChannel)
	}
	if got["event"] != EventResubscribed {
		t.Errorf("event = %v, want %q", got["event"], EventResubscribed)
	}
	if got["ts_ns"] != float64(rx.UnixNano()) {
		t.Errorf("ts_ns = %v, want %d", got["ts_ns"], rx.UnixNano())
	}
	if strings.Contains(string(line), "\n") {
		t.Error("a sentinel must be a single line: the writer appends the newline")
	}
}

func TestDisconnectedSentinelCarriesNoReasonYet(t *testing.T) {
	// Pins the known gap so it cannot regress silently: if the connector ever starts
	// passing the error through, this test should be updated deliberately rather than
	// the shape changing by accident.
	line, err := SentinelLine(EventDisconnected, time.Unix(0, 1))
	if err != nil {
		t.Fatalf("SentinelLine: %v", err)
	}
	if strings.Contains(string(line), "reason") {
		t.Errorf("disconnected gained a reason field — the connector now reports the error; "+
			"wire it through and update PERP_COLLECTOR_SPEC.md §8 item 8: %s", line)
	}
}
