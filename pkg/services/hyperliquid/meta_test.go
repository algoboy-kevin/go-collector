package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	// postErr fails every Post. dexMeta serves HIP-3 universes by dex name.
	postErr   error
	postCalls int
	dexMeta   map[string][]byte
}

func (s *stubInfo) MetaRaw(context.Context) ([]byte, error) {
	s.metaCalls++
	return s.meta, s.metaErr
}

func (s *stubInfo) PerpDexsRaw(context.Context) ([]byte, error) {
	s.perpDexsCalls++
	return s.perpDexs, nil
}

func (s *stubInfo) Post(_ context.Context, payload any) ([]byte, error) {
	s.postCalls++
	if s.postErr != nil {
		return nil, s.postErr
	}
	body, ok := payload.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("stub: unexpected payload %T", payload)
	}
	dex, _ := body["dex"].(string)
	if raw, ok := s.dexMeta[dex]; ok {
		return raw, nil
	}
	return nil, fmt.Errorf("stub: no meta for dex %q", dex)
}

// newStubInfo builds a stub whose default-dex universe contains exactly these assets.
func newStubInfo(assets ...string) *stubInfo {
	return &stubInfo{meta: universeJSON(assets...)}
}

// universeJSON renders a `{"type":"meta"}` body listing these asset names.
func universeJSON(assets ...string) []byte {
	var b strings.Builder
	b.WriteString(`{"universe":[`)
	for i, a := range assets {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"name":%q,"szDecimals":5}`, a)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
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

func (f *flakyInfo) Post(context.Context, any) ([]byte, error) {
	return nil, errors.New("flakyInfo: no dex metas")
}

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

	first := Manifest{RunID: "2026-10-05T080959Z", Venue: "hyperliquid", StartedNS: 1, Coins: CoinRefsFor([]string{"BTC"})}
	if err := WriteManifest(root, first); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	assertNotExists(t, filepath.Join(root, ManifestFile+".tmp"))

	// The shutdown rewrite is the point: the run's own file gains the outcome.
	second := first
	second.EndedNS = 2
	second.StopReason = StopSignal
	second.ChannelReports = []ChannelReport{{Channel: "trades", Frames: 7}}
	if err := WriteManifest(root, second); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	var got Manifest
	raw, err := os.ReadFile(ManifestPath(root, first.RunID))
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

func TestASecondRunCannotDestroyTheFirstRunsManifest(t *testing.T) {
	// The loss this exists to prevent: one manifest.json was written at startup and
	// rewritten at shutdown, so the next run on the same root erased the previous run's
	// record — its window, counts and stop reason — while that run's frames survived in
	// quarantine. The capture kept the data and lost the explanation.
	root := t.TempDir()

	// The aborted run, whose frames ended up quarantined.
	aborted := Manifest{
		RunID: "2026-10-05T080959Z", Venue: "hyperliquid", StartedNS: 100,
		StopReason: StopSignal, EndedNS: 200, Coins: CoinRefsFor([]string{"BTC"}),
		ChannelReports: []ChannelReport{{Channel: "bbo", Frames: 1234}},
	}
	if err := WriteManifest(root, aborted); err != nil {
		t.Fatalf("WriteManifest(aborted): %v", err)
	}

	// The 2h run that displaced it, exactly as it happened.
	full := Manifest{
		RunID: "2026-10-05T084829Z", Venue: "hyperliquid", StartedNS: 300,
		StopReason: StopDuration, EndedNS: 400, Coins: CoinRefsFor([]string{"BTC"}),
		ChannelReports: []ChannelReport{{Channel: "bbo", Frames: 103805}},
	}
	if err := WriteManifest(root, full); err != nil {
		t.Fatalf("WriteManifest(full): %v", err)
	}

	got, err := ReadManifests(root)
	if err != nil {
		t.Fatalf("ReadManifests: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d run records, want 2 — the first run's provenance was destroyed", len(got))
	}
	// Chronological order, because run ids sort as strings.
	if got[0].RunID != aborted.RunID || got[1].RunID != full.RunID {
		t.Errorf("records out of order: %q then %q", got[0].RunID, got[1].RunID)
	}
	if got[0].Frames() != 1234 || got[0].StopReason != StopSignal {
		t.Errorf("the aborted run's outcome was lost: %+v", got[0].ChannelReports)
	}

	// The convenience copy tracks the newest run, and says which one that is.
	raw, err := os.ReadFile(filepath.Join(root, ManifestFile))
	if err != nil {
		t.Fatalf("read %s: %v", ManifestFile, err)
	}
	var copyOf Manifest
	if err := json.Unmarshal(raw, &copyOf); err != nil {
		t.Fatalf("manifest.json is not JSON: %v", err)
	}
	if copyOf.RunID != full.RunID {
		t.Errorf("manifest.json names run %q, want the newest (%q)", copyOf.RunID, full.RunID)
	}
	// Per-run records must carry a unique id: without it they overwrite each other again.
	if err := WriteManifest(root, Manifest{Venue: "hyperliquid"}); err == nil {
		t.Error("a manifest with no run_id must be refused, not silently written")
	}
}

func TestSameSecondRunsCannotShareAManifestFile(t *testing.T) {
	// Second resolution plus "one recorder per root" rules out CONCURRENT runs, but not a
	// sequential restart inside the same second — which is exactly what a crash loop does.
	// Without disambiguation the restart overwrites the previous run's manifest, the same
	// provenance loss the per-run file exists to prevent.
	root := t.TempDir()
	base := RunID(time.Date(2026, 10, 5, 8, 48, 29, 0, time.UTC))

	first := uniqueRunID(root, base)
	if first != base {
		t.Fatalf("first run id = %q, want the bare %q", first, base)
	}
	if err := WriteManifest(root, Manifest{RunID: first, Venue: "hyperliquid"}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	second := uniqueRunID(root, base)
	if second == first {
		t.Fatal("a second run in the same second reused the first run's id")
	}
	if !strings.HasPrefix(second, base) {
		t.Errorf("second id = %q, want it to keep the %q prefix so ids still sort by time", second, base)
	}
	if err := WriteManifest(root, Manifest{RunID: second, Venue: "hyperliquid"}); err != nil {
		t.Fatalf("WriteManifest(second): %v", err)
	}

	runs, err := ReadManifests(root)
	if err != nil {
		t.Fatalf("ReadManifests: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d manifests, want 2 — one run's record was overwritten", len(runs))
	}
	// Prefix ordering keeps the pair in time order, which is what makes "newest" a scan.
	if runs[0].RunID != first || runs[1].RunID != second {
		t.Errorf("records out of order: %q then %q", runs[0].RunID, runs[1].RunID)
	}

	third := uniqueRunID(root, base)
	if third == first || third == second {
		t.Errorf("third id %q collides with an earlier one", third)
	}
}

func TestManifestHeaderOmitsUnsetL2BookParams(t *testing.T) {
	// Absent params must be distinguishable from "we asked for the venue default",
	// which is why the field is a pointer.
	unset := mustLoad(t, baseConfig)
	m := unset.ManifestHeader(time.Unix(0, 1), "run", 6)
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
	if got := set.ManifestHeader(time.Unix(0, 1), "run", 6); got.L2BookParams == nil || !got.L2BookParams.Fast {
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
	line, err := SentinelLine(Sentinel{Event: EventSubscribed, RunID: "2026-10-04T140000Z"}, rx)
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
	if got["event"] != EventSubscribed {
		t.Errorf("event = %v, want %q", got["event"], EventSubscribed)
	}
	if got["ts_ns"] != float64(rx.UnixNano()) {
		t.Errorf("ts_ns = %v, want %d", got["ts_ns"], rx.UnixNano())
	}
	if got["run_id"] != "2026-10-04T140000Z" {
		t.Errorf("run_id = %v, want the run it belongs to", got["run_id"])
	}
	// Only the fields relevant to the event may appear: a `count` on a `subscribed` mark
	// would be a number with no meaning.
	for _, absent := range []string{"reason", "count", "file", "err"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%q must be omitted when unset, got %s", absent, line)
		}
	}
	if strings.Contains(string(line), "\n") {
		t.Error("a sentinel must be a single line: the writer appends the newline")
	}
}

func TestDisconnectedSentinelCarriesTheReason(t *testing.T) {
	// RECORDER_SPEC.md §3 asks for a `reason` on `disconnected`. It can be filled from
	// go-exchange-connector v0.7.2, which added SetOnDisconnect(func(error)); this pins
	// the field so it cannot silently disappear again.
	reason := `failed to read: status = policy violation and reason = "Cannot open more than 15 connections."`
	line, err := SentinelLine(Sentinel{Event: EventDisconnected, Reason: reason, RunID: "r"}, time.Unix(0, 1))
	if err != nil {
		t.Fatalf("SentinelLine: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("sentinel is not JSON: %v (%s)", err, line)
	}
	if got["reason"] != reason {
		t.Errorf("reason = %v, want %q", got["reason"], reason)
	}
	if strings.Contains(string(line), "\n") {
		// The reason is free text from the venue. A newline in it would split the line
		// and corrupt the file, so this is checked rather than assumed.
		t.Error("the reason must be escaped, not embedded: a sentinel is one line")
	}
}

func TestLossSentinelsCarryWhatTheManifestCannot(t *testing.T) {
	// Drops and write errors are counted in the manifest, but the consumer reads the
	// stream — so a hole in the data needs a cause in the data.
	line, err := SentinelLine(Sentinel{Event: EventDropped, Count: 42, RunID: "r"}, time.Unix(0, 1))
	if err != nil {
		t.Fatalf("SentinelLine: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("sentinel is not JSON: %v", err)
	}
	if got["event"] != EventDropped || got["count"] != float64(42) {
		t.Errorf("dropped mark = %s, want event=dropped count=42", line)
	}

	line, err = SentinelLine(Sentinel{
		Event: EventWriteError, File: "2026-10-05T08.jsonl.gz", Err: "no space left on device",
	}, time.Unix(0, 1))
	if err != nil {
		t.Fatalf("SentinelLine: %v", err)
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("sentinel is not JSON: %v", err)
	}
	if got["file"] != "2026-10-05T08.jsonl.gz" || got["err"] != "no space left on device" {
		t.Errorf("write_error mark = %s, want the file and the error", line)
	}
}

func TestRunIDIsTheStartInstantToTheSecond(t *testing.T) {
	// The layout is load-bearing: it must sort as a plain string, match the hour-bucket
	// convention, and be stable across time zones (it is always UTC).
	// This is the real run from the 2h capture: started 2026-10-05 08:48:29 UTC.
	started := time.Date(2026, 10, 5, 8, 48, 29, 249452338, time.UTC)
	if got := RunID(started); got != "2026-10-05T084829Z" {
		t.Errorf("RunID = %q, want %q", got, "2026-10-05T084829Z")
	}
	// A run in a non-UTC zone must name the same instant, not the local wall clock.
	zone := time.FixedZone("UTC+7", 7*3600)
	if got := RunID(started.In(zone)); got != "2026-10-05T084829Z" {
		t.Errorf("RunID in a non-UTC zone = %q, want the UTC instant", got)
	}
	// Sub-second precision must not leak in: two runs a fraction apart within the same
	// second collide, which is why deploy-hl.sh's single-instance rule backs this up.
	if a, b := RunID(started), RunID(started.Add(700*time.Millisecond)); a != b {
		t.Errorf("same-second runs must share an id: %q vs %q", a, b)
	}
}

func TestDisconnectReasonHandlesTheShapesTheConnectorEmits(t *testing.T) {
	// A deliberate Disconnect() reports nil, which must not become the literal "<nil>".
	if got := DisconnectReason(nil); got != "" {
		t.Errorf("DisconnectReason(nil) = %q, want empty", got)
	}
	if got := DisconnectReason(errors.New("  failed to read: EOF  ")); got != "failed to read: EOF" {
		t.Errorf("DisconnectReason trimmed = %q, want %q", got, "failed to read: EOF")
	}
}
