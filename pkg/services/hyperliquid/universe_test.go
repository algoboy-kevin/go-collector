package hyperliquid

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────
// Coin -> dex
// ─────────────────────────────────────────────────────────────

func TestCoinDex(t *testing.T) {
	cases := map[string]string{
		"BTC":          DefaultDex,
		"SOL":          DefaultDex,
		"vntl:OPENAI":  "vntl",
		"xyz:JP225":    "xyz",
		"io:OAI":       "io",
		"kPEPE":        DefaultDex,
		":BROKEN":      DefaultDex, // no prefix, so it is just an unknown default-dex name
		"vntl:OPEN:AI": "vntl",     // only the first colon splits
	}
	for coin, want := range cases {
		if got := CoinDex(coin); got != want {
			t.Errorf("CoinDex(%q) = %q, want %q", coin, got, want)
		}
	}
}

func TestGroupCoinsByDex(t *testing.T) {
	groups := GroupCoinsByDex([]string{"vntl:OPENAI", "BTC", "xyz:JP225", "SOL", "vntl:SPACEX"})

	if len(groups) != 3 {
		t.Fatalf("got %d groups, want 3: %+v", len(groups), groups)
	}
	// Default dex sorts first (empty string), then alphabetical.
	if !groups[0].IsDefault() {
		t.Errorf("first group should be the default dex, got %q", groups[0].Dex)
	}
	if got := strings.Join(groups[0].Coins, ","); got != "BTC,SOL" {
		t.Errorf("default dex coins = %q, want BTC,SOL", got)
	}
	if groups[1].Dex != "vntl" || len(groups[1].Coins) != 2 {
		t.Errorf("second group = %+v, want vntl with 2 coins", groups[1])
	}
	if groups[2].Dex != "xyz" || len(groups[2].Coins) != 1 {
		t.Errorf("third group = %+v, want xyz with 1 coin", groups[2])
	}
}

func TestDexMetaFile(t *testing.T) {
	if got, want := DexMetaFile("vntl"), "meta-vntl.json"; got != want {
		t.Errorf("DexMetaFile = %q, want %q", got, want)
	}
}

func TestParseUniverseRejectsGarbage(t *testing.T) {
	if _, err := ParseUniverse([]byte("not json")); err == nil {
		t.Fatal("expected an error for a non-JSON body")
	}
	u, err := ParseUniverse(universeJSON("BTC", "ETH"))
	if err != nil {
		t.Fatalf("ParseUniverse: %v", err)
	}
	if u.Len() != 2 || !u.Has("BTC") || u.Has("btc") {
		t.Errorf("universe = %+v (case must matter at the venue)", u.Assets)
	}
}

func TestParseUniverseReadsDelistedAndSzDecimals(t *testing.T) {
	// The venue keeps a delisted market in its universe and still accepts the
	// subscription, so this flag is the only thing that distinguishes it from a live one
	// before frames start arriving — and the frames make it look healthy, because l2Book
	// is paced and its counts match every other coin's.
	raw := []byte(`{"universe":[
		{"name":"BTC","szDecimals":5},
		{"name":"vntl:OPENAI","szDecimals":3,"isDelisted":true}
	]}`)
	u, err := ParseUniverse(raw)
	if err != nil {
		t.Fatalf("ParseUniverse: %v", err)
	}
	btc, ok := u.Asset("BTC")
	if !ok || btc.IsDelisted || btc.SzDecimals != 5 {
		t.Errorf("BTC = %+v, want szDecimals 5 and not delisted", btc)
	}
	openai, ok := u.Asset("vntl:OPENAI")
	if !ok || !openai.IsDelisted || openai.SzDecimals != 3 {
		t.Errorf("vntl:OPENAI = %+v, want delisted with szDecimals 3", openai)
	}
}

func TestDelistedPolicyDecidesWhoIsSubscribed(t *testing.T) {
	universes := map[string]Universe{
		DefaultDex: {Assets: map[string]Asset{"BTC": {Name: "BTC"}}},
		"vntl":     {Assets: map[string]Asset{"vntl:OPENAI": {Name: "vntl:OPENAI", IsDelisted: true}}},
	}
	coins := []string{"BTC", "vntl:OPENAI"}

	kept, skipped, err := ApplyDelistedPolicy(coins, universes, DelistedSkip)
	if err != nil {
		t.Fatalf("skip: %v", err)
	}
	if len(kept) != 1 || kept[0] != "BTC" {
		t.Errorf("kept = %v, want just BTC", kept)
	}
	if len(skipped) != 1 || skipped[0].Coin != "vntl:OPENAI" || skipped[0].Reason != "isDelisted" {
		t.Errorf("skipped = %+v, want vntl:OPENAI with a reason", skipped)
	}

	// record: subscribe anyway, and claim nothing was skipped.
	kept, skipped, err = ApplyDelistedPolicy(coins, universes, DelistedRecord)
	if err != nil || len(kept) != 2 || len(skipped) != 0 {
		t.Errorf("record: kept=%v skipped=%v err=%v", kept, skipped, err)
	}

	// fail: the operator asked to be certain every coin is live.
	if _, _, err := ApplyDelistedPolicy(coins, universes, DelistedFail); err == nil {
		t.Error("fail: a delisted coin must stop the run")
	}

	// An unknown coin is NOT a delisted coin: it must reach checkCoinsExist, which
	// reports it properly, rather than being dropped here as if the policy covered it.
	kept, skipped, err = ApplyDelistedPolicy([]string{"NOPE"}, universes, DelistedSkip)
	if err != nil || len(kept) != 1 || kept[0] != "NOPE" || len(skipped) != 0 {
		t.Errorf("unknown coin: kept=%v skipped=%v err=%v", kept, skipped, err)
	}
}

func TestUniverseFingerprintTracksWhatDecidesInstruments(t *testing.T) {
	metas := map[string][]byte{"meta.json": []byte(`{"universe":[]}`)}
	base := UniverseFingerprint([]string{"BTC", "SOL"}, []string{"bbo", "trades"}, nil, metas)

	if base != UniverseFingerprint([]string{"SOL", "BTC"}, []string{"trades", "bbo"}, nil, metas) {
		t.Error("coin and channel order must not change the fingerprint")
	}

	// The coin set is part of the experiment.
	if base == UniverseFingerprint([]string{"BTC"}, []string{"bbo", "trades"}, nil, metas) {
		t.Error("adding a coin must change the fingerprint")
	}
	// So is the metadata, because that is where szDecimals comes from — the field the
	// consumer derives price precision from.
	other := map[string][]byte{"meta.json": []byte(`{"universe":[{"name":"BTC","szDecimals":4}]}`)}
	if base == UniverseFingerprint([]string{"BTC", "SOL"}, []string{"bbo", "trades"}, nil, other) {
		t.Error("different meta bytes must change the fingerprint")
	}
	// So is the thinning, since it changes what the book frames contain.
	if base == UniverseFingerprint([]string{"BTC", "SOL"}, []string{"bbo", "trades"}, &L2BookParams{Fast: true}, metas) {
		t.Error("l2Book thinning must change the fingerprint")
	}
}

// ─────────────────────────────────────────────────────────────
// Startup metadata
// ─────────────────────────────────────────────────────────────

func TestFetchStartupMetaWritesOneFilePerDex(t *testing.T) {
	root := t.TempDir()
	info := newStubInfo("BTC", "SOL")
	info.dexMeta = map[string][]byte{
		"vntl": universeJSON("vntl:OPENAI", "vntl:SPACEX"),
		"xyz":  universeJSON("xyz:JP225"),
	}

	dumps, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"BTC", "vntl:OPENAI", "xyz:JP225", "SOL"}, MetaOptions{Attempts: 1})
	if err != nil {
		t.Fatalf("FetchStartupMeta: %v", err)
	}

	// The default dex keeps the documented file name, byte for byte.
	assertExists(t, filepath.Join(root, MetaFile))
	if got, err := os.ReadFile(filepath.Join(root, MetaFile)); err != nil || !strings.Contains(string(got), `"BTC"`) {
		t.Errorf("meta.json should hold the default universe, got %q (err %v)", got, err)
	}
	// Each HIP-3 dex gets its own, because meta.json cannot describe its instruments.
	assertExists(t, filepath.Join(root, "meta-vntl.json"))
	assertExists(t, filepath.Join(root, "meta-xyz.json"))

	if dumps.DefaultAssets != 2 {
		t.Errorf("DefaultAssets = %d, want 2", dumps.DefaultAssets)
	}
	if len(dumps.Dexes) != 2 {
		t.Fatalf("Dexes = %+v, want 2 entries", dumps.Dexes)
	}
	for _, d := range dumps.Dexes {
		if d.MetaFile != DexMetaFile(d.Dex) {
			t.Errorf("%s: MetaFile = %q", d.Dex, d.MetaFile)
		}
	}
	if got := dumps.Describe(); !strings.Contains(got, "vntl=2 assets") {
		t.Errorf("Describe = %q", got)
	}
}

func TestFetchStartupMetaOnlyFetchesReferencedDexes(t *testing.T) {
	// A default-dex-only config must not pay for HIP-3 lookups it does not need.
	root := t.TempDir()
	info := newStubInfo("BTC")

	if _, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"BTC"}, MetaOptions{Attempts: 1}); err != nil {
		t.Fatalf("FetchStartupMeta: %v", err)
	}
	if info.postCalls != 0 {
		t.Errorf("postCalls = %d, want 0 for a default-dex-only config", info.postCalls)
	}
	assertNotExists(t, filepath.Join(root, "meta-vntl.json"))
}

func TestFetchStartupMetaRejectsUnknownCoins(t *testing.T) {
	// The whole point: a coin the venue does not know gets the *connection* closed, so
	// it used to surface as an unexplained reconnect storm. It must be an error instead.
	root := t.TempDir()
	info := newStubInfo("BTC", "SOL")

	_, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"BTC", "JP225", "OPENAI"}, MetaOptions{Attempts: 1})
	if err == nil {
		t.Fatal("an unknown coin must fail the run")
	}
	// Both bad coins named at once, so one run fixes both.
	for _, want := range []string{"JP225", "OPENAI", "not on the default dex", "refusing to record"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}

	// The universes are still written: they are exactly what you need to find the right
	// name, and the error tells the reader to look there.
	assertExists(t, filepath.Join(root, MetaFile))
}

func TestFetchStartupMetaNamesTheDexForAPrefixedCoin(t *testing.T) {
	root := t.TempDir()
	info := newStubInfo("BTC")
	info.dexMeta = map[string][]byte{"vntl": universeJSON("vntl:SPACEX")}

	_, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"vntl:OPENAI"}, MetaOptions{Attempts: 1})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), `dex "vntl"`) {
		t.Errorf("error should name the dex it searched, got:\n%v", err)
	}
}

func TestFetchStartupMetaFailsWhenADexMetaIsUnavailable(t *testing.T) {
	// A HIP-3 dex's meta is not optional: without it the ingest cannot build the
	// instrument, so the frames for that market are unusable.
	root := t.TempDir()
	info := newStubInfo("BTC")
	info.postErr = errors.New("503 from a proxy")

	_, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"BTC", "vntl:OPENAI"}, MetaOptions{Attempts: 1})
	if err == nil {
		t.Fatal("a missing dex meta must fail the run")
	}
	if !strings.Contains(err.Error(), "meta-vntl.json") {
		t.Errorf("error should name the file it could not write, got:\n%v", err)
	}
}

func TestFetchStartupMetaRetriesDexFetches(t *testing.T) {
	// Every startup dump gets the same retry tolerance, not just the default dex.
	root := t.TempDir()
	info := &flakyPostInfo{failures: 1, dex: "vntl", body: universeJSON("vntl:OPENAI")}

	_, err := FetchStartupMeta(context.Background(), info, root,
		[]string{"vntl:OPENAI"}, MetaOptions{Attempts: 3, Delay: time.Millisecond})
	if err != nil {
		t.Fatalf("FetchStartupMeta: %v", err)
	}
	if info.calls != 2 {
		t.Errorf("calls = %d, want 2 (one failure then success)", info.calls)
	}
}

type flakyPostInfo struct {
	failures int
	calls    int
	dex      string
	body     []byte
}

func (f *flakyPostInfo) MetaRaw(context.Context) ([]byte, error) { return universeJSON(), nil }

func (f *flakyPostInfo) PerpDexsRaw(context.Context) ([]byte, error) { return nil, nil }

func (f *flakyPostInfo) Post(_ context.Context, payload any) ([]byte, error) {
	f.calls++
	if f.calls <= f.failures {
		return nil, errors.New("connection reset")
	}
	if body, ok := payload.(map[string]any); ok && body["dex"] == f.dex {
		return f.body, nil
	}
	return nil, errors.New("unexpected dex")
}
