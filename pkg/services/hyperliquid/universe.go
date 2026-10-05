package hyperliquid

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
)

// DefaultDex is the dex prefix of the main perpetual universe. Coins there carry no
// prefix ("BTC"); everything else is a HIP-3 builder-deployed market and its name
// includes the dex ("vntl:OPENAI").
const DefaultDex = ""

// Asset is one entry of a dex's universe, as far as the recorder needs to read it.
type Asset struct {
	Name       string
	SzDecimals int
	// IsDelisted marks a market the venue keeps in its universe but no longer trades.
	//
	// It is worth reading rather than discovering from the data: the venue still accepts
	// the subscription and still paces `l2Book` and `activeAssetCtx`, so the feed looks
	// healthy while carrying nothing actionable. In the capture that prompted this,
	// `vntl:OPENAI` delivered 13,242 l2Book updates and 30 trades over two hours and
	// **zero** `bbo` frames — and because `l2Book` is paced, its frame counts were
	// identical to every live coin's. Only `bbo` and `trades` revealed it.
	IsDelisted bool
}

// Universe is the set of assets one dex reports.
type Universe struct {
	Assets map[string]Asset
}

// Has reports whether the dex lists this exact asset name.
func (u Universe) Has(name string) bool {
	_, ok := u.Assets[name]
	return ok
}

// Asset returns one asset's metadata.
func (u Universe) Asset(name string) (Asset, bool) {
	a, ok := u.Assets[name]
	return a, ok
}

// Len returns how many assets the dex lists.
func (u Universe) Len() int { return len(u.Assets) }

// ParseUniverse reads the asset entries out of a `{"type":"meta"}` response.
//
// This parses the *metadata* response, which is a different thing from the frames the
// recorder handles: meta.json is still stored verbatim and untouched, and this exists
// only so a mistyped coin fails at startup instead of becoming a reconnect loop, and so
// a delisted one can be skipped deliberately.
func ParseUniverse(raw []byte) (Universe, error) {
	var body struct {
		Universe []struct {
			Name       string `json:"name"`
			SzDecimals int    `json:"szDecimals"`
			IsDelisted bool   `json:"isDelisted"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return Universe{}, fmt.Errorf("hyperliquid: parse meta universe: %w", err)
	}
	assets := make(map[string]Asset, len(body.Universe))
	for _, entry := range body.Universe {
		if entry.Name == "" {
			continue
		}
		assets[entry.Name] = Asset{
			Name:       entry.Name,
			SzDecimals: entry.SzDecimals,
			IsDelisted: entry.IsDelisted,
		}
	}
	return Universe{Assets: assets}, nil
}

// CoinDex returns the dex prefix of a coin, or DefaultDex for a market on the main
// universe. "vntl:OPENAI" is on "vntl"; "BTC" is on the default dex.
func CoinDex(coin string) string {
	if i := strings.IndexByte(coin, ':'); i > 0 {
		return coin[:i]
	}
	return DefaultDex
}

// GroupCoinsByDex groups coins by their dex prefix, with the default dex under
// DefaultDex. The result is sorted by dex (default first) for deterministic file
// writing and log output.
func GroupCoinsByDex(coins []string) []DexCoins {
	byDex := map[string][]string{}
	for _, coin := range coins {
		dex := CoinDex(coin)
		byDex[dex] = append(byDex[dex], coin)
	}

	out := make([]DexCoins, 0, len(byDex))
	for dex, group := range byDex {
		out = append(out, DexCoins{Dex: dex, Coins: group})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Dex < out[j].Dex })
	return out
}

// DexCoins is one dex and the configured coins that live on it.
type DexCoins struct {
	Dex   string
	Coins []string
}

// IsDefault reports whether this group is the main universe.
func (d DexCoins) IsDefault() bool { return d.Dex == DefaultDex }

// DexMetaFile is the file name a dex's meta dump is written to.
//
// A separate file rather than a merged universe: meta.json must stay byte-for-byte the
// venue's default-dex response, and a HIP-3 dex's instrument set changes independently.
func DexMetaFile(dex string) string {
	return "meta-" + dex + ".json"
}

// DexFileFor is the meta file a dex's assets are defined in, with the default dex on
// meta.json.
func DexFileFor(dex string) string {
	if dex == DefaultDex {
		return MetaFile
	}
	return DexMetaFile(dex)
}

// CoinRef identifies a recorded coin and where its instrument definition lives.
//
// The dex is not decoration. meta.json holds the DEFAULT dex only, so a consumer that
// reads that one file cannot build an instrument for `vntl:OPENAI` and silently drops
// its frames — which is exactly what happened on the first ingest: 2 of 5 recorded coins
// were rejected as `unknown_coin`, discarding 26,489 of 66,358 l2Book updates. Naming
// the dex and the file per coin is what makes the capture self-routing.
type CoinRef struct {
	Coin     string `json:"coin"`
	Dex      string `json:"dex,omitempty"`
	MetaFile string `json:"meta_file"`
}

// CoinRefsFor renders a coin list as manifest entries.
func CoinRefsFor(coins []string) []CoinRef {
	out := make([]CoinRef, 0, len(coins))
	for _, coin := range coins {
		dex := CoinDex(coin)
		out = append(out, CoinRef{Coin: coin, Dex: dex, MetaFile: DexFileFor(dex)})
	}
	return out
}

// CoinSkip records a configured coin that was not recorded, and why.
//
// A skipped coin is a decision, and a decision that is not written down is
// indistinguishable later from a coin that was never asked for.
type CoinSkip struct {
	Coin   string `json:"coin"`
	Reason string `json:"reason"`
}

// Delisted-coin policies, for `delisted_policy` in the config.
const (
	// DelistedSkip leaves a delisted coin out of the subscriptions and records it in
	// `coins_skipped`. The default: the market exists but does not trade, so subscribing
	// spends a per-IP subscription and one of a very small number of channels on silence.
	DelistedSkip = "skip"
	// DelistedRecord subscribes anyway, for a study that needs the venue's own silence on
	// a delisted market to be part of the capture.
	DelistedRecord = "record"
	// DelistedFail refuses to start, for a run that must be certain every configured coin
	// is live.
	DelistedFail = "fail"
)

// ApplyDelistedPolicy splits the configured coins into the ones to record and the ones
// to skip, according to the policy.
//
// The policy decides the *subscription set*; nothing here touches the meta dumps, so a
// skipped coin's instrument definition is still in the capture and the decision is
// reversible from the manifest. That matters because the alternative — not fetching a
// skipped dex — would make the skip unrecoverable.
func ApplyDelistedPolicy(coins []string, universes map[string]Universe, policy string) ([]string, []CoinSkip, error) {
	if policy == DelistedRecord {
		return coins, nil, nil
	}

	kept := make([]string, 0, len(coins))
	var skipped []CoinSkip
	for _, coin := range coins {
		asset, ok := universes[CoinDex(coin)].Asset(coin)
		if !ok || !asset.IsDelisted {
			// Not listed at all: leave it to checkCoinsExist, which reports it with the
			// right message rather than quietly dropping it here.
			kept = append(kept, coin)
			continue
		}
		if policy == DelistedFail {
			return nil, nil, fmt.Errorf("hyperliquid: coin %q is delisted on %s and delisted_policy is %q",
				coin, dexLabel(CoinDex(coin)), DelistedFail)
		}
		slog.Warn("hyperliquid: skipping delisted coin", "coin", coin, "dex", dexLabel(CoinDex(coin)))
		skipped = append(skipped, CoinSkip{Coin: coin, Reason: "isDelisted"})
	}
	return kept, skipped, nil
}

// UniverseFingerprint digests everything that decides what a capture's instruments are:
// the recorded coins, the channels, the l2Book thinning, and the bytes of every meta
// dump.
//
// Its purpose is to refuse to blend two different experiments into one directory. A
// consumer builds ONE instrument set from ONE meta.json and derives ONE price precision
// per coin from the whole directory, so a second session with a different universe — or
// the same coin at different szDecimals — silently contaminates the first: the catalog
// either rejects the write with a confusing "mixed identities" error or, worse, the
// backtest fills at the wrong prices. Comparing fingerprints at startup turns that into
// one sentence before any data is written.
func UniverseFingerprint(coins, channels []string, l2 *L2BookParams, metas map[string][]byte) string {
	h := sha256.New()

	sortedCoins := append([]string(nil), coins...)
	sort.Strings(sortedCoins)
	for _, coin := range sortedCoins {
		fmt.Fprintf(h, "coin:%s\n", coin)
	}

	sortedChannels := append([]string(nil), channels...)
	sort.Strings(sortedChannels)
	for _, channel := range sortedChannels {
		fmt.Fprintf(h, "channel:%s\n", channel)
	}

	if l2 == nil {
		fmt.Fprintln(h, "l2:venue-default")
	} else {
		fmt.Fprintf(h, "l2:fast=%t,nSigFigs=%d,mantissa=%d\n", l2.Fast, l2.NSigFigs, l2.Mantissa)
	}

	// The meta bytes are the load-bearing part: they carry szDecimals per coin, which is
	// what the price/size precision is derived from.
	names := make([]string, 0, len(metas))
	for name := range metas {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(h, "meta:%s:\n", name)
		h.Write(metas[name])
		h.Write([]byte("\n"))
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// DexDump records one dex's metadata dump in the manifest.
type DexDump struct {
	Dex      string `json:"dex"`
	Assets   int    `json:"assets"`
	MetaFile string `json:"meta_file"`
}

// MetaDumps reports what the startup metadata fetch produced.
type MetaDumps struct {
	DefaultAssets int
	Dexes         []DexDump

	// Universes is every fetched dex's parsed universe, keyed by dex (the default dex
	// under DefaultDex). The delisted-coin policy and the coin existence check both need
	// it, and neither should have to re-fetch or re-parse to get it.
	Universes map[string]Universe

	// Metas is the raw body of each dump keyed by file name, so the universe fingerprint
	// covers the exact bytes that were written rather than a re-serialization of them.
	Metas map[string][]byte

	// MetaSHA256 is the digest of each dump, recorded in the manifest so a copy of a
	// capture can be checked against it by reading the copy alone.
	MetaSHA256 map[string]string
}

// sha256Hex is the bare digest spelling used for file and meta identities.
func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// FetchStartupMeta writes the instrument metadata a capture needs and checks that every
// configured coin actually exists.
//
// Three things happen here, and all three must succeed before a frame is recorded:
//
//  1. `meta.json` — the default dex universe, verbatim.
//  2. `meta-<dex>.json` for every dex named by a configured coin. Without this the
//     ingest has no definition for a HIP-3 market and cannot build its instrument,
//     which makes those frames unusable rather than merely incomplete.
//  3. Every configured coin is checked against its own dex's universe.
//
// Step 3 is the one that turns a whole class of outage into a single error line.
// Subscribing to a coin the venue does not know gets the *connection* closed rather
// than an error frame, so a typo used to surface as an unexplained reconnect storm
// burning the venue's connection budget — several hundred connections a minute, with
// nothing captured.
//
// It parses the metadata responses to do this. That is not a violation of "never
// parse": the rule exists so a bug here cannot lose a frame, and meta.json is still
// written out exactly as received.
func FetchStartupMeta(ctx context.Context, c InfoClient, root string, coins []string, opts MetaOptions) (MetaDumps, error) {
	groups := GroupCoinsByDex(coins)

	dumps := MetaDumps{
		Universes:  make(map[string]Universe, len(groups)),
		Metas:      make(map[string][]byte, len(groups)),
		MetaSHA256: make(map[string]string, len(groups)),
	}

	for _, group := range groups {
		var (
			file string
			get  func(context.Context) ([]byte, error)
		)
		if group.IsDefault() {
			file = MetaFile
			get = c.MetaRaw
		} else {
			file = DexMetaFile(group.Dex)
			// Post is the connector's documented escape hatch for exactly this: the
			// typed Meta() wrapper only covers the default dex.
			dex := group.Dex
			get = func(ctx context.Context) ([]byte, error) {
				return c.Post(ctx, map[string]any{"type": "meta", "dex": dex})
			}
		}
		raw, err := fetchWithRetry(ctx, get, opts, "meta for dex "+dexLabel(group.Dex))
		if err != nil {
			return dumps, fmt.Errorf("hyperliquid: %s: %w", file, err)
		}
		if err := writeFileAtomic(filepath.Join(root, file), raw, 0o644); err != nil {
			return dumps, fmt.Errorf("hyperliquid: write %s: %w", file, err)
		}

		universe, err := ParseUniverse(raw)
		if err != nil {
			return dumps, fmt.Errorf("hyperliquid: dex %q: %w", group.Dex, err)
		}
		dumps.Universes[group.Dex] = universe
		dumps.Metas[file] = raw
		dumps.MetaSHA256[file] = sha256Hex(raw)
		if group.IsDefault() {
			dumps.DefaultAssets = universe.Len()
		} else {
			dumps.Dexes = append(dumps.Dexes, DexDump{
				Dex: group.Dex, Assets: universe.Len(), MetaFile: file,
			})
		}
	}

	if err := checkCoinsExist(groups, dumps.Universes); err != nil {
		return dumps, err
	}
	return dumps, nil
}

// dexLabel renders a dex for a log or error message.
func dexLabel(dex string) string {
	if dex == DefaultDex {
		return "(default)"
	}
	return dex
}

// checkCoinsExist reports every configured coin that its dex does not list, with a
// suggestion when the bare name exists on some other dex.
//
// All the problems are reported at once: a config with two typos should take one run to
// fix, not two.
func checkCoinsExist(groups []DexCoins, universes map[string]Universe) error {
	var problems []string
	for _, group := range groups {
		universe := universes[group.Dex]
		for _, coin := range group.Coins {
			if universe.Has(coin) {
				continue
			}
			where := "the default dex"
			if !group.IsDefault() {
				where = fmt.Sprintf("dex %q", group.Dex)
			}
			problems = append(problems, fmt.Sprintf("%q is not on %s", coin, where))
		}
	}
	if len(problems) == 0 {
		return nil
	}

	// Only the default dex has an unambiguous bare name, so a bare unknown coin is
	// worth pointing at the dex-prefixed form.
	hint := fmt.Sprintf("check the name against the venue's universe (%s and one meta-<dex>.json "+
		"per HIP-3 dex are in the capture root)", MetaFile)
	return fmt.Errorf("hyperliquid: refusing to record, unknown coin(s): %s — %s",
		strings.Join(problems, "; "), hint)
}

// Describe renders the dumps for a log line.
func (d MetaDumps) Describe() string {
	parts := []string{fmt.Sprintf("default=%d assets", d.DefaultAssets)}
	for _, dex := range d.Dexes {
		parts = append(parts, fmt.Sprintf("%s=%d assets", dex.Dex, dex.Assets))
	}
	return strings.Join(parts, ", ")
}
