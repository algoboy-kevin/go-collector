# Polymarket price_change coalescing (opt-in)

`collector.price_change_coalesce_ms` aggregates each market's `price_change`
updates into fixed arrival-time windows before they are written to
`events.gz`. It is **off by default** and only ever turns on when the config
explicitly sets a positive window.

```yaml
collector:
  price_change_coalesce_ms: 100   # 0 / absent = raw (the default)
```

Supported windows are any positive millisecond value within `time.Duration`;
100 is the intended deployment setting, 250 and 1000 are also supported. A
negative or overflowing value fails startup (`loadConfig` rejects it before the
collector opens a socket) rather than being silently ignored.

## What changes

| | raw (default) | coalesced (positive window) |
|---|---|---|
| records | one `price_change` record per source update | one record per window (plus distinct per-window records after a boundary) |
| fields | source record verbatim | connector-compatible payload + `coalesced_window` block |
| levels | every update, in order | LAST absolute size per (asset_id, side, canonical decimal price) |
| deleted levels | a `size: "0"` update | kept as a `size: "0"` tombstone |
| tokens | untouched | every emitted change for a token carries that token's FINAL best_bid/best_ask |
| `timestamp`/`received_at` | source times | the flush cutoff (see below) |
| compression | `gzip.BestSpeed` (L1) | `gzip.DefaultCompression` (L6) |

Everything that is not a `price_change` — trades, `tick_change`,
`book_snapshot`, `market_resolved`, unknown controls — is written **unchanged**
and in source order. A pending window is always flushed *before* such a record
(and before a market-WS **connection transition**, `flush_reason: connection`),
so a window can never straddle a control, a snapshot or a disconnect.

## The `coalesced_window` block

```json
{"format_version":1,"window_ms":100,"bucket_start_ms":1700000000000,
 "emission_cutoff_ms":1700000000100,"last_source_received_ms":1700000000090,
 "flush_reason":"window_end","source_price_change_records":3,
 "source_change_items":5,"synthetic":true,"right_censored":false}
```

`flush_reason` is one of `window_end` (the window's own boundary arrived),
`timer` (a real wall-clock timer fired on a quiet feed), `control`,
`snapshot`, `connection` (a market-WS status transition), `resolve`,
`finalize` (the session closed: right-censored) or `fallback` (a record that
could not be aggregated forced the flush).

The synthetic `timestamp`/`received_at` are the **flush cutoff** — the window
end, or the forcing record's own arrival time for an early flush. For a
non-regressing source clock it is never earlier than a contributing update and
never later than the record that forced the flush. A regressing clock cannot
satisfy both bounds: the aggregate is clamped to its latest contributing arrival
(and bucket start), while the raw control keeps its original timestamp; the
regression is counted rather than inputs being reordered. An early flush may
have a **zero-duration** window: when the source update and the
forcing control share one millisecond the cutoff EQUALS the bucket start. It is
deliberately *not* nudged forward by a millisecond, which would place the
aggregated record *after* the control it must precede. `seq_id` is the last
merged source record's id and the `hash` fields are last-update hashes:
provenance only, not an aggregate book checksum.

A window flush is **not** a durability point: records are written through the
normal gzip buffer (no forced `gzip.Flush`, no `fsync`), exactly as in raw mode.
It only bounds how long an update may stay in memory.

## What is lost, and what is not

Lost: intra-window ordering and timing of individual updates, their per-update
outer sequence numbers, and the exact source spelling of a price/size (values
are canonicalised, so `0.50` and `0.5` are the same level).

Not lost: the book state. Retaining the last absolute size per level — with
zero kept as a tombstone — reproduces the raw book exactly at every flush
boundary, and the per-token BBO is carried through. Data-quality statistics
(latency, broker-timestamp gaps, first/last timestamps) are computed from the
**raw** inputs, exactly once, so aggregation cannot make freshness or data loss
look healthier.

## Failure behaviour

A `price_change` that cannot be aggregated is never silently dropped or
half-aggregated: the pending window is flushed and the record is written **raw
and unmodified**, counted in `metadata.json` → `coalescing.malformed_fallbacks`.
This covers unparseable JSON, a missing/empty `market`, no `changes` at all, an
empty `asset_id`, a `side` that is not `BUY`/`SELL`, and a `price`/`size` that
is not a valid decimal. A payload whose raw JSON is itself unencodable is
surfaced as a write failure (below), never reported as a success.

A write failure is sticky: the session stops accepting events and
`Finalize` returns the error (the metadata is still written, with
`coalescing.write_error`), so a failed flush can never be mistaken for success.
A repeated `Finalize` keeps returning the same failure — finalizing twice never
turns a failed recording into a successful one.

A source clock that steps backwards is counted
(`coalescing.receipt_regressions`) and bucketed against the monotonic arrival
watermark; inputs are never reordered.

## metadata.json

```json
"coalescing": {"enabled":true,"window_ms":100,"compression":"gzip6",
  "input_price_change_records":11341,"input_price_change_items":22570,
  "emitted_coalesced_records":978,"emitted_change_items":8400}
```

The block is omitted entirely for a raw recording, so the existing schema is
unchanged unless the feature is on. The input counts describe every raw
`price_change` record and change item that arrived — including any that fell
back to raw — while `emitted_*` counts only the aggregated records actually
written, so the two are never conflated.

## Reading a coalesced capture

A reader that already parses `price_change` needs one change: it must ignore
(or branch on) `coalesced_window`, and must not treat the synthetic timestamps
as source times. Payloads are otherwise connector-compatible, so replay code
can apply `changes` to a book as-is (size `0` deletes the level).

## Known limitations

- Aggregation is per market session, so the reduction depends on how busy and
  how bursty that market is; a sparse market (a far weekly-hit rung) coalesces
  almost nothing.
- An update that arrives after its window was already flushed by the timer
  (possible only when the wall clock leads the source's `received_at`) opens a
  second record for the same `bucket_start_ms`. The data stays correct; the
  window is simply emitted in two deltas.
- The timestamp fields describe emission, not the venue's clock, so they must
  not be used to date the underlying updates.

## Build and next deployment

Enable the intended mode explicitly in a credential-bearing deployment config
kept outside version control:

```yaml
collector:
  price_change_coalesce_ms: 100
```

The tracked examples intentionally remain at `0`. Binance/RTDS and Hyperliquid
recording are not coalesced or otherwise changed by this setting.

### Reproducible dependency checkout

The existing `go.mod` uses a sibling checkout replacement for
`go-exchange-connector`; the declared `v0.7.3` tag is not published. The exact
compatible connector commit is
`91a1cbca36bb032a0158c7594cd6137e7285dd49`, available in the connector GitHub
history. Build in a **separate clean workspace** with `go-collector/` and
`go-exchange-connector/` as siblings, checking the latter out at that commit.
Do not reset an active development checkout to prepare a deployment.

From the clean collector checkout containing this document:

```bash
go test -mod=readonly -race -count=1 -timeout 60s \
  ./cmd/collector ./pkg/services/collector ./pkg/libs
go vet -mod=readonly ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly \
  -ldflags="-s -w" -o bin/collector ./cmd/collector
```

The repository-wide `go test ./...` currently also includes an unrelated,
pre-existing Hyperliquid shipped-config mismatch: `TestShippedConfigDecodes`
expects `xyz:JP225` and `vntl:OPENAI`, which the tracked `collector_hl.yaml`
does not contain. This failure reproduces on the pre-coalescing commit; it was
not fixed or bundled into this Polymarket feature. The scoped tests above cover
the Polymarket code; repository-wide vet and the collector builds are separate
gates.

A newer sibling connector can require different indirect modules/toolchain
versions; that is not evidence that the coalescing feature needs those upgrades.
The clean commit-pair recipe avoids changing an unrelated dependency checkout.

### Acceptance boundary

Local tests exercise real session gzip output, independent book/BBO replay,
timer flushing, control/connection boundaries, shutdown and failure handling.
They do **not** prove multi-day live RSS, CPU, network stability or storage rate.
Before starting a week-long run, perform an explicitly authorized bounded smoke
capture on the selected host, check that coalescing metadata says `100`/`gzip6`,
and measure resource headroom and file growth. A 100 ms logical window is not a
hard real-time deadline under CPU contention.

Creating a droplet, uploading a binary and starting a capture are separate
operator actions. Existing `deploy.sh` deletes its remote recording directory;
never run it against retained data without a verified backup. Its legacy
`GOME_MAXHEAP` setting is not a Go runtime memory limit; use an explicit reviewed
launch/resource plan rather than relying on that flag for OOM protection.
