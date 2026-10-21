# Go Recorder Spec — raw Hyperliquid capture for NautilusTrader backtests

The contract between the Go collector (writes raw frames) and
`py_perpbt.data.ingest` (turns them into a Nautilus `ParquetDataCatalog`).

Design goal on the Go side: **never parse**. Write received frames to disk and
nothing else. Memory must be flat regardless of throughput — bounded by the
compression buffer, not by message count.

---

## 1. What to subscribe

Perpetuals on the default dex. `<coin>` is the bare venue symbol (`BTC`, `ETH`,
`SOL`) — **not** `BTC-USD-PERP`.

| # | Subscription | Message | Priority | Produces |
| --- | --- | --- | --- | --- |
| 1 | `l2Book` | `{"method":"subscribe","subscription":{"type":"l2Book","coin":"<coin>"}}` | **Required** | `OrderBookDelta` |
| 2 | `trades` | `{"method":"subscribe","subscription":{"type":"trades","coin":"<coin>"}}` | **Required** | `TradeTick` |
| 3 | `bbo` | `{"method":"subscribe","subscription":{"type":"bbo","coin":"<coin>"}}` | Strongly recommended | `QuoteTick` |
| 4 | `activeAssetCtx` | `{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"<coin>"}}` | Strongly recommended | `MarkPriceUpdate`, `FundingRateUpdate` |
| 5 | `allMids` | `{"method":"subscribe","subscription":{"type":"allMids"}}` | Optional | cross-asset sanity checks |

> **Corrected 2026-10-05.** This table previously showed the bare
> `{"type":"l2Book","coin":"<coin>"}` form and flagged it as unverified. A live capture
> (connector v0.7.1, verified against the venue docs and `sonirico/go-hyperliquid`) shows the
> venue takes **`{"method":"subscribe","subscription":{...}}`, one subscription per
> frame**, acknowledged with `{"channel":"subscriptionResponse"}`. The bare form is not the
> documented protocol. Everything else in this document — the line format, rotation,
> sentinels, the `rx_ns` rule — is unaffected; only the frame you *send* was wrong.

### Why each one

**`l2Book` — required.** The only source of depth. Without it there is no queue
position and every passive fill in the backtest is invented by the fill model.

> **Throttle warning.** The docs describe `WsBook` as a "snapshot feed, pushed on
> each block that is at least 0.5 since last push". So the public book is a
> **~2 Hz sampled snapshot, not a tick-by-tick book**. Your backtest's book
> resolution is ~500 ms. This is a real ceiling on how precisely you can model
> quote placement and cancellation — it is a property of the venue feed, not
> something a better recorder can fix.

Optional params: `nSigFigs` (2–5), `mantissa` (1, 2, 5 — only with
`nSigFigs=5`), `fast` (bool; `true` = 5 levels, omitted/`false` = 20 levels).

Recommended: `{"method":"subscribe","subscription":{"type":"l2Book","coin":"<coin>","fast":true}}`. Five levels is more
than a market maker quotes, and it roughly halves bandwidth. **Do not set
`nSigFigs`** — it thins prices as well as levels, and you want raw precision.

> **Corrected 2026-10-21 (bandwidth, and the depth standard).** "Roughly halves
> bandwidth" is wrong. Measured on our own captures: `fast:false` (20 levels) delivers
> **~2.8× FEWER bytes/s** than `fast:true` — the feed is paced by depth (20-level
> snapshots arrive ~0.19/s, 5-level ~1.84/s), so the deeper feed is also the lighter one.
> The real trade is cadence vs depth (book staleness ~5.4 s vs ~0.5 s). For dataset /
> backtest-grade captures the standard is now **`fast:false`**; `fast:true` is a valid
> cadence-first mode but is a reduced-fidelity capture. See §7b.

**`trades` — required.** The only source of aggressor side. Needed for
order-arrival intensity (`kappa`), fill probability, and markout/adverse-selection
calibration. Cannot be reconstructed from the book.

**`bbo` — strongly recommended.** Fires on every block *when it changes* — finer
than `l2Book`'s 0.5 s throttle. Gives you top-of-book for latency modelling and
quote-replacement timing that the throttled book cannot. It is also the cheapest
way to detect a stale book.

**`activeAssetCtx` — strongly recommended.** One message carries mark price, oracle
price, **funding rate**, and open interest. Funding is a real carry cost on perp
inventory (Hyperliquid funds hourly) and the backtest's `funding_enabled = true`
is meaningless without it. This closes a known gap in the scaffold.

**`allMids` — optional.** One subscription covering every coin. Cheap and handy for
cross-asset context, but derivable from the book.

### What NOT to subscribe

- **`candle`** — bars are useless for market making (no queue position). Aggregate
  from `trades` if you ever want them. Recording them wastes disk.
- **`userEvents`, `userFills`, `orderUpdates`, `clearinghouseState`,
  `spotState`, `webData3`, `twapStates`, `activeAssetData`** — live-trading state,
  not backtest input. The single exception is `userFills` at phase 4, to calibrate
  the fill model against your own real fills.
- **`fastAssetCtxs`** — base64 + raw-DEFLATE, mark/mid only. `activeAssetCtx`
  supersedes it and is plain JSON (so it stays greppable).
- **`allDexsAssetCtxs`** — one subscription for all contexts, but the payload is
  **positional with no coin names**, so it needs an exactly-matching universe
  snapshot to decode. Fragile for the bandwidth saved. Skip.
- **`notification`** — noise.

### Connection budget

Hyperliquid enforces, per IP: **10 simultaneous connections**, 30 new
connections/minute, 1000 active subscriptions, 2000 outbound messages/minute.

One connection per **channel** (4 connections: `l2Book`, `trades`, `bbo`,
`activeAssetCtx`), each subscribing to every coin. At 12 assets that is 48
subscriptions — comfortably inside the limits. Do **not** use one connection per
`(channel, coin)`: with 4 channels you would exceed 10 connections at just 3
assets.

Because each connection carries exactly one channel, routing a frame to a file
needs no parsing beyond the file already being open.

### Why not CCXT?

CCXT does support Hyperliquid (certified, with WebSocket via CCXT Pro, and Go
bindings at `github.com/ccxt/ccxt/go/v4`), so it is a legitimate way to *talk* to
the venue. It is not a good way to *store* the data, for four reasons.

**1. It costs 1 bps on Hyperliquid.** Hyperliquid is listed in CCXT's "Builder Code
Exchanges" program, where "users pay a small fee (1 bps) on top of the exchanges'
fees when using the exchanges' API through CCXT". For a market maker quoting a
5–10 bps spread, 1 bps per side is a fifth of the edge — and unlike the
NautilusTrader adapter, whose builder attribution is documented as *zero fee*,
this one is not free. It can be disabled with
`exchange.options['builderFee'] = False`, but it is on by default, which is a
sharp edge to leave in a strategy.

**2. It loses the queue-depth field.** A CCXT unified order book level is
`[price, amount]`. Hyperliquid's `WsLevel` is `{px, sz, n}` where `n` is the number
of orders at that level — the venue's only queue-depth hint, and the field a market
maker most wants. It survives only inside CCXT's raw `info` blob.

**3. It is a reconstruction, not the venue's snapshot.** `watchOrderBook` maintains
a local book to a `limit` and hands you the result. That is a different object from
the throttled full-depth snapshot Hyperliquid actually sends, and it hides staleness
— which is exactly what you need to see.

**4. No local receive time.** CCXT gives the venue `timestamp` in ms. There is no
`rx_ns`, so ingest latency becomes unmeasurable.

And it does not solve the underlying problem: **CCXT has no historical L2 either**,
so it changes the client, not the data availability.

If CCXT interop is wanted, the right place for it is a *consumer* of the catalog —
convert to CCXT shape at analysis time, keeping the raw capture as the source of
truth. Or, if the collector is rewritten on CCXT Pro, write CCXT's `info` field
(which carries the raw venue payload) rather than the normalized structure, so no
field is lost.

---

## 2. One-shot REST dumps

At startup, before the WS loop, `POST https://api.hyperliquid.xyz/info` and save
the raw response body verbatim:

| Body | Endpoint | Why |
| --- | --- | --- |
| `{"type":"meta"}` | `/info` | Per-asset `szDecimals` and universe order — required to build Nautilus instruments and to quantize prices/sizes consistently |
| `{"type":"meta","dex":"<dex>"}` | `/info` | One per **HIP-3 dex** named by a recorded coin, written to `meta-<dex>.json` |
| `{"type":"perpDexs"}` | `/info` | Only if recording HIP-3 markets |

Without `meta` the converter cannot build instruments, and **the catalog is
useless to the backtest without instrument definitions**. Save it once per run into
the same directory as the frames so a capture is self-contained and reproducible
offline.

**`meta.json` holds the DEFAULT dex only.** A HIP-3 market needs the dex's own
universe, so each referenced dex gets `meta-<dex>.json` beside it. This is not
optional politeness: a consumer that reads only `meta.json` cannot build an
instrument for `vntl:OPENAI` and therefore throws its frames away — measured on the
first real capture, 2 of 5 coins were rejected as `unknown_coin`, discarding 26,489 of
66,358 `l2Book` updates. The manifest's `coins[]` names the file each coin needs
(§5).

---

## 3. File layout

```
raw/
└── hyperliquid/
    ├── meta.json                       # verbatim /info {"type":"meta"} response
    ├── meta-<dex>.json                 # one per HIP-3 dex that has a recorded coin
    ├── manifest.json                   # a COPY of the newest run's record (§5)
    ├── manifests/
    │   └── <run_id>.json               # one record per run, never overwritten
    ├── quarantine/
    │   └── <channel>/<hour>.<replaced|truncated>-<run_id>.jsonl.gz
    ├── l2Book/
    │   ├── 2026-10-04T14.jsonl.gz
    │   └── 2026-10-04T15.jsonl.gz
    ├── trades/
    │   └── 2026-10-04T14.jsonl.gz
    ├── bbo/
    │   └── ...
    └── activeAssetCtx/
        └── ...
```

- **One file per channel per UTC hour**, rotated on the **local receive clock**
  (not the venue timestamp — otherwise a clock skew would rotate files strangely).
- Hourly rotation bounds file size, makes backfill granular, and lets the
  converter stream `gunzip -c` without ever holding a whole file.
- gzip, because the frames are highly repetitive JSON and compress roughly 10:1.

**`manifests/` and `quarantine/` are NOT part of the time series.** A reader walks
`<root>/<channel>/`; it must never recurse over the root looking for `*.jsonl.gz`,
because quarantined files are named with that suffix on purpose (§ Crash safety). The
channel set is closed: `l2Book`, `trades`, `bbo`, `activeAssetCtx` (plus `allMids` if
configured). Everything else under the root is metadata.

### Line format

Each line is:

```
<rx_ns>\t<verbatim WebSocket text frame>\n
```

- `rx_ns` — local receive time, Unix nanoseconds, decimal integer.
- Then a single **TAB**.
- Then the frame **exactly as received**, unmodified: the whole
  `{"channel":"l2Book","data":{...}}` envelope.
- Then `\n`.

A tab prefix is used rather than a JSON envelope so Go never has to re-encode
(copy) the payload — it writes an integer, a tab, and the raw bytes it already
has. This is what keeps the collector write-only and flat-memory.

**Keep the full envelope.** `channel` makes each line self-describing (useful if a
connection ever carries more than one channel, and for debugging), and
`isSnapshot` on subscription acks must survive.

### Why `rx_ns` matters

The venue's own `time` field (ms) is in the payload, but it is the venue's clock.
Local receive time is what lets you measure ingest latency, and it is the only way
to model the delay between "book changed" and "strategy could have reacted". The
converter uses `rx_ns` for `ts_init` and the venue `time` for `ts_event`, which is
exactly the split Nautilus wants.

### Rotation and flushing

- Wrap the file in a `bufio.Writer` inside a `gzip.Writer`. Flush both on a timer
  (~1 s) and always on shutdown.
- Memory is then bounded by the buffer sizes, independent of message rate.
- Handle `SIGINT` **and** `SIGTERM`: flush, close, exit. Document that
  `kill -9` loses up to one flush interval.

### Crash safety: `.open` suffix

Write to `<hour>.jsonl.gz.open` and rename to `<hour>.jsonl.gz` on clean close.
The converter can then tell a complete file from one truncated by a crash, and
warn instead of silently ingesting a partial hour.

**Anything preserved is moved to `quarantine/<channel>/`, never left beside real
hour files**, and named by *its own* hour plus the run that displaced it:

```
quarantine/<channel>/<hour>.replaced-<run_id>.jsonl.gz     # a finished hour a new run displaced
quarantine/<channel>/<hour>.truncated-<run_id>.jsonl.gz    # a `.open` left by a crashed process
```

The earlier scheme appended the displacement instant (`<name>.jsonl.gz.replaced-<rx_ns>`)
and left the file in the channel directory. Both halves of that were wrong: the suffix was
the **first `rx_ns` of the file that replaced it**, so it described the displacing run
rather than the orphan (verified: the suffix equals the new file's first line, not anything
about the old one) — and a file sitting among real hours reads as data to a human doing `ls`
while being invisible to a consumer that globs the channel. The manifest's `quarantined[]`
carries each orphan's own line count and receive-time range (§5), which is what a
reader needs to reconcile the hole.

### Heartbeats and reconnect sentinels

- Send `{"method":"ping"}` — an ordinary text frame, not a WebSocket control ping.
  Hyperliquid closes idle connections after ~60 s, so the shipped recorder pings every
  **25 s**. Verified live 2026-10-05 that the connector's 50 s default was honoured at
  *exactly* 50.0 s (72 pongs in an hour): 10 s of margin on a 60 s timer is one delayed
  write away from an avoidable reconnect. The WS-level control ping is not documented to
  reset the venue's application idle timer, so it does not make a longer interval safe.
- Hyperliquid pushes `{"channel":"pong"}`. It is **archived like any other frame** rather
  than filtered out: filtering would mean parsing, and a frame the recorder understood is
  exactly the kind it must not drop. The converter skips it (§7).

Sentinels are in-band lines on the `_meta` channel, framed exactly like a venue frame:

```
<rx_ns>\t{"channel":"_meta","event":"<event>","ts_ns":<rx_ns>,"run_id":"<run_id>",...}\n
  event          when                                  extra fields
  subscribed     the FIRST successful connect           —
  resubscribed   every connect after a drop             —
  disconnected   an unexpected drop                     reason
  dropped        frames lost to a full write queue      count
  write_error    a failed write                         file, err
  stopped        clean shutdown, written last           —
```

- `subscribed` vs `resubscribed` matters: a consumer counting `resubscribed` as "this
  channel reconnected" must not count the initial connect. Before the split, a clean run
  reported `reconnects: 0` in the manifest while the ingest counted **4** reconnect events
  — one per channel, on first connect.
- `run_id` is on every mark, so a sentinel lifted out of its directory still identifies
  its capture. All extra fields are omitted when not applicable.
- `dropped` is written by the **writer** goroutine, not enqueued: the queue is precisely
  what is full when frames are being dropped, so a mark sent through it would be lost too.
  It is what makes a hole in the book attributable from the stream alone, since the
  consumer does not read the manifest.
- `write_error` is best effort and may not be writable (the write path is what failed).
  The authoritative signal for a failed capture is a stream with **no `stopped` mark**.

  **From go-exchange-connector v0.7.2, `disconnected` carries the venue's `reason`:**
  the close status and the peer's own words, e.g. `"reason":"failed to read: status =
  policy violation and reason = \"Cannot open more than 15 connections.\""`, taken from
  the connector's `SetOnDisconnect` callback. A capture written by an older build has no
  `reason` field, so **a converter must not require the field** — treat it as optional
  diagnostic text and never branch on its content.

- **Do not** replay or reorder: write frames in arrival order.

---

## 4. Reference payloads

Exact shapes, from the Hyperliquid docs. The converter parses these.

```jsonc
// channel: "l2Book"  — full snapshot every time, never a delta
{"channel":"l2Book","data":{
  "coin":"BTC",
  "time":1696000000000,                       // ms, venue clock
  "levels":[
    [{"px":"99990.0","sz":"1.5","n":3}, ...], // bids, best first
    [{"px":"100010.0","sz":"1.2","n":2}, ...] // asks, best first
  ]
}}

// channel: "trades"  — an array
{"channel":"trades","data":[
  {"coin":"BTC","side":"B","px":"100005.0","sz":"0.35",
   "time":1696000000123,"hash":"0x..","tid":123456,"users":["0x..","0x.."]}
]}

// channel: "bbo"
{"channel":"bbo","data":{
  "coin":"BTC","time":1696000000000,
  "bbo":[{"px":"99990.0","sz":"1.5","n":3},{"px":"100010.0","sz":"1.2","n":2}]
}}

// channel: "activeAssetCtx"
{"channel":"activeAssetCtx","data":{
  "coin":"BTC",
  "ctx":{"dayNtlVlm":1.2e9,"prevDayPx":99000.0,"markPx":100000.5,
         "midPx":"100000.0","funding":0.0000125,"openInterest":1234.5,
         "oraclePx":99999.0}
}}
```

Field notes that will bite the converter:

- `px` and `sz` are **strings**, and their decimal places vary run to run. Nautilus
  requires one precision per instrument per write, so the converter quantizes
  through the instrument built from `meta.json`.
- `n` (orders at that level) is the only queue-depth hint the venue gives. Keep it
  — it feeds `queue_ahead_fraction`.
- `trades[].side` is documented only as `string`. **Verified live 2026-10-05: it is
  `"B"/"A"`** (buy/ask-side), never `"buy"/"sell"`. The converter should still accept both,
  but the venue's actual encoding is settled.
- `activeAssetCtx.ctx.midPx` may be **absent**; `markPx` is a float, `midPx` a
  string.
- `bbo` entries may be **`null`** (one-sided book) — the converter skips those.

---

## 5. Run records (`manifests/<run_id>.json`)

Not required for replay, but it makes a capture self-describing and lets the converter fail
loudly instead of guessing:

```json
{
  "run_id": "2026-10-05T084829Z",
  "venue": "hyperliquid",
  "environment": "mainnet",
  "started_ns": 1791190109249452338,
  "ended_ns": 1791197309254189584,
  "stop_reason": "duration",
  "coins": [{"coin": "xyz:JP225", "dex": "xyz", "meta_file": "meta-xyz.json"}],
  "channels": ["l2Book", "trades", "bbo", "activeAssetCtx"],
  "l2book_params": {"fast": true},
  "collector_version": "0.1.0"
}
```

**One file per run, in `manifests/`, plus `manifest.json` as a copy of the newest.** A
single `manifest.json` was rewritten at startup and again at shutdown, so the next run on
the same root overwrote the previous run's record — its window, its counts and its stop
reason — while that run's frames survived in `quarantine/`. The capture kept the data and
lost the explanation. The root `manifest.json` remains for readers that expect it, and its
`run_id` says which run it describes.

`run_id` is the run's start instant in UTC to the second, disambiguated (`-2`, `-3`) if a
restart lands in the same second — a second-resolution id alone lets a crash loop overwrite
the record again. Order runs by `started_ns`, not by file name.

> **As built, each record carries more than the sketch above.** Also written:
> `connector_version`, `error`, `coins_skipped[]`, `universe_fingerprint`, `meta_sha256`,
> `hip3_dexes[]`, a `recording` block (gzip level, flush interval, queue size, the
> **effective** read limit and ping interval, `min_free_bytes`, free space at start and end)
> and a `channel_reports[]` array.
>
> Read four things:
>
> | Field | What a non-default value means |
> | --- | --- |
> | `channel_reports[].drops` / `dropped_sentinels` | The capture is **visibly not clean** |
> | `channel_reports[].acks < subscriptions` | The venue refused a subscription |
> | `channel_reports[].largest_frame_bytes` | How close a burst came to the read limit — re-measure it on every long run, it is a floor on venue behaviour and grew 14x in three hours once |
> | `channel_reports[].quarantined[]` | A hole exists, and this is where the surviving bytes are, with their own line count and range |
>
> A capture must not be able to *look* clean when it is not, which is why these live in the
> capture rather than only in the logs that produced it.

Per-channel, each report also carries `first_frame_ns` / `last_frame_ns` (the window that
channel actually observed), `max_gap_ms` (monotonic, so a wall-clock step cannot fake a
stall), `files[]` (name, lines, bytes, window, `sha256` — so a copy is verified by reading
the copy) and `clock_regression_samples[]` (how far a rewind reached; `clock_regressions`
stays the exact count). `connect_attempts` counts dials that **succeeded**, because the
connector reports no dial failure — do not read it as attempts.

---

## 6. Volume expectations

**Measured end to end 2026-10-05** — a real 60 s capture by `hlrecorder` against the live
venue: 5 coins (BTC/ETH/SOL/HYPE/DOGE), 4 channels, `l2Book fast:true`, gzip level 6.

| Channel | Frames/60 s | Raw/hour | On disk/hour | Ratio |
| --- | --- | --- | --- | --- |
| `l2Book` | 526 | 15.1 MB | 2.0 MB | 7.5:1 |
| `trades` | 278 | 14.6 MB | 2.7 MB | 5.5:1 |
| `bbo` | 1,858 | 18.3 MB | 2.2 MB | 8.3:1 |
| `activeAssetCtx` | 258 | 5.2 MB | 0.6 MB | 8.9:1 |
| **total** | **2,920** | **53 MB** | **7.5 MB** | **7.1:1** |

`Raw` is the byte count of the frames themselves; the `<rx_ns>\t` prefix and the newline add
~21 B per line, so the ratio is computed against the decompressed file rather than against
the frame bytes.

A capture is therefore **~180 MB/day**. 10 GiB holds roughly 55 days, and a 500 MiB
free-space floor is weeks of headroom rather than hours. **The disk is not the constraint it
looks like**: a capture being irreplaceable is.

Notes on the numbers:

- The original estimates were for 12 assets and are ~10× pessimistic **per coin**. Scaling
  this table to 12 coins is the honest way to use it, not re-reading the old table.
- Real compression (7.1:1) beats the synthetic gzip prediction (6.1:1 at level 6,
  PERP_COLLECTOR_SPEC.md §6.2) but falls short of this document's original ~10:1 guess.
  `trades` compresses worst, because transaction hashes are high-entropy and incompressible.
- **The ~2 Hz `l2Book` throttle is real** — 1.87 frames/s/coin. If a capture ever shows far
  more than ~2 lines/sec/coin the throttle assumption is wrong, which would be good news for
  the backtest and worth re-checking against `fast`/`nSigFigs`.
- **`activeAssetCtx` is once per block** — ~1/s/coin.
- **`bbo` leads on frame count, `l2Book` leads on bytes.** Neither is a problem, but `bbo` is
  the one to watch if the universe grows.
- **`trades` has the largest single frame by ~20×** (9 KB vs 480 B), because the payload is
  an *array*: a burst of prints arrives as one frame. That is the frame most likely to grow
  with more coins, and the one to watch against any read limit.
- **Subscriptions are acknowledged individually** — one `subscriptionResponse` per coin, and
  a `pong` per ping. `ack count == coin count` is the cheapest check that nothing was
  silently rejected, and it is available within a second of startup.

---

## 7. Mapping to Nautilus

`py_perpbt.data.ingest` performs these conversions.

> **The file is not only market data.** Because the recorder never parses, the venue's own
> handshake traffic is archived verbatim alongside the data. A converter that assumes every
> line is a market-data frame fails on the **first line of the first file**, because the
> `resubscribed` sentinel is written before any data arrives. The channels to expect:
>
> | `channel` | Meaning | Converter |
> | --- | --- | --- |
> | `subscriptionResponse` | one per subscription, on every (re)connect | skip |
> | `pong` | answer to the application-level ping | skip |
> | `_meta` | the recorder's own marks (`resubscribed`, `disconnected`, `stopped`) | gap warning |
> | `error` | a subscription the venue refused | warn loudly — it is the only direct signal |
>
> This is exactly why `channel` must survive in the payload rather than being stripped.

| Raw channel | Nautilus type | Notes |
| --- | --- | --- |
| `l2Book` | `OrderBookDelta` | One `CLEAR` per snapshot, then one `ADD` per level; `F_LAST` on the final delta. `action`/`flags` must be plain ints — v2 rejects the enums. Level `n` is not representable and is dropped |
| `trades` | `TradeTick` | `AggressorSide.BUY` / `.SELL` (not `BUYER`) |
| `bbo` | `QuoteTick` | Skip null sides and any crossed pair |
| `activeAssetCtx` | `MarkPriceUpdate` | `markPx` |
| `activeAssetCtx` | *sidecar Parquet* | `funding` — see below |
| `meta.json` | `Instrument` | Written to the catalog — **required**, or the backtest cannot resolve the data |

Timestamps: `ts_event` from the venue's `time` (ms → ns), `ts_init` from the line's
`rx_ns` prefix.

### Gotchas found while implementing the converter

**Price precision cannot be a constant.** Hyperliquid allows 5 significant figures
*and* at most `6 - szDecimals` decimals, so the usable decimal count depends on price
magnitude — `99990.0` gets 0 decimals, `0.0012345` gets 7. Build the instrument from
a price actually observed in the capture. Sizing precision from the **lowest** price
seen in the run is the safe choice: a smaller price needs more decimals, so this keeps
every price representable even if the market moves down through a power of ten.

**Funding cannot go into the catalog.** v2 routes custom types through
`write_custom_data`, which requires the class to implement an Arrow codec
(`decode_record_batch_py`) and to be registered via `register_custom_data_class`.
The built-in `FundingRateUpdate` does not implement one, so registration fails with
`Custom data class must have decode_record_batch_py(metadata, batch)`.

Since a capture is irreplaceable, `ingest_run` writes funding to a sidecar
`funding.parquet` next to the catalog (`ts_event_ns`, `instrument_id`, `rate`,
`interval_mins`) rather than dropping it. **Applying funding inside the backtest is
still an open gap** — it needs either a custom `CustomData` subclass with a working
Arrow codec, or confirmation that the engine consumes funding another way.

**Instruments are mandatory.** A catalog full of book data with no instrument
definitions is unusable — the backtest cannot resolve the symbol and `build()`
produces no engine. `ingest_run` always writes them, even for a capture with no
frames.

**One write call, one instrument.** Mixing raises `Cannot write ... data with mixed
identities`, so the writer groups by `(data type, instrument_id)` before flushing.

---

## 7b. Mapping to hftbacktest (NPZ)

Second consumer of the same raw capture: the `quantlab data transform` pipeline produces
hftbacktest v2 structured event arrays (`.npz`: `ev`, `exch_ts`, `local_ts`, `px`, `qty`,
`aux`, …). This section locks what a capture must contain for that conversion to be
faithful — "hftbacktest standard" means every box below holds. (Verified against
hftbacktest's source 2026-10-21: `hftbacktest/src/depth/hashmapmarketdepth.rs`,
`backtest/proc/nopartialfillexchange.rs`, and the shipped
`py-hftbacktest/hftbacktest/data/utils/difforderbooksnapshot.py`.)

### Engine semantics the capture feeds

hftbacktest's L2 book is a price→qty map in integer ticks with **set** semantics:
`DEPTH_SNAPSHOT_EVENT` and `DEPTH_EVENT` are the *same* operation (set the level's qty; a
level below one lot is removed), and only `DEPTH_CLEAR_EVENT` resets state. Snapshot frames
are therefore NOT special to the engine — what makes a state authoritative is the producer's
contract that each `l2Book` frame **is** the complete top-N book at its `rx_ns`.

### Capture pattern (the general architecture)

**Snapshot first, stream after.** A backtest-grade capture is anchored by a full-depth
snapshot at connection start (and re-anchored periodically), then continued by the venue's
depth stream. Two fidelity classes, classified per venue — the classifier is the venue's
capability, never the recorder's choice:

| Class | Venue gives | Anchors | Queue claims valid |
|---|---|---|---|
| `full-anchored` | full book (initial snapshot + diffs, or per-order/L4) | full snapshot at connect + periodic | anywhere in the recorded book |
| `windowed-N` | top-N snapshots only (N = 20 for Hyperliquid, `fast:false`) | best-available snapshot (top-N) at connect + periodic (`bookAnchors`) | **only inside the top-N window** (queue-window rule below) |

Hyperliquid is the canonical `windowed-N` venue: its public API caps depth at 20/side on
both WS and REST (verified live 2026-10-21; `nLevels` is silently ignored, `l4Book` is
schema-rejected — full depth exists only via third-party infra). Any future broker that
cannot produce a full book lands in the same class. The recorder's job is to capture the
best the venue offers at anchor times (`book_anchors`, §7b contract below) and to record the
class in the run manifest (`l2book_params` + channel reports identify it); the converter
stamps `book_fidelity` into the dataset manifest, and the SIM must gate queue claims on it —
see the queue-window rule. A simulation that treats `windowed-N` data as `full-anchored` is
not "hftbacktest standard" no matter how it was recorded.

### Contract

1. **Per-frame snapshot property.** Every `l2Book` frame is an authoritative top-N snapshot
   (N = 20, or 5 with `fast`) at its line's `rx_ns`. The converter diffs consecutive frames
   with `DiffOrderBookSnapshot`, which classifies deletions as in-book vs
   out-of-window-below/above — a level leaving the top-N window is not a book deletion. The
   never-parse rule is what preserves this property; any future parser that aggregates,
   coalesces or drops frames breaks §7b.
   *Emission convention (locked 2026-10-21):* the npz carries one set-to-qty row per present
   level **plus one removal row (qty 0, same side, frame clocks) per vanished level** — the
   engine book mirrors the recorded window exactly and never holds phantom depth. Each
   removal row's `ival` parks the `DiffOrderBookSnapshot` classification (`0`
   IN_THE_BOOK_DELETION, `1` OUT_OF_BOOK_DELETION_BELOW, `2` OUT_OF_BOOK_DELETION_ABOVE),
   so "not a book deletion" stays true at the data level: the sim's queue-window rule reads
   these labels and treats out-of-window prices as hidden-queue, never front-of-queue.
2. **Depth standard.** Backtest-grade captures record `fast:false` (20 levels). `fast:true`
   is a valid cadence-first mode (~1.84/s vs ~0.19/s) but the run's `l2book_params` in the
   manifest must record it, and downstream findings carry the reduced-depth flag. Never set
   `nSigFigs`/`mantissa` (raw price precision is required — the book rounds to ticks).
3. **`n` (resting orders per level) must survive.** It is the only queue signal the venue
   gives. hftbacktest's L2 book has no field for it (levels hold qty only), so the converter
   parks it in the npz `aux` column; its queue models consume qty only, so queue position
   stays *inferred, never observed*. Verbatim frames guarantee `n` survives — this is the
   reason the never-parse rule exists beyond loss safety.
4. **Clocks.** The line prefix `rx_ns` → `local_ts`; the venue `time` (ms) inside the frame
   → `exch_ts`. Never substitute one for the other (§3 "Why `rx_ns` matters").
5. **Instrument definitions are part of the dataset.** `meta.json` (+ `meta-<dex>.json` for
   every HIP-3 dex in `coins`) supplies `szDecimals` (lot size) and the universe; without it
   the converter cannot build an instrument. Startup already writes these before the first
   frame and refuses unknown coins — keep it that way.
6. **Trades carry aggressor side** (`"side":"B"|"A"` in the trade frame) → `TRADE_EVENT` +
   BUY/SELL bit, which drives hftbacktest's fill logic against the book. Required channel.
7. **Gaps stay honest.** After a reconnect the first `l2Book` frame is a fresh authoritative
   snapshot (the converter re-anchors there); heartbeat/reconnect sentinels (§3) let the
   converter mask venue-time-loss windows rather than treating a stall as live state. A run
   without a clean `ended_ns` in its manifest is converted as *partial coverage*, never as
   a full day.

### Fidelity ceiling (unchanged by any of this)

The venue feed is a paced snapshot stream (~0.5 s or slower, §1 throttle warning); the
public API caps depth at **20 levels/side on both WS and REST** (verified live 2026-10-21:
`nLevels` is silently ignored by the WS, and `l4Book`/per-order data is schema-rejected —
full-depth/L4 exists only via third-party infra). Queue position is inferred from level `n`
and qty; the backtest must never present it as observed.

**Queue-window rule.** An order placed at a price OUTSIDE the recorded top-N window has an
*unknown* prior queue — it must never be modeled as joining an empty level (front of
queue). Simulations may only claim queue position at levels inside the recorded window;
placements beyond it are either prohibited by the strategy or penalized as hidden-queue.
Any run that quotes outside the window is not "hftbacktest standard" regardless of how it
was recorded.

## 8. Minimal Go sketch

Not a full implementation — just the shape that satisfies the spec.

```go
// One connection per channel; each writes one gzip file per UTC hour.
type HourlyWriter struct {
    dir     string
    channel string
    cur     string          // current hour bucket "2006-01-02T15"
    file    *os.File
    gz      *gzip.Writer
    buf     *bufio.Writer
}

// WriteLine appends one received frame. No parsing, no allocation of a parsed
// struct — the payload bytes are written through untouched.
func (w *HourlyWriter) WriteLine(now time.Time, frame []byte) error {
    bucket := now.UTC().Format("2006-01-02T15")
    if bucket != w.cur {
        if err := w.rotate(bucket); err != nil {
            return err
        }
    }
    // <rx_ns>\t<verbatim frame>\n
    if _, err := fmt.Fprintf(w.buf, "%d\t", now.UnixNano()); err != nil {
        return err
    }
    if _, err := w.buf.Write(frame); err != nil {
        return err
    }
    return w.buf.WriteByte('\n')
}

// rotate closes the current file (renaming .open -> final) and opens the next.
func (w *HourlyWriter) rotate(bucket string) error { /* gz.Close, buf.Flush,
    os.Rename(tmp, final), create next */ }
```

The read loop is then: `conn.ReadMessage()` → `w[channel].WriteLine(time.Now(), msg)`.
Both return values are the only error paths; everything else is the standard
library.

---

## 9. Checklist

- [ ] `meta.json` captured once per run, saved alongside the frames
- [ ] `meta-<dex>.json` dumped for every HIP-3 dex named in `coins` (§7b)
- [ ] One connection per channel, all coins subscribed on each
- [ ] `l2Book` depth chosen per §7b — `fast:false` for dataset captures; `fast:true` only
      for cadence-first runs, with `l2book_params` recorded in the manifest
- [ ] `nSigFigs`/`mantissa` left unset (raw price precision, §7b)
- [ ] No `candle` subscription
- [ ] Line = `<rx_ns>\t<frame>\n`, frame unmodified
- [ ] gzip + `bufio`, flushed on a ~1 s timer
- [ ] SIGINT/SIGTERM flush and close
- [ ] `.open` suffix until clean close, then rename
- [ ] `{"method":"ping"}` every ~30 s
- [ ] `_meta` sentinel lines on (re)connect and disconnect
- [ ] Frames written in arrival order, never reordered
