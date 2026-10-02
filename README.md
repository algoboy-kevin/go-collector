# go-collector

Polymarket BTC market recorder for DES (Discrete Event Simulation) backtest
replay.

The collector connects to the Polymarket WebSocket (via `go-exchange-connector`)
and writes every raw event (price changes, book snapshots, trades, tick changes,
resolutions) to disk — alongside the **Binance spot feed its markets settle on**,
so an outcome can be recomputed from our own data.

Markets are **discovered**, never synthesized: each tick resolves the settlement
instant each family's live market has, asks Gamma for the event that settles at
exactly that instant, and records that event's markets. Nothing is derived from
`time.Now().Truncate(...)`, so the collector cannot drift onto the wrong market.
Full design notes and the reasoning behind each rule: [`CONTEXT.md`](CONTEXT.md).

## The daily epoch

Everything is keyed to one boundary:

```
epoch D  =  [ 12:00 ET on D−1 ,  12:00 ET on D )
             └── 16:00Z (EDT) / 17:00Z (EST) ──┘
```

Noon ET is the anchor because three of the four families settle on the 12:00 ET
candle of their title date, and the fourth (up/down 1d) compares that candle
across two consecutive days — so one epoch is exactly one settlement cycle. The
boundary is computed in `America/New_York`, so it follows DST.

Two things follow from it:

* **Market recordings** are filed under the epoch their settlement falls in:
  `data/market/<epoch>/<marketID>/`. An epoch therefore holds 24 hourly windows
  and 6 four-hourly ones, plus the daily and ladder markets that settle at its
  end. A market's *title date* is not always its epoch — an intraday market is
  named after the date its window opens, so a 2pm ET window on the 2nd is filed
  under the epoch that closes at noon on the 3rd. Both are in `metadata.json`.
* **Feed recordings** are cut on the same boundary. The settle candle sits at the
  anchor, i.e. it is the *first* point of the epoch that opens there, so a market
  settling at noon on D reads from `…_2026-10-0(D+1)`. `settlement.derived_from`
  names the right bucket.

Ladder families (Above/Below, Price Range) additionally hold the **next**
settlement day's event, so each dated event accumulates a full pre-settlement
history before it settles.

## 🪶 Lightweight in RAM

The collector is intentionally **very lightweight in memory**: it **streams
events straight to disk** as gzip-compressed JSONL files instead of buffering
them in RAM. Each market session / feed bucket is flushed continuously, so
memory usage stays low no matter how long the collector runs or how much data
it records. On the server it runs with a small max heap (default `256mb`).

## Remote layout

On the server the collector lives under `go-collector/`:

```
go-collector/
├── collector_daily.yaml  # configuration (collector.yaml = short validation run)
├── data/                 # recorded files
│   ├── market/<epoch>/<marketID>/{events.gz,metadata.json}
│   │   └── <epoch>/manifest.json      # index of the epoch's markets
│   ├── rtds/                          # reference prices (incl. chainlink_twap)
│   └── binance/{spot,perp}/           # aggTrade + depth + final kline_5m
│       └── <symbol>_<epoch>/{events.gz, events.001.gz, …, metadata.json}
└── collector             # linux/amd64 binary
```

A feed bucket is split into parts once it passes 256 MiB (`DefaultFeedPartBytes`),
because a day of aggTrade + depth is otherwise a single unwieldy file. The read
order is **`events.gz` first, then `events.001.gz`, `events.002.gz`, …** — which is
what the bucket's `metadata.json` records in `parts` (a plain `events*.gz` glob
sorts `events.001.gz` ahead of `events.gz`, so read the list, not a glob).

## Build & run locally

```bash
go build -o bin/collector ./cmd/collector
./bin/collector -config collector.yaml
# custom data dir:
./bin/collector -config collector.yaml -recording-dir ./data
```

## Scripts: deploy / download / clear

All three scripts **require** the server and SSH key as flags and fail fast if
either is missing. The IP is masked with `YOUR.IP.GOES.HERE`.

| Script        | Purpose                                                        |
|---------------|----------------------------------------------------------------|
| `deploy.sh`   | Build for linux/amd64, wipe remote dir, ship binary + config, run with max heap |
| `download.sh` | List + download the remote `data/` folder to a local out-dir   |
| `clear.sh`    | Delete all files inside the remote `data/` folder (keeps the folder) |

### Example: deploy

```bash
./deploy.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -heap 256mb
```

- `-server`  (required) SSH target, e.g. `root@YOUR.IP.GOES.HERE`
- `-ssh-key` (required) path to your private key, e.g. `~/.ssh/id_ed25519`
- `-heap`    (optional, default `256mb`) Go max heap via `GOME_MAXHEAP`

On re-run the remote `go-collector/` folder is wiped first (`rm -rf`) so stale
files never linger between deploys.

### Example: download

```bash
./download.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -out-dir ./data-2026-08-27
```

Lists the remote contents first, then downloads everything inside `data/`
(`market/`, `binance/`, `rtds/`, ...) directly into the out-dir. `scp` prints a
progress meter per file so you can track the download.

### Example: clear

```bash
./clear.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519
```

Removes all files inside the remote `data/` folder while keeping the folder
itself.

## Configuration

Two configs ship, both documented inline:

| file | purpose |
|---|---|
| `collector_daily.yaml` | full schedule: `btc_1h`, `btc_4h`, `btc_1d`, `btc_above`, `btc_range`, 3 epochs |
| `collector.yaml` | short validation run: `btc_1h` + `btc_1d`, 1 epoch |

The important blocks:

* `recording` — `days` (how many epochs), `epoch` (`noon_et`), `ladder_offset_days`.
  The run stops by itself once `days` epochs have been recorded.
* `series` — families, by **registry shorthand** (`btc_1h`, `btc_above`, …). The
  registry in `pkg/libs/series.go` holds the verified Gamma series slug and
  resolution rule, so a typo fails at startup rather than recording the wrong
  market. `strikes` limits a ladder to N rungs each side of spot (`0` = all).
* `binance.spot` — the **settlement venue**: every family except up/down-4h
  resolves on a Binance BTC/USDT candle. `aggTrade` is the settlement source
  (the 1m candle is derived from it); `kline_5m` is the integrity cross-check
  and is stored for **final** candles only.
* `rtds.chainlink_twap` — required by up/down-4h, the only Chainlink-settled
  family.

### Dry run

Planning runs the real discovery and selection without starting sessions,
subscribing to anything, or writing:

```bash
go run ./.tmp/smoke -config collector_daily.yaml
```

It prints, per family, the event that would be recorded, its settle instant and
epoch, and how many markets the strike filter would keep.

To settle the Binance side before a deploy, the same tool can watch the real
`kline_5m` chain for one closed candle (a few minutes) and decode what it wrote:

```bash
go run ./.tmp/smoke -klines
```

It fails loudly if no closed candle arrives, or if an in-progress candle was
persisted — the two ways the §7 integrity cross-check could silently have no data.

Note the smoke tool is **not** part of `go build ./...`: the Go tool skips
directories whose name starts with a dot, so build or run it by path as above.

## Requirements

- Go 1.24+ (to build)
- SSH access to the server with key-based login
- `ssh` / `scp` on the local machine
