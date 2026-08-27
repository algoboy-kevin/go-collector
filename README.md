# go-collector

Polymarket rolling-market event recorder for DES (Discrete Event Simulation)
backtest replay.

The collector connects to the Polymarket WebSocket (via `go-exchange-connector`),
subscribes to configured rolling market series, and writes every raw event
(price changes, book snapshots, trades, tick changes, resolutions) to disk.

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
├── collector.yaml   # configuration
├── data/            # recorded files
│   ├── market/      # market event recordings
│   ├── rtds/        # reference price recordings (incl. chainlink_twap)
│   └── binance/     # aggTrade + partial depth recordings
└── collector        # linux/amd64 binary
```

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

## Requirements

- Go 1.24+ (to build)
- SSH access to the server with key-based login
- `ssh` / `scp` on the local machine

## Configuration

See `collector.yaml` for the full config: Polymarket API credentials, rolling
market series, RTDS reference prices and Chainlink TWAP, and Binance streams.
