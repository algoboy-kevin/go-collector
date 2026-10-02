#!/usr/bin/env bash
#
# Build the go-collector binary for Linux (amd64), ship it to the Golang
# server, and run it with max heap.
#
# Remote layout:
#   go-collector/
#     collector.yaml
#     data/          (recorded files)
#     collector      (binary)
#
# On re-run the remote go-collector/ folder is wiped first so stale files
# never linger between deploys.
#
# Usage:
#   ./deploy.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -heap 256mb
#   ./deploy.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -config collector.yaml
#
# Required: -server and -ssh-key. Optional: -heap (default 256mb) and
# -config (default collector_daily.yaml — the full 5-family, 3-epoch schedule;
# collector.yaml is the short 1-epoch validation run).
#
# NOTE: the collector runs for `recording.days` epochs and then exits by itself;
# re-deploying wipes the remote data directory, so download before re-deploying.

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
# Server + SSH key are REQUIRED via flags. SERVER defaults to a placeholder.
SERVER="YOUR.IP.GOES.HERE"
SSH_KEY=""
MAX_HEAP="256mb"
REMOTE_DIR="/root/go-collector"
REMOTE_DATA_DIR="$REMOTE_DIR/data"
REMOTE_BIN="$REMOTE_DIR/collector"
REMOTE_CONFIG="$REMOTE_DIR/collector.yaml"
REMOTE_LOG="$REMOTE_DIR/collector.log"

# Local paths.
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_BIN="$LOCAL_DIR/bin/collector"
CONFIG_NAME="collector_daily.yaml"
LOCAL_CONFIG="$LOCAL_DIR/$CONFIG_NAME"

# ── Parse flags ──────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    -heap)
      MAX_HEAP="$2"; shift 2 ;;
    -config)
      CONFIG_NAME="$2"; shift 2
      LOCAL_CONFIG="$LOCAL_DIR/$CONFIG_NAME" ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 [-heap 256mb] [-config collector_daily.yaml]" >&2
      exit 1 ;;
  esac
done

# ── Validate required flags ──────────────────────────────────────────────────
if [[ -z "$SERVER" || "$SERVER" == *"YOUR.IP.GOES.HERE"* ]]; then
  echo "Error: -server is required (e.g. -server root@YOUR.IP.GOES.HERE)" >&2
  exit 1
fi
if [[ -z "$SSH_KEY" ]]; then
  echo "Error: -ssh-key is required (e.g. -ssh-key ~/.ssh/id_ed25519)" >&2
  exit 1
fi
if [[ ! -f "$LOCAL_CONFIG" ]]; then
  echo "Error: config not found: $LOCAL_CONFIG" >&2
  exit 1
fi

# ── 1. Build for Linux amd64 ────────────────────────────────────────────────
echo "==> Building collector for linux/amd64 ..."
cd "$LOCAL_DIR"
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o "$LOCAL_BIN" ./cmd/collector
echo "    built: $LOCAL_BIN"

# ── 2. Ship binary + config to the server ───────────────────────────────────
echo "==> Shipping to $SERVER ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" "mkdir -p $REMOTE_DATA_DIR"

# Wipe any previous go-collector folder so a re-run starts clean.
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" "rm -rf $REMOTE_DIR && mkdir -p $REMOTE_DATA_DIR"

scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$LOCAL_BIN" "$SERVER:$REMOTE_BIN"
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$LOCAL_CONFIG" "$SERVER:$REMOTE_CONFIG"

echo "    deployed: $REMOTE_BIN"
echo "    deployed: $REMOTE_CONFIG"

# ── 3. Run the collector on the server (detached) with max heap ──────────────
# The collector is long-running (it records for `recording.days` epochs), so it
# must be fully detached — foregrounding it through ssh would leave deploy.sh
# attached to the collector's live log stream and it would never return.
#
# Plain "nohup ... &" is NOT enough: the process stays in the same session as
# the sshd channel, so ssh waits for it to exit and deploy.sh still hangs.
# setsid starts the collector in a brand-new session (no controlling tty), so
# sshd sees the session end, the ssh call returns, and the collector keeps
# running on its own. Output goes to $REMOTE_LOG; we only print the tail to
# confirm startup.
echo "==> Starting collector on $SERVER (max heap: $MAX_HEAP, log: $REMOTE_LOG) ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" \
  "cd $REMOTE_DIR && GOME_MAXHEAP=$MAX_HEAP setsid ./collector -config $REMOTE_CONFIG > $REMOTE_LOG 2>&1 < /dev/null &"

# Give it a moment to boot, then show only the last log lines (not a live tail).
sleep 3
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" "tail -n 20 $REMOTE_LOG"

echo "==> Done. Collector running in the background on $SERVER."
echo "    Watch it live:  ssh -i $SSH_KEY $SERVER 'tail -f $REMOTE_LOG'"