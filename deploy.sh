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
#
# Required: -server and -ssh-key. Optional: -heap (default 256mb).

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

# Local paths.
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_BIN="$LOCAL_DIR/bin/collector"
LOCAL_CONFIG="$LOCAL_DIR/collector.yaml"

# ── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    -heap)
      MAX_HEAP="$2"; shift 2 ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -heap 256mb" >&2
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

# ── 3. Run the collector on the server with max heap ────────────────────────
echo "==> Starting collector on $SERVER (max heap: $MAX_HEAP) ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" \
  "cd $REMOTE_DIR && GOME_MAXHEAP=$MAX_HEAP ./collector -config $REMOTE_CONFIG"

echo "==> Done."