#!/usr/bin/env bash
#
# Download the remote go-collector data/ folder to a local out-dir.
# The remote data folder contains subfolders (market/, binance/, rtds/) which
# land directly inside the given out-dir. Shows progress for each file.
#
# Usage:
#   ./download.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -out-dir ./download
#
# Required: -server and -ssh-key. Optional: -out-dir (default ./download).

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
# Server + SSH key are REQUIRED via flags. SERVER defaults to a placeholder.
SERVER="YOUR.IP.GOES.HERE"
SSH_KEY=""
OUT_DIR="./download"
REMOTE_DATA_DIR="/root/go-collector/data"

# ── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    -out-dir)
      OUT_DIR="$2"; shift 2 ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 -out-dir ./download" >&2
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

# ── 1. Show what is on the server first ─────────────────────────────────────
echo "==> Remote contents of $SERVER:$REMOTE_DATA_DIR ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" \
  "find $REMOTE_DATA_DIR -type f | sort"

# ── 2. Download ─────────────────────────────────────────────────────────────
mkdir -p "$OUT_DIR"
echo "==> Downloading $SERVER:$REMOTE_DATA_DIR -> $OUT_DIR ..."
# The "/." trick copies the contents of data/ (market, binance, rtds, ...)
# directly into OUT_DIR. scp prints a progress meter per file so we can track
# each downloaded file.
scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes \
  -r "$SERVER:$REMOTE_DATA_DIR/." "$OUT_DIR/"

echo "==> Done. Files downloaded to: $OUT_DIR"
echo "    $(find "$OUT_DIR" -type f | wc -l | tr -d ' ') file(s) local."
