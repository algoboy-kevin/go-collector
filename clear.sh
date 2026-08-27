#!/usr/bin/env bash
#
# Remove all files inside the remote go-collector data/ folder.
# The data/ directory itself is kept; only its contents are wiped.
#
# Usage:
#   ./clear.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519
#
# Required: -server and -ssh-key.

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
# Server + SSH key are REQUIRED via flags. SERVER defaults to a placeholder.
SERVER="YOUR.IP.GOES.HERE"
SSH_KEY=""
REMOTE_DATA_DIR="/root/go-collector/data"

# ── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519" >&2
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

# ── 1. Show what will be removed ─────────────────────────────────────────────
echo "==> Remote contents of $SERVER:$REMOTE_DATA_DIR ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" \
  "find $REMOTE_DATA_DIR -mindepth 1 | sort" || true

# ── 2. Clear everything inside data/ ─────────────────────────────────────────
echo "==> Clearing $SERVER:$REMOTE_DATA_DIR ..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$SERVER" \
  "find $REMOTE_DATA_DIR -mindepth 1 -delete"

echo "==> Done. Remote data/ is now empty."
