#!/usr/bin/env bash
#
# Download the remote capture folders to a local out-dir.
#
# There are TWO captures on the server, written by two binaries into two directories,
# and this script fetches either or both:
#
#   polymarket   /root/go-collector/data     (cmd/collector)  -> binance/ market/ rtds/
#   hyperliquid  /root/go-hlrecorder/data    (cmd/hlrecorder) -> hyperliquid/
#
# Each tree's contents land directly inside the out-dir, so fetching both into one place
# cannot collide: out-dir/{binance,market,rtds} plus out-dir/hyperliquid/.
#
# A tree that is not deployed is skipped with a warning when both are requested; naming
# one explicitly and finding it missing is an error.
#
# The Hyperliquid capture is written continuously, so it can contain `.open` files —
# the current, still-unrolling UTC hour. That is normal: the final `.jsonl.gz` name
# appears only once the hour is closed and fsynced.
#
# Usage:
#   ./download.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 \
#                 [-out-dir ./download] [-source polymarket|hyperliquid|both]
#
# Required: -server and -ssh-key. Optional: -out-dir (default ./download),
# -source (default both).

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
# Server + SSH key are REQUIRED via flags. SERVER defaults to a placeholder.
SERVER="YOUR.IP.GOES.HERE"
SSH_KEY=""
OUT_DIR="./download"
SOURCE="both"

# The two captures live in separate remote directories by design: deploy-hl.sh keeps its
# own tree so it can never `rm -rf` the Polymarket one (see that script's header).
REMOTE_PM_DATA_DIR="/root/go-collector/data"
REMOTE_HL_DATA_DIR="/root/go-hlrecorder/data"

# ── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    -out-dir)
      OUT_DIR="$2"; shift 2 ;;
    -source)
      SOURCE="$2"; shift 2 ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519 [-out-dir ./download] [-source polymarket|hyperliquid|both]" >&2
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
case "$SOURCE" in
  polymarket|hyperliquid|both) ;;
  *)
    echo "Error: -source must be polymarket, hyperliquid or both (got '$SOURCE')" >&2
    exit 1 ;;
esac

# ── Helpers ──────────────────────────────────────────────────────────────────
ssh_() { ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$@"; }
scp_() { scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o BatchMode=yes "$@"; }

# download_tree <label> <remote-dir> <required>
#   required=1 -> a missing remote dir is an error (this source was named explicitly)
#   required=0 -> a missing remote dir is skipped, so `both` still works on a host that
#                 runs only one of the two binaries
#
download_tree() {
  local label="$1" remote="$2" required="$3"

  # `test -d` is the condition of an `if`, so an absent directory is handled here rather
  # than aborting the script under `set -e`.
  if ! ssh_ "$SERVER" "test -d '$remote'"; then
    if [[ "$required" == 1 ]]; then
      echo "Error: $remote does not exist on $SERVER — $label is not deployed there." >&2
      exit 1
    fi
    echo "==> Skipping $label: $remote does not exist on $SERVER."
    return 0
  fi

  echo "==> $label: remote contents of $SERVER:$remote ..."
  ssh_ "$SERVER" "find '$remote' -type f | sort"

  # The "/." trick copies the *contents* of the remote directory rather than the
  # directory itself, so each tree lands directly inside OUT_DIR. scp prints a progress
  # meter per file.
  echo "==> $label: downloading $remote -> $OUT_DIR ..."
  scp_ -r "$SERVER:$remote/." "$OUT_DIR/"
}

# ── Fail fast on an unreachable host ─────────────────────────────────────────
# Done before the per-tree checks, because otherwise a bad key or a dead host would be
# reported as "not deployed" for both trees — a misleading diagnosis of a real problem.
if ! ssh_ "$SERVER" true; then
  echo "Error: cannot SSH to $SERVER with key $SSH_KEY." >&2
  exit 1
fi

# ── Download ─────────────────────────────────────────────────────────────────
mkdir -p "$OUT_DIR"

if [[ "$SOURCE" == "polymarket" || "$SOURCE" == "both" ]]; then
  required=0
  if [[ "$SOURCE" == "polymarket" ]]; then required=1; fi
  download_tree "polymarket" "$REMOTE_PM_DATA_DIR" "$required"
fi

if [[ "$SOURCE" == "hyperliquid" || "$SOURCE" == "both" ]]; then
  required=0
  if [[ "$SOURCE" == "hyperliquid" ]]; then required=1; fi
  download_tree "hyperliquid" "$REMOTE_HL_DATA_DIR" "$required"
fi

echo "==> Done. Files downloaded to: $OUT_DIR"
echo "    $(find "$OUT_DIR" -type f | wc -l | tr -d ' ') file(s) local."
