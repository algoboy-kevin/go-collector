#!/usr/bin/env bash
#
# Deploy the Hyperliquid raw recorder to a droplet and start it detached.
#
#   ./deploy-hl.sh -server root@YOUR.IP.GOES.HERE -ssh-key ~/.ssh/id_ed25519
#   ./deploy-hl.sh -server root@1.2.3.4 -ssh-key ~/.ssh/id_ed25519 -duration 1h
#   ./deploy-hl.sh -server root@1.2.3.4 -ssh-key ~/.ssh/id_ed25519 -config .tmp/collector_hl.local.yaml
#
# Deliberately separate from deploy.sh and pointed at its own remote directory
# (/root/go-hlrecorder), so it is impossible for this script to touch the Polymarket
# capture under /root/go-collector. Two binaries, two directories, two captures.
#
# Two differences from deploy.sh worth knowing:
#
#   1. It does NOT wipe the data directory. The Polymarket deploy does `rm -rf` on its
#      whole remote dir, which is survivable because that capture is re-derivable from
#      live markets. This one is not: a Hyperliquid capture is irreplaceable, and the
#      last thing a redeploy should do is delete the hours already recorded. Only the
#      binary and config are replaced. Pass -wipe-data if you really mean it.
#
#   2. It refuses to start a second recorder. Two processes writing the same hour file
#      would fight over the same `.open` name, and one of them would lose its bytes.

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
SERVER="YOUR.IP.GOES.HERE"
SSH_KEY=""
REMOTE_DIR="/root/go-hlrecorder"
REMOTE_DATA_DIR="$REMOTE_DIR/data"
REMOTE_BIN="$REMOTE_DIR/hlrecorder"
REMOTE_CONFIG="$REMOTE_DIR/collector_hl.yaml"
REMOTE_LOG="$REMOTE_DIR/hlrecorder.log"

CONFIG_NAME="collector_hl.yaml"
DURATION=""
WIPE_DATA=0
FORCE=0

LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_BIN="$LOCAL_DIR/bin/hlrecorder"
LOCAL_CONFIG="$LOCAL_DIR/$CONFIG_NAME"

SSH_OPTS=(-o StrictHostKeyChecking=no -o BatchMode=yes)

# ── Parse flags ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -server)
      SERVER="$2"; shift 2 ;;
    -ssh-key)
      SSH_KEY="$2"; shift 2 ;;
    -config)
      CONFIG_NAME="$2"; shift 2
      LOCAL_CONFIG="$LOCAL_DIR/$CONFIG_NAME" ;;
    -duration)
      DURATION="$2"; shift 2 ;;
    -wipe-data)
      WIPE_DATA=1; shift ;;
    -force)
      FORCE=1; shift ;;
    *)
      echo "Unknown flag: $1" >&2
      echo "Usage: $0 -server root@IP -ssh-key ~/.ssh/id_ed25519 [-config collector_hl.yaml] [-duration 1h] [-wipe-data] [-force]" >&2
      exit 1 ;;
  esac
done

# ── Validate ─────────────────────────────────────────────────────────────────
if [[ -z "$SERVER" || "$SERVER" == *"YOUR.IP.GOES.HERE"* ]]; then
  echo "Error: -server is required (e.g. -server root@YOUR.IP.GOES.HERE)" >&2
  exit 1
fi
if [[ -z "$SSH_KEY" ]]; then
  echo "Error: -ssh-key is required (e.g. -ssh-key ~/.ssh/id_ed25519)" >&2
  exit 1
fi

# Guard: never let this script near the Polymarket recorder's directory. The paths are
# hardcoded above, so this is a tripwire for a careless future edit rather than a
# runtime condition — which is exactly the kind of edit that deletes someone's capture.
# Checked before anything else, so a wrong path is refused even if the config is missing.
case "$REMOTE_DIR" in
  */go-collector|*/go-collector/*)
    echo "Error: REMOTE_DIR ($REMOTE_DIR) is the Polymarket collector's directory." >&2
    echo "       This script must use its own, e.g. /root/go-hlrecorder." >&2
    exit 1 ;;
esac
if [[ "$REMOTE_DIR" != *hl* ]]; then
  echo "Error: REMOTE_DIR ($REMOTE_DIR) does not look like a Hyperliquid recorder directory." >&2
  exit 1
fi

if [[ ! -f "$LOCAL_CONFIG" ]]; then
  echo "Error: config not found: $LOCAL_CONFIG" >&2
  exit 1
fi

# ── 1. Build for Linux amd64 ─────────────────────────────────────────────────
echo "==> Building hlrecorder for linux/amd64 ..."
cd "$LOCAL_DIR"
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o "$LOCAL_BIN" ./cmd/hlrecorder
echo "    built: $LOCAL_BIN"

# Validate the config before shipping it, so a bad file fails here rather than on the
# droplet at 3am. -dry-run builds the real subscription frames and exits.
echo "==> Validating $CONFIG_NAME ..."
go run ./cmd/hlrecorder -config "$CONFIG_NAME" -dry-run > /dev/null
echo "    config is valid"

# ── 2. Refuse to run twice ───────────────────────────────────────────────────
# The bracket in '[h]lrecorder' is load-bearing, not cosmetics. Over ssh this runs inside a
# shell whose own command line contains the pattern, so a plain `pgrep -f hlrecorder` matches
# the checking shell and always reports one process — a guard that fires every single time.
# '[h]lrecorder' still matches the string "hlrecorder" in another process's argv, but the
# shell's own argv contains the literal brackets, which the pattern does not match.
running="$(ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$SERVER" "pgrep -fc '[h]lrecorder' 2>/dev/null || true" | tr -d '[:space:]')"
if [[ -n "$running" && "$running" != "0" ]]; then
  if [[ "$FORCE" != "1" ]]; then
    echo "Error: hlrecorder is already running on $SERVER ($running process(es))." >&2
    echo "       Two recorders would fight over the same hour file." >&2
    echo "       Stop it first:  ssh -i $SSH_KEY $SERVER \"pkill -f '[h]lrecorder'\"" >&2
    echo "       Or pass -force if you know what you are doing." >&2
    exit 1
  fi
  echo "    warning: $running hlrecorder process(es) already running (-force given)"
fi

# ── 3. Ship binary + config ──────────────────────────────────────────────────
echo "==> Shipping to $SERVER ..."
if [[ "$WIPE_DATA" == "1" ]]; then
  # Only on request, and never silently: this deletes recorded hours.
  echo "    -wipe-data: deleting $REMOTE_DATA_DIR (recorded captures will be LOST)"
  ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$SERVER" "rm -rf $REMOTE_DATA_DIR"
fi
ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$SERVER" "mkdir -p $REMOTE_DIR $REMOTE_DATA_DIR"

# The binary and config are replaced; the data directory is left alone.
scp -i "$SSH_KEY" "${SSH_OPTS[@]}" "$LOCAL_BIN" "$SERVER:$REMOTE_BIN"
scp -i "$SSH_KEY" "${SSH_OPTS[@]}" "$LOCAL_CONFIG" "$SERVER:$REMOTE_CONFIG"
echo "    deployed: $REMOTE_BIN"
echo "    deployed: $REMOTE_CONFIG"
echo "    data dir: $REMOTE_DATA_DIR (untouched)"

# ── 4. Start it, detached ────────────────────────────────────────────────────
# The recorder is long-running, so it must be fully detached: foregrounding it through
# ssh would leave deploy.sh attached to the live log stream and it would never return.
# Plain "nohup ... &" is not enough either — the process would stay in the same session
# as the sshd channel, so ssh would wait for it to exit. setsid puts it in a brand-new
# session with no controlling tty, so sshd sees the session end and the recorder keeps
# going.
#
# No GOMEMLIMIT here on purpose: the whole process is ~35 MB RSS (4 gzip writers, 4
# queues, 4 sockets), so a heap cap would be decoration. What actually protects this box
# is the free-space guard in the config, which stops the recorder cleanly rather than
# letting the disk fill.
DURATION_ARGS=""
if [[ -n "$DURATION" ]]; then
  DURATION_ARGS="-duration $DURATION"
  echo "==> Starting a bounded capture (-duration $DURATION) ..."
else
  echo "==> Starting hlrecorder (log: $REMOTE_LOG) ..."
fi

ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$SERVER" \
  "cd $REMOTE_DIR && setsid ./hlrecorder -config $REMOTE_CONFIG $DURATION_ARGS > $REMOTE_LOG 2>&1 < /dev/null &"

# Give it a moment to dial and subscribe, then show only the tail (not a live follow).
sleep 5
echo "==> Startup log:"
ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$SERVER" "tail -n 20 $REMOTE_LOG"

echo
echo "==> Done. Recorder running in the background on $SERVER."
echo "    tail -f:      ssh -i $SSH_KEY $SERVER 'tail -f $REMOTE_LOG'"
echo "    stop:         ssh -i $SSH_KEY $SERVER \"pkill -f '[h]lrecorder'\""
echo "    capture:      $REMOTE_DATA_DIR  (use download.sh to fetch it)"
