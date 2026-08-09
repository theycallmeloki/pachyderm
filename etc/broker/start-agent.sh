#!/bin/sh
# start-agent.sh — bring up a pachd broker agent (a light kubelet) on a
# second node so pachd can place pipeline workers there.
#
# Node B prerequisites:
#   1. docker (the agent runs workers as containers of the pipeline's image)
#   2. the pachd worker binary (same build as the daemon) — PACH_WORKER_BINARY
#   3. network reachability to the pachd node (same LAN as the daemon's mDNS)
#
# Usage:
#   start-agent.sh --tag gpu[,cpu] [--join <pachd-host>] [--worker-binary <path>]
#
# With no --join, the agent discovers pachd via mDNS on the local link and
# derives the broker address from the advertisement. Set PACH_AGENT_TAG and
# PACH_WORKER_BINARY in the environment instead of flags if you prefer.
#
# The tag is the pipeline contract: pipelines whose transform env carries
# PACH_NODE_TAG matching a registered tag run their workers on this node
# (comma-separated tags accept any of them; placement is round-robin across
# the nodes serving the first matching tag).
set -e

TAG="${PACH_AGENT_TAG:-}"
JOIN="${PACH_JOIN:-}"
WORKER="${PACH_WORKER_BINARY:-}"
BROKER_DIR="${PACH_AGENT_DIR:-/var/lib/pachd-agent}"

while [ $# -gt 0 ]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --join) JOIN="$2"; shift 2 ;;
        --worker-binary) WORKER="$2"; shift 2 ;;
        --local-dir) BROKER_DIR="$2"; shift 2 ;;
        *) echo "start-agent.sh: unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [ -z "$TAG" ]; then
    echo "start-agent.sh: no tag given (--tag or PACH_AGENT_TAG)" >&2
    exit 1
fi
if [ -z "$WORKER" ]; then
    echo "start-agent.sh: no worker binary given (--worker-binary or PACH_WORKER_BINARY)" >&2
    exit 1
fi

AGENT="$(command -v pachd-agent || echo /usr/local/bin/pachd-agent)"
if [ ! -x "$AGENT" ]; then
    echo "start-agent.sh: pachd-agent not found (build it with: go build -o \$GOPATH/bin/pachd-agent ./src/server/cmd/agent)" >&2
    exit 1
fi

exec "$AGENT" \
    --tag "$TAG" \
    --local-dir "$BROKER_DIR" \
    --worker-binary "$WORKER" \
    ${JOIN:+--join "$JOIN"} \
    "$@"
