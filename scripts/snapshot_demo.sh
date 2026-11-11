#!/usr/bin/env bash
# Refreshes the website's demo snapshot from your running stack:
#   bash scripts/snapshot_demo.sh
# The page falls back to web/demo/history.json when its API isn't reachable (and in the published preview).
set -euo pipefail
curl -sf http://localhost:8000/api/history -o web/demo/history.json
echo "saved web/demo/history.json ($(wc -c < web/demo/history.json) bytes)"
