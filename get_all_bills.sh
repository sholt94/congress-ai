#!/usr/bin/env bash
set -euo pipefail

# Project root where 'data/' lives
ROOT="/Users/skylerholt/civic/congress"
USCRUN="$ROOT/env/bin/usc-run"   # use the venv's executable explicitly

cd "$ROOT"
echo "Project root: $(pwd)"
echo "Using USCRUN: $USCRUN"

# Ensure usc-run exists
if [[ ! -x "$USCRUN" ]]; then
  echo "ERROR: $USCRUN not found or not executable."
  echo "Run:  source env/bin/activate && pip install congress"
  exit 1
fi

# Congresses with BILLS data on govinfo (93rd through 119th)
START=93
END=119

for C in $(seq "$START" "$END"); do
  echo "=== Downloading BILLS for the ${C}th Congress ==="
  "$USCRUN" govinfo --bulkdata=BILLS --congress="$C"
  echo "=== Finished ${C}th Congress ==="
done

echo "All done."
echo "Example bill text files:"
find data -type f -path "*/bills/*/BILLS-*.xml" | head -n 10

echo -n "Total BILLS (bill text) files: "
find data -type f -path "*/bills/*/BILLS-*.xml" | wc -l