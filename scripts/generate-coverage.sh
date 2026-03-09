#!/usr/bin/env bash

set -euo pipefail

RAW_OUT="${1:-coverage.raw.out}"
OUT="${2:-coverage.out}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Mirror CI exclusions used previously in workflow YAML.
EXCLUDE_RE='github.com/orneryd/nornicdb/pkg/cypher/(fn|testutil)$|github.com/orneryd/nornicdb/pkg/nornicgrpc/gen$|github.com/orneryd/nornicdb/pkg/localllm$|github.com/orneryd/nornicdb/pkg/gpu($|/)|github.com/orneryd/nornicdb/pkg/graphql/generated$'

run_pkg_with_retry() {
	local pkg="$1"
	local prof="$2"
	local attempts=3

	for attempt in $(seq 1 "$attempts"); do
		if go test -coverprofile="$prof" "$pkg"; then
			return 0
		fi
		if [ "$attempt" -lt "$attempts" ]; then
			echo "retrying coverage test for $pkg (attempt $((attempt + 1))/$attempts)" >&2
			sleep 1
		fi
	done

	echo "coverage test failed after $attempts attempts: $pkg" >&2
	return 1
}

PKGS=()
while IFS= read -r pkg; do
	PKGS+=("$pkg")
done < <(go list ./pkg/... | grep -Ev "$EXCLUDE_RE")

echo "mode: set" > "$RAW_OUT"

i=0
for pkg in "${PKGS[@]}"; do
	i=$((i + 1))
	prof="$TMP_DIR/$i.cover"
	run_pkg_with_retry "$pkg" "$prof"
	# Skip mode line; append statement blocks.
	tail -n +2 "$prof" >> "$RAW_OUT"
done

bash scripts/filter-generated-coverage.sh "$RAW_OUT" "$OUT"
