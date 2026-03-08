#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_hook="$repo_root/.githooks/pre-commit"
target_hook="$repo_root/.git/hooks/pre-commit"

if [ ! -f "$source_hook" ]; then
    echo "missing hook template: $source_hook" >&2
    exit 1
fi

mkdir -p "$(dirname "$target_hook")"
cp "$source_hook" "$target_hook"
chmod +x "$target_hook"

echo "installed git hook: $target_hook"
