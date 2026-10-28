#!/usr/bin/env bash
set -e

# Find maven binary: first check PATH, then user .m2 wrapper dists, then fallback
if command -v mvn >/dev/null 2>&1; then
    exec mvn "$@"
fi

M2_MVN=$(find "$HOME/.m2/wrapper/dists" -name mvn -type f -perm -111 2>/dev/null | head -n 1 || true)
if [ -n "$M2_MVN" ] && [ -x "$M2_MVN" ]; then
    exec "$M2_MVN" "$@"
fi

echo "Error: Maven not found in PATH or ~/.m2/wrapper/dists" >&2
exit 1
