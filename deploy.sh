#!/usr/bin/env bash
# Stamps a build id into index.html + build.txt, then deploys the worker.
set -e
cd "$(dirname "$0")"
B=$(date -u +%Y%m%d%H%M%S)
echo -n "$B" > public/build.txt
sed -i "s/const BUILD=\"[^\"]*\";/const BUILD=\"$B\";/" public/index.html
npx wrangler deploy "$@"
echo "build $B"
