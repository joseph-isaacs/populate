#!/usr/bin/env bash
# Fetch the LDBC Spark datagen's dictionaries (src/main/resources, ~72 MB),
# which populate-ldbc reads, at the commit populate-ldbc was ported from.
#
#   scripts/fetch-ldbc-resources.sh [DEST]
#
# DEST defaults to target/ldbc_snb_datagen_spark, where populate-ldbc looks
# when LDBC_DATAGEN_RESOURCES is unset.
set -euo pipefail
COMMIT=a2210c52ddeceeeafbbdd4d4d166d0d0da82f739
REPO=https://github.com/ldbc/ldbc_snb_datagen_spark.git
DEST=${1:-"$(cd "$(dirname "$0")/.." && pwd)/target/ldbc_snb_datagen_spark"}
if [ -d "$DEST/src/main/resources/dictionaries" ]; then
  echo "already there: $DEST/src/main/resources"
  exit 0
fi
mkdir -p "$DEST"
cd "$DEST"
git init -q
git remote add origin "$REPO" 2>/dev/null || true
git sparse-checkout set src/main/resources
git fetch -q --depth 1 --filter=blob:none origin "$COMMIT"
git checkout -q FETCH_HEAD
echo "resources in $DEST/src/main/resources"
echo "(or point LDBC_DATAGEN_RESOURCES at it)"
