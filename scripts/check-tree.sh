#!/usr/bin/env bash
# Fails if the tree holds things CLAUDE.md's "Docs and comments" rules keep
# out of the repository: result files and graphs, planning docs, and
# PR-history wording in docs and comments. Run from the repository root.
set -euo pipefail

fail=0

files=$(git ls-files -- '*.png' '*.svg' '*.jpg' '*.jpeg' '*.gif' '*.pdf' \
  '*.cargo-ok' '**/TODO.md' '**/FUTURE.md' 'TODO.md' 'FUTURE.md')
if [ -n "$files" ]; then
  echo "Files that belong in a PR description or an issue, not the tree:"
  echo "$files"
  fail=1
fi

# Vendored code keeps its upstream wording; CLAUDE.md quotes the phrases.
hits=$(git grep -nIiE 'before this (PR|change)|after (this|the) PR|on this PR|PR #?[0-9]+|adversarial review|faster than (on )?main' \
  -- '*.md' '*.rs' '*.toml' '*.sh' '*.py' ':!CLAUDE.md' ':!**/CLAUDE.md' \
  ':!scripts/check-tree.sh' ':!**/dsbgen/**' ':!**/dsbgen-arrow/**' || true)
if [ -n "$hits" ]; then
  echo "PR-history wording (describe the code as it is):"
  echo "$hits"
  fail=1
fi

exit $fail
