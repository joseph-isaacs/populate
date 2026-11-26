#!/usr/bin/env bash
# Fails if the tree holds things CONTRIBUTING.md's "Docs and comments" rules keep
# out of the repository: graphs, planning docs and tool output, and
# PR-history wording in docs and comments. It catches the common cases, not
# every one.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

fail=0

files=$(git ls-files -- \
  ':(icase,glob)**/*.png' ':(icase,glob)**/*.svg' ':(icase,glob)**/*.jpg' \
  ':(icase,glob)**/*.jpeg' ':(icase,glob)**/*.gif' ':(icase,glob)**/*.webp' \
  ':(icase,glob)**/*.pdf' ':(icase,glob)**/*.log' ':(glob)**/.cargo-ok' \
  ':(glob)**/mutants.out*/**' ':(icase,glob)**/TODO.md' ':(icase,glob)**/FUTURE.md')
if [ -n "$files" ]; then
  echo "Files that belong in a PR description or an issue, not the tree:"
  echo "$files"
  fail=1
fi

# Vendored code keeps its upstream wording; CONTRIBUTING.md quotes the phrases.
phrases='\b(before|after|on) this (PR|change)\b|\b(PR|pull request) #?[0-9]+|\b(before|after) (merging|(the )?review)\b|adversarial review|faster than (on )?main\b|\bused to (be|have)\b|\bsince .* landed\b'
hits=$(git grep -nIiE "$phrases" -- \
  '*.md' '*.rs' '*.toml' '*.sh' '*.py' '*.yml' '*.txt' '*.java' '*.R' '*.json' \
  ':(exclude,glob)**/CLAUDE.md' ':!CONTRIBUTING.md' ':!scripts/check-tree.sh' \
  ':(exclude,glob)**/dsbgen/**' ':(exclude,glob)**/dsbgen-arrow/**' || true)
if [ -n "$hits" ]; then
  echo "PR-history wording (describe the code as it is):"
  echo "$hits"
  fail=1
fi

# Extensionless text files outside scripts are usually pasted notes.
stray=$(git ls-files -- ':(exclude,glob)**/dsbgen/**' ':(exclude,glob)**/dsbgen-arrow/**' |
  grep -vE '(^|/)[^/]*\.[^/]*$' | grep -vE '^(LICENSE|NOTICE|COPYING)$|/(LICENSE|NOTICE|COPYING)$' || true)
if [ -n "$stray" ]; then
  echo "Files with no extension (notes or prompts?):"
  echo "$stray"
  fail=1
fi

exit $fail
