#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME="colombo-dev"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is not installed. Install tmux first (e.g. brew install tmux)."
  exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "Session '$SESSION_NAME' already exists. Attaching..."
  if [ -n "${TMUX:-}" ]; then
    tmux switch-client -t "$SESSION_NAME"
  else
    tmux attach-session -t "$SESSION_NAME"
  fi
  exit 0
fi

BACKEND_CMD="cd \"$ROOT_DIR\" && ./mvnw spring-boot:run; code=\$?; echo; echo \"[backend stopped] shutting down tmux session...\"; tmux kill-session -t \"$SESSION_NAME\" >/dev/null 2>&1 || true; exit \$code"

# Left pane: backend app. Right pane: interactive shell for git/logs/curl/etc.
tmux new-session -d -s "$SESSION_NAME" -n "dev" -c "$ROOT_DIR" "bash -lc '$BACKEND_CMD'"
tmux split-window -h -t "$SESSION_NAME":0 -c "$ROOT_DIR" "bash -lc 'echo \"[colombo helper shell]\"; exec bash'"
tmux select-layout -t "$SESSION_NAME":0 even-horizontal
tmux set-option -t "$SESSION_NAME" -g mouse on
tmux set-option -t "$SESSION_NAME" -g history-limit 100000

if [ -n "${TMUX:-}" ]; then
  tmux switch-client -t "$SESSION_NAME"
else
  tmux attach-session -t "$SESSION_NAME"
fi
