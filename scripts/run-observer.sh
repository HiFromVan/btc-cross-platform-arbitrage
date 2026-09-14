#!/bin/zsh
set -eu

PROJECT_DIR="/Users/van/computer/btc-cross-platform-arbitrage"
cd "$PROJECT_DIR"
mkdir -p .data

if [[ -z "${all_proxy:-}" ]] && /usr/bin/nc -z -G 1 127.0.0.1 7890 >/dev/null 2>&1; then
  export all_proxy="http://127.0.0.1:7890"
fi

# 接通电源时保持系统唤醒；关机或合盖导致的硬件睡眠无法在本机继续采集。
exec /usr/bin/caffeinate -s "$PROJECT_DIR/target/release/app" serve
