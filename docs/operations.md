# 本地运行与运维

构建并启动：

```bash
cargo build --release --workspace
launchctl kickstart -k gui/$(id -u)/com.van.arbitrage-observer
```

访问 `http://127.0.0.1:8787`。查看状态和日志：

```bash
curl -sS http://127.0.0.1:8787/api/state
tail -f .data/observer.log
tail -f .data/observer.error.log
sqlite3 .data/observer.sqlite3 "select count(*), max(observed_at_ms) from observations;"
```

停止或重启可使用 `launchctl kickstart -k gui/$(id -u)/com.van.arbitrage-observer`，数据会保留在 SQLite。接通电源并只关闭显示器时，LaunchAgent 和 `caffeinate -s` 可继续运行；关机、断电或合盖进入睡眠后无法采集。定期备份 `.data/observer.sqlite3` 及其 WAL 文件。

提交前运行 `cargo fmt --all`、`cargo check --workspace`、`cargo test --workspace` 和 `git diff --check`，确认 `.env`、`.data/` 未进入 Git。
