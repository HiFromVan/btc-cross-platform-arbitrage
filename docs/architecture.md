# 系统架构

项目是 Rust Cargo workspace，主要 crate 为 `domain`、`exchange-binance`、`exchange-polymarket`、`matcher`、`arbitrage`、`execution` 和 `app`。

`app` 负责配置、扫描循环和 Axum 后台；交易所 crate 将原始公开接口转换为统一行情；`matcher` 先验证事件和结算兼容性；`arbitrage` 使用订单簿深度和 Decimal 计算两方向净空间；`execution` 当前只有模拟执行。扫描结果写入 SQLite WAL，页面从 API 读取实时状态、历史排名、结算和模拟账本。

扫描器每秒更新市场和参考币价，记录双边卖一、可成交数量、费用、净空间、抓取延迟、盘口时间差、距结算时间及参考币价。市场结束后继续回查双方结算结果。

macOS 使用 LaunchAgent 自动启动 `scripts/run-observer.sh`，异常退出会重启。脚本在接通电源时用 `caffeinate -s` 防止自动睡眠。
