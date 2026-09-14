# 项目记忆

## 当前阶段

这是 Binance Web3 Prediction Trading 与 Polymarket CLOB 的跨平台价差观察项目。当前只允许公开行情、市场匹配、统计和模拟执行，禁止真实资金和真实下单。凭据只能通过 `.env` 注入，不能提交。

## 当前运行

- 后台：`http://127.0.0.1:8787`
- 数据库：`.data/observer.sqlite3`（SQLite WAL，重启后继续累计）
- 日志：`.data/observer.log`、`.data/observer.error.log`
- macOS 常驻：`~/Library/LaunchAgents/com.van.arbitrage-observer.plist`
- 启动脚本：`scripts/run-observer.sh`
- 代理：启动脚本会探测并使用 `http://127.0.0.1:7890`

页面默认显示实时对比，可选择 BTC/ETH/BNB 及 5m、15m、1h、1d，包含 Binance/Polymarket Up/Down 卖一、两边标的币价、价差、费用、净空间、深度和图表。Polymarket Chainlink 60 秒 TWAP 由后端尝试订阅，浏览器页面另有 RTDS 展示兜底。

## 已知结论

首批已结算窗口的双方结果曾一致，但样本仍少，市场目前主要被标为 `basis`，不能据此认定无风险套利。必须继续记录至少数百至一千个窗口，验证 Oracle、时间边界、结算币种、盘口延迟、深度和双腿成交风险。当前策略建议分散多个窗口、小额模拟，禁止单次重仓。

## 下次优先事项

1. 检查 PM RTDS 后端连接稳定性和页面实时参考价。
2. 持续积累并分析结算兼容性、净空间、信号持续时间、深度和实际模拟兑付。
3. 完善手续费、滑点、资金占用和异常重连指标；在完成长期验证前不进入实盘。

详细资料见 `docs/`。
