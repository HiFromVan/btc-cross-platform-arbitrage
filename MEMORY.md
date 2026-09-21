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

页面默认显示实时对比，可选择 BTC/ETH/BNB 及 5m、15m、1h、1d，包含 Binance/Polymarket Up/Down 卖一、两边标的币价、价差、费用、净空间、深度和图表。后端通过三个独立 RTDS 连接订阅 BTC/ETH/BNB Chainlink 60 秒 TWAP，三种资产均已验证持续入库；浏览器页面另有 RTDS 展示兜底。

## 已知结论

截至 2026-09-15，首批 562 个已完成双边结算的窗口中有 43 个方向分歧。公开 API 复核确认这些记录是两边均已结算但胜出方向不同，主要来自 5m/15m 市场的 Chainlink USDT Top of Book 与 Chainlink USD 60 秒 TWAP 差异。第二版严格成交模型的首批 10 笔已结算模拟合计亏损约 51.02，说明不能把跨 Oracle 价差当作套利。1h 市场两边均使用 Binance USDT 小时 K 线，历史起止价和胜出方向一致；第四版模拟模型只允许这类结算规则对齐、最多仅有 USDT/USDC 兑付差异的组合。该模型使用双方完整卖盘深度和动态 Polymarket 费率，每腿按指定份数与最高买入价执行 FOK 模拟，并扣除第二腿滑移、USDT/USDC 转换及资金占用缓冲。限价内深度不足时整腿零成交；跨平台无法原子撤回已经成交的另一腿，发生时必须标记未对冲并暂停。每个方向会保存可审计的拒绝原因，原始观察仍全部保留。

## 下次优先事项

1. 持续积累第四版 1h 结算对齐模型，统计净空间、信号持续时间、深度和实际模拟兑付。
2. 分开统计 USDT/USDC 兑付差异与双腿成交风险，不再把 5m/15m 跨 Oracle 信号计入可套利账本。
3. 后台已对规则对齐的 1h 首次合格信号自动执行 250ms 二次重报价 FOK 影子模拟，并写入 `shadow_executions`；持续统计双腿成功率、取消率、错误率和未对冲率，在完成长期验证前不进入实盘。

详细资料见 `docs/`。
