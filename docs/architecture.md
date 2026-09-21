# 系统架构

项目是 Rust Cargo workspace，主要 crate 为 `domain`、`exchange-binance`、`exchange-polymarket`、`matcher`、`arbitrage`、`execution` 和 `app`。

`app` 负责配置、扫描循环和 Axum 后台；交易所 crate 将原始公开接口转换为统一行情；`matcher` 先验证事件和结算兼容性；`arbitrage` 使用订单簿深度和 Decimal 计算两方向净空间；`execution` 当前只有模拟执行。扫描结果写入 SQLite WAL，页面从 API 读取实时状态、历史排名、结算、模拟账本和自动影子执行记录。自动影子执行只对规则对齐的 1h 信号二次读取公开盘口并模拟两腿 FOK，不持有交易凭据，也不调用下单接口。

扫描器按轮更新市场和参考币价，记录双边卖一、完整卖盘深度下的可成交数量、动态费用、风险缓冲、净空间、抓取延迟、盘口时间差、盘口年龄、距结算时间及参考币价。市场结束后继续回查双方结算结果。

同一轮扫描还分别计算 Binance 与 Polymarket 的同市场互补份额双买。Polymarket 内部计算仅组合同一个 condition 的 Up/Yes 与 Down/No，按两腿完整深度和逐档费用估算，因而不依赖跨平台 Oracle 兼容性；当前仍只记录和展示，不调用真实下单接口。

`basis` 后台任务独立读取 Binance 公开现货和 USDT 永续深度、标记价、指数价、资金费率及资金费周期。它以 BTCUSDT、ETHUSDT、SOLUSDT、XRPUSDT 为首批标的，模拟买现货和卖出等数量永续，结果写入 `binance_basis_observations`。该任务不需要账户权限，且不与预测市场固定兑付模型混用。

第二个基差任务通过 USDT-M 与现货 `exchangeInfo` 动态发现非永续合约和交易规则，读取现货 ask 与交割合约 bid 深度，按两边共同 `LOT_SIZE` 向下取整后计算持有到交割的现金套利，结果写入 `binance_delivery_basis_observations`。只接受实际处于交易状态、尚未到期且基础现货已纳入研究范围的线性合约。资金占用前利润为正时，任务按每合约每小时最多一次触发 250ms、1s、5s 延迟重报价，以原始两腿限价验证完整成交能力，结果写入 `binance_delivery_requotes`；整个流程只读取公开行情。

macOS 使用 LaunchAgent 自动启动 `scripts/run-observer.sh`，异常退出会重启。脚本在接通电源时用 `caffeinate -s` 防止自动睡眠。
