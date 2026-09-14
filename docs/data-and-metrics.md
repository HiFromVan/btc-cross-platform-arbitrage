# 数据与指标口径

SQLite 文件为 `.data/observer.sqlite3`，启用 WAL。观察记录以 UTC 毫秒时间戳保存，核心金额、价格、数量和费用使用 `rust_decimal::Decimal`。

每秒样本包含：市场标识和分类、Binance/Polymarket Up/Down 卖一及数量、两种方向的组合成本、手续费、净空间、可成交数量、首档理论利润、抓取耗时、双边盘口时间差、距结算时间、连续正信号时长，以及 Binance Spot 和 Polymarket Chainlink TWAP 参考币价。

方向 A 为 Binance UP + Polymarket DOWN；方向 B 为 Binance DOWN + Polymarket UP。只有结算兼容性通过、成本和数量有效且扣除手续费、滑点缓冲和安全缓冲后仍满足门槛，才算观察机会。历史排名按窗口聚合，避免把同一窗口内每秒重复信号误当独立交易。

结算记录保存双方最终结果和模拟交易兑付，用于验证“价格之和小于 1”是否真的对应可兑付组合。
