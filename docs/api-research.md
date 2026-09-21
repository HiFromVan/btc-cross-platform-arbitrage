# API 与数据来源

## Binance

项目使用公开市场发现、订单簿和 Spot ticker。参考币价使用 `/api/v3/ticker/price` 的 BTCUSDT、ETHUSDT、BNBUSDT。Prediction Trading 接口的可用市场、产品规则和结算定义必须以官方文档及实际响应为准，不能仅凭名称判断兼容性。

## Polymarket

市场元数据来自 Gamma，订单簿来自 CLOB。结算兼容性需核对事件、窗口、Oracle、参考价格和兑付规则。

Polymarket 官方 Chainlink TWAP 文档：`https://docs.polymarket.com/market-data/chainlink-twap.md`。实时数据服务为 `wss://ws-live-data.polymarket.com`，60 秒 TWAP topic 为 `crypto_prices_twap_sixty`，过滤器示例为 `{"symbol":"btc/usd"}`。返回的 `full_accuracy_value` 是 E18 定点字符串，代码使用 `Decimal` 转换，不能用浮点作为核心计算。

Polymarket taker 费用按市场元数据中的 `feeSchedule.rate` 读取。官方公式为 `fee = shares × feeRate × price × (1 - price)`，USDC 费用舍入到 5 位小数；CLOB `/fee-rate` 返回的 `base_fee` 是订单签名使用的 base fee 参数，不能直接当作上述小数费率。

## 凭据与网络

第一阶段公开行情不需要交易 Secret。任何 API key 只从本地 `.env` 读取，`.env` 已被 Git 忽略。网络代理由操作终端统一设置 `all_proxy`（必要时同时设置 `http_proxy`、`https_proxy`）为 `http://127.0.0.1:7890`。Rust WebSocket 库不会自动读取 HTTP 代理，连接失败时页面端 RTDS 仅作为展示兜底。
