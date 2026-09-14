# BTC 5 分钟跨平台套利机器人

> Binance Prediction Market ↔ Polymarket CLOB 的 BTC 5 分钟 Up/Down 跨平台套利项目。使用 Rust，第一阶段仅提供行情读取、机会计算和模拟执行，绝对禁止真实交易。

## 1. 文档用途与当前状态

本文是整理后的项目需求与后续开发提示词，可作为资深量化交易系统工程师的实施说明。

当前已建立 Cargo workspace、领域模型、结算配对分级、套利计算、模拟执行，以及 Binance Prediction Trading / Polymarket 的只读行情观察台。真实交易仍然禁用。

观察台默认把数据写入 `.data/observer.sqlite3`（SQLite WAL）。每秒样本包括双边报价、扣费净空间、首档数量、整轮抓取耗时、跨平台盘口时间差和距结算时间；市场结束后继续回查双方结算结果。历史排名还统计最长连续正信号和首档最大理论利润，避免把同一窗口内重复出现的信号误认为独立交易机会。

macOS 本地常驻可使用 `config/com.van.arbitrage-observer.plist`。它在用户登录时启动 `target/release/app serve`，异常退出后自动重启，日志保存到 `.data/observer.log` 与 `.data/observer.error.log`。电脑关机期间无法采集行情，但已经写入 SQLite 的数据不会丢失。

## 2. 项目目标与策略范围

同时监控 Binance Prediction Market 和 Polymarket CLOB 的 BTC 5 分钟 Up/Down 市场，仅研究以下两种买入套利方向：

```text
方向 A：Binance UP ASK   + Polymarket DOWN ASK < 1
方向 B：Binance DOWN ASK + Polymarket UP ASK   < 1
```

统一定义 `ArbitrageDirection`：

- `BinanceUpPolymarketDown`
- `BinanceDownPolymarketUp`

示例：买入 1,000 份价格为 0.40 的 Binance UP，以及 1,000 份价格为 0.50 的 Polymarket DOWN，总成本为 900；如果两份合约严格互补且每份胜出兑付 1，则组合兑付为 1,000，理论毛利润为 100。

此示例忽略手续费、滑点、执行失败及兑付风险。金额使用统一计价单位表达；不得未经核实就把不同平台的抵押资产或结算币种等同为 USDT。实际币种、单位兑付金额与换算成本必须查证并纳入计算。

## 3. 不可违反的约束

- 第一阶段禁止连接真实资金、真实下单和实现可调用的真实下单逻辑。
- 模拟器不需要 API Secret；允许读取公开行情。
- 默认 `execution.mode = "simulation"`；当前阶段遇到 live 配置必须拒绝启动。
- API key 等凭据不得写入代码或提交版本库，通过 `.env` 注入，`.env.example` 仅包含占位内容。
- 不得为了让程序运行而假设两个平台一定存在相同的 BTC 5 分钟市场。
- 接入前核实 Binance 对应产品、市场、公开 API、访问条件及结算文档；不得杜撰端点或规则。无法确认时使用明确标记的 Mock 数据。
- 市场匹配与套利计算必须彻底分离；套利引擎只能接受已经通过结算兼容性验证的市场组合。
- Oracle 未知、结算规则未知、窗口无法精确对应或仅能根据名称猜测时，一律禁止套利。
- 不得仅因价格之和小于 1 就宣称无风险套利。

## 4. 技术栈与工程规范

使用 Rust stable，建议依赖：

```text
tokio、serde、serde_json、reqwest、tokio-tungstenite
tracing、tracing-subscriber、anyhow、thiserror
chrono、rust_decimal、uuid、config、dotenvy
```

- 金额、价格、概率、费用及核心计算禁止使用 `f64`，优先使用 `rust_decimal::Decimal`。
- 时间统一使用 UTC，明确外部时间戳单位与精度。
- 配置中的 Decimal 使用字符串，明确份额步长、价格精度与舍入规则。
- 不生成单个巨型代码文件；模块保持清晰领域边界。
- 每完成一个开发阶段，运行 `cargo check` 与 `cargo test`，修复错误后再继续。

## 5. 建议架构

优先采用 Cargo workspace；若初期采用单 crate 加 modules，也必须保留相同的领域边界。以下为未来结构，不在本次创建：

```text
btc-cross-platform-arbitrage/
├── Cargo.toml
├── readme.md
├── .env.example
├── .gitignore
├── config/
│   └── config.toml
└── crates/
    ├── domain/
    │   ├── Cargo.toml
    │   └── src/{lib,market,orderbook,position,event,opportunity}.rs
    ├── exchange-binance/
    │   ├── Cargo.toml
    │   └── src/{lib,client,market,websocket,parser}.rs
    ├── exchange-polymarket/
    │   ├── Cargo.toml
    │   └── src/{lib,client,market,websocket,parser}.rs
    ├── matcher/
    │   ├── Cargo.toml
    │   └── src/{lib,matcher,settlement}.rs
    ├── arbitrage/
    │   ├── Cargo.toml
    │   └── src/{lib,engine,calculator,risk}.rs
    ├── execution/
    │   ├── Cargo.toml
    │   └── src/{lib,executor,simulator,order_manager}.rs
    └── app/
        ├── Cargo.toml
        ├── src/main.rs
        └── tests/{arbitrage,matcher,orderbook}.rs
```

花括号表示多个独立文件。各 crate 的单元测试放在对应模块；跨模块集成测试放在 `app/tests/` 等实际 package 下，避免虚拟 workspace 根目录的测试未被 Cargo 自动发现。workspace 配置 `default-members` 指向 app，使文中的 `cargo run -- ...` 命令可以直接使用。

模块职责：

- `domain`：统一市场、事件、订单簿、持仓、机会及接口共享类型。
- `exchange-binance`：Binance 市场发现、公开行情适配、WebSocket 与解析。
- `exchange-polymarket`：Polymarket 市场发现、公开行情适配、WebSocket 与解析。
- `matcher`：事件标识、市场匹配、结算兼容性验证与拒绝原因。
- `arbitrage`：订单簿深度计算、双方向扫描、净利润计算与风险控制。
- `execution`：执行接口、模拟订单、部分成交、订单管理及未对冲处理。
- `app`：配置加载、CLI、日志、组件装配与事件循环。

交易所适配器负责数据规范化，套利引擎不得依赖平台原始响应结构。

## 6. 统一领域模型

### 基础类型

- `Venue`：`Binance`、`Polymarket`。
- `Outcome`：`Up`、`Down`。
- `Market`：`venue`、`market_id`、`symbol`、`outcome`、`start_time`、`end_time`、`settlement_source`、`settlement_rule`、`status`。
- `Quote`：`bid`、`ask`、`available_size`、`timestamp`；明确 `available_size` 对应的盘口方向，缺失报价不得用零价格伪装。
- `OrderBook`：`bids`、`asks`、`timestamp`，每档包含价格与数量，并关联平台、市场及 outcome。
- `EventIdentity`：`asset`、`start_time`、`end_time`、`settlement_source`、`settlement_rule`。
- `Position`：记录每腿成交量、成本、对冲份额、剩余敞口及结算状态。

`ArbitrageOpportunity` 至少包含：

```text
market_a, market_b, outcome_a, outcome_b
executable_size, cost, gross_payout, gross_profit
fees, slippage, net_profit, net_profit_rate, detected_at
```

同时记录方向、计价币种、安全缓冲、两腿 VWAP、使用的订单簿版本及验证结果，保证机会可追溯。

## 7. 市场匹配与结算验证

设计 `MarketMatcher`、`SettlementCompatibility`、`EventIdentity` 和 `SettlementCompatibilityValidator`，提供：

```text
match_markets()
validate_settlement_compatibility()
```

禁止以 `market.name.contains("BTC Up")` 作为匹配依据。必须验证：

- 标的确实为 BTC，5 分钟窗口、开始时间和结束时间完全一致。
- 两个平台 Up/Down 的定义，包括涨跌比较方式和参考价格取值时点。
- Oracle / Price Source、TWAP / Index Price / Spot Price 定义。
- 相等价格、窗口边界、缺失报价、异常行情、取消或作废等规则。
- 结算时间、结算流程及单位兑付定义。
- 所选两腿是否存在同时输、同时赢或无法保证组合兑付的情形。

仅允许完整且已知的 `EventIdentity` 精确匹配，或通过有明确依据的兼容性验证。两个未知值相等不能视为验证通过。验证结果应携带规则来源和拒绝原因；通过受控构造的已验证市场对传给套利引擎。

不兼容时返回明确错误，至少包括：

```text
SettlementMismatch::Oracle
SettlementMismatch::TimeWindow
SettlementMismatch::ReferencePrice
SettlementMismatch::Rule
SettlementMismatch::Unknown
```

## 8. 完整订单簿与可执行数量

必须支持完整深度，不能只读取 best ask。示例：

```text
Binance UP asks：    0.40 × 100，0.41 × 500，0.42 × 1000
Polymarket DOWN asks：0.50 × 200，0.51 × 500

买入两腿各 600 份：
Binance 成本    = 100 × 0.40 + 500 × 0.41 = 245
Polymarket 成本 = 200 × 0.50 + 400 × 0.51 = 304
组合成本       = 549
毛利润         = 600 - 549 = 51
```

逐档计算每腿真实 VWAP，并结合两边深度、手续费、滑点缓冲、利润门槛、资金和持仓限制求最大可套利数量。不能因为首档价格之和小于 1 就推断 10,000 份都可套利。

订单簿维护需处理快照、增量、序列缺口、乱序、重连及过期数据；同步状态不可信时停止使用该盘口计算机会。

## 9. 套利计算

实现 `calculate_buy_arbitrage()`，输入包括已验证市场对、UP 与 DOWN 订单簿、费用规则、滑点缓冲、安全缓冲、最小利润、最小利润率及最大持仓/交易限制。

```text
cost            = cost_of_buying_up + cost_of_buying_down
gross_payout    = quantity × 1
gross_profit    = gross_payout - cost
net_profit      = gross_profit - trading_fees - estimated_slippage - safety_buffer
net_profit_rate = net_profit / cost
```

`quantity × 1` 只适用于已确认互补、兑付单位相同的合约；否则应拒绝该组合。深度产生的实际买入成本已计入 `cost`，额外滑点缓冲不得重复计算同一项成本。费用模型按平台实际规则实现，不假设统一固定费率。

仅在以下条件同时成立且风控通过时输出 `ArbitrageOpportunity`：

```text
net_profit > minimum_profit
net_profit_rate > minimum_profit_rate
```

无机会或输入无效时返回明确结果；拒绝零成本、无效价格、负数量等输入。

## 10. 行情接口与扫描流程

定义 `trait MarketDataProvider`：

```text
async fn get_markets(...)
async fn get_orderbook(market_id, ...)
async fn subscribe_orderbook(...)
```

提供 `BinanceMarketDataProvider`、`PolymarketMarketDataProvider`，优先完成 `MockMarketDataProvider` 和固定订单簿测试数据。真实接口暂不可用时，保留适配边界并报告原因。

扫描流程：

```text
Market Discovery → Market Matching → Settlement Validation
→ Orderbook Update → Calculate Executable Quantity → Calculate Fees
→ Calculate Net Profit → Risk Check → Opportunity
→ Simulated Execution（仅在模拟执行流程中）
```

优先使用 WebSocket 和事件驱动架构进行高频更新，不把固定 `sleep` 轮询订单簿作为最终架构。`scan` 只扫描并显示机会，不提交任何订单。

## 11. 模拟执行与未来接口

实现 `SimulatedExecutor`，支持 `place_order()`、`cancel_order()`、`fill_order()`，模拟部分成交、拒单、超时和撤单竞争，不假设两腿同时成交。

示例：第一腿成交 1,000 份，第二腿成交 700 份，则未对冲数量为 300 份。立即产生 `UnhedgedPosition`，暂停新套利，进入模拟补腿或紧急处理。份额敞口与金额敞口分开记录。

设计并明确合法转换、触发事件与失败分支，覆盖以下状态：

```text
Detected, Preparing, Submitting, PartiallyFilled, FullyFilled
Hedged, Failed, Unhedged, Settled, Redeemed
```

这些状态不是要求按列表顺序线性执行；失败也不得丢弃已成交持仓。结算和赎回在本阶段同样仅为模拟行为。

定义统一 `trait Executor`，预留异步接口：

```text
async fn place_order(...)
async fn cancel_order(...)
async fn get_order(...)
async fn get_balance(...)
```

未来可新增 `BinanceExecutor` 和 `PolymarketExecutor`，替换模拟执行器而不修改套利引擎。当前不实现真实下单。未来真实交易代码必须隔离在明确 feature / module 中；live 模式必须同时要求 `LIVE_TRADING=true`，启动时打印 `WARNING: LIVE TRADING ENABLED`。

## 12. 风险引擎

实现 `RiskEngine`，至少检查：

- 最大单次交易金额、最大持仓、最大未对冲金额 `max_unhedged_exposure`。
- 最大滑点、最小净利润与最小利润率。
- 市场剩余时间、市场状态与结算兼容性。
- 两边流动性、API 延迟、数据时间戳与订单簿有效性。

任意一腿成交但另一腿未充分成交时立即识别敞口并暂停新套利，不等待敞口超过阈值；`max_unhedged_exposure` 用于进一步控制风险及紧急处理。模拟器也必须遵守此规则。

## 13. 配置与日志

未来 `config/config.toml` 的基础示例：

```toml
[arbitrage]
minimum_profit = "1"
minimum_profit_rate = "0.005"
max_trade_amount = "1000"
max_unhedged_exposure = "100"
slippage_buffer = "0.002"

[execution]
mode = "simulation"

[binance]
enabled = true

[polymarket]
enabled = true
```

实现时补充最大持仓、最短剩余时间、数据过期阈值、最大延迟、费用与安全缓冲等参数，并明确单位。建议 `slippage_buffer` 表示相对买入成本的比例，`"0.002"` 表示 0.2%。`enabled` 表示允许启用对应数据源，不代表开放交易能力。

使用 `tracing` 和 `tracing-subscriber` 输出可追踪日志，包括 UTC 时间、事件窗口、平台/市场标识、套利方向、best ask、每腿 VWAP、可执行份额、总成本、费用、滑点、安全缓冲、净利润及净 ROI。区分每份组合价格与整笔总成本，日志数值必须与计算结果一致。

同时记录匹配失败原因、风控拒绝原因、订单状态变化和未对冲敞口；禁止输出凭据。

## 14. CLI 与模拟器

待实现命令：

```bash
cargo run -- markets
cargo run -- orderbook
cargo run -- scan
cargo run -- simulator
cargo run -- status
cargo run -- serve
```

- `markets`：列出市场及匹配、结算验证情况。
- `orderbook`：查看指定平台、市场与 outcome 的订单簿深度。
- `scan`：只扫描、显示机会，不下单。
- `simulator`：生成两个平台的 BTC 5 分钟 Up/Down Mock 市场和盘口，计算机会并模拟执行。
- `status`：显示运行模式、数据源状态、模拟订单、持仓及未对冲敞口；实现时明确独立 CLI 进程读取运行状态的方式。
- `serve`：启动 `http://127.0.0.1:8787` 本地观察台，发现 Crypto Up/Down 市场，匹配 Polymarket 同 slug 事件并将观察写入 SQLite。需要 Binance 只读 API 凭据，因为其市场数据也是签名端点。

模拟器随机产生价格、流动性、价差和延迟，并实时打印机会；支持固定随机种子复现。Mock 结算规则必须显式设置，不能被当作真实平台规则的证明。

## 15. 测试与验收

必须覆盖以下单元或集成测试：

1. UP = 0.40、DOWN = 0.50：在费用和阈值允许时发现套利。
2. UP = 0.50、DOWN = 0.50：无套利。
3. UP = 0.40、DOWN = 0.60：无毛套利。
4. 毛利润为正但扣除手续费后无套利。
5. 第二档及后续深度变差，正确计算 VWAP 和数量上限。
6. 两边可成交数量不同，只允许按可对冲深度计算。
7. 部分成交被识别为未对冲，暂停新套利并进入模拟处理。
8. 时间窗口不一致时拒绝匹配。
9. Oracle 不一致时拒绝匹配。
10. Settlement rule 不一致时拒绝匹配。
11. 市场接近结束时被风控拦截。
12. 数据时间戳过期时被风控拦截。
13. Oracle 或结算规则未知，即使名称和价格匹配也拒绝套利。
14. 两种套利方向均正确计算。
15. 相等价格、边界条件和异常结算不能证明互补时拒绝套利。
16. 缺档、乱序、断线重连与无效订单簿不得产生虚假机会。
17. 当前版本拒绝 live 配置；模拟器无需密钥即可运行。

价格测试使用已验证的 Mock 市场对，显式配置费用、数量及利润阈值，避免仅测试价格之和而绕过结算校验。

## 16. 分阶段开发顺序

1. 分析架构并初始化最小 Cargo workspace、各 crate 入口和配置模板；不写交易代码。
2. 建立 domain model、Decimal 与 UTC 约定。
3. 建立完整订单簿与深度成本计算。
4. 建立市场匹配、结算兼容性验证及已验证市场对。
5. 建立双方向套利计算器与风险引擎。
6. 建立 Mock 行情、模拟执行状态机、模拟器与 CLI。
7. 在各阶段同步编写测试，并补齐端到端验收。
8. 最后核实并接入 Binance / Polymarket 公开行情，保持模拟执行。

每阶段执行：

```bash
cargo check
cargo test
```

必要时使用 `cargo check --workspace`、`cargo test --workspace` 验证所有成员。测试失败必须修复；因环境或依赖无法验证时如实报告，不宣称通过。

## 17. 后续开发任务的首次交付要求

开始代码开发时，第一步仅分析架构、创建 Cargo workspace 和最小可编译骨架，运行检查与测试后汇报：

1. 创建了哪些文件。
2. 每个模块负责什么。
3. 当前已经实现了什么。
4. 尚未实现什么，以及验证是否受阻。
5. 下一阶段建议做什么。

始终遵守：先验证结算事件，再计算套利；当前阶段绝不进入真实交易。
