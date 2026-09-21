use crate::scanner::DashboardState;
use crate::storage::{BinanceInternalStatsView, RankingView, SettlementView, Storage};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
struct WebState {
    dashboard: Arc<RwLock<DashboardState>>,
    storage: Storage,
}

pub fn router(state: Arc<RwLock<DashboardState>>, storage: Storage) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/history/{slug}", get(api_history))
        .route("/api/rankings", get(api_rankings))
        .route(
            "/api/binance-internal-stats",
            get(api_binance_internal_stats),
        )
        .route(
            "/api/polymarket-internal-stats",
            get(api_polymarket_internal_stats),
        )
        .route("/api/binance-basis", get(api_binance_basis))
        .route(
            "/api/binance-delivery-basis",
            get(api_binance_delivery_basis),
        )
        .route(
            "/api/binance-delivery-requotes",
            get(api_binance_delivery_requotes),
        )
        .route(
            "/api/binance-basis/{symbol}",
            get(api_binance_basis_history),
        )
        .route("/api/paper-trades", get(api_paper_trades))
        .route("/api/shadow-executions", get(api_shadow_executions))
        .route("/api/settlements", get(api_settlements))
        .with_state(WebState {
            dashboard: state,
            storage,
        })
}

async fn api_state(State(state): State<WebState>) -> Json<DashboardState> {
    Json(state.dashboard.read().await.clone())
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
}

async fn api_history(
    State(state): State<WebState>,
    Path(slug): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<crate::scanner::ObservationView>>, StatusCode> {
    state
        .storage
        .history(&slug, query.limit.unwrap_or(300).min(2_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_rankings(State(state): State<WebState>) -> Result<Json<Vec<RankingView>>, StatusCode> {
    let storage = state.storage.clone();
    tokio::task::spawn_blocking(move || storage.rankings())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_binance_internal_stats(
    State(state): State<WebState>,
) -> Result<Json<Vec<BinanceInternalStatsView>>, StatusCode> {
    let storage = state.storage.clone();
    tokio::task::spawn_blocking(move || storage.binance_internal_stats())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_polymarket_internal_stats(
    State(state): State<WebState>,
) -> Result<Json<Vec<BinanceInternalStatsView>>, StatusCode> {
    let storage = state.storage.clone();
    tokio::task::spawn_blocking(move || storage.polymarket_internal_stats())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_binance_basis(
    State(state): State<WebState>,
) -> Result<Json<Vec<crate::basis::BinanceBasisView>>, StatusCode> {
    state
        .storage
        .binance_basis_latest()
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_binance_delivery_basis(
    State(state): State<WebState>,
) -> Result<Json<Vec<crate::basis::BinanceDeliveryBasisView>>, StatusCode> {
    state
        .storage
        .binance_delivery_basis_latest()
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_binance_delivery_requotes(
    State(state): State<WebState>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<crate::basis::BinanceDeliveryRequoteView>>, StatusCode> {
    state
        .storage
        .binance_delivery_requotes(query.limit.unwrap_or(100).min(1_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_binance_basis_history(
    State(state): State<WebState>,
    Path(symbol): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<crate::basis::BinanceBasisView>>, StatusCode> {
    state
        .storage
        .binance_basis_history(&symbol, query.limit.unwrap_or(300).min(2_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_paper_trades(
    State(state): State<WebState>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<crate::scanner::PaperTradeView>>, StatusCode> {
    state
        .storage
        .paper_trades(query.limit.unwrap_or(100).min(1_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_shadow_executions(
    State(state): State<WebState>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<crate::scanner::ShadowExecutionView>>, StatusCode> {
    state
        .storage
        .shadow_executions(query.limit.unwrap_or(100).min(1_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_settlements(
    State(state): State<WebState>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<Vec<SettlementView>>, StatusCode> {
    state
        .storage
        .settlements(query.limit.unwrap_or(200).min(1_000))
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>跨平台价差观察台</title>
  <style>
    :root{--ink:#16181b;--muted:#687078;--line:#d9dde1;--paper:#f5f6f7;--surface:#fff;--green:#087f5b;--green-bg:#e6f5ef;--red:#b42318;--red-bg:#fce8e6;--amber:#8a5a00;--amber-bg:#fff3d6;--blue:#2457a7;--nav:#20252b}
    *{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.45 ui-sans-serif,system-ui,-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;letter-spacing:0}
    button,select{font:inherit}header{height:58px;background:var(--nav);color:#fff;display:flex;align-items:center;padding:0 22px;gap:20px}header h1{font-size:17px;margin:0;font-weight:650}header .mode{font-size:12px;padding:3px 8px;border:1px solid #5f6872;border-radius:4px;color:#d9e0e6}header .health{margin-left:auto;display:flex;align-items:center;gap:8px;color:#d9e0e6;font-size:12px}.dot{width:8px;height:8px;border-radius:50%;background:#adb5bd}.dot.ok{background:#40c78a}.dot.bad{background:#ff766d}
    main{max-width:1480px;margin:auto;padding:18px 20px 40px}.metrics{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:1px;background:var(--line);border:1px solid var(--line);border-radius:6px;overflow:hidden}.metric{background:var(--surface);padding:13px 15px;min-height:72px}.metric label{display:block;color:var(--muted);font-size:12px;margin-bottom:5px}.metric strong{font-size:22px;font-weight:650}.metric small{color:var(--muted);margin-left:5px}
    .tabs{display:flex;gap:20px;margin-top:18px;border-bottom:1px solid var(--line)}.tab{border:0;background:transparent;padding:9px 2px;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent}.tab.active{color:var(--ink);border-color:var(--blue);font-weight:700}.view{display:none}.view.active{display:block}.toolbar{display:flex;align-items:center;gap:8px;margin:12px 0 10px}.toolbar h2{font-size:15px;margin:0 auto 0 0}.toolbar select,.toolbar button{height:32px;border:1px solid var(--line);background:var(--surface);border-radius:4px;padding:0 9px;color:var(--ink)}.toolbar button{cursor:pointer}.toolbar button:hover{border-color:#9ca3aa}
    .workspace{display:grid;grid-template-columns:minmax(700px,1fr) 360px;gap:12px;align-items:start}.table-wrap{background:var(--surface);border:1px solid var(--line);border-radius:6px;overflow:auto}table{width:100%;border-collapse:collapse;white-space:nowrap}th{text-align:left;color:var(--muted);font-size:11px;text-transform:uppercase;font-weight:650;background:#fafafa;padding:10px;border-bottom:1px solid var(--line)}td{padding:10px;border-bottom:1px solid #eceff1;font-variant-numeric:tabular-nums}tbody tr{cursor:pointer}tbody tr:hover,tbody tr.selected{background:#f2f5f8}.asset{font-weight:700}.sub{display:block;color:var(--muted);font-size:11px;margin-top:2px}.badge{display:inline-block;border-radius:4px;padding:2px 6px;font-size:11px;font-weight:650}.basis,.pending{background:var(--amber-bg);color:var(--amber)}.exact,.matching{background:var(--green-bg);color:var(--green)}.incompatible,.divergent{background:var(--red-bg);color:var(--red)}.positive{color:var(--green);font-weight:700}.negative{color:var(--muted)}.error{color:var(--red)}
    aside{background:var(--surface);border:1px solid var(--line);border-radius:6px;min-height:360px;position:sticky;top:12px}aside h2{font-size:15px;margin:0;padding:14px 15px;border-bottom:1px solid var(--line)}.detail{padding:14px 15px}.detail h3{font-size:13px;margin:17px 0 7px}.detail h3:first-child{margin-top:0}.kv{display:grid;grid-template-columns:1fr auto;gap:7px 12px}.kv span:nth-child(odd){color:var(--muted)}.kv span:nth-child(even){font-variant-numeric:tabular-nums;text-align:right}.rule{margin:5px 0;padding:7px 9px;background:var(--amber-bg);color:#624500;border-radius:4px;font-size:12px;white-space:normal}.chart{width:100%;height:130px;border:1px solid var(--line);border-radius:4px;background:#fff}.legend{display:flex;gap:12px;color:var(--muted);font-size:11px;margin:5px 0}.legend i{display:inline-block;width:12px;height:2px;vertical-align:middle;margin-right:4px}.empty{color:var(--muted);padding:40px 15px;text-align:center}.notice{margin-top:12px;color:var(--muted);font-size:12px}.loading{height:2px;background:var(--blue);position:fixed;left:0;top:58px;transition:width .25s;width:0}.wide-panel{background:var(--surface);border:1px solid var(--line);border-radius:6px;overflow:auto;margin-top:10px}.section-note{color:var(--muted);font-size:12px;margin:10px 0}.model{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;color:var(--muted)}
    .live-status{display:flex;align-items:center;gap:12px;background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:13px 15px;margin-bottom:12px}.live-status h2{font-size:17px;margin:0}.live-status .slug{color:var(--muted);font-size:11px}.live-status .countdown{margin-left:auto;text-align:right}.live-status .countdown strong{display:block;font-size:20px;font-variant-numeric:tabular-nums}.venue-grid,.direction-grid,.chart-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-bottom:12px}.venue,.direction,.chart-panel{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:14px 15px}.venue h3,.direction h3,.chart-panel h3{font-size:13px;margin:0 0 11px}.coin-price{border-left:3px solid var(--blue);padding:2px 0 9px 11px;margin-bottom:10px}.coin-price label{display:block;color:var(--muted);font-size:11px}.coin-price strong{display:block;font-size:25px;font-variant-numeric:tabular-nums;margin:1px 0}.coin-price small{color:var(--muted);font-size:11px}.quote-pair{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--line);border:1px solid var(--line)}.quote{background:#fff;padding:12px}.quote label,.calc label{display:block;color:var(--muted);font-size:11px}.quote strong{display:block;font-size:26px;margin-top:4px;font-variant-numeric:tabular-nums}.quote small{color:var(--muted)}.formula{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--muted);font-size:11px;margin-bottom:10px}.calc-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px}.calc{border-left:2px solid var(--line);padding-left:9px}.calc strong{display:block;margin-top:3px;font-size:16px;font-variant-numeric:tabular-nums}.direction.profitable{border-color:#83cfb4;background:#fbfffd}.direction.profitable .net strong{color:var(--green)}.live-chart{display:block;width:100%;height:230px}.reference-chart{grid-column:1/-1}.chart-legend{display:flex;flex-wrap:wrap;gap:12px;color:var(--muted);font-size:11px;margin-bottom:7px}.chart-legend i{display:inline-block;width:13px;height:3px;vertical-align:middle;margin-right:4px}.quality-row{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);font-size:11px;margin:3px 0 13px}.quality-row strong{color:var(--ink);font-weight:600}.auto-label{color:var(--green);font-size:11px;font-weight:650}
    @media(max-width:980px){.workspace{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}aside{position:static}.toolbar{flex-wrap:wrap}.toolbar h2{width:100%;margin-bottom:4px}.calc-grid{grid-template-columns:repeat(2,1fr)}}
    @media(max-width:620px){header{padding:0 12px}.metrics{grid-template-columns:1fr 1fr}main{padding:12px 10px}.metric strong{font-size:18px}.workspace{display:block}aside{margin-top:10px}.venue-grid,.direction-grid,.chart-grid{grid-template-columns:1fr}.live-status{align-items:flex-start;flex-wrap:wrap}.live-status .countdown{margin-left:0;width:100%;text-align:left}.calc-grid{grid-template-columns:repeat(2,1fr)}}
  </style>
</head>
<body>
  <header><h1>跨平台价差观察台</h1><span class="mode">仅观察 · 禁止下单</span><div class="health"><span id="statusText">连接中</span><span id="statusDot" class="dot"></span></div></header>
  <div id="loading" class="loading"></div>
  <main>
    <section class="metrics">
      <div class="metric"><label>候选配对</label><strong id="pairCount">0</strong><small>个</small></div>
      <div class="metric"><label>当前正净空间</label><strong id="signalCount">0</strong><small>个</small></div>
      <div class="metric"><label>累计观察</label><strong id="observationCount">0</strong><small>次</small></div>
      <div class="metric"><label>研究信号</label><strong id="opportunityCount">0</strong><small>次</small></div>
    </section>
    <nav class="tabs"><button class="tab active" data-view="livePanel">实时对比</button><button class="tab" data-view="basisPanel">币安基差</button><button class="tab" data-view="binancePanel">币安内部</button><button class="tab" data-view="polymarketPanel">PM 内部</button><button class="tab" data-view="radarPanel">市场雷达</button><button class="tab" data-view="rankingPanel">历史排名</button><button class="tab" data-view="settlementPanel">结算对比</button><button class="tab" data-view="tradePanel">模拟账本</button></nav>
    <section id="livePanel" class="view active">
      <div class="toolbar"><h2>当前交易对</h2><span class="auto-label">自动衔接下一窗口</span><select id="liveAsset"></select><select id="liveDuration"></select></div>
      <div id="liveEmpty" class="empty">正在等待当前窗口行情</div>
      <div id="liveContent" hidden>
        <div class="live-status"><div><h2 id="liveTitle">—</h2><span id="liveSlug" class="slug">—</span></div><span id="liveClass" class="badge">—</span><div class="countdown"><strong id="liveCountdown">—</strong><span>距离结算</span></div></div>
        <div class="venue-grid">
          <section class="venue"><h3>Binance</h3><div class="coin-price"><label id="binanceCoinLabel">当前币价</label><strong id="binanceCoinPrice">—</strong><small id="binanceCoinMeta">Binance Spot · 等待数据</small></div><div class="quote-pair"><div class="quote"><label>UP ASK</label><strong id="binanceUp">—</strong><small id="binanceUpSize">深度 —</small></div><div class="quote"><label>DOWN ASK</label><strong id="binanceDown">—</strong><small id="binanceDownSize">深度 —</small></div></div></section>
          <section class="venue"><h3>Polymarket</h3><div class="coin-price"><label id="pmCoinLabel">当前币价</label><strong id="pmCoinPrice">—</strong><small id="pmCoinMeta">Chainlink 60s TWAP · 等待数据</small></div><div class="quote-pair"><div class="quote"><label>UP ASK</label><strong id="pmUp">—</strong><small id="pmUpSize">深度 —</small></div><div class="quote"><label>DOWN ASK</label><strong id="pmDown">—</strong><small id="pmDownSize">深度 —</small></div></div></section>
        </div>
        <div class="direction-grid">
          <section id="directionA" class="direction"><h3>方向 A</h3><div class="formula">Binance UP + Polymarket DOWN</div><div class="calc-grid"><div class="calc"><label>深度均价成本/份</label><strong id="aCost">—</strong></div><div class="calc"><label>手续费/份</label><strong id="aFees">—</strong></div><div class="calc"><label>风险缓冲/份</label><strong id="aBuffers">—</strong></div><div class="calc net"><label>净利润/份</label><strong id="aNet">—</strong></div><div class="calc"><label>可成交数量</label><strong id="aSize">—</strong></div><div class="calc net"><label>深度理论利润</label><strong id="aProfit">—</strong></div></div><div id="aRejections" class="formula">—</div></section>
          <section id="directionB" class="direction"><h3>方向 B</h3><div class="formula">Binance DOWN + Polymarket UP</div><div class="calc-grid"><div class="calc"><label>深度均价成本/份</label><strong id="bCost">—</strong></div><div class="calc"><label>手续费/份</label><strong id="bFees">—</strong></div><div class="calc"><label>风险缓冲/份</label><strong id="bBuffers">—</strong></div><div class="calc net"><label>净利润/份</label><strong id="bNet">—</strong></div><div class="calc"><label>可成交数量</label><strong id="bSize">—</strong></div><div class="calc net"><label>深度理论利润</label><strong id="bProfit">—</strong></div></div><div id="bRejections" class="formula">—</div></section>
        </div>
        <section id="binanceInternalLive" class="direction"><h3>Binance 同市场双买统计</h3><div class="formula">Binance UP + Binance DOWN，按最多 10 份完整卖盘深度计算</div><div class="calc-grid"><div class="calc"><label>深度成本/份</label><strong id="biCost">—</strong></div><div class="calc"><label>手续费/份</label><strong id="biFees">—</strong></div><div class="calc"><label>风险缓冲/份</label><strong id="biBuffers">—</strong></div><div class="calc net"><label>净利润/份</label><strong id="biNet">—</strong></div><div class="calc"><label>可成交数量</label><strong id="biSize">—</strong></div><div class="calc"><label>盘口时间差</label><strong id="biSkew">—</strong></div></div><div id="biRejections" class="formula">—</div></section>
        <section id="polymarketInternalLive" class="direction"><h3>Polymarket 同市场双买统计</h3><div class="formula">Polymarket UP/YES + DOWN/NO，按最多 10 份完整卖盘深度计算</div><div class="calc-grid"><div class="calc"><label>深度成本/份</label><strong id="piCost">—</strong></div><div class="calc"><label>手续费/份</label><strong id="piFees">—</strong></div><div class="calc"><label>风险缓冲/份</label><strong id="piBuffers">—</strong></div><div class="calc net"><label>净利润/份</label><strong id="piNet">—</strong></div><div class="calc"><label>可成交数量</label><strong id="piSize">—</strong></div><div class="calc"><label>盘口时间差</label><strong id="piSkew">—</strong></div></div><div id="piRejections" class="formula">—</div></section>
        <div class="quality-row"><span>两边币价差 <strong id="coinPriceDifference">—</strong></span><span>抓取耗时 <strong id="liveLatency">—</strong></span><span>A 盘口时间差 <strong id="liveASkew">—</strong></span><span>B 盘口时间差 <strong id="liveBSkew">—</strong></span><span>最后报价 <strong id="liveUpdated">—</strong></span></div>
        <div class="chart-grid">
          <section class="chart-panel reference-chart"><h3>Binance 现货与 Polymarket 结算参考币价</h3><div class="chart-legend"><span><i style="background:#087f5b"></i>Binance Spot</span><span><i style="background:#2457a7"></i>PM Chainlink 60s TWAP</span></div><canvas id="referenceChart" class="live-chart" width="1380" height="260"></canvas></section>
          <section class="chart-panel"><h3>两边当前合约价格</h3><div class="chart-legend"><span><i style="background:#087f5b"></i>币安 UP</span><span><i style="background:#e09f1f"></i>币安 DOWN</span><span><i style="background:#2457a7"></i>PM UP</span><span><i style="background:#a054a0"></i>PM DOWN</span></div><canvas id="priceChart" class="live-chart" width="680" height="280"></canvas></section>
          <section class="chart-panel"><h3>扣除费用和风险缓冲后的净利润/份</h3><div class="chart-legend"><span><i style="background:#087f5b"></i>方向 A</span><span><i style="background:#2457a7"></i>方向 B</span><span><i style="background:#b42318"></i>零线</span></div><canvas id="edgeChart" class="live-chart" width="680" height="280"></canvas></section>
        </div>
        <p class="notice">净利润空间使用最多 10 份完整卖盘深度，已扣除动态手续费、第二腿滑移、USDT/USDC 转换和资金占用缓冲。</p>
      </div>
    </section>
    <section id="basisPanel" class="view">
      <div class="toolbar"><h2>Binance 现货—USDT 永续资金费率套利</h2></div>
      <p class="section-note">模拟方向为买入现货、做空等数量 USDT 永续。按最多 1,000 USDT 的完整订单簿深度计算，保守计入现货 10 bps、永续 5 bps 的 taker 费率并按开仓和平仓各收一次，再加 10 bps 往返滑移。24 小时与 7 天收益假设当前资金费率保持不变，仅用于研究，不是收益承诺。</p>
      <div class="wide-panel"><table><thead><tr><th>交易对</th><th>模拟数量</th><th>现货买入 VWAP</th><th>永续卖出 VWAP</th><th>入场基差</th><th>当前资金费率</th><th>资金费 APR</th><th>每轮资金收入</th><th>24h 净收益</th><th>7d 净收益</th><th>回本轮数</th><th>响应时差</th><th>下次资金结算</th><th>判断</th><th>更新时间</th></tr></thead><tbody id="basisRows"></tbody></table></div>
      <p class="notice">第一版使用 REST 深度快照，响应时差不是交易所盘口更新时间；数量尚未按各交易对 LOT_SIZE 步长取整，因此只作为保守筛选器，不能直接用于下单。</p>
      <div class="toolbar"><h2>Binance 现货—USDT 交割合约现金套利</h2></div>
      <p class="section-note">模拟买入现货并做空同币种交割合约，数量按两边共同 LOT_SIZE 向下取整。资金占用前利润扣除保守往返手续费、10 bps 滑移和 2 bps 交割缓冲；最终利润再按 10% 年化计入持有至交割的现货资金成本。当前仅展示 Binance 实际挂牌合约。</p>
      <div class="wide-panel"><table><thead><tr><th>合约</th><th>期限</th><th>剩余天数</th><th>数量 / 步长</th><th>现货 VWAP</th><th>交割卖出 VWAP</th><th>毛基差</th><th>交易摩擦</th><th>交割缓冲</th><th>资金占用</th><th>资金占用前利润</th><th>最终净利润</th><th>净收益率</th><th>净 APR</th><th>响应时差</th><th>交割时间</th><th>判断</th><th>更新时间</th></tr></thead><tbody id="deliveryBasisRows"></tbody></table></div>
      <div class="toolbar"><h2>交割基差延迟重报价</h2></div>
      <p class="section-note">当资金占用前利润为正时，每个合约每小时最多触发一次只读影子验证；保留原始两腿最差成交限价，并在 250ms、1s、5s 后重新读取公开订单簿，检查两腿能否在原限价内完整成交。不会发送真实订单。</p>
      <div class="wide-panel"><table><thead><tr><th>合约</th><th>报价时间</th><th>延迟</th><th>数量</th><th>现货限价</th><th>交割限价</th><th>现货可成交</th><th>交割可成交</th><th>延迟后毛基差</th><th>状态 / 错误</th><th>检查时间</th></tr></thead><tbody id="deliveryRequoteRows"></tbody></table></div>
    </section>
    <section id="binancePanel" class="view">
      <div class="toolbar"><h2>Binance 同市场 Up + Down 统计</h2></div>
      <p class="section-note">原始价差只看卖一之和是否低于 1；正净空间使用双方完整卖盘，扣除 Binance 动态手续费、第二腿滑移和资金占用。静态合格还要求盘口时间差不超过 1 秒、年龄不超过 5 秒、距结算至少 60 秒；连续确认要求信号跨度至少 5 秒。</p>
      <div class="wide-panel"><table><thead><tr><th>资产/周期</th><th>采样</th><th>窗口</th><th>卖一和 &lt; 1</th><th>扣费后正空间</th><th>静态合格</th><th>连续确认</th><th>正空间均值</th><th>最大净空间</th><th>最大理论利润</th><th>最大盘口时差</th><th>最新采样</th></tr></thead><tbody id="binanceInternalRows"></tbody></table></div>
    </section>
    <section id="polymarketPanel" class="view">
      <div class="toolbar"><h2>Polymarket 同市场 Up/Yes + Down/No 统计</h2></div>
      <p class="section-note">两腿来自同一个 Polymarket condition，不依赖跨平台结算一致性。正净空间按双方完整卖盘计算，并扣除逐档 taker 手续费、第二腿滑移和资金占用；静态门槛及连续确认规则与 Binance 内部统计一致。</p>
      <div class="wide-panel"><table><thead><tr><th>资产/周期</th><th>采样</th><th>窗口</th><th>卖一和 &lt; 1</th><th>扣费后正空间</th><th>静态合格</th><th>连续确认</th><th>正空间均值</th><th>最大净空间</th><th>最大理论利润</th><th>最大盘口时差</th><th>最新采样</th></tr></thead><tbody id="polymarketInternalRows"></tbody></table></div>
    </section>
    <section id="radarPanel" class="view">
      <div class="toolbar"><h2>活跃候选</h2><select id="assetFilter"><option value="">全部资产</option></select><select id="durationFilter"><option value="">全部周期</option></select><select id="classFilter"><option value="">全部分类</option><option value="exact">严格匹配</option><option value="basis">基差候选</option><option value="incompatible">不兼容</option></select><button id="refresh">刷新</button></div>
      <div class="workspace">
        <div class="table-wrap"><table><thead><tr><th>市场</th><th>分类</th><th>方向 A 净空间</th><th>A 深度</th><th>方向 B 净空间</th><th>B 深度</th><th>结束时间</th><th>状态</th></tr></thead><tbody id="rows"></tbody></table></div>
        <aside><h2>配对证据与计算</h2><div id="detail" class="empty">选择一个市场查看详细信息</div></aside>
      </div>
      <p class="notice">正净空间已按完整卖盘深度扣除动态手续费及保守风险缓冲；5m/15m 跨 Oracle 市场只观察，不进入结算对齐模拟账本。</p>
    </section>
    <section id="rankingPanel" class="view">
      <div class="toolbar"><h2>市场窗口排名</h2><select id="rankingAssetFilter"><option value="">全部资产</option></select><select id="rankingDurationFilter"><option value="">全部周期</option></select><select id="rankingClassFilter"><option value="">全部分类</option><option value="exact">严格匹配</option><option value="basis">基差候选</option><option value="incompatible">不兼容</option></select></div>
      <p class="section-note">每行是近 6 小时内有采样的具体市场窗口。按正信号的单份净空间均值排序；正信号率 = 正净空间方向数 / 有效报价方向数。数值是扣除动态费用和风险缓冲后的瞬时报价信号，不是已成交或已实现利润；basis 市场还存在结算基准差异。</p>
      <div class="wide-panel"><table><thead><tr><th>市场窗口</th><th>分类</th><th>采样批次</th><th>有效方向</th><th>正信号方向</th><th>正信号率</th><th>正信号平均净空间<br>名义美元/份</th><th>最大净空间<br>名义美元/份</th><th>深度内最大理论利润</th><th>最长连续信号</th><th>平均抓取耗时</th><th>最大盘口时间差</th></tr></thead><tbody id="rankingRows"></tbody></table></div>
    </section>
    <section id="tradePanel" class="view">
      <div class="toolbar"><h2>模拟账本</h2><select id="tradeAssetFilter"><option value="">全部资产</option></select><select id="tradeDurationFilter"><option value="">全部周期</option></select><select id="tradeClassFilter"><option value="">全部分类</option><option value="exact">严格匹配</option><option value="basis">基差候选</option></select></div>
      <p class="section-note">模拟账本按模型版本独立记录。当前模型要求结算规则对齐、净空间至少 2%、连续确认，并以最多 10 份完整卖盘深度检查两腿 FOK 可成交性。二次重报价与单腿成交风险由只读 shadow-fok 模拟单独验证。</p>
      <div class="wide-panel"><table><thead><tr><th>时间</th><th>市场</th><th>方向</th><th>分类</th><th>数量</th><th>总成本</th><th>手续费</th><th>风险缓冲</th><th>预期利润</th><th>实际兑付</th><th>结算利润</th><th>状态/模型</th></tr></thead><tbody id="tradeRows"></tbody></table></div>
      <div class="toolbar"><h2>自动影子执行</h2></div>
      <p class="section-note">规则对齐的跨平台 1h 信号或 Polymarket 同市场正净信号首次通过静态风控后，系统保留当时的两腿 FOK 限价，等待 250ms 再拉取真实盘口并模拟同时提交。记录只用于测量执行风险，不会发送真实订单；一腿已成交时无法靠撤单恢复。</p>
      <div class="wide-panel"><table><thead><tr><th>时间</th><th>市场</th><th>方向</th><th>份数</th><th>第一腿限价</th><th>第一腿结果</th><th>第二腿限价</th><th>第二腿结果</th><th>总延迟</th><th>未对冲</th><th>总状态/来源</th><th>原因</th></tr></thead><tbody id="shadowRows"></tbody></table></div>
    </section>
    <section id="settlementPanel" class="view">
      <div class="toolbar"><h2>历史结算结果</h2><select id="settlementAssetFilter"><option value="">全部资产</option></select><select id="settlementDurationFilter"><option value="">全部周期</option></select><select id="relationshipFilter"><option value="">全部关系</option><option value="matching">同向</option><option value="divergent">分歧</option><option value="pending">待结算</option></select></div>
      <p id="settlementSummary" class="section-note">等待首批市场结算。</p>
      <div class="wide-panel"><table><thead><tr><th>市场窗口</th><th>结束时间</th><th>Binance 结果</th><th>Binance 起始/结束价</th><th>Polymarket 结果</th><th>PM 起始/结束价</th><th>关系</th><th>最后核对</th></tr></thead><tbody id="settlementRows"></tbody></table></div>
    </section>
  </main>
  <script>
    const $=id=>document.getElementById(id);let state={pairs:[],selected:null,history:[],historySlug:null,historyLoading:false,slowLoading:false,basisLoading:false,lastHistoryAt:0},rankings=[],binanceInternalStats=[],polymarketInternalStats=[],basisLatest=[],deliveryBasisLatest=[],deliveryRequotes=[],trades=[],shadowExecutions=[],settlements=[],pmStream=null,pmStreamAsset=null,pmLive=null;
    const val=(v,fallback='—')=>v===null||v===undefined||v===''?fallback:v;
    const positive=v=>v!==null&&v!==undefined&&Number(v)>0;
    const fmt=v=>v===null||v===undefined?'—':Number(v).toFixed(5);
    const priceFmt=v=>v===null||v===undefined?'—':Number(v).toFixed(4);
    const pct=v=>v===null||v===undefined?'—':(Number(v)*100).toFixed(4)+'%';
    const coinFmt=v=>v===null||v===undefined?'—':Number(v).toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:4});
    const duration=ms=>ms===null||ms===undefined?'—':ms<1000?ms+' ms':(ms/1000).toFixed(1)+' s';
    const time=ms=>new Date(ms).toLocaleTimeString('zh-CN',{hour12:false});
    const durationOrder=v=>v.endsWith('m')?Number(v.slice(0,-1)):v.endsWith('h')?Number(v.slice(0,-1))*60:Number(v.slice(0,-1))*1440;
    function optionSet(id,values,label){const el=$(id),current=el.value;while(el.options.length>1)el.remove(1);[...new Set(values)].sort().forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v+label;el.appendChild(o)});el.value=current}
    function replaceOptions(el,values,preferred){el.replaceChildren();values.forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;el.appendChild(o)});el.value=values.includes(preferred)?preferred:(values[0]||'')}
    function eligiblePairs(){return state.pairs.filter(p=>p.classification==='exact'||p.classification==='basis')}
    function syncLiveSelectors(){const pairs=eligiblePairs(),assets=[...new Set(pairs.map(p=>p.asset))].sort(),savedAsset=localStorage.getItem('liveAsset')||'BTC';replaceOptions($('liveAsset'),assets,$('liveAsset').value||savedAsset);const durations=[...new Set(pairs.filter(p=>p.asset===$('liveAsset').value).map(p=>p.duration))].sort((a,b)=>durationOrder(a)-durationOrder(b)),savedDuration=localStorage.getItem('liveDuration')||'5m';replaceOptions($('liveDuration'),durations,$('liveDuration').value||savedDuration)}
    function focusedPair(){const now=Date.now(),matches=eligiblePairs().filter(p=>p.asset===$('liveAsset').value&&p.duration===$('liveDuration').value&&p.end_ms>now);return matches.find(p=>p.start_ms<=now)||matches.sort((a,b)=>a.start_ms-b.start_ms)[0]}
    function setValue(id,value,formatter=fmt){$(id).textContent=formatter(value)}
    function countdown(ms){if(ms<=0)return'切换中';const seconds=Math.floor(ms/1000),h=Math.floor(seconds/3600),m=Math.floor(seconds%3600/60),s=seconds%60;return h?`${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`:`${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`}
    function renderLive(){
      syncLiveSelectors();const p=focusedPair();$('liveEmpty').hidden=Boolean(p);$('liveContent').hidden=!p;if(!p)return;connectPmStream(p.asset);
      const q=p.latest||{},backendReference=state.reference_prices?.[p.asset]||{},reference=pmLive?.asset===p.asset?{...backendReference,polymarket_twap_60s:pmLive.value,polymarket_observed_at_ms:pmLive.timestamp,price_difference:backendReference.binance_spot_price?(Number(backendReference.binance_spot_price)-Number(pmLive.value)).toString():backendReference.price_difference}:backendReference;
      $('liveTitle').textContent=`${p.asset} · ${p.duration}`;$('liveSlug').textContent=p.slug;$('liveClass').textContent=p.classification;$('liveClass').className='badge '+p.classification;$('liveCountdown').textContent=countdown(p.end_ms-Date.now());$('binanceCoinLabel').textContent=`${p.asset}/USDT 当前币价`;$('pmCoinLabel').textContent=`${p.asset}/USD 当前币价`;
      setValue('binanceCoinPrice',reference.binance_spot_price,coinFmt);setValue('pmCoinPrice',reference.polymarket_twap_60s,coinFmt);$('binanceCoinMeta').textContent='Binance Spot · '+(reference.binance_observed_at_ms?duration(Date.now()-reference.binance_observed_at_ms)+' 前':'等待数据');$('pmCoinMeta').textContent='Chainlink 60s TWAP · '+(reference.polymarket_observed_at_ms?duration(Date.now()-reference.polymarket_observed_at_ms)+' 前':'等待数据');$('coinPriceDifference').textContent=reference.price_difference===null||reference.price_difference===undefined?'—':coinFmt(reference.price_difference)+' USDT-USD';
      setValue('binanceUp',q.binance_up_ask,priceFmt);setValue('binanceDown',q.binance_down_ask,priceFmt);setValue('pmUp',q.polymarket_up_ask,priceFmt);setValue('pmDown',q.polymarket_down_ask,priceFmt);$('binanceUpSize').textContent='方向 A 可成交 '+val(q.direction_a_size);$('pmDownSize').textContent='方向 A 可成交 '+val(q.direction_a_size);$('binanceDownSize').textContent='方向 B 可成交 '+val(q.direction_b_size);$('pmUpSize').textContent='方向 B 可成交 '+val(q.direction_b_size);
      renderDirection('a',q.direction_a_cost,q.direction_a_fees,q.direction_a_risk_buffers,q.direction_a_net_edge,q.direction_a_size,q.direction_a_rejections);renderDirection('b',q.direction_b_cost,q.direction_b_fees,q.direction_b_risk_buffers,q.direction_b_net_edge,q.direction_b_size,q.direction_b_rejections);
      renderInternal('bi','binanceInternalLive',q.binance_internal);renderInternal('pi','polymarketInternalLive',q.polymarket_internal);
      $('liveLatency').textContent=duration(q.scan_latency_ms);$('liveASkew').textContent=duration(q.direction_a_quote_skew_ms);$('liveBSkew').textContent=duration(q.direction_b_quote_skew_ms);$('liveUpdated').textContent=q.observed_at_ms?time(q.observed_at_ms):'—';if(state.historySlug!==p.slug){state.historySlug=p.slug;state.history=[];state.lastHistoryAt=0}refreshHistory(p.slug)
    }
    function renderInternal(prefix,panelId,value){const x=value||{};setValue(prefix+'Cost',x.cost);setValue(prefix+'Fees',x.fees);setValue(prefix+'Buffers',x.risk_buffers);setValue(prefix+'Net',x.net_edge);setValue(prefix+'Size',x.size);$(prefix+'Skew').textContent=duration(x.quote_skew_ms);$(prefix+'Rejections').textContent=x.rejections?.length?x.rejections.join(' · '):value?'静态条件通过，等待连续确认':'等待行情';$(panelId).classList.toggle('profitable',positive(x.net_edge)&&!x.rejections?.length)}
    function connectPmStream(asset){if(pmStreamAsset===asset&&pmStream&&pmStream.readyState<=1)return;if(pmStream){try{pmStream.close()}catch(e){}}pmStreamAsset=asset;pmLive=null;try{pmStream=new WebSocket('wss://ws-live-data.polymarket.com');pmStream.onopen=()=>pmStream.send(JSON.stringify({action:'subscribe',subscriptions:[{topic:'crypto_prices_twap_sixty',type:'update',filters:JSON.stringify({symbol:asset.toLowerCase()+'/usd'})}]}));pmStream.onmessage=event=>{try{const data=JSON.parse(event.data),payload=data.payload||{};if(data.topic!=='crypto_prices_twap_sixty'||payload.symbol!==asset.toLowerCase()+'/usd')return;const value=payload.full_accuracy_value?Number(payload.full_accuracy_value)/1e18:Number(payload.value);if(!Number.isFinite(value))return;pmLive={asset,value,timestamp:Number(payload.timestamp)||Date.now()};if(focusedPair()?.asset===asset)renderLive()}catch(e){}};pmStream.onclose=()=>{if(pmStreamAsset===asset)setTimeout(()=>connectPmStream(asset),3000)}}catch(e){pmStream=null}}
    function renderDirection(prefix,cost,fees,buffers,net,size,rejections){setValue(prefix+'Cost',cost);setValue(prefix+'Fees',fees);setValue(prefix+'Buffers',buffers);setValue(prefix+'Net',net);setValue(prefix+'Size',size,v=>v===null||v===undefined?'—':Number(v).toFixed(4));const profit=net!==null&&net!==undefined&&size!==null&&size!==undefined?Number(net)*Number(size):null;setValue(prefix+'Profit',profit);$(prefix+'Rejections').textContent=rejections?.length?rejections.join(' · '):'静态风控通过，等待连续确认';$(prefix==='a'?'directionA':'directionB').classList.toggle('profitable',positive(net))}
    async function refreshHistory(slug){if(state.historyLoading||Date.now()-state.lastHistoryAt<1800)return;state.historyLoading=true;state.lastHistoryAt=Date.now();try{const response=await fetch('/api/history/'+encodeURIComponent(slug)+'?limit=240',{cache:'no-store'}),points=await response.json();if(state.historySlug===slug){state.history=points;drawLiveCharts()}}finally{state.historyLoading=false}}
    function drawLiveCharts(){drawSeriesChart($('referenceChart'),state.history,[['binance_reference_price','#087f5b'],['polymarket_reference_price','#2457a7']],false);drawSeriesChart($('priceChart'),state.history,[['binance_up_ask','#087f5b'],['binance_down_ask','#e09f1f'],['polymarket_up_ask','#2457a7'],['polymarket_down_ask','#a054a0']],false);drawSeriesChart($('edgeChart'),state.history,[['direction_a_net_edge','#087f5b'],['direction_b_net_edge','#2457a7']],true)}
    function drawSeriesChart(canvas,points,series,includeZero){const c=canvas.getContext('2d'),w=canvas.width,h=canvas.height,padL=62,padR=12,padY=18;c.clearRect(0,0,w,h);const values=points.flatMap(p=>series.map(([key])=>p[key])).filter(v=>v!==null&&v!==undefined).map(Number);if(!values.length){c.fillStyle='#687078';c.font='12px system-ui';c.fillText('等待价格数据',padL,35);return}if(includeZero)values.push(0);let min=Math.min(...values),max=Math.max(...values),range=max-min;if(range<.02){min-=.01;max+=.01}else{min-=range*.08;max+=range*.08}if(!includeZero&&max<=1){min=Math.max(0,min);max=Math.min(1,max)}const y=v=>padY+(max-v)/(max-min)*(h-padY*2),x=i=>padL+i/Math.max(points.length-1,1)*(w-padL-padR);c.font='11px system-ui';c.textAlign='right';for(let i=0;i<5;i++){const value=max-(max-min)*i/4,yy=y(value),label=max>100?value.toLocaleString('zh-CN',{maximumFractionDigits:2}):value.toFixed(3);c.strokeStyle='#e2e5e8';c.lineWidth=1;c.beginPath();c.moveTo(padL,yy);c.lineTo(w-padR,yy);c.stroke();c.fillStyle='#687078';c.fillText(label,padL-7,yy+4)}if(includeZero&&min<=0&&max>=0){c.strokeStyle='#b42318';c.setLineDash([5,4]);c.beginPath();c.moveTo(padL,y(0));c.lineTo(w-padR,y(0));c.stroke();c.setLineDash([])}series.forEach(([key,color])=>{c.strokeStyle=color;c.lineWidth=2;c.beginPath();let started=false;points.forEach((p,i)=>{if(p[key]===null||p[key]===undefined){started=false;return}const xx=x(i),yy=y(Number(p[key]));started?c.lineTo(xx,yy):c.moveTo(xx,yy);started=true});c.stroke()})}
    function renderRadar(){optionSet('assetFilter',state.pairs.map(p=>p.asset),'');optionSet('durationFilter',state.pairs.map(p=>p.duration),'');const filters=[$('assetFilter').value,$('durationFilter').value,$('classFilter').value],rows=$('rows');rows.replaceChildren();state.pairs.filter(p=>(!filters[0]||p.asset===filters[0])&&(!filters[1]||p.duration===filters[1])&&(!filters[2]||p.classification===filters[2])).forEach(p=>{const tr=document.createElement('tr');if(p.slug===state.selected)tr.className='selected';const cells=[`${p.asset}|${p.duration}`,p.classification,fmt(p.latest?.direction_a_net_edge),val(p.latest?.direction_a_size),fmt(p.latest?.direction_b_net_edge),val(p.latest?.direction_b_size),time(p.end_ms),p.error?'异常':'监听中'];cells.forEach((text,i)=>{const td=document.createElement('td');if(i===0){const a=text.split('|'),strong=document.createElement('span'),sub=document.createElement('span');strong.className='asset';strong.textContent=a[0];sub.className='sub';sub.textContent=a[1]+' · '+p.slug;td.append(strong,sub)}else if(i===1){td.textContent=text;td.className='badge '+text}else{td.textContent=text;if((i===2||i===4)&&positive(i===2?p.latest?.direction_a_net_edge:p.latest?.direction_b_net_edge))td.className='positive';if(i===7&&p.error){td.textContent=p.error;td.className='error'}}tr.appendChild(td)});tr.onclick=()=>{state.selected=p.slug;renderRadar();renderDetail(p)};rows.appendChild(tr)})}
    async function renderDetail(p){const d=$('detail');d.className='detail';d.replaceChildren();const section=(title,items)=>{const h=document.createElement('h3'),kv=document.createElement('div');h.textContent=title;kv.className='kv';items.forEach(([k,v,c])=>{const a=document.createElement('span'),b=document.createElement('span');a.textContent=k;b.textContent=val(v);if(c)b.className=c;kv.append(a,b)});d.append(h,kv)};section('市场',[['标题',p.title],['分类',p.classification],['Binance 来源',p.binance_source],['Polymarket 来源',p.polymarket_source],['Binance 费率',p.binance_fee_bps+' bps'],['Polymarket taker rate',p.polymarket_fee_rate],['流动性',p.liquidity+' USDT'],['成交量',p.trade_volume+' USDT']]);section('方向 A：Binance Up + PM Down',[['深度均价成本',fmt(p.latest?.direction_a_cost)],['估算费用',fmt(p.latest?.direction_a_fees)],['风险缓冲',fmt(p.latest?.direction_a_risk_buffers)],['净空间/份',fmt(p.latest?.direction_a_net_edge),positive(p.latest?.direction_a_net_edge)?'positive':''],['可成交数量',p.latest?.direction_a_size],['拒绝原因',p.latest?.direction_a_rejections?.join(' · ')||'静态风控通过']]);section('方向 B：Binance Down + PM Up',[['深度均价成本',fmt(p.latest?.direction_b_cost)],['估算费用',fmt(p.latest?.direction_b_fees)],['风险缓冲',fmt(p.latest?.direction_b_risk_buffers)],['净空间/份',fmt(p.latest?.direction_b_net_edge),positive(p.latest?.direction_b_net_edge)?'positive':''],['可成交数量',p.latest?.direction_b_size],['拒绝原因',p.latest?.direction_b_rejections?.join(' · ')||'静态风控通过']]);section('数据质量',[['整轮抓取耗时',duration(p.latest?.scan_latency_ms)],['方向 A 盘口时间差',duration(p.latest?.direction_a_quote_skew_ms)],['方向 A 最大盘口年龄',duration(p.latest?.direction_a_quote_age_ms)],['方向 B 盘口时间差',duration(p.latest?.direction_b_quote_skew_ms)],['方向 B 最大盘口年龄',duration(p.latest?.direction_b_quote_age_ms)],['距结算',duration(p.latest?.remaining_time_ms)]]);const h=document.createElement('h3');h.textContent='规则差异';d.appendChild(h);p.differences.forEach(text=>{const x=document.createElement('div');x.className='rule';x.textContent=text;d.appendChild(x)});if(p.error){const x=document.createElement('p');x.className='error';x.textContent=p.error;d.appendChild(x)}}
    function renderRankings(){optionSet('rankingAssetFilter',rankings.map(r=>r.asset),'');optionSet('rankingDurationFilter',rankings.map(r=>r.duration),'');const filters=[$('rankingAssetFilter').value,$('rankingDurationFilter').value,$('rankingClassFilter').value],body=$('rankingRows');body.replaceChildren();rankings.filter(r=>(!filters[0]||r.asset===filters[0])&&(!filters[1]||r.duration===filters[1])&&(!filters[2]||r.classification===filters[2])).forEach(r=>{const tr=document.createElement('tr');[[r.asset+' '+r.duration,r.slug],[r.classification],[r.samples],[r.evaluated_directions],[r.signals],[(Number(r.signal_rate)*100).toFixed(2)+'%'],[fmt(r.average_positive_edge)],[fmt(r.maximum_net_edge)],[fmt(r.maximum_executable_profit)],[duration(r.longest_positive_run_ms)],[duration(r.average_scan_latency_ms)],[duration(r.maximum_quote_skew_ms)]].forEach((parts,i)=>{const td=document.createElement('td');td.textContent=parts[0];if(i===0){const sub=document.createElement('span');sub.className='sub';sub.textContent=parts[1];td.appendChild(sub)}if(i===1)td.className='badge '+r.classification;if(i>=6&&i<=8&&Number(parts[0])>0)td.className='positive';tr.appendChild(td)});body.appendChild(tr)})}
    function renderInternalStats(bodyId,values){const body=$(bodyId);body.replaceChildren();values.forEach(x=>{const tr=document.createElement('tr'),data=[x.asset+' '+x.duration,x.samples,x.windows,x.raw_below_one_samples,x.positive_net_samples,x.eligible_samples,x.confirmed_samples,fmt(x.average_positive_edge),fmt(x.maximum_net_edge),fmt(x.maximum_theoretical_profit),duration(x.maximum_quote_skew_ms),time(x.latest_observed_at_ms)];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if(i>=6&&i<=9&&Number(v)>0)td.className='positive';tr.appendChild(td)});body.appendChild(tr)})}
    function renderBinanceInternalStats(){renderInternalStats('binanceInternalRows',binanceInternalStats)}
    function renderPolymarketInternalStats(){renderInternalStats('polymarketInternalRows',polymarketInternalStats)}
    function renderBasis(){const body=$('basisRows');body.replaceChildren();basisLatest.forEach(x=>{const tr=document.createElement('tr'),ok=!x.rejections?.length,data=[x.symbol,Number(x.quantity).toFixed(8),coinFmt(x.spot_vwap_ask),coinFmt(x.perp_vwap_bid),pct(x.entry_basis_rate),pct(x.funding_rate)+' / '+x.funding_interval_hours+'h',pct(x.funding_apr),fmt(x.funding_income_per_round),fmt(x.projected_net_24h),fmt(x.projected_net_7d),x.break_even_rounds??'无法回本',duration(x.response_skew_ms),time(x.next_funding_time_ms),ok?'24h 模型通过':x.rejections.join(' · '),time(x.observed_at_ms)];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if((i===8||i===9)&&Number(i===8?x.projected_net_24h:x.projected_net_7d)>0)td.className='positive';if(i===13)td.className=ok?'positive':'error';tr.appendChild(td)});body.appendChild(tr)})}
    function renderDeliveryBasis(){const body=$('deliveryBasisRows');body.replaceChildren();deliveryBasisLatest.forEach(x=>{const tr=document.createElement('tr'),ok=!x.rejections?.length,friction=Number(x.estimated_round_trip_fees)+Number(x.slippage_buffer),data=[x.symbol,x.contract_type,Number(x.remaining_days).toFixed(2),Number(x.quantity).toFixed(8)+' / '+x.quantity_step,coinFmt(x.spot_vwap_ask),coinFmt(x.delivery_vwap_bid),fmt(x.gross_basis),fmt(friction),fmt(x.settlement_buffer),fmt(x.capital_cost),fmt(x.pre_capital_net_profit),fmt(x.net_profit_at_delivery),pct(x.net_return),pct(x.net_apr),duration(x.response_skew_ms),time(x.delivery_time_ms),ok?'最终净利润为正':x.rejections.join(' · '),time(x.observed_at_ms)];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if((i===10||i===11||i===12||i===13)&&Number([x.pre_capital_net_profit,x.net_profit_at_delivery,x.net_return,x.net_apr][i-10])>0)td.className='positive';if(i===16)td.className=ok?'positive':'error';tr.appendChild(td)});body.appendChild(tr)})}
    function renderDeliveryRequotes(){const body=$('deliveryRequoteRows');body.replaceChildren();deliveryRequotes.forEach(x=>{const both=x.spot_fillable&&x.delivery_fillable,tr=document.createElement('tr'),data=[x.symbol,time(x.quoted_at_ms),duration(x.delay_ms),Number(x.quantity).toFixed(8),coinFmt(x.spot_limit_price),coinFmt(x.delivery_limit_price),x.spot_fillable?'是':'否',x.delivery_fillable?'是':'否',fmt(x.gross_basis),x.error||x.status,time(x.checked_at_ms)];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if(i===8&&Number(x.gross_basis)>0)td.className='positive';if(i===9)td.className=both&&!x.error?'positive':'error';tr.appendChild(td)});body.appendChild(tr)})}
    function renderTrades(){optionSet('tradeAssetFilter',trades.map(t=>t.asset),'');optionSet('tradeDurationFilter',trades.map(t=>t.duration),'');const filters=[$('tradeAssetFilter').value,$('tradeDurationFilter').value,$('tradeClassFilter').value],body=$('tradeRows');body.replaceChildren();trades.filter(t=>(!filters[0]||t.asset===filters[0])&&(!filters[1]||t.duration===filters[1])&&(!filters[2]||t.classification===filters[2])).forEach(t=>{const tr=document.createElement('tr'),data=[time(t.detected_at_ms),t.asset+' '+t.duration,t.direction,t.classification,t.quantity,t.total_cost,t.total_fees,t.total_risk_buffers,t.expected_profit,val(t.actual_payout),val(t.realized_profit),t.status+' · '+t.fill_model];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if(i===3)td.className='badge '+t.classification;if(i===8||i===10&&Number(v)>0)td.className='positive';if(i===10&&Number(v)<0)td.className='error';if(i===11)td.className='model';tr.appendChild(td)});body.appendChild(tr)})}
    function renderShadowExecutions(){const body=$('shadowRows');body.replaceChildren();shadowExecutions.forEach(x=>{const tr=document.createElement('tr'),first=`${x.first_status} · ${x.first_filled_quantity} @ ${val(x.first_average_price)}`,second=`${x.second_status} · ${x.second_filled_quantity} @ ${val(x.second_average_price)}`,data=[time(x.executed_at_ms),x.asset+' · '+x.slug,x.direction,x.requested_quantity,x.first_limit_price,first,x.second_limit_price,second,duration(x.executed_at_ms-x.quoted_at_ms),x.unhedged_quantity,x.status+' · '+x.trigger_source,val(x.error,'—')];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if((i===9&&Number(v)>0)||i===11&&x.error)td.className='error';if(i===10&&x.status==='hedged')td.className='positive';if(i===10)td.classList.add('model');tr.appendChild(td)});body.appendChild(tr)})}
    function renderSettlements(){optionSet('settlementAssetFilter',settlements.map(s=>s.asset),'');optionSet('settlementDurationFilter',settlements.map(s=>s.duration),'');const filters=[$('settlementAssetFilter').value,$('settlementDurationFilter').value,$('relationshipFilter').value],values=settlements.filter(s=>(!filters[0]||s.asset===filters[0])&&(!filters[1]||s.duration===filters[1])&&(!filters[2]||s.relationship===filters[2])),body=$('settlementRows');body.replaceChildren();const resolved=values.filter(s=>s.relationship!=='pending'),different=resolved.filter(s=>s.relationship==='divergent').length;$('settlementSummary').textContent=resolved.length?`已完成 ${resolved.length} 个窗口，结算分歧 ${different} 个，分歧率 ${(different/resolved.length*100).toFixed(2)}%。`:`共 ${values.length} 个待核对窗口，尚无双方均完成结算的样本。`;values.forEach(s=>{const tr=document.createElement('tr'),market=document.createElement('td');market.textContent=s.asset+' '+s.duration;const slug=document.createElement('span');slug.className='sub';slug.textContent=s.slug;market.appendChild(slug);tr.appendChild(market);const data=[time(s.end_ms),val(s.binance_outcome)+' · '+s.binance_status,val(s.binance_start_price)+' / '+val(s.binance_end_price),val(s.polymarket_outcome)+' · '+s.polymarket_status,val(s.polymarket_price_to_beat)+' / '+val(s.polymarket_final_price),s.relationship,time(s.checked_at_ms)];data.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v;if(i===5)td.className='badge '+s.relationship;tr.appendChild(td)});body.appendChild(tr)})}
    function renderHeader(){$('pairCount').textContent=state.pairs.length;$('observationCount').textContent=state.observation_count||0;$('opportunityCount').textContent=state.opportunity_count||0;$('signalCount').textContent=state.pairs.filter(p=>p.latest&&(positive(p.latest.direction_a_net_edge)||positive(p.latest.direction_b_net_edge))).length;$('statusText').textContent=state.scanner_status==='running'?'数据源正常':'数据源异常';$('statusDot').className='dot '+(state.scanner_status==='running'?'ok':'bad')}
    async function loadState(){try{const response=await fetch('/api/state',{cache:'no-store'});state={...state,...await response.json()};renderHeader();renderLive();if(document.querySelector('.tab.active')?.dataset.view==='radarPanel')renderRadar()}catch(e){$('statusText').textContent='连接失败';$('statusDot').className='dot bad'}}
    async function loadBasis(){if(state.basisLoading)return;state.basisLoading=true;try{const [basisResponse,deliveryResponse,requoteResponse]=await Promise.all([fetch('/api/binance-basis',{cache:'no-store'}),fetch('/api/binance-delivery-basis',{cache:'no-store'}),fetch('/api/binance-delivery-requotes?limit=100',{cache:'no-store'})]);basisLatest=await basisResponse.json();deliveryBasisLatest=await deliveryResponse.json();deliveryRequotes=await requoteResponse.json();renderBasis();renderDeliveryBasis();renderDeliveryRequotes()}finally{state.basisLoading=false}}
    async function loadSlow(){if(state.slowLoading)return;state.slowLoading=true;try{$('loading').style.width='65%';const [r,b,p,t,x,c]=await Promise.all([fetch('/api/rankings',{cache:'no-store'}),fetch('/api/binance-internal-stats',{cache:'no-store'}),fetch('/api/polymarket-internal-stats',{cache:'no-store'}),fetch('/api/paper-trades?limit=200',{cache:'no-store'}),fetch('/api/shadow-executions?limit=200',{cache:'no-store'}),fetch('/api/settlements?limit=500',{cache:'no-store'})]);rankings=await r.json();binanceInternalStats=await b.json();polymarketInternalStats=await p.json();trades=await t.json();shadowExecutions=await x.json();settlements=await c.json();renderRankings();renderBinanceInternalStats();renderPolymarketInternalStats();renderTrades();renderShadowExecutions();renderSettlements()}finally{state.slowLoading=false;$('loading').style.width='0'}}
    $('liveAsset').onchange=()=>{localStorage.setItem('liveAsset',$('liveAsset').value);state.historySlug=null;syncLiveSelectors();localStorage.setItem('liveDuration',$('liveDuration').value);renderLive()};$('liveDuration').onchange=()=>{localStorage.setItem('liveDuration',$('liveDuration').value);state.historySlug=null;renderLive()};['assetFilter','durationFilter','classFilter'].forEach(id=>$(id).onchange=renderRadar);['rankingAssetFilter','rankingDurationFilter','rankingClassFilter'].forEach(id=>$(id).onchange=renderRankings);['tradeAssetFilter','tradeDurationFilter','tradeClassFilter'].forEach(id=>$(id).onchange=renderTrades);['settlementAssetFilter','settlementDurationFilter','relationshipFilter'].forEach(id=>$(id).onchange=renderSettlements);$('refresh').onclick=()=>{loadState();loadSlow();loadBasis()};document.querySelectorAll('.tab').forEach(tab=>tab.onclick=()=>{document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));tab.classList.add('active');$(tab.dataset.view).classList.add('active');if(tab.dataset.view==='radarPanel')renderRadar();if(tab.dataset.view==='basisPanel')loadBasis()});loadState();loadSlow();loadBasis();setInterval(loadState,1000);setInterval(loadBasis,3000);setInterval(loadSlow,15000);
  </script>
</body></html>"#;
