use chrono::{DateTime, Utc};
use domain::{PairClassification, PriceLevel, ResolutionSpec};
use exchange_binance::{BinanceClient, BinanceMarketDetail, BinanceMarketTopic};
use exchange_polymarket::PolymarketClient;
use execution::{
    preflight_pair_fok, quantity_for_budget_at_step, simulate_fok, FokOrderRequest, PairFokReport,
    SimulatedExecutor,
};
use futures_util::{SinkExt, StreamExt};
use matcher::classify_candidate_pair;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{client_async_tls, connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::storage::{PendingSettlement, Storage};

#[derive(Debug, Clone, Default, Serialize)]
pub struct DashboardState {
    pub mode: String,
    pub scanner_status: String,
    pub last_discovery_ms: Option<i64>,
    pub last_error: Option<String>,
    pub pairs: Vec<PairSnapshot>,
    pub observation_count: u64,
    pub opportunity_count: u64,
    pub reference_prices: HashMap<String, ReferencePriceView>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReferencePriceView {
    pub binance_spot_price: Option<String>,
    pub binance_observed_at_ms: Option<i64>,
    pub polymarket_twap_60s: Option<String>,
    pub polymarket_observed_at_ms: Option<i64>,
    pub price_difference: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairSnapshot {
    #[serde(flatten)]
    pub pair: TrackedPairView,
    pub latest: Option<ObservationView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackedPairView {
    pub slug: String,
    pub binance_topic_id: i64,
    pub title: String,
    pub asset: String,
    pub duration: String,
    pub classification: String,
    pub differences: Vec<String>,
    pub binance_source: String,
    pub polymarket_source: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub binance_fee_bps: i64,
    pub polymarket_fee_rate: String,
    pub liquidity: String,
    pub trade_volume: String,
}

#[derive(Debug, Clone)]
pub struct SettlementUpdate {
    pub slug: String,
    pub binance_status: String,
    pub binance_start_price: Option<String>,
    pub binance_end_price: Option<String>,
    pub binance_outcome: Option<String>,
    pub polymarket_status: String,
    pub polymarket_price_to_beat: Option<String>,
    pub polymarket_final_price: Option<String>,
    pub polymarket_outcome: Option<String>,
    pub relationship: String,
    pub checked_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObservationView {
    pub slug: String,
    pub observed_at_ms: i64,
    pub binance_up_ask: Option<String>,
    pub binance_down_ask: Option<String>,
    pub polymarket_up_ask: Option<String>,
    pub polymarket_down_ask: Option<String>,
    pub direction_a_cost: Option<String>,
    pub direction_a_fees: Option<String>,
    pub direction_a_net_edge: Option<String>,
    pub direction_a_risk_buffers: Option<String>,
    pub direction_a_size: Option<String>,
    pub direction_b_cost: Option<String>,
    pub direction_b_fees: Option<String>,
    pub direction_b_net_edge: Option<String>,
    pub direction_b_risk_buffers: Option<String>,
    pub direction_b_size: Option<String>,
    pub direction_a_rejections: Vec<String>,
    pub direction_b_rejections: Vec<String>,
    pub scan_latency_ms: Option<i64>,
    pub direction_a_quote_skew_ms: Option<i64>,
    pub direction_b_quote_skew_ms: Option<i64>,
    pub direction_a_quote_age_ms: Option<i64>,
    pub direction_b_quote_age_ms: Option<i64>,
    pub remaining_time_ms: Option<i64>,
    pub binance_reference_price: Option<String>,
    pub binance_reference_at_ms: Option<i64>,
    pub polymarket_reference_price: Option<String>,
    pub polymarket_reference_at_ms: Option<i64>,
    pub binance_internal: Option<BinanceInternalView>,
    pub polymarket_internal: Option<PolymarketInternalView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceInternalView {
    pub cost: String,
    pub fees: String,
    pub risk_buffers: String,
    pub net_edge: String,
    pub size: String,
    pub quote_skew_ms: i64,
    pub quote_age_ms: i64,
    pub rejections: Vec<String>,
}

pub type PolymarketInternalView = BinanceInternalView;

#[derive(Debug, Clone, Serialize)]
pub struct PaperTradeView {
    pub id: Option<i64>,
    pub slug: String,
    pub asset: String,
    pub duration: String,
    pub direction: String,
    pub classification: String,
    pub detected_at_ms: i64,
    pub quantity: String,
    pub total_cost: String,
    pub total_fees: String,
    pub total_risk_buffers: String,
    pub expected_profit: String,
    pub actual_payout: Option<String>,
    pub realized_profit: Option<String>,
    pub settled_at_ms: Option<i64>,
    pub status: String,
    pub fill_model: String,
}

#[derive(Clone)]
struct TrackedPair {
    view: TrackedPairView,
    binance: BinanceMarketDetail,
    polymarket_up_token: String,
    polymarket_down_token: String,
    polymarket_fee_rate: Decimal,
}

pub struct LiveFokSimulation {
    pub slug: String,
    pub direction: String,
    pub quoted_at_ms: i64,
    pub executed_at_ms: i64,
    pub first_request: FokOrderRequest,
    pub second_request: FokOrderRequest,
    pub report: PairFokReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowExecutionView {
    pub id: Option<i64>,
    pub slug: String,
    pub asset: String,
    pub direction: String,
    pub quoted_at_ms: i64,
    pub executed_at_ms: i64,
    pub requested_quantity: String,
    pub first_limit_price: String,
    pub second_limit_price: String,
    pub first_status: String,
    pub first_filled_quantity: String,
    pub first_average_price: Option<String>,
    pub first_total_cost: String,
    pub second_status: String,
    pub second_filled_quantity: String,
    pub second_average_price: Option<String>,
    pub second_total_cost: String,
    pub unhedged_quantity: String,
    pub status: String,
    pub trigger_source: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct ShadowPlan {
    first_request: FokOrderRequest,
    second_request: FokOrderRequest,
}

struct ScanPairResult {
    observation: ObservationView,
    direction_a_plan: Option<ShadowPlan>,
    direction_b_plan: Option<ShadowPlan>,
    polymarket_internal_plan: Option<ShadowPlan>,
}

struct SameMarketCalculation {
    view: PolymarketInternalView,
    plan: ShadowPlan,
}

#[derive(Debug, Clone, Copy)]
struct ActiveSignal {
    since_ms: i64,
    last_seen_ms: i64,
    samples: u32,
}

const PAPER_MAX_QUANTITY: Decimal = Decimal::TEN;
const PAPER_MIN_REMAINING_MS: i64 = 60_000;
const PAPER_MAX_QUOTE_SKEW_MS: i64 = 1_000;
const PAPER_MAX_QUOTE_AGE_MS: i64 = 5_000;
const PAPER_MAX_REFERENCE_AGE_MS: i64 = 10_000;
const PAPER_CONFIRMATION_MS: i64 = 5_000;
const PAPER_MAX_SIGNAL_GAP_MS: i64 = 15_000;
const PAPER_SECOND_LEG_SLIPPAGE_BPS: i64 = 20;
const PAPER_STABLECOIN_CONVERSION_BPS: i64 = 10;
const PAPER_CAPITAL_APR_BPS: i64 = 1_000;
const MILLISECONDS_PER_YEAR: i64 = 365 * 24 * 60 * 60 * 1_000;
const PAPER_FILL_MODEL: &str =
    "fok_depth_10_dynamic_fee_slip_20bps_fx_10bps_capital_10apr_5s_confirmed_aligned";

pub async fn run(
    binance: BinanceClient,
    polymarket: PolymarketClient,
    storage: Storage,
    state: Arc<RwLock<DashboardState>>,
) {
    tokio::spawn(run_binance_reference_prices(binance.clone(), state.clone()));
    tokio::spawn(run_polymarket_reference_prices(state.clone()));
    let mut tracked: HashMap<String, TrackedPair> = HashMap::new();
    let mut paper_executed: HashSet<(String, String)> = HashSet::new();
    let mut polymarket_internal_shadow_executed: HashSet<String> = HashSet::new();
    let mut active_signals: HashMap<(String, String), ActiveSignal> = HashMap::new();
    let mut next_discovery = Instant::now();
    let mut next_settlement_check = Instant::now();

    loop {
        if Instant::now() >= next_discovery {
            match discover(&binance, &polymarket).await {
                Ok(discovered) => {
                    let now = Utc::now().timestamp_millis();
                    for pair in &discovered {
                        if let Err(error) = storage.upsert_pair(&pair.view, now) {
                            warn!(%error, "保存市场配对失败");
                        }
                    }
                    tracked = discovered
                        .into_iter()
                        .map(|pair| (pair.view.slug.clone(), pair))
                        .collect();
                    let mut dashboard = state.write().await;
                    dashboard.scanner_status = "running".into();
                    dashboard.last_discovery_ms = Some(now);
                    dashboard.last_error = None;
                    dashboard.pairs = tracked
                        .values()
                        .map(|pair| PairSnapshot {
                            pair: pair.view.clone(),
                            latest: None,
                            error: None,
                        })
                        .collect();
                    dashboard.pairs.sort_by_key(|pair| pair.pair.end_ms);
                    info!(pairs = tracked.len(), "市场发现完成");
                }
                Err(error) => {
                    let mut dashboard = state.write().await;
                    dashboard.scanner_status = "degraded".into();
                    dashboard.last_error = Some(error);
                }
            }
            next_discovery = Instant::now() + Duration::from_secs(10);
        }

        if Instant::now() >= next_settlement_check {
            match storage.pending_settlements(Utc::now().timestamp_millis(), 30) {
                Ok(pending) => {
                    reconcile_settlements(&binance, &polymarket, &storage, pending).await;
                }
                Err(error) => warn!(%error, "读取待核对结算市场失败"),
            }
            next_settlement_check = Instant::now() + Duration::from_secs(15);
        }

        let now = Utc::now().timestamp_millis();
        let active: Vec<_> = tracked
            .values()
            .filter(|pair| pair.view.start_ms <= now && pair.view.end_ms > now)
            .cloned()
            .collect();
        let mut tasks = JoinSet::new();
        for pair in active {
            let binance = binance.clone();
            let polymarket = polymarket.clone();
            tasks.spawn(async move {
                let slug = pair.view.slug.clone();
                (slug, scan_pair(&binance, &polymarket, &pair).await)
            });
        }
        while let Some(result) = tasks.join_next().await {
            if let Ok((slug, result)) = result {
                let mut dashboard = state.write().await;
                if let Some(index) = dashboard
                    .pairs
                    .iter()
                    .position(|item| item.pair.slug == slug)
                {
                    match result {
                        Ok(mut scan) => {
                            let observation = &mut scan.observation;
                            if let Some(reference) = dashboard
                                .reference_prices
                                .get(&dashboard.pairs[index].pair.asset)
                                .cloned()
                            {
                                observation.binance_reference_price = reference.binance_spot_price;
                                observation.binance_reference_at_ms =
                                    reference.binance_observed_at_ms;
                                observation.polymarket_reference_price =
                                    reference.polymarket_twap_60s;
                                observation.polymarket_reference_at_ms =
                                    reference.polymarket_observed_at_ms;
                            }
                            observation.direction_a_rejections = paper_rejection_reasons(
                                &dashboard.pairs[index].pair,
                                &observation,
                                "A",
                            );
                            observation.direction_b_rejections = paper_rejection_reasons(
                                &dashboard.pairs[index].pair,
                                &observation,
                                "B",
                            );
                            if let Err(error) = storage.insert_observation(&observation) {
                                dashboard.pairs[index].error = Some(format!("SQLite：{error}"));
                            } else {
                                let eligible = is_research_eligible(
                                    &dashboard.pairs[index].pair.classification,
                                );
                                let has_opportunity = eligible
                                    && (positive(&observation.direction_a_net_edge)
                                        || positive(&observation.direction_b_net_edge));
                                dashboard.observation_count += 1;
                                if has_opportunity {
                                    dashboard.opportunity_count += 1;
                                }
                                for direction in ["A", "B"] {
                                    let key = (slug.clone(), direction.to_string());
                                    let candidate = paper_trade(
                                        &dashboard.pairs[index].pair,
                                        observation,
                                        direction,
                                    );
                                    if let Some(trade) = candidate {
                                        let was_active = active_signals.contains_key(&key);
                                        let confirmed = record_qualifying_sample(
                                            &mut active_signals,
                                            key.clone(),
                                            observation.observed_at_ms,
                                        );
                                        if !was_active
                                            && dashboard.pairs[index].pair.duration == "1h"
                                        {
                                            let plan = if direction == "A" {
                                                scan.direction_a_plan
                                            } else {
                                                scan.direction_b_plan
                                            };
                                            if let Some(plan) = plan {
                                                let binance = binance.clone();
                                                let polymarket = polymarket.clone();
                                                let storage = storage.clone();
                                                let pair = dashboard.pairs[index].pair.clone();
                                                let tracked_pair = tracked.get(&slug).cloned();
                                                if let Some(tracked_pair) = tracked_pair {
                                                    let direction = direction.to_string();
                                                    let quoted_at_ms = observation.observed_at_ms;
                                                    tokio::spawn(async move {
                                                        run_automatic_shadow(
                                                            binance,
                                                            polymarket,
                                                            storage,
                                                            tracked_pair,
                                                            pair,
                                                            direction,
                                                            quoted_at_ms,
                                                            plan,
                                                        )
                                                        .await;
                                                    });
                                                }
                                            }
                                        }
                                        if confirmed && !paper_executed.contains(&key) {
                                            match storage.insert_paper_trade(&trade) {
                                                Ok(()) => {
                                                    paper_executed.insert(key.clone());
                                                }
                                                Err(error) => {
                                                    dashboard.pairs[index].error =
                                                        Some(format!("模拟交易写入失败：{error}"));
                                                }
                                            }
                                        }
                                    } else {
                                        active_signals.remove(&key);
                                    }
                                }
                                let polymarket_internal_eligible = observation
                                    .polymarket_internal
                                    .as_ref()
                                    .is_some_and(|value| {
                                        value.rejections.is_empty()
                                            && Decimal::from_str(&value.net_edge)
                                                .is_ok_and(|edge| edge > Decimal::ZERO)
                                    });
                                if polymarket_internal_eligible
                                    && !polymarket_internal_shadow_executed.contains(&slug)
                                {
                                    if let (Some(plan), Some(tracked_pair)) =
                                        (scan.polymarket_internal_plan, tracked.get(&slug).cloned())
                                    {
                                        polymarket_internal_shadow_executed.insert(slug.clone());
                                        let polymarket = polymarket.clone();
                                        let storage = storage.clone();
                                        let pair = dashboard.pairs[index].pair.clone();
                                        let quoted_at_ms = observation.observed_at_ms;
                                        tokio::spawn(async move {
                                            run_automatic_polymarket_internal_shadow(
                                                polymarket,
                                                storage,
                                                tracked_pair,
                                                pair,
                                                quoted_at_ms,
                                                plan,
                                            )
                                            .await;
                                        });
                                    }
                                }
                                dashboard.pairs[index].latest = Some(scan.observation);
                                dashboard.pairs[index].error = None;
                            }
                        }
                        Err(error) => dashboard.pairs[index].error = Some(error),
                    }
                }
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

async fn run_binance_reference_prices(binance: BinanceClient, state: Arc<RwLock<DashboardState>>) {
    loop {
        let observed_at_ms = Utc::now().timestamp_millis();
        let (btc, eth, bnb) = tokio::join!(
            binance.spot_price("BTCUSDT"),
            binance.spot_price("ETHUSDT"),
            binance.spot_price("BNBUSDT")
        );
        let mut dashboard = state.write().await;
        for (asset, result) in [("BTC", btc), ("ETH", eth), ("BNB", bnb)] {
            match result {
                Ok(price) => {
                    let entry = dashboard.reference_prices.entry(asset.into()).or_default();
                    entry.binance_spot_price = Some(price.price);
                    entry.binance_observed_at_ms = Some(observed_at_ms);
                    update_reference_difference(entry);
                }
                Err(error) => warn!(%error, %asset, "Binance 现货参考价读取失败"),
            }
        }
        drop(dashboard);
        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Debug, Deserialize)]
struct RtdsTwapEvent {
    topic: String,
    payload: RtdsTwapPayload,
}

#[derive(Debug, Deserialize)]
struct RtdsTwapPayload {
    symbol: String,
    full_accuracy_value: String,
    timestamp: i64,
}

async fn run_polymarket_reference_prices(state: Arc<RwLock<DashboardState>>) {
    let mut tasks = JoinSet::new();
    for asset in ["BTC", "ETH", "BNB"] {
        let state = state.clone();
        tasks.spawn(async move { run_polymarket_reference_price(state, asset).await });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            warn!(%error, "Polymarket RTDS 资产任务退出");
        }
    }
}

async fn run_polymarket_reference_price(state: Arc<RwLock<DashboardState>>, asset: &'static str) {
    loop {
        match connect_rtds().await {
            Ok(stream) => {
                let (mut writer, mut reader) = stream.split();
                let symbol = format!("{}/usd", asset.to_ascii_lowercase());
                let subscription = serde_json::json!({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_twap_sixty",
                        "type": "update",
                        "filters": serde_json::json!({"symbol": symbol}).to_string()
                    }]
                });
                if writer
                    .send(Message::Text(subscription.to_string().into()))
                    .await
                    .is_err()
                {
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
                let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            if writer.send(Message::Text("PING".into())).await.is_err() {
                                break;
                            }
                        }
                        message = reader.next() => {
                            let Some(Ok(message)) = message else { break };
                            let Ok(text) = message.to_text() else { continue };
                            let Ok(event) = serde_json::from_str::<RtdsTwapEvent>(text) else { continue };
                            if event.topic != "crypto_prices_twap_sixty" { continue }
                            if !event.payload.symbol.eq_ignore_ascii_case(&symbol) { continue }
                            let Some(value) = rtds_fixed_point_price(&event.payload.full_accuracy_value) else { continue };
                            let mut dashboard = state.write().await;
                            let entry = dashboard.reference_prices.entry(asset.into()).or_default();
                            entry.polymarket_twap_60s = Some(value.normalize().to_string());
                            entry.polymarket_observed_at_ms = rtds_timestamp_ms(event.payload.timestamp);
                            update_reference_difference(entry);
                        }
                    }
                }
            }
            Err(error) => warn!(%error, %asset, "Polymarket RTDS 连接失败"),
        }
        sleep(Duration::from_secs(3)).await;
    }
}

async fn connect_rtds(
) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>, String>
{
    const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
    const RTDS_HOST: &str = "ws-live-data.polymarket.com";
    let proxy = ["all_proxy", "ALL_PROXY", "https_proxy", "HTTPS_PROXY"]
        .into_iter()
        .find_map(|name| env::var(name).ok().filter(|value| !value.is_empty()));
    let Some(proxy) = proxy else {
        return connect_async(RTDS_URL)
            .await
            .map(|(stream, _)| stream)
            .map_err(|error| error.to_string());
    };
    let proxy = reqwest::Url::parse(&proxy).map_err(|error| format!("代理地址无效：{error}"))?;
    if proxy.scheme() != "http" || !proxy.username().is_empty() || proxy.password().is_some() {
        return Err("RTDS 仅支持无认证 HTTP 代理".into());
    }
    let proxy_host = proxy.host_str().ok_or("代理地址缺少主机")?;
    let proxy_port = proxy.port_or_known_default().ok_or("代理地址缺少端口")?;
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        TcpStream::connect((proxy_host, proxy_port)),
    )
    .await
    .map_err(|_| "连接 RTDS 代理超时".to_string())?
    .map_err(|error| format!("连接 RTDS 代理失败：{error}"))?;
    let request = format!(
        "CONNECT {RTDS_HOST}:443 HTTP/1.1\r\nHost: {RTDS_HOST}:443\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("写入 RTDS 代理请求失败：{error}"))?;
    let mut response = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buffer))
            .await
            .map_err(|_| "等待 RTDS 代理响应超时".to_string())?
            .map_err(|error| format!("读取 RTDS 代理响应失败：{error}"))?;
        if read == 0 {
            return Err("RTDS 代理提前关闭连接".into());
        }
        response.extend_from_slice(&buffer[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > 16 * 1024 {
            return Err("RTDS 代理响应头过大".into());
        }
    }
    let status_line = String::from_utf8_lossy(&response)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string();
    if !status_line.contains(" 200 ") {
        return Err(format!("RTDS 代理隧道失败：{status_line}"));
    }
    client_async_tls(RTDS_URL, stream)
        .await
        .map(|(stream, _)| stream)
        .map_err(|error| error.to_string())
}

fn update_reference_difference(reference: &mut ReferencePriceView) {
    reference.price_difference = reference
        .binance_spot_price
        .as_deref()
        .and_then(|value| Decimal::from_str(value).ok())
        .zip(
            reference
                .polymarket_twap_60s
                .as_deref()
                .and_then(|value| Decimal::from_str(value).ok()),
        )
        .map(|(binance, polymarket)| (binance - polymarket).normalize().to_string());
}

fn rtds_fixed_point_price(value: &str) -> Option<Decimal> {
    Decimal::from_str(value)
        .ok()
        .map(|fixed_point| fixed_point * Decimal::new(1, 18))
}

fn rtds_timestamp_ms(timestamp: i64) -> Option<i64> {
    if timestamp <= 0 {
        None
    } else if timestamp < 1_000_000_000_000 {
        timestamp.checked_mul(1_000)
    } else {
        Some(timestamp)
    }
}

async fn discover(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
) -> Result<Vec<TrackedPair>, String> {
    let topics = binance
        .list_crypto_up_down_markets()
        .await
        .map_err(|error| error.to_string())?;
    let now = Utc::now().timestamp_millis();
    let candidates: Vec<_> = topics
        .into_iter()
        .filter(|topic| topic.end_date > now && topic.start_date <= now + 86_400_000)
        .collect();
    let mut tasks = JoinSet::new();
    for topic in candidates {
        let binance = binance.clone();
        let polymarket = polymarket.clone();
        tasks.spawn(async move { build_pair(&binance, &polymarket, topic).await });
    }
    let mut pairs = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(Some(pair))) => pairs.push(pair),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => warn!(%error, "候选市场加载失败"),
            Err(error) => warn!(%error, "候选市场任务失败"),
        }
    }
    Ok(pairs)
}

pub async fn simulate_live_fok(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    asset: &str,
    direction: &str,
    quantity: Option<Decimal>,
    budget: Option<Decimal>,
    first_limit: Option<Decimal>,
    second_limit: Option<Decimal>,
    quantity_step: Decimal,
    delay_ms: u64,
) -> Result<LiveFokSimulation, String> {
    if quantity.is_some() && budget.is_some() {
        return Err("份数和预算只能指定一个".into());
    }
    if !matches!(direction, "A" | "B") {
        return Err("方向只能是 A 或 B".into());
    }
    let now = Utc::now().timestamp_millis();
    let symbol = format!("{}USDT", asset.to_ascii_uppercase());
    let topics = binance
        .list_crypto_up_down_markets()
        .await
        .map_err(|error| error.to_string())?;
    let topic = topics
        .into_iter()
        .find(|topic| {
            topic.symbol.eq_ignore_ascii_case(&symbol)
                && topic.start_date <= now
                && topic.end_date > now
                && topic.end_date.saturating_sub(topic.start_date) == 3_600_000
        })
        .ok_or_else(|| format!("没有找到当前 {asset} 1h 市场"))?;
    let pair = build_pair(binance, polymarket, topic)
        .await?
        .filter(|pair| is_paper_settlement_aligned(&pair.view))
        .ok_or_else(|| format!("当前 {asset} 1h 市场的结算规则未对齐"))?;
    let (quoted_first, quoted_second, quoted_at_ms) =
        direction_levels(binance, polymarket, &pair, direction).await?;
    let first_limit = first_limit
        .or_else(|| quoted_first.first().map(|level| level.price))
        .ok_or("第一腿没有卖盘")?;
    let second_limit = second_limit
        .or_else(|| quoted_second.first().map(|level| level.price))
        .ok_or("第二腿没有卖盘")?;
    let quantity = match (quantity, budget) {
        (Some(quantity), None) => quantity,
        (None, Some(budget)) => {
            quantity_for_budget_at_step(budget, first_limit, second_limit, quantity_step)
                .ok_or("预算、限价或数量步长无效")?
        }
        (None, None) => PAPER_MAX_QUANTITY,
        (Some(_), Some(_)) => unreachable!(),
    };
    let first_request = FokOrderRequest {
        quantity,
        limit_price: first_limit,
    };
    let second_request = FokOrderRequest {
        quantity,
        limit_price: second_limit,
    };
    preflight_pair_fok(first_request, &quoted_first, second_request, &quoted_second)
        .map_err(|reasons| format!("提交前预检拒绝：{}", reasons.join("；")))?;

    sleep(Duration::from_millis(delay_ms)).await;
    let (execution_first, execution_second, executed_at_ms) =
        direction_levels(binance, polymarket, &pair, direction).await?;
    let mut executor = SimulatedExecutor::default();
    let report = executor.execute_pair_fok(
        first_request,
        &execution_first,
        second_request,
        &execution_second,
    );
    Ok(LiveFokSimulation {
        slug: pair.view.slug,
        direction: direction.into(),
        quoted_at_ms,
        executed_at_ms,
        first_request,
        second_request,
        report,
    })
}

async fn direction_levels(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    pair: &TrackedPair,
    direction: &str,
) -> Result<(Vec<PriceLevel>, Vec<PriceLevel>, i64), String> {
    let market = pair
        .binance
        .markets
        .first()
        .ok_or_else(|| "Binance outcome 市场缺失".to_string())?;
    let binance_outcome = if direction == "A" { "up" } else { "down" };
    let token = market
        .outcomes
        .iter()
        .find(|outcome| outcome.name.eq_ignore_ascii_case(binance_outcome))
        .ok_or_else(|| format!("Binance {binance_outcome} Token 缺失"))?;
    let polymarket_token = if direction == "A" {
        &pair.polymarket_down_token
    } else {
        &pair.polymarket_up_token
    };
    let vendor = pair.binance.vendor.to_ascii_lowercase();
    let (first, second) = tokio::join!(
        binance.order_book(&vendor, market.market_id, &token.token_id),
        polymarket.order_book(polymarket_token),
    );
    let first = first.map_err(|error| error.to_string())?;
    let second = second.map_err(|error| error.to_string())?;
    let observed_at_ms = Utc::now().timestamp_millis();
    let skew = quote_skew_ms(first.timestamp, &second.timestamp).ok_or("无法解析双边盘口时间戳")?;
    let age = quote_age_ms(observed_at_ms, first.timestamp, &second.timestamp)
        .ok_or("无法解析双边盘口年龄")?;
    if skew > PAPER_MAX_QUOTE_SKEW_MS {
        return Err(format!("双边盘口时间差 {skew}ms 超过 1000ms"));
    }
    if age > PAPER_MAX_QUOTE_AGE_MS {
        return Err(format!("盘口年龄 {age}ms 超过 5000ms"));
    }
    let first = first
        .ask_levels()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(price, quantity)| PriceLevel { price, quantity })
        .collect();
    let second = second
        .ask_levels()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(price, quantity)| PriceLevel { price, quantity })
        .collect();
    Ok((first, second, observed_at_ms))
}

async fn run_automatic_shadow(
    binance: BinanceClient,
    polymarket: PolymarketClient,
    storage: Storage,
    tracked_pair: TrackedPair,
    pair: TrackedPairView,
    direction: String,
    quoted_at_ms: i64,
    plan: ShadowPlan,
) {
    sleep(Duration::from_millis(250)).await;
    let executed_at_ms = Utc::now().timestamp_millis();
    let execution = direction_levels(&binance, &polymarket, &tracked_pair, &direction).await;
    let record = match execution {
        Ok((first_levels, second_levels, executed_at_ms)) => {
            let mut executor = SimulatedExecutor::default();
            let report = executor.execute_pair_fok(
                plan.first_request,
                &first_levels,
                plan.second_request,
                &second_levels,
            );
            let reasons = [
                report.first.reason.as_deref(),
                report.second.reason.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("；");
            ShadowExecutionView {
                id: None,
                slug: pair.slug,
                asset: pair.asset,
                direction,
                quoted_at_ms,
                executed_at_ms,
                requested_quantity: plan.first_request.quantity.to_string(),
                first_limit_price: plan.first_request.limit_price.to_string(),
                second_limit_price: plan.second_request.limit_price.to_string(),
                first_status: order_status_name(report.first.status),
                first_filled_quantity: report.first.filled_quantity.to_string(),
                first_average_price: report.first.average_price.map(|value| value.to_string()),
                first_total_cost: report.first.total_cost.to_string(),
                second_status: order_status_name(report.second.status),
                second_filled_quantity: report.second.filled_quantity.to_string(),
                second_average_price: report.second.average_price.map(|value| value.to_string()),
                second_total_cost: report.second.total_cost.to_string(),
                unhedged_quantity: report.hedge.exposure.to_string(),
                status: order_status_name(report.status),
                trigger_source: "automatic".into(),
                error: (!reasons.is_empty()).then_some(reasons),
            }
        }
        Err(error) => ShadowExecutionView {
            id: None,
            slug: pair.slug,
            asset: pair.asset,
            direction,
            quoted_at_ms,
            executed_at_ms,
            requested_quantity: plan.first_request.quantity.to_string(),
            first_limit_price: plan.first_request.limit_price.to_string(),
            second_limit_price: plan.second_request.limit_price.to_string(),
            first_status: "not_submitted".into(),
            first_filled_quantity: "0".into(),
            first_average_price: None,
            first_total_cost: "0".into(),
            second_status: "not_submitted".into(),
            second_filled_quantity: "0".into(),
            second_average_price: None,
            second_total_cost: "0".into(),
            unhedged_quantity: "0".into(),
            status: "error".into(),
            trigger_source: "automatic".into(),
            error: Some(error),
        },
    };
    if let Err(error) = storage.insert_shadow_execution(&record) {
        warn!(%error, slug = %record.slug, direction = %record.direction, "保存自动影子执行失败");
    }
}

async fn polymarket_internal_levels(
    polymarket: &PolymarketClient,
    pair: &TrackedPair,
) -> Result<(Vec<PriceLevel>, Vec<PriceLevel>, i64), String> {
    let (up, down) = tokio::join!(
        polymarket.order_book(&pair.polymarket_up_token),
        polymarket.order_book(&pair.polymarket_down_token),
    );
    let up = up.map_err(|error| error.to_string())?;
    let down = down.map_err(|error| error.to_string())?;
    let observed_at_ms = Utc::now().timestamp_millis();
    let up_timestamp = up
        .timestamp
        .parse::<i64>()
        .map_err(|_| "无法解析 Polymarket Up 盘口时间戳".to_string())?;
    let down_timestamp = down
        .timestamp
        .parse::<i64>()
        .map_err(|_| "无法解析 Polymarket Down 盘口时间戳".to_string())?;
    let skew = up_timestamp.abs_diff(down_timestamp).min(i64::MAX as u64) as i64;
    let age = observed_at_ms
        .abs_diff(up_timestamp)
        .max(observed_at_ms.abs_diff(down_timestamp))
        .min(i64::MAX as u64) as i64;
    if skew > PAPER_MAX_QUOTE_SKEW_MS {
        return Err(format!("Polymarket 两盘口时间差 {skew}ms 超过 1000ms"));
    }
    if age > PAPER_MAX_QUOTE_AGE_MS {
        return Err(format!("Polymarket 盘口年龄 {age}ms 超过 5000ms"));
    }
    let up = up
        .ask_levels()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(price, quantity)| PriceLevel { price, quantity })
        .collect();
    let down = down
        .ask_levels()
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|(price, quantity)| PriceLevel { price, quantity })
        .collect();
    Ok((up, down, observed_at_ms))
}

async fn run_automatic_polymarket_internal_shadow(
    polymarket: PolymarketClient,
    storage: Storage,
    tracked_pair: TrackedPair,
    pair: TrackedPairView,
    quoted_at_ms: i64,
    plan: ShadowPlan,
) {
    sleep(Duration::from_millis(250)).await;
    let fallback_executed_at_ms = Utc::now().timestamp_millis();
    let execution = polymarket_internal_levels(&polymarket, &tracked_pair).await;
    let record = match execution {
        Ok((up_levels, down_levels, executed_at_ms)) => {
            let mut executor = SimulatedExecutor::default();
            let report = executor.execute_pair_fok(
                plan.first_request,
                &up_levels,
                plan.second_request,
                &down_levels,
            );
            let reasons = [
                report.first.reason.as_deref(),
                report.second.reason.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("；");
            ShadowExecutionView {
                id: None,
                slug: pair.slug,
                asset: pair.asset,
                direction: "PM_INTERNAL".into(),
                quoted_at_ms,
                executed_at_ms,
                requested_quantity: plan.first_request.quantity.to_string(),
                first_limit_price: plan.first_request.limit_price.to_string(),
                second_limit_price: plan.second_request.limit_price.to_string(),
                first_status: order_status_name(report.first.status),
                first_filled_quantity: report.first.filled_quantity.to_string(),
                first_average_price: report.first.average_price.map(|value| value.to_string()),
                first_total_cost: report.first.total_cost.to_string(),
                second_status: order_status_name(report.second.status),
                second_filled_quantity: report.second.filled_quantity.to_string(),
                second_average_price: report.second.average_price.map(|value| value.to_string()),
                second_total_cost: report.second.total_cost.to_string(),
                unhedged_quantity: report.hedge.exposure.to_string(),
                status: order_status_name(report.status),
                trigger_source: "automatic_polymarket_internal_250ms".into(),
                error: (!reasons.is_empty()).then_some(reasons),
            }
        }
        Err(error) => ShadowExecutionView {
            id: None,
            slug: pair.slug,
            asset: pair.asset,
            direction: "PM_INTERNAL".into(),
            quoted_at_ms,
            executed_at_ms: fallback_executed_at_ms,
            requested_quantity: plan.first_request.quantity.to_string(),
            first_limit_price: plan.first_request.limit_price.to_string(),
            second_limit_price: plan.second_request.limit_price.to_string(),
            first_status: "not_submitted".into(),
            first_filled_quantity: "0".into(),
            first_average_price: None,
            first_total_cost: "0".into(),
            second_status: "not_submitted".into(),
            second_filled_quantity: "0".into(),
            second_average_price: None,
            second_total_cost: "0".into(),
            unhedged_quantity: "0".into(),
            status: "error".into(),
            trigger_source: "automatic_polymarket_internal_250ms".into(),
            error: Some(error),
        },
    };
    if let Err(error) = storage.insert_shadow_execution(&record) {
        warn!(%error, slug = %record.slug, "保存 Polymarket 内部自动影子执行失败");
    }
}

fn order_status_name(status: domain::OrderStatus) -> String {
    use domain::OrderStatus;
    match status {
        OrderStatus::Detected => "detected",
        OrderStatus::Preparing => "preparing",
        OrderStatus::Submitting => "submitting",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::FullyFilled => "fully_filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Hedged => "hedged",
        OrderStatus::Failed => "failed",
        OrderStatus::Unhedged => "unhedged",
        OrderStatus::Settled => "settled",
        OrderStatus::Redeemed => "redeemed",
    }
    .into()
}

async fn build_pair(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    topic: BinanceMarketTopic,
) -> Result<Option<TrackedPair>, String> {
    let (detail, event) = tokio::join!(
        binance.market_detail(topic.market_topic_id),
        polymarket.event_by_slug(&topic.slug)
    );
    let detail = detail.map_err(|error| error.to_string())?;
    let Some(event) = event.map_err(|error| error.to_string())? else {
        return Ok(None);
    };
    let Some(pm_market) = event
        .markets
        .into_iter()
        .find(|market| market.slug == detail.slug)
    else {
        return Ok(None);
    };
    let (polymarket_up_token, polymarket_down_token) = pm_market
        .outcome_tokens()
        .map_err(|error| error.to_string())?;
    let polymarket_fee_rate = if pm_market.fees_enabled {
        let schedule = pm_market
            .fee_schedule
            .as_ref()
            .ok_or_else(|| format!("Polymarket 市场缺少 feeSchedule：{}", detail.slug))?;
        if schedule.exponent != 1 {
            return Err(format!(
                "Polymarket 暂不支持的费用指数：{} exponent={}",
                detail.slug, schedule.exponent
            ));
        }
        schedule.rate
    } else {
        Decimal::ZERO
    };
    let asset = detail
        .symbol
        .strip_suffix("USDT")
        .unwrap_or(&detail.symbol)
        .to_string();
    let duration_minutes = (detail.end_date - detail.start_date) / 60_000;
    let duration = if duration_minutes < 60 {
        format!("{duration_minutes}m")
    } else if duration_minutes % 1440 == 0 {
        format!("{}d", duration_minutes / 1440)
    } else {
        format!("{}h", duration_minutes / 60)
    };
    let binance_uses_candle = detail
        .variant_data
        .price_feed_provider
        .eq_ignore_ascii_case("BINANCE");
    let binance_uses_greater_or_equal = detail
        .description
        .as_deref()
        .is_some_and(|description| description.contains("greater than or equal"));
    let binance_spec = ResolutionSpec {
        asset: asset.clone(),
        quote_currency: "USDT".into(),
        price_feed_provider: detail.variant_data.price_feed_provider.clone(),
        price_feed_id: detail.variant_data.price_feed_id.clone(),
        calculation: if binance_uses_candle {
            format!("candle_open_close_{duration}")
        } else {
            "top_of_book_5m_candle_close".into()
        },
        up_comparison: if binance_uses_greater_or_equal {
            "greater_than_or_equal".into()
        } else {
            "greater_than".into()
        },
        equal_rule: if binance_uses_greater_or_equal {
            "up".into()
        } else {
            "split_50_50".into()
        },
        fallback_rule: detail.description.as_ref().and_then(|description| {
            description
                .contains("consensus of reliable sources")
                .then(|| "source_consensus".into())
        }),
        collateral: detail.collateral.clone(),
    };
    let polymarket_uses_binance = pm_market
        .resolution_source
        .to_ascii_lowercase()
        .contains("binance.com");
    let polymarket_spec = ResolutionSpec {
        asset: asset.clone(),
        quote_currency: if polymarket_uses_binance {
            "USDT".into()
        } else {
            "USD".into()
        },
        price_feed_provider: if polymarket_uses_binance {
            "BINANCE".into()
        } else {
            "CHAINLINK".into()
        },
        price_feed_id: if polymarket_uses_binance {
            None
        } else {
            Some(pm_market.resolution_source.clone())
        },
        calculation: if polymarket_uses_binance {
            format!("candle_open_close_{duration}")
        } else {
            "twap_60s".into()
        },
        up_comparison: "greater_than_or_equal".into(),
        equal_rule: "up".into(),
        fallback_rule: None,
        collateral: "USDC".into(),
    };
    let pm_end_ms = DateTime::parse_from_rfc3339(&event.end_date)
        .map(|value| value.timestamp_millis())
        .unwrap_or_default();
    let pm_start_ms = pm_market
        .event_start_time
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis())
        .or_else(|| slug_start_ms(&topic.slug));
    let assessment = classify_candidate_pair(
        pm_start_ms == Some(detail.start_date),
        pm_end_ms == detail.end_date,
        &binance_spec,
        &polymarket_spec,
    );
    let classification = match assessment.classification {
        PairClassification::Exact => "exact",
        PairClassification::Basis => "basis",
        PairClassification::Incompatible => "incompatible",
    };
    let market = detail
        .markets
        .first()
        .ok_or_else(|| "Binance 市场缺少 outcome".to_string())?;
    if market.outcomes.len() < 2 {
        return Err("Binance 市场缺少 Up/Down Token".into());
    }
    Ok(Some(TrackedPair {
        view: TrackedPairView {
            slug: detail.slug.clone(),
            binance_topic_id: detail.market_topic_id,
            title: detail.title.clone(),
            asset,
            duration,
            classification: classification.into(),
            differences: assessment.differences,
            binance_source: format!(
                "{}/{}",
                detail.variant_data.price_feed_provider, detail.variant_data.price_feed_symbol
            ),
            polymarket_source: pm_market.resolution_source.clone(),
            start_ms: detail.start_date,
            end_ms: detail.end_date,
            binance_fee_bps: detail.fee_rate_bps,
            polymarket_fee_rate: polymarket_fee_rate.to_string(),
            liquidity: topic.liquidity,
            trade_volume: topic.trade_volume,
        },
        binance: detail,
        polymarket_up_token,
        polymarket_down_token,
        polymarket_fee_rate,
    }))
}

async fn reconcile_settlements(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    storage: &Storage,
    pending: Vec<PendingSettlement>,
) {
    let mut tasks = JoinSet::new();
    for item in pending {
        let binance = binance.clone();
        let polymarket = polymarket.clone();
        tasks.spawn(async move { settlement_update(&binance, &polymarket, item).await });
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(update)) => {
                if let Err(error) = storage.upsert_settlement(&update) {
                    warn!(%error, slug = update.slug, "保存结算对比失败");
                } else if let Err(error) = storage.settle_paper_trades(&update) {
                    warn!(%error, slug = update.slug, "结算模拟交易失败");
                }
            }
            Ok(Err(error)) => warn!(%error, "回查结算结果失败"),
            Err(error) => warn!(%error, "结算回查任务失败"),
        }
    }
}

async fn settlement_update(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    pending: PendingSettlement,
) -> Result<SettlementUpdate, String> {
    let (detail, event) = tokio::join!(
        binance.market_detail(pending.binance_topic_id),
        polymarket.event_by_slug(&pending.slug)
    );
    let detail = detail.map_err(|error| error.to_string())?;
    let event = event
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("Polymarket 缺少事件：{}", pending.slug))?;
    let market = event
        .markets
        .iter()
        .find(|market| market.slug == pending.slug)
        .or_else(|| event.markets.first())
        .ok_or_else(|| format!("Polymarket 事件缺少市场：{}", pending.slug))?;

    let binance_outcome = binance_outcome(
        detail.variant_data.start_price.as_deref(),
        detail.variant_data.end_price.as_deref(),
    );
    let polymarket_outcome = market
        .resolved_outcome()
        .map_err(|error| error.to_string())?;
    let relationship = match (&binance_outcome, &polymarket_outcome) {
        (Some(left), Some(right)) if left.eq_ignore_ascii_case(right) => "matching",
        (Some(_), Some(_)) => "divergent",
        _ => "pending",
    };
    let metadata = event.event_metadata;
    Ok(SettlementUpdate {
        slug: pending.slug,
        binance_status: detail.status,
        binance_start_price: detail.variant_data.start_price,
        binance_end_price: detail.variant_data.end_price,
        binance_outcome,
        polymarket_status: market
            .uma_resolution_status
            .clone()
            .unwrap_or_else(|| if event.closed { "closed" } else { "open" }.into()),
        polymarket_price_to_beat: metadata
            .as_ref()
            .and_then(|value| value.price_to_beat)
            .map(|value| value.to_string()),
        polymarket_final_price: metadata
            .and_then(|value| value.final_price)
            .map(|value| value.to_string()),
        polymarket_outcome,
        relationship: relationship.into(),
        checked_at_ms: Utc::now().timestamp_millis(),
    })
}

fn binance_outcome(start_price: Option<&str>, end_price: Option<&str>) -> Option<String> {
    let start = Decimal::from_str(start_price?).ok()?;
    let end = Decimal::from_str(end_price?).ok()?;
    Some(
        if end > start {
            "Up"
        } else if end < start {
            "Down"
        } else {
            "Split"
        }
        .into(),
    )
}

async fn scan_pair(
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    pair: &TrackedPair,
) -> Result<ScanPairResult, String> {
    let scan_started = Instant::now();
    let market = pair
        .binance
        .markets
        .first()
        .ok_or_else(|| "Binance outcome 市场缺失".to_string())?;
    let up_token = market
        .outcomes
        .iter()
        .find(|outcome| outcome.name.eq_ignore_ascii_case("up"))
        .ok_or_else(|| "Binance Up Token 缺失".to_string())?;
    let down_token = market
        .outcomes
        .iter()
        .find(|outcome| outcome.name.eq_ignore_ascii_case("down"))
        .ok_or_else(|| "Binance Down Token 缺失".to_string())?;
    let vendor = pair.binance.vendor.to_ascii_lowercase();
    let (bin_up, bin_down, pm_up, pm_down) = tokio::join!(
        binance.order_book(&vendor, market.market_id, &up_token.token_id),
        binance.order_book(&vendor, market.market_id, &down_token.token_id),
        polymarket.order_book(&pair.polymarket_up_token),
        polymarket.order_book(&pair.polymarket_down_token),
    );
    let bin_up = bin_up.map_err(|error| error.to_string())?;
    let bin_down = bin_down.map_err(|error| error.to_string())?;
    let pm_up = pm_up.map_err(|error| error.to_string())?;
    let pm_down = pm_down.map_err(|error| error.to_string())?;
    let direction_a_quote_skew_ms = quote_skew_ms(bin_up.timestamp, &pm_down.timestamp);
    let direction_b_quote_skew_ms = quote_skew_ms(bin_down.timestamp, &pm_up.timestamp);
    let observed_at_ms = Utc::now().timestamp_millis();
    let binance_internal_quote_skew_ms = bin_up
        .timestamp
        .abs_diff(bin_down.timestamp)
        .min(i64::MAX as u64) as i64;
    let binance_internal_quote_age_ms = observed_at_ms
        .abs_diff(bin_up.timestamp)
        .max(observed_at_ms.abs_diff(bin_down.timestamp))
        .min(i64::MAX as u64) as i64;
    let polymarket_internal_quote_skew_ms = pm_up
        .timestamp
        .parse::<i64>()
        .ok()
        .zip(pm_down.timestamp.parse::<i64>().ok())
        .map(|(up, down)| up.abs_diff(down).min(i64::MAX as u64) as i64);
    let polymarket_internal_quote_age_ms = pm_up
        .timestamp
        .parse::<i64>()
        .ok()
        .zip(pm_down.timestamp.parse::<i64>().ok())
        .map(|(up, down)| {
            observed_at_ms
                .abs_diff(up)
                .max(observed_at_ms.abs_diff(down))
                .min(i64::MAX as u64) as i64
        });
    let direction_a_quote_age_ms =
        quote_age_ms(observed_at_ms, bin_up.timestamp, &pm_down.timestamp);
    let direction_b_quote_age_ms =
        quote_age_ms(observed_at_ms, bin_down.timestamp, &pm_up.timestamp);
    let bin_up_levels = bin_up.ask_levels().map_err(|error| error.to_string())?;
    let bin_down_levels = bin_down.ask_levels().map_err(|error| error.to_string())?;
    let pm_up_levels = pm_up.ask_levels().map_err(|error| error.to_string())?;
    let pm_down_levels = pm_down.ask_levels().map_err(|error| error.to_string())?;
    let bin_up = bin_up_levels.first().copied();
    let bin_down = bin_down_levels.first().copied();
    let pm_up = pm_up_levels.first().copied();
    let pm_down = pm_down_levels.first().copied();
    let binance_fee_rate = Decimal::from(pair.binance.fee_rate_bps) / Decimal::from(10_000);
    let binance_internal = calculate_binance_internal(
        &bin_up_levels,
        &bin_down_levels,
        binance_fee_rate,
        pair.view.end_ms.saturating_sub(observed_at_ms),
        binance_internal_quote_skew_ms,
        binance_internal_quote_age_ms,
    );
    let polymarket_internal_calculation = polymarket_internal_quote_skew_ms
        .zip(polymarket_internal_quote_age_ms)
        .and_then(|(quote_skew_ms, quote_age_ms)| {
            calculate_polymarket_internal(
                &pm_up_levels,
                &pm_down_levels,
                pair.polymarket_fee_rate,
                pair.view.end_ms.saturating_sub(observed_at_ms),
                quote_skew_ms,
                quote_age_ms,
            )
        });
    let polymarket_internal_plan = polymarket_internal_calculation
        .as_ref()
        .map(|calculation| calculation.plan);
    let polymarket_internal = polymarket_internal_calculation.map(|calculation| calculation.view);
    let (direction_a, direction_b) =
        if observed_at_ms < pair.view.end_ms && is_research_eligible(&pair.view.classification) {
            (
                calculate_cross(
                    &bin_up_levels,
                    &pm_down_levels,
                    binance_fee_rate,
                    pair.polymarket_fee_rate,
                    pair.view.end_ms.saturating_sub(observed_at_ms),
                ),
                calculate_cross(
                    &bin_down_levels,
                    &pm_up_levels,
                    binance_fee_rate,
                    pair.polymarket_fee_rate,
                    pair.view.end_ms.saturating_sub(observed_at_ms),
                ),
            )
        } else {
            (None, None)
        };
    let direction_a_plan = direction_a.as_ref().map(|value| value.plan);
    let direction_b_plan = direction_b.as_ref().map(|value| value.plan);
    Ok(ScanPairResult {
        observation: ObservationView {
            slug: pair.view.slug.clone(),
            observed_at_ms,
            binance_up_ask: price(bin_up),
            binance_down_ask: price(bin_down),
            polymarket_up_ask: price(pm_up),
            polymarket_down_ask: price(pm_down),
            direction_a_cost: direction_a.as_ref().map(|value| value.cost.to_string()),
            direction_a_fees: direction_a.as_ref().map(|value| value.fees.to_string()),
            direction_a_net_edge: direction_a.as_ref().map(|value| value.net_edge.to_string()),
            direction_a_risk_buffers: direction_a
                .as_ref()
                .map(|value| value.risk_buffers.to_string()),
            direction_a_size: direction_a.as_ref().map(|value| value.size.to_string()),
            direction_b_cost: direction_b.as_ref().map(|value| value.cost.to_string()),
            direction_b_fees: direction_b.as_ref().map(|value| value.fees.to_string()),
            direction_b_net_edge: direction_b.as_ref().map(|value| value.net_edge.to_string()),
            direction_b_risk_buffers: direction_b
                .as_ref()
                .map(|value| value.risk_buffers.to_string()),
            direction_b_size: direction_b.as_ref().map(|value| value.size.to_string()),
            direction_a_rejections: Vec::new(),
            direction_b_rejections: Vec::new(),
            scan_latency_ms: Some(scan_started.elapsed().as_millis().min(i64::MAX as u128) as i64),
            direction_a_quote_skew_ms,
            direction_b_quote_skew_ms,
            direction_a_quote_age_ms,
            direction_b_quote_age_ms,
            remaining_time_ms: Some(pair.view.end_ms.saturating_sub(observed_at_ms)),
            binance_reference_price: None,
            binance_reference_at_ms: None,
            polymarket_reference_price: None,
            polymarket_reference_at_ms: None,
            binance_internal,
            polymarket_internal,
        },
        direction_a_plan,
        direction_b_plan,
        polymarket_internal_plan,
    })
}

fn calculate_polymarket_internal(
    up_levels: &[(Decimal, Decimal)],
    down_levels: &[(Decimal, Decimal)],
    fee_rate: Decimal,
    remaining_time_ms: i64,
    quote_skew_ms: i64,
    quote_age_ms: i64,
) -> Option<SameMarketCalculation> {
    if fee_rate < Decimal::ZERO {
        return None;
    }
    let up_depth: Decimal = up_levels.iter().map(|(_, size)| *size).sum();
    let down_depth: Decimal = down_levels.iter().map(|(_, size)| *size).sum();
    let size = up_depth.min(down_depth).min(PAPER_MAX_QUANTITY);
    if size <= Decimal::ZERO {
        return None;
    }
    let (up_cost, up_limit) = fok_cost_for_quantity(up_levels, size)?;
    let (down_cost, down_limit) = fok_cost_for_quantity(down_levels, size)?;
    let total_cost = up_cost + down_cost;
    let fees = polymarket_fee_for_quantity(up_levels, size, fee_rate)?
        + polymarket_fee_for_quantity(down_levels, size, fee_rate)?;
    let second_leg_slippage =
        total_cost * Decimal::from(PAPER_SECOND_LEG_SLIPPAGE_BPS) / Decimal::from(10_000);
    let capital_cost = total_cost * Decimal::from(PAPER_CAPITAL_APR_BPS) / Decimal::from(10_000)
        * Decimal::from(remaining_time_ms.max(0))
        / Decimal::from(MILLISECONDS_PER_YEAR);
    let risk_buffers = second_leg_slippage + capital_cost;
    let net_edge = (size - total_cost - fees - risk_buffers) / size;
    let mut rejections = Vec::new();
    if net_edge <= Decimal::ZERO {
        rejections.push("扣费及缓冲后无正净空间".into());
    }
    if quote_skew_ms > PAPER_MAX_QUOTE_SKEW_MS {
        rejections.push("Polymarket 两盘口时间差超过 1 秒".into());
    }
    if quote_age_ms > PAPER_MAX_QUOTE_AGE_MS {
        rejections.push("Polymarket 盘口年龄超过 5 秒".into());
    }
    if remaining_time_ms < PAPER_MIN_REMAINING_MS {
        rejections.push("距结算不足 60 秒".into());
    }
    let valid_price = |levels: &[(Decimal, Decimal)]| {
        levels
            .first()
            .is_some_and(|(price, _)| (Decimal::new(5, 2)..=Decimal::new(95, 2)).contains(price))
    };
    if !valid_price(up_levels) || !valid_price(down_levels) {
        rejections.push("单腿价格超出 0.05 至 0.95".into());
    }
    Some(SameMarketCalculation {
        view: PolymarketInternalView {
            cost: (total_cost / size).to_string(),
            fees: (fees / size).to_string(),
            risk_buffers: (risk_buffers / size).to_string(),
            net_edge: net_edge.to_string(),
            size: size.to_string(),
            quote_skew_ms,
            quote_age_ms,
            rejections,
        },
        plan: ShadowPlan {
            first_request: FokOrderRequest {
                quantity: size,
                limit_price: up_limit,
            },
            second_request: FokOrderRequest {
                quantity: size,
                limit_price: down_limit,
            },
        },
    })
}

fn calculate_binance_internal(
    up_levels: &[(Decimal, Decimal)],
    down_levels: &[(Decimal, Decimal)],
    fee_rate: Decimal,
    remaining_time_ms: i64,
    quote_skew_ms: i64,
    quote_age_ms: i64,
) -> Option<BinanceInternalView> {
    if fee_rate < Decimal::ZERO {
        return None;
    }
    let up_depth: Decimal = up_levels.iter().map(|(_, size)| *size).sum();
    let down_depth: Decimal = down_levels.iter().map(|(_, size)| *size).sum();
    let size = up_depth.min(down_depth).min(PAPER_MAX_QUANTITY);
    if size <= Decimal::ZERO {
        return None;
    }
    let (up_cost, _) = fok_cost_for_quantity(up_levels, size)?;
    let (down_cost, _) = fok_cost_for_quantity(down_levels, size)?;
    let total_cost = up_cost + down_cost;
    let fees = total_cost * fee_rate;
    let second_leg_slippage =
        total_cost * Decimal::from(PAPER_SECOND_LEG_SLIPPAGE_BPS) / Decimal::from(10_000);
    let capital_cost = total_cost * Decimal::from(PAPER_CAPITAL_APR_BPS) / Decimal::from(10_000)
        * Decimal::from(remaining_time_ms.max(0))
        / Decimal::from(MILLISECONDS_PER_YEAR);
    let risk_buffers = second_leg_slippage + capital_cost;
    let net_edge = (size - total_cost - fees - risk_buffers) / size;
    let mut rejections = Vec::new();
    if net_edge <= Decimal::ZERO {
        rejections.push("扣费及缓冲后无正净空间".into());
    }
    if quote_skew_ms > PAPER_MAX_QUOTE_SKEW_MS {
        rejections.push("Binance 两盘口时间差超过 1 秒".into());
    }
    if quote_age_ms > PAPER_MAX_QUOTE_AGE_MS {
        rejections.push("Binance 盘口年龄超过 5 秒".into());
    }
    if remaining_time_ms < PAPER_MIN_REMAINING_MS {
        rejections.push("距结算不足 60 秒".into());
    }
    let valid_price = |levels: &[(Decimal, Decimal)]| {
        levels
            .first()
            .is_some_and(|(price, _)| (Decimal::new(5, 2)..=Decimal::new(95, 2)).contains(price))
    };
    if !valid_price(up_levels) || !valid_price(down_levels) {
        rejections.push("单腿价格超出 0.05 至 0.95".into());
    }
    Some(BinanceInternalView {
        cost: (total_cost / size).to_string(),
        fees: (fees / size).to_string(),
        risk_buffers: (risk_buffers / size).to_string(),
        net_edge: net_edge.to_string(),
        size: size.to_string(),
        quote_skew_ms,
        quote_age_ms,
        rejections,
    })
}

fn quote_skew_ms(binance_timestamp_ms: i64, polymarket_timestamp_ms: &str) -> Option<i64> {
    let polymarket_timestamp_ms = polymarket_timestamp_ms.parse::<i64>().ok()?;
    Some(
        binance_timestamp_ms
            .abs_diff(polymarket_timestamp_ms)
            .min(i64::MAX as u64) as i64,
    )
}

fn quote_age_ms(
    observed_at_ms: i64,
    binance_timestamp_ms: i64,
    polymarket_timestamp_ms: &str,
) -> Option<i64> {
    let polymarket_timestamp_ms = polymarket_timestamp_ms.parse::<i64>().ok()?;
    Some(
        observed_at_ms
            .abs_diff(binance_timestamp_ms)
            .max(observed_at_ms.abs_diff(polymarket_timestamp_ms))
            .min(i64::MAX as u64) as i64,
    )
}

struct CrossCalculation {
    cost: Decimal,
    fees: Decimal,
    risk_buffers: Decimal,
    net_edge: Decimal,
    size: Decimal,
    plan: ShadowPlan,
}

fn calculate_cross(
    binance_levels: &[(Decimal, Decimal)],
    polymarket_levels: &[(Decimal, Decimal)],
    binance_fee_rate: Decimal,
    polymarket_fee_rate: Decimal,
    remaining_time_ms: i64,
) -> Option<CrossCalculation> {
    if binance_fee_rate < Decimal::ZERO || polymarket_fee_rate < Decimal::ZERO {
        return None;
    }
    let binance_depth: Decimal = binance_levels.iter().map(|(_, size)| *size).sum();
    let polymarket_depth: Decimal = polymarket_levels.iter().map(|(_, size)| *size).sum();
    let size = binance_depth.min(polymarket_depth).min(PAPER_MAX_QUANTITY);
    if size <= Decimal::ZERO {
        return None;
    }
    let (binance_cost, binance_limit) = fok_cost_for_quantity(binance_levels, size)?;
    let (polymarket_cost, polymarket_limit) = fok_cost_for_quantity(polymarket_levels, size)?;
    let cost = binance_cost + polymarket_cost;
    let binance_fee = binance_cost * binance_fee_rate;
    let polymarket_fee = polymarket_fee_for_quantity(polymarket_levels, size, polymarket_fee_rate)?;
    let fees = binance_fee + polymarket_fee;
    let second_leg_slippage =
        cost * Decimal::from(PAPER_SECOND_LEG_SLIPPAGE_BPS) / Decimal::from(10_000);
    let stablecoin_conversion =
        size * Decimal::from(PAPER_STABLECOIN_CONVERSION_BPS) / Decimal::from(10_000);
    let capital_cost = cost * Decimal::from(PAPER_CAPITAL_APR_BPS) / Decimal::from(10_000)
        * Decimal::from(remaining_time_ms.max(0))
        / Decimal::from(MILLISECONDS_PER_YEAR);
    let risk_buffers = second_leg_slippage + stablecoin_conversion + capital_cost;
    Some(CrossCalculation {
        cost: cost / size,
        fees: fees / size,
        risk_buffers: risk_buffers / size,
        net_edge: (size - cost - fees - risk_buffers) / size,
        size,
        plan: ShadowPlan {
            first_request: FokOrderRequest {
                quantity: size,
                limit_price: binance_limit,
            },
            second_request: FokOrderRequest {
                quantity: size,
                limit_price: polymarket_limit,
            },
        },
    })
}

fn fok_cost_for_quantity(
    levels: &[(Decimal, Decimal)],
    quantity: Decimal,
) -> Option<(Decimal, Decimal)> {
    let mut remaining = quantity;
    let mut limit_price = None;
    for (price, available) in levels {
        let filled = remaining.min(*available);
        remaining -= filled;
        if filled > Decimal::ZERO {
            limit_price = Some(*price);
        }
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    if remaining > Decimal::ZERO {
        return None;
    }
    let asks: Vec<_> = levels
        .iter()
        .map(|(price, quantity)| PriceLevel {
            price: *price,
            quantity: *quantity,
        })
        .collect();
    let fill = simulate_fok(
        FokOrderRequest {
            quantity,
            limit_price: limit_price?,
        },
        &asks,
    );
    (fill.filled_quantity == quantity).then_some((fill.total_cost, limit_price?))
}

fn polymarket_fee_for_quantity(
    levels: &[(Decimal, Decimal)],
    quantity: Decimal,
    fee_rate: Decimal,
) -> Option<Decimal> {
    let mut remaining = quantity;
    let mut fee = Decimal::ZERO;
    for (price, available) in levels {
        let filled = remaining.min(*available);
        fee += filled * fee_rate * *price * (Decimal::ONE - *price);
        remaining -= filled;
        if remaining <= Decimal::ZERO {
            return Some(fee.round_dp(5));
        }
    }
    None
}

fn is_research_eligible(classification: &str) -> bool {
    matches!(classification, "exact" | "basis")
}

fn is_paper_settlement_aligned(pair: &TrackedPairView) -> bool {
    pair.classification == "exact"
        || (pair.classification == "basis"
            && pair
                .differences
                .iter()
                .all(|difference| difference.starts_with("抵押/兑付币种不同")))
}

fn price(level: Option<(Decimal, Decimal)>) -> Option<String> {
    level.map(|(price, _)| price.to_string())
}

fn positive(value: &Option<String>) -> bool {
    value
        .as_deref()
        .and_then(|value| Decimal::from_str(value).ok())
        .is_some_and(|value| value > Decimal::ZERO)
}

fn paper_trade(
    pair: &TrackedPairView,
    observation: &ObservationView,
    direction: &str,
) -> Option<PaperTradeView> {
    if !paper_rejection_reasons(pair, observation, direction).is_empty() {
        return None;
    }
    let (cost, fees, net_edge, size) = if direction == "A" {
        (
            &observation.direction_a_cost,
            &observation.direction_a_fees,
            &observation.direction_a_net_edge,
            &observation.direction_a_size,
        )
    } else {
        (
            &observation.direction_b_cost,
            &observation.direction_b_fees,
            &observation.direction_b_net_edge,
            &observation.direction_b_size,
        )
    };
    let cost = Decimal::from_str(cost.as_deref()?).ok()?;
    let fees = Decimal::from_str(fees.as_deref()?).ok()?;
    let net_edge = Decimal::from_str(net_edge.as_deref()?).ok()?;
    let size = Decimal::from_str(size.as_deref()?).ok()?;
    let risk_buffers = Decimal::from_str(if direction == "A" {
        observation.direction_a_risk_buffers.as_deref()
    } else {
        observation.direction_b_risk_buffers.as_deref()
    }?)
    .ok()?;
    let quantity = size;
    Some(PaperTradeView {
        id: None,
        slug: pair.slug.clone(),
        asset: pair.asset.clone(),
        duration: pair.duration.clone(),
        direction: direction.into(),
        classification: pair.classification.clone(),
        detected_at_ms: observation.observed_at_ms,
        quantity: quantity.to_string(),
        total_cost: (cost * quantity).to_string(),
        total_fees: (fees * quantity).to_string(),
        total_risk_buffers: (risk_buffers * quantity).to_string(),
        expected_profit: (net_edge * quantity).to_string(),
        actual_payout: None,
        realized_profit: None,
        settled_at_ms: None,
        status: "open".into(),
        fill_model: PAPER_FILL_MODEL.into(),
    })
}

fn paper_rejection_reasons(
    pair: &TrackedPairView,
    observation: &ObservationView,
    direction: &str,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !is_paper_settlement_aligned(pair) {
        reasons.push("结算规则未对齐".into());
    }
    let (net_edge, size, first_leg, second_leg, quote_skew_ms, quote_age_ms) = if direction == "A" {
        (
            &observation.direction_a_net_edge,
            &observation.direction_a_size,
            &observation.binance_up_ask,
            &observation.polymarket_down_ask,
            observation.direction_a_quote_skew_ms,
            observation.direction_a_quote_age_ms,
        )
    } else {
        (
            &observation.direction_b_net_edge,
            &observation.direction_b_size,
            &observation.binance_down_ask,
            &observation.polymarket_up_ask,
            observation.direction_b_quote_skew_ms,
            observation.direction_b_quote_age_ms,
        )
    };
    let decimal = |value: &Option<String>| {
        value
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok())
    };
    match decimal(net_edge) {
        Some(value) if value >= Decimal::new(2, 2) => {}
        Some(_) => reasons.push("净空间低于 2%".into()),
        None => reasons.push("缺少可执行净空间".into()),
    }
    if !decimal(size).is_some_and(|value| value > Decimal::ZERO) {
        reasons.push("缺少双边可成交深度".into());
    }
    if quote_skew_ms.is_none_or(|value| value > PAPER_MAX_QUOTE_SKEW_MS) {
        reasons.push("盘口时间差超过 1 秒".into());
    }
    if quote_age_ms.is_none_or(|value| value > PAPER_MAX_QUOTE_AGE_MS) {
        reasons.push("盘口年龄超过 5 秒".into());
    }
    if observation
        .remaining_time_ms
        .is_none_or(|value| value < PAPER_MIN_REMAINING_MS)
    {
        reasons.push("距结算不足 60 秒".into());
    }
    let reference_is_fresh = [
        observation.binance_reference_at_ms,
        observation.polymarket_reference_at_ms,
    ]
    .into_iter()
    .all(|timestamp| {
        timestamp.is_some_and(|timestamp| {
            observation.observed_at_ms.saturating_sub(timestamp).abs() <= PAPER_MAX_REFERENCE_AGE_MS
        })
    });
    if !reference_is_fresh {
        reasons.push("参考价缺失或超过 10 秒".into());
    }
    let valid_price = |value: &Option<String>| {
        decimal(value)
            .is_some_and(|price| (Decimal::new(5, 2)..=Decimal::new(95, 2)).contains(&price))
    };
    if !valid_price(first_leg) || !valid_price(second_leg) {
        reasons.push("单腿价格超出 0.05 至 0.95".into());
    }
    reasons
}

fn record_qualifying_sample(
    active_signals: &mut HashMap<(String, String), ActiveSignal>,
    key: (String, String),
    observed_at_ms: i64,
) -> bool {
    let active = active_signals.entry(key).or_insert(ActiveSignal {
        since_ms: observed_at_ms,
        last_seen_ms: observed_at_ms,
        samples: 0,
    });
    if observed_at_ms.saturating_sub(active.last_seen_ms) > PAPER_MAX_SIGNAL_GAP_MS {
        active.since_ms = observed_at_ms;
        active.samples = 0;
    }
    active.last_seen_ms = observed_at_ms;
    active.samples = active.samples.saturating_add(1);
    active.samples >= 2 && observed_at_ms.saturating_sub(active.since_ms) >= PAPER_CONFIRMATION_MS
}

fn slug_start_ms(slug: &str) -> Option<i64> {
    slug.rsplit('-')
        .next()?
        .parse::<i64>()
        .ok()
        .map(|value| value * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pair() -> TrackedPairView {
        TrackedPairView {
            slug: "btc-up-or-down-1h-1".into(),
            binance_topic_id: 1,
            title: "BTC".into(),
            asset: "BTC".into(),
            duration: "1h".into(),
            classification: "basis".into(),
            differences: vec!["抵押/兑付币种不同：USDT / USDC".into()],
            binance_source: "BINANCE/BTCUSDT".into(),
            polymarket_source: "https://www.binance.com/en/trade/BTC_USDT".into(),
            start_ms: 0,
            end_ms: 200_000,
            binance_fee_bps: 200,
            polymarket_fee_rate: "0.07".into(),
            liquidity: "1".into(),
            trade_volume: "1".into(),
        }
    }

    #[test]
    fn calculates_cross_fees_with_decimal() {
        let calculation = calculate_cross(
            &[(Decimal::new(69, 2), Decimal::new(224, 0))],
            &[(Decimal::new(26, 2), Decimal::new(300, 0))],
            Decimal::new(2, 2),
            Decimal::new(7, 2),
            0,
        )
        .unwrap();
        assert_eq!(calculation.cost, Decimal::new(95, 2));
        assert_eq!(calculation.fees, Decimal::new(27268, 6));
        assert_eq!(calculation.risk_buffers, Decimal::new(29, 4));
        assert_eq!(calculation.net_edge, Decimal::new(19832, 6));
        assert_eq!(calculation.size, Decimal::TEN);
        assert_eq!(
            calculation.plan.first_request.limit_price,
            Decimal::new(69, 2)
        );
        assert_eq!(
            calculation.plan.second_request.limit_price,
            Decimal::new(26, 2)
        );
    }

    #[test]
    fn consumes_multiple_levels_and_partially_fills_last_level() {
        let calculation = calculate_cross(
            &[
                (Decimal::new(40, 2), Decimal::new(3, 0)),
                (Decimal::new(50, 2), Decimal::new(20, 0)),
            ],
            &[
                (Decimal::new(40, 2), Decimal::new(5, 0)),
                (Decimal::new(45, 2), Decimal::new(20, 0)),
            ],
            Decimal::ZERO,
            Decimal::ZERO,
            0,
        )
        .unwrap();
        assert_eq!(calculation.size, Decimal::TEN);
        assert_eq!(calculation.cost, Decimal::new(895, 3));
        assert_eq!(
            calculation.plan.first_request.limit_price,
            Decimal::new(50, 2)
        );
        assert_eq!(
            calculation.plan.second_request.limit_price,
            Decimal::new(45, 2)
        );
        assert_eq!(calculation.risk_buffers, Decimal::new(279, 5));
        assert_eq!(calculation.net_edge, Decimal::new(10221, 5));
    }

    #[test]
    fn rounds_polymarket_fee_to_five_decimals() {
        assert_eq!(
            polymarket_fee_for_quantity(
                &[(Decimal::new(33, 2), Decimal::ONE)],
                Decimal::ONE,
                Decimal::new(7, 2),
            ),
            Some(Decimal::new(15477, 6).round_dp(5))
        );
    }

    #[test]
    fn calculates_binance_internal_pair_from_full_depth_and_fees() {
        let calculation = calculate_binance_internal(
            &[
                (Decimal::new(40, 2), Decimal::from(4)),
                (Decimal::new(41, 2), Decimal::from(6)),
            ],
            &[
                (Decimal::new(50, 2), Decimal::from(5)),
                (Decimal::new(51, 2), Decimal::from(5)),
            ],
            Decimal::new(2, 2),
            3_600_000,
            120,
            500,
        )
        .unwrap();

        assert_eq!(calculation.size, "10");
        assert_eq!(
            Decimal::from_str(&calculation.cost).unwrap(),
            Decimal::new(911, 3)
        );
        assert_eq!(
            Decimal::from_str(&calculation.fees).unwrap(),
            Decimal::new(1822, 5)
        );
        assert!(Decimal::from_str(&calculation.net_edge).unwrap() > Decimal::ZERO);
        assert!(calculation.rejections.is_empty());
    }

    #[test]
    fn rejects_stale_binance_internal_pair_even_when_price_is_profitable() {
        let calculation = calculate_binance_internal(
            &[(Decimal::new(40, 2), Decimal::TEN)],
            &[(Decimal::new(50, 2), Decimal::TEN)],
            Decimal::new(2, 2),
            3_600_000,
            2_000,
            6_000,
        )
        .unwrap();

        assert!(calculation
            .rejections
            .contains(&"Binance 两盘口时间差超过 1 秒".into()));
        assert!(calculation
            .rejections
            .contains(&"Binance 盘口年龄超过 5 秒".into()));
    }

    #[test]
    fn calculates_polymarket_internal_pair_with_per_level_fees() {
        let calculation = calculate_polymarket_internal(
            &[(Decimal::new(40, 2), Decimal::TEN)],
            &[(Decimal::new(50, 2), Decimal::TEN)],
            Decimal::new(7, 2),
            3_600_000,
            120,
            500,
        )
        .unwrap();

        assert_eq!(calculation.view.size, "10");
        assert_eq!(
            Decimal::from_str(&calculation.view.cost).unwrap(),
            Decimal::new(9, 1)
        );
        assert_eq!(
            Decimal::from_str(&calculation.view.fees).unwrap(),
            Decimal::new(343, 4)
        );
        assert!(Decimal::from_str(&calculation.view.net_edge).unwrap() > Decimal::ZERO);
        assert!(calculation.view.rejections.is_empty());
    }

    #[test]
    fn rejects_stale_polymarket_internal_pair() {
        let calculation = calculate_polymarket_internal(
            &[(Decimal::new(40, 2), Decimal::TEN)],
            &[(Decimal::new(50, 2), Decimal::TEN)],
            Decimal::ZERO,
            3_600_000,
            2_000,
            6_000,
        )
        .unwrap();

        assert!(calculation
            .view
            .rejections
            .contains(&"Polymarket 两盘口时间差超过 1 秒".into()));
        assert!(calculation
            .view
            .rejections
            .contains(&"Polymarket 盘口年龄超过 5 秒".into()));
    }

    #[test]
    fn creates_risk_filtered_paper_trade_capped_at_ten_shares() {
        let pair = TrackedPairView {
            slug: "btc-updown-5m-1".into(),
            binance_topic_id: 1,
            title: "BTC".into(),
            asset: "BTC".into(),
            duration: "5m".into(),
            classification: "basis".into(),
            differences: vec!["抵押/兑付币种不同：USDT / USDC".into()],
            binance_source: "source-a".into(),
            polymarket_source: "source-b".into(),
            start_ms: 0,
            end_ms: 200_000,
            binance_fee_bps: 200,
            polymarket_fee_rate: "0.07".into(),
            liquidity: "1".into(),
            trade_volume: "1".into(),
        };
        let observation = ObservationView {
            slug: pair.slug.clone(),
            observed_at_ms: 100_000,
            binance_up_ask: Some("0.45".into()),
            binance_down_ask: None,
            polymarket_up_ask: None,
            polymarket_down_ask: Some("0.50".into()),
            direction_a_cost: Some("0.95".into()),
            direction_a_fees: Some("0.02".into()),
            direction_a_net_edge: Some("0.03".into()),
            direction_a_risk_buffers: Some("0.001".into()),
            direction_a_size: Some("10".into()),
            direction_a_rejections: vec![],
            direction_b_cost: None,
            direction_b_fees: None,
            direction_b_net_edge: None,
            direction_b_risk_buffers: None,
            direction_b_size: None,
            direction_b_rejections: vec![],
            scan_latency_ms: None,
            direction_a_quote_skew_ms: Some(500),
            direction_b_quote_skew_ms: None,
            direction_a_quote_age_ms: Some(1_000),
            direction_b_quote_age_ms: None,
            remaining_time_ms: Some(100_000),
            binance_reference_price: Some("65000".into()),
            binance_reference_at_ms: Some(99_000),
            polymarket_reference_price: Some("64990".into()),
            polymarket_reference_at_ms: Some(99_000),
            binance_internal: None,
            polymarket_internal: None,
        };
        let trade = paper_trade(&pair, &observation, "A").unwrap();
        assert_eq!(trade.quantity, "10");
        assert_eq!(trade.expected_profit, "0.30");
        assert_eq!(trade.total_risk_buffers, "0.010");
    }

    #[test]
    fn rejects_paper_trade_for_incompatible_pair() {
        let pair = TrackedPairView {
            slug: "btc-updown-1h-1".into(),
            binance_topic_id: 1,
            title: "BTC".into(),
            asset: "BTC".into(),
            duration: "1h".into(),
            classification: "incompatible".into(),
            differences: vec!["事件时间窗口不同".into()],
            binance_source: "source-a".into(),
            polymarket_source: "source-b".into(),
            start_ms: 0,
            end_ms: 1,
            binance_fee_bps: 200,
            polymarket_fee_rate: "0.07".into(),
            liquidity: "1".into(),
            trade_volume: "1".into(),
        };
        let observation = ObservationView {
            slug: pair.slug.clone(),
            observed_at_ms: 1,
            binance_up_ask: Some("0.40".into()),
            binance_down_ask: None,
            polymarket_up_ask: None,
            polymarket_down_ask: Some("0.40".into()),
            direction_a_cost: Some("0.80".into()),
            direction_a_fees: Some("0.02".into()),
            direction_a_net_edge: Some("0.18".into()),
            direction_a_risk_buffers: Some("0.001".into()),
            direction_a_size: Some("100".into()),
            direction_a_rejections: vec![],
            direction_b_cost: None,
            direction_b_fees: None,
            direction_b_net_edge: None,
            direction_b_risk_buffers: None,
            direction_b_size: None,
            direction_b_rejections: vec![],
            scan_latency_ms: None,
            direction_a_quote_skew_ms: None,
            direction_b_quote_skew_ms: None,
            direction_a_quote_age_ms: None,
            direction_b_quote_age_ms: None,
            remaining_time_ms: None,
            binance_reference_price: None,
            binance_reference_at_ms: None,
            polymarket_reference_price: None,
            polymarket_reference_at_ms: None,
            binance_internal: None,
            polymarket_internal: None,
        };

        assert!(paper_trade(&pair, &observation, "A").is_none());
    }

    #[test]
    fn only_paper_trades_settlement_aligned_pairs() {
        let aligned = TrackedPairView {
            classification: "basis".into(),
            differences: vec!["抵押/兑付币种不同：USDT / USDC".into()],
            ..test_pair()
        };
        assert!(is_paper_settlement_aligned(&aligned));

        let cross_oracle = TrackedPairView {
            classification: "basis".into(),
            differences: vec!["价格源 Feed 不同".into()],
            ..test_pair()
        };
        assert!(!is_paper_settlement_aligned(&cross_oracle));
    }

    #[test]
    fn reports_actionable_paper_rejection_reasons() {
        let pair = TrackedPairView {
            classification: "basis".into(),
            differences: vec!["价格源 Feed 不同".into()],
            ..test_pair()
        };
        let observation = ObservationView {
            slug: pair.slug.clone(),
            observed_at_ms: 100_000,
            binance_up_ask: Some("0.45".into()),
            binance_down_ask: None,
            polymarket_up_ask: None,
            polymarket_down_ask: Some("0.50".into()),
            direction_a_cost: Some("0.95".into()),
            direction_a_fees: Some("0.02".into()),
            direction_a_net_edge: Some("0.01".into()),
            direction_a_risk_buffers: Some("0.005".into()),
            direction_a_size: Some("10".into()),
            direction_a_rejections: vec![],
            direction_b_cost: None,
            direction_b_fees: None,
            direction_b_net_edge: None,
            direction_b_risk_buffers: None,
            direction_b_size: None,
            direction_b_rejections: vec![],
            scan_latency_ms: None,
            direction_a_quote_skew_ms: Some(1_500),
            direction_b_quote_skew_ms: None,
            direction_a_quote_age_ms: Some(1_000),
            direction_b_quote_age_ms: None,
            remaining_time_ms: Some(100_000),
            binance_reference_price: Some("65000".into()),
            binance_reference_at_ms: Some(99_000),
            polymarket_reference_price: Some("64990".into()),
            polymarket_reference_at_ms: Some(99_000),
            binance_internal: None,
            polymarket_internal: None,
        };
        let reasons = paper_rejection_reasons(&pair, &observation, "A");
        assert!(reasons.contains(&"结算规则未对齐".into()));
        assert!(reasons.contains(&"净空间低于 2%".into()));
        assert!(reasons.contains(&"盘口时间差超过 1 秒".into()));
    }

    #[test]
    fn confirms_signal_across_normal_scan_interval_and_resets_after_gap() {
        let key = ("btc-window".to_string(), "A".to_string());
        let mut active = HashMap::new();

        assert!(!record_qualifying_sample(&mut active, key.clone(), 1_000));
        assert!(record_qualifying_sample(&mut active, key.clone(), 7_000));

        active.remove(&key);
        assert!(!record_qualifying_sample(&mut active, key.clone(), 1_000));
        assert!(!record_qualifying_sample(&mut active, key.clone(), 17_000));
        assert!(record_qualifying_sample(&mut active, key, 23_000));
    }

    #[test]
    fn derives_binance_outcome_from_decimal_prices() {
        assert_eq!(
            binance_outcome(Some("100.01"), Some("100.02")),
            Some("Up".into())
        );
        assert_eq!(
            binance_outcome(Some("100.01"), Some("99.99")),
            Some("Down".into())
        );
        assert_eq!(
            binance_outcome(Some("100.010"), Some("100.01")),
            Some("Split".into())
        );
        assert_eq!(binance_outcome(Some("100"), None), None);
    }

    #[test]
    fn calculates_quote_timestamp_skew_in_milliseconds() {
        assert_eq!(quote_skew_ms(1_000, "1250"), Some(250));
        assert_eq!(quote_skew_ms(1_250, "1000"), Some(250));
        assert_eq!(quote_skew_ms(1_000, "invalid"), None);
    }

    #[test]
    fn normalizes_rtds_timestamp_units_to_milliseconds() {
        assert_eq!(rtds_timestamp_ms(1_789_460_900), Some(1_789_460_900_000));
        assert_eq!(
            rtds_timestamp_ms(1_789_460_900_123),
            Some(1_789_460_900_123)
        );
        assert_eq!(rtds_timestamp_ms(0), None);
    }

    #[test]
    fn parses_polymarket_rtds_e18_price_without_float() {
        assert_eq!(
            rtds_fixed_point_price("65000500000000000000000"),
            Some(Decimal::new(650005, 1))
        );
        assert_eq!(rtds_fixed_point_price("invalid"), None);
    }
}
