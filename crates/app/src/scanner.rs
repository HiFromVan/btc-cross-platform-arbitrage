use chrono::{DateTime, Utc};
use domain::{PairClassification, ResolutionSpec};
use exchange_binance::{BinanceClient, BinanceMarketDetail, BinanceMarketTopic};
use exchange_polymarket::PolymarketClient;
use futures_util::{SinkExt, StreamExt};
use matcher::classify_candidate_pair;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
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
    pub direction_a_size: Option<String>,
    pub direction_b_cost: Option<String>,
    pub direction_b_fees: Option<String>,
    pub direction_b_net_edge: Option<String>,
    pub direction_b_size: Option<String>,
    pub scan_latency_ms: Option<i64>,
    pub direction_a_quote_skew_ms: Option<i64>,
    pub direction_b_quote_skew_ms: Option<i64>,
    pub remaining_time_ms: Option<i64>,
    pub binance_reference_price: Option<String>,
    pub binance_reference_at_ms: Option<i64>,
    pub polymarket_reference_price: Option<String>,
    pub polymarket_reference_at_ms: Option<i64>,
}

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
}

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
                        Ok(mut observation) => {
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
                                    if !paper_executed.contains(&key) {
                                        if let Some(trade) = paper_trade(
                                            &dashboard.pairs[index].pair,
                                            &observation,
                                            direction,
                                        ) {
                                            match storage.insert_paper_trade(&trade) {
                                                Ok(()) => {
                                                    paper_executed.insert(key);
                                                }
                                                Err(error) => {
                                                    dashboard.pairs[index].error =
                                                        Some(format!("模拟交易写入失败：{error}"));
                                                }
                                            }
                                        }
                                    }
                                }
                                dashboard.pairs[index].latest = Some(observation);
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
    loop {
        match connect_async("wss://ws-live-data.polymarket.com").await {
            Ok((stream, _)) => {
                let (mut writer, mut reader) = stream.split();
                let subscription = serde_json::json!({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_twap_sixty",
                        "type": "update"
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
                            let Some(asset) = event.payload.symbol.split('/').next().map(str::to_ascii_uppercase) else { continue };
                            if !matches!(asset.as_str(), "BTC" | "ETH" | "BNB") { continue }
                            let Some(value) = rtds_fixed_point_price(&event.payload.full_accuracy_value) else { continue };
                            let mut dashboard = state.write().await;
                            let entry = dashboard.reference_prices.entry(asset).or_default();
                            entry.polymarket_twap_60s = Some(value.normalize().to_string());
                            entry.polymarket_observed_at_ms = Some(event.payload.timestamp);
                            update_reference_difference(entry);
                        }
                    }
                }
            }
            Err(error) => warn!(%error, "Polymarket RTDS 连接失败"),
        }
        sleep(Duration::from_secs(3)).await;
    }
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
    let Some(pm_market) = event.markets.into_iter().next() else {
        return Ok(None);
    };
    let (polymarket_up_token, polymarket_down_token) = pm_market
        .outcome_tokens()
        .map_err(|error| error.to_string())?;
    let asset = detail
        .symbol
        .strip_suffix("USDT")
        .unwrap_or(&detail.symbol)
        .to_string();
    let binance_spec = ResolutionSpec {
        asset: asset.clone(),
        quote_currency: "USDT".into(),
        price_feed_provider: detail.variant_data.price_feed_provider.clone(),
        price_feed_id: detail.variant_data.price_feed_id.clone(),
        calculation: "top_of_book_5m_candle_close".into(),
        up_comparison: "greater_than".into(),
        equal_rule: "split_50_50".into(),
        fallback_rule: detail.description.as_ref().and_then(|description| {
            description
                .contains("consensus of reliable sources")
                .then(|| "source_consensus".into())
        }),
        collateral: detail.collateral.clone(),
    };
    let polymarket_spec = ResolutionSpec {
        asset: asset.clone(),
        quote_currency: "USD".into(),
        price_feed_provider: "CHAINLINK".into(),
        price_feed_id: Some(pm_market.resolution_source.clone()),
        calculation: "twap_60s".into(),
        up_comparison: "greater_than_or_equal".into(),
        equal_rule: "up".into(),
        fallback_rule: None,
        collateral: "USDC".into(),
    };
    let pm_end_ms = DateTime::parse_from_rfc3339(&event.end_date)
        .map(|value| value.timestamp_millis())
        .unwrap_or_default();
    let assessment = classify_candidate_pair(
        slug_start_ms(&topic.slug) == Some(detail.start_date),
        pm_end_ms == detail.end_date,
        &binance_spec,
        &polymarket_spec,
    );
    let duration_minutes = (detail.end_date - detail.start_date) / 60_000;
    let duration = if duration_minutes < 60 {
        format!("{duration_minutes}m")
    } else if duration_minutes % 1440 == 0 {
        format!("{}d", duration_minutes / 1440)
    } else {
        format!("{}h", duration_minutes / 60)
    };
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
            liquidity: topic.liquidity,
            trade_volume: topic.trade_volume,
        },
        binance: detail,
        polymarket_up_token,
        polymarket_down_token,
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
) -> Result<ObservationView, String> {
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
    let bin_up = bin_up.best_ask().map_err(|error| error.to_string())?;
    let bin_down = bin_down.best_ask().map_err(|error| error.to_string())?;
    let pm_up = pm_up.best_ask().map_err(|error| error.to_string())?;
    let pm_down = pm_down.best_ask().map_err(|error| error.to_string())?;
    let (direction_a, direction_b) = if is_research_eligible(&pair.view.classification) {
        let fee_rate = Decimal::from(pair.binance.fee_rate_bps) / Decimal::from(10_000);
        (
            calculate_cross(bin_up, pm_down, fee_rate),
            calculate_cross(bin_down, pm_up, fee_rate),
        )
    } else {
        (None, None)
    };
    let observed_at_ms = Utc::now().timestamp_millis();
    Ok(ObservationView {
        slug: pair.view.slug.clone(),
        observed_at_ms,
        binance_up_ask: price(bin_up),
        binance_down_ask: price(bin_down),
        polymarket_up_ask: price(pm_up),
        polymarket_down_ask: price(pm_down),
        direction_a_cost: direction_a.as_ref().map(|value| value.cost.to_string()),
        direction_a_fees: direction_a.as_ref().map(|value| value.fees.to_string()),
        direction_a_net_edge: direction_a.as_ref().map(|value| value.net_edge.to_string()),
        direction_a_size: direction_a.as_ref().map(|value| value.size.to_string()),
        direction_b_cost: direction_b.as_ref().map(|value| value.cost.to_string()),
        direction_b_fees: direction_b.as_ref().map(|value| value.fees.to_string()),
        direction_b_net_edge: direction_b.as_ref().map(|value| value.net_edge.to_string()),
        direction_b_size: direction_b.as_ref().map(|value| value.size.to_string()),
        scan_latency_ms: Some(scan_started.elapsed().as_millis().min(i64::MAX as u128) as i64),
        direction_a_quote_skew_ms,
        direction_b_quote_skew_ms,
        remaining_time_ms: Some(pair.view.end_ms.saturating_sub(observed_at_ms)),
        binance_reference_price: None,
        binance_reference_at_ms: None,
        polymarket_reference_price: None,
        polymarket_reference_at_ms: None,
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

struct CrossCalculation {
    cost: Decimal,
    fees: Decimal,
    net_edge: Decimal,
    size: Decimal,
}

fn calculate_cross(
    binance_leg: Option<(Decimal, Decimal)>,
    polymarket_leg: Option<(Decimal, Decimal)>,
    binance_fee_rate: Decimal,
) -> Option<CrossCalculation> {
    let ((binance_price, binance_size), (pm_price, pm_size)) = (binance_leg?, polymarket_leg?);
    let cost = binance_price + pm_price;
    let binance_fee = binance_price * binance_fee_rate;
    let polymarket_fee = Decimal::new(7, 2) * pm_price * (Decimal::ONE - pm_price);
    let fees = binance_fee + polymarket_fee;
    Some(CrossCalculation {
        cost,
        fees,
        net_edge: Decimal::ONE - cost - fees,
        size: binance_size.min(pm_size),
    })
}

fn is_research_eligible(classification: &str) -> bool {
    matches!(classification, "exact" | "basis")
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
    if !is_research_eligible(&pair.classification) {
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
    let threshold = Decimal::new(5, 3);
    if net_edge < threshold || size <= Decimal::ZERO {
        return None;
    }
    let quantity = size.min(Decimal::from(100));
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
        expected_profit: (net_edge * quantity).to_string(),
        actual_payout: None,
        realized_profit: None,
        settled_at_ms: None,
        status: "open".into(),
        fill_model: "best_ask_instant_full_fill".into(),
    })
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

    #[test]
    fn calculates_cross_fees_with_decimal() {
        let calculation = calculate_cross(
            Some((Decimal::new(69, 2), Decimal::new(224, 0))),
            Some((Decimal::new(26, 2), Decimal::new(300, 0))),
            Decimal::new(2, 2),
        )
        .unwrap();
        assert_eq!(calculation.cost, Decimal::new(95, 2));
        assert_eq!(calculation.fees, Decimal::new(27268, 6));
        assert_eq!(calculation.net_edge, Decimal::new(22732, 6));
        assert_eq!(calculation.size, Decimal::new(224, 0));
    }

    #[test]
    fn creates_at_most_one_hundred_share_paper_trade() {
        let pair = TrackedPairView {
            slug: "btc-updown-5m-1".into(),
            binance_topic_id: 1,
            title: "BTC".into(),
            asset: "BTC".into(),
            duration: "5m".into(),
            classification: "basis".into(),
            differences: vec![],
            binance_source: "source-a".into(),
            polymarket_source: "source-b".into(),
            start_ms: 0,
            end_ms: 1,
            binance_fee_bps: 200,
            liquidity: "1".into(),
            trade_volume: "1".into(),
        };
        let observation = ObservationView {
            slug: pair.slug.clone(),
            observed_at_ms: 1,
            binance_up_ask: None,
            binance_down_ask: None,
            polymarket_up_ask: None,
            polymarket_down_ask: None,
            direction_a_cost: Some("0.95".into()),
            direction_a_fees: Some("0.02".into()),
            direction_a_net_edge: Some("0.03".into()),
            direction_a_size: Some("224".into()),
            direction_b_cost: None,
            direction_b_fees: None,
            direction_b_net_edge: None,
            direction_b_size: None,
            scan_latency_ms: None,
            direction_a_quote_skew_ms: None,
            direction_b_quote_skew_ms: None,
            remaining_time_ms: None,
            binance_reference_price: None,
            binance_reference_at_ms: None,
            polymarket_reference_price: None,
            polymarket_reference_at_ms: None,
        };
        let trade = paper_trade(&pair, &observation, "A").unwrap();
        assert_eq!(trade.quantity, "100");
        assert_eq!(trade.expected_profit, "3.00");
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
            direction_a_size: Some("100".into()),
            direction_b_cost: None,
            direction_b_fees: None,
            direction_b_net_edge: None,
            direction_b_size: None,
            scan_latency_ms: None,
            direction_a_quote_skew_ms: None,
            direction_b_quote_skew_ms: None,
            remaining_time_ms: None,
            binance_reference_price: None,
            binance_reference_at_ms: None,
            polymarket_reference_price: None,
            polymarket_reference_at_ms: None,
        };

        assert!(paper_trade(&pair, &observation, "A").is_none());
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
    fn parses_polymarket_rtds_e18_price_without_float() {
        assert_eq!(
            rtds_fixed_point_price("65000500000000000000000"),
            Some(Decimal::new(650005, 1))
        );
        assert_eq!(rtds_fixed_point_price("invalid"), None);
    }
}
