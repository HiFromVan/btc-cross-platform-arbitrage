use chrono::Utc;
use exchange_binance::BinanceClient;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, Instant};
use tracing::warn;

use crate::storage::Storage;

const SYMBOLS: [&str; 4] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];
const MAX_NOTIONAL: Decimal = Decimal::from_parts(1_000, 0, 0, false, 0);
const SPOT_ROUND_TRIP_TAKER_BPS: i64 = 20;
const FUTURES_ROUND_TRIP_TAKER_BPS: i64 = 10;
const ROUND_TRIP_SLIPPAGE_BPS: i64 = 10;
const DEFAULT_FUNDING_INTERVAL_HOURS: i64 = 8;
const MAX_RESPONSE_SKEW_MS: i64 = 1_000;
const MAX_PREMIUM_AGE_MS: i64 = 5_000;
const CAPITAL_APR_BPS: i64 = 1_000;
const DELIVERY_SETTLEMENT_BUFFER_BPS: i64 = 2;
const MILLISECONDS_PER_YEAR: i64 = 365 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceBasisView {
    pub symbol: String,
    pub observed_at_ms: i64,
    pub quantity: String,
    pub spot_vwap_ask: String,
    pub perp_vwap_bid: String,
    pub spot_cost: String,
    pub perp_proceeds: String,
    pub entry_basis: String,
    pub entry_basis_rate: String,
    pub estimated_round_trip_fees: String,
    pub slippage_buffer: String,
    pub funding_rate: String,
    pub funding_interval_hours: i64,
    pub funding_income_per_round: String,
    pub projected_net_24h: String,
    pub projected_net_7d: String,
    pub funding_apr: String,
    pub break_even_rounds: Option<String>,
    pub mark_price: String,
    pub index_price: String,
    pub next_funding_time_ms: i64,
    pub response_skew_ms: i64,
    pub premium_age_ms: i64,
    pub scan_latency_ms: i64,
    pub rejections: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceDeliveryBasisView {
    pub symbol: String,
    pub pair: String,
    pub contract_type: String,
    pub delivery_time_ms: i64,
    pub observed_at_ms: i64,
    pub remaining_days: String,
    pub quantity: String,
    pub quantity_step: String,
    pub spot_price_tick: String,
    pub delivery_price_tick: String,
    pub spot_vwap_ask: String,
    pub delivery_vwap_bid: String,
    pub spot_cost: String,
    pub delivery_proceeds: String,
    pub gross_basis: String,
    pub gross_basis_rate: String,
    pub estimated_round_trip_fees: String,
    pub slippage_buffer: String,
    pub settlement_buffer: String,
    pub capital_cost: String,
    pub pre_capital_net_profit: String,
    pub net_profit_at_delivery: String,
    pub net_return: String,
    pub net_apr: String,
    pub mark_price: String,
    pub index_price: String,
    pub response_skew_ms: i64,
    pub premium_age_ms: i64,
    pub scan_latency_ms: i64,
    pub rejections: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceDeliveryRequoteView {
    pub symbol: String,
    pub quoted_at_ms: i64,
    pub checked_at_ms: i64,
    pub delay_ms: i64,
    pub quantity: String,
    pub spot_limit_price: String,
    pub delivery_limit_price: String,
    pub spot_fillable: bool,
    pub delivery_fillable: bool,
    pub spot_cost: Option<String>,
    pub delivery_proceeds: Option<String>,
    pub gross_basis: Option<String>,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Clone)]
struct DeliveryContract {
    symbol: String,
    pair: String,
    contract_type: String,
    delivery_date: i64,
    quantity_step: Decimal,
    minimum_quantity: Decimal,
    spot_price_tick: Decimal,
    delivery_price_tick: Decimal,
}

#[derive(Clone, Copy)]
struct DeliveryPlan {
    quantity: Decimal,
    spot_limit_price: Decimal,
    delivery_limit_price: Decimal,
}

struct DeliveryScanResult {
    observation: BinanceDeliveryBasisView,
    plan: DeliveryPlan,
}

struct Timed<T> {
    value: T,
    completed_at_ms: i64,
}

pub async fn run(client: BinanceClient, storage: Storage) {
    let mut funding_intervals = HashMap::new();
    let mut next_funding_info_refresh = Instant::now();
    loop {
        if Instant::now() >= next_funding_info_refresh {
            match client.usdt_futures_funding_info().await {
                Ok(values) => {
                    funding_intervals = values
                        .into_iter()
                        .map(|value| (value.symbol, value.funding_interval_hours))
                        .collect();
                }
                Err(error) => warn!(%error, "读取 Binance 资金费周期失败，保留默认或上次结果"),
            }
            next_funding_info_refresh = Instant::now() + Duration::from_secs(60);
        }
        let mut tasks = JoinSet::new();
        for symbol in SYMBOLS {
            let client = client.clone();
            let funding_interval_hours = funding_intervals
                .get(symbol)
                .copied()
                .unwrap_or(DEFAULT_FUNDING_INTERVAL_HOURS);
            tasks.spawn(async move { scan_symbol(&client, symbol, funding_interval_hours).await });
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(observation)) => {
                    if let Err(error) = storage.insert_binance_basis(&observation) {
                        warn!(%error, symbol = observation.symbol, "保存 Binance 基差数据失败");
                    }
                }
                Ok(Err(error)) => warn!(%error, "Binance 基差扫描失败"),
                Err(error) => warn!(%error, "Binance 基差扫描任务失败"),
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
}

pub async fn run_delivery(client: BinanceClient, storage: Storage) {
    let mut contracts = Vec::new();
    let mut last_requote_by_symbol: HashMap<String, i64> = HashMap::new();
    let mut next_discovery = Instant::now();
    loop {
        if Instant::now() >= next_discovery {
            match tokio::try_join!(
                client.usdt_futures_exchange_info(),
                client.spot_exchange_info()
            ) {
                Ok((futures, spot)) => {
                    let spot_rules: HashMap<_, _> = spot
                        .symbols
                        .into_iter()
                        .filter(|value| value.status == "TRADING")
                        .filter_map(|value| {
                            let lot = value
                                .filters
                                .iter()
                                .find(|filter| filter.filter_type == "LOT_SIZE")?;
                            let price = value
                                .filters
                                .iter()
                                .find(|filter| filter.filter_type == "PRICE_FILTER")?;
                            Some((
                                value.symbol,
                                (
                                    lot.decimal_step_size()?,
                                    lot.decimal_min_qty()?,
                                    price.decimal_tick_size()?,
                                ),
                            ))
                        })
                        .collect();
                    contracts = futures
                        .symbols
                        .into_iter()
                        .filter(|value| {
                            value.status == "TRADING"
                                && value.contract_type != "PERPETUAL"
                                && matches!(
                                    value.pair.as_str(),
                                    "BTCUSDT" | "ETHUSDT" | "SOLUSDT" | "XRPUSDT"
                                )
                                && value.delivery_date > Utc::now().timestamp_millis()
                        })
                        .filter_map(|value| {
                            let (spot_step, spot_minimum, spot_tick) =
                                spot_rules.get(&value.pair).copied()?;
                            let lot = value
                                .filters
                                .iter()
                                .find(|filter| filter.filter_type == "LOT_SIZE")?;
                            let price = value
                                .filters
                                .iter()
                                .find(|filter| filter.filter_type == "PRICE_FILTER")?;
                            let futures_step = lot.decimal_step_size()?;
                            let futures_minimum = lot.decimal_min_qty()?;
                            Some(DeliveryContract {
                                symbol: value.symbol,
                                pair: value.pair,
                                contract_type: value.contract_type,
                                delivery_date: value.delivery_date,
                                quantity_step: spot_step.max(futures_step),
                                minimum_quantity: spot_minimum.max(futures_minimum),
                                spot_price_tick: spot_tick,
                                delivery_price_tick: price.decimal_tick_size()?,
                            })
                        })
                        .collect();
                }
                Err(error) => warn!(%error, "发现 Binance USDT 交割合约失败"),
            }
            next_discovery = Instant::now() + Duration::from_secs(60);
        }
        let mut tasks = JoinSet::new();
        for contract in contracts.clone() {
            let client = client.clone();
            tasks.spawn(async move { scan_delivery(&client, &contract).await });
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(result)) => {
                    let observation = result.observation;
                    if let Err(error) = storage.insert_binance_delivery_basis(&observation) {
                        warn!(%error, symbol = observation.symbol, "保存 Binance 交割基差失败");
                    }
                    let pre_capital_positive =
                        Decimal::from_str(&observation.pre_capital_net_profit)
                            .is_ok_and(|value| value > Decimal::ZERO);
                    let last_requote = last_requote_by_symbol
                        .get(&observation.symbol)
                        .copied()
                        .unwrap_or_default();
                    if pre_capital_positive
                        && observation.observed_at_ms.saturating_sub(last_requote) >= 3_600_000
                    {
                        last_requote_by_symbol
                            .insert(observation.symbol.clone(), observation.observed_at_ms);
                        if let Some(contract) = contracts
                            .iter()
                            .find(|contract| contract.symbol == observation.symbol)
                            .cloned()
                        {
                            let client = client.clone();
                            let storage = storage.clone();
                            let quoted_at_ms = observation.observed_at_ms;
                            tokio::spawn(async move {
                                run_delivery_requotes(
                                    client,
                                    storage,
                                    contract,
                                    quoted_at_ms,
                                    result.plan,
                                )
                                .await;
                            });
                        }
                    }
                }
                Ok(Err(error)) => warn!(%error, "Binance 交割基差扫描失败"),
                Err(error) => warn!(%error, "Binance 交割基差任务失败"),
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn scan_delivery(
    client: &BinanceClient,
    contract: &DeliveryContract,
) -> Result<DeliveryScanResult, String> {
    let started = Instant::now();
    let spot = async {
        let value = client
            .spot_depth(&contract.pair, 100)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let delivery = async {
        let value = client
            .usdt_futures_depth(&contract.symbol, 100)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let premium = async {
        let value = client
            .usdt_delivery_premium_index(&contract.symbol)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let (spot, delivery, premium) = tokio::try_join!(spot, delivery, premium)?;
    let observed_at_ms = Utc::now().timestamp_millis();
    let observation = calculate_delivery_basis(
        &contract.symbol,
        &contract.pair,
        &contract.contract_type,
        contract.delivery_date,
        observed_at_ms,
        contract.quantity_step,
        contract.minimum_quantity,
        contract.spot_price_tick,
        contract.delivery_price_tick,
        &spot.value.ask_levels().map_err(|error| error.to_string())?,
        &delivery
            .value
            .bid_levels()
            .map_err(|error| error.to_string())?,
        Decimal::from_str(&premium.value.mark_price).map_err(|_| "交割标记价无效")?,
        Decimal::from_str(&premium.value.index_price).map_err(|_| "交割指数价无效")?,
        spot.completed_at_ms.abs_diff(delivery.completed_at_ms) as i64,
        observed_at_ms.abs_diff(premium.value.time) as i64,
        started.elapsed().as_millis().min(i64::MAX as u128) as i64,
    )
    .ok_or_else(|| format!("{} 交割订单簿深度不足或已到期", contract.symbol))?;
    let quantity = Decimal::from_str(&observation.quantity).map_err(|_| "模拟数量无效")?;
    let spot_levels = spot.value.ask_levels().map_err(|error| error.to_string())?;
    let delivery_levels = delivery
        .value
        .bid_levels()
        .map_err(|error| error.to_string())?;
    Ok(DeliveryScanResult {
        plan: DeliveryPlan {
            quantity,
            spot_limit_price: limit_price_for_quantity(&spot_levels, quantity)?,
            delivery_limit_price: limit_price_for_quantity(&delivery_levels, quantity)?,
        },
        observation,
    })
}

async fn run_delivery_requotes(
    client: BinanceClient,
    storage: Storage,
    contract: DeliveryContract,
    quoted_at_ms: i64,
    plan: DeliveryPlan,
) {
    let mut previous_delay = 0_i64;
    for delay_ms in [250_i64, 1_000, 5_000] {
        sleep(Duration::from_millis((delay_ms - previous_delay) as u64)).await;
        previous_delay = delay_ms;
        let checked_at_ms = Utc::now().timestamp_millis();
        let books = tokio::try_join!(
            client.spot_depth(&contract.pair, 100),
            client.usdt_futures_depth(&contract.symbol, 100)
        );
        let record = match books {
            Ok((spot, delivery)) => {
                let spot_levels = spot.ask_levels().map_err(|error| error.to_string());
                let delivery_levels = delivery.bid_levels().map_err(|error| error.to_string());
                match (spot_levels, delivery_levels) {
                    (Ok(spot_levels), Ok(delivery_levels)) => {
                        let spot_cost = consume_depth_at_limit(
                            &spot_levels,
                            plan.quantity,
                            plan.spot_limit_price,
                            true,
                        );
                        let delivery_proceeds = consume_depth_at_limit(
                            &delivery_levels,
                            plan.quantity,
                            plan.delivery_limit_price,
                            false,
                        );
                        let gross_basis = spot_cost
                            .zip(delivery_proceeds)
                            .map(|(spot, delivery)| delivery - spot);
                        BinanceDeliveryRequoteView {
                            symbol: contract.symbol.clone(),
                            quoted_at_ms,
                            checked_at_ms,
                            delay_ms,
                            quantity: plan.quantity.normalize().to_string(),
                            spot_limit_price: plan.spot_limit_price.normalize().to_string(),
                            delivery_limit_price: plan.delivery_limit_price.normalize().to_string(),
                            spot_fillable: spot_cost.is_some(),
                            delivery_fillable: delivery_proceeds.is_some(),
                            spot_cost: spot_cost.map(|value| value.normalize().to_string()),
                            delivery_proceeds: delivery_proceeds
                                .map(|value| value.normalize().to_string()),
                            gross_basis: gross_basis.map(|value| value.normalize().to_string()),
                            status: if gross_basis.is_some() {
                                "both_fillable"
                            } else {
                                "cancelled"
                            }
                            .into(),
                            error: None,
                        }
                    }
                    (spot, delivery) => BinanceDeliveryRequoteView {
                        symbol: contract.symbol.clone(),
                        quoted_at_ms,
                        checked_at_ms,
                        delay_ms,
                        quantity: plan.quantity.normalize().to_string(),
                        spot_limit_price: plan.spot_limit_price.normalize().to_string(),
                        delivery_limit_price: plan.delivery_limit_price.normalize().to_string(),
                        spot_fillable: false,
                        delivery_fillable: false,
                        spot_cost: None,
                        delivery_proceeds: None,
                        gross_basis: None,
                        status: "error".into(),
                        error: Some(
                            spot.err()
                                .or_else(|| delivery.err())
                                .unwrap_or_else(|| "订单簿解析失败".into()),
                        ),
                    },
                }
            }
            Err(error) => BinanceDeliveryRequoteView {
                symbol: contract.symbol.clone(),
                quoted_at_ms,
                checked_at_ms,
                delay_ms,
                quantity: plan.quantity.normalize().to_string(),
                spot_limit_price: plan.spot_limit_price.normalize().to_string(),
                delivery_limit_price: plan.delivery_limit_price.normalize().to_string(),
                spot_fillable: false,
                delivery_fillable: false,
                spot_cost: None,
                delivery_proceeds: None,
                gross_basis: None,
                status: "error".into(),
                error: Some(error.to_string()),
            },
        };
        if let Err(error) = storage.insert_binance_delivery_requote(&record) {
            warn!(%error, symbol = record.symbol, delay_ms, "保存交割基差延迟重报价失败");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn calculate_delivery_basis(
    symbol: &str,
    pair: &str,
    contract_type: &str,
    delivery_time_ms: i64,
    observed_at_ms: i64,
    quantity_step: Decimal,
    minimum_quantity: Decimal,
    spot_price_tick: Decimal,
    delivery_price_tick: Decimal,
    spot_asks: &[(Decimal, Decimal)],
    delivery_bids: &[(Decimal, Decimal)],
    mark_price: Decimal,
    index_price: Decimal,
    response_skew_ms: i64,
    premium_age_ms: i64,
    scan_latency_ms: i64,
) -> Option<BinanceDeliveryBasisView> {
    let remaining_ms = delivery_time_ms.checked_sub(observed_at_ms)?;
    if remaining_ms <= 0 {
        return None;
    }
    let spot_best_ask = spot_asks.first()?.0;
    let spot_depth: Decimal = spot_asks.iter().map(|(_, quantity)| *quantity).sum();
    let delivery_depth: Decimal = delivery_bids.iter().map(|(_, quantity)| *quantity).sum();
    let raw_quantity = (MAX_NOTIONAL / spot_best_ask)
        .min(spot_depth)
        .min(delivery_depth);
    if quantity_step <= Decimal::ZERO {
        return None;
    }
    let quantity = (raw_quantity / quantity_step).floor() * quantity_step;
    if quantity < minimum_quantity {
        return None;
    }
    let spot_cost = consume_depth(spot_asks, quantity)?;
    let delivery_proceeds = consume_depth(delivery_bids, quantity)?;
    let spot_vwap = spot_cost / quantity;
    let delivery_vwap = delivery_proceeds / quantity;
    let gross_basis = delivery_proceeds - spot_cost;
    let gross_basis_rate = gross_basis / spot_cost;
    let estimated_round_trip_fees = (spot_cost * Decimal::from(SPOT_ROUND_TRIP_TAKER_BPS)
        + delivery_proceeds * Decimal::from(FUTURES_ROUND_TRIP_TAKER_BPS))
        / Decimal::from(10_000);
    let slippage_buffer = (spot_cost + delivery_proceeds) * Decimal::from(ROUND_TRIP_SLIPPAGE_BPS)
        / Decimal::from(10_000);
    let settlement_buffer =
        delivery_proceeds * Decimal::from(DELIVERY_SETTLEMENT_BUFFER_BPS) / Decimal::from(10_000);
    let capital_cost = spot_cost * Decimal::from(CAPITAL_APR_BPS) / Decimal::from(10_000)
        * Decimal::from(remaining_ms)
        / Decimal::from(MILLISECONDS_PER_YEAR);
    let pre_capital_net_profit =
        gross_basis - estimated_round_trip_fees - slippage_buffer - settlement_buffer;
    let net_profit = pre_capital_net_profit - capital_cost;
    let net_return = net_profit / spot_cost;
    let milliseconds_per_year = Decimal::from(365_i64 * 24 * 60 * 60 * 1_000);
    let net_apr = net_return * milliseconds_per_year / Decimal::from(remaining_ms);
    let remaining_days = Decimal::from(remaining_ms) / Decimal::from(24 * 60 * 60 * 1_000_i64);
    let mut rejections = Vec::new();
    if gross_basis <= Decimal::ZERO {
        rejections.push("交割合约未高于现货买入成本".into());
    }
    if net_profit <= Decimal::ZERO {
        rejections.push("扣除费用、缓冲和资金占用后无正净利润".into());
    }
    if response_skew_ms > MAX_RESPONSE_SKEW_MS {
        rejections.push("现货与交割合约接口响应完成时差超过 1 秒".into());
    }
    if premium_age_ms > MAX_PREMIUM_AGE_MS {
        rejections.push("交割合约标记价数据超过 5 秒".into());
    }
    Some(BinanceDeliveryBasisView {
        symbol: symbol.into(),
        pair: pair.into(),
        contract_type: contract_type.into(),
        delivery_time_ms,
        observed_at_ms,
        remaining_days: remaining_days.normalize().to_string(),
        quantity: quantity.normalize().to_string(),
        quantity_step: quantity_step.normalize().to_string(),
        spot_price_tick: spot_price_tick.normalize().to_string(),
        delivery_price_tick: delivery_price_tick.normalize().to_string(),
        spot_vwap_ask: spot_vwap.normalize().to_string(),
        delivery_vwap_bid: delivery_vwap.normalize().to_string(),
        spot_cost: spot_cost.normalize().to_string(),
        delivery_proceeds: delivery_proceeds.normalize().to_string(),
        gross_basis: gross_basis.normalize().to_string(),
        gross_basis_rate: gross_basis_rate.normalize().to_string(),
        estimated_round_trip_fees: estimated_round_trip_fees.normalize().to_string(),
        slippage_buffer: slippage_buffer.normalize().to_string(),
        settlement_buffer: settlement_buffer.normalize().to_string(),
        capital_cost: capital_cost.normalize().to_string(),
        pre_capital_net_profit: pre_capital_net_profit.normalize().to_string(),
        net_profit_at_delivery: net_profit.normalize().to_string(),
        net_return: net_return.normalize().to_string(),
        net_apr: net_apr.normalize().to_string(),
        mark_price: mark_price.normalize().to_string(),
        index_price: index_price.normalize().to_string(),
        response_skew_ms,
        premium_age_ms,
        scan_latency_ms,
        rejections,
    })
}

async fn scan_symbol(
    client: &BinanceClient,
    symbol: &str,
    funding_interval_hours: i64,
) -> Result<BinanceBasisView, String> {
    let started = Instant::now();
    let spot = async {
        let value = client
            .spot_depth(symbol, 100)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let perp = async {
        let value = client
            .usdt_futures_depth(symbol, 100)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let premium = async {
        let value = client
            .usdt_futures_premium_index(symbol)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(Timed {
            value,
            completed_at_ms: Utc::now().timestamp_millis(),
        })
    };
    let (spot, perp, premium) = tokio::try_join!(spot, perp, premium)?;
    let observed_at_ms = Utc::now().timestamp_millis();
    calculate_basis(
        symbol,
        observed_at_ms,
        &spot.value.ask_levels().map_err(|error| error.to_string())?,
        &perp.value.bid_levels().map_err(|error| error.to_string())?,
        Decimal::from_str(&premium.value.mark_price).map_err(|_| "标记价格无效")?,
        Decimal::from_str(&premium.value.index_price).map_err(|_| "指数价格无效")?,
        Decimal::from_str(&premium.value.last_funding_rate).map_err(|_| "资金费率无效")?,
        funding_interval_hours,
        premium.value.next_funding_time,
        spot.completed_at_ms.abs_diff(perp.completed_at_ms) as i64,
        observed_at_ms.abs_diff(premium.value.time) as i64,
        started.elapsed().as_millis().min(i64::MAX as u128) as i64,
    )
    .ok_or_else(|| format!("{symbol} 订单簿深度不足"))
}

#[allow(clippy::too_many_arguments)]
fn calculate_basis(
    symbol: &str,
    observed_at_ms: i64,
    spot_asks: &[(Decimal, Decimal)],
    perp_bids: &[(Decimal, Decimal)],
    mark_price: Decimal,
    index_price: Decimal,
    funding_rate: Decimal,
    funding_interval_hours: i64,
    next_funding_time_ms: i64,
    response_skew_ms: i64,
    premium_age_ms: i64,
    scan_latency_ms: i64,
) -> Option<BinanceBasisView> {
    let spot_best_ask = spot_asks.first()?.0;
    if spot_best_ask <= Decimal::ZERO || mark_price <= Decimal::ZERO {
        return None;
    }
    let spot_depth: Decimal = spot_asks.iter().map(|(_, quantity)| *quantity).sum();
    let perp_depth: Decimal = perp_bids.iter().map(|(_, quantity)| *quantity).sum();
    let quantity = (MAX_NOTIONAL / spot_best_ask)
        .min(spot_depth)
        .min(perp_depth);
    if quantity <= Decimal::ZERO {
        return None;
    }
    let spot_cost = consume_depth(spot_asks, quantity)?;
    let perp_proceeds = consume_depth(perp_bids, quantity)?;
    let spot_vwap = spot_cost / quantity;
    let perp_vwap = perp_proceeds / quantity;
    let entry_basis = perp_proceeds - spot_cost;
    let entry_basis_rate = entry_basis / spot_cost;
    let estimated_round_trip_fees = (spot_cost * Decimal::from(SPOT_ROUND_TRIP_TAKER_BPS)
        + perp_proceeds * Decimal::from(FUTURES_ROUND_TRIP_TAKER_BPS))
        / Decimal::from(10_000);
    let slippage_buffer = (spot_cost + perp_proceeds) * Decimal::from(ROUND_TRIP_SLIPPAGE_BPS)
        / Decimal::from(10_000);
    let funding_income_per_round = mark_price * quantity * funding_rate;
    if funding_interval_hours <= 0 || 24 % funding_interval_hours != 0 {
        return None;
    }
    let intervals_per_day = Decimal::from(24) / Decimal::from(funding_interval_hours);
    let base_net = entry_basis - estimated_round_trip_fees - slippage_buffer;
    let projected_net_24h = base_net + funding_income_per_round * intervals_per_day;
    let projected_net_7d =
        base_net + funding_income_per_round * intervals_per_day * Decimal::from(7);
    let funding_apr = funding_rate * intervals_per_day * Decimal::from(365);
    let required_funding =
        (estimated_round_trip_fees + slippage_buffer - entry_basis).max(Decimal::ZERO);
    let break_even_rounds = if required_funding == Decimal::ZERO {
        Some(Decimal::ZERO)
    } else if funding_income_per_round > Decimal::ZERO {
        Some((required_funding / funding_income_per_round).ceil())
    } else {
        None
    };
    let mut rejections = Vec::new();
    if funding_rate <= Decimal::ZERO {
        rejections.push("当前资金费率不利于买现货、空永续".into());
    }
    if projected_net_24h <= Decimal::ZERO {
        rejections.push("按当前资金费率投影 24 小时仍无正净收益".into());
    }
    if response_skew_ms > MAX_RESPONSE_SKEW_MS {
        rejections.push("现货与永续接口响应完成时差超过 1 秒".into());
    }
    if premium_age_ms > MAX_PREMIUM_AGE_MS {
        rejections.push("永续标记价或资金费率数据超过 5 秒".into());
    }
    Some(BinanceBasisView {
        symbol: symbol.into(),
        observed_at_ms,
        quantity: quantity.normalize().to_string(),
        spot_vwap_ask: spot_vwap.normalize().to_string(),
        perp_vwap_bid: perp_vwap.normalize().to_string(),
        spot_cost: spot_cost.normalize().to_string(),
        perp_proceeds: perp_proceeds.normalize().to_string(),
        entry_basis: entry_basis.normalize().to_string(),
        entry_basis_rate: entry_basis_rate.normalize().to_string(),
        estimated_round_trip_fees: estimated_round_trip_fees.normalize().to_string(),
        slippage_buffer: slippage_buffer.normalize().to_string(),
        funding_rate: funding_rate.normalize().to_string(),
        funding_interval_hours,
        funding_income_per_round: funding_income_per_round.normalize().to_string(),
        projected_net_24h: projected_net_24h.normalize().to_string(),
        projected_net_7d: projected_net_7d.normalize().to_string(),
        funding_apr: funding_apr.normalize().to_string(),
        break_even_rounds: break_even_rounds.map(|value| value.normalize().to_string()),
        mark_price: mark_price.normalize().to_string(),
        index_price: index_price.normalize().to_string(),
        next_funding_time_ms,
        response_skew_ms,
        premium_age_ms,
        scan_latency_ms,
        rejections,
    })
}

fn consume_depth(levels: &[(Decimal, Decimal)], quantity: Decimal) -> Option<Decimal> {
    let mut remaining = quantity;
    let mut value = Decimal::ZERO;
    for (price, available) in levels {
        let filled = remaining.min(*available);
        value += filled * *price;
        remaining -= filled;
        if remaining <= Decimal::ZERO {
            return Some(value);
        }
    }
    None
}

fn limit_price_for_quantity(
    levels: &[(Decimal, Decimal)],
    quantity: Decimal,
) -> Result<Decimal, String> {
    let mut remaining = quantity;
    for (price, available) in levels {
        remaining -= remaining.min(*available);
        if remaining <= Decimal::ZERO {
            return Ok(*price);
        }
    }
    Err("限价内深度不足".into())
}

fn consume_depth_at_limit(
    levels: &[(Decimal, Decimal)],
    quantity: Decimal,
    limit_price: Decimal,
    is_buy: bool,
) -> Option<Decimal> {
    let mut remaining = quantity;
    let mut value = Decimal::ZERO;
    for (price, available) in levels {
        let price_allowed = if is_buy {
            *price <= limit_price
        } else {
            *price >= limit_price
        };
        if !price_allowed {
            continue;
        }
        let filled = remaining.min(*available);
        value += filled * *price;
        remaining -= filled;
        if remaining <= Decimal::ZERO {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_positive_spot_perp_carry_without_float() {
        let view = calculate_basis(
            "BTCUSDT",
            1_000,
            &[(Decimal::from(100), Decimal::from(10))],
            &[(Decimal::new(1005, 1), Decimal::from(10))],
            Decimal::new(1005, 1),
            Decimal::from(100),
            Decimal::new(1, 3),
            8,
            2_000,
            20,
            50,
            100,
        )
        .unwrap();

        assert_eq!(view.quantity, "10");
        assert_eq!(view.entry_basis, "5");
        assert!(Decimal::from_str(&view.projected_net_24h).unwrap() > Decimal::ZERO);
        assert!(view.rejections.is_empty());
    }

    #[test]
    fn rejects_negative_funding_for_long_spot_short_perp() {
        let view = calculate_basis(
            "ETHUSDT",
            1_000,
            &[(Decimal::from(100), Decimal::from(10))],
            &[(Decimal::from(100), Decimal::from(10))],
            Decimal::from(100),
            Decimal::from(100),
            Decimal::new(-1, 3),
            8,
            2_000,
            20,
            50,
            100,
        )
        .unwrap();

        assert!(view
            .rejections
            .contains(&"当前资金费率不利于买现货、空永续".into()));
        assert!(view.break_even_rounds.is_none());
    }

    #[test]
    fn calculates_delivery_cash_and_carry_net_apr() {
        let day_ms = 24 * 60 * 60 * 1_000_i64;
        let view = calculate_delivery_basis(
            "BTCUSDT_261225",
            "BTCUSDT",
            "NEXT_QUARTER",
            31 * day_ms,
            day_ms,
            Decimal::new(1, 3),
            Decimal::new(1, 3),
            Decimal::new(1, 2),
            Decimal::new(1, 1),
            &[(Decimal::from(100), Decimal::TEN)],
            &[(Decimal::from(102), Decimal::TEN)],
            Decimal::from(102),
            Decimal::from(100),
            20,
            50,
            100,
        )
        .unwrap();

        assert_eq!(view.gross_basis, "20");
        assert!(Decimal::from_str(&view.net_profit_at_delivery).unwrap() > Decimal::ZERO);
        assert!(Decimal::from_str(&view.net_apr).unwrap() > Decimal::ZERO);
        assert!(view.rejections.is_empty());
    }
}
