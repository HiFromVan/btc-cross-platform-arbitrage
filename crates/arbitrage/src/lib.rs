use chrono::{DateTime, Duration, Utc};
use domain::{
    ArbitrageDirection, ArbitrageOpportunity, Market, MarketStatus, OrderBook, Outcome, Venue,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_trade_amount: Decimal,
    pub minimum_remaining_time: Duration,
    pub maximum_book_age: Duration,
}
#[derive(Debug, PartialEq, Eq)]
pub enum RiskRejection {
    MarketNotOpen,
    MarketEndingSoon,
    StaleOrderBook,
    InvalidBook,
}
pub fn check_risk(
    a: &Market,
    b: &Market,
    first: &OrderBook,
    second: &OrderBook,
    now: DateTime<Utc>,
    limits: &RiskLimits,
) -> Result<(), RiskRejection> {
    if a.status != MarketStatus::Open || b.status != MarketStatus::Open {
        return Err(RiskRejection::MarketNotOpen);
    }
    if a.end_time - now < limits.minimum_remaining_time
        || b.end_time - now < limits.minimum_remaining_time
    {
        return Err(RiskRejection::MarketEndingSoon);
    }
    if now - first.timestamp > limits.maximum_book_age
        || now - second.timestamp > limits.maximum_book_age
    {
        return Err(RiskRejection::StaleOrderBook);
    }
    if first.asks.is_empty()
        || second.asks.is_empty()
        || first
            .asks
            .iter()
            .chain(second.asks.iter())
            .any(|l| l.price <= Decimal::ZERO || l.quantity <= Decimal::ZERO)
    {
        return Err(RiskRejection::InvalidBook);
    }
    Ok(())
}

pub fn calculate_buy_arbitrage(
    up: &OrderBook,
    down: &OrderBook,
    minimum_profit: Decimal,
    fee_rate: Decimal,
    slippage_buffer: Decimal,
) -> Option<ArbitrageOpportunity> {
    let direction = match (up.venue, up.outcome, down.venue, down.outcome) {
        (Venue::Binance, Outcome::Up, Venue::Polymarket, Outcome::Down) => {
            ArbitrageDirection::BinanceUpPolymarketDown
        }
        (Venue::Binance, Outcome::Down, Venue::Polymarket, Outcome::Up) => {
            ArbitrageDirection::BinanceDownPolymarketUp
        }
        _ => return None,
    };
    let maximum = up
        .asks
        .iter()
        .map(|x| x.quantity)
        .sum::<Decimal>()
        .min(down.asks.iter().map(|x| x.quantity).sum());
    if maximum <= Decimal::ZERO || fee_rate < Decimal::ZERO || slippage_buffer < Decimal::ZERO {
        return None;
    }

    // 只在任一订单簿跨过一个价格档位时，组合边际成本才会改变。
    let mut candidates = vec![maximum];
    let mut running = Decimal::ZERO;
    for level in &up.asks {
        running += level.quantity;
        if running <= maximum {
            candidates.push(running);
        }
    }
    running = Decimal::ZERO;
    for level in &down.asks {
        running += level.quantity;
        if running <= maximum {
            candidates.push(running);
        }
    }
    candidates.sort();
    candidates.dedup();

    candidates
        .into_iter()
        .filter_map(|quantity| {
            let cost = up.cost_for_quantity(quantity)? + down.cost_for_quantity(quantity)?;
            if cost <= Decimal::ZERO {
                return None;
            }
            let gross = quantity - cost;
            let fees = cost * fee_rate;
            let slip = cost * slippage_buffer;
            let net = gross - fees - slip;
            (net > minimum_profit).then_some(ArbitrageOpportunity {
                direction,
                executable_size: quantity,
                cost,
                gross_payout: quantity,
                gross_profit: gross,
                fees,
                slippage: slip,
                safety_buffer: Decimal::ZERO,
                net_profit: net,
                net_profit_rate: net / cost,
                detected_at: Utc::now(),
            })
        })
        .max_by_key(|opportunity| opportunity.executable_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use domain::{Outcome, PriceLevel, Venue};
    fn book(venue: Venue, outcome: Outcome, asks: &[(i64, i64)]) -> OrderBook {
        OrderBook {
            venue,
            market_id: "id".into(),
            outcome,
            bids: vec![],
            asks: asks
                .iter()
                .map(|(p, q)| PriceLevel {
                    price: Decimal::new(*p, 2),
                    quantity: Decimal::new(*q, 0),
                })
                .collect(),
            timestamp: Utc::now(),
        }
    }
    #[test]
    fn detects_profitable_depth() {
        let o = calculate_buy_arbitrage(
            &book(Venue::Binance, Outcome::Up, &[(40, 100), (41, 500)]),
            &book(Venue::Polymarket, Outcome::Down, &[(50, 200), (51, 500)]),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
        .unwrap();
        assert_eq!(o.executable_size, Decimal::new(600, 0));
        assert_eq!(o.cost, Decimal::new(549, 0));
    }
    #[test]
    fn rejects_equal_prices() {
        assert!(calculate_buy_arbitrage(
            &book(Venue::Binance, Outcome::Up, &[(50, 100)]),
            &book(Venue::Polymarket, Outcome::Down, &[(50, 100)]),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO
        )
        .is_none());
    }
    #[test]
    fn fees_can_remove_opportunity() {
        assert!(calculate_buy_arbitrage(
            &book(Venue::Binance, Outcome::Up, &[(40, 100)]),
            &book(Venue::Polymarket, Outcome::Down, &[(50, 100)]),
            Decimal::ZERO,
            Decimal::new(2, 1),
            Decimal::ZERO
        )
        .is_none());
    }

    #[test]
    fn detects_second_direction() {
        let opportunity = calculate_buy_arbitrage(
            &book(Venue::Binance, Outcome::Down, &[(40, 100)]),
            &book(Venue::Polymarket, Outcome::Up, &[(50, 100)]),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
        .unwrap();
        assert_eq!(
            opportunity.direction,
            ArbitrageDirection::BinanceDownPolymarketUp
        );
    }

    #[test]
    fn rejects_stale_orderbook() {
        let now = Utc::now();
        let market = Market {
            venue: Venue::Binance,
            market_id: "id".into(),
            symbol: "BTC".into(),
            outcome: Outcome::Up,
            start_time: now,
            end_time: now + Duration::minutes(5),
            settlement_source: Some("mock".into()),
            settlement_rule: Some("mock".into()),
            status: MarketStatus::Open,
        };
        let mut stale = book(Venue::Binance, Outcome::Up, &[(40, 100)]);
        stale.timestamp = now - Duration::seconds(6);
        let fresh = book(Venue::Polymarket, Outcome::Down, &[(50, 100)]);
        let limits = RiskLimits {
            max_trade_amount: Decimal::new(1000, 0),
            minimum_remaining_time: Duration::seconds(30),
            maximum_book_age: Duration::seconds(5),
        };
        assert_eq!(
            check_risk(&market, &market, &stale, &fresh, now, &limits),
            Err(RiskRejection::StaleOrderBook)
        );
    }
}
