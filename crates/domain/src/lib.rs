use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Venue {
    Binance,
    Polymarket,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Up,
    Down,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArbitrageDirection {
    BinanceUpPolymarketDown,
    BinanceDownPolymarketUp,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Detected,
    Preparing,
    Submitting,
    PartiallyFilled,
    FullyFilled,
    Hedged,
    Failed,
    Unhedged,
    Settled,
    Redeemed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketStatus {
    Open,
    Closed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PairClassification {
    Exact,
    Basis,
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionSpec {
    pub asset: String,
    pub quote_currency: String,
    pub price_feed_provider: String,
    pub price_feed_id: Option<String>,
    pub calculation: String,
    pub up_comparison: String,
    pub equal_rule: String,
    pub fallback_rule: Option<String>,
    pub collateral: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairAssessment {
    pub classification: PairClassification,
    pub differences: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventIdentity {
    pub asset: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub settlement_source: String,
    pub settlement_rule: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub venue: Venue,
    pub market_id: String,
    pub symbol: String,
    pub outcome: Outcome,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub settlement_source: Option<String>,
    pub settlement_rule: Option<String>,
    pub status: MarketStatus,
}
impl Market {
    pub fn identity(&self) -> Option<EventIdentity> {
        Some(EventIdentity {
            asset: self.symbol.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
            settlement_source: self.settlement_source.clone()?,
            settlement_rule: self.settlement_rule.clone()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub venue: Venue,
    pub market_id: String,
    pub outcome: Outcome,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: DateTime<Utc>,
}
impl OrderBook {
    pub fn cost_for_quantity(&self, mut quantity: Decimal) -> Option<Decimal> {
        if quantity <= Decimal::ZERO {
            return None;
        }
        let mut cost = Decimal::ZERO;
        for l in &self.asks {
            if l.price <= Decimal::ZERO || l.quantity <= Decimal::ZERO {
                return None;
            }
            let take = quantity.min(l.quantity);
            cost += take * l.price;
            quantity -= take;
            if quantity.is_zero() {
                return Some(cost);
            }
        }
        None
    }
    pub fn max_quantity_at_or_below(&self, max_cost_per_unit: Decimal) -> Decimal {
        self.asks
            .iter()
            .filter(|l| {
                l.price > Decimal::ZERO
                    && l.quantity > Decimal::ZERO
                    && l.price <= max_cost_per_unit
            })
            .map(|l| l.quantity)
            .sum()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub direction: ArbitrageDirection,
    pub executable_size: Decimal,
    pub cost: Decimal,
    pub gross_payout: Decimal,
    pub gross_profit: Decimal,
    pub fees: Decimal,
    pub slippage: Decimal,
    pub safety_buffer: Decimal,
    pub net_profit: Decimal,
    pub net_profit_rate: Decimal,
    pub detected_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn computes_cost_over_multiple_levels() {
        let book = OrderBook {
            venue: Venue::Binance,
            market_id: "x".into(),
            outcome: Outcome::Up,
            bids: vec![],
            asks: vec![
                PriceLevel {
                    price: Decimal::new(40, 2),
                    quantity: Decimal::new(100, 0),
                },
                PriceLevel {
                    price: Decimal::new(41, 2),
                    quantity: Decimal::new(500, 0),
                },
            ],
            timestamp: Utc::now(),
        };
        assert_eq!(
            book.cost_for_quantity(Decimal::new(600, 0)),
            Some(Decimal::new(245, 0))
        );
    }
}
