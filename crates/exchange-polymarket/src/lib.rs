use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolymarketError {
    #[error("Polymarket HTTP 请求失败：{0}")]
    Http(#[from] reqwest::Error),
    #[error("Polymarket 响应字段无效：{0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct PolymarketClient {
    http: reqwest::Client,
    gamma_base: String,
    clob_base: String,
}

impl PolymarketClient {
    pub fn new(gamma_base: String, clob_base: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            gamma_base,
            clob_base,
        }
    }

    pub async fn event_by_slug(
        &self,
        slug: &str,
    ) -> Result<Option<PolymarketEvent>, PolymarketError> {
        let events: Vec<PolymarketEvent> = self
            .http
            .get(format!("{}/events", self.gamma_base))
            .query(&[("slug", slug)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(events.into_iter().next())
    }

    pub async fn order_book(&self, token_id: &str) -> Result<PolymarketOrderBook, PolymarketError> {
        Ok(self
            .http
            .get(format!("{}/book", self.clob_base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketEvent {
    pub id: String,
    pub title: String,
    pub slug: String,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub event_metadata: Option<PolymarketEventMetadata>,
    pub markets: Vec<PolymarketMarket>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketEventMetadata {
    pub price_to_beat: Option<Decimal>,
    pub final_price: Option<Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolymarketMarket {
    pub id: String,
    pub question: String,
    pub description: String,
    pub slug: String,
    pub outcomes: String,
    pub outcome_prices: String,
    pub clob_token_ids: String,
    pub fees_enabled: bool,
    #[serde(default)]
    pub fee_schedule: Option<PolymarketFeeSchedule>,
    pub resolution_source: String,
    #[serde(default)]
    pub event_start_time: Option<String>,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub uma_resolution_status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketFeeSchedule {
    pub rate: Decimal,
    pub exponent: i64,
}

impl PolymarketMarket {
    pub fn outcome_tokens(&self) -> Result<(String, String), PolymarketError> {
        let outcomes: Vec<String> = serde_json::from_str(&self.outcomes)
            .map_err(|error| PolymarketError::Invalid(format!("outcomes：{error}")))?;
        let tokens: Vec<String> = serde_json::from_str(&self.clob_token_ids)
            .map_err(|error| PolymarketError::Invalid(format!("clobTokenIds：{error}")))?;
        let up = outcomes
            .iter()
            .position(|value| value.eq_ignore_ascii_case("up"));
        let down = outcomes
            .iter()
            .position(|value| value.eq_ignore_ascii_case("down"));
        match (up, down) {
            (Some(up), Some(down)) if up < tokens.len() && down < tokens.len() => {
                Ok((tokens[up].clone(), tokens[down].clone()))
            }
            _ => Err(PolymarketError::Invalid("缺少 Up/Down Token".into())),
        }
    }

    pub fn resolved_outcome(&self) -> Result<Option<String>, PolymarketError> {
        if !self.closed || self.uma_resolution_status.as_deref() != Some("resolved") {
            return Ok(None);
        }
        let outcomes: Vec<String> = serde_json::from_str(&self.outcomes)
            .map_err(|error| PolymarketError::Invalid(format!("outcomes：{error}")))?;
        let prices: Vec<String> = serde_json::from_str(&self.outcome_prices)
            .map_err(|error| PolymarketError::Invalid(format!("outcomePrices：{error}")))?;
        outcomes
            .into_iter()
            .zip(prices)
            .find_map(|(outcome, price)| {
                Decimal::from_str(&price)
                    .ok()
                    .filter(|value| *value == Decimal::ONE)
                    .map(|_| outcome)
            })
            .map(Some)
            .ok_or_else(|| PolymarketError::Invalid("已结算市场缺少胜出 outcome".into()))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketOrderBook {
    pub timestamp: String,
    pub hash: String,
    pub bids: Vec<PolymarketPriceLevel>,
    pub asks: Vec<PolymarketPriceLevel>,
    pub min_order_size: String,
    pub tick_size: String,
}

impl PolymarketOrderBook {
    pub fn best_ask(&self) -> Result<Option<(Decimal, Decimal)>, PolymarketError> {
        Ok(self.ask_levels()?.into_iter().next())
    }

    pub fn ask_levels(&self) -> Result<Vec<(Decimal, Decimal)>, PolymarketError> {
        let mut levels = self
            .asks
            .iter()
            .map(|level| {
                Ok((
                    Decimal::from_str(&level.price)
                        .map_err(|_| PolymarketError::Invalid("订单价格".into()))?,
                    Decimal::from_str(&level.size)
                        .map_err(|_| PolymarketError::Invalid("订单数量".into()))?,
                ))
            })
            .collect::<Result<Vec<_>, PolymarketError>>()?;
        levels.sort_by_key(|(price, _)| *price);
        Ok(levels)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolymarketPriceLevel {
    pub price: String,
    pub size: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_tokens_by_outcome_name() {
        let market = PolymarketMarket {
            id: "1".into(),
            question: "q".into(),
            description: "d".into(),
            slug: "s".into(),
            outcomes: "[\"Down\",\"Up\"]".into(),
            outcome_prices: "[]".into(),
            clob_token_ids: "[\"down-token\",\"up-token\"]".into(),
            fees_enabled: true,
            fee_schedule: None,
            resolution_source: "source".into(),
            event_start_time: None,
            closed: false,
            uma_resolution_status: None,
        };
        assert_eq!(
            market.outcome_tokens().unwrap(),
            ("up-token".into(), "down-token".into())
        );
    }

    #[test]
    fn reads_resolved_outcome_without_floating_point() {
        let market = PolymarketMarket {
            id: "1".into(),
            question: "q".into(),
            description: "d".into(),
            slug: "s".into(),
            outcomes: "[\"Up\",\"Down\"]".into(),
            outcome_prices: "[\"0\",\"1\"]".into(),
            clob_token_ids: "[]".into(),
            fees_enabled: true,
            fee_schedule: None,
            resolution_source: "source".into(),
            event_start_time: None,
            closed: true,
            uma_resolution_status: Some("resolved".into()),
        };
        assert_eq!(market.resolved_outcome().unwrap(), Some("Down".into()));
    }
}
