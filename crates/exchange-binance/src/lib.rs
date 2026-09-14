use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use sha2::Sha256;
use std::str::FromStr;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum BinanceError {
    #[error("Binance HTTP 请求失败：{0}")]
    Http(#[from] reqwest::Error),
    #[error("Binance API {code}：{message}")]
    Api { code: i64, message: String },
    #[error("Binance 响应字段无效：{0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct BinanceClient {
    http: reqwest::Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl BinanceClient {
    pub fn new(api_key: String, api_secret: String) -> Result<Self, BinanceError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-MBX-APIKEY",
            HeaderValue::from_str(&api_key)
                .map_err(|_| BinanceError::Invalid("API Key 格式".into()))?,
        );
        Ok(Self {
            http: reqwest::Client::builder()
                .default_headers(headers)
                .build()?,
            api_key,
            api_secret,
            base_url: "https://api.binance.com".into(),
        })
    }

    pub fn api_key_is_configured(&self) -> bool {
        !self.api_key.is_empty()
    }

    async fn signed_get<T: DeserializeOwned>(
        &self,
        path: &str,
        parameters: &str,
    ) -> Result<T, BinanceError> {
        let timestamp = Utc::now().timestamp_millis();
        let query = if parameters.is_empty() {
            format!("timestamp={timestamp}&recvWindow=10000")
        } else {
            format!("{parameters}&timestamp={timestamp}&recvWindow=10000")
        };
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| BinanceError::Invalid("API Secret 格式".into()))?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, path, query, signature
        );
        let response = self.http.get(url).send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            if let Ok(error) = serde_json::from_slice::<ApiError>(&body) {
                return Err(BinanceError::Api {
                    code: error.code,
                    message: error.msg,
                });
            }
            return Err(BinanceError::Invalid(format!("HTTP {status}")));
        }
        serde_json::from_slice(&body)
            .map_err(|error| BinanceError::Invalid(format!("JSON：{error}")))
    }

    pub async fn list_crypto_up_down_markets(
        &self,
    ) -> Result<Vec<BinanceMarketTopic>, BinanceError> {
        let response: MarketListResponse = self
            .signed_get(
                "/sapi/v1/w3w/wallet/prediction/market/list",
                "l1Category=crypto&l2Category=up-down&sortBy=END_DATE&orderBy=ASC&offset=0&limit=100",
            )
            .await?;
        Ok(response.market_topics)
    }

    pub async fn market_detail(&self, topic_id: i64) -> Result<BinanceMarketDetail, BinanceError> {
        self.signed_get(
            "/sapi/v1/w3w/wallet/prediction/market/detail",
            &format!("marketTopicId={topic_id}"),
        )
        .await
    }

    pub async fn order_book(
        &self,
        vendor: &str,
        market_id: i64,
        token_id: &str,
    ) -> Result<BinanceOrderBook, BinanceError> {
        self.signed_get(
            "/sapi/v1/w3w/wallet/prediction/order-book",
            &format!("vendor={vendor}&marketId={market_id}&tokenId={token_id}"),
        )
        .await
    }

    pub async fn spot_price(&self, symbol: &str) -> Result<BinanceSpotPrice, BinanceError> {
        let response = self
            .http
            .get(format!("{}/api/v3/ticker/price", self.base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceSpotPrice>()
            .await?;
        Decimal::from_str(&response.price).map_err(|_| BinanceError::Invalid("现货价格".into()))?;
        Ok(response)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSpotPrice {
    pub symbol: String,
    pub price: String,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketListResponse {
    market_topics: Vec<BinanceMarketTopic>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceMarketTopic {
    pub market_topic_id: i64,
    pub vendor: String,
    pub slug: String,
    pub title: String,
    pub question: String,
    pub symbol: String,
    pub collateral: String,
    pub fee_rate_bps: i64,
    pub slippage_bps: i64,
    pub liquidity: String,
    pub trade_volume: String,
    pub start_date: i64,
    pub end_date: i64,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceMarketDetail {
    pub market_topic_id: i64,
    pub vendor: String,
    pub slug: String,
    pub title: String,
    pub question: String,
    pub description: Option<String>,
    pub symbol: String,
    pub collateral: String,
    pub fee_rate_bps: i64,
    pub slippage_bps: i64,
    pub start_date: i64,
    pub end_date: i64,
    pub status: String,
    pub variant_data: BinanceVariantData,
    pub markets: Vec<BinanceOutcomeMarket>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceVariantData {
    pub r#type: String,
    pub start_price: Option<String>,
    pub end_price: Option<String>,
    pub price_feed_id: Option<String>,
    pub price_feed_provider: String,
    pub price_feed_symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOutcomeMarket {
    pub market_id: i64,
    pub external_id: String,
    pub title: String,
    pub description: Option<String>,
    pub condition_id: String,
    pub status: String,
    pub trading_status: String,
    pub decimal_precision: i64,
    pub outcomes: Vec<BinanceOutcome>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOutcome {
    pub name: String,
    pub price: String,
    pub chance: String,
    pub index: i64,
    pub token_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceOrderBook {
    pub outcome: String,
    pub token_id: String,
    pub timestamp: i64,
    pub bids: Vec<BinancePriceLevel>,
    pub asks: Vec<BinancePriceLevel>,
}

impl BinanceOrderBook {
    pub fn best_ask(&self) -> Result<Option<(Decimal, Decimal)>, BinanceError> {
        decimal_best_ask(&self.asks)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinancePriceLevel {
    pub price: String,
    pub size: String,
}

fn decimal_best_ask(
    levels: &[BinancePriceLevel],
) -> Result<Option<(Decimal, Decimal)>, BinanceError> {
    levels
        .iter()
        .map(|level| {
            Ok((
                Decimal::from_str(&level.price)
                    .map_err(|_| BinanceError::Invalid("订单价格".into()))?,
                Decimal::from_str(&level.size)
                    .map_err(|_| BinanceError::Invalid("订单数量".into()))?,
            ))
        })
        .collect::<Result<Vec<_>, BinanceError>>()
        .map(|levels| levels.into_iter().min_by_key(|(price, _)| *price))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chooses_lowest_ask_without_floating_point() {
        let levels = vec![
            BinancePriceLevel {
                price: "0.12".into(),
                size: "5".into(),
            },
            BinancePriceLevel {
                price: "0.07".into(),
                size: "3.5".into(),
            },
        ];
        assert_eq!(
            decimal_best_ask(&levels).unwrap(),
            Some((Decimal::new(7, 2), Decimal::new(35, 1)))
        );
    }
}
