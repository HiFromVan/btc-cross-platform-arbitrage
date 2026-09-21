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
    futures_base_url: String,
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
            futures_base_url: "https://fapi.binance.com".into(),
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

    pub async fn spot_depth(
        &self,
        symbol: &str,
        limit: u16,
    ) -> Result<BinancePublicOrderBook, BinanceError> {
        self.public_depth(&self.base_url, "/api/v3/depth", symbol, limit)
            .await
    }

    pub async fn usdt_futures_depth(
        &self,
        symbol: &str,
        limit: u16,
    ) -> Result<BinancePublicOrderBook, BinanceError> {
        self.public_depth(&self.futures_base_url, "/fapi/v1/depth", symbol, limit)
            .await
    }

    async fn public_depth(
        &self,
        base_url: &str,
        path: &str,
        symbol: &str,
        limit: u16,
    ) -> Result<BinancePublicOrderBook, BinanceError> {
        let limit = limit.to_string();
        let response = self
            .http
            .get(format!("{base_url}{path}"))
            .query(&[("symbol", symbol), ("limit", limit.as_str())])
            .send()
            .await?
            .error_for_status()?
            .json::<BinancePublicOrderBook>()
            .await?;
        response.validate()?;
        Ok(response)
    }

    pub async fn usdt_futures_premium_index(
        &self,
        symbol: &str,
    ) -> Result<BinancePremiumIndex, BinanceError> {
        let response = self
            .http
            .get(format!("{}/fapi/v1/premiumIndex", self.futures_base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .error_for_status()?
            .json::<BinancePremiumIndex>()
            .await?;
        response.validate()?;
        Ok(response)
    }

    pub async fn usdt_futures_funding_info(&self) -> Result<Vec<BinanceFundingInfo>, BinanceError> {
        Ok(self
            .http
            .get(format!("{}/fapi/v1/fundingInfo", self.futures_base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<BinanceFundingInfo>>()
            .await?)
    }

    pub async fn usdt_futures_exchange_info(
        &self,
    ) -> Result<BinanceFuturesExchangeInfo, BinanceError> {
        Ok(self
            .http
            .get(format!("{}/fapi/v1/exchangeInfo", self.futures_base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceFuturesExchangeInfo>()
            .await?)
    }

    pub async fn spot_exchange_info(&self) -> Result<BinanceSpotExchangeInfo, BinanceError> {
        Ok(self
            .http
            .get(format!("{}/api/v3/exchangeInfo", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceSpotExchangeInfo>()
            .await?)
    }

    pub async fn usdt_delivery_premium_index(
        &self,
        symbol: &str,
    ) -> Result<BinanceDeliveryPremiumIndex, BinanceError> {
        let response = self
            .http
            .get(format!("{}/fapi/v1/premiumIndex", self.futures_base_url))
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceDeliveryPremiumIndex>()
            .await?;
        response.validate()?;
        Ok(response)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSpotPrice {
    pub symbol: String,
    pub price: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinancePublicOrderBook {
    pub last_update_id: i64,
    #[serde(default)]
    pub event_time: Option<i64>,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

impl BinancePublicOrderBook {
    fn validate(&self) -> Result<(), BinanceError> {
        self.bid_levels()?;
        self.ask_levels()?;
        Ok(())
    }

    pub fn bid_levels(&self) -> Result<Vec<(Decimal, Decimal)>, BinanceError> {
        decimal_public_levels(&self.bids, true)
    }

    pub fn ask_levels(&self) -> Result<Vec<(Decimal, Decimal)>, BinanceError> {
        decimal_public_levels(&self.asks, false)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinancePremiumIndex {
    pub symbol: String,
    pub mark_price: String,
    pub index_price: String,
    pub last_funding_rate: String,
    pub next_funding_time: i64,
    pub time: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFundingInfo {
    pub symbol: String,
    pub funding_interval_hours: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFuturesExchangeInfo {
    pub symbols: Vec<BinanceFuturesSymbol>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFuturesSymbol {
    pub symbol: String,
    pub pair: String,
    pub contract_type: String,
    pub delivery_date: i64,
    pub status: String,
    #[serde(default)]
    pub filters: Vec<BinanceSymbolFilter>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSpotExchangeInfo {
    pub symbols: Vec<BinanceSpotSymbol>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSpotSymbol {
    pub symbol: String,
    pub status: String,
    #[serde(default)]
    pub filters: Vec<BinanceSymbolFilter>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceSymbolFilter {
    pub filter_type: String,
    #[serde(default)]
    pub tick_size: Option<String>,
    #[serde(default)]
    pub step_size: Option<String>,
    #[serde(default)]
    pub min_qty: Option<String>,
}

impl BinanceSymbolFilter {
    pub fn decimal_step_size(&self) -> Option<Decimal> {
        self.step_size
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok())
    }

    pub fn decimal_tick_size(&self) -> Option<Decimal> {
        self.tick_size
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok())
    }

    pub fn decimal_min_qty(&self) -> Option<Decimal> {
        self.min_qty
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceDeliveryPremiumIndex {
    pub symbol: String,
    pub mark_price: String,
    pub index_price: String,
    pub time: i64,
}

impl BinanceDeliveryPremiumIndex {
    fn validate(&self) -> Result<(), BinanceError> {
        Decimal::from_str(&self.mark_price)
            .map_err(|_| BinanceError::Invalid("交割合约标记价格".into()))?;
        Decimal::from_str(&self.index_price)
            .map_err(|_| BinanceError::Invalid("交割合约指数价格".into()))?;
        Ok(())
    }
}

impl BinancePremiumIndex {
    fn validate(&self) -> Result<(), BinanceError> {
        for (name, value) in [
            ("标记价格", &self.mark_price),
            ("指数价格", &self.index_price),
            ("资金费率", &self.last_funding_rate),
        ] {
            Decimal::from_str(value).map_err(|_| BinanceError::Invalid(name.into()))?;
        }
        Ok(())
    }
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
        Ok(self.ask_levels()?.into_iter().next())
    }

    pub fn ask_levels(&self) -> Result<Vec<(Decimal, Decimal)>, BinanceError> {
        decimal_ask_levels(&self.asks)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinancePriceLevel {
    pub price: String,
    pub size: String,
}

fn decimal_ask_levels(
    levels: &[BinancePriceLevel],
) -> Result<Vec<(Decimal, Decimal)>, BinanceError> {
    let mut levels = levels
        .iter()
        .map(|level| {
            Ok((
                Decimal::from_str(&level.price)
                    .map_err(|_| BinanceError::Invalid("订单价格".into()))?,
                Decimal::from_str(&level.size)
                    .map_err(|_| BinanceError::Invalid("订单数量".into()))?,
            ))
        })
        .collect::<Result<Vec<_>, BinanceError>>()?;
    levels.sort_by_key(|(price, _)| *price);
    Ok(levels)
}

fn decimal_public_levels(
    levels: &[[String; 2]],
    descending: bool,
) -> Result<Vec<(Decimal, Decimal)>, BinanceError> {
    let mut values = levels
        .iter()
        .map(|level| {
            Ok((
                Decimal::from_str(&level[0])
                    .map_err(|_| BinanceError::Invalid("公开订单价格".into()))?,
                Decimal::from_str(&level[1])
                    .map_err(|_| BinanceError::Invalid("公开订单数量".into()))?,
            ))
        })
        .collect::<Result<Vec<_>, BinanceError>>()?;
    if descending {
        values.sort_by(|left, right| right.0.cmp(&left.0));
    } else {
        values.sort_by_key(|(price, _)| *price);
    }
    Ok(values)
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
            decimal_ask_levels(&levels).unwrap().first().copied(),
            Some((Decimal::new(7, 2), Decimal::new(35, 1)))
        );
    }

    #[test]
    fn sorts_public_bids_highest_first_and_asks_lowest_first() {
        let levels = vec![["100.1".into(), "2".into()], ["100.3".into(), "1".into()]];
        assert_eq!(
            decimal_public_levels(&levels, true)
                .unwrap()
                .first()
                .copied(),
            Some((Decimal::new(1003, 1), Decimal::ONE))
        );
        assert_eq!(
            decimal_public_levels(&levels, false)
                .unwrap()
                .first()
                .copied(),
            Some((Decimal::new(1001, 1), Decimal::from(2)))
        );
    }
}
