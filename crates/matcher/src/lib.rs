use domain::{EventIdentity, Market, PairAssessment, PairClassification, ResolutionSpec};
use thiserror::Error;
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SettlementMismatch {
    #[error("oracle 不一致")]
    Oracle,
    #[error("时间窗口不一致")]
    TimeWindow,
    #[error("参考价格规则不一致")]
    ReferencePrice,
    #[error("结算规则不一致")]
    Rule,
    #[error("未知结算信息")]
    Unknown,
}
pub fn validate_settlement_compatibility(
    a: &Market,
    b: &Market,
) -> Result<EventIdentity, SettlementMismatch> {
    let ia = a.identity().ok_or(SettlementMismatch::Unknown)?;
    let ib = b.identity().ok_or(SettlementMismatch::Unknown)?;
    if ia.asset.to_uppercase() != "BTC" || ib.asset.to_uppercase() != "BTC" {
        return Err(SettlementMismatch::Unknown);
    }
    if ia.start_time != ib.start_time || ia.end_time != ib.end_time {
        return Err(SettlementMismatch::TimeWindow);
    }
    if ia.settlement_source != ib.settlement_source {
        return Err(SettlementMismatch::Oracle);
    }
    if ia.settlement_rule != ib.settlement_rule {
        return Err(SettlementMismatch::Rule);
    }
    Ok(ia)
}

pub fn classify_candidate_pair(
    start_matches: bool,
    end_matches: bool,
    left: &ResolutionSpec,
    right: &ResolutionSpec,
) -> PairAssessment {
    let mut differences = Vec::new();
    if left.asset != right.asset {
        differences.push(format!("标的不同：{} / {}", left.asset, right.asset));
    }
    if !start_matches || !end_matches {
        differences.push("事件时间窗口不同".into());
    }
    if !differences.is_empty() {
        return PairAssessment {
            classification: PairClassification::Incompatible,
            differences,
        };
    }

    if left.quote_currency != right.quote_currency {
        differences.push(format!(
            "报价币种不同：{} / {}",
            left.quote_currency, right.quote_currency
        ));
    }
    if left.price_feed_provider != right.price_feed_provider {
        differences.push(format!(
            "价格源提供方不同：{} / {}",
            left.price_feed_provider, right.price_feed_provider
        ));
    }
    if left.price_feed_id != right.price_feed_id {
        differences.push("价格源 Feed 不同".into());
    }
    if left.calculation != right.calculation {
        differences.push(format!(
            "价格计算不同：{} / {}",
            left.calculation, right.calculation
        ));
    }
    if left.up_comparison != right.up_comparison {
        differences.push(format!(
            "上涨边界不同：{} / {}",
            left.up_comparison, right.up_comparison
        ));
    }
    if left.equal_rule != right.equal_rule {
        differences.push(format!(
            "相等价格处理不同：{} / {}",
            left.equal_rule, right.equal_rule
        ));
    }
    if left.fallback_rule != right.fallback_rule {
        differences.push("价格源故障回退规则不同".into());
    }
    if left.collateral != right.collateral {
        differences.push(format!(
            "抵押/兑付币种不同：{} / {}",
            left.collateral, right.collateral
        ));
    }

    PairAssessment {
        classification: if differences.is_empty() {
            PairClassification::Exact
        } else {
            PairClassification::Basis
        },
        differences,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use domain::{MarketStatus, Outcome, Venue};
    fn market(venue: Venue, source: Option<&str>, end_offset: i64) -> Market {
        let start = Utc::now();
        Market {
            venue,
            market_id: "x".into(),
            symbol: "BTC".into(),
            outcome: Outcome::Up,
            start_time: start,
            end_time: start + Duration::minutes(5 + end_offset),
            settlement_source: source.map(str::to_string),
            settlement_rule: Some("index close; equal is void".into()),
            status: MarketStatus::Open,
        }
    }
    #[test]
    fn rejects_unknown_oracle() {
        let first = market(Venue::Binance, None, 0);
        let mut second = first.clone();
        second.venue = Venue::Polymarket;
        assert_eq!(
            validate_settlement_compatibility(&first, &second).unwrap_err(),
            SettlementMismatch::Unknown
        )
    }

    fn resolution(quote: &str, calculation: &str) -> ResolutionSpec {
        ResolutionSpec {
            asset: "BTC".into(),
            quote_currency: quote.into(),
            price_feed_provider: "CHAINLINK".into(),
            price_feed_id: Some("feed".into()),
            calculation: calculation.into(),
            up_comparison: "greater_than".into(),
            equal_rule: "split".into(),
            fallback_rule: None,
            collateral: "USDC".into(),
        }
    }

    #[test]
    fn classifies_same_event_with_different_price_method_as_basis() {
        let assessment = classify_candidate_pair(
            true,
            true,
            &resolution("USDT", "top_of_book_close"),
            &resolution("USD", "twap_60s"),
        );
        assert_eq!(assessment.classification, PairClassification::Basis);
        assert!(assessment.differences.len() >= 2);
    }

    #[test]
    fn rejects_different_time_window() {
        let spec = resolution("USD", "twap_60s");
        let assessment = classify_candidate_pair(true, false, &spec, &spec);
        assert_eq!(assessment.classification, PairClassification::Incompatible);
    }
}
