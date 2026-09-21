use domain::{OrderStatus, PriceLevel};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SimulatedOrder {
    pub id: Uuid,
    pub quantity: Decimal,
    pub filled: Decimal,
    pub limit_price: Option<Decimal>,
    pub total_cost: Decimal,
    pub status: OrderStatus,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct FokOrderRequest {
    pub quantity: Decimal,
    pub limit_price: Decimal,
}

#[derive(Debug, Clone)]
pub struct FokFill {
    pub requested_quantity: Decimal,
    pub filled_quantity: Decimal,
    pub limit_price: Decimal,
    pub average_price: Option<Decimal>,
    pub worst_price: Option<Decimal>,
    pub total_cost: Decimal,
    pub status: OrderStatus,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PairFokReport {
    pub first: FokFill,
    pub second: FokFill,
    pub hedge: UnhedgedPosition,
    pub status: OrderStatus,
}

#[derive(Debug, Clone)]
pub struct UnhedgedPosition {
    pub first_leg: Decimal,
    pub second_leg: Decimal,
    pub exposure: Decimal,
}
#[derive(Debug, Default)]
pub struct SimulatedExecutor {
    pub orders: Vec<SimulatedOrder>,
    pub paused: bool,
}
impl SimulatedExecutor {
    pub fn place_order(&mut self, quantity: Decimal) -> SimulatedOrder {
        let order = SimulatedOrder {
            id: Uuid::new_v4(),
            quantity,
            filled: Decimal::ZERO,
            limit_price: None,
            total_cost: Decimal::ZERO,
            status: OrderStatus::Submitting,
            reason: None,
        };
        self.orders.push(order.clone());
        order
    }
    pub fn fill_order(&mut self, id: Uuid, filled: Decimal) -> Option<UnhedgedPosition> {
        let order = self.orders.iter_mut().find(|o| o.id == id)?;
        order.filled = filled.min(order.quantity);
        order.status = if order.filled == order.quantity {
            OrderStatus::FullyFilled
        } else {
            OrderStatus::PartiallyFilled
        };
        None
    }

    pub fn execute_pair_fok(
        &mut self,
        first_request: FokOrderRequest,
        first_asks: &[PriceLevel],
        second_request: FokOrderRequest,
        second_asks: &[PriceLevel],
    ) -> PairFokReport {
        let first = simulate_fok(first_request, first_asks);
        let second = simulate_fok(second_request, second_asks);
        self.orders.push(order_from_fill(&first));
        self.orders.push(order_from_fill(&second));
        let hedge = self.assess_hedge(first.filled_quantity, second.filled_quantity);
        let status = if hedge.exposure > Decimal::ZERO {
            OrderStatus::Unhedged
        } else if first.status == OrderStatus::FullyFilled
            && second.status == OrderStatus::FullyFilled
        {
            OrderStatus::Hedged
        } else {
            OrderStatus::Cancelled
        };
        PairFokReport {
            first,
            second,
            hedge,
            status,
        }
    }
    pub fn assess_hedge(&mut self, first: Decimal, second: Decimal) -> UnhedgedPosition {
        let exposure = (first - second).abs();
        if exposure > Decimal::ZERO {
            self.paused = true;
        }
        UnhedgedPosition {
            first_leg: first,
            second_leg: second,
            exposure,
        }
    }
}

pub fn quantity_for_budget(
    budget: Decimal,
    first_limit_price: Decimal,
    second_limit_price: Decimal,
) -> Option<Decimal> {
    let combined_limit = first_limit_price + second_limit_price;
    (budget > Decimal::ZERO
        && first_limit_price > Decimal::ZERO
        && second_limit_price > Decimal::ZERO
        && combined_limit > Decimal::ZERO)
        .then(|| budget / combined_limit)
}

pub fn quantity_for_budget_at_step(
    budget: Decimal,
    first_limit_price: Decimal,
    second_limit_price: Decimal,
    quantity_step: Decimal,
) -> Option<Decimal> {
    if quantity_step <= Decimal::ZERO {
        return None;
    }
    let raw = quantity_for_budget(budget, first_limit_price, second_limit_price)?;
    let quantity = (raw / quantity_step).floor() * quantity_step;
    (quantity > Decimal::ZERO).then_some(quantity)
}

pub fn preflight_pair_fok(
    first_request: FokOrderRequest,
    first_asks: &[PriceLevel],
    second_request: FokOrderRequest,
    second_asks: &[PriceLevel],
) -> Result<(), Vec<String>> {
    let first = simulate_fok(first_request, first_asks);
    let second = simulate_fok(second_request, second_asks);
    let mut reasons = Vec::new();
    if first.status != OrderStatus::FullyFilled {
        reasons.push(format!(
            "第一腿预检失败：{}",
            first.reason.as_deref().unwrap_or("无法完全成交")
        ));
    }
    if second.status != OrderStatus::FullyFilled {
        reasons.push(format!(
            "第二腿预检失败：{}",
            second.reason.as_deref().unwrap_or("无法完全成交")
        ));
    }
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(reasons)
    }
}

pub fn simulate_fok(request: FokOrderRequest, asks: &[PriceLevel]) -> FokFill {
    if request.quantity <= Decimal::ZERO
        || request.limit_price <= Decimal::ZERO
        || request.limit_price > Decimal::ONE
    {
        return empty_fill(request, OrderStatus::Rejected, "数量或限价无效");
    }

    let mut levels = asks.to_vec();
    levels.sort_by_key(|level| level.price);
    if levels
        .iter()
        .any(|level| level.price <= Decimal::ZERO || level.quantity <= Decimal::ZERO)
    {
        return empty_fill(request, OrderStatus::Rejected, "订单簿包含无效档位");
    }

    let available: Decimal = levels
        .iter()
        .filter(|level| level.price <= request.limit_price)
        .map(|level| level.quantity)
        .sum();
    if available < request.quantity {
        return empty_fill(
            request,
            OrderStatus::Cancelled,
            "FOK：指定限价内深度不足，整单取消",
        );
    }

    let mut remaining = request.quantity;
    let mut total_cost = Decimal::ZERO;
    let mut worst_price = None;
    for level in levels
        .iter()
        .filter(|level| level.price <= request.limit_price)
    {
        let filled = remaining.min(level.quantity);
        total_cost += filled * level.price;
        remaining -= filled;
        if filled > Decimal::ZERO {
            worst_price = Some(level.price);
        }
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    FokFill {
        requested_quantity: request.quantity,
        filled_quantity: request.quantity,
        limit_price: request.limit_price,
        average_price: Some(total_cost / request.quantity),
        worst_price,
        total_cost,
        status: OrderStatus::FullyFilled,
        reason: None,
    }
}

fn empty_fill(request: FokOrderRequest, status: OrderStatus, reason: &str) -> FokFill {
    FokFill {
        requested_quantity: request.quantity,
        filled_quantity: Decimal::ZERO,
        limit_price: request.limit_price,
        average_price: None,
        worst_price: None,
        total_cost: Decimal::ZERO,
        status,
        reason: Some(reason.into()),
    }
}

fn order_from_fill(fill: &FokFill) -> SimulatedOrder {
    SimulatedOrder {
        id: Uuid::new_v4(),
        quantity: fill.requested_quantity,
        filled: fill.filled_quantity,
        limit_price: Some(fill.limit_price),
        total_cost: fill.total_cost,
        status: fill.status,
        reason: fill.reason.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_fill_pauses_on_unhedged() {
        let mut e = SimulatedExecutor::default();
        let p = e.assess_hedge(Decimal::new(1000, 0), Decimal::new(700, 0));
        assert_eq!(p.exposure, Decimal::new(300, 0));
        assert!(e.paused);
    }

    fn level(price: i64, quantity: i64) -> PriceLevel {
        PriceLevel {
            price: Decimal::new(price, 2),
            quantity: Decimal::from(quantity),
        }
    }

    #[test]
    fn fok_fills_all_levels_at_or_below_limit() {
        let fill = simulate_fok(
            FokOrderRequest {
                quantity: Decimal::from(10),
                limit_price: Decimal::new(42, 2),
            },
            &[level(40, 4), level(41, 4), level(42, 5)],
        );
        assert_eq!(fill.status, OrderStatus::FullyFilled);
        assert_eq!(fill.filled_quantity, Decimal::TEN);
        assert_eq!(fill.total_cost, Decimal::new(408, 2));
        assert_eq!(fill.average_price, Some(Decimal::new(408, 3)));
        assert_eq!(fill.worst_price, Some(Decimal::new(42, 2)));
    }

    #[test]
    fn fok_cancels_without_partial_fill_when_limit_depth_is_insufficient() {
        let fill = simulate_fok(
            FokOrderRequest {
                quantity: Decimal::from(10),
                limit_price: Decimal::new(40, 2),
            },
            &[level(40, 9), level(41, 100)],
        );
        assert_eq!(fill.status, OrderStatus::Cancelled);
        assert_eq!(fill.filled_quantity, Decimal::ZERO);
        assert_eq!(fill.total_cost, Decimal::ZERO);
    }

    #[test]
    fn cross_venue_fok_detects_unhedged_race() {
        let request = FokOrderRequest {
            quantity: Decimal::TEN,
            limit_price: Decimal::new(50, 2),
        };
        let mut executor = SimulatedExecutor::default();
        let report =
            executor.execute_pair_fok(request, &[level(49, 10)], request, &[level(51, 10)]);
        assert_eq!(report.first.status, OrderStatus::FullyFilled);
        assert_eq!(report.second.status, OrderStatus::Cancelled);
        assert_eq!(report.status, OrderStatus::Unhedged);
        assert_eq!(report.hedge.exposure, Decimal::TEN);
        assert!(executor.paused);
    }

    #[test]
    fn derives_quantity_from_total_pair_budget() {
        assert_eq!(
            quantity_for_budget(Decimal::from(100), Decimal::new(40, 2), Decimal::new(50, 2)),
            Some(Decimal::from(100) / Decimal::new(90, 2))
        );
        assert_eq!(
            quantity_for_budget_at_step(
                Decimal::from(100),
                Decimal::new(41, 2),
                Decimal::new(51, 2),
                Decimal::new(1, 2),
            ),
            Some(Decimal::new(10869, 2))
        );
    }

    #[test]
    fn preflight_rejects_pair_before_submission_when_either_leg_cannot_fill() {
        let request = FokOrderRequest {
            quantity: Decimal::TEN,
            limit_price: Decimal::new(50, 2),
        };
        let rejection =
            preflight_pair_fok(request, &[level(49, 10)], request, &[level(51, 10)]).unwrap_err();
        assert_eq!(rejection.len(), 1);
        assert!(rejection[0].starts_with("第二腿预检失败"));
    }
}
