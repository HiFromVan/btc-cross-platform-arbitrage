use domain::OrderStatus;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SimulatedOrder {
    pub id: Uuid,
    pub quantity: Decimal,
    pub filled: Decimal,
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
            status: OrderStatus::Submitting,
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
}
