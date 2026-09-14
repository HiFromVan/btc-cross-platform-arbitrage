use crate::scanner::{ObservationView, PaperTradeView, SettlementUpdate, TrackedPairView};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Storage {
    connection: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS market_pairs (
               slug TEXT PRIMARY KEY,
               binance_topic_id INTEGER,
               asset TEXT NOT NULL,
               duration TEXT NOT NULL,
               classification TEXT NOT NULL,
               differences_json TEXT NOT NULL,
               binance_source TEXT NOT NULL,
               polymarket_source TEXT NOT NULL,
               start_ms INTEGER NOT NULL,
               end_ms INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS observations (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               slug TEXT NOT NULL,
               observed_at_ms INTEGER NOT NULL,
               binance_up_ask TEXT,
               binance_down_ask TEXT,
               polymarket_up_ask TEXT,
               polymarket_down_ask TEXT,
               direction_a_cost TEXT,
               direction_a_fees TEXT,
               direction_a_net_edge TEXT,
               direction_a_size TEXT,
               direction_b_cost TEXT,
               direction_b_fees TEXT,
               direction_b_net_edge TEXT,
               direction_b_size TEXT,
               scan_latency_ms INTEGER,
               direction_a_quote_skew_ms INTEGER,
               direction_b_quote_skew_ms INTEGER,
               remaining_time_ms INTEGER,
               binance_reference_price TEXT,
               binance_reference_at_ms INTEGER,
               polymarket_reference_price TEXT,
               polymarket_reference_at_ms INTEGER,
               FOREIGN KEY(slug) REFERENCES market_pairs(slug)
             );
             CREATE INDEX IF NOT EXISTS observations_slug_time
               ON observations(slug, observed_at_ms DESC);
             CREATE TABLE IF NOT EXISTS paper_trades (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               slug TEXT NOT NULL,
               asset TEXT NOT NULL,
               duration TEXT NOT NULL,
               direction TEXT NOT NULL,
               classification TEXT NOT NULL,
               detected_at_ms INTEGER NOT NULL,
               quantity TEXT NOT NULL,
               total_cost TEXT NOT NULL,
               total_fees TEXT NOT NULL,
               expected_profit TEXT NOT NULL,
               actual_payout TEXT,
               realized_profit TEXT,
               settled_at_ms INTEGER,
               status TEXT NOT NULL,
               fill_model TEXT NOT NULL,
               UNIQUE(slug, direction)
             );
             CREATE TABLE IF NOT EXISTS settlement_comparisons (
               slug TEXT PRIMARY KEY,
               binance_status TEXT NOT NULL,
               binance_start_price TEXT,
               binance_end_price TEXT,
               binance_outcome TEXT,
               polymarket_status TEXT NOT NULL,
               polymarket_price_to_beat TEXT,
               polymarket_final_price TEXT,
               polymarket_outcome TEXT,
               relationship TEXT NOT NULL,
               checked_at_ms INTEGER NOT NULL,
               FOREIGN KEY(slug) REFERENCES market_pairs(slug)
             );
             UPDATE observations
             SET direction_a_cost=NULL, direction_a_fees=NULL,
                 direction_a_net_edge=NULL, direction_a_size=NULL,
                 direction_b_cost=NULL, direction_b_fees=NULL,
                 direction_b_net_edge=NULL, direction_b_size=NULL
             WHERE slug IN (
               SELECT slug FROM market_pairs WHERE classification='incompatible'
             );",
        )?;
        let has_topic_id = connection
            .prepare("PRAGMA table_info(market_pairs)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|name| name == "binance_topic_id");
        if !has_topic_id {
            connection.execute(
                "ALTER TABLE market_pairs ADD COLUMN binance_topic_id INTEGER",
                [],
            )?;
        }
        for (name, definition) in [
            ("scan_latency_ms", "INTEGER"),
            ("direction_a_quote_skew_ms", "INTEGER"),
            ("direction_b_quote_skew_ms", "INTEGER"),
            ("remaining_time_ms", "INTEGER"),
            ("binance_reference_price", "TEXT"),
            ("binance_reference_at_ms", "INTEGER"),
            ("polymarket_reference_price", "TEXT"),
            ("polymarket_reference_at_ms", "INTEGER"),
        ] {
            let exists = connection
                .prepare("PRAGMA table_info(observations)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .iter()
                .any(|column| column == name);
            if !exists {
                connection.execute(
                    &format!("ALTER TABLE observations ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        for (name, definition) in [
            ("actual_payout", "TEXT"),
            ("realized_profit", "TEXT"),
            ("settled_at_ms", "INTEGER"),
        ] {
            let exists = connection
                .prepare("PRAGMA table_info(paper_trades)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .iter()
                .any(|column| column == name);
            if !exists {
                connection.execute(
                    &format!("ALTER TABLE paper_trades ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn upsert_pair(&self, pair: &TrackedPairView, updated_at_ms: i64) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO market_pairs (
               slug, binance_topic_id, asset, duration, classification, differences_json,
               binance_source, polymarket_source, start_ms, end_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(slug) DO UPDATE SET
               binance_topic_id=excluded.binance_topic_id,
               classification=excluded.classification,
               differences_json=excluded.differences_json,
               updated_at_ms=excluded.updated_at_ms",
            params![
                pair.slug,
                pair.binance_topic_id,
                pair.asset,
                pair.duration,
                pair.classification,
                serde_json::to_string(&pair.differences).unwrap_or_else(|_| "[]".into()),
                pair.binance_source,
                pair.polymarket_source,
                pair.start_ms,
                pair.end_ms,
                updated_at_ms,
            ],
        )?;
        Ok(())
    }

    pub fn observation_statistics(&self) -> rusqlite::Result<(u64, u64)> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection
            .prepare("SELECT direction_a_net_edge, direction_b_net_edge FROM observations")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })?;
        let mut observations = 0_u64;
        let mut opportunities = 0_u64;
        for row in rows {
            let (direction_a, direction_b) = row?;
            observations += 1;
            if [direction_a, direction_b]
                .into_iter()
                .flatten()
                .filter_map(|value| Decimal::from_str(&value).ok())
                .any(|value| value > Decimal::ZERO)
            {
                opportunities += 1;
            }
        }
        Ok((observations, opportunities))
    }

    pub fn insert_observation(&self, observation: &ObservationView) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO observations (
               slug, observed_at_ms,
               binance_up_ask, binance_down_ask, polymarket_up_ask, polymarket_down_ask,
               direction_a_cost, direction_a_fees, direction_a_net_edge, direction_a_size,
               direction_b_cost, direction_b_fees, direction_b_net_edge, direction_b_size,
               scan_latency_ms, direction_a_quote_skew_ms, direction_b_quote_skew_ms,
               remaining_time_ms, binance_reference_price, binance_reference_at_ms,
               polymarket_reference_price, polymarket_reference_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                       ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                observation.slug,
                observation.observed_at_ms,
                observation.binance_up_ask,
                observation.binance_down_ask,
                observation.polymarket_up_ask,
                observation.polymarket_down_ask,
                observation.direction_a_cost,
                observation.direction_a_fees,
                observation.direction_a_net_edge,
                observation.direction_a_size,
                observation.direction_b_cost,
                observation.direction_b_fees,
                observation.direction_b_net_edge,
                observation.direction_b_size,
                observation.scan_latency_ms,
                observation.direction_a_quote_skew_ms,
                observation.direction_b_quote_skew_ms,
                observation.remaining_time_ms,
                observation.binance_reference_price,
                observation.binance_reference_at_ms,
                observation.polymarket_reference_price,
                observation.polymarket_reference_at_ms,
            ],
        )?;
        Ok(())
    }

    pub fn insert_paper_trade(&self, trade: &PaperTradeView) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT OR IGNORE INTO paper_trades (
               slug, asset, duration, direction, classification, detected_at_ms,
               quantity, total_cost, total_fees, expected_profit,
               actual_payout, realized_profit, settled_at_ms, status, fill_model
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                trade.slug,
                trade.asset,
                trade.duration,
                trade.direction,
                trade.classification,
                trade.detected_at_ms,
                trade.quantity,
                trade.total_cost,
                trade.total_fees,
                trade.expected_profit,
                trade.actual_payout,
                trade.realized_profit,
                trade.settled_at_ms,
                trade.status,
                trade.fill_model,
            ],
        )?;
        Ok(())
    }

    pub fn history(&self, slug: &str, limit: usize) -> rusqlite::Result<Vec<ObservationView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT slug, observed_at_ms,
                    binance_up_ask, binance_down_ask, polymarket_up_ask, polymarket_down_ask,
                    direction_a_cost, direction_a_fees, direction_a_net_edge, direction_a_size,
                    direction_b_cost, direction_b_fees, direction_b_net_edge, direction_b_size,
                    scan_latency_ms, direction_a_quote_skew_ms, direction_b_quote_skew_ms,
                    remaining_time_ms, binance_reference_price, binance_reference_at_ms,
                    polymarket_reference_price, polymarket_reference_at_ms
             FROM observations WHERE slug=?1 ORDER BY observed_at_ms DESC LIMIT ?2",
        )?;
        let mut values: Vec<_> = statement
            .query_map(params![slug, limit as i64], observation_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        values.reverse();
        Ok(values)
    }

    pub fn paper_trades(&self, limit: usize) -> rusqlite::Result<Vec<PaperTradeView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, slug, asset, duration, direction, classification, detected_at_ms,
                    quantity, total_cost, total_fees, expected_profit,
                    actual_payout, realized_profit, settled_at_ms, status, fill_model
             FROM paper_trades ORDER BY detected_at_ms DESC LIMIT ?1",
        )?;
        let trades = statement
            .query_map(params![limit as i64], |row| {
                Ok(PaperTradeView {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    asset: row.get(2)?,
                    duration: row.get(3)?,
                    direction: row.get(4)?,
                    classification: row.get(5)?,
                    detected_at_ms: row.get(6)?,
                    quantity: row.get(7)?,
                    total_cost: row.get(8)?,
                    total_fees: row.get(9)?,
                    expected_profit: row.get(10)?,
                    actual_payout: row.get(11)?,
                    realized_profit: row.get(12)?,
                    settled_at_ms: row.get(13)?,
                    status: row.get(14)?,
                    fill_model: row.get(15)?,
                })
            })?
            .collect();
        trades
    }

    pub fn rankings(&self) -> rusqlite::Result<Vec<RankingView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT p.slug, p.asset, p.duration, p.classification,
                    o.direction_a_net_edge, o.direction_b_net_edge,
                    o.direction_a_size, o.direction_b_size, o.observed_at_ms,
                    o.scan_latency_ms, o.direction_a_quote_skew_ms, o.direction_b_quote_skew_ms
             FROM market_pairs p JOIN observations o ON o.slug=p.slug
             ORDER BY p.slug, o.observed_at_ms",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<i64>>(11)?,
            ))
        })?;
        let mut aggregates: HashMap<String, RankingAccumulator> = HashMap::new();
        for row in rows {
            let (
                slug,
                asset,
                duration,
                classification,
                a,
                b,
                a_size,
                b_size,
                observed_at_ms,
                scan_latency_ms,
                a_skew_ms,
                b_skew_ms,
            ) = row?;
            let aggregate = aggregates
                .entry(slug.clone())
                .or_insert(RankingAccumulator {
                    slug,
                    asset,
                    duration,
                    classification,
                    samples: 0,
                    evaluated_directions: 0,
                    signals: 0,
                    maximum_net_edge: Decimal::ZERO,
                    positive_edge_sum: Decimal::ZERO,
                    maximum_executable_profit: Decimal::ZERO,
                    longest_positive_run_ms: 0,
                    active_a_since_ms: None,
                    active_b_since_ms: None,
                    previous_observed_at_ms: None,
                    latency_sum_ms: 0,
                    latency_samples: 0,
                    maximum_quote_skew_ms: 0,
                });
            aggregate.samples += 1;
            if let Some(latency) = scan_latency_ms {
                aggregate.latency_sum_ms += latency.max(0) as u64;
                aggregate.latency_samples += 1;
            }
            for skew in [a_skew_ms, b_skew_ms].into_iter().flatten() {
                aggregate.maximum_quote_skew_ms = aggregate.maximum_quote_skew_ms.max(skew.max(0));
            }
            let has_gap = aggregate
                .previous_observed_at_ms
                .is_some_and(|previous| observed_at_ms.saturating_sub(previous) > 5_000);
            if has_gap {
                aggregate.active_a_since_ms = None;
                aggregate.active_b_since_ms = None;
            }
            for (direction, value, size) in [(0, a, a_size), (1, b, b_size)] {
                if let Ok(value) = Decimal::from_str(value.as_deref().unwrap_or_default()) {
                    aggregate.evaluated_directions += 1;
                    if value > Decimal::ZERO {
                        aggregate.signals += 1;
                        aggregate.positive_edge_sum += value;
                        aggregate.maximum_net_edge = aggregate.maximum_net_edge.max(value);
                        if let Some(size) = size.as_deref().and_then(|s| Decimal::from_str(s).ok())
                        {
                            aggregate.maximum_executable_profit =
                                aggregate.maximum_executable_profit.max(value * size);
                        }
                        let active_since = if direction == 0 {
                            &mut aggregate.active_a_since_ms
                        } else {
                            &mut aggregate.active_b_since_ms
                        };
                        let start = *active_since.get_or_insert(observed_at_ms);
                        aggregate.longest_positive_run_ms = aggregate
                            .longest_positive_run_ms
                            .max(observed_at_ms.saturating_sub(start));
                    } else if direction == 0 {
                        aggregate.active_a_since_ms = None;
                    } else {
                        aggregate.active_b_since_ms = None;
                    }
                } else if direction == 0 {
                    aggregate.active_a_since_ms = None;
                } else {
                    aggregate.active_b_since_ms = None;
                }
            }
            aggregate.previous_observed_at_ms = Some(observed_at_ms);
        }
        let mut rankings: Vec<_> = aggregates
            .into_values()
            .map(|value| {
                let signal_rate = if value.evaluated_directions == 0 {
                    Decimal::ZERO
                } else {
                    Decimal::from(value.signals) / Decimal::from(value.evaluated_directions)
                };
                let average_positive_edge = if value.signals == 0 {
                    Decimal::ZERO
                } else {
                    value.positive_edge_sum / Decimal::from(value.signals)
                };
                RankingView {
                    slug: value.slug,
                    asset: value.asset,
                    duration: value.duration,
                    classification: value.classification,
                    samples: value.samples,
                    evaluated_directions: value.evaluated_directions,
                    signals: value.signals,
                    signal_rate: signal_rate.to_string(),
                    maximum_net_edge: value.maximum_net_edge.to_string(),
                    average_positive_edge: average_positive_edge.to_string(),
                    maximum_executable_profit: value.maximum_executable_profit.to_string(),
                    longest_positive_run_ms: value.longest_positive_run_ms,
                    average_scan_latency_ms: if value.latency_samples == 0 {
                        None
                    } else {
                        Some(value.latency_sum_ms / value.latency_samples)
                    },
                    maximum_quote_skew_ms: (value.maximum_quote_skew_ms > 0)
                        .then_some(value.maximum_quote_skew_ms),
                }
            })
            .collect();
        rankings.sort_by(|left, right| {
            Decimal::from_str(&right.average_positive_edge)
                .unwrap_or_default()
                .cmp(&Decimal::from_str(&left.average_positive_edge).unwrap_or_default())
        });
        Ok(rankings)
    }

    pub fn pending_settlements(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> rusqlite::Result<Vec<PendingSettlement>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT p.slug, p.binance_topic_id
             FROM market_pairs p
             LEFT JOIN settlement_comparisons s ON s.slug=p.slug
             WHERE p.binance_topic_id IS NOT NULL
               AND p.end_ms <= ?1
               AND (s.slug IS NULL OR s.relationship='pending')
             ORDER BY p.end_ms DESC LIMIT ?2",
        )?;
        let pending = statement
            .query_map(params![now_ms, limit as i64], |row| {
                Ok(PendingSettlement {
                    slug: row.get(0)?,
                    binance_topic_id: row.get(1)?,
                })
            })?
            .collect();
        pending
    }

    pub fn upsert_settlement(&self, update: &SettlementUpdate) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO settlement_comparisons (
               slug, binance_status, binance_start_price, binance_end_price, binance_outcome,
               polymarket_status, polymarket_price_to_beat, polymarket_final_price,
               polymarket_outcome, relationship, checked_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(slug) DO UPDATE SET
               binance_status=excluded.binance_status,
               binance_start_price=excluded.binance_start_price,
               binance_end_price=excluded.binance_end_price,
               binance_outcome=excluded.binance_outcome,
               polymarket_status=excluded.polymarket_status,
               polymarket_price_to_beat=excluded.polymarket_price_to_beat,
               polymarket_final_price=excluded.polymarket_final_price,
               polymarket_outcome=excluded.polymarket_outcome,
               relationship=excluded.relationship,
               checked_at_ms=excluded.checked_at_ms",
            params![
                update.slug,
                update.binance_status,
                update.binance_start_price,
                update.binance_end_price,
                update.binance_outcome,
                update.polymarket_status,
                update.polymarket_price_to_beat,
                update.polymarket_final_price,
                update.polymarket_outcome,
                update.relationship,
                update.checked_at_ms,
            ],
        )?;
        Ok(())
    }

    pub fn settle_paper_trades(&self, update: &SettlementUpdate) -> rusqlite::Result<()> {
        let (Some(binance_outcome), Some(polymarket_outcome)) =
            (&update.binance_outcome, &update.polymarket_outcome)
        else {
            return Ok(());
        };
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, direction, quantity, total_cost, total_fees
             FROM paper_trades WHERE slug=?1 AND status='open'",
        )?;
        let rows = statement
            .query_map(params![update.slug], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for (id, direction, quantity, total_cost, total_fees) in rows {
            let Ok(quantity) = Decimal::from_str(&quantity) else {
                continue;
            };
            let Ok(total_cost) = Decimal::from_str(&total_cost) else {
                continue;
            };
            let Ok(total_fees) = Decimal::from_str(&total_fees) else {
                continue;
            };
            let payout =
                paper_trade_payout(&direction, quantity, binance_outcome, polymarket_outcome);
            let realized_profit = payout - total_cost - total_fees;
            connection.execute(
                "UPDATE paper_trades
                 SET actual_payout=?1, realized_profit=?2, settled_at_ms=?3, status='settled'
                 WHERE id=?4",
                params![
                    payout.to_string(),
                    realized_profit.to_string(),
                    update.checked_at_ms,
                    id
                ],
            )?;
        }
        Ok(())
    }

    pub fn settle_stored_paper_trades(&self) -> rusqlite::Result<()> {
        let updates = {
            let connection = self.connection.lock().expect("SQLite mutex poisoned");
            let mut statement = connection.prepare(
                "SELECT DISTINCT s.slug, s.binance_outcome, s.polymarket_outcome, s.checked_at_ms
                 FROM settlement_comparisons s
                 JOIN paper_trades t ON t.slug=s.slug
                 WHERE t.status='open'
                   AND s.binance_outcome IS NOT NULL
                   AND s.polymarket_outcome IS NOT NULL",
            )?;
            let updates = statement
                .query_map([], |row| {
                    Ok(SettlementUpdate {
                        slug: row.get(0)?,
                        binance_status: String::new(),
                        binance_start_price: None,
                        binance_end_price: None,
                        binance_outcome: row.get(1)?,
                        polymarket_status: String::new(),
                        polymarket_price_to_beat: None,
                        polymarket_final_price: None,
                        polymarket_outcome: row.get(2)?,
                        relationship: String::new(),
                        checked_at_ms: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            updates
        };
        for update in updates {
            self.settle_paper_trades(&update)?;
        }
        Ok(())
    }

    pub fn settlements(&self, limit: usize) -> rusqlite::Result<Vec<SettlementView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT s.slug, p.asset, p.duration, p.classification, p.end_ms,
                    s.binance_status, s.binance_start_price, s.binance_end_price,
                    s.binance_outcome, s.polymarket_status, s.polymarket_price_to_beat,
                    s.polymarket_final_price, s.polymarket_outcome, s.relationship,
                    s.checked_at_ms
             FROM settlement_comparisons s
             JOIN market_pairs p ON p.slug=s.slug
             ORDER BY p.end_ms DESC LIMIT ?1",
        )?;
        let settlements = statement
            .query_map(params![limit as i64], |row| {
                Ok(SettlementView {
                    slug: row.get(0)?,
                    asset: row.get(1)?,
                    duration: row.get(2)?,
                    classification: row.get(3)?,
                    end_ms: row.get(4)?,
                    binance_status: row.get(5)?,
                    binance_start_price: row.get(6)?,
                    binance_end_price: row.get(7)?,
                    binance_outcome: row.get(8)?,
                    polymarket_status: row.get(9)?,
                    polymarket_price_to_beat: row.get(10)?,
                    polymarket_final_price: row.get(11)?,
                    polymarket_outcome: row.get(12)?,
                    relationship: row.get(13)?,
                    checked_at_ms: row.get(14)?,
                })
            })?
            .collect();
        settlements
    }
}

#[derive(Debug, Clone)]
pub struct PendingSettlement {
    pub slug: String,
    pub binance_topic_id: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SettlementView {
    pub slug: String,
    pub asset: String,
    pub duration: String,
    pub classification: String,
    pub end_ms: i64,
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

fn observation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservationView> {
    Ok(ObservationView {
        slug: row.get(0)?,
        observed_at_ms: row.get(1)?,
        binance_up_ask: row.get(2)?,
        binance_down_ask: row.get(3)?,
        polymarket_up_ask: row.get(4)?,
        polymarket_down_ask: row.get(5)?,
        direction_a_cost: row.get(6)?,
        direction_a_fees: row.get(7)?,
        direction_a_net_edge: row.get(8)?,
        direction_a_size: row.get(9)?,
        direction_b_cost: row.get(10)?,
        direction_b_fees: row.get(11)?,
        direction_b_net_edge: row.get(12)?,
        direction_b_size: row.get(13)?,
        scan_latency_ms: row.get(14)?,
        direction_a_quote_skew_ms: row.get(15)?,
        direction_b_quote_skew_ms: row.get(16)?,
        remaining_time_ms: row.get(17)?,
        binance_reference_price: row.get(18)?,
        binance_reference_at_ms: row.get(19)?,
        polymarket_reference_price: row.get(20)?,
        polymarket_reference_at_ms: row.get(21)?,
    })
}

struct RankingAccumulator {
    slug: String,
    asset: String,
    duration: String,
    classification: String,
    samples: u64,
    evaluated_directions: u64,
    signals: u64,
    maximum_net_edge: Decimal,
    positive_edge_sum: Decimal,
    maximum_executable_profit: Decimal,
    longest_positive_run_ms: i64,
    active_a_since_ms: Option<i64>,
    active_b_since_ms: Option<i64>,
    previous_observed_at_ms: Option<i64>,
    latency_sum_ms: u64,
    latency_samples: u64,
    maximum_quote_skew_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RankingView {
    pub slug: String,
    pub asset: String,
    pub duration: String,
    pub classification: String,
    pub samples: u64,
    pub evaluated_directions: u64,
    pub signals: u64,
    pub signal_rate: String,
    pub maximum_net_edge: String,
    pub average_positive_edge: String,
    pub maximum_executable_profit: String,
    pub longest_positive_run_ms: i64,
    pub average_scan_latency_ms: Option<u64>,
    pub maximum_quote_skew_ms: Option<i64>,
}

fn paper_trade_payout(
    direction: &str,
    quantity: Decimal,
    binance_outcome: &str,
    polymarket_outcome: &str,
) -> Decimal {
    let (binance_leg, polymarket_leg) = if direction == "A" {
        ("Up", "Down")
    } else {
        ("Down", "Up")
    };
    let winning_legs = i64::from(binance_outcome.eq_ignore_ascii_case(binance_leg))
        + i64::from(polymarket_outcome.eq_ignore_ascii_case(polymarket_leg));
    quantity * Decimal::from(winning_legs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_payout_for_matching_and_divergent_settlements() {
        let quantity = Decimal::from(100);
        assert_eq!(paper_trade_payout("A", quantity, "Up", "Up"), quantity);
        assert_eq!(
            paper_trade_payout("A", quantity, "Up", "Down"),
            quantity * Decimal::TWO
        );
        assert_eq!(
            paper_trade_payout("B", quantity, "Up", "Down"),
            Decimal::ZERO
        );
    }
}
