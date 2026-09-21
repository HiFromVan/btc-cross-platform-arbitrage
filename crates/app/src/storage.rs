use crate::basis::{BinanceBasisView, BinanceDeliveryBasisView, BinanceDeliveryRequoteView};
use crate::scanner::{
    BinanceInternalView, ObservationView, PaperTradeView, PolymarketInternalView, SettlementUpdate,
    ShadowExecutionView, TrackedPairView,
};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
               direction_a_risk_buffers TEXT,
               direction_a_size TEXT,
               direction_a_rejections_json TEXT,
               direction_b_cost TEXT,
               direction_b_fees TEXT,
               direction_b_net_edge TEXT,
               direction_b_risk_buffers TEXT,
               direction_b_size TEXT,
               direction_b_rejections_json TEXT,
               scan_latency_ms INTEGER,
               direction_a_quote_skew_ms INTEGER,
               direction_b_quote_skew_ms INTEGER,
               direction_a_quote_age_ms INTEGER,
               direction_b_quote_age_ms INTEGER,
               remaining_time_ms INTEGER,
               binance_reference_price TEXT,
               binance_reference_at_ms INTEGER,
               polymarket_reference_price TEXT,
               polymarket_reference_at_ms INTEGER,
               binance_internal_json TEXT,
               polymarket_internal_json TEXT,
               FOREIGN KEY(slug) REFERENCES market_pairs(slug)
             );
             CREATE INDEX IF NOT EXISTS observations_slug_time
               ON observations(slug, observed_at_ms DESC);
             CREATE INDEX IF NOT EXISTS observations_time
               ON observations(observed_at_ms DESC);
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
               total_risk_buffers TEXT NOT NULL DEFAULT '0',
               expected_profit TEXT NOT NULL,
               actual_payout TEXT,
               realized_profit TEXT,
               settled_at_ms INTEGER,
               status TEXT NOT NULL,
               fill_model TEXT NOT NULL,
               UNIQUE(slug, direction, fill_model)
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
             CREATE TABLE IF NOT EXISTS shadow_executions (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               slug TEXT NOT NULL,
               asset TEXT NOT NULL,
               direction TEXT NOT NULL,
               quoted_at_ms INTEGER NOT NULL,
               executed_at_ms INTEGER NOT NULL,
               requested_quantity TEXT NOT NULL,
               first_limit_price TEXT NOT NULL,
               second_limit_price TEXT NOT NULL,
               first_status TEXT NOT NULL,
               first_filled_quantity TEXT NOT NULL,
               first_average_price TEXT,
               first_total_cost TEXT NOT NULL,
               second_status TEXT NOT NULL,
               second_filled_quantity TEXT NOT NULL,
               second_average_price TEXT,
               second_total_cost TEXT NOT NULL,
               unhedged_quantity TEXT NOT NULL,
               status TEXT NOT NULL,
               trigger_source TEXT NOT NULL,
               error TEXT
             );
             CREATE INDEX IF NOT EXISTS shadow_executions_time
               ON shadow_executions(executed_at_ms DESC);
             CREATE TABLE IF NOT EXISTS binance_basis_observations (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               symbol TEXT NOT NULL,
               observed_at_ms INTEGER NOT NULL,
               quantity TEXT NOT NULL,
               spot_vwap_ask TEXT NOT NULL,
               perp_vwap_bid TEXT NOT NULL,
               spot_cost TEXT NOT NULL,
               perp_proceeds TEXT NOT NULL,
               entry_basis TEXT NOT NULL,
               entry_basis_rate TEXT NOT NULL,
               estimated_round_trip_fees TEXT NOT NULL,
               slippage_buffer TEXT NOT NULL,
               funding_rate TEXT NOT NULL,
               funding_interval_hours INTEGER NOT NULL,
               funding_income_per_round TEXT NOT NULL,
               projected_net_24h TEXT NOT NULL,
               projected_net_7d TEXT NOT NULL,
               funding_apr TEXT NOT NULL,
               break_even_rounds TEXT,
               mark_price TEXT NOT NULL,
               index_price TEXT NOT NULL,
               next_funding_time_ms INTEGER NOT NULL,
               response_skew_ms INTEGER NOT NULL,
               premium_age_ms INTEGER NOT NULL,
               scan_latency_ms INTEGER NOT NULL,
               rejections_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS binance_basis_symbol_time
               ON binance_basis_observations(symbol, observed_at_ms DESC);
             CREATE TABLE IF NOT EXISTS binance_delivery_basis_observations (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               symbol TEXT NOT NULL, pair TEXT NOT NULL, contract_type TEXT NOT NULL,
               delivery_time_ms INTEGER NOT NULL, observed_at_ms INTEGER NOT NULL,
               remaining_days TEXT NOT NULL, quantity TEXT NOT NULL,
               quantity_step TEXT NOT NULL DEFAULT '0',
               spot_price_tick TEXT NOT NULL DEFAULT '0',
               delivery_price_tick TEXT NOT NULL DEFAULT '0',
               spot_vwap_ask TEXT NOT NULL, delivery_vwap_bid TEXT NOT NULL,
               spot_cost TEXT NOT NULL, delivery_proceeds TEXT NOT NULL,
               gross_basis TEXT NOT NULL, gross_basis_rate TEXT NOT NULL,
               estimated_round_trip_fees TEXT NOT NULL, slippage_buffer TEXT NOT NULL,
               settlement_buffer TEXT NOT NULL DEFAULT '0',
               capital_cost TEXT NOT NULL DEFAULT '0',
               pre_capital_net_profit TEXT NOT NULL DEFAULT '0',
               net_profit_at_delivery TEXT NOT NULL, net_return TEXT NOT NULL,
               net_apr TEXT NOT NULL, mark_price TEXT NOT NULL, index_price TEXT NOT NULL,
               response_skew_ms INTEGER NOT NULL, premium_age_ms INTEGER NOT NULL,
               scan_latency_ms INTEGER NOT NULL, rejections_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS binance_delivery_basis_symbol_time
               ON binance_delivery_basis_observations(symbol, observed_at_ms DESC);
             CREATE TABLE IF NOT EXISTS binance_delivery_requotes (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               symbol TEXT NOT NULL, quoted_at_ms INTEGER NOT NULL,
               checked_at_ms INTEGER NOT NULL, delay_ms INTEGER NOT NULL,
               quantity TEXT NOT NULL, spot_limit_price TEXT NOT NULL,
               delivery_limit_price TEXT NOT NULL, spot_fillable INTEGER NOT NULL,
               delivery_fillable INTEGER NOT NULL, spot_cost TEXT,
               delivery_proceeds TEXT, gross_basis TEXT, status TEXT NOT NULL, error TEXT
             );
             CREATE INDEX IF NOT EXISTS binance_delivery_requotes_time
               ON binance_delivery_requotes(checked_at_ms DESC);
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
            ("direction_a_quote_age_ms", "INTEGER"),
            ("direction_b_quote_age_ms", "INTEGER"),
            ("direction_a_risk_buffers", "TEXT"),
            ("direction_b_risk_buffers", "TEXT"),
            ("direction_a_rejections_json", "TEXT"),
            ("direction_b_rejections_json", "TEXT"),
            ("remaining_time_ms", "INTEGER"),
            ("binance_reference_price", "TEXT"),
            ("binance_reference_at_ms", "INTEGER"),
            ("polymarket_reference_price", "TEXT"),
            ("polymarket_reference_at_ms", "INTEGER"),
            ("binance_internal_json", "TEXT"),
            ("polymarket_internal_json", "TEXT"),
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
            ("quantity_step", "TEXT NOT NULL DEFAULT '0'"),
            ("spot_price_tick", "TEXT NOT NULL DEFAULT '0'"),
            ("delivery_price_tick", "TEXT NOT NULL DEFAULT '0'"),
            ("settlement_buffer", "TEXT NOT NULL DEFAULT '0'"),
            ("capital_cost", "TEXT NOT NULL DEFAULT '0'"),
            ("pre_capital_net_profit", "TEXT NOT NULL DEFAULT '0'"),
        ] {
            let exists = connection
                .prepare("PRAGMA table_info(binance_delivery_basis_observations)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .iter()
                .any(|column| column == name);
            if !exists {
                connection.execute(
                    &format!("ALTER TABLE binance_delivery_basis_observations ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        for (name, definition) in [
            ("actual_payout", "TEXT"),
            ("realized_profit", "TEXT"),
            ("settled_at_ms", "INTEGER"),
            ("total_risk_buffers", "TEXT NOT NULL DEFAULT '0'"),
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
        let paper_table_sql: String = connection.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_trades'",
            [],
            |row| row.get(0),
        )?;
        if paper_table_sql.contains("UNIQUE(slug, direction)")
            && !paper_table_sql.contains("UNIQUE(slug, direction, fill_model)")
        {
            connection.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE paper_trades_v2 (
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
                   total_risk_buffers TEXT NOT NULL DEFAULT '0',
                   expected_profit TEXT NOT NULL,
                   actual_payout TEXT,
                   realized_profit TEXT,
                   settled_at_ms INTEGER,
                   status TEXT NOT NULL,
                   fill_model TEXT NOT NULL,
                   UNIQUE(slug, direction, fill_model)
                 );
                 INSERT INTO paper_trades_v2 (
                   id, slug, asset, duration, direction, classification, detected_at_ms,
                   quantity, total_cost, total_fees, total_risk_buffers, expected_profit,
                   actual_payout, realized_profit, settled_at_ms, status, fill_model
                 )
                 SELECT id, slug, asset, duration, direction, classification, detected_at_ms,
                        quantity, total_cost, total_fees, total_risk_buffers, expected_profit,
                        actual_payout, realized_profit, settled_at_ms, status, fill_model
                 FROM paper_trades;
                 DROP TABLE paper_trades;
                 ALTER TABLE paper_trades_v2 RENAME TO paper_trades;
                 COMMIT;",
            )?;
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
               direction_a_risk_buffers, direction_a_rejections_json,
               direction_b_risk_buffers, direction_b_rejections_json,
               scan_latency_ms, direction_a_quote_skew_ms, direction_b_quote_skew_ms,
               direction_a_quote_age_ms, direction_b_quote_age_ms,
               remaining_time_ms, binance_reference_price, binance_reference_at_ms,
               polymarket_reference_price, polymarket_reference_at_ms, binance_internal_json,
               polymarket_internal_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                       ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29,
                       ?30)",
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
                observation.direction_a_risk_buffers,
                serde_json::to_string(&observation.direction_a_rejections).unwrap_or_default(),
                observation.direction_b_risk_buffers,
                serde_json::to_string(&observation.direction_b_rejections).unwrap_or_default(),
                observation.scan_latency_ms,
                observation.direction_a_quote_skew_ms,
                observation.direction_b_quote_skew_ms,
                observation.direction_a_quote_age_ms,
                observation.direction_b_quote_age_ms,
                observation.remaining_time_ms,
                observation.binance_reference_price,
                observation.binance_reference_at_ms,
                observation.polymarket_reference_price,
                observation.polymarket_reference_at_ms,
                observation
                    .binance_internal
                    .as_ref()
                    .and_then(|value| serde_json::to_string(value).ok()),
                observation
                    .polymarket_internal
                    .as_ref()
                    .and_then(|value| serde_json::to_string(value).ok()),
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
               total_risk_buffers, actual_payout, realized_profit, settled_at_ms, status, fill_model
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
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
                trade.total_risk_buffers,
                trade.actual_payout,
                trade.realized_profit,
                trade.settled_at_ms,
                trade.status,
                trade.fill_model,
            ],
        )?;
        Ok(())
    }

    pub fn insert_shadow_execution(&self, execution: &ShadowExecutionView) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO shadow_executions (
               slug, asset, direction, quoted_at_ms, executed_at_ms, requested_quantity,
               first_limit_price, second_limit_price, first_status, first_filled_quantity,
               first_average_price, first_total_cost, second_status, second_filled_quantity,
               second_average_price, second_total_cost, unhedged_quantity, status,
               trigger_source, error
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
               ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
             )",
            params![
                execution.slug,
                execution.asset,
                execution.direction,
                execution.quoted_at_ms,
                execution.executed_at_ms,
                execution.requested_quantity,
                execution.first_limit_price,
                execution.second_limit_price,
                execution.first_status,
                execution.first_filled_quantity,
                execution.first_average_price,
                execution.first_total_cost,
                execution.second_status,
                execution.second_filled_quantity,
                execution.second_average_price,
                execution.second_total_cost,
                execution.unhedged_quantity,
                execution.status,
                execution.trigger_source,
                execution.error,
            ],
        )?;
        Ok(())
    }

    pub fn insert_binance_basis(&self, value: &BinanceBasisView) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO binance_basis_observations (
               symbol, observed_at_ms, quantity, spot_vwap_ask, perp_vwap_bid,
               spot_cost, perp_proceeds, entry_basis, entry_basis_rate,
               estimated_round_trip_fees, slippage_buffer, funding_rate,
               funding_interval_hours, funding_income_per_round,
               projected_net_24h, projected_net_7d, funding_apr, break_even_rounds,
               mark_price, index_price, next_funding_time_ms, response_skew_ms,
               premium_age_ms, scan_latency_ms, rejections_json
             ) VALUES (
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
               ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
               ?21, ?22, ?23, ?24, ?25
             )",
            params![
                value.symbol,
                value.observed_at_ms,
                value.quantity,
                value.spot_vwap_ask,
                value.perp_vwap_bid,
                value.spot_cost,
                value.perp_proceeds,
                value.entry_basis,
                value.entry_basis_rate,
                value.estimated_round_trip_fees,
                value.slippage_buffer,
                value.funding_rate,
                value.funding_interval_hours,
                value.funding_income_per_round,
                value.projected_net_24h,
                value.projected_net_7d,
                value.funding_apr,
                value.break_even_rounds,
                value.mark_price,
                value.index_price,
                value.next_funding_time_ms,
                value.response_skew_ms,
                value.premium_age_ms,
                value.scan_latency_ms,
                serde_json::to_string(&value.rejections).unwrap_or_else(|_| "[]".into()),
            ],
        )?;
        Ok(())
    }

    pub fn binance_basis_latest(&self) -> rusqlite::Result<Vec<BinanceBasisView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT symbol, observed_at_ms, quantity, spot_vwap_ask, perp_vwap_bid,
                    spot_cost, perp_proceeds, entry_basis, entry_basis_rate,
                    estimated_round_trip_fees, slippage_buffer, funding_rate,
                    funding_interval_hours, funding_income_per_round,
                    projected_net_24h, projected_net_7d, funding_apr, break_even_rounds,
                    mark_price, index_price, next_funding_time_ms, response_skew_ms,
                    premium_age_ms, scan_latency_ms, rejections_json
             FROM (
               SELECT *, ROW_NUMBER() OVER (
                 PARTITION BY symbol ORDER BY observed_at_ms DESC
               ) AS row_number
               FROM binance_basis_observations
             )
             WHERE row_number=1 ORDER BY symbol",
        )?;
        let values = statement
            .query_map([], binance_basis_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(values)
    }

    pub fn binance_basis_history(
        &self,
        symbol: &str,
        limit: usize,
    ) -> rusqlite::Result<Vec<BinanceBasisView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT symbol, observed_at_ms, quantity, spot_vwap_ask, perp_vwap_bid,
                    spot_cost, perp_proceeds, entry_basis, entry_basis_rate,
                    estimated_round_trip_fees, slippage_buffer, funding_rate,
                    funding_interval_hours, funding_income_per_round,
                    projected_net_24h, projected_net_7d, funding_apr, break_even_rounds,
                    mark_price, index_price, next_funding_time_ms, response_skew_ms,
                    premium_age_ms, scan_latency_ms, rejections_json
             FROM binance_basis_observations
             WHERE symbol=?1 ORDER BY observed_at_ms DESC LIMIT ?2",
        )?;
        let mut values = statement
            .query_map(params![symbol, limit as i64], binance_basis_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        values.reverse();
        Ok(values)
    }

    pub fn insert_binance_delivery_basis(
        &self,
        value: &BinanceDeliveryBasisView,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO binance_delivery_basis_observations (
               symbol,pair,contract_type,delivery_time_ms,observed_at_ms,remaining_days,
               quantity,quantity_step,spot_price_tick,delivery_price_tick,
               spot_vwap_ask,delivery_vwap_bid,spot_cost,delivery_proceeds,
               gross_basis,gross_basis_rate,estimated_round_trip_fees,slippage_buffer,
               settlement_buffer,capital_cost,pre_capital_net_profit,
               net_profit_at_delivery,net_return,net_apr,mark_price,index_price,
               response_skew_ms,premium_age_ms,scan_latency_ms,rejections_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30)",
            params![
                value.symbol,value.pair,value.contract_type,value.delivery_time_ms,
                value.observed_at_ms,value.remaining_days,value.quantity,value.quantity_step,
                value.spot_price_tick,value.delivery_price_tick,value.spot_vwap_ask,
                value.delivery_vwap_bid,value.spot_cost,value.delivery_proceeds,value.gross_basis,
                value.gross_basis_rate,value.estimated_round_trip_fees,value.slippage_buffer,
                value.settlement_buffer,value.capital_cost,value.pre_capital_net_profit,
                value.net_profit_at_delivery,value.net_return,value.net_apr,value.mark_price,
                value.index_price,value.response_skew_ms,value.premium_age_ms,value.scan_latency_ms,
                serde_json::to_string(&value.rejections).unwrap_or_else(|_| "[]".into()),
            ],
        )?;
        Ok(())
    }

    pub fn binance_delivery_basis_latest(&self) -> rusqlite::Result<Vec<BinanceDeliveryBasisView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT symbol,pair,contract_type,delivery_time_ms,observed_at_ms,remaining_days,
                    quantity,quantity_step,spot_price_tick,delivery_price_tick,
                    spot_vwap_ask,delivery_vwap_bid,spot_cost,delivery_proceeds,
                    gross_basis,gross_basis_rate,estimated_round_trip_fees,slippage_buffer,
                    settlement_buffer,capital_cost,pre_capital_net_profit,
                    net_profit_at_delivery,net_return,net_apr,mark_price,index_price,
                    response_skew_ms,premium_age_ms,scan_latency_ms,rejections_json
             FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY observed_at_ms DESC) row_number
                   FROM binance_delivery_basis_observations)
             WHERE row_number=1 ORDER BY delivery_time_ms,pair",
        )?;
        let values = statement
            .query_map([], binance_delivery_basis_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(values)
    }

    pub fn insert_binance_delivery_requote(
        &self,
        value: &BinanceDeliveryRequoteView,
    ) -> rusqlite::Result<()> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        connection.execute(
            "INSERT INTO binance_delivery_requotes (
               symbol,quoted_at_ms,checked_at_ms,delay_ms,quantity,spot_limit_price,
               delivery_limit_price,spot_fillable,delivery_fillable,spot_cost,
               delivery_proceeds,gross_basis,status,error
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                value.symbol,
                value.quoted_at_ms,
                value.checked_at_ms,
                value.delay_ms,
                value.quantity,
                value.spot_limit_price,
                value.delivery_limit_price,
                value.spot_fillable,
                value.delivery_fillable,
                value.spot_cost,
                value.delivery_proceeds,
                value.gross_basis,
                value.status,
                value.error,
            ],
        )?;
        Ok(())
    }

    pub fn binance_delivery_requotes(
        &self,
        limit: usize,
    ) -> rusqlite::Result<Vec<BinanceDeliveryRequoteView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT symbol,quoted_at_ms,checked_at_ms,delay_ms,quantity,spot_limit_price,
                    delivery_limit_price,spot_fillable,delivery_fillable,spot_cost,
                    delivery_proceeds,gross_basis,status,error
             FROM binance_delivery_requotes ORDER BY checked_at_ms DESC LIMIT ?1",
        )?;
        let values = statement
            .query_map(params![limit as i64], |row| {
                Ok(BinanceDeliveryRequoteView {
                    symbol: row.get(0)?,
                    quoted_at_ms: row.get(1)?,
                    checked_at_ms: row.get(2)?,
                    delay_ms: row.get(3)?,
                    quantity: row.get(4)?,
                    spot_limit_price: row.get(5)?,
                    delivery_limit_price: row.get(6)?,
                    spot_fillable: row.get(7)?,
                    delivery_fillable: row.get(8)?,
                    spot_cost: row.get(9)?,
                    delivery_proceeds: row.get(10)?,
                    gross_basis: row.get(11)?,
                    status: row.get(12)?,
                    error: row.get(13)?,
                })
            })?
            .collect();
        values
    }

    pub fn shadow_executions(&self, limit: usize) -> rusqlite::Result<Vec<ShadowExecutionView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, slug, asset, direction, quoted_at_ms, executed_at_ms,
                    requested_quantity, first_limit_price, second_limit_price,
                    first_status, first_filled_quantity, first_average_price, first_total_cost,
                    second_status, second_filled_quantity, second_average_price, second_total_cost,
                    unhedged_quantity, status, trigger_source, error
             FROM shadow_executions ORDER BY executed_at_ms DESC LIMIT ?1",
        )?;
        let executions = statement
            .query_map(params![limit as i64], |row| {
                Ok(ShadowExecutionView {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    asset: row.get(2)?,
                    direction: row.get(3)?,
                    quoted_at_ms: row.get(4)?,
                    executed_at_ms: row.get(5)?,
                    requested_quantity: row.get(6)?,
                    first_limit_price: row.get(7)?,
                    second_limit_price: row.get(8)?,
                    first_status: row.get(9)?,
                    first_filled_quantity: row.get(10)?,
                    first_average_price: row.get(11)?,
                    first_total_cost: row.get(12)?,
                    second_status: row.get(13)?,
                    second_filled_quantity: row.get(14)?,
                    second_average_price: row.get(15)?,
                    second_total_cost: row.get(16)?,
                    unhedged_quantity: row.get(17)?,
                    status: row.get(18)?,
                    trigger_source: row.get(19)?,
                    error: row.get(20)?,
                })
            })?
            .collect();
        executions
    }

    pub fn history(&self, slug: &str, limit: usize) -> rusqlite::Result<Vec<ObservationView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT slug, observed_at_ms,
                    binance_up_ask, binance_down_ask, polymarket_up_ask, polymarket_down_ask,
                    direction_a_cost, direction_a_fees, direction_a_net_edge, direction_a_size,
                    direction_b_cost, direction_b_fees, direction_b_net_edge, direction_b_size,
                    direction_a_risk_buffers, direction_a_rejections_json,
                    direction_b_risk_buffers, direction_b_rejections_json,
                    scan_latency_ms, direction_a_quote_skew_ms, direction_b_quote_skew_ms,
                    direction_a_quote_age_ms, direction_b_quote_age_ms,
                    remaining_time_ms, binance_reference_price, binance_reference_at_ms,
                    polymarket_reference_price, polymarket_reference_at_ms, binance_internal_json,
                    polymarket_internal_json
             FROM observations WHERE slug=?1 ORDER BY observed_at_ms DESC LIMIT ?2",
        )?;
        let mut values: Vec<_> = statement
            .query_map(params![slug, limit as i64], observation_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        values.reverse();
        Ok(values)
    }

    pub fn binance_internal_stats(&self) -> rusqlite::Result<Vec<BinanceInternalStatsView>> {
        self.same_market_stats(
            "binance_internal_json",
            "binance_up_ask",
            "binance_down_ask",
        )
    }

    pub fn polymarket_internal_stats(&self) -> rusqlite::Result<Vec<BinanceInternalStatsView>> {
        self.same_market_stats(
            "polymarket_internal_json",
            "polymarket_up_ask",
            "polymarket_down_ask",
        )
    }

    fn same_market_stats(
        &self,
        json_column: &str,
        up_column: &str,
        down_column: &str,
    ) -> rusqlite::Result<Vec<BinanceInternalStatsView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let cutoff_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(DASHBOARD_STATS_LOOKBACK_MS);
        let sql = format!(
            "SELECT p.asset, p.duration, o.slug, o.observed_at_ms,
                    o.{up_column}, o.{down_column}, o.{json_column}
             FROM observations o JOIN market_pairs p ON p.slug=o.slug
             WHERE o.{json_column} IS NOT NULL AND o.observed_at_ms>=?1
             ORDER BY p.asset, p.duration, o.slug, o.observed_at_ms"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params![cutoff_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;
        let mut aggregates: HashMap<(String, String), BinanceInternalStatsAccumulator> =
            HashMap::new();
        for row in rows {
            let (asset, duration, slug, observed_at_ms, up, down, json) = row?;
            let Ok(value) = serde_json::from_str::<BinanceInternalView>(&json) else {
                continue;
            };
            let Ok(net_edge) = Decimal::from_str(&value.net_edge) else {
                continue;
            };
            let size = Decimal::from_str(&value.size).unwrap_or_default();
            let aggregate = aggregates
                .entry((asset.clone(), duration.clone()))
                .or_insert_with(|| BinanceInternalStatsAccumulator::new(asset, duration));
            aggregate.samples += 1;
            aggregate.windows.insert(slug.clone());
            aggregate.latest_observed_at_ms = aggregate.latest_observed_at_ms.max(observed_at_ms);
            aggregate.maximum_quote_skew_ms =
                aggregate.maximum_quote_skew_ms.max(value.quote_skew_ms);
            let ask_sum_below_one = up
                .as_deref()
                .and_then(|v| Decimal::from_str(v).ok())
                .zip(down.as_deref().and_then(|v| Decimal::from_str(v).ok()))
                .is_some_and(|(up, down)| up + down < Decimal::ONE);
            if ask_sum_below_one {
                aggregate.raw_below_one_samples += 1;
            }
            if net_edge > Decimal::ZERO {
                aggregate.positive_net_samples += 1;
                aggregate.positive_edge_sum += net_edge;
                aggregate.maximum_net_edge = aggregate.maximum_net_edge.max(net_edge);
                aggregate.maximum_theoretical_profit =
                    aggregate.maximum_theoretical_profit.max(net_edge * size);
            }
            if value.rejections.is_empty() {
                aggregate.eligible_samples += 1;
                let active = aggregate
                    .active_by_slug
                    .entry(slug)
                    .or_insert((observed_at_ms, observed_at_ms));
                if observed_at_ms.saturating_sub(active.1) > 15_000 {
                    *active = (observed_at_ms, observed_at_ms);
                } else {
                    active.1 = observed_at_ms;
                }
                if observed_at_ms.saturating_sub(active.0) >= 5_000 {
                    aggregate.confirmed_samples += 1;
                }
            } else {
                aggregate.active_by_slug.remove(&slug);
            }
        }
        let mut values: Vec<_> = aggregates
            .into_values()
            .map(|value| {
                let average_positive_edge = if value.positive_net_samples == 0 {
                    Decimal::ZERO
                } else {
                    value.positive_edge_sum / Decimal::from(value.positive_net_samples)
                };
                BinanceInternalStatsView {
                    asset: value.asset,
                    duration: value.duration,
                    samples: value.samples,
                    windows: value.windows.len() as u64,
                    raw_below_one_samples: value.raw_below_one_samples,
                    positive_net_samples: value.positive_net_samples,
                    eligible_samples: value.eligible_samples,
                    confirmed_samples: value.confirmed_samples,
                    average_positive_edge: average_positive_edge.to_string(),
                    maximum_net_edge: value.maximum_net_edge.to_string(),
                    maximum_theoretical_profit: value.maximum_theoretical_profit.to_string(),
                    maximum_quote_skew_ms: value.maximum_quote_skew_ms,
                    latest_observed_at_ms: value.latest_observed_at_ms,
                }
            })
            .collect();
        values.sort_by(|a, b| a.duration.cmp(&b.duration).then(a.asset.cmp(&b.asset)));
        Ok(values)
    }

    pub fn paper_trades(&self, limit: usize) -> rusqlite::Result<Vec<PaperTradeView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let mut statement = connection.prepare(
            "SELECT id, slug, asset, duration, direction, classification, detected_at_ms,
                    quantity, total_cost, total_fees, total_risk_buffers, expected_profit,
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
                    total_risk_buffers: row.get(10)?,
                    expected_profit: row.get(11)?,
                    actual_payout: row.get(12)?,
                    realized_profit: row.get(13)?,
                    settled_at_ms: row.get(14)?,
                    status: row.get(15)?,
                    fill_model: row.get(16)?,
                })
            })?
            .collect();
        trades
    }

    pub fn rankings(&self) -> rusqlite::Result<Vec<RankingView>> {
        let connection = self.connection.lock().expect("SQLite mutex poisoned");
        let cutoff_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(DASHBOARD_STATS_LOOKBACK_MS);
        let mut statement = connection.prepare(
            "SELECT p.slug, p.asset, p.duration, p.classification,
                    o.direction_a_net_edge, o.direction_b_net_edge,
                    o.direction_a_size, o.direction_b_size, o.observed_at_ms,
                    o.scan_latency_ms, o.direction_a_quote_skew_ms, o.direction_b_quote_skew_ms
             FROM market_pairs p JOIN observations o ON o.slug=p.slug
             WHERE o.observed_at_ms>=?1
             ORDER BY p.slug, o.observed_at_ms",
        )?;
        let rows = statement.query_map(params![cutoff_ms], |row| {
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
            "SELECT id, direction, quantity, total_cost, total_fees, total_risk_buffers
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
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for (id, direction, quantity, total_cost, total_fees, total_risk_buffers) in rows {
            let Ok(quantity) = Decimal::from_str(&quantity) else {
                continue;
            };
            let Ok(total_cost) = Decimal::from_str(&total_cost) else {
                continue;
            };
            let Ok(total_fees) = Decimal::from_str(&total_fees) else {
                continue;
            };
            let Ok(total_risk_buffers) = Decimal::from_str(&total_risk_buffers) else {
                continue;
            };
            let payout =
                paper_trade_payout(&direction, quantity, binance_outcome, polymarket_outcome);
            let realized_profit = payout - total_cost - total_fees - total_risk_buffers;
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
        direction_a_risk_buffers: row.get(14)?,
        direction_a_rejections: row
            .get::<_, Option<String>>(15)?
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        direction_b_risk_buffers: row.get(16)?,
        direction_b_rejections: row
            .get::<_, Option<String>>(17)?
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default(),
        scan_latency_ms: row.get(18)?,
        direction_a_quote_skew_ms: row.get(19)?,
        direction_b_quote_skew_ms: row.get(20)?,
        direction_a_quote_age_ms: row.get(21)?,
        direction_b_quote_age_ms: row.get(22)?,
        remaining_time_ms: row.get(23)?,
        binance_reference_price: row.get(24)?,
        binance_reference_at_ms: row.get(25)?,
        polymarket_reference_price: row.get(26)?,
        polymarket_reference_at_ms: row.get(27)?,
        binance_internal: row
            .get::<_, Option<String>>(28)?
            .and_then(|value| serde_json::from_str(&value).ok()),
        polymarket_internal: row
            .get::<_, Option<String>>(29)?
            .and_then(|value| serde_json::from_str::<PolymarketInternalView>(&value).ok()),
    })
}

fn binance_basis_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BinanceBasisView> {
    Ok(BinanceBasisView {
        symbol: row.get(0)?,
        observed_at_ms: row.get(1)?,
        quantity: row.get(2)?,
        spot_vwap_ask: row.get(3)?,
        perp_vwap_bid: row.get(4)?,
        spot_cost: row.get(5)?,
        perp_proceeds: row.get(6)?,
        entry_basis: row.get(7)?,
        entry_basis_rate: row.get(8)?,
        estimated_round_trip_fees: row.get(9)?,
        slippage_buffer: row.get(10)?,
        funding_rate: row.get(11)?,
        funding_interval_hours: row.get(12)?,
        funding_income_per_round: row.get(13)?,
        projected_net_24h: row.get(14)?,
        projected_net_7d: row.get(15)?,
        funding_apr: row.get(16)?,
        break_even_rounds: row.get(17)?,
        mark_price: row.get(18)?,
        index_price: row.get(19)?,
        next_funding_time_ms: row.get(20)?,
        response_skew_ms: row.get(21)?,
        premium_age_ms: row.get(22)?,
        scan_latency_ms: row.get(23)?,
        rejections: serde_json::from_str(&row.get::<_, String>(24)?).unwrap_or_default(),
    })
}

fn binance_delivery_basis_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BinanceDeliveryBasisView> {
    Ok(BinanceDeliveryBasisView {
        symbol: row.get(0)?,
        pair: row.get(1)?,
        contract_type: row.get(2)?,
        delivery_time_ms: row.get(3)?,
        observed_at_ms: row.get(4)?,
        remaining_days: row.get(5)?,
        quantity: row.get(6)?,
        quantity_step: row.get(7)?,
        spot_price_tick: row.get(8)?,
        delivery_price_tick: row.get(9)?,
        spot_vwap_ask: row.get(10)?,
        delivery_vwap_bid: row.get(11)?,
        spot_cost: row.get(12)?,
        delivery_proceeds: row.get(13)?,
        gross_basis: row.get(14)?,
        gross_basis_rate: row.get(15)?,
        estimated_round_trip_fees: row.get(16)?,
        slippage_buffer: row.get(17)?,
        settlement_buffer: row.get(18)?,
        capital_cost: row.get(19)?,
        pre_capital_net_profit: row.get(20)?,
        net_profit_at_delivery: row.get(21)?,
        net_return: row.get(22)?,
        net_apr: row.get(23)?,
        mark_price: row.get(24)?,
        index_price: row.get(25)?,
        response_skew_ms: row.get(26)?,
        premium_age_ms: row.get(27)?,
        scan_latency_ms: row.get(28)?,
        rejections: serde_json::from_str(&row.get::<_, String>(29)?).unwrap_or_default(),
    })
}

struct BinanceInternalStatsAccumulator {
    asset: String,
    duration: String,
    samples: u64,
    windows: HashSet<String>,
    raw_below_one_samples: u64,
    positive_net_samples: u64,
    eligible_samples: u64,
    confirmed_samples: u64,
    positive_edge_sum: Decimal,
    maximum_net_edge: Decimal,
    maximum_theoretical_profit: Decimal,
    maximum_quote_skew_ms: i64,
    latest_observed_at_ms: i64,
    active_by_slug: HashMap<String, (i64, i64)>,
}

impl BinanceInternalStatsAccumulator {
    fn new(asset: String, duration: String) -> Self {
        Self {
            asset,
            duration,
            samples: 0,
            windows: HashSet::new(),
            raw_below_one_samples: 0,
            positive_net_samples: 0,
            eligible_samples: 0,
            confirmed_samples: 0,
            positive_edge_sum: Decimal::ZERO,
            maximum_net_edge: Decimal::ZERO,
            maximum_theoretical_profit: Decimal::ZERO,
            maximum_quote_skew_ms: 0,
            latest_observed_at_ms: 0,
            active_by_slug: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceInternalStatsView {
    pub asset: String,
    pub duration: String,
    pub samples: u64,
    pub windows: u64,
    pub raw_below_one_samples: u64,
    pub positive_net_samples: u64,
    pub eligible_samples: u64,
    pub confirmed_samples: u64,
    pub average_positive_edge: String,
    pub maximum_net_edge: String,
    pub maximum_theoretical_profit: String,
    pub maximum_quote_skew_ms: i64,
    pub latest_observed_at_ms: i64,
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
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn stores_and_reads_latest_binance_basis_without_decimal_loss() {
        let storage = Storage::open(":memory:").unwrap();
        let value = BinanceBasisView {
            symbol: "BTCUSDT".into(),
            observed_at_ms: 1_000,
            quantity: "0.01234567".into(),
            spot_vwap_ask: "80640.09".into(),
            perp_vwap_bid: "80599.90".into(),
            spot_cost: "995.556677".into(),
            perp_proceeds: "995.060001".into(),
            entry_basis: "-0.496676".into(),
            entry_basis_rate: "-0.0004989".into(),
            estimated_round_trip_fees: "2.986".into(),
            slippage_buffer: "1.990".into(),
            funding_rate: "0.00009028".into(),
            funding_interval_hours: 8,
            funding_income_per_round: "0.0898".into(),
            projected_net_24h: "-5.203".into(),
            projected_net_7d: "-3.591".into(),
            funding_apr: "0.0988566".into(),
            break_even_rounds: Some("56".into()),
            mark_price: "80599.90".into(),
            index_price: "80620.38".into(),
            next_funding_time_ms: 2_000,
            response_skew_ms: 20,
            premium_age_ms: 50,
            scan_latency_ms: 100,
            rejections: vec!["24 小时无正净收益".into()],
        };

        storage.insert_binance_basis(&value).unwrap();
        let rows = storage.binance_basis_latest().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].quantity, value.quantity);
        assert_eq!(rows[0].funding_rate, value.funding_rate);
        assert_eq!(rows[0].rejections, value.rejections);
    }

    #[test]
    fn migrates_legacy_paper_trade_uniqueness_without_losing_history() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "arbitrage-storage-migration-{}-{unique}.sqlite",
            std::process::id()
        ));

        {
            let connection = Connection::open(&path).expect("open legacy database");
            connection
                .execute_batch(
                    "CREATE TABLE paper_trades (
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
                     INSERT INTO paper_trades (
                       slug, asset, duration, direction, classification, detected_at_ms,
                       quantity, total_cost, total_fees, expected_profit, status, fill_model
                     ) VALUES (
                       'btc-window', 'BTC', '1h', 'A', 'basis', 1000,
                       '1', '0.9', '0.01', '0.09', 'open', 'legacy_model'
                     );",
                )
                .expect("create legacy schema");
        }

        let storage = Storage::open(&path).expect("migrate legacy database");
        let trade = PaperTradeView {
            id: None,
            slug: "btc-window".into(),
            asset: "BTC".into(),
            duration: "1h".into(),
            direction: "A".into(),
            classification: "basis".into(),
            detected_at_ms: 2000,
            quantity: "1".into(),
            total_cost: "0.88".into(),
            total_fees: "0.01".into(),
            total_risk_buffers: "0.01".into(),
            expected_profit: "0.10".into(),
            actual_payout: None,
            realized_profit: None,
            settled_at_ms: None,
            status: "open".into(),
            fill_model: "new_model".into(),
        };
        storage
            .insert_paper_trade(&trade)
            .expect("insert trade for new model");
        storage
            .insert_paper_trade(&trade)
            .expect("ignore duplicate trade for same model");

        let connection = storage.connection.lock().expect("SQLite mutex poisoned");
        let table_sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='paper_trades'",
                [],
                |row| row.get(0),
            )
            .expect("read migrated table schema");
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM paper_trades WHERE slug='btc-window' AND direction='A'",
                [],
                |row| row.get(0),
            )
            .expect("count migrated trades");
        let legacy_buffers: String = connection
            .query_row(
                "SELECT total_risk_buffers FROM paper_trades WHERE fill_model='legacy_model'",
                [],
                |row| row.get(0),
            )
            .expect("read migrated legacy trade");

        assert!(table_sql.contains("UNIQUE(slug, direction, fill_model)"));
        assert_eq!(count, 2);
        assert_eq!(legacy_buffers, "0");
        drop(connection);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn stores_and_reads_shadow_execution_without_decimal_loss() {
        let storage = Storage::open(":memory:").expect("open in-memory database");
        let execution = ShadowExecutionView {
            id: None,
            slug: "btc-window".into(),
            asset: "BTC".into(),
            direction: "A".into(),
            quoted_at_ms: 1_000,
            executed_at_ms: 1_321,
            requested_quantity: "10.123456789".into(),
            first_limit_price: "0.314159265".into(),
            second_limit_price: "0.650000001".into(),
            first_status: "fullyfilled".into(),
            first_filled_quantity: "10.123456789".into(),
            first_average_price: Some("0.31".into()),
            first_total_cost: "3.13827160459".into(),
            second_status: "cancelled".into(),
            second_filled_quantity: "0".into(),
            second_average_price: None,
            second_total_cost: "0".into(),
            unhedged_quantity: "10.123456789".into(),
            status: "unhedged".into(),
            trigger_source: "automatic".into(),
            error: Some("第二腿 FOK 取消".into()),
        };

        storage
            .insert_shadow_execution(&execution)
            .expect("insert shadow execution");
        let rows = storage
            .shadow_executions(10)
            .expect("read shadow executions");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].requested_quantity, "10.123456789");
        assert_eq!(rows[0].first_limit_price, "0.314159265");
        assert_eq!(rows[0].unhedged_quantity, "10.123456789");
        assert_eq!(rows[0].trigger_source, "automatic");
    }
}
const DASHBOARD_STATS_LOOKBACK_MS: i64 = 6 * 60 * 60 * 1_000;
