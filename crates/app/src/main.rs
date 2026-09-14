use arbitrage::{calculate_buy_arbitrage, check_risk, RiskLimits};
use chrono::{Duration, Utc};
use domain::{Market, MarketStatus, OrderBook, Outcome, PriceLevel, Venue};
use execution::SimulatedExecutor;
use matcher::validate_settlement_compatibility;
use rust_decimal::Decimal;
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

mod scanner;
mod storage;
mod web;

fn mock_market(venue: Venue, outcome: Outcome, start: chrono::DateTime<Utc>) -> Market {
    Market {
        venue,
        market_id: format!("mock-{venue:?}-{outcome:?}"),
        symbol: "BTC".into(),
        outcome,
        start_time: start,
        end_time: start + Duration::minutes(5),
        settlement_source: Some("MOCK_BTC_INDEX".into()),
        settlement_rule: Some("Mock: close above opening price is Up; equal is void".into()),
        status: MarketStatus::Open,
    }
}
fn mock_book(
    venue: Venue,
    outcome: Outcome,
    levels: &[(i64, i64)],
    now: chrono::DateTime<Utc>,
) -> OrderBook {
    OrderBook {
        venue,
        market_id: format!("mock-{venue:?}-{outcome:?}"),
        outcome,
        bids: vec![],
        asks: levels
            .iter()
            .map(|(price, quantity)| PriceLevel {
                price: Decimal::new(*price, 2),
                quantity: Decimal::new(*quantity, 0),
            })
            .collect(),
        timestamp: now,
    }
}
fn scan() -> Result<(), String> {
    let now = Utc::now();
    let start = now - Duration::minutes(1);
    let binance_up = mock_market(Venue::Binance, Outcome::Up, start);
    let polymarket_down = mock_market(Venue::Polymarket, Outcome::Down, start);
    validate_settlement_compatibility(&binance_up, &polymarket_down).map_err(|e| e.to_string())?;
    let up = mock_book(Venue::Binance, Outcome::Up, &[(40, 100), (41, 500)], now);
    let down = mock_book(
        Venue::Polymarket,
        Outcome::Down,
        &[(50, 200), (51, 500)],
        now,
    );
    let limits = RiskLimits {
        max_trade_amount: Decimal::new(1000, 0),
        minimum_remaining_time: Duration::seconds(30),
        maximum_book_age: Duration::seconds(5),
    };
    check_risk(&binance_up, &polymarket_down, &up, &down, now, &limits)
        .map_err(|e| format!("风控拒绝：{e:?}"))?;
    let opportunity = calculate_buy_arbitrage(
        &up,
        &down,
        Decimal::ZERO,
        Decimal::new(1, 3),
        Decimal::new(2, 3),
    )
    .ok_or("无可执行机会")?;
    if opportunity.cost > limits.max_trade_amount {
        return Err("风控拒绝：交易金额超限".into());
    }
    println!(
        "发现 Mock 机会：方向={:?} 数量={} 成本={} 净利润={} ROI={}",
        opportunity.direction,
        opportunity.executable_size,
        opportunity.cost,
        opportunity.net_profit,
        opportunity.net_profit_rate
    );
    Ok(())
}
async fn serve_dashboard() -> Result<(), Box<dyn Error>> {
    let api_key = env::var("BINANCE_API_KEY").map_err(|_| "缺少 BINANCE_API_KEY")?;
    let api_secret = env::var("BINANCE_API_SECRET").map_err(|_| "缺少 BINANCE_API_SECRET")?;
    let gamma_base = env::var("POLYMARKET_API_BASE")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".into());
    let clob_base =
        env::var("POLYMARKET_CLOB_BASE").unwrap_or_else(|_| "https://clob.polymarket.com".into());
    let database_path =
        env::var("DATABASE_PATH").unwrap_or_else(|_| ".data/observer.sqlite3".into());
    if let Some(parent) = std::path::Path::new(&database_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let binance = exchange_binance::BinanceClient::new(api_key, api_secret)?;
    let polymarket = exchange_polymarket::PolymarketClient::new(gamma_base, clob_base);
    let storage = storage::Storage::open(database_path)?;
    storage.settle_stored_paper_trades()?;
    let (observation_count, opportunity_count) = storage.observation_statistics()?;
    let state = Arc::new(RwLock::new(scanner::DashboardState {
        mode: "observation".into(),
        scanner_status: "starting".into(),
        observation_count,
        opportunity_count,
        ..Default::default()
    }));
    tokio::spawn(scanner::run(
        binance,
        polymarket,
        storage.clone(),
        state.clone(),
    ));

    let address = env::var("DASHBOARD_ADDR").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("本地观察台：http://{address}");
    axum::serve(listener, web::router(state, storage)).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "app=info".into()),
        )
        .init();
    if env::var("EXECUTION_MODE").unwrap_or_else(|_| "simulation".into()) == "live" {
        eprintln!("拒绝启动：第一阶段禁止 live 执行");
        std::process::exit(2);
    }
    match env::args().nth(1).as_deref() {
        Some("markets") => {
            println!("Mock 市场：BTC 5 分钟 Binance UP / Polymarket DOWN；结算来源=MOCK_BTC_INDEX")
        }
        Some("orderbook") => println!(
            "Mock 卖盘：Binance UP [0.40×100, 0.41×500]；Polymarket DOWN [0.50×200, 0.51×500]"
        ),
        Some("scan") => {
            if let Err(error) = scan() {
                eprintln!("scan 未产生机会：{error}");
                std::process::exit(1);
            }
        }
        Some("simulator") => {
            scan().unwrap_or_else(|error| eprintln!("模拟扫描失败：{error}"));
            let mut executor = SimulatedExecutor::default();
            let first = executor.place_order(Decimal::new(600, 0));
            let second = executor.place_order(Decimal::new(600, 0));
            executor.fill_order(first.id, Decimal::new(600, 0));
            executor.fill_order(second.id, Decimal::new(500, 0));
            let position = executor.assess_hedge(Decimal::new(600, 0), Decimal::new(500, 0));
            println!(
                "模拟部分成交：未对冲份额={}，新套利已暂停={}",
                position.exposure, executor.paused
            );
        }
        Some("status") => println!("status：mode=simulation；真实下单=禁用；数据源=Mock"),
        Some("serve") => {
            if let Err(error) = serve_dashboard().await {
                eprintln!("观察台启动失败：{error}");
                std::process::exit(1);
            }
        }
        _ => println!("用法：cargo run -- [markets|orderbook|scan|simulator|status|serve]"),
    }
}
