use arbitrage::{calculate_buy_arbitrage, check_risk, RiskLimits};
use chrono::{Duration, Utc};
use domain::{Market, MarketStatus, OrderBook, Outcome, PriceLevel, Venue};
use execution::{
    preflight_pair_fok, quantity_for_budget_at_step, FokOrderRequest, SimulatedExecutor,
};
use matcher::validate_settlement_compatibility;
use rust_decimal::Decimal;
use std::env;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

mod basis;
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

fn argument_decimal(arguments: &[String], name: &str) -> Result<Option<Decimal>, String> {
    let Some(index) = arguments.iter().position(|value| value == name) else {
        return Ok(None);
    };
    let value = arguments
        .get(index + 1)
        .ok_or_else(|| format!("{name} 缺少数值"))?;
    Decimal::from_str(value)
        .map(Some)
        .map_err(|_| format!("{name} 不是有效十进制定点数：{value}"))
}

fn argument_string(arguments: &[String], name: &str) -> Result<Option<String>, String> {
    let Some(index) = arguments.iter().position(|value| value == name) else {
        return Ok(None);
    };
    arguments
        .get(index + 1)
        .cloned()
        .ok_or_else(|| format!("{name} 缺少值"))
        .map(Some)
}

fn run_fok_simulator(arguments: &[String]) -> Result<(), String> {
    let first_limit =
        argument_decimal(arguments, "--binance-limit")?.unwrap_or_else(|| Decimal::new(41, 2));
    let second_limit =
        argument_decimal(arguments, "--polymarket-limit")?.unwrap_or_else(|| Decimal::new(51, 2));
    let explicit_quantity = argument_decimal(arguments, "--quantity")?;
    let budget = argument_decimal(arguments, "--budget")?;
    let quantity_step =
        argument_decimal(arguments, "--quantity-step")?.unwrap_or_else(|| Decimal::new(1, 2));
    if explicit_quantity.is_some() && budget.is_some() {
        return Err("--quantity 与 --budget 只能指定一个".into());
    }
    let quantity = match (explicit_quantity, budget) {
        (Some(quantity), None) => quantity,
        (None, Some(budget)) => {
            quantity_for_budget_at_step(budget, first_limit, second_limit, quantity_step)
                .ok_or("预算、限价或数量步长无效")?
        }
        (None, None) => Decimal::TEN,
        (Some(_), Some(_)) => unreachable!(),
    };
    let second_execution_price =
        argument_decimal(arguments, "--second-leg-price")?.unwrap_or_else(|| Decimal::new(50, 2));
    let first_asks = vec![
        PriceLevel {
            price: Decimal::new(40, 2),
            quantity: Decimal::from(100),
        },
        PriceLevel {
            price: Decimal::new(41, 2),
            quantity: Decimal::from(500),
        },
    ];
    let second_asks = vec![PriceLevel {
        price: second_execution_price,
        quantity: Decimal::from(500),
    }];
    let quoted_second_asks = vec![PriceLevel {
        price: Decimal::new(50, 2),
        quantity: Decimal::from(500),
    }];
    let first_request = FokOrderRequest {
        quantity,
        limit_price: first_limit,
    };
    let second_request = FokOrderRequest {
        quantity,
        limit_price: second_limit,
    };
    preflight_pair_fok(
        first_request,
        &first_asks,
        second_request,
        &quoted_second_asks,
    )
    .map_err(|reasons| format!("双腿提交前预检拒绝：{}", reasons.join("；")))?;
    println!("提交前预检：两腿在各自限价内均可完全成交");
    let mut executor = SimulatedExecutor::default();
    let report =
        executor.execute_pair_fok(first_request, &first_asks, second_request, &second_asks);
    println!(
        "FOK 模拟：数量={}，Binance 限价={}，Polymarket 限价={}，组合状态={:?}",
        quantity, first_limit, second_limit, report.status
    );
    println!(
        "Binance：状态={:?}，成交={}，均价={}，成本={}{}",
        report.first.status,
        report.first.filled_quantity,
        report
            .first
            .average_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        report.first.total_cost,
        report
            .first
            .reason
            .as_deref()
            .map(|reason| format!("，原因={reason}"))
            .unwrap_or_default()
    );
    println!(
        "Polymarket：状态={:?}，成交={}，均价={}，成本={}{}",
        report.second.status,
        report.second.filled_quantity,
        report
            .second
            .average_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        report.second.total_cost,
        report
            .second
            .reason
            .as_deref()
            .map(|reason| format!("，原因={reason}"))
            .unwrap_or_default()
    );
    println!(
        "未对冲份额={}，暂停后续执行={}",
        report.hedge.exposure, executor.paused
    );
    Ok(())
}

async fn run_live_fok_simulator(arguments: &[String]) -> Result<(), String> {
    let api_key = env::var("BINANCE_API_KEY").map_err(|_| "缺少 BINANCE_API_KEY")?;
    let api_secret = env::var("BINANCE_API_SECRET").map_err(|_| "缺少 BINANCE_API_SECRET")?;
    let gamma_base = env::var("POLYMARKET_API_BASE")
        .unwrap_or_else(|_| "https://gamma-api.polymarket.com".into());
    let clob_base =
        env::var("POLYMARKET_CLOB_BASE").unwrap_or_else(|_| "https://clob.polymarket.com".into());
    let binance = exchange_binance::BinanceClient::new(api_key, api_secret)
        .map_err(|error| error.to_string())?;
    let polymarket = exchange_polymarket::PolymarketClient::new(gamma_base, clob_base);
    let asset = argument_string(arguments, "--asset")?.unwrap_or_else(|| "BTC".into());
    let direction = argument_string(arguments, "--direction")?
        .unwrap_or_else(|| "A".into())
        .to_ascii_uppercase();
    let quantity = argument_decimal(arguments, "--quantity")?;
    let budget = argument_decimal(arguments, "--budget")?;
    let first_limit = argument_decimal(arguments, "--binance-limit")?;
    let second_limit = argument_decimal(arguments, "--polymarket-limit")?;
    let quantity_step =
        argument_decimal(arguments, "--quantity-step")?.unwrap_or_else(|| Decimal::new(1, 2));
    let delay_ms = argument_string(arguments, "--delay-ms")?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| format!("--delay-ms 不是有效非负整数：{value}"))
        })
        .transpose()?
        .unwrap_or(250);
    let simulation = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        scanner::simulate_live_fok(
            &binance,
            &polymarket,
            &asset,
            &direction,
            quantity,
            budget,
            first_limit,
            second_limit,
            quantity_step,
            delay_ms,
        ),
    )
    .await
    .map_err(|_| "真实行情影子 FOK 总耗时超过 20 秒".to_string())??;
    println!(
        "真实行情影子 FOK：市场={}，方向={}，首次盘口={}ms，到达盘口={}ms，模拟延迟={}ms",
        simulation.slug,
        simulation.direction,
        simulation.quoted_at_ms,
        simulation.executed_at_ms,
        delay_ms
    );
    println!(
        "第一腿：限价={}，请求={}，状态={:?}，成交={}，均价={}，成本={}{}",
        simulation.first_request.limit_price,
        simulation.first_request.quantity,
        simulation.report.first.status,
        simulation.report.first.filled_quantity,
        simulation
            .report
            .first
            .average_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        simulation.report.first.total_cost,
        simulation
            .report
            .first
            .reason
            .as_deref()
            .map(|reason| format!("，原因={reason}"))
            .unwrap_or_default()
    );
    println!(
        "第二腿：限价={}，请求={}，状态={:?}，成交={}，均价={}，成本={}{}",
        simulation.second_request.limit_price,
        simulation.second_request.quantity,
        simulation.report.second.status,
        simulation.report.second.filled_quantity,
        simulation
            .report
            .second
            .average_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        simulation.report.second.total_cost,
        simulation
            .report
            .second
            .reason
            .as_deref()
            .map(|reason| format!("，原因={reason}"))
            .unwrap_or_default()
    );
    println!(
        "组合状态={:?}，未对冲份额={}",
        simulation.report.status, simulation.report.hedge.exposure
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
    tokio::spawn(basis::run(binance.clone(), storage.clone()));
    tokio::spawn(basis::run_delivery(binance.clone(), storage.clone()));
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
    let arguments: Vec<String> = env::args().collect();
    match arguments.get(1).map(String::as_str) {
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
            if let Err(error) = run_fok_simulator(&arguments[2..]) {
                eprintln!("FOK 模拟失败：{error}");
                std::process::exit(1);
            }
        }
        Some("shadow-fok") => {
            if let Err(error) = run_live_fok_simulator(&arguments[2..]).await {
                eprintln!("真实行情影子 FOK 失败：{error}");
                std::process::exit(1);
            }
        }
        Some("status") => println!("status：mode=simulation；真实下单=禁用；数据源=Mock"),
        Some("serve") => {
            if let Err(error) = serve_dashboard().await {
                eprintln!("观察台启动失败：{error}");
                std::process::exit(1);
            }
        }
        _ => println!(
            "用法：cargo run -- [markets|orderbook|scan|simulator [--quantity Q|--budget U] [--quantity-step Q] [--binance-limit P] [--polymarket-limit P] [--second-leg-price P]|shadow-fok [--asset BTC] [--direction A|B] [--quantity Q|--budget U] [--quantity-step Q] [--binance-limit P] [--polymarket-limit P] [--delay-ms N]|status|serve]"
        ),
    }
}
