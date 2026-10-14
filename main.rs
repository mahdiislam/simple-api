//! Polls Elhub's metering point statistics CSV, stores it in SQLite,
//! and serves it as JSON on an unauthenticated HTTP endpoint.

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

const SOURCE_URL: &str = "https://data.elhub.no/download/metering_point_statistics/metering_point_statistics-all-en-0000-00-00.csv";

type Db = Arc<Mutex<Connection>>;

/// One CSV row. Reads the uppercase CSV header; serializes to snake_case JSON.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all(deserialize = "SCREAMING_SNAKE_CASE"))]
struct Record {
    month: String,
    meteringpoint_type: String,
    production_type: String,
    settlement_method: String,
    grid_area_type: String,
    meter_reading_occurrence: String,
    meteringpoint_count: i64,
}

fn init_db(path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS metering_point_statistics (
            month                    TEXT    NOT NULL,
            meteringpoint_type       TEXT    NOT NULL,
            production_type          TEXT    NOT NULL,
            settlement_method        TEXT    NOT NULL,
            grid_area_type           TEXT    NOT NULL,
            meter_reading_occurrence TEXT    NOT NULL,
            meteringpoint_count      INTEGER NOT NULL,
            PRIMARY KEY (month, meteringpoint_type, production_type,
                         settlement_method, grid_area_type, meter_reading_occurrence)
        );",
    )?;
    Ok(conn)
}

/// Download the CSV and upsert every row. Re-running is safe: existing rows
/// get their count updated, new months are added.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn poll_once(client: &reqwest::Client, db: &Db) -> Result<usize, BoxError> {
    let body = client.get(SOURCE_URL).send().await?.error_for_status()?.text().await?;

    let records: Vec<Record> = csv::Reader::from_reader(body.as_bytes())
        .deserialize()
        .collect::<Result<_, _>>()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("CSV parse failed: {e}")))?;

    let mut conn = db.lock().unwrap();
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO metering_point_statistics VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT DO UPDATE SET meteringpoint_count = excluded.meteringpoint_count",
        )?;
        for r in &records {
            stmt.execute(params![
                &r.month,
                &r.meteringpoint_type,
                &r.production_type,
                &r.settlement_method,
                &r.grid_area_type,
                &r.meter_reading_occurrence,
                r.meteringpoint_count
            ])?;
        }
    }
    tx.commit()?;
    Ok(records.len())
}

async fn poll_loop(db: Db, interval: Duration) {
    let client = reqwest::Client::new();
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await; // first tick fires immediately
        match poll_once(&client, &db).await {
            Ok(n) => println!("poll ok: {n} rows upserted"),
            Err(e) => eprintln!("poll failed: {e}"),
        }
    }
}

/// Optional filters: ?month=2024-01-01&meteringpoint_type=Production&limit=100&offset=0
#[derive(Deserialize)]
struct Filter {
    month: Option<String>,
    meteringpoint_type: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn get_data(
    State(db): State<Db>,
    Query(f): Query<Filter>,
) -> Result<Json<Vec<Record>>, (StatusCode, String)> {
    let err = |e: rusqlite::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
    let conn = db.lock().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT month, meteringpoint_type, production_type, settlement_method,
                    grid_area_type, meter_reading_occurrence, meteringpoint_count
             FROM metering_point_statistics
             WHERE (?1 IS NULL OR month = ?1)
               AND (?2 IS NULL OR meteringpoint_type = ?2)
             ORDER BY month, meteringpoint_type, production_type, settlement_method,
                      grid_area_type, meter_reading_occurrence
             LIMIT ?3 OFFSET ?4",
        )
        .map_err(err)?;
    let rows = stmt
        .query_map(
            params![f.month, f.meteringpoint_type, f.limit.unwrap_or(-1), f.offset.unwrap_or(0)],
            |r| {
                Ok(Record {
                    month: r.get(0)?,
                    meteringpoint_type: r.get(1)?,
                    production_type: r.get(2)?,
                    settlement_method: r.get(3)?,
                    grid_area_type: r.get(4)?,
                    meter_reading_occurrence: r.get(5)?,
                    meteringpoint_count: r.get(6)?,
                })
            },
        )
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(Json(rows))
}

#[tokio::main]
async fn main() {
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "elhub.db".into());
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let interval_secs: u64 = env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600);

    let db: Db = Arc::new(Mutex::new(init_db(&db_path).expect("open database")));

    tokio::spawn(poll_loop(db.clone(), Duration::from_secs(interval_secs)));

    let app = Router::new()
        .route("/metering-point-statistics", get(get_data))
        .with_state(db);

    let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind");
    println!("listening on http://{bind}/metering-point-statistics");
    axum::serve(listener, app).await.unwrap();
}
