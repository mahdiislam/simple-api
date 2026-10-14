# simple-api

Polls Elhub's public metering point statistics CSV, stores it in SQLite, and serves it as JSON. No authentication.

Source: https://data.elhub.no/download/metering_point_statistics/metering_point_statistics-all-en-0000-00-00.csv

## Run

```sh
cargo run --release
```

It fetches immediately on startup, then every hour. Rows are upserted on
(month, type, production type, settlement method, grid area type, reading
occurrence), so repeated polls update counts instead of duplicating rows.

## Endpoint

`GET /metering-point-statistics`

Optional query parameters: `month`, `meteringpoint_type`, `limit`, `offset`.

```sh
curl 'http://localhost:3000/metering-point-statistics?month=2024-01-01&meteringpoint_type=Production&limit=5'
```

```json
[
  {
    "month": "2024-01-01",
    "meteringpoint_type": "Production",
    "production_type": "*",
    "settlement_method": "Non-profiled",
    "grid_area_type": "Distribution and Regional",
    "meter_reading_occurrence": "60 minutes",
    "meteringpoint_count": 3
  }
]
```

## Configuration (env vars)

| Variable             | Default        |
|----------------------|----------------|
| `DB_PATH`            | `elhub.db`     |
| `BIND_ADDR`          | `0.0.0.0:3000` |
| `POLL_INTERVAL_SECS` | `3600`         |
