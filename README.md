# NYC Taxi AI Dashboard

An AI-powered analytics dashboard built on NYC TLC taxi data (2019–present), featuring real-time aggregations, ML model predictions, and interactive visualizations.

---

## Stack

| Layer        | Technology                        |
|--------------|-----------------------------------|
| Ingestion    | Python + Requests                 |
| Processing   | PySpark (standalone cluster)      |
| ML           | Spark MLlib                       |
| Broker       | RabbitMQ                          |
| Cache / State| Redis                             |
| Database     | PostgreSQL                        |
| Monitoring   | Prometheus + Grafana + Pushgateway|
| Backend      | FastAPI *(planned)*               |
| Frontend     | Plotly Dash *(planned)*           |
| Infra        | Docker Compose                    |

---

## Data Sources

NYC TLC Trip Record Data (Parquet), published monthly with ~3 month delay:

- Yellow Taxi (2019–present)
- Green Taxi (2019–present)
- FHV / For-Hire Vehicles (2019–present)
- FHVHV / High Volume FHV — Uber, Lyft (2019–present)

Source: `https://d37ci6vzurychx.cloudfront.net/trip-data/`

---

## Architecture

![Docker Compose Architecture](assets/docker-compose.svg)

### RabbitMQ Queues & Exchanges

| Name | Type | Published by | Consumed by |
| --- | --- | --- | --- |
| `etl.extracted` | queue | Extract | Transform |
| `etl.transformed` | queue | Transform | Load |
| `etl.loaded` | fanout exchange | Load | All model services |

### Redis Usage

Transform tracks processed files and output state to enable crash-safe incremental writes:

```bash
# Set of filenames already transformed (prevents reprocessing on restart)
{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}   → SADD / SISMEMBER

# Dirty flag per taxi type ("0" = in-progress, "1" = last run completed cleanly)
{REDIS_TRACKING_ROOT}:{taxi_type}             → SET / GET
```

Model services write their training status to Redis:

```bash
model:fare:status     = "ready"
model:demand:status   = "ready"
model:tip:status      = "ready"
model:anomaly:status  = "ready"
```

FastAPI checks these keys before serving predictions — if a key is missing or not `"ready"`, it returns a "model not ready" response.

### Data Volumes

```bash
/data/raw/{taxi_type}_tripdata_{year}-{month}.parquet   ← Extract writes here
/data/processed/zone_hourly/{taxi_type}/                ← Transform writes here
/data/processed/daily_stats/{taxi_type}/
/data/processed/zone_time_buckets/{taxi_type}/
/data/processed/zone_anomaly_stats/{taxi_type}/
/data/models/fare/                                      ← Model services write here
/data/models/demand/
/data/models/tip/
/data/models/anomaly/
```

---

## Services

### ✅ Extract (`ETL/extract/`)

- Downloads raw Parquet files from TLC CDN for Yellow and Green taxi types
- Validates each file's Parquet magic bytes (`PAR1`) after download; retries once on corruption, marks as `DOWNLOAD_FAILED` if retry also corrupt
- On startup: scans existing files, deletes and requeues any corrupt ones, then downloads all missing files
- Runs on startup then schedules a monthly refresh (configurable via `EXTRACT_CRON_DAY` / `EXTRACT_CRON_HOUR`, defaults to day 15 at 02:00 UTC)
- End date automatically set to 3 months ago to avoid 403s on unreleased data
- Publishes `{ event, timestamp, new_files, summary }` to `etl.extracted`
- Pushes Prometheus metrics to Pushgateway (`job=etl_extract`)

---

### ✅ Transform (`ETL/transform/`)

- Consumes messages from `etl.extracted` queue
- Scans for any pending files not yet in Redis tracking set (catch-up on restart or missed messages)
- Validates Parquet magic bytes before passing files to Spark; skips corrupt files
- Normalises column names across all 4 taxi types to a unified schema
- Cleans data: null guards, year range (2019–present), trip duration (0–300 min), distance (<200 mi), fare (<$1000)
- Engineers time features: hour, day of week, month, time bucket (morning/afternoon/evening/night), is_weekend
- Produces 4 aggregated Parquet datasets per taxi type:
  - `zone_hourly` — trip count + averages per zone per hour (feeds map + demand forecast)
  - `daily_stats` — daily summary per taxi type (feeds KPI cards)
  - `zone_time_buckets` — trip count per zone per time of day (feeds clustering)
  - `zone_anomaly_stats` — mean + stddev per zone (feeds anomaly detection)
- **Crash-safe incremental writes**: reads dirty flag from Redis before marking dirty; merges new data with existing output if previous run completed cleanly; overwrites on detected crash; uses write-to-tmp-then-rename to avoid read/write conflict on same directory
- **Redis file tracking**: marks each file as processed after all 4 writes succeed; skips already-processed files on subsequent runs
- Runs a daily cron (configurable via `TRANSFORM_CRON_HOUR` / `TRANSFORM_CRON_MINUTE`) to catch any pending files
- Pushes Prometheus metrics to Pushgateway (`job=etl_transform`): files processed, corrupt files, rows before/after cleaning, processing duration per taxi type

---

### ✅ Load (`ETL/load/`)

- Consumes messages from `etl.transformed` queue
- Reads aggregated Parquet files from `/data/processed`; auto-discovers taxi type subdirectories (no hardcoded list)
- Upserts all 4 datasets into PostgreSQL using `ON CONFLICT DO UPDATE`
- Publishes to `etl.loaded` fanout exchange to trigger all model services
- Pushes Prometheus metrics to Pushgateway (`job=etl_load`)

---

### ✅ FarePrediction (`models/fare/`)

- Subscribes to `etl.loaded` fanout exchange
- Trains LinearRegression on `zone_hourly` aggregated data (predicts `avg_fare` per zone/hour); features: pickup zone, hour, day of week, month, taxi type
- Saves model artifact to `/data/models/fare/`
- Sets `model:fare:status = "ready"` in Redis
- Pushes Prometheus metrics to Pushgateway (`job=model_fare`)

---

### 🔲 DemandForecast (`models/demand/`)

- Subscribes to `etl.loaded` fanout exchange
- Trains regression model on: zone, hour, day of week, month
- Saves model artifact to `/data/models/demand/`
- Sets `model:demand:status = "ready"` in Redis

---

### 🔲 TipPrediction (`models/tip/`)

- Subscribes to `etl.loaded` fanout exchange
- Trains classifier on: fare, zone, time, payment type (yellow/green only)
- Saves model artifact to `/data/models/tip/`
- Sets `model:tip:status = "ready"` in Redis

---

### 🔲 AnomalyDetection (`models/anomaly/`)

- Subscribes to `etl.loaded` fanout exchange
- Trains KMeans clustering model, flags trips far from cluster centers
- Saves model artifact to `/data/models/anomaly/`
- Sets `model:anomaly:status = "ready"` in Redis

---

### 🔲 FastAPI (`api/`)

- Checks Redis for model readiness before serving predictions
- Planned endpoints:
  - `GET /stats/zones` — zone-level hourly stats
  - `GET /stats/daily` — daily KPI data
  - `POST /predict/fare` — fare prediction
  - `POST /predict/demand` — demand forecast
  - `POST /predict/tip` — tip prediction
  - `GET /anomalies` — flagged anomalous trips

---

### 🔲 Dash Frontend (`dashboard/`)

- Map view — pickup heatmap per zone
- KPI cards — total trips, revenue, average fare per day
- Demand forecast chart — predicted vs actual trips per zone
- Fare predictor — user inputs zone + time, gets predicted fare
- Anomaly explorer — flagged trips with drill-down
- Clustering view — zone hotspots by time of day

---

## Getting Started

```bash
# Run full stack
docker-compose up --build

# Check downloaded files
docker exec extract-service find /data/raw -name "*.parquet"

# Check processed data
docker exec transform-service find /data/processed -name "*.parquet"

# RabbitMQ management UI
open http://localhost:15672   # guest / guest

# Grafana dashboards
open http://localhost:3000    # admin / admin

# Spark master UI
open http://localhost:8080
```

---

## Known Issues & Planned Improvements

- [ ] Extract: `START_DATE` should be set to 2019 via `.env` for full production data
- [ ] Extract: FHV and FHVHV types commented out pending schema validation
- [ ] Models: demand, tip, and anomaly services not yet implemented
- [ ] API + Dashboard: not yet implemented
