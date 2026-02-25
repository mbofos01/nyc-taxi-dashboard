# NYC Taxi Dashboard

## Architecture

![Docker Compose Architecture](assets/docker-compose.svg)

## Current Implementation

ETL pipeline for processing NYC taxi trip data:

- **Extract Service**: Downloads Parquet files from NYC TLC data source, publishes messages via RabbitMQ
- **Transform Service**: Processes data with PySpark, generates aggregated datasets, listens for RabbitMQ messages
- **RabbitMQ**: Message broker for service communication

Services run in Docker containers orchestrated by docker-compose.

## Plan

- Add FastAPI backend for data access
- Add Dash/Plotly frontend for data visualization

## Setup

```bash
docker-compose up --build
```

## Data

- Raw data: `/data/raw` (Parquet files by taxi type and date)
- Processed data: `/data/processed` (Aggregated Parquet files)

Source: https://d37ci6vzurychx.cloudfront.net/trip-data/
