# Databricks Weather & Air Quality Lakehouse Pipeline

## Overview

This project demonstrates an **end-to-end Databricks Lakehouse ETL pipeline** built using public REST APIs for **weather** and **air quality** data. The pipeline follows **Bronze–Silver–Gold architecture** and simulates streaming ingestion using **scheduled micro-batch jobs**.

The goal of this project is to showcase **production-style data engineering practices** on Databricks, aligned with **Databricks Data Engineer Associate** concepts.

---

## Architecture

**Data Flow**

```
Open-Meteo APIs
     ↓
Bronze Layer (Raw JSON, Append-Only)
     ↓
Silver Layer (Schema Enforcement, Parsing, Time-Series)
     ↓
Gold Layer (Analytics & Aggregations)
```

**Technologies Used**

* Databricks
* Apache Spark (PySpark)
* Delta Lake
* REST APIs (Open-Meteo)
* Databricks Jobs (Scheduling)

---

## Data Sources

**Open-Meteo APIs (No API Key Required)**

* Weather API (current weather snapshots)
* Air Quality API (hourly AQI forecasts – 5 days)

These APIs are ideal for demos and interviews because they are public, stable, and easy to integrate.

---

## Project Structure

```
databricks-weather-pipeline/
│
├── notebooks/
│   ├── 01_ingest_weather_api.py
│   ├── 02_ingest_air_quality_api.py
│   ├── 03_bronze_to_silver_weather.py
│   ├── 03_bronze_to_silver_air_quality.py
│   ├── 04_silver_to_gold.py
│
├── config/
│   └── api_config.py
│
├── sql/
│   └── analytics_queries.sql
│
└── README.md
```

---

## Bronze Layer – Raw Ingestion

**Objective**

* Ingest raw JSON data from REST APIs
* Store data exactly as received
* Append-only writes
* Add ingestion metadata

**Key Characteristics**

* No transformations
* No schema enforcement
* Immutable raw data

**Metadata Columns**

* `ingestion_timestamp`
* `source_api`
* `location`

Streaming is simulated by running ingestion notebooks as **scheduled Databricks Jobs** (micro-batches).

---

## Silver Layer – Clean & Historical Data

**Objective**

* Parse raw JSON payloads
* Enforce explicit schemas
* Create historical datasets

**Silver Tables**

* `silver_weather_current` – weather snapshots per city
* `silver_air_quality_hourly` – hourly AQI time-series

**Key Processing**

* JSON parsing with predefined schemas
* Explosion of hourly AQI arrays (120+ rows per ingestion)
* Deduplication using window functions

---

## Gold Layer – Analytics & Business Views

**Objective**

* Create analytics-ready tables
* Apply business logic
* Optimize for BI and SQL queries

**Gold Tables**

* `gold_daily_air_quality` – daily average AQI per city
* `gold_latest_conditions` – latest weather and AQI per city
* `gold_weather_aqi_correlation` – weather vs AQI analysis

**Important Logic**

* AQI hourly data is deduplicated per city and hour using the latest ingestion timestamp
* Daily averages are computed from exactly **24 hourly values per day**

---

## Key Learnings

* Designing a production-style **Lakehouse architecture** on Databricks
* Handling **API-based ingestion** with micro-batch scheduling
* Managing **time-series data** and forecast duplication
* Building **analytics-ready datasets** using Delta Lake

---

## How to Run

1. Create a Databricks workspace
2. Import the repository as a Databricks Repo
3. Configure API endpoints in `config/api_config.py`
4. Run ingestion notebooks manually or via Databricks Jobs
5. Execute Silver and Gold notebooks in order

---

## Author

This project was built as a **data engineering portfolio project** to demonstrate Databricks and Lakehouse best practices.
