# Stock Analytics Pipeline

> Automated data pipeline that fetches real stock market data,
> stores it in a cloud database, transforms it with dbt, and
> surfaces insights through a dashboard.

## Overview
This project pulls daily stock prices for 5 companies — Apple, 
Google, Microsoft, Tesla, and Amazon — from a live financial API. 
The data flows through an automated ingestion layer into PostgreSQL, 
gets transformed and modeled with dbt, and is deployed to a cloud 
database with a dashboard on top.

Built to demonstrate end-to-end data engineering and analytics 
skills — from raw API data to production-ready models and insights.

## Architecture
```
Alpha Vantage API
       ↓
Python (requests + pandas)
       ↓
PostgreSQL — Docker locally → Supabase in cloud
       ↓
dbt — transformation + modeling + tests
       ↓
Dashboard (Looker Studio / Power BI)
```

## What It Does

### Data Ingestion
Calls the Alpha Vantage REST API to fetch 100 days of OHLCV 
(Open, High, Low, Close, Volume) price data for 5 stocks. 
Handles API rate limiting automatically and saves structured 
data to PostgreSQL — 500 rows across 5 companies.

### Storage & Schema Design
PostgreSQL database with a designed schema using appropriate 
data types — DATE for timestamps, FLOAT for prices, BIGINT 
for volume. Loaded via psycopg2 with parameterized queries.

### SQL Analysis
Advanced queries written directly on the raw data:
- Average, min, max closing price per stock
- Daily price volatility — (high - low) as a derived metric
- 7-day moving average using window functions
- Percentage returns using FIRST_VALUE / LAST_VALUE with PARTITION BY

### Data Modeling with dbt ⏳
Transforming raw stock data into clean, tested, documented 
models — fact and dimension tables, metrics layer, and 
data quality tests.

### Cloud Deployment ⏳
Migrating from local Docker to Supabase (free cloud PostgreSQL) 
so the pipeline runs end-to-end in the cloud.

### Dashboard ⏳
Looker Studio or Power BI dashboard built on top of dbt models 
showing price trends, volatility, and performance metrics.

## Tech Stack
| Tool | Purpose |
|---|---|
| Python | Data ingestion, API calls, database loading |
| requests | HTTP calls to Alpha Vantage REST API |
| pandas | Data manipulation and transformation |
| PostgreSQL | Relational database |
| psycopg2 | Python to PostgreSQL connection |
| Docker | Local PostgreSQL without installation |
| dbt | Data transformation and modeling |
| Supabase | Cloud PostgreSQL deployment |
| SQL | Advanced analytics — window functions, CTEs, subqueries |
| Looker Studio | Dashboard and visualization |

## SQL Concepts Demonstrated
- Aggregate functions with GROUP BY
- Calculated columns for derived metrics
- Subqueries for dynamic filtering
- Window functions — AVG() OVER() for moving averages
- PARTITION BY — per-stock calculations
- FIRST_VALUE / LAST_VALUE — for return calculations

## Data Source
[Alpha Vantage](https://www.alphavantage.co/) — free financial API  
Stocks: AAPL · GOOGL · MSFT · TSLA · AMZN
