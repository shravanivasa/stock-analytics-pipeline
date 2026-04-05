import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stocks",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

# ── Create dim_stocks ─────────────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_stocks (
        stock_id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) UNIQUE NOT NULL,
        company_name VARCHAR(100),
        sector VARCHAR(50),
        exchange VARCHAR(20)
    )
""")

# ── Create dim_dates ──────────────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_dates (
        date_id SERIAL PRIMARY KEY,
        date DATE UNIQUE NOT NULL,
        year INT,
        month INT,
        day INT,
        quarter INT,
        day_of_week VARCHAR(10),
        is_weekend BOOLEAN
    )
""")

# ── Create fact_stock_prices ──────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_stock_prices (
        price_id SERIAL PRIMARY KEY,
        date_id INT REFERENCES dim_dates(date_id),
        stock_id INT REFERENCES dim_stocks(stock_id),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    )
""")

conn.commit()
print("Star schema created!")
print("Tables: dim_stocks, dim_dates, fact_stock_prices")

cursor.close()
conn.close()