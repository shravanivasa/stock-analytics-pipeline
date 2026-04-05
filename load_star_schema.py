import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stocks",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

# ── Load dim_stocks ───────────────────────────────────────
stocks = [
    ("AAPL", "Apple Inc.", "Technology", "NASDAQ"),
    ("GOOGL", "Alphabet Inc.", "Technology", "NASDAQ"),
    ("MSFT", "Microsoft Corporation", "Technology", "NASDAQ"),
    ("TSLA", "Tesla Inc.", "Automotive", "NASDAQ"),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", "NASDAQ"),
]

for stock in stocks:
    cursor.execute("""
        INSERT INTO dim_stocks (symbol, company_name, sector, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING
    """, stock)

print("dim_stocks loaded!")

# ── Load dim_dates ────────────────────────────────────────
cursor.execute("SELECT DISTINCT date FROM stock_prices ORDER BY date")
dates = cursor.fetchall()

for (date,) in dates:
    cursor.execute("""
        INSERT INTO dim_dates (date, year, month, day, quarter, day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
    """, (
        date,
        date.year,
        date.month,
        date.day,
        (date.month - 1) // 3 + 1,  # quarter calculation
        date.strftime("%A"),          # Monday, Tuesday etc.
        date.weekday() >= 5           # True if Saturday or Sunday
    ))

print("dim_dates loaded!")

# ── Load fact_stock_prices ────────────────────────────────
cursor.execute("SELECT date, symbol, open, high, low, close, volume FROM stock_prices")
raw_data = cursor.fetchall()

for row in raw_data:
    date, symbol, open_, high, low, close, volume = row

    # get stock_id
    cursor.execute("SELECT stock_id FROM dim_stocks WHERE symbol = %s", (symbol,))
    stock_id = cursor.fetchone()[0]

    # get date_id
    cursor.execute("SELECT date_id FROM dim_dates WHERE date = %s", (date,))
    date_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO fact_stock_prices 
        (date_id, stock_id, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (date_id, stock_id, open_, high, low, close, volume))

conn.commit()
print("fact_stock_prices loaded!")
print("Star schema fully populated!")

cursor.close()
conn.close()