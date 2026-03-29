import psycopg2
import pandas as pd

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stocks",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

# How many rows do we have?
cursor.execute("SELECT COUNT(*) FROM stock_prices")
count = cursor.fetchone()
print(f"Total rows: {count[0]}")

# Show first 5 rows
cursor.execute("SELECT * FROM stock_prices LIMIT 5")
rows = cursor.fetchall()
print("\nFirst 5 rows:")
for row in rows:
    print(row)

# What stocks do we have?
cursor.execute("SELECT DISTINCT symbol FROM stock_prices")
symbols = cursor.fetchall()
print("\nStocks in database:")
for s in symbols:
    print(s[0])

# Latest closing price per stock
cursor.execute("""
    SELECT symbol, date, close
    FROM stock_prices
    WHERE date = (SELECT MAX(date) FROM stock_prices)
    ORDER BY symbol
""")
latest = cursor.fetchall()
print("\nLatest closing prices:")
for row in latest:
    print(f"{row[0]}: ${row[2]} on {row[1]}")

cursor.close()
conn.close()