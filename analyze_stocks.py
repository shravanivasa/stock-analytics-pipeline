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


print("=" * 50)
print("AVERAGE CLOSING PRICE PER STOCK")
print("=" * 50)
cursor.execute("""
    SELECT 
        symbol,
        ROUND(AVG(close)::numeric, 2) AS avg_close,
        ROUND(MIN(close)::numeric, 2) AS min_close,
        ROUND(MAX(close)::numeric, 2) AS max_close
    FROM stock_prices
    GROUP BY symbol
    ORDER BY avg_close DESC
""")
for row in cursor.fetchall():
    print(f"{row[0]}: avg=${row[1]}, min=${row[2]}, max=${row[3]}")


print("\n" + "=" * 50)
print("BIGGEST SINGLE DAY PRICE SWING")
print("=" * 50)
cursor.execute("""
    SELECT 
        symbol,
        date,
        ROUND((high - low)::numeric, 2) AS price_swing
    FROM stock_prices
    ORDER BY price_swing DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"{row[0]} on {row[1]}: ${row[2]} swing")

# 7-day moving average for AAPL
print("\n" + "=" * 50)
print("7-DAY MOVING AVERAGE — AAPL")
print("=" * 50)
cursor.execute("""
    SELECT 
        date,
        close,
        ROUND(AVG(close) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::numeric, 2) AS moving_avg_7d
    FROM stock_prices
    WHERE symbol = 'AAPL'
    ORDER BY date DESC
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"{row[0]}: close=${row[1]}, 7d_avg=${row[2]}")

#  Best performing stock (% gain) 
print("\n" + "=" * 50)
print("STOCK PERFORMANCE — FIRST VS LATEST PRICE")
print("=" * 50)
cursor.execute("""
    SELECT 
        symbol,
        ROUND(first_close::numeric, 2) AS first_price,
        ROUND(last_close::numeric, 2) AS latest_price,
        ROUND(((last_close - first_close) / first_close * 100)::numeric, 2) AS pct_change
    FROM (
        SELECT 
            symbol,
            FIRST_VALUE(close) OVER (
                PARTITION BY symbol ORDER BY date ASC
            ) AS first_close,
            LAST_VALUE(close) OVER (
                PARTITION BY symbol ORDER BY date ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_close
        FROM stock_prices
    ) subquery
    GROUP BY symbol, first_close, last_close
    ORDER BY pct_change DESC
""")
for row in cursor.fetchall():
    arrow = "📈" if row[3] > 0 else "📉"
    print(f"{arrow} {row[0]}: {row[3]}% (${row[1]} → ${row[2]})")

cursor.close()
conn.close()