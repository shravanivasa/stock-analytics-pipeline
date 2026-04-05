import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stocks",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

# ── Query 1: Check dim_stocks ─────────────────────────────
print("=" * 50)
print("DIM_STOCKS")
print("=" * 50)
cursor.execute("SELECT * FROM dim_stocks")
for row in cursor.fetchall():
    print(row)

# ── Query 2: Check dim_dates sample ──────────────────────
print("\n" + "=" * 50)
print("DIM_DATES — Sample")
print("=" * 50)
cursor.execute("SELECT * FROM dim_dates ORDER BY date DESC LIMIT 5")
for row in cursor.fetchall():
    print(row)

# ── Query 3: Join all 3 tables ────────────────────────────
print("\n" + "=" * 50)
print("FACT + DIMENSIONS JOINED")
print("=" * 50)
cursor.execute("""
    SELECT 
        d.date,
        d.day_of_week,
        d.quarter,
        s.company_name,
        s.sector,
        f.close,
        f.volume
    FROM fact_stock_prices f
    JOIN dim_dates d ON f.date_id = d.date_id
    JOIN dim_stocks s ON f.stock_id = s.stock_id
    WHERE s.symbol = 'AAPL'
    ORDER BY d.date DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(row)

# ── Query 4: Average close by sector and month ────────────
print("\n" + "=" * 50)
print("AVERAGE CLOSE BY SECTOR AND MONTH")
print("=" * 50)
cursor.execute("""
    SELECT 
        s.sector,
        d.month,
        ROUND(AVG(f.close)::numeric, 2) AS avg_close
    FROM fact_stock_prices f
    JOIN dim_dates d ON f.date_id = d.date_id
    JOIN dim_stocks s ON f.stock_id = s.stock_id
    GROUP BY s.sector, d.month
    ORDER BY s.sector, d.month
""")
for row in cursor.fetchall():
    print(f"Sector: {row[0]}, Month: {row[1]}, Avg Close: ${row[2]}")

cursor.close()
conn.close()