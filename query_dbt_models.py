import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stocks",
    user="postgres",
    password="password"
)

cursor = conn.cursor()

# Query 1 — Check fct_stock_prices
print("=" * 50)
print("FCT_STOCK_PRICES — First 5 rows")
print("=" * 50)
cursor.execute("""
    SELECT 
        price_date,
        symbol,
        close_price,
        daily_range,
        daily_change,
        price_direction
    FROM fct_stock_prices
    ORDER BY price_date DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(row)

# Query 2 — Check dim_stocks
print("\n" + "=" * 50)
print("DIM_STOCKS — All companies")
print("=" * 50)
cursor.execute("SELECT * FROM dim_stocks")
for row in cursor.fetchall():
    print(row)

# Query 3 — Join fct and dim
print("\n" + "=" * 50)
print("FCT + DIM JOINED")
print("=" * 50)
cursor.execute("""
    SELECT 
        f.price_date,
        d.company_name,
        d.sector,
        f.close_price,
        f.price_direction
    FROM fct_stock_prices f
    JOIN dim_stocks d ON f.symbol = d.symbol
    WHERE f.symbol = 'AAPL'
    ORDER BY f.price_date DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()