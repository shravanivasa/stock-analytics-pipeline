import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="db.dvpwyivylcezwyftudxg.supabase.co",
    port=5432,
    database="postgres",
    user="postgres",
    password="Jobsearch@3105"
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        date DATE,
        symbol VARCHAR(10),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    )
""")

conn.commit()
print("Table created!")

df = pd.read_csv("all_stocks_data.csv")
print(f"Loaded {len(df)} rows from CSV")

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO stock_prices (date, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['date'], row['symbol'], row['open'], 
          row['high'], row['low'], row['close'], row['volume']))

conn.commit()
print("Data loaded into PostgreSQL!")

cursor.close()
conn.close()