import requests
import pandas as pd
import time

# Your API key
API_KEY = "8FHJLOZBMFKLNCMY"  # keep your actual key

# Stocks to track
symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

def fetch_stock_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    
    response = requests.get(url)
    data = response.json()
    
    time_series = data["Time Series (Daily)"]
    
    rows = []
    for date, values in time_series.items():
        rows.append({
            "date": date,
            "symbol": symbol,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        })
    
    return pd.DataFrame(rows)

# Pull data for all 5 stocks
all_data = []

for i, symbol in enumerate(symbols):
    if i > 0:  # wait before every call except the first
        print("Waiting 15 seconds...")
        time.sleep(15)
    print(f"Fetching {symbol}...")
    df = fetch_stock_data(symbol)
    all_data.append(df)

# Combine all stocks into one table
final_df = pd.concat(all_data, ignore_index=True)
print(final_df.head())
print(f"Total shape: {final_df.shape}")

# Save to CSV
final_df.to_csv("all_stocks_data.csv", index=False)
print("Saved to all_stocks_data.csv!")