import yfinance as yf
import time

# Replace this with your actual NSE500 ticker list
NSE500_TICKERS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    # Add more tickers here...
]

def fetch_nse500_data(start_date="2023-01-01", end_date="2023-12-31", delay=1.5):
    successful = {}
    failed = []

    for symbol in NSE500_TICKERS:
        try:
            print(f"Fetching {symbol}...")
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            if not df.empty:
                successful[symbol] = df
            else:
                print(f"No data for {symbol}")
                failed.append(symbol)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            failed.append(symbol)
        time.sleep(delay)  # avoid rate limits

    return successful, failed
