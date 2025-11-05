import pandas as pd
import yfinance as yf
import requests
import io
from datetime import datetime
from db import get_conn

def ensure_table_exists(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bhavcopy (
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            date TEXT
        )
    """)

def get_nifty500_symbols():
    url = "https://raw.githubusercontent.com/Hpareek07/NSEData/master/ind_nifty500list.csv"
    try:
        response = requests.get(url, timeout=10)
        df = pd.read_csv(io.StringIO(response.text))
        symbols = df["Symbol"].dropna().unique().tolist()
        return [s + ".NS" for s in symbols]
    except Exception as e:
        print("Error fetching symbols:", e)
        return []

def update_today_bhavcopy():
    today = datetime.today().strftime('%Y-%m-%d')
    conn = get_conn()
    ensure_table_exists(conn)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM bhavcopy WHERE date = ?", (today,))
        if cursor.fetchone()[0] > 0:
            print("Already updated for today.")
            return {"status": "already updated"}

        symbols = get_nifty500_symbols()
        if not symbols:
            print("Failed to fetch symbols.")
            return {"status": "failed to fetch symbols"}

        records = []
        batches = [symbols[i:i+50] for i in range(0, len(symbols), 50)]
        for batch_num, batch in enumerate(batches, start=1):
            print(f"Processing batch {batch_num}/{len(batches)}...")
            for symbol in batch:
                try:
                    print(f"Fetching {symbol}...")
                    data = yf.download(symbol, start=today, end=today)
                    if not data.empty:
                        row = data.iloc[0]
                        records.append({
                            "symbol": symbol.replace(".NS", ""),
                            "open": row["Open"],
                            "high": row["High"],
                            "low": row["Low"],
                            "close": row["Close"],
                            "volume": int(row["Volume"]),
                            "date": today
                        })
                except Exception as e:
                    print(f"Error fetching {symbol}: {e}")

        if records:
            df = pd.DataFrame(records)
            df.to_sql("bhavcopy", conn, if_exists="append", index=False)
            print(f"Inserted {len(records)} records.")
            return {"status": "updated", "count": len(records)}
        else:
            print("No data found.")
            return {"status": "no data found"}

    except Exception as e:
        import traceback
        print("Exception occurred:", e)
        return {
            "error": str(e),
            "trace": traceback.format_exc()
        }

def check_missing_dates():
    conn = get_conn()
    ensure_table_exists(conn)
    df = pd.read_sql("SELECT DISTINCT date FROM bhavcopy", conn)
    df["date"] = pd.to_datetime(df["date"])
    all_days = pd.date_range(start=df["date"].min(), end=datetime.today(), freq="B")
    missing = all_days.difference(df["date"])
    return {"missing_dates": missing.strftime("%Y-%m-%d").tolist()}

def load_historical_data():
    symbols = get_nifty500_symbols()
    start = (datetime.today() - pd.Timedelta(days=1000)).strftime('%Y-%m-%d')
    end = (datetime.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    conn = get_conn()
    ensure_table_exists(conn)

    batches = [symbols[i:i+50] for i in range(0, len(symbols), 50)]
    for batch_num, batch in enumerate(batches, start=1):
        print(f"Loading batch {batch_num}/{len(batches)}...")
        for symbol in batch:
            try:
                print(f"Fetching history for {symbol}")
                data = yf.download(symbol, start=start, end=end)
                if not data.empty:
                    data.reset_index(inplace=True)
                    data["symbol"] = symbol.replace(".NS", "")
                    data.rename(columns={
                        "Open": "open", "High": "high", "Low": "low",
                        "Close": "close", "Volume": "volume", "Date": "date"
                    }, inplace=True)
                    data[["symbol", "open", "high", "low", "close", "volume", "date"]].to_sql(
                        "bhavcopy", conn, if_exists="append", index=False
                    )
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

