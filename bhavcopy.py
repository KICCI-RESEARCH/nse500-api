import pandas as pd
import yfinance as yf
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

def update_today_bhavcopy():
    today = datetime.today().strftime('%Y-%m-%d')
    conn = get_conn()
    ensure_table_exists(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM bhavcopy WHERE date = ?", (today,))
        if cursor.fetchone()[0] > 0:
            return {"status": "already updated"}

        # List of NSE symbols to fetch (append .NS for Yahoo Finance)
        symbols = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS"]
        records = []

        for symbol in symbols:
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

        if records:
            df = pd.DataFrame(records)
            df.to_sql("bhavcopy", conn, if_exists="append", index=False)
            return {"status": "updated", "count": len(records)}
        else:
            return {"status": "no data found"}
    except Exception as e:
        import traceback
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


