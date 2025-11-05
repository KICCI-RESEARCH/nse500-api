import pandas as pd
import requests
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
    today = datetime.today()
    conn = get_conn()
    ensure_table_exists(conn)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM bhavcopy WHERE date = ?", (today.strftime('%Y-%m-%d'),))
        if cursor.fetchone()[0] > 0:
            return {"status": "already updated"}

        url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }

        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            records = data["data"]
            df = pd.DataFrame([{
                "symbol": r["symbol"],
                "open": r["open"],
                "high": r["dayHigh"],
                "low": r["dayLow"],
                "close": r["lastPrice"],
                "volume": r["quantityTraded"],
                "date": today.strftime('%Y-%m-%d')
            } for r in records])
            df.to_sql("bhavcopy", conn, if_exists="append", index=False)
            return {"status": "updated"}
        else:
            return {"status": "NSE API error", "code": response.status_code}
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

