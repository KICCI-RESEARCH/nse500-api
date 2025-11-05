import pandas as pd
import requests, zipfile, io
from datetime import datetime
from db import get_conn

def bhavcopy_url(date):
    return f"https://www1.nseindia.com/content/historical/EQUITIES/{date:%Y}/{date:%b}".upper() + f"/cm{date:%d%m%Y}bhav.csv.zip"

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
    cursor.execute("SELECT COUNT(*) FROM bhavcopy WHERE date = ?", (today.strftime('%Y-%m-%d'),))
    if cursor.fetchone()[0] > 0:
        return {"status": "already updated"}
    try:
        url = bhavcopy_url(today)
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                df = pd.read_csv(z.open(z.namelist()[0]))
                df = df[["SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY"]]
                df["DATE"] = today.strftime('%Y-%m-%d')
                df.columns = ["symbol", "open", "high", "low", "close", "volume", "date"]
                df.to_sql("bhavcopy", conn, if_exists="append", index=False)
                return {"status": "updated"}
        else:
            return {"status": "bhavcopy not available", "code": response.status_code}
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
