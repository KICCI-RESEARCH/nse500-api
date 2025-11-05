import sqlite3
import pandas as pd

DB_PATH = "nse500_history.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

def get_stock_data(symbol):
    conn = get_conn()
    df = pd.read_sql(f"SELECT * FROM bhavcopy WHERE symbol = '{symbol.upper()}'", conn)
    conn.close()
    return df.to_dict(orient="records")
