from fastapi import FastAPI
from nse500_fetcher import fetch_nse500_data

app = FastAPI()

@app.get("/fetch_nse500")
def fetch_nse500():
    data, failed = fetch_nse500_data()
    return {
        "status": "completed",
        "successful_count": len(data),
        "failed_count": len(failed),
        "failed_tickers": failed
    }
