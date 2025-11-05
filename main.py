from fastapi import FastAPI
from bhavcopy import update_today_bhavcopy, check_missing_dates
from db import get_stock_data

app = FastAPI()

@app.get("/")
def root():
    return {"message": "NSE 500 Bhavcopy API is running"}

@app.get("/update_today")
def update_today():
    return update_today_bhavcopy()

@app.get("/missing_dates")
def missing():
    return check_missing_dates()

@app.get("/get_stock_data")
def get_data(symbol: str):
    return get_stock_data(symbol)
