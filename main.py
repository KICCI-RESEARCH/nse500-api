from fastapi import FastAPI
from bhavcopy import update_today_bhavcopy, load_historical_data

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/update_today")
def update_today():
    return update_today_bhavcopy()

@app.get("/load_history")
def load_history():
    print("Route hit!")
    return {"status": "dummy route works"}

