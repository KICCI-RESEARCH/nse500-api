git add main.py
git commit -m "Trigger redeploy"
git push



from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test")
def test():
    return {"status": "test route active"}
