from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    print("Health endpoint was pinged")
    return {"ok": True}
