from fastapi import FastAPI, HTTPException

from src.db.connection import check_db

app = FastAPI(title="soup-job-queue")


@app.get("/health")
def health():
    if not check_db():
        raise HTTPException(status_code=503, detail="db unreachable")
    return {"status": "ok", "db": "ok"}