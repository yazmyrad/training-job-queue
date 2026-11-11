from fastapi import FastAPI, HTTPException

from src.db.connection import get_db
from src.api.routes import router as jobs_router

app = FastAPI(title="soup-job-queue")
app.include_router(jobs_router)

@app.get("/health")
def health():
    if not get_db():
        raise HTTPException(status_code=503, detail="db unreachable")
    return {"status": "ok", "db": "ok"}