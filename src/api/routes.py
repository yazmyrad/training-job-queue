import datetime
import json
import uuid
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from src.db.connection import get_db
from src.db.models import CreateJobReq

router = APIRouter()


def _job_row_to_dict(row):
    return {
        "id": str(row[0]),
        "state": row[1],
        "created_at": row[2].isoformat(),
    }


@router.post("/jobs", status_code=201)
def create_jobs(req: CreateJobReq, db=Depends(get_db)):
    q = """
        INSERT INTO jobs (idempotency_key, payload)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id, state, created_at;
    """
    with db:                              
        with db.cursor() as cur: 
            cur.execute(q, (req.idempotency_key, json.dumps(req.payload)))
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "SELECT id, state, created_at FROM jobs WHERE idempotency_key = %s;",
                    (req.idempotency_key,),
                )
                row = cur.fetchone()
                return JSONResponse(content=_job_row_to_dict(row), status_code=200)
            return _job_row_to_dict(row)
        

@router.get("/jobs/{id}")
def get_jobs(id: uuid.UUID, db=Depends(get_db)):
    q = """
        SELECT id, idempotency_key, payload, state, attempt,
            worker_id, lease_until, last_error, created_at, updated_at
        FROM jobs 
        WHERE id = %s;
    """                             
    with db.cursor() as cur: 
        cur.execute(q, (str(id),))
        row = cur.fetchone()
        if row:
            return JSONResponse(content=jsonable_encoder(row), status_code=200)
        raise HTTPException(detail="job not found", status_code=404)


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: uuid.UUID, db=Depends(get_db)):
    update_q = """
        UPDATE jobs
        SET state = 'cancelled', updated_at = now()
        WHERE id = %s AND state IN ('queued','running')
        RETURNING id, state;
    """
    with db:
        with db.cursor() as cur:
            cur.execute(update_q, (str(job_id),))
            row = cur.fetchone()
            if row is not None:
                return {"state": "cancelled"}        

            cur.execute("SELECT state FROM jobs WHERE id = %s;", (str(job_id),))
            current = cur.fetchone()

    if current is None:
        raise HTTPException(status_code=404, detail="job not found")
    if current[0] == "cancelled":
        return {"state": "cancelled"}           
    raise HTTPException(status_code=409, detail=f"job is {current[0]}, cannot cancel")

