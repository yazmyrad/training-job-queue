from pydantic import BaseModel
from typing import Any

class CreateJobReq(BaseModel):
    idempotency_key: str
    payload: dict[str, Any] = {}