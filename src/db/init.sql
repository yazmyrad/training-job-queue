CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT UNIQUE NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    state           TEXT NOT NULL DEFAULT 'queued'
                    CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
    attempt         INT NOT NULL DEFAULT 0,
    worker_id       TEXT,
    lease_until     TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_pending_idx
    ON jobs (created_at)
    WHERE state = 'queued';

CREATE INDEX jobs_lease_idx
    ON jobs (lease_until)
    WHERE state = 'running';

CREATE TABLE side_effects (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID NOT NULL REFERENCES jobs(id),
    attempt    INT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id)          
);