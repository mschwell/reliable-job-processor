-- Demo schema. Startup creates missing tables; use versioned migrations in production.
CREATE TABLE IF NOT EXISTS jobs (
    id                 uuid PRIMARY KEY,
    kind               text NOT NULL,
    payload            jsonb NOT NULL,
    idempotency_key    text NULL,
    status             text NOT NULL DEFAULT 'queued'
                       CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
    attempts           integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts       integer NOT NULL CHECK (max_attempts >= 1),
    scheduled_for      timestamptz NULL,
    available_at       timestamptz NOT NULL DEFAULT now(),
    lease_token        uuid NULL,
    lease_expires_at   timestamptz NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    completed_at       timestamptz NULL,
    last_error         text NULL,
    CONSTRAINT running_lease_required CHECK (
       (status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
       OR (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_idempotency_key
  ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_jobs_due
  ON jobs(available_at, created_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS ix_jobs_expired
  ON jobs(lease_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS ix_jobs_created
  ON jobs(created_at DESC);

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id       uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_no   integer NOT NULL,
    outcome      text NOT NULL CHECK (outcome IN ('running', 'succeeded', 'failed', 'lease_expired')),
    started_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz NULL,
    error        text NULL,
    PRIMARY KEY (job_id, attempt_no)
);

-- A synthetic, transactional sink. Unique job_id makes replay idempotent.
CREATE TABLE IF NOT EXISTS job_effects (
    job_id     uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    output     jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
