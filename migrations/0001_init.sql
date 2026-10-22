CREATE TABLE skipq_jobs (
    id               bigserial PRIMARY KEY,
    queue            text        NOT NULL DEFAULT 'default',
    kind             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    state            text        NOT NULL DEFAULT 'available'
                     CHECK (state IN ('available', 'running', 'succeeded')),
    priority         integer     NOT NULL DEFAULT 0,
    attempt          integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 25 CHECK (max_attempts > 0),
    run_at           timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    locked_by        text,
    last_error       text,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

CREATE INDEX skipq_jobs_claim_idx ON skipq_jobs (queue, priority DESC, run_at, id)
    WHERE state = 'available';

CREATE INDEX skipq_jobs_lease_idx ON skipq_jobs (lease_expires_at)
    WHERE state = 'running';

CREATE UNIQUE INDEX skipq_jobs_idempotency_key_idx ON skipq_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE skipq_dead_jobs (
    id              bigint      PRIMARY KEY,
    queue           text        NOT NULL,
    kind            text        NOT NULL,
    payload         jsonb       NOT NULL,
    priority        integer     NOT NULL,
    attempt         integer     NOT NULL,
    max_attempts    integer     NOT NULL,
    last_error      text,
    idempotency_key text,
    created_at      timestamptz NOT NULL,
    dead_at         timestamptz NOT NULL DEFAULT now()
);
