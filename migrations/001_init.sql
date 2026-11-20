CREATE TYPE taskforge_job_status AS ENUM ('pending', 'running', 'completed', 'dead');

CREATE TABLE taskforge_jobs (
  id              BIGSERIAL PRIMARY KEY,
  queue           TEXT NOT NULL DEFAULT 'default',
  name            TEXT NOT NULL,
  payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
  status          taskforge_job_status NOT NULL DEFAULT 'pending',
  priority        INTEGER NOT NULL DEFAULT 0,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_at       TIMESTAMPTZ,
  locked_by       TEXT,
  last_error      TEXT,
  result          JSONB,
  idempotency_key TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ
);

-- The hot path: workers fetch the highest-priority due job per queue.
-- A partial index keeps it tiny no matter how many finished jobs accumulate.
CREATE INDEX taskforge_jobs_fetch_idx
  ON taskforge_jobs (queue, priority DESC, run_at, id)
  WHERE status = 'pending';

-- The reaper looks for jobs whose worker died mid-flight.
CREATE INDEX taskforge_jobs_running_idx
  ON taskforge_jobs (locked_at)
  WHERE status = 'running';

CREATE INDEX taskforge_jobs_status_idx ON taskforge_jobs (status, updated_at DESC);

-- Enqueuing twice with the same key is a no-op (exactly-once enqueue).
CREATE UNIQUE INDEX taskforge_jobs_idempotency_idx
  ON taskforge_jobs (queue, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE taskforge_schedules (
  name        TEXT PRIMARY KEY,
  cron        TEXT NOT NULL,
  queue       TEXT NOT NULL DEFAULT 'default',
  job_name    TEXT NOT NULL,
  payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_run_at TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Wake idle workers the moment a job becomes available instead of waiting for the next poll.
CREATE FUNCTION taskforge_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('taskforge_jobs', NEW.queue);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER taskforge_jobs_notify
  AFTER INSERT ON taskforge_jobs
  FOR EACH ROW
  WHEN (NEW.status = 'pending' AND NEW.run_at <= now())
  EXECUTE FUNCTION taskforge_notify();
