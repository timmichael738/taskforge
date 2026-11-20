# Taskforge

[![CI](https://github.com/timmichael738/taskforge/actions/workflows/ci.yml/badge.svg)](https://github.com/timmichael738/taskforge/actions/workflows/ci.yml)
![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178c6)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

**A reliable background job queue for Node.js, built on the Postgres you already run.** No Redis,
no broker: jobs live in a table and are claimed with `FOR UPDATE SKIP LOCKED`, so any number of
workers can compete for work safely. Includes retries with jittered exponential backoff, a
dead-letter queue, cron schedules, idempotent enqueue and a live dashboard.

```ts
const queue = new Queue(pool);
await queue.enqueue(
  'send-email',
  { to: 'ada@example.com' },
  { priority: 10, idempotencyKey: 'welcome:42' },
);

new Worker(pool, { concurrency: 10 })
  .register('send-email', async ({ to }, job, { signal }) => {
    await mailer.send({ to, signal });
  })
  .start();
```

## Features

|                             |                                                                                                                                                                      |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Exactly-one claim**       | `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)`. Competing workers never block each other or double-process a job. Tested with 4 workers × 200 jobs. |
| **Instant pickup**          | A Postgres trigger fires `NOTIFY` on insert. Idle workers wake immediately instead of waiting for the next poll.                                                     |
| **Retries**                 | Exponential backoff with _full jitter_ to avoid synchronised retry storms. Throw `PermanentJobError` to skip retries.                                                |
| **Dead-letter queue**       | Jobs that exhaust `maxAttempts` become `dead`, keep their last error and can be retried from the dashboard or API.                                                   |
| **Crash recovery**          | Each claim is a lease. A reaper requeues jobs whose worker died. Completion is guarded by `locked_by`, so a zombie worker can't overwrite a reclaimed job.           |
| **Timeouts & cancellation** | Handlers receive an `AbortSignal` that fires on timeout or shutdown.                                                                                                 |
| **Cron schedules**          | Run the scheduler on every worker: schedule rows are claimed with `SKIP LOCKED`, and each run has an idempotency key, so no run is ever duplicated.                  |
| **Idempotent enqueue**      | `idempotencyKey` is backed by a partial unique index, so double-submitting a request can't create duplicate jobs.                                                    |
| **Priorities & delays**     | `priority` (higher first) and `delayMs`/`runAt`, served by a partial index that only covers pending jobs.                                                            |
| **Graceful shutdown**       | `stop()` stops fetching, lets in-flight jobs finish, then aborts any that are left.                                                                                  |
| **Dashboard**               | REST API plus a zero-dependency live UI: counts by status, recent jobs, errors and one-click retry.                                                                  |

## Quick start

```bash
docker compose up --build   # Postgres + demo producers/workers + dashboard
open http://localhost:3000
```

The demo enqueues a stream of jobs. Some fail transiently and retry, and some fail permanently
and land in the dead-letter queue.

Run it against a local database instead:

```bash
docker compose up -d postgres
npm install
npm run demo
```

## Architecture

```text
 producers ──enqueue──▶ ┌──────────── PostgreSQL ────────────┐ ◀──claim (SKIP LOCKED)── worker A
                        │ taskforge_jobs      (partial idx)  │ ◀──claim (SKIP LOCKED)── worker B
 dashboard ──REST──────▶│ taskforge_schedules                │ ──NOTIFY taskforge_jobs──▶ A, B
                        └────────────────────────────────────┘
```

Job lifecycle:

```text
pending ──claim──▶ running ──success──▶ completed
   ▲                  │
   └──retry+backoff───┤ failure, attempts < max
                      └──▶ dead (attempts exhausted, PermanentJobError, or lease expired too often)
```

## API

```ts
// Producer
queue.enqueue(name, payload, { queue, priority, delayMs, runAt, maxAttempts, idempotencyKey })
queue.get(id) / queue.list({ status, queue, limit, offset }) / queue.stats()
queue.retry(id)            // dead → pending
queue.remove(id)
queue.purgeCompleted(olderThanMs)
queue.schedule(name, cron, jobName, payload)   // upsert
queue.unschedule(name)

// Consumer
const worker = new Worker(pool, { queues, concurrency, jobTimeoutMs, leaseMs, scheduler, backoff });
worker.register(name, handler).start();
worker.on('completed' | 'retrying' | 'dead' | 'error', ...);
await worker.stop();

// Setup
await migrate(pool);       // idempotent, advisory-locked migrations
const app = await createDashboard(queue);
```

## Testing

The integration tests run against a real Postgres. They cover concurrency (no double
processing), retries, the DLQ, timeouts, lease reclamation, scheduler deduplication and
graceful shutdown.

```bash
docker compose up -d postgres
DATABASE_URL=postgres://taskforge:taskforge@localhost:5432/taskforge npm test
```

## Design trade-offs

- **Why Postgres instead of Redis/RabbitMQ?** One less piece of infrastructure, and jobs can be
  enqueued **in the same transaction** as the business write that triggered them (no dual-write
  problem). Postgres handles thousands of jobs per second with `SKIP LOCKED`, which is plenty for most apps.
- **At-least-once delivery.** If a worker crashes after doing the work but before marking it
  complete, the job runs again. Handlers should be idempotent, which is the standard contract for job queues.

## License

[MIT](LICENSE) © Michael Timothy
