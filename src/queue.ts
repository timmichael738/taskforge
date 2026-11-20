import { CronExpressionParser } from 'cron-parser';
import type { Pool } from 'pg';
import { toJob, type EnqueueOptions, type Job, type JobRow, type JobStatus } from './types.js';

export interface ListOptions {
  status?: JobStatus;
  queue?: string;
  limit?: number;
  offset?: number;
}

export interface QueueStats {
  queue: string;
  pending: number;
  running: number;
  completed: number;
  dead: number;
}

export interface Schedule {
  name: string;
  cron: string;
  queue: string;
  jobName: string;
  payload: unknown;
  nextRunAt: Date;
}

/** Producer-side API: enqueue, inspect and manage jobs. Safe to share across the app. */
export class Queue {
  constructor(private readonly pool: Pool) {}

  async enqueue<T>(name: string, payload: T, options: EnqueueOptions = {}): Promise<Job<T>> {
    const queue = options.queue ?? 'default';
    const runAt = options.runAt ?? new Date(Date.now() + (options.delayMs ?? 0));
    const { rows } = await this.pool.query<JobRow>(
      `INSERT INTO taskforge_jobs (queue, name, payload, priority, max_attempts, run_at, idempotency_key)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
       RETURNING *`,
      [
        queue,
        name,
        JSON.stringify(payload ?? {}),
        options.priority ?? 0,
        options.maxAttempts ?? 5,
        runAt,
        options.idempotencyKey ?? null,
      ],
    );
    if (rows[0]) return toJob<T>(rows[0]);

    // Idempotency key already used: return the original job.
    const existing = await this.pool.query<JobRow>(
      'SELECT * FROM taskforge_jobs WHERE queue = $1 AND idempotency_key = $2',
      [queue, options.idempotencyKey],
    );
    return toJob<T>(existing.rows[0]!);
  }

  async get<T = unknown>(id: string): Promise<Job<T> | null> {
    const { rows } = await this.pool.query<JobRow>('SELECT * FROM taskforge_jobs WHERE id = $1', [
      id,
    ]);
    return rows[0] ? toJob<T>(rows[0]) : null;
  }

  async list({ status, queue, limit = 50, offset = 0 }: ListOptions = {}): Promise<Job[]> {
    const { rows } = await this.pool.query<JobRow>(
      `SELECT * FROM taskforge_jobs
       WHERE ($1::taskforge_job_status IS NULL OR status = $1)
         AND ($2::text IS NULL OR queue = $2)
       ORDER BY updated_at DESC, id DESC
       LIMIT $3 OFFSET $4`,
      [status ?? null, queue ?? null, Math.min(limit, 500), offset],
    );
    return rows.map((r) => toJob(r));
  }

  async stats(): Promise<QueueStats[]> {
    const { rows } = await this.pool.query<QueueStats>(
      `SELECT queue,
              count(*) FILTER (WHERE status = 'pending')::int   AS pending,
              count(*) FILTER (WHERE status = 'running')::int   AS running,
              count(*) FILTER (WHERE status = 'completed')::int AS completed,
              count(*) FILTER (WHERE status = 'dead')::int      AS dead
       FROM taskforge_jobs GROUP BY queue ORDER BY queue`,
    );
    return rows;
  }

  /** Move a dead-lettered job back into the queue with a fresh attempt budget. */
  async retry(id: string): Promise<Job | null> {
    const { rows } = await this.pool.query<JobRow>(
      `UPDATE taskforge_jobs
       SET status = 'pending', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
       WHERE id = $1 AND status = 'dead'
       RETURNING *`,
      [id],
    );
    if (rows[0]) await this.pool.query("SELECT pg_notify('taskforge_jobs', $1)", [rows[0].queue]);
    return rows[0] ? toJob(rows[0]) : null;
  }

  /** Delete a job that is not currently running. */
  async remove(id: string): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      "DELETE FROM taskforge_jobs WHERE id = $1 AND status <> 'running'",
      [id],
    );
    return rowCount === 1;
  }

  /** Remove finished jobs older than the given age. Returns the number deleted. */
  async purgeCompleted(olderThanMs: number): Promise<number> {
    const { rowCount } = await this.pool.query(
      `DELETE FROM taskforge_jobs
       WHERE status = 'completed' AND finished_at < now() - ($1 || ' milliseconds')::interval`,
      [olderThanMs],
    );
    return rowCount ?? 0;
  }

  /** Create or update a recurring job. Workers with the scheduler enabled will enqueue it. */
  async schedule(
    name: string,
    cron: string,
    jobName: string,
    payload: unknown = {},
    options: { queue?: string } = {},
  ): Promise<Schedule> {
    const nextRunAt = nextCronRun(cron, new Date());
    await this.pool.query(
      `INSERT INTO taskforge_schedules (name, cron, queue, job_name, payload, next_run_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (name) DO UPDATE
       SET cron = EXCLUDED.cron, queue = EXCLUDED.queue, job_name = EXCLUDED.job_name,
           payload = EXCLUDED.payload, next_run_at = EXCLUDED.next_run_at, updated_at = now()`,
      [name, cron, options.queue ?? 'default', jobName, JSON.stringify(payload), nextRunAt],
    );
    return { name, cron, queue: options.queue ?? 'default', jobName, payload, nextRunAt };
  }

  async unschedule(name: string): Promise<boolean> {
    const { rowCount } = await this.pool.query('DELETE FROM taskforge_schedules WHERE name = $1', [
      name,
    ]);
    return rowCount === 1;
  }

  async schedules(): Promise<Schedule[]> {
    const { rows } = await this.pool.query<{
      name: string;
      cron: string;
      queue: string;
      job_name: string;
      payload: unknown;
      next_run_at: Date;
    }>('SELECT * FROM taskforge_schedules ORDER BY name');
    return rows.map((r) => ({
      name: r.name,
      cron: r.cron,
      queue: r.queue,
      jobName: r.job_name,
      payload: r.payload,
      nextRunAt: r.next_run_at,
    }));
  }
}

export function nextCronRun(cron: string, after: Date): Date {
  return CronExpressionParser.parse(cron, { currentDate: after, tz: 'UTC' }).next().toDate();
}
