import { EventEmitter } from 'node:events';
import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { DEFAULT_BACKOFF, retryDelayMs, type BackoffOptions } from './backoff.js';
import { nextCronRun } from './queue.js';
import {
  PermanentJobError,
  toJob,
  type Job,
  type JobContext,
  type JobHandler,
  type JobRow,
} from './types.js';

export interface WorkerOptions {
  /** Queues this worker consumes. Default ['default']. */
  queues?: string[];
  /** Max jobs processed in parallel by this worker. Default 5. */
  concurrency?: number;
  /** Fallback poll interval; LISTEN/NOTIFY wakes the worker sooner. Default 1000ms. */
  pollIntervalMs?: number;
  /** A job running longer than this is aborted and counted as a failed attempt. Default 5 min. */
  jobTimeoutMs?: number;
  /** Running jobs whose lock is older than this are presumed orphaned and requeued. */
  leaseMs?: number;
  /** Whether this worker also enqueues due cron schedules. Safe to enable on every worker. */
  scheduler?: boolean;
  backoff?: BackoffOptions;
  /** How long stop() waits for in-flight jobs before aborting them. Default 30s. */
  shutdownTimeoutMs?: number;
  id?: string;
}

export interface WorkerEvents {
  completed: [job: Job, result: unknown];
  retrying: [job: Job, error: Error, delayMs: number];
  dead: [job: Job, error: Error];
  error: [error: Error];
}

export class Worker extends EventEmitter<WorkerEvents> {
  readonly id: string;
  private readonly handlers = new Map<string, JobHandler>();
  private readonly opts: Required<Omit<WorkerOptions, 'id'>>;
  private readonly active = new Map<string, AbortController>();
  private running = false;
  private loopDone: Promise<void> = Promise.resolve();
  private wakeUp: (() => void) | null = null;
  private listener: PoolClient | null = null;
  private timers: NodeJS.Timeout[] = [];

  constructor(
    private readonly pool: Pool,
    options: WorkerOptions = {},
  ) {
    super();
    this.id = options.id ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    const jobTimeoutMs = options.jobTimeoutMs ?? 5 * 60_000;
    this.opts = {
      queues: options.queues ?? ['default'],
      concurrency: options.concurrency ?? 5,
      pollIntervalMs: options.pollIntervalMs ?? 1000,
      jobTimeoutMs,
      leaseMs: options.leaseMs ?? jobTimeoutMs + 60_000,
      scheduler: options.scheduler ?? true,
      backoff: options.backoff ?? DEFAULT_BACKOFF,
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? 30_000,
    };
  }

  register<T, R>(name: string, handler: JobHandler<T, R>): this {
    this.handlers.set(name, handler as JobHandler);
    return this;
  }

  get activeCount(): number {
    return this.active.size;
  }

  async start(): Promise<void> {
    if (this.running) return;
    if (this.handlers.size === 0) throw new Error('Register at least one handler before start()');
    this.running = true;

    this.listener = await this.pool.connect();
    this.listener.on('notification', (msg) => {
      if (msg.payload && this.opts.queues.includes(msg.payload)) this.wake();
    });
    this.listener.on('error', (err) => this.emit('error', err));
    await this.listener.query('LISTEN taskforge_jobs');

    this.every(Math.max(1000, this.opts.leaseMs / 4), () => this.reapExpiredLeases());
    if (this.opts.scheduler) this.every(1000, () => this.enqueueDueSchedules());

    this.loopDone = this.loop();
  }

  /** Stop fetching, wait for in-flight jobs (up to shutdownTimeoutMs), then abort stragglers. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.timers.forEach(clearInterval);
    this.timers = [];
    this.wake();
    await this.loopDone;

    const deadline = Date.now() + this.opts.shutdownTimeoutMs;
    while (this.active.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25));
    }
    for (const controller of this.active.values()) controller.abort(new Error('worker shutdown'));
    while (this.active.size > 0) await new Promise((r) => setTimeout(r, 10));

    if (this.listener) {
      await this.listener.query('UNLISTEN taskforge_jobs').catch(() => {});
      this.listener.release();
      this.listener = null;
    }
  }

  private async loop(): Promise<void> {
    while (this.running) {
      if (this.active.size >= this.opts.concurrency) {
        await this.sleep(this.opts.pollIntervalMs);
        continue;
      }
      let job: Job | null = null;
      try {
        job = await this.fetchNext();
      } catch (error) {
        this.emit('error', error as Error);
      }
      if (job) {
        void this.process(job);
      } else {
        await this.sleep(this.opts.pollIntervalMs);
      }
    }
  }

  /**
   * Atomically claim the next due job. FOR UPDATE SKIP LOCKED lets any number of workers
   * poll the same table concurrently: each row is claimed by exactly one of them, and
   * nobody blocks waiting on a row another worker already holds.
   */
  private async fetchNext(): Promise<Job | null> {
    const { rows } = await this.pool.query<JobRow>(
      `UPDATE taskforge_jobs
       SET status = 'running', attempts = attempts + 1, locked_at = now(), locked_by = $1, updated_at = now()
       WHERE id = (
         SELECT id FROM taskforge_jobs
         WHERE status = 'pending' AND queue = ANY($2) AND name = ANY($3) AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         FOR UPDATE SKIP LOCKED
         LIMIT 1
       )
       RETURNING *`,
      [this.id, this.opts.queues, [...this.handlers.keys()]],
    );
    return rows[0] ? toJob(rows[0]) : null;
  }

  private async process(job: Job): Promise<void> {
    const controller = new AbortController();
    this.active.set(job.id, controller);
    const timeout = setTimeout(
      () => controller.abort(new Error(`job timed out after ${this.opts.jobTimeoutMs}ms`)),
      this.opts.jobTimeoutMs,
    );
    const ctx: JobContext = {
      signal: controller.signal,
      log: (message, extra) =>
        console.log(
          JSON.stringify({ level: 'info', jobId: job.id, job: job.name, message, ...extra }),
        ),
    };

    try {
      const handler = this.handlers.get(job.name)!;
      const result = await Promise.race([
        Promise.resolve(handler(job.payload, job, ctx)),
        new Promise<never>((_, reject) => {
          controller.signal.addEventListener('abort', () => reject(controller.signal.reason), {
            once: true,
          });
        }),
      ]);
      await this.complete(job, result);
      this.emit('completed', job, result);
    } catch (thrown) {
      const error = thrown instanceof Error ? thrown : new Error(String(thrown));
      await this.fail(job, error).catch((e: Error) => this.emit('error', e));
    } finally {
      clearTimeout(timeout);
      this.active.delete(job.id);
      this.wake();
    }
  }

  private async complete(job: Job, result: unknown): Promise<void> {
    // locked_by guard: if our lease expired and another worker took over, don't clobber it.
    await this.pool.query(
      `UPDATE taskforge_jobs
       SET status = 'completed', result = $3, locked_at = NULL, locked_by = NULL,
           finished_at = now(), updated_at = now()
       WHERE id = $1 AND locked_by = $2`,
      [job.id, this.id, JSON.stringify(result ?? null)],
    );
  }

  private async fail(job: Job, error: Error): Promise<void> {
    const permanent = error instanceof PermanentJobError;
    const exhausted = job.attempts >= job.maxAttempts;
    const message = `${error.name}: ${error.message}`.slice(0, 4000);

    if (permanent || exhausted) {
      await this.pool.query(
        `UPDATE taskforge_jobs
         SET status = 'dead', last_error = $3, locked_at = NULL, locked_by = NULL,
             finished_at = now(), updated_at = now()
         WHERE id = $1 AND locked_by = $2`,
        [job.id, this.id, message],
      );
      this.emit('dead', job, error);
      return;
    }

    const delayMs = retryDelayMs(job.attempts, this.opts.backoff);
    await this.pool.query(
      `UPDATE taskforge_jobs
       SET status = 'pending', last_error = $3, locked_at = NULL, locked_by = NULL,
           run_at = now() + ($4 || ' milliseconds')::interval, updated_at = now()
       WHERE id = $1 AND locked_by = $2`,
      [job.id, this.id, message, delayMs],
    );
    this.emit('retrying', job, error, delayMs);
  }

  /** Requeue jobs held by workers that crashed or lost their connection. */
  async reapExpiredLeases(): Promise<number> {
    const { rowCount } = await this.pool.query(
      `UPDATE taskforge_jobs
       SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END::taskforge_job_status,
           finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
           last_error = 'Lease expired: worker stopped responding',
           locked_at = NULL, locked_by = NULL, run_at = now(), updated_at = now()
       WHERE status = 'running' AND locked_at < now() - ($1 || ' milliseconds')::interval`,
      [this.opts.leaseMs],
    );
    return rowCount ?? 0;
  }

  /**
   * Enqueue jobs for due cron schedules. Rows are claimed with SKIP LOCKED and each run uses
   * an idempotency key derived from its scheduled time, so running the scheduler on many
   * workers never produces duplicate jobs.
   */
  async enqueueDueSchedules(): Promise<number> {
    const client = await this.pool.connect();
    let enqueued = 0;
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<{
        name: string;
        cron: string;
        queue: string;
        job_name: string;
        payload: unknown;
        next_run_at: Date;
      }>(
        `SELECT * FROM taskforge_schedules WHERE next_run_at <= now()
         FOR UPDATE SKIP LOCKED`,
      );
      for (const s of rows) {
        const { rowCount } = await client.query(
          `INSERT INTO taskforge_jobs (queue, name, payload, idempotency_key)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
          [
            s.queue,
            s.job_name,
            JSON.stringify(s.payload),
            `schedule:${s.name}:${s.next_run_at.toISOString()}`,
          ],
        );
        enqueued += rowCount ?? 0;
        await client.query(
          'UPDATE taskforge_schedules SET next_run_at = $2, updated_at = now() WHERE name = $1',
          [s.name, nextCronRun(s.cron, new Date())],
        );
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
    return enqueued;
  }

  private every(ms: number, task: () => Promise<unknown>): void {
    const timer = setInterval(() => {
      task().catch((e: Error) => this.emit('error', e));
    }, ms);
    timer.unref();
    this.timers.push(timer);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        if (this.wakeUp === done) this.wakeUp = null;
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.wakeUp = done;
    });
  }

  private wake(): void {
    this.wakeUp?.();
  }
}
