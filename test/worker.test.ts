import type pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { PermanentJobError, Queue, Worker, type WorkerOptions } from '../src/index.js';
import { createTestPool, DATABASE_URL, resetDatabase, waitFor } from './helpers.js';

describe.skipIf(!DATABASE_URL)('Worker', () => {
  let pool: pg.Pool;
  let queue: Queue;
  const workers: Worker[] = [];

  const createWorker = (opts: WorkerOptions = {}) => {
    const worker = new Worker(pool, {
      pollIntervalMs: 50,
      backoff: { baseMs: 1, maxMs: 5 },
      scheduler: false,
      ...opts,
    });
    workers.push(worker);
    return worker;
  };

  beforeAll(async () => {
    pool = await createTestPool();
    queue = new Queue(pool);
  });
  beforeEach(() => resetDatabase(pool));
  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.stop()));
  });
  afterAll(() => pool.end());

  it('processes a job and stores its result', async () => {
    const worker = createWorker().register<{ n: number }, number>('double', ({ n }) => n * 2);
    await worker.start();
    const job = await queue.enqueue('double', { n: 21 });

    await waitFor(async () => (await queue.get(job.id))?.status === 'completed');
    expect((await queue.get(job.id))!.result).toBe(42);
  });

  it('retries transient failures with backoff, then succeeds', async () => {
    let calls = 0;
    const worker = createWorker().register('flaky', () => {
      calls += 1;
      if (calls < 3) throw new Error('temporary outage');
      return 'ok';
    });
    await worker.start();
    const job = await queue.enqueue('flaky', {});

    await waitFor(async () => (await queue.get(job.id))?.status === 'completed');
    const done = (await queue.get(job.id))!;
    expect(done.attempts).toBe(3);
    expect(done.lastError).toContain('temporary outage');
  });

  it('dead-letters a job after max attempts', async () => {
    const worker = createWorker().register('always-fails', () => {
      throw new Error('boom');
    });
    await worker.start();
    const job = await queue.enqueue('always-fails', {}, { maxAttempts: 3 });

    await waitFor(async () => (await queue.get(job.id))?.status === 'dead');
    expect((await queue.get(job.id))!.attempts).toBe(3);
  });

  it('skips retries on PermanentJobError', async () => {
    const worker = createWorker().register('invalid', () => {
      throw new PermanentJobError('bad payload');
    });
    await worker.start();
    const job = await queue.enqueue('invalid', {}, { maxAttempts: 10 });

    await waitFor(async () => (await queue.get(job.id))?.status === 'dead');
    expect((await queue.get(job.id))!.attempts).toBe(1);
  });

  it('processes higher-priority jobs first', async () => {
    const order: string[] = [];
    await queue.enqueue('track', { id: 'low' }, { priority: 0 });
    await queue.enqueue('track', { id: 'high' }, { priority: 10 });
    await queue.enqueue('track', { id: 'mid' }, { priority: 5 });

    const worker = createWorker({ concurrency: 1 }).register<{ id: string }, void>(
      'track',
      ({ id }) => {
        order.push(id);
      },
    );
    await worker.start();
    await waitFor(() => order.length === 3);
    expect(order).toEqual(['high', 'mid', 'low']);
  });

  it('does not run delayed jobs early', async () => {
    const worker = createWorker().register('later', () => 'ran');
    await worker.start();
    const job = await queue.enqueue('later', {}, { delayMs: 60_000 });
    await new Promise((r) => setTimeout(r, 300));
    expect((await queue.get(job.id))!.status).toBe('pending');
  });

  it('never processes a job twice across competing workers (SKIP LOCKED)', async () => {
    const seen = new Map<number, number>();
    const handler = async ({ i }: { i: number }) => {
      seen.set(i, (seen.get(i) ?? 0) + 1);
      await new Promise((r) => setTimeout(r, Math.random() * 10));
    };
    for (let w = 0; w < 4; w++) {
      await createWorker({ concurrency: 5 }).register('count', handler).start();
    }
    await Promise.all(Array.from({ length: 200 }, (_, i) => queue.enqueue('count', { i })));

    await waitFor(() => seen.size === 200, 15_000);
    await waitFor(async () => (await queue.stats())[0]!.completed === 200);
    expect([...seen.values()].every((count) => count === 1)).toBe(true);
  });

  it('aborts jobs that exceed the timeout', async () => {
    let aborted = false;
    const worker = createWorker({ jobTimeoutMs: 100 }).register('slow', (_p, _j, ctx) => {
      ctx.signal.addEventListener('abort', () => (aborted = true));
      return new Promise(() => {}); // never resolves
    });
    await worker.start();
    const job = await queue.enqueue('slow', {}, { maxAttempts: 1 });

    await waitFor(async () => (await queue.get(job.id))?.status === 'dead');
    expect(aborted).toBe(true);
    expect((await queue.get(job.id))!.lastError).toContain('timed out');
  });

  it('reclaims jobs whose worker died mid-flight', async () => {
    const job = await queue.enqueue('orphan', {});
    // Simulate a worker that claimed the job and then crashed.
    await pool.query(
      `UPDATE taskforge_jobs SET status = 'running', attempts = 1, locked_by = 'dead-worker',
       locked_at = now() - interval '1 hour' WHERE id = $1`,
      [job.id],
    );
    const worker = createWorker({ leaseMs: 1000 }).register('orphan', () => 'recovered');
    expect(await worker.reapExpiredLeases()).toBe(1);
    await worker.start();

    await waitFor(async () => (await queue.get(job.id))?.status === 'completed');
    expect((await queue.get(job.id))!.result).toBe('recovered');
  });

  it('enqueues each scheduled run exactly once across workers', async () => {
    await queue.schedule('tick', '* * * * *', 'tick');
    await pool.query("UPDATE taskforge_schedules SET next_run_at = now() - interval '1 second'");

    const a = createWorker().register('tick', () => null);
    const b = createWorker().register('tick', () => null);
    const results = await Promise.all([
      a.enqueueDueSchedules(),
      b.enqueueDueSchedules(),
      a.enqueueDueSchedules(),
    ]);
    expect(results.reduce((x, y) => x + y, 0)).toBe(1);
    const [schedule] = await queue.schedules();
    expect(schedule!.nextRunAt.getTime()).toBeGreaterThan(Date.now());
  });

  it('waits for in-flight jobs on graceful shutdown', async () => {
    let finished = false;
    const worker = createWorker({ shutdownTimeoutMs: 2000 }).register('work', async () => {
      await new Promise((r) => setTimeout(r, 200));
      finished = true;
    });
    await worker.start();
    await queue.enqueue('work', {});
    await waitFor(() => worker.activeCount === 1);
    await worker.stop();
    expect(finished).toBe(true);
  });
});
