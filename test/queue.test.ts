import type pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createDashboard, Queue } from '../src/index.js';
import { createTestPool, DATABASE_URL, resetDatabase } from './helpers.js';

describe.skipIf(!DATABASE_URL)('Queue', () => {
  let pool: pg.Pool;
  let queue: Queue;

  beforeAll(async () => {
    pool = await createTestPool();
    queue = new Queue(pool);
  });
  beforeEach(() => resetDatabase(pool));
  afterAll(() => pool.end());

  it('enqueues a job with defaults', async () => {
    const job = await queue.enqueue('send-email', { to: 'a@example.com' });
    expect(job).toMatchObject({
      queue: 'default',
      name: 'send-email',
      payload: { to: 'a@example.com' },
      status: 'pending',
      attempts: 0,
      maxAttempts: 5,
    });
  });

  it('deduplicates by idempotency key', async () => {
    const first = await queue.enqueue('charge', { amount: 10 }, { idempotencyKey: 'order-42' });
    const second = await queue.enqueue('charge', { amount: 10 }, { idempotencyKey: 'order-42' });
    expect(second.id).toBe(first.id);
    expect((await queue.stats())[0]!.pending).toBe(1);
  });

  it('reports per-queue stats', async () => {
    await queue.enqueue('a', {});
    await queue.enqueue('b', {}, { queue: 'emails' });
    await queue.enqueue('c', {}, { queue: 'emails' });
    expect(await queue.stats()).toEqual([
      { queue: 'default', pending: 1, running: 0, completed: 0, dead: 0 },
      { queue: 'emails', pending: 2, running: 0, completed: 0, dead: 0 },
    ]);
  });

  it('only retries dead jobs', async () => {
    const job = await queue.enqueue('a', {});
    expect(await queue.retry(job.id)).toBeNull();
    await pool.query("UPDATE taskforge_jobs SET status = 'dead', attempts = 5 WHERE id = $1", [
      job.id,
    ]);
    expect(await queue.retry(job.id)).toMatchObject({ status: 'pending', attempts: 0 });
  });

  it('upserts cron schedules', async () => {
    await queue.schedule('nightly', '0 0 * * *', 'report');
    await queue.schedule('nightly', '0 1 * * *', 'report');
    const schedules = await queue.schedules();
    expect(schedules).toHaveLength(1);
    expect(schedules[0]!.cron).toBe('0 1 * * *');
  });

  describe('dashboard API', () => {
    it('enqueues, lists and validates input', async () => {
      const app = await createDashboard(queue, { logger: false });
      const created = await app.inject({
        method: 'POST',
        url: '/api/jobs',
        payload: { name: 'resize-image', payload: { id: 7 }, priority: 3 },
      });
      expect(created.statusCode).toBe(201);

      const listed = await app.inject({ url: '/api/jobs?status=pending' });
      expect(listed.json().jobs).toHaveLength(1);

      const invalid = await app.inject({ url: '/api/jobs?status=bogus' });
      expect(invalid.statusCode).toBe(400);

      const html = await app.inject({ url: '/' });
      expect(html.body).toContain('Taskforge');
      await app.close();
    });
  });
});
