/**
 * End-to-end demo: migrates the database, starts two workers and the dashboard, and keeps
 * producing a realistic mix of jobs, some of which fail and retry.
 *
 *   docker compose up -d postgres && npm run demo  →  http://localhost:3000
 */
import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';
import { createDashboard, migrate, PermanentJobError, Queue, Worker } from '../src/index.js';

const pool = new pg.Pool({
  connectionString:
    process.env.DATABASE_URL ?? 'postgres://taskforge:taskforge@localhost:5432/taskforge',
});
await migrate(pool);

const queue = new Queue(pool);

interface EmailPayload {
  to: string;
  template: string;
}

function createWorker(id: string) {
  return new Worker(pool, { id, concurrency: 4, backoff: { baseMs: 500, maxMs: 5000 } })
    .register<EmailPayload, { messageId: string }>('send-email', async ({ to }, _job, ctx) => {
      await sleep(200 + Math.random() * 800, undefined, { signal: ctx.signal });
      if (to.endsWith('@invalid.test')) throw new PermanentJobError(`Unknown recipient ${to}`);
      if (Math.random() < 0.25) throw new Error('SMTP connection reset'); // transient
      return { messageId: crypto.randomUUID() };
    })
    .register('generate-report', async (_payload, _job, ctx) => {
      for (let step = 1; step <= 5; step++) {
        await sleep(300, undefined, { signal: ctx.signal });
        ctx.log('report progress', { step });
      }
      return { rows: Math.floor(Math.random() * 10_000) };
    })
    .register('cleanup', async () => ({ purged: await queue.purgeCompleted(10 * 60_000) }));
}

const workers = [createWorker('worker-a'), createWorker('worker-b')];
for (const worker of workers) {
  worker.on('dead', (job, error) =>
    console.warn(`☠️  job ${job.id} (${job.name}) dead: ${error.message}`),
  );
  worker.on('error', (error) => console.error('worker error', error));
  await worker.start();
}

await queue.schedule('hourly-cleanup', '*/1 * * * *', 'cleanup');

const dashboard = await createDashboard(queue, { logger: false });
await dashboard.listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' });
console.log('Dashboard: http://localhost:3000');

const producer = setInterval(() => {
  const n = Math.floor(Math.random() * 1000);
  void queue.enqueue<EmailPayload>('send-email', {
    to: n % 10 === 0 ? `user${n}@invalid.test` : `user${n}@example.com`,
    template: 'welcome',
  });
  if (n % 4 === 0) void queue.enqueue('generate-report', { month: '2026-11' }, { priority: 10 });
}, 400);

const shutdown = async () => {
  clearInterval(producer);
  await Promise.all(workers.map((w) => w.stop()));
  await dashboard.close();
  await pool.end();
  process.exit(0);
};
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
