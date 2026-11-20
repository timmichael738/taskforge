import pg from 'pg';
import { migrate } from '../src/index.js';

export const DATABASE_URL = process.env.DATABASE_URL;

export async function createTestPool(): Promise<pg.Pool> {
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });
  await migrate(pool);
  return pool;
}

export async function resetDatabase(pool: pg.Pool): Promise<void> {
  await pool.query('TRUNCATE taskforge_jobs, taskforge_schedules RESTART IDENTITY');
}

export async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 20));
  }
}
