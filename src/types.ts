export type JobStatus = 'pending' | 'running' | 'completed' | 'dead';

export interface Job<T = unknown> {
  id: string;
  queue: string;
  name: string;
  payload: T;
  status: JobStatus;
  priority: number;
  attempts: number;
  maxAttempts: number;
  runAt: Date;
  lockedAt: Date | null;
  lockedBy: string | null;
  lastError: string | null;
  result: unknown;
  idempotencyKey: string | null;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
}

export interface EnqueueOptions {
  queue?: string;
  /** Higher runs first. Default 0. */
  priority?: number;
  /** Delay before the job becomes eligible to run. */
  delayMs?: number;
  /** Absolute time at which the job becomes eligible (overrides delayMs). */
  runAt?: Date;
  /** Total attempts including the first one. Default 5. */
  maxAttempts?: number;
  /** Enqueueing again with the same key (per queue) returns the existing job. */
  idempotencyKey?: string;
}

export interface JobContext {
  /** Aborted when the job exceeds its timeout or the worker is shutting down. */
  signal: AbortSignal;
  /** Structured logger scoped to the job. */
  log: (message: string, extra?: Record<string, unknown>) => void;
}

export type JobHandler<T = unknown, R = unknown> = (
  payload: T,
  job: Job<T>,
  ctx: JobContext,
) => Promise<R> | R;

/** Throw this from a handler to skip remaining retries and dead-letter the job immediately. */
export class PermanentJobError extends Error {
  override name = 'PermanentJobError';
}

interface JobRow {
  id: string;
  queue: string;
  name: string;
  payload: unknown;
  status: JobStatus;
  priority: number;
  attempts: number;
  max_attempts: number;
  run_at: Date;
  locked_at: Date | null;
  locked_by: string | null;
  last_error: string | null;
  result: unknown;
  idempotency_key: string | null;
  created_at: Date;
  updated_at: Date;
  finished_at: Date | null;
}

export function toJob<T>(row: JobRow): Job<T> {
  return {
    id: String(row.id),
    queue: row.queue,
    name: row.name,
    payload: row.payload as T,
    status: row.status,
    priority: row.priority,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    runAt: row.run_at,
    lockedAt: row.locked_at,
    lockedBy: row.locked_by,
    lastError: row.last_error,
    result: row.result,
    idempotencyKey: row.idempotency_key,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    finishedAt: row.finished_at,
  };
}

export type { JobRow };
