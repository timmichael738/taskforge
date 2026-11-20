import { describe, expect, it } from 'vitest';
import { retryDelayMs } from '../src/backoff.js';
import { nextCronRun } from '../src/queue.js';

describe('retryDelayMs', () => {
  const opts = { baseMs: 100, maxMs: 1000 };

  it('doubles the ceiling with each attempt', () => {
    const max = () => 0.999999;
    expect(retryDelayMs(1, opts, max)).toBe(99);
    expect(retryDelayMs(2, opts, max)).toBe(199);
    expect(retryDelayMs(3, opts, max)).toBe(399);
  });

  it('never exceeds maxMs', () => {
    expect(retryDelayMs(20, opts, () => 0.999999)).toBeLessThan(1000);
  });

  it('applies full jitter (can be anywhere from 0 to the ceiling)', () => {
    expect(retryDelayMs(5, opts, () => 0)).toBe(0);
    const samples = Array.from({ length: 200 }, () => retryDelayMs(4, opts));
    expect(new Set(samples).size).toBeGreaterThan(50);
  });
});

describe('nextCronRun', () => {
  it('computes the next run in UTC', () => {
    const next = nextCronRun('0 9 * * 1', new Date('2026-11-20T12:00:00Z')); // Friday
    expect(next.toISOString()).toBe('2026-11-23T09:00:00.000Z'); // next Monday
  });
});
