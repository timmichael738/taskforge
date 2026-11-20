export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
}

export const DEFAULT_BACKOFF: BackoffOptions = { baseMs: 1_000, maxMs: 10 * 60_000 };

/**
 * Exponential backoff with "full jitter" (AWS Architecture Blog): a uniformly random delay
 * in [0, min(max, base * 2^attempt)]. Jitter spreads retries out so a burst of failures
 * doesn't turn into a synchronised retry storm against a struggling dependency.
 */
export function retryDelayMs(
  attempt: number,
  { baseMs, maxMs }: BackoffOptions = DEFAULT_BACKOFF,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(random() * ceiling);
}
