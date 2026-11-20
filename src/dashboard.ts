import { fileURLToPath } from 'node:url';
import fastifyStatic from '@fastify/static';
import Fastify, { type FastifyServerOptions } from 'fastify';
import { z } from 'zod';
import type { Queue } from './queue.js';

const PUBLIC_DIR = fileURLToPath(new URL('../public/', import.meta.url));

const listQuery = z.object({
  status: z.enum(['pending', 'running', 'completed', 'dead']).optional(),
  queue: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(500).default(50),
  offset: z.coerce.number().int().min(0).default(0),
});

const enqueueBody = z.object({
  name: z.string().min(1),
  payload: z.unknown().default({}),
  queue: z.string().optional(),
  priority: z.number().int().optional(),
  delayMs: z.number().int().min(0).optional(),
  maxAttempts: z.number().int().min(1).optional(),
  idempotencyKey: z.string().optional(),
});

const idParam = z.object({ id: z.string().regex(/^\d+$/) });

/** REST API + single-page dashboard for monitoring and managing queues. */
export async function createDashboard(
  queue: Queue,
  options: { logger?: FastifyServerOptions['logger'] } = {},
) {
  const app = Fastify({ logger: options.logger ?? true });

  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof z.ZodError) {
      return reply.code(400).send({ error: 'Bad Request', issues: error.issues });
    }
    reply.log.error(error);
    return reply.code(500).send({ error: 'Internal Server Error' });
  });

  await app.register(fastifyStatic, { root: PUBLIC_DIR });

  app.get('/api/stats', async () => ({ queues: await queue.stats() }));

  app.get('/api/jobs', async (request) => ({
    jobs: await queue.list(listQuery.parse(request.query)),
  }));

  app.get('/api/jobs/:id', async (request, reply) => {
    const job = await queue.get(idParam.parse(request.params).id);
    return job ?? reply.code(404).send({ error: 'Not Found' });
  });

  app.post('/api/jobs', async (request, reply) => {
    const { name, payload, ...opts } = enqueueBody.parse(request.body);
    return reply.code(201).send(await queue.enqueue(name, payload, opts));
  });

  app.post('/api/jobs/:id/retry', async (request, reply) => {
    const job = await queue.retry(idParam.parse(request.params).id);
    return job ?? reply.code(409).send({ error: 'Only dead jobs can be retried' });
  });

  app.delete('/api/jobs/:id', async (request, reply) => {
    const removed = await queue.remove(idParam.parse(request.params).id);
    return removed
      ? reply.code(204).send()
      : reply.code(409).send({ error: 'Job not found or running' });
  });

  app.get('/api/schedules', async () => ({ schedules: await queue.schedules() }));

  app.get('/healthz', async () => ({ status: 'ok' }));

  return app;
}
