import { JobvizAgent } from './agent.js';
import type { JobvizConfig } from './agent.js';

export { JobvizAgent };
export type { JobvizConfig };
export type { RedactKeysConfig } from './agent.js';
export type { JobEvent } from './buffer.js';

// Queue provider interface + built-in implementations
export type { QueueProvider } from './providers/types.js';
export { BullMQProvider } from './providers/bullmq.js';
export type { BullMQProviderConfig } from './providers/bullmq.js';
export { BeeQueueProvider } from './providers/bee-queue.js';
export type { BeeQueueProviderConfig } from './providers/bee-queue.js';
export { AgendaProvider } from './providers/agenda.js';
export type { AgendaProviderConfig } from './providers/agenda.js';
export { MultiProvider } from './providers/multi.js';

// ── Convenience singleton ──────────────────────────────────────────────
// For simple setups where a single global agent is sufficient.
// For multi-instance use (e.g. tests, multi-tenant), use JobvizAgent directly.

let agent: JobvizAgent | null = null;

export async function initJobviz(config: JobvizConfig): Promise<JobvizAgent> {
  if (agent) {
    throw new Error(
      'Jobviz agent already initialized. Call stopJobviz() first, or use new JobvizAgent() for multiple instances.',
    );
  }
  const instance = new JobvizAgent(config);
  await instance.start();
  agent = instance; // only set after successful start
  return agent;
}

export async function stopJobviz(): Promise<void> {
  if (!agent) return;
  await agent.stop();
  agent = null;
}

/**
 * Namespace for helper functions that work with the active agent.
 *
 * Usage inside a BullMQ worker:
 *   import { jobviz } from 'jobviz-agent'
 *
 *   const worker = new Worker('emails', async (job) => {
 *     jobviz.log(job, 'Fetching template')
 *     const tpl = await fetchTemplate(job.data.templateId)
 *     jobviz.log(job, 'Sending email', { recipients: 3 })
 *     await send(tpl, job.data)
 *   })
 */
export const jobviz = {
  /**
   * Attach a structured log entry to a running job.
   * No-op if the agent hasn't been initialized yet.
   */
  log(
    job: { id?: string; name?: string; queueName?: string },
    message: string,
    meta?: Record<string, unknown>,
  ): void {
    agent?.log(job, message, meta);
  },
};
