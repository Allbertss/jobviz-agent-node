/**
 * BeeQueueProvider — monitors queues managed by the `bee-queue` npm package.
 *
 * bee-queue is a simpler, lightweight Redis-backed queue that does not use
 * Redis streams (unlike BullMQ). This provider attaches to existing queues
 * in "watcher" mode (isWorker: false, getEvents: true) so it receives
 * pub/sub notifications without consuming or processing any jobs.
 *
 * Supported events: completed, failed, progress, retrying (mapped to failed).
 * Note: bee-queue does not emit waiting/active/stalled events in watcher mode.
 *
 * Usage:
 *   import { BeeQueueProvider } from "jobviz-agent"
 *
 *   initJobviz({
 *     apiKey,
 *     provider: new BeeQueueProvider({
 *       queues: ["notifications", "sms"],
 *       redisUrl: process.env.REDIS_URL,
 *       env: "production",
 *     }),
 *   })
 */

import type { JobEvent } from '../buffer.js';
import type { QueueProvider } from './types.js';

/** Configuration for the bee-queue provider. */
export interface BeeQueueProviderConfig {
  /** Queue names to monitor. */
  queues: string[];
  /** Full Redis URL, e.g. "redis://localhost:6379" */
  redisUrl: string;
  env?: string;
  /** Whether to capture job input data. Defaults to `true`. */
  captureInput?: boolean;
  /** Whether to capture stack traces on failure. Defaults to `true`. */
  captureStackTraces?: boolean;
}

/** Minimal interface for a bee-queue instance in watcher mode. */
interface BeeQueueInstance {
  on(event: string, listener: (...args: unknown[]) => void): void;
  close(): Promise<void>;
}

export class BeeQueueProvider implements QueueProvider {
  private instances: BeeQueueInstance[] = [];
  private readonly captureStackTraces: boolean;

  constructor(private readonly config: BeeQueueProviderConfig) {
    this.captureStackTraces = config.captureStackTraces ?? true;
  }

  async connect(push: (event: JobEvent) => void): Promise<void> {
    // Dynamic import — bee-queue is an optional peer dependency.
    // Loaded here (not at module top-level) so importing jobviz-agent
    // doesn't crash when bee-queue isn't installed.
    const BeeQueue = (await import('bee-queue')).default ?? (await import('bee-queue'));

    for (const queueName of this.config.queues) {
      const q = new BeeQueue(queueName, {
        // bee-queue passes this to node_redis createClient, which accepts a
        // URL string even though the TypeScript type says ClientOpts.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        redis: this.config.redisUrl as any,
        isWorker: false, // do not pull or process jobs
        getEvents: true, // subscribe to Redis pub/sub job events
        activateDelayedJobs: false,
        removeOnSuccess: false,
        removeOnFailure: false,
      });

      // bee-queue emits these events on the queue instance when getEvents: true
      q.on('job succeeded', (jobId: string, result: unknown) => {
        push(this.make(jobId, queueName, 'completed', { returnValue: result }));
      });

      q.on('job failed', (jobId: string, err: Error) => {
        push(
          this.make(jobId, queueName, 'failed', {
            failedReason: err?.message ?? String(err),
            ...(this.captureStackTraces && err?.stack && { stack: err.stack }),
          }),
        );
      });

      q.on('job retrying', (jobId: string, err: Error) => {
        // bee-queue retrying = job failed but will be retried; surface as failed
        push(
          this.make(jobId, queueName, 'failed', {
            failedReason: `[retrying] ${err?.message ?? String(err)}`,
            ...(this.captureStackTraces && err?.stack && { stack: err.stack }),
          }),
        );
      });

      q.on('job progress', (jobId: string, progress: number) => {
        push(this.make(jobId, queueName, 'progress', { progress }));
      });

      this.instances.push(q);
    }
  }

  async disconnect(): Promise<void> {
    await Promise.all(this.instances.map((q) => q.close()));
    this.instances = [];
  }

  private make(
    jobId: string,
    queue: string,
    event: JobEvent['event'],
    data?: JobEvent['data'],
  ): JobEvent {
    return {
      jobId,
      jobName: queue, // bee-queue has no per-job name; use queue name as fallback
      queue,
      ...(this.config.env && { env: this.config.env }),
      event,
      timestamp: Date.now(),
      ...(data && { data }),
    };
  }
}
