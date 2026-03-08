import type { JobEvent } from '../buffer.js';
import type { QueueProvider } from './types.js';

/**
 * MultiProvider — fans out to multiple queue providers simultaneously.
 *
 * Use this when your application uses more than one queue system
 * (e.g. BullMQ for background jobs + bee-queue for notifications).
 *
 * Example:
 *   import { MultiProvider, BullMQProvider, BeeQueueProvider } from "jobviz-agent"
 *
 *   initJobviz({
 *     apiKey,
 *     provider: new MultiProvider([
 *       new BullMQProvider({ queues: ["emails", "reports"], connection }),
 *       new BeeQueueProvider({ queues: ["notifications"], redisUrl }),
 *     ]),
 *   })
 */
export class MultiProvider implements QueueProvider {
  constructor(private readonly providers: QueueProvider[]) {}

  async connect(push: (event: JobEvent) => void): Promise<void> {
    await Promise.all(this.providers.map((p) => p.connect(push)));
  }

  async disconnect(): Promise<void> {
    await Promise.all(this.providers.map((p) => p.disconnect()));
  }
}
