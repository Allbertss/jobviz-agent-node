import { Job, JobProgress, Queue, QueueEvents } from 'bullmq';
import type { ConnectionOptions } from 'bullmq';
import type { JobEvent } from '../buffer.js';
import type { QueueProvider } from './types.js';

/** Configuration for the built-in BullMQ provider. */
export interface BullMQProviderConfig {
  /** Queue names to monitor. */
  queues: string[];
  /** BullMQ/ioredis connection options (e.g. `{ url: "redis://..." }`). */
  connection: ConnectionOptions;
  /** Environment tag (e.g. `"production"`, `"staging"`). */
  env?: string;
  /** Whether to capture job input data. Defaults to `true`. */
  captureInput?: boolean;
  /** Whether to capture stack traces on failure. Defaults to `true`. */
  captureStackTraces?: boolean;
}

interface CachedJob {
  name: string;
  traceId?: string;
  parent?: { parentJobId: string; parentQueue: string };
  input?: Record<string, unknown>;
  cachedAt: number;
}

const CACHE_TTL_MS = 30 * 60 * 1_000; // 30 minutes
const CACHE_SWEEP_INTERVAL_MS = 60 * 1_000; // sweep every 60s
const MAX_CACHE_SIZE = 50_000; // cap to prevent unbounded memory growth

export class BullMQProvider implements QueueProvider {
  private listeners: QueueEvents[] = [];
  private queues: Queue[] = [];
  private jobCache = new Map<string, CachedJob>();
  private push: (event: JobEvent) => void = () => {};
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private readonly captureInput: boolean;
  private readonly captureStackTraces: boolean;

  constructor(private readonly config: BullMQProviderConfig) {
    this.captureInput = config.captureInput ?? true;
    this.captureStackTraces = config.captureStackTraces ?? true;
  }

  connect(push: (event: JobEvent) => void): void | Promise<void> {
    this.push = push;

    for (const queueName of this.config.queues) {
      const queueEvents = new QueueEvents(queueName, {
        connection: this.config.connection,
      });
      const queue = new Queue(queueName, {
        connection: this.config.connection,
      });

      this.attachListeners(queueEvents, queue, queueName);
      this.listeners.push(queueEvents);
      this.queues.push(queue);
    }

    // Periodically evict stale cache entries (stuck/orphaned jobs)
    this.sweepTimer = setInterval(
      () => this.sweepCache(),
      CACHE_SWEEP_INTERVAL_MS,
    );
    this.sweepTimer.unref();
  }

  async disconnect(): Promise<void> {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    await Promise.all([
      ...this.listeners.map((qe) => qe.close()),
      ...this.queues.map((q) => q.close()),
    ]);
    this.listeners = [];
    this.queues = [];
    this.jobCache.clear();
  }

  private cacheSet(jobId: string, entry: CachedJob): void {
    // Evict oldest entries if cache is at capacity
    if (this.jobCache.size >= MAX_CACHE_SIZE) {
      const first = this.jobCache.keys().next().value;
      if (first !== undefined) this.jobCache.delete(first);
    }
    this.jobCache.set(jobId, entry);
  }

  private sweepCache(): void {
    const cutoff = Date.now() - CACHE_TTL_MS;
    for (const [jobId, entry] of this.jobCache) {
      if (entry.cachedAt < cutoff) {
        this.jobCache.delete(jobId);
      }
    }
  }

  private extractTraceId(job: Job | undefined): string | undefined {
    const data = job?.data;
    if (!data || typeof data !== 'object') return undefined;
    const id = data.traceId ?? data.correlationId;
    return typeof id === 'string' ? id : undefined;
  }

  private getInput(
    job: Job | undefined,
  ): Record<string, unknown> | undefined {
    if (!this.captureInput) return undefined;
    return job?.data && Object.keys(job.data).length > 0
      ? job.data
      : undefined;
  }

  private attachListeners(
    queueEvents: QueueEvents,
    queue: Queue,
    queueName: string,
  ): void {
    // waiting: fetch job metadata and cache everything we need for later events
    queueEvents.on('waiting', ({ jobId }: { jobId: string }) => {
      void Job.fromId(queue, jobId)
        .then((job: Job | undefined) => {
          const name = job?.name || 'unknown';
          const traceId = this.extractTraceId(job);
          const parent = this.extractParent(job);
          const input = this.getInput(job);

          this.cacheSet(jobId, {
            name,
            traceId,
            parent,
            input,
            cachedAt: Date.now(),
          });

          this.push(
            this.makeEvent(
              jobId,
              name,
              queueName,
              'waiting',
              undefined,
              parent,
              traceId,
            ),
          );
        })
        .catch(() => {
          // Redis lookup failed — emit a minimal event so the job isn't invisible
          this.push(
            this.makeEvent(jobId, 'unknown', queueName, 'waiting'),
          );
        });
    });

    // active: use cache from waiting instead of calling Job.fromId() again
    queueEvents.on('active', ({ jobId }: { jobId: string }) => {
      const cached = this.jobCache.get(jobId);

      if (cached) {
        // Cache hit — skip Redis call entirely
        this.push(
          this.makeEvent(
            jobId,
            cached.name,
            queueName,
            'active',
            cached.input ? { input: cached.input } : undefined,
            cached.parent,
            cached.traceId,
          ),
        );
      } else {
        // Cache miss (e.g. agent started after job was already waiting) — fallback to Redis
        void Job.fromId(queue, jobId)
          .then((job: Job | undefined) => {
            const name = job?.name || 'unknown';
            const traceId = this.extractTraceId(job);
            const parent = this.extractParent(job);
            const input = this.getInput(job);

            this.cacheSet(jobId, {
              name,
              traceId,
              parent,
              input,
              cachedAt: Date.now(),
            });

            this.push(
              this.makeEvent(
                jobId,
                name,
                queueName,
                'active',
                input ? { input } : undefined,
                parent,
                traceId,
              ),
            );
          })
          .catch(() => {
            this.push(
              this.makeEvent(jobId, 'unknown', queueName, 'active'),
            );
          });
      }
    });

    queueEvents.on(
      'completed',
      ({ jobId, returnvalue }: { jobId: string; returnvalue: string }) => {
        const cached = this.jobCache.get(jobId);
        this.push(
          this.makeEvent(
            jobId,
            cached?.name || 'unknown',
            queueName,
            'completed',
            {
              returnValue: returnvalue,
            },
            undefined,
            cached?.traceId,
          ),
        );
        this.jobCache.delete(jobId);
      },
    );

    queueEvents.on(
      'failed',
      ({ jobId, failedReason }: { jobId: string; failedReason: string }) => {
        if (this.captureStackTraces) {
          void Job.fromId(queue, jobId)
            .then((job: Job | undefined) => {
              const cached = this.jobCache.get(jobId);
              const stack = job?.stacktrace?.length
                ? job.stacktrace.join('\n')
                : undefined;
              this.push(
                this.makeEvent(
                  jobId,
                  cached?.name || 'unknown',
                  queueName,
                  'failed',
                  {
                    failedReason,
                    ...(stack && { stack }),
                  },
                  undefined,
                  cached?.traceId,
                ),
              );
              this.jobCache.delete(jobId);
            })
            .catch(() => {
              const cached = this.jobCache.get(jobId);
              this.push(
                this.makeEvent(
                  jobId,
                  cached?.name || 'unknown',
                  queueName,
                  'failed',
                  { failedReason },
                  undefined,
                  cached?.traceId,
                ),
              );
              this.jobCache.delete(jobId);
            });
        } else {
          // Stack traces disabled — skip the Redis lookup entirely
          const cached = this.jobCache.get(jobId);
          this.push(
            this.makeEvent(
              jobId,
              cached?.name || 'unknown',
              queueName,
              'failed',
              { failedReason },
              undefined,
              cached?.traceId,
            ),
          );
          this.jobCache.delete(jobId);
        }
      },
    );

    queueEvents.on(
      'delayed',
      ({ jobId, delay }: { jobId: string; delay: number }) => {
        const cached = this.jobCache.get(jobId);
        this.push(
          this.makeEvent(
            jobId,
            cached?.name || 'unknown',
            queueName,
            'delayed',
            { delay },
            undefined,
            cached?.traceId,
          ),
        );
      },
    );

    queueEvents.on('stalled', ({ jobId }: { jobId: string }) => {
      const cached = this.jobCache.get(jobId);
      this.push(
        this.makeEvent(
          jobId,
          cached?.name || 'unknown',
          queueName,
          'stalled',
          undefined,
          undefined,
          cached?.traceId,
        ),
      );
    });

    queueEvents.on(
      'progress',
      ({ jobId, data }: { jobId: string; data: JobProgress }) => {
        const cached = this.jobCache.get(jobId);
        this.push(
          this.makeEvent(
            jobId,
            cached?.name || 'unknown',
            queueName,
            'progress',
            {
              progress: data,
            },
            undefined,
            cached?.traceId,
          ),
        );
      },
    );
  }

  private extractParent(
    job: Job | undefined,
  ): { parentJobId: string; parentQueue: string } | undefined {
    const parentId = job?.parent?.id;
    if (!parentId) return undefined;
    // job.parent.queueKey is "bull:{queueName}:{prefix}" — extract queue name
    const queueKey = job?.parent?.queueKey ?? '';
    const parts = queueKey.split(':');
    const parentQueue = parts.length >= 2 ? parts[1] : queueKey;
    return { parentJobId: parentId, parentQueue };
  }

  private makeEvent(
    jobId: string,
    jobName: string,
    queue: string,
    event: JobEvent['event'],
    data?: JobEvent['data'],
    parent?: { parentJobId: string; parentQueue: string },
    traceId?: string,
  ): JobEvent {
    return {
      jobId,
      jobName,
      queue,
      ...(this.config.env && { env: this.config.env }),
      event,
      timestamp: Date.now(),
      ...(data && { data }),
      ...(parent && {
        parentJobId: parent.parentJobId,
        parentQueue: parent.parentQueue,
      }),
      ...(traceId && { traceId }),
    };
  }
}
