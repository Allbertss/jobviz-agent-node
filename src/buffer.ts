/**
 * A normalised job lifecycle event emitted by a queue provider and sent to
 * the Jobviz backend.  All providers map their native events into this shape.
 */
export interface JobEvent {
  /** Unique job identifier within its queue. */
  jobId: string;
  /** Human-readable job name (e.g. `"send-email"`). */
  jobName: string;
  /** Queue the job belongs to. */
  queue: string;
  /** Environment tag (e.g. `"production"`, `"staging"`). */
  env?: string;
  /** Lifecycle event type. */
  event:
    | 'waiting'
    | 'active'
    | 'completed'
    | 'failed'
    | 'delayed'
    | 'stalled'
    | 'progress'
    | 'deployment';
  /** Unix timestamp in milliseconds. */
  timestamp: number;
  /** ID of the parent job, if this job was spawned by a flow. */
  parentJobId?: string;
  /** Queue of the parent job. */
  parentQueue?: string;
  /** Distributed trace ID extracted from job data (`traceId` or `correlationId`). */
  traceId?: string;
  /** Optional event-specific payload. */
  data?: {
    returnValue?: unknown;
    failedReason?: string;
    stack?: string;
    progress?: unknown;
    delay?: number;
    attemptsMade?: number;
    input?: Record<string, unknown>;
    log?: { message: string; meta?: Record<string, unknown> };
    version?: string;
    commitHash?: string | null;
    description?: string | null;
  };
}

/** Callback invoked by {@link EventBuffer} to deliver a batch of events. */
export type FlushCallback = (events: JobEvent[]) => Promise<void>;

const MAX_CHUNK_SIZE = 500;
const DEFAULT_MAX_BUFFER_SIZE = 10_000;

export class EventBuffer {
  private buffer: JobEvent[] = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private flushing = false;
  private readonly maxBufferSize: number;

  constructor(
    private readonly batchSize: number,
    private readonly flushInterval: number,
    private readonly onFlush: FlushCallback,
    maxBufferSize?: number,
  ) {
    if (batchSize < 1) throw new Error('Jobviz: batchSize must be >= 1');
    if (flushInterval < 100)
      throw new Error('Jobviz: flushInterval must be >= 100ms');
    this.maxBufferSize = maxBufferSize ?? DEFAULT_MAX_BUFFER_SIZE;
  }

  push(event: JobEvent): void {
    if (this.buffer.length >= this.maxBufferSize) {
      this.buffer.shift(); // drop oldest event to prevent OOM
    }
    this.buffer.push(event);

    if (this.buffer.length >= this.batchSize) {
      void this.flush();
    }
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.flush();
    }, this.flushInterval);
    this.timer.unref();
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    // Wait for any in-progress flush to finish before draining remaining events
    while (this.flushing) {
      await new Promise((r) => setTimeout(r, 10));
    }
    await this.flush();
  }

  private async flush(): Promise<void> {
    if (this.flushing || this.buffer.length === 0) return;
    this.flushing = true;

    try {
      const batch = this.buffer.splice(0, this.buffer.length);

      // Split oversized batches into chunks to avoid huge HTTP payloads
      for (let i = 0; i < batch.length; i += MAX_CHUNK_SIZE) {
        const chunk = batch.slice(i, i + MAX_CHUNK_SIZE);
        try {
          await this.onFlush(chunk);
        } catch {
          // Transport errors must never surface to the host application.
          // HttpTransport handles its own retries and onError callback.
        }
      }
    } finally {
      this.flushing = false;
    }
  }
}
