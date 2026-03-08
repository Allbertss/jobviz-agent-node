/**
 * AgendaProvider — monitors jobs managed by the Agenda.js npm package.
 *
 * Agenda is a MongoDB-backed job scheduling library for Node.js.
 * This provider listens to Agenda's built-in event emitter for job
 * lifecycle events and pushes normalised JobEvents into the agent.
 *
 * Supported events: active (start), completed (complete), failed (fail).
 * Note: Agenda does not emit waiting/delayed/stalled/progress events.
 *
 * Usage:
 *   import { AgendaProvider } from "jobviz-agent"
 *
 *   initJobviz({
 *     apiKey,
 *     provider: new AgendaProvider({
 *       agenda: myAgendaInstance,
 *       env: "production",
 *     }),
 *   })
 *
 * Or let the provider create the Agenda instance:
 *   initJobviz({
 *     apiKey,
 *     provider: new AgendaProvider({
 *       mongoUrl: "mongodb://localhost:27017/agenda",
 *       env: "production",
 *     }),
 *   })
 */

import type { JobEvent } from '../buffer.js';
import type { QueueProvider } from './types.js';

/** Configuration for the Agenda.js provider. */
export interface AgendaProviderConfig {
  /**
   * Pass an existing Agenda instance. The provider will NOT call agenda.start()
   * or agenda.stop() — you manage the lifecycle yourself.
   */
  agenda?: AgendaInstance;
  /**
   * Alternatively, pass a MongoDB connection string and the provider will
   * create and manage its own Agenda instance.
   */
  mongoUrl?: string;
  /** Optional collection name (default: "agendaJobs"). */
  collection?: string;
  /** Environment tag (e.g. "production", "staging"). */
  env?: string;
  /** Whether to capture job input data. Defaults to `true`. */
  captureInput?: boolean;
  /** Whether to capture stack traces on failure. Defaults to `true`. */
  captureStackTraces?: boolean;
}

/**
 * Minimal Agenda instance shape — avoids requiring `agenda` as a direct
 * dependency. Users must install `agenda` themselves.
 */
interface AgendaInstance {
  on(event: string, listener: (...args: unknown[]) => void): void;
  off?(event: string, listener: (...args: unknown[]) => void): void;
  removeListener?(event: string, listener: (...args: unknown[]) => void): void;
  start(): Promise<void>;
  stop(): Promise<void>;
}

interface AgendaJob {
  attrs: {
    _id?: unknown;
    name: string;
    data?: Record<string, unknown>;
    failCount?: number;
    failReason?: string;
    lastFinishedAt?: Date;
  };
}

export class AgendaProvider implements QueueProvider {
  private agenda: AgendaInstance | null = null;
  private ownsAgenda = false;
  private listeners: Array<{
    event: string;
    fn: (...args: unknown[]) => void;
  }> = [];
  private readonly captureInput: boolean;
  private readonly captureStackTraces: boolean;

  constructor(private readonly config: AgendaProviderConfig) {
    if (!config.agenda && !config.mongoUrl) {
      throw new Error(
        'AgendaProvider requires either an `agenda` instance or a `mongoUrl`.',
      );
    }
    this.captureInput = config.captureInput ?? true;
    this.captureStackTraces = config.captureStackTraces ?? true;
  }

  async connect(push: (event: JobEvent) => void): Promise<void> {
    if (this.config.agenda) {
      this.agenda = this.config.agenda;
      this.ownsAgenda = false;
    } else {
      // Dynamic import — agenda is an optional peer dependency
      // @ts-expect-error — agenda is an optional peer dep, no type declarations at build time
      const mod = await import('agenda');
      const Agenda = (mod as Record<string, unknown>).default ?? mod;
      this.agenda = new (Agenda as {
        new (opts: Record<string, unknown>): AgendaInstance;
      })({
        db: {
          address: this.config.mongoUrl,
          collection: this.config.collection ?? 'agendaJobs',
        },
      });
      this.ownsAgenda = true;
      void this.agenda.start();
    }

    const onStart = (job: AgendaJob) => {
      push(
        this.make(job, 'active', this.captureInput ? { input: job.attrs.data } : undefined),
      );
    };

    const onComplete = (job: AgendaJob) => {
      push(this.make(job, 'completed'));
    };

    const onFail = (err: Error, job: AgendaJob) => {
      push(
        this.make(job, 'failed', {
          failedReason: err?.message ?? String(err),
          ...(this.captureStackTraces && err?.stack && { stack: err.stack }),
          ...(job.attrs.failCount != null && {
            attemptsMade: job.attrs.failCount,
          }),
        }),
      );
    };

    this.agenda.on('start', onStart as (...args: unknown[]) => void);
    this.agenda.on('complete', onComplete as (...args: unknown[]) => void);
    this.agenda.on('fail', onFail as (...args: unknown[]) => void);

    this.listeners = [
      { event: 'start', fn: onStart as (...args: unknown[]) => void },
      { event: 'complete', fn: onComplete as (...args: unknown[]) => void },
      { event: 'fail', fn: onFail as (...args: unknown[]) => void },
    ];
  }

  async disconnect(): Promise<void> {
    if (this.agenda) {
      // Remove our listeners
      for (const { event, fn } of this.listeners) {
        if (this.agenda.off) {
          this.agenda.off(event, fn);
        } else if (this.agenda.removeListener) {
          this.agenda.removeListener(event, fn);
        }
      }
      this.listeners = [];

      // Only stop the instance if we created it
      if (this.ownsAgenda) {
        await this.agenda.stop();
      }
      this.agenda = null;
    }
  }

  private make(
    job: AgendaJob,
    event: JobEvent['event'],
    data?: JobEvent['data'],
  ): JobEvent {
    const jobId = job.attrs._id != null ? String(job.attrs._id) : '';
    const traceId = this.extractTraceId(job);
    return {
      jobId,
      jobName: job.attrs.name,
      queue: 'agenda',
      ...(this.config.env && { env: this.config.env }),
      event,
      timestamp: Date.now(),
      ...(data && { data }),
      ...(traceId && { traceId }),
    };
  }

  private extractTraceId(job: AgendaJob): string | undefined {
    const d = job.attrs.data;
    if (!d || typeof d !== 'object') return undefined;
    const id =
      (d as Record<string, unknown>).traceId ??
      (d as Record<string, unknown>).correlationId;
    return typeof id === 'string' ? id : undefined;
  }
}
