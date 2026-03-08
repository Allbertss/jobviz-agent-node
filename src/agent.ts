import { randomUUID } from 'node:crypto';
import { EventBuffer } from './buffer.js';
import { DebugLogger } from './debug-logger.js';
import { DebugServer } from './debug-server.js';
import { BullMQProvider } from './providers/bullmq.js';
import type { QueueProvider } from './providers/types.js';
import { HttpTransport } from './transport.js';
import { AGENT_VERSION } from './version.js';

function defaultDevLogger(err: Error, droppedCount: number): void {
  console.error(`[jobviz] dropped ${droppedCount} events: ${err.message}`);
}

/**
 * Maximum byte length of a serialised `input` object before redaction is
 * attempted.  Objects larger than this are replaced wholesale with a
 * `"[INPUT_TOO_LARGE]"` placeholder to avoid CPU stalls during recursive
 * traversal.  Value: 1 MB.
 */
const MAX_INPUT_BYTES_FOR_REDACTION = 1_024 * 1_024;

/** Default keys that are always redacted when `redactKeys` is enabled. */
const DEFAULT_REDACT_KEYS = new Set([
  'password',
  'secret',
  'token',
  'apikey',
  'api_key',
  'authorization',
  'creditcard',
  'credit_card',
  'ssn',
  'accesstoken',
  'access_token',
  'refreshtoken',
  'refresh_token',
]);

/**
 * Fine-grained configuration for sensitive-key redaction.
 *
 * Use this instead of a plain `boolean` or `string[]` when you need to add,
 * remove, or completely replace the built-in redaction key list.
 */
export interface RedactKeysConfig {
  /** Additional keys to redact (merged with built-in defaults). */
  include?: string[];
  /** Keys to remove from the built-in default set. */
  exclude?: string[];
  /** Use ONLY these keys — ignores the built-in defaults entirely. */
  only?: string[];
}

interface JobvizBaseConfig {
  apiKey: string;
  endpoint?: string;
  batchSize?: number;
  flushInterval?: number;
  /**
   * dev (default) — logs errors to stderr, never throws.
   * production    — silent unless onError is provided.
   */
  env?: 'development' | 'production';
  /** Called when events are dropped after retries. Never throws. */
  onError?: (err: Error, droppedCount: number) => void;
  /** Enable diagnostic mode: verbose logging + local health endpoint. */
  debug?: boolean;
  /** Port for the local /agent/health endpoint (default 9888). Only used when debug is true. */
  debugPort?: number;
  /**
   * Whether to capture job input data (job.data) in events.
   * Defaults to `true`.  Set to `false` to never capture input payloads.
   */
  captureInput?: boolean;
  /**
   * Whether to capture error stack traces in failed events.
   * Defaults to `true`.  Set to `false` to omit stack traces.
   */
  captureStackTraces?: boolean;
  /**
   * Keys to redact from captured job input data.
   *
   * - `true`  — redact a built-in set of sensitive keys (password, token, secret, etc.)
   * - `string[]` — redact these specific keys (merged with the built-in set)
   * - `false` / omitted — no redaction (default)
   * - `{ include?, exclude?, only? }` — fine-grained control:
   *   - `include` — additional keys to redact (merged with built-in set)
   *   - `exclude` — keys to remove from the built-in set
   *   - `only` — use ONLY these keys, ignoring the built-in set entirely
   *
   * Redacted values are replaced with `"[REDACTED]"`.
   */
  redactKeys?: boolean | string[] | RedactKeysConfig;
  /**
   * Maximum number of events to buffer in memory before dropping the oldest.
   * Prevents unbounded memory growth if the backend is unreachable.
   * Defaults to `10000`.
   */
  maxBufferSize?: number;
}

/** Auto-creates a BullMQProvider from Redis connection details (backward-compatible). */
interface JobvizBullMQConfig extends JobvizBaseConfig {
  queues: string[];
  redisUrl: string;
  provider?: never;
}

/**
 * Bring-your-own-provider mode.  Implement QueueProvider and pass it here.
 * `queues` and `redisUrl` are not needed in this mode.
 *
 * Example:
 *   initJobviz({
 *     apiKey,
 *     provider: new BeeQueueProvider({ queues: ["notifications"], redisUrl }),
 *   })
 */
interface JobvizCustomConfig extends JobvizBaseConfig {
  provider: QueueProvider;
  queues?: never;
  redisUrl?: never;
}

/**
 * Configuration accepted by {@link JobvizAgent} and {@link initJobviz}.
 *
 * Either pass `queues` + `redisUrl` for the built-in BullMQ shorthand, or
 * pass a `provider` instance for bee-queue, Agenda, or a custom provider.
 */
export type JobvizConfig = JobvizBullMQConfig | JobvizCustomConfig;

export class JobvizAgent {
  private buffer: EventBuffer;
  private provider: QueueProvider;
  private transport: HttpTransport;
  private debugLogger: DebugLogger | null = null;
  private debugServer: DebugServer | null = null;
  private sanitizedConfig: Record<string, unknown>;
  private redactSet: Set<string> | null = null;

  /** Resolved config flags exposed to providers. */
  readonly captureInput: boolean;
  readonly captureStackTraces: boolean;

  constructor(config: JobvizConfig) {
    // --- Validate required fields ---
    if (!config.apiKey || typeof config.apiKey !== 'string') {
      throw new Error(
        'Jobviz: apiKey is required and must be a non-empty string',
      );
    }
    if (config.apiKey.length < 10) {
      throw new Error(
        'Jobviz: apiKey looks invalid (too short). Expected a Jobviz API key.',
      );
    }

    const batchSize = config.batchSize ?? 100;
    const flushInterval = config.flushInterval ?? 3000;

    if (typeof batchSize !== 'number' || batchSize < 1) {
      throw new Error('Jobviz: batchSize must be a number >= 1');
    }
    if (typeof flushInterval !== 'number' || flushInterval < 100) {
      throw new Error('Jobviz: flushInterval must be a number >= 100ms');
    }

    const isDev =
      (config.env ?? process.env.NODE_ENV ?? 'development') !== 'production';

    this.captureInput = config.captureInput ?? true;
    this.captureStackTraces = config.captureStackTraces ?? true;

    // Build redact key set
    this.redactSet = buildRedactSet(config.redactKeys);

    // Warn in dev mode if capturing input without redaction
    if (isDev && this.captureInput && !this.redactSet) {
      console.warn(
        '[jobviz] Warning: captureInput is enabled but redactKeys is off. ' +
          'Job payloads may contain sensitive data. Set redactKeys: true to enable automatic redaction.',
      );
    }

    // Build sanitized config (no apiKey or redisUrl — avoids leaking credentials)
    this.sanitizedConfig = {
      endpoint: config.endpoint ?? 'https://app.jobviz.dev',
      batchSize,
      flushInterval,
      env: config.env ?? (isDev ? 'development' : 'production'),
      debug: config.debug ?? false,
      captureInput: this.captureInput,
      captureStackTraces: this.captureStackTraces,
      redactKeys: !!config.redactKeys,
      queues: 'queues' in config && config.queues ? config.queues : undefined,
      provider:
        'provider' in config && config.provider
          ? config.provider.constructor.name
          : 'BullMQProvider',
    };

    // Set up debug logger if enabled
    if (config.debug) {
      this.debugLogger = new DebugLogger();
    }

    // Wrap onError to also log via debug logger
    const baseOnError =
      config.onError ?? (isDev ? defaultDevLogger : undefined);
    const onError = this.debugLogger
      ? (err: Error, dropped: number) => {
          this.debugLogger!.logDrop(err, dropped);
          baseOnError?.(err, dropped);
        }
      : baseOnError;

    this.transport = new HttpTransport({
      endpoint: config.endpoint ?? 'https://app.jobviz.dev',
      apiKey: config.apiKey,
      onError,
      agentVersion: AGENT_VERSION,
      agentMeta: this.sanitizedConfig,
    });

    const debugLogger = this.debugLogger;
    this.buffer = new EventBuffer(
      batchSize,
      flushInterval,
      async (events) => {
        await this.transport.send(events);
        debugLogger?.logFlush(events.length);
      },
      config.maxBufferSize,
    );

    if (config.provider) {
      this.provider = config.provider;
    } else {
      const envName =
        config.env ??
        (process.env.NODE_ENV === 'production' ? 'production' : 'development');
      this.provider = new BullMQProvider({
        queues: config.queues,
        connection: { url: config.redisUrl },
        env: envName,
        captureInput: this.captureInput,
        captureStackTraces: this.captureStackTraces,
      });
    }

    // Set up debug server if enabled
    if (config.debug) {
      this.debugServer = new DebugServer(
        config.debugPort ?? 9888,
        () => this.sanitizedConfig,
        () => this.debugLogger!.getStats(),
      );
    }
  }

  async start(): Promise<void> {
    const debugLogger = this.debugLogger;
    await this.provider.connect((event) => {
      // Apply redaction to input data if configured
      if (this.redactSet && event.data?.input) {
        // Guard against extremely large input objects that would stall the CPU
        // during recursive redaction.  Replace them with a placeholder instead.
        const raw = JSON.stringify(event.data.input);
        if (raw.length > MAX_INPUT_BYTES_FOR_REDACTION) {
          event.data.input = { _notice: '[INPUT_TOO_LARGE]' };
        } else {
          event.data.input = this.redactObject(event.data.input);
        }
      }
      debugLogger?.logEvent(event);
      this.buffer.push(event);
    });
    this.buffer.start();
    this.debugServer?.start();
  }

  /**
   * Attach a structured log entry to a running job.
   * Sends a "progress" event with the log message in the data payload.
   */
  log(
    job: { id?: string; name?: string; queueName?: string },
    message: string,
    meta?: Record<string, unknown>,
  ): void {
    this.buffer.push({
      jobId: job.id ?? '',
      jobName: job.name ?? '',
      queue: job.queueName ?? '',
      event: 'progress',
      timestamp: Date.now(),
      data: {
        log: {
          message,
          ...(meta && Object.keys(meta).length > 0 ? { meta } : {}),
        },
      },
    });
  }

  /**
   * Record a deployment event for correlation with job failures.
   * Usage: agent.trackDeployment({ version: '1.2.3', commitHash: 'abc123' })
   */
  trackDeployment(info: {
    version: string;
    commitHash?: string;
    description?: string;
  }): void {
    this.buffer.push({
      jobId: `deploy-${randomUUID()}`,
      jobName: 'deployment',
      queue: '__system',
      event: 'deployment',
      timestamp: Date.now(),
      data: {
        version: info.version,
        commitHash: info.commitHash ?? null,
        description: info.description ?? null,
      },
    });
  }

  async stop(): Promise<void> {
    await this.debugServer?.stop();
    await this.buffer.stop();
    await this.provider.disconnect();
  }

  private static readonly MAX_REDACT_DEPTH = 20;

  /** Recursively redact sensitive keys from a plain object. */
  private redactObject(
    obj: Record<string, unknown>,
    depth = 0,
  ): Record<string, unknown> {
    if (depth >= JobvizAgent.MAX_REDACT_DEPTH) return obj;

    const result: Record<string, unknown> = {};
    for (const key of Object.keys(obj)) {
      if (!Object.hasOwn(obj, key)) continue;
      if (key === '__proto__' || key === 'constructor' || key === 'prototype') {
        continue;
      }
      const value = obj[key];
      if (this.redactSet!.has(key.toLowerCase())) {
        result[key] = '[REDACTED]';
      } else if (Array.isArray(value)) {
        result[key] = this.redactArray(value, depth + 1);
      } else if (value && typeof value === 'object') {
        result[key] = this.redactObject(
          value as Record<string, unknown>,
          depth + 1,
        );
      } else {
        result[key] = value;
      }
    }
    return result;
  }

  /** Recursively redact sensitive keys inside arrays (including nested arrays). */
  private redactArray(arr: unknown[], depth: number): unknown[] {
    if (depth >= JobvizAgent.MAX_REDACT_DEPTH) return arr;

    return arr.map((item) => {
      if (Array.isArray(item)) return this.redactArray(item, depth + 1);
      if (item && typeof item === 'object') {
        return this.redactObject(item as Record<string, unknown>, depth + 1);
      }
      return item;
    });
  }
}

/** Build the redact key set from the config value. */
function buildRedactSet(
  redactKeys: boolean | string[] | RedactKeysConfig | undefined,
): Set<string> | null {
  if (!redactKeys) return null;

  if (redactKeys === true) {
    return new Set(DEFAULT_REDACT_KEYS);
  }

  if (Array.isArray(redactKeys)) {
    return new Set([
      ...DEFAULT_REDACT_KEYS,
      ...redactKeys.map((k) => k.toLowerCase()),
    ]);
  }

  // Object config: { include?, exclude?, only? }
  if (redactKeys.only) {
    return new Set(redactKeys.only.map((k) => k.toLowerCase()));
  }

  const set = new Set(DEFAULT_REDACT_KEYS);
  if (redactKeys.include) {
    for (const k of redactKeys.include) {
      set.add(k.toLowerCase());
    }
  }
  if (redactKeys.exclude) {
    for (const k of redactKeys.exclude) {
      set.delete(k.toLowerCase());
    }
  }
  return set;
}
