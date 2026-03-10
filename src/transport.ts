import type { JobEvent } from './buffer.js';

export interface TransportResponse {
  accepted: number;
  rejected: number;
  errors?: Array<{ index: number; errors: string[] }>;
}

export interface TransportConfig {
  endpoint: string;
  apiKey: string;
  onError?: (err: Error, dropped: number) => void;
  onResponse?: (body: TransportResponse) => void;
  agentVersion?: string;
  agentMeta?: Record<string, unknown>;
}

const DEFAULT_RETRY_DELAYS_MS = [1_000, 5_000, 15_000];
const MAX_PAYLOAD_BYTES = 5 * 1024 * 1024; // 5 MB

export class HttpTransport {
  constructor(private readonly config: TransportConfig) {}

  /** Fire-and-forget: retries up to 3 times, then drops the batch silently. */
  async send(events: JobEvent[]): Promise<void> {
    const body = JSON.stringify({ events });

    if (body.length > MAX_PAYLOAD_BYTES) {
      this.config.onError?.(
        new Error(
          `Jobviz: payload too large (${(body.length / 1024 / 1024).toFixed(1)} MB, max ${MAX_PAYLOAD_BYTES / 1024 / 1024} MB). Consider reducing batchSize or disabling captureInput.`,
        ),
        events.length,
      );
      return;
    }
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      Authorization: `Bearer ${this.config.apiKey}`,
    };

    if (this.config.agentVersion) {
      headers['X-Jobviz-Agent-Version'] = this.config.agentVersion;
    }
    if (this.config.agentMeta) {
      // Only send lightweight, non-sensitive fields — omit queues/endpoint
      const { queues: _q, endpoint: _e, ...lightMeta } =
        this.config.agentMeta as Record<string, unknown>;
      headers['X-Jobviz-Agent-Meta'] = JSON.stringify(lightMeta);
    }

    for (let attempt = 0; attempt <= DEFAULT_RETRY_DELAYS_MS.length; attempt++) {
      try {
        const res = await fetch(`${this.config.endpoint}/api/v1/events`, {
          method: 'POST',
          headers,
          body,
          signal: AbortSignal.timeout(10_000),
        });

        if (res.ok) {
          if (this.config.onResponse) {
            try {
              const resBody = (await res.json()) as TransportResponse;
              this.config.onResponse(resBody);
            } catch {
              // Response was not JSON — ignore (e.g. empty 204 or non-JSON 200)
            }
          }
          return;
        }

        // 429 = rate limited, retryable — respect Retry-After header if present
        if (res.status === 429) {
          const retryAfter = parseRetryAfter(res.headers.get('Retry-After'));
          const isLastAttempt = attempt === DEFAULT_RETRY_DELAYS_MS.length;
          if (isLastAttempt) {
            this.config.onError?.(
              new Error('Jobviz: 429 Too Many Requests'),
              events.length,
            );
            return;
          }
          await sleep(retryAfter ?? DEFAULT_RETRY_DELAYS_MS[attempt]);
          continue;
        }

        // 4xx (non-429) = permanent failure (bad key, validation), no retry
        if (res.status >= 400 && res.status < 500) {
          let message = `Jobviz: ${res.status} ${res.statusText}`;
          try {
            const resBody = (await res.json()) as { error?: string };
            if (resBody.error) message = `Jobviz: ${resBody.error}`;
          } catch { /* ignore JSON parse errors — use status text fallback */ }
          this.config.onError?.(new Error(message), events.length);
          return;
        }

        throw new Error(`Jobviz: ${res.status} ${res.statusText}`);
      } catch (err) {
        const isLastAttempt = attempt === DEFAULT_RETRY_DELAYS_MS.length;
        if (isLastAttempt) {
          this.config.onError?.(
            err instanceof Error ? err : new Error(String(err)),
            events.length,
          );
          return;
        }
        await sleep(DEFAULT_RETRY_DELAYS_MS[attempt]);
      }
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Parse a Retry-After header value (seconds or HTTP-date) into milliseconds. */
function parseRetryAfter(value: string | null): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (!Number.isNaN(seconds) && seconds > 0) {
    return Math.min(seconds * 1_000, 60_000); // cap at 60s
  }
  // Try HTTP-date format
  const date = Date.parse(value);
  if (!Number.isNaN(date)) {
    const delay = date - Date.now();
    return delay > 0 ? Math.min(delay, 60_000) : undefined;
  }
  return undefined;
}
