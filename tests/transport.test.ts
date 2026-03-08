import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { HttpTransport } from '../src/transport.js';
import type { JobEvent } from '../src/buffer.js';

function makeEvent(overrides: Partial<JobEvent> = {}): JobEvent {
  return {
    jobId: '1',
    jobName: 'test-job',
    queue: 'test-queue',
    event: 'completed',
    timestamp: Date.now(),
    ...overrides,
  };
}

describe('HttpTransport', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchSpy = vi.spyOn(globalThis, 'fetch');
  });

  afterEach(() => {
    vi.useRealTimers();
    fetchSpy.mockRestore();
  });

  it('sends events with correct headers', async () => {
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'jv_test_key',
      agentVersion: '0.1.0',
    });

    await transport.send([makeEvent()]);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, opts] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('https://test.jobviz.dev/api/v1/events');
    expect((opts.headers as Record<string, string>)['Authorization']).toBe(
      'Bearer jv_test_key',
    );
    expect((opts.headers as Record<string, string>)['Content-Type']).toBe(
      'application/json',
    );
    expect(
      (opts.headers as Record<string, string>)['X-Jobviz-Agent-Version'],
    ).toBe('0.1.0');
  });

  it('sends agent meta header when provided', async () => {
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const meta = { env: 'test' };
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'jv_test_key',
      agentMeta: meta,
    });

    await transport.send([makeEvent()]);

    const [, opts] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(
      (opts.headers as Record<string, string>)['X-Jobviz-Agent-Meta'],
    ).toBe(JSON.stringify(meta));
  });

  it('does not retry on 4xx errors (non-429)', async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response(JSON.stringify({ error: 'Invalid API key' }), {
        status: 401,
      }),
    );

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'bad_key',
      onError,
    });

    await transport.send([makeEvent()]);

    expect(fetchSpy).toHaveBeenCalledTimes(1); // no retries
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0].message).toContain('Invalid API key');
    expect(onError.mock.calls[0][1]).toBe(1); // dropped count
  });

  it('retries on 429 Too Many Requests', async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response('Rate Limited', {
        status: 429,
        statusText: 'Too Many Requests',
      }),
    );
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
    });

    const sendPromise = transport.send([makeEvent()]);
    await vi.advanceTimersByTimeAsync(1_000); // default retry delay
    await sendPromise;

    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it('respects Retry-After header on 429', async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response('Rate Limited', {
        status: 429,
        headers: { 'Retry-After': '3' },
      }),
    );
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
    });

    const sendPromise = transport.send([makeEvent()]);
    // Should wait 3 seconds (from Retry-After), not 1 second
    await vi.advanceTimersByTimeAsync(3_000);
    await sendPromise;

    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it('drops events after max retries on persistent 429', async () => {
    for (let i = 0; i < 4; i++) {
      fetchSpy.mockResolvedValueOnce(
        new Response('Rate Limited', {
          status: 429,
          statusText: 'Too Many Requests',
        }),
      );
    }

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
      onError,
    });

    const sendPromise = transport.send([makeEvent(), makeEvent()]);
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(5_000);
    await vi.advanceTimersByTimeAsync(15_000);
    await sendPromise;

    expect(fetchSpy).toHaveBeenCalledTimes(4);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0].message).toContain('429');
    expect(onError.mock.calls[0][1]).toBe(2);
  });

  it('handles 4xx with non-JSON body', async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response('Bad Request', { status: 400, statusText: 'Bad Request' }),
    );

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
      onError,
    });

    await transport.send([makeEvent()]);

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0].message).toContain('400');
  });

  it('retries on 5xx errors and gives up after max attempts', async () => {
    // 4 total attempts: initial + 3 retries
    for (let i = 0; i < 4; i++) {
      fetchSpy.mockResolvedValueOnce(
        new Response('Internal Server Error', { status: 500, statusText: 'Internal Server Error' }),
      );
    }

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
      onError,
    });

    const sendPromise = transport.send([makeEvent(), makeEvent()]);

    // Advance through retry delays: 1s, 5s, 15s
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(5_000);
    await vi.advanceTimersByTimeAsync(15_000);

    await sendPromise;

    expect(fetchSpy).toHaveBeenCalledTimes(4);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][1]).toBe(2); // 2 events dropped
  });

  it('retries on network errors', async () => {
    fetchSpy.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
    });

    const sendPromise = transport.send([makeEvent()]);
    await vi.advanceTimersByTimeAsync(1_000);
    await sendPromise;

    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it('handles non-Error thrown values', async () => {
    for (let i = 0; i < 4; i++) {
      fetchSpy.mockRejectedValueOnce('string error');
    }

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
      onError,
    });

    const sendPromise = transport.send([makeEvent()]);
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(5_000);
    await vi.advanceTimersByTimeAsync(15_000);
    await sendPromise;

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0][0]).toBeInstanceOf(Error);
    expect(onError.mock.calls[0][0].message).toBe('string error');
  });

  it('succeeds on first try when server returns 200', async () => {
    fetchSpy.mockResolvedValueOnce(new Response('OK', { status: 200 }));

    const onError = vi.fn();
    const transport = new HttpTransport({
      endpoint: 'https://test.jobviz.dev',
      apiKey: 'key',
      onError,
    });

    await transport.send([makeEvent()]);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });
});
