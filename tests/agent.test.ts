import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { JobvizAgent } from '../src/agent.js';
import type { JobvizConfig } from '../src/agent.js';
import type { QueueProvider } from '../src/providers/types.js';
import type { JobEvent } from '../src/buffer.js';

/** Minimal mock provider for testing the agent without Redis. */
class MockProvider implements QueueProvider {
  private pushFn: ((event: JobEvent) => void) | null = null;

  connect(push: (event: JobEvent) => void): void {
    this.pushFn = push;
  }

  async disconnect(): Promise<void> {
    this.pushFn = null;
  }

  /** Simulate an event from the queue system. */
  emit(event: JobEvent): void {
    this.pushFn?.(event);
  }
}

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

function makeConfig(
  provider: MockProvider,
  overrides: Partial<JobvizConfig> = {},
): JobvizConfig {
  return {
    apiKey: 'jv_test_key_123',
    provider,
    ...overrides,
  } as JobvizConfig;
}

describe('JobvizAgent', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('OK', { status: 200 }),
    );
  });

  afterEach(() => {
    vi.useRealTimers();
    fetchSpy.mockRestore();
  });

  it('throws when apiKey is missing', () => {
    const provider = new MockProvider();
    expect(
      () => new JobvizAgent({ apiKey: '', provider } as JobvizConfig),
    ).toThrow('apiKey is required');
  });

  it('throws when apiKey is not a string', () => {
    const provider = new MockProvider();
    expect(
      () =>
        new JobvizAgent({
          apiKey: 123 as unknown as string,
          provider,
        } as JobvizConfig),
    ).toThrow('apiKey is required');
  });

  it('starts and forwards events through the buffer to transport', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    provider.emit(makeEvent({ jobId: 'job-1' }));

    // Advance past flush interval (default 3s)
    await vi.advanceTimersByTimeAsync(3_000);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    expect(body.events).toHaveLength(1);
    expect(body.events[0].jobId).toBe('job-1');

    await agent.stop();
  });

  it('flushes remaining events on stop', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    provider.emit(makeEvent({ jobId: 'job-2' }));

    await agent.stop();

    expect(fetchSpy).toHaveBeenCalled();
  });

  it('log() pushes progress events', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    agent.log(
      { id: 'j1', name: 'send-email', queueName: 'emails' },
      'Sending email',
      { recipients: 3 },
    );

    await agent.stop();

    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    const event = body.events[0];
    expect(event.event).toBe('progress');
    expect(event.data.log.message).toBe('Sending email');
    expect(event.data.log.meta).toEqual({ recipients: 3 });
  });

  it('log() omits meta when empty', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    agent.log({ id: 'j1' }, 'Step 1');

    await agent.stop();

    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    expect(body.events[0].data.log.meta).toBeUndefined();
  });

  it('trackDeployment() pushes deployment event', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    agent.trackDeployment({
      version: '1.2.3',
      commitHash: 'abc123',
      description: 'Fix bug',
    });

    await agent.stop();

    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    const event = body.events[0];
    expect(event.event).toBe('deployment');
    expect(event.queue).toBe('__system');
    expect(event.data.version).toBe('1.2.3');
    expect(event.data.commitHash).toBe('abc123');
  });

  it('trackDeployment() handles optional fields', async () => {
    const provider = new MockProvider();
    const agent = new JobvizAgent(makeConfig(provider));
    await agent.start();

    agent.trackDeployment({ version: '2.0.0' });

    await agent.stop();

    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    expect(body.events[0].data.commitHash).toBeNull();
    expect(body.events[0].data.description).toBeNull();
  });

  describe('redaction', () => {
    it('redacts default sensitive keys when redactKeys is true', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: true }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              email: 'user@test.com',
              password: 's3cret',
              token: 'abc',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.email).toBe('user@test.com');
      expect(input.password).toBe('[REDACTED]');
      expect(input.token).toBe('[REDACTED]');
    });

    it('redacts custom keys merged with defaults', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: ['mySecret'] }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              mySecret: 'hidden',
              password: 'also-hidden',
              normal: 'visible',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.mySecret).toBe('[REDACTED]');
      expect(input.password).toBe('[REDACTED]');
      expect(input.normal).toBe('visible');
    });

    it('redacts nested object keys', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: true }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              user: {
                name: 'Alice',
                password: 'hunter2',
              },
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      expect(body.events[0].data.input.user.name).toBe('Alice');
      expect(body.events[0].data.input.user.password).toBe('[REDACTED]');
    });

    it('does not redact when redactKeys is not set', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(makeConfig(provider));
      await agent.start();

      provider.emit(
        makeEvent({
          data: { input: { password: 'visible' } },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      expect(body.events[0].data.input.password).toBe('visible');
    });

    it('handles events without input data', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: true }),
      );
      await agent.start();

      provider.emit(makeEvent({ data: { failedReason: 'timeout' } }));

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      expect(body.events[0].data.failedReason).toBe('timeout');
    });

    it('redacts sensitive keys inside arrays', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: true }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              users: [
                { name: 'Alice', password: 'hunter2' },
                { name: 'Bob', token: 'xyz' },
              ],
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const users = body.events[0].data.input.users;
      expect(users[0].name).toBe('Alice');
      expect(users[0].password).toBe('[REDACTED]');
      expect(users[1].name).toBe('Bob');
      expect(users[1].token).toBe('[REDACTED]');
    });

    it('skips prototype pollution keys', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: true }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              __proto__: 'malicious',
              constructor: 'evil',
              safe: 'value',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.safe).toBe('value');
      expect(Object.prototype.hasOwnProperty.call(input, '__proto__')).toBe(false);
      expect(Object.prototype.hasOwnProperty.call(input, 'constructor')).toBe(false);
    });

    it('redactKeys with { only } uses only specified keys', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: { only: ['myField'] } }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              myField: 'hidden',
              password: 'visible-because-only-mode',
              normal: 'visible',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.myField).toBe('[REDACTED]');
      expect(input.password).toBe('visible-because-only-mode');
      expect(input.normal).toBe('visible');
    });

    it('redactKeys with { exclude } removes keys from defaults', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: { exclude: ['password', 'token'] } }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              password: 'now-visible',
              token: 'now-visible',
              secret: 'still-redacted',
              normal: 'visible',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.password).toBe('now-visible');
      expect(input.token).toBe('now-visible');
      expect(input.secret).toBe('[REDACTED]');
      expect(input.normal).toBe('visible');
    });

    it('redactKeys with { include } adds keys to defaults', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { redactKeys: { include: ['dob', 'bankAccount'] } }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              dob: '1990-01-01',
              bankAccount: '123456',
              password: 'hidden',
              normal: 'visible',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.dob).toBe('[REDACTED]');
      expect(input.bankAccount).toBe('[REDACTED]');
      expect(input.password).toBe('[REDACTED]');
      expect(input.normal).toBe('visible');
    });

    it('redactKeys with { include, exclude } combines both', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, {
          redactKeys: { include: ['customKey'], exclude: ['password'] },
        }),
      );
      await agent.start();

      provider.emit(
        makeEvent({
          data: {
            input: {
              password: 'now-visible',
              customKey: 'hidden',
              secret: 'still-redacted',
            },
          },
        }),
      );

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      const input = body.events[0].data.input;
      expect(input.password).toBe('now-visible');
      expect(input.customKey).toBe('[REDACTED]');
      expect(input.secret).toBe('[REDACTED]');
    });
  });

  describe('config options', () => {
    it('captureInput defaults to true', () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(makeConfig(provider));
      expect(agent.captureInput).toBe(true);
    });

    it('captureStackTraces defaults to true', () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(makeConfig(provider));
      expect(agent.captureStackTraces).toBe(true);
    });

    it('respects captureInput: false', () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { captureInput: false }),
      );
      expect(agent.captureInput).toBe(false);
    });

    it('respects captureStackTraces: false', () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { captureStackTraces: false }),
      );
      expect(agent.captureStackTraces).toBe(false);
    });

    it('passes maxBufferSize to the event buffer', async () => {
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { maxBufferSize: 3 }),
      );
      await agent.start();

      // Push 5 events — only last 3 should survive
      for (let i = 0; i < 5; i++) {
        provider.emit(makeEvent({ jobId: String(i) }));
      }

      await agent.stop();

      const body = JSON.parse(
        (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
      );
      expect(body.events).toHaveLength(3);
      expect(body.events[0].jobId).toBe('2');
      expect(body.events[2].jobId).toBe('4');
    });
  });

  describe('error handling', () => {
    it('calls onError callback in development mode by default', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      fetchSpy.mockReset();
      // 4 total attempts all fail
      for (let i = 0; i < 4; i++) {
        fetchSpy.mockResolvedValueOnce(
          new Response('Error', { status: 500, statusText: 'Internal Server Error' }),
        );
      }

      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { env: 'development' }),
      );
      await agent.start();

      provider.emit(makeEvent());

      // Advance through flush + retries
      await vi.advanceTimersByTimeAsync(3_000); // flush
      await vi.advanceTimersByTimeAsync(1_000); // retry 1
      await vi.advanceTimersByTimeAsync(5_000); // retry 2
      await vi.advanceTimersByTimeAsync(15_000); // retry 3

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[jobviz] dropped'),
      );

      consoleSpy.mockRestore();
      await agent.stop();
    });

    it('calls custom onError callback', async () => {
      fetchSpy.mockReset();
      fetchSpy.mockResolvedValue(
        new Response(JSON.stringify({ error: 'Bad key' }), { status: 401 }),
      );

      const onError = vi.fn();
      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { onError }),
      );
      await agent.start();

      provider.emit(makeEvent());
      await vi.advanceTimersByTimeAsync(3_000);

      expect(onError).toHaveBeenCalled();
      await agent.stop();
    });

    it('is silent in production mode without onError', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      fetchSpy.mockReset();
      fetchSpy.mockResolvedValue(
        new Response(JSON.stringify({ error: 'Bad key' }), { status: 401 }),
      );

      const provider = new MockProvider();
      const agent = new JobvizAgent(
        makeConfig(provider, { env: 'production' }),
      );
      await agent.start();

      provider.emit(makeEvent());
      await vi.advanceTimersByTimeAsync(3_000);

      // In production without onError, should not log to console
      expect(consoleSpy).not.toHaveBeenCalledWith(
        expect.stringContaining('[jobviz]'),
      );

      consoleSpy.mockRestore();
      await agent.stop();
    });
  });
});
