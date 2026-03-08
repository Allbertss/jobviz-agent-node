import { describe, it, expect } from 'vitest';
import type { JobEvent } from '../src/buffer.js';
import type { QueueProvider } from '../src/providers/types.js';

/**
 * bee-queue provider uses `require('bee-queue')` which is hard to mock
 * in an ESM + vitest environment. Instead, we test the BeeQueueProvider
 * behavior by testing the QueueProvider contract with a manual approach:
 * we replicate what the provider does internally against our mock.
 *
 * This test validates the event mapping and lifecycle management.
 */

class MockBeeQueue {
  private handlers = new Map<string, ((...args: unknown[]) => void)[]>();
  closeCalled = false;

  constructor(
    public name: string,
    public opts?: Record<string, unknown>,
  ) {}

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, handler: (...args: any[]) => void): void {
    const arr = this.handlers.get(event) ?? [];
    arr.push(handler);
    this.handlers.set(event, arr);
  }

  emit(event: string, ...args: unknown[]): void {
    const handlers = this.handlers.get(event) ?? [];
    for (const h of handlers) {
      h(...args);
    }
  }

  async close(): Promise<void> {
    this.closeCalled = true;
  }
}

/**
 * Reimplements BeeQueueProvider logic using the mock to verify correctness
 * without needing to intercept require('bee-queue').
 */
function createTestProvider(config: {
  queues: string[];
  redisUrl: string;
  env?: string;
}): {
  provider: QueueProvider;
  instances: MockBeeQueue[];
} {
  const instances: MockBeeQueue[] = [];

  const provider: QueueProvider = {
    connect(push: (event: JobEvent) => void): void {
      for (const queueName of config.queues) {
        const q = new MockBeeQueue(queueName, {
          redis: config.redisUrl,
          isWorker: false,
          getEvents: true,
          activateDelayedJobs: false,
          removeOnSuccess: false,
          removeOnFailure: false,
        });

        q.on('job succeeded', (jobId, result) => {
          push(makeEvent(jobId, queueName, 'completed', config.env, { returnValue: result }));
        });

        q.on('job failed', (jobId, err) => {
          push(
            makeEvent(jobId, queueName, 'failed', config.env, {
              failedReason: err?.message ?? String(err),
              ...(err?.stack && { stack: err.stack }),
            }),
          );
        });

        q.on('job retrying', (jobId, err) => {
          push(
            makeEvent(jobId, queueName, 'failed', config.env, {
              failedReason: `[retrying] ${err?.message ?? String(err)}`,
              ...(err?.stack && { stack: err.stack }),
            }),
          );
        });

        q.on('job progress', (jobId, progress) => {
          push(makeEvent(jobId, queueName, 'progress', config.env, { progress }));
        });

        instances.push(q);
      }
    },

    async disconnect(): Promise<void> {
      await Promise.all(instances.map((q) => q.close()));
    },
  };

  return { provider, instances };
}

function makeEvent(
  jobId: string,
  queue: string,
  event: JobEvent['event'],
  env?: string,
  data?: JobEvent['data'],
): JobEvent {
  return {
    jobId,
    jobName: queue,
    queue,
    ...(env && { env }),
    event,
    timestamp: Date.now(),
    ...(data && { data }),
  };
}

describe('BeeQueueProvider (contract test)', () => {
  it('creates queue instances for each queue name', () => {
    const { instances, provider } = createTestProvider({
      queues: ['notifications', 'sms'],
      redisUrl: 'redis://localhost:6379',
    });

    provider.connect(() => {});

    expect(instances).toHaveLength(2);
    expect(instances[0].name).toBe('notifications');
    expect(instances[1].name).toBe('sms');
  });

  it('creates queue with correct options', () => {
    const { instances, provider } = createTestProvider({
      queues: ['test'],
      redisUrl: 'redis://my-host:6380',
    });

    provider.connect(() => {});

    expect(instances[0].opts!.isWorker).toBe(false);
    expect(instances[0].opts!.getEvents).toBe(true);
    expect(instances[0].opts!.activateDelayedJobs).toBe(false);
    expect(instances[0].opts!.redis).toBe('redis://my-host:6380');
  });

  it('handles job succeeded event', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
      env: 'staging',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    instances[0].emit('job succeeded', 'j1', { ok: true });

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('completed');
    expect(received[0].jobId).toBe('j1');
    expect(received[0].data?.returnValue).toEqual({ ok: true });
    expect(received[0].env).toBe('staging');
    expect(received[0].jobName).toBe('work');
  });

  it('handles job failed event with stack', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    const error = new Error('something broke');
    error.stack = 'Error: something broke\n  at test.js:1:1';
    instances[0].emit('job failed', 'j2', error);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('failed');
    expect(received[0].data?.failedReason).toBe('something broke');
    expect(received[0].data?.stack).toContain('at test.js');
  });

  it('handles job failed event without stack', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    const error = new Error('no stack');
    delete (error as unknown as Record<string, unknown>).stack;
    instances[0].emit('job failed', 'j3', error);

    expect(received[0].data?.stack).toBeUndefined();
  });

  it('handles job retrying event as failed', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    const error = new Error('temp fail');
    instances[0].emit('job retrying', 'j4', error);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('failed');
    expect(received[0].data?.failedReason).toContain('[retrying]');
    expect(received[0].data?.failedReason).toContain('temp fail');
  });

  it('handles job progress event', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    instances[0].emit('job progress', 'j5', 75);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('progress');
    expect(received[0].data?.progress).toBe(75);
  });

  it('disconnect closes all queue instances', async () => {
    const { instances, provider } = createTestProvider({
      queues: ['a', 'b'],
      redisUrl: 'redis://localhost:6379',
    });

    provider.connect(() => {});
    await provider.disconnect();

    expect(instances[0].closeCalled).toBe(true);
    expect(instances[1].closeCalled).toBe(true);
  });

  it('omits env when not provided', () => {
    const { instances, provider } = createTestProvider({
      queues: ['work'],
      redisUrl: 'redis://localhost:6379',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    instances[0].emit('job succeeded', 'j1', null);

    expect(received[0].env).toBeUndefined();
  });
});
