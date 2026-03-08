import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { JobEvent } from '../src/buffer.js';

// Mock BullMQ before importing the provider
const mockQueueEventsInstances: MockQueueEvents[] = [];
const mockQueueInstances: MockQueue[] = [];

class MockQueueEvents {
  private handlers = new Map<string, ((...args: unknown[]) => void)[]>();

  constructor(
    public queueName: string,
    public opts?: unknown,
  ) {
    mockQueueEventsInstances.push(this);
  }

  on(event: string, handler: (...args: unknown[]) => void): void {
    const arr = this.handlers.get(event) ?? [];
    arr.push(handler);
    this.handlers.set(event, arr);
  }

  emit(event: string, data: unknown): void {
    const handlers = this.handlers.get(event) ?? [];
    for (const h of handlers) {
      h(data);
    }
  }

  async close(): Promise<void> {}
}

class MockQueue {
  constructor(
    public queueName: string,
    public opts?: unknown,
  ) {
    mockQueueInstances.push(this);
  }

  async close(): Promise<void> {}
}

class MockJob {
  constructor(
    public id: string,
    public name: string,
    public data: Record<string, unknown> = {},
    public parent?: { id: string; queueKey: string },
    public stacktrace: string[] = [],
  ) {}
}

let mockJobFromId: ReturnType<typeof vi.fn>;

vi.mock('bullmq', () => {
  mockJobFromId = vi.fn();
  return {
    QueueEvents: MockQueueEvents,
    Queue: MockQueue,
    Job: {
      fromId: mockJobFromId,
    },
  };
});

// Import after mock is set up
const { BullMQProvider } = await import('../src/providers/bullmq.js');

describe('BullMQProvider', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockQueueEventsInstances.length = 0;
    mockQueueInstances.length = 0;
    mockJobFromId.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('creates QueueEvents and Queue for each queue name', () => {
    const provider = new BullMQProvider({
      queues: ['emails', 'reports'],
      connection: { url: 'redis://localhost:6379' },
    });

    provider.connect(() => {});
    expect(mockQueueEventsInstances).toHaveLength(2);
    expect(mockQueueInstances).toHaveLength(2);
    expect(mockQueueEventsInstances[0].queueName).toBe('emails');
    expect(mockQueueEventsInstances[1].queueName).toBe('reports');
  });

  it('handles waiting event — fetches job and caches', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'send-email', { to: 'user@test.com' }),
    );

    const provider = new BullMQProvider({
      queues: ['emails'],
      connection: { url: 'redis://localhost:6379' },
      env: 'test',
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('waiting');
    expect(received[0].jobName).toBe('send-email');
    expect(received[0].env).toBe('test');

    await provider.disconnect();
  });

  it('handles active event with cache hit', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'send-email', { to: 'user@test.com' }),
    );

    const provider = new BullMQProvider({
      queues: ['emails'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // First: waiting (caches the job)
    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    // Then: active (should use cache)
    mockQueueEventsInstances[0].emit('active', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('active');
    expect(received[1].jobName).toBe('send-email');
    expect(received[1].data?.input).toEqual({ to: 'user@test.com' });

    // Job.fromId should only be called once (for waiting)
    expect(mockJobFromId).toHaveBeenCalledTimes(1);

    await provider.disconnect();
  });

  it('handles active event with cache miss (fallback to Redis)', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j2', 'gen-pdf', { docId: 42 }),
    );

    const provider = new BullMQProvider({
      queues: ['reports'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Directly emit active without waiting first
    mockQueueEventsInstances[0].emit('active', { jobId: 'j2' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('active');
    expect(received[0].jobName).toBe('gen-pdf');

    await provider.disconnect();
  });

  it('handles completed event', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Cache the job first
    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('completed', {
      jobId: 'j1',
      returnvalue: '{"ok":true}',
    });

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('completed');
    expect(received[1].data?.returnValue).toBe('{"ok":true}');

    await provider.disconnect();
  });

  it('handles failed event with stack traces enabled', async () => {
    mockJobFromId
      .mockResolvedValueOnce(new MockJob('j1', 'task'))
      .mockResolvedValueOnce(
        new MockJob('j1', 'task', {}, undefined, [
          'Error: boom',
          '  at fn (file.js:1:1)',
        ]),
      );

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
      captureStackTraces: true,
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Cache job
    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('failed', {
      jobId: 'j1',
      failedReason: 'boom',
    });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('failed');
    expect(received[1].data?.failedReason).toBe('boom');
    expect(received[1].data?.stack).toContain('Error: boom');

    await provider.disconnect();
  });

  it('handles failed event with stack traces disabled', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
      captureStackTraces: false,
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Cache the job
    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('failed', {
      jobId: 'j1',
      failedReason: 'timeout',
    });

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('failed');
    expect(received[1].data?.failedReason).toBe('timeout');
    expect(received[1].data?.stack).toBeUndefined();

    // Should NOT have called Job.fromId for the failed event
    expect(mockJobFromId).toHaveBeenCalledTimes(1); // only for waiting

    await provider.disconnect();
  });

  it('handles delayed event', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('delayed', {
      jobId: 'j1',
      delay: 5000,
    });

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('delayed');
    expect(received[1].data?.delay).toBe(5000);

    await provider.disconnect();
  });

  it('handles stalled event', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('stalled', { jobId: 'j1' });

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('stalled');

    await provider.disconnect();
  });

  it('handles progress event', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('progress', {
      jobId: 'j1',
      data: 50,
    });

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('progress');
    expect(received[1].data?.progress).toBe(50);

    await provider.disconnect();
  });

  it('extracts traceId from job data', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'task', { traceId: 'trace-xyz' }),
    );

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].traceId).toBe('trace-xyz');

    await provider.disconnect();
  });

  it('extracts correlationId as traceId', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'task', { correlationId: 'corr-abc' }),
    );

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].traceId).toBe('corr-abc');

    await provider.disconnect();
  });

  it('extracts parent job info', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'child-task', {}, {
        id: 'parent-1',
        queueKey: 'bull:parent-queue:prefix',
      }),
    );

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].parentJobId).toBe('parent-1');
    expect(received[0].parentQueue).toBe('parent-queue');

    await provider.disconnect();
  });

  it('captureInput: false omits job data', async () => {
    mockJobFromId.mockResolvedValue(
      new MockJob('j1', 'task', { secret: 'sensitive' }),
    );

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
      captureInput: false,
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Trigger active with cache miss to test getInput
    mockQueueEventsInstances[0].emit('active', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].data?.input).toBeUndefined();

    await provider.disconnect();
  });

  it('sweeps stale cache entries', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Add a job to cache
    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    // Advance past cache TTL (30 min) + sweep interval (60s)
    await vi.advanceTimersByTimeAsync(31 * 60 * 1000);

    // Stalled event on expired job — should still work (empty name)
    mockQueueEventsInstances[0].emit('stalled', { jobId: 'j1' });

    const stalledEvent = received.find(
      (e) => e.event === 'stalled' && e.jobId === 'j1',
    );
    expect(stalledEvent).toBeDefined();
    expect(stalledEvent!.jobName).toBe(''); // cache was swept

    await provider.disconnect();
  });

  it('handles Job.fromId failure on waiting event', async () => {
    mockJobFromId.mockRejectedValue(new Error('Redis connection refused'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    // Should still emit a minimal event
    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('waiting');
    expect(received[0].jobName).toBe('');

    await provider.disconnect();
  });

  it('handles Job.fromId failure on active event (cache miss)', async () => {
    mockJobFromId.mockRejectedValue(new Error('Redis timeout'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('active', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('active');

    await provider.disconnect();
  });

  it('handles Job.fromId failure on failed event', async () => {
    mockJobFromId
      .mockResolvedValueOnce(new MockJob('j1', 'task')) // for waiting
      .mockRejectedValueOnce(new Error('Redis gone')); // for failed

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
      captureStackTraces: true,
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    mockQueueEventsInstances[0].emit('failed', {
      jobId: 'j1',
      failedReason: 'boom',
    });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('failed');
    expect(received[1].data?.failedReason).toBe('boom');
    // No stack since Redis call failed
    expect(received[1].data?.stack).toBeUndefined();

    await provider.disconnect();
  });

  it('disconnect clears timers, listeners, queues, and cache', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task'));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    provider.connect(() => {});

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    await provider.disconnect();

    // Internal state should be cleaned up — verify by checking no errors on second disconnect
    await provider.disconnect();
  });

  it('handles job with empty data', async () => {
    mockJobFromId.mockResolvedValue(new MockJob('j1', 'task', {}));

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('active', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].data?.input).toBeUndefined();

    await provider.disconnect();
  });

  it('handles job with undefined data', async () => {
    const job = new MockJob('j1', 'task');
    job.data = undefined as unknown as Record<string, unknown>;
    mockJobFromId.mockResolvedValue(job);

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received[0].traceId).toBeUndefined();

    await provider.disconnect();
  });

  it('handles Job.fromId returning undefined', async () => {
    mockJobFromId.mockResolvedValue(undefined);

    const provider = new BullMQProvider({
      queues: ['work'],
      connection: { url: 'redis://localhost:6379' },
    });

    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    mockQueueEventsInstances[0].emit('waiting', { jobId: 'j1' });
    await vi.advanceTimersByTimeAsync(0);

    expect(received).toHaveLength(1);
    expect(received[0].jobName).toBe('');

    await provider.disconnect();
  });
});
