import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EventBuffer, type JobEvent } from '../src/buffer.js';

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

describe('EventBuffer', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('flushes when batchSize is reached', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(3, 60_000, onFlush);
    buffer.start();

    buffer.push(makeEvent({ jobId: '1' }));
    buffer.push(makeEvent({ jobId: '2' }));
    buffer.push(makeEvent({ jobId: '3' }));

    // flush is async — give microtasks a tick
    await vi.advanceTimersByTimeAsync(0);

    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(onFlush.mock.calls[0][0]).toHaveLength(3);
    await buffer.stop();
  });

  it('flushes on interval', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(100, 1000, onFlush);
    buffer.start();

    buffer.push(makeEvent());
    expect(onFlush).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1000);

    expect(onFlush).toHaveBeenCalledTimes(1);
    await buffer.stop();
  });

  it('does not flush when buffer is empty', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(100, 1000, onFlush);
    buffer.start();

    await vi.advanceTimersByTimeAsync(1000);

    expect(onFlush).not.toHaveBeenCalled();
    await buffer.stop();
  });

  it('flushes remaining events on stop', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(100, 60_000, onFlush);
    buffer.start();

    buffer.push(makeEvent({ jobId: '1' }));
    buffer.push(makeEvent({ jobId: '2' }));

    await buffer.stop();

    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(onFlush.mock.calls[0][0]).toHaveLength(2);
  });

  it('chunks oversized batches (MAX_CHUNK_SIZE = 500)', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(1000, 60_000, onFlush);
    buffer.start();

    for (let i = 0; i < 750; i++) {
      buffer.push(makeEvent({ jobId: String(i) }));
    }

    await buffer.stop();

    // 750 events should be split into 500 + 250
    expect(onFlush).toHaveBeenCalledTimes(2);
    expect(onFlush.mock.calls[0][0]).toHaveLength(500);
    expect(onFlush.mock.calls[1][0]).toHaveLength(250);
  });

  it('swallows flush errors silently', async () => {
    const onFlush = vi.fn().mockRejectedValue(new Error('network fail'));
    const buffer = new EventBuffer(1, 60_000, onFlush);
    buffer.start();

    // Should not throw
    buffer.push(makeEvent());
    await vi.advanceTimersByTimeAsync(0);

    await buffer.stop();
    expect(onFlush).toHaveBeenCalled();
  });

  it('throws on invalid batchSize', () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    expect(() => new EventBuffer(0, 1000, onFlush)).toThrow('batchSize must be >= 1');
    expect(() => new EventBuffer(-1, 1000, onFlush)).toThrow('batchSize must be >= 1');
  });

  it('throws on invalid flushInterval', () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    expect(() => new EventBuffer(10, 50, onFlush)).toThrow('flushInterval must be >= 100ms');
    expect(() => new EventBuffer(10, -1, onFlush)).toThrow('flushInterval must be >= 100ms');
  });

  it('drops oldest events when maxBufferSize is exceeded', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(100, 60_000, onFlush, 5);
    buffer.start();

    for (let i = 0; i < 8; i++) {
      buffer.push(makeEvent({ jobId: String(i) }));
    }

    await buffer.stop();

    // Buffer capped at 5, so oldest 3 were dropped
    expect(onFlush).toHaveBeenCalledTimes(1);
    const flushed = onFlush.mock.calls[0][0];
    expect(flushed).toHaveLength(5);
    expect(flushed[0].jobId).toBe('3');
    expect(flushed[4].jobId).toBe('7');
  });

  it('start is idempotent', async () => {
    const onFlush = vi.fn().mockResolvedValue(undefined);
    const buffer = new EventBuffer(100, 1000, onFlush);

    buffer.start();
    buffer.start(); // second call should be a no-op

    buffer.push(makeEvent());
    await vi.advanceTimersByTimeAsync(1000);

    // Only one flush — proves only one interval is running
    expect(onFlush).toHaveBeenCalledTimes(1);
    await buffer.stop();
  });

  it('prevents concurrent flushes via flush guard', async () => {
    let resolveFlush!: () => void;
    let callCount = 0;
    const onFlush = vi.fn().mockImplementation(() => {
      callCount++;
      if (callCount === 1) {
        // First flush hangs until manually resolved
        return new Promise<void>((resolve) => { resolveFlush = resolve; });
      }
      return Promise.resolve();
    });
    const buffer = new EventBuffer(2, 60_000, onFlush);
    buffer.start();

    // Push 2 events to trigger a batch-size flush
    buffer.push(makeEvent({ jobId: '1' }));
    buffer.push(makeEvent({ jobId: '2' }));
    await vi.advanceTimersByTimeAsync(0);

    // First flush is now in-flight; push 2 more and trigger another flush attempt
    buffer.push(makeEvent({ jobId: '3' }));
    buffer.push(makeEvent({ jobId: '4' }));
    await vi.advanceTimersByTimeAsync(0);

    // Only 1 flush call should have happened (second was skipped by guard)
    expect(onFlush).toHaveBeenCalledTimes(1);

    // Resolve the in-flight flush so the guard releases
    resolveFlush();
    await vi.advanceTimersByTimeAsync(0);

    // Now stop to flush the remaining 2 events
    await buffer.stop();

    // Second batch should now be flushed
    expect(onFlush).toHaveBeenCalledTimes(2);
    expect(onFlush.mock.calls[1][0]).toHaveLength(2);
    expect(onFlush.mock.calls[1][0][0].jobId).toBe('3');
  });
});
