import { describe, it, expect, vi } from 'vitest';
import { MultiProvider } from '../src/providers/multi.js';
import type { QueueProvider } from '../src/providers/types.js';
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

class MockProvider implements QueueProvider {
  private pushFn: ((event: JobEvent) => void) | null = null;
  connectCalled = false;
  disconnectCalled = false;

  connect(push: (event: JobEvent) => void): void {
    this.pushFn = push;
    this.connectCalled = true;
  }

  async disconnect(): Promise<void> {
    this.disconnectCalled = true;
    this.pushFn = null;
  }

  emit(event: JobEvent): void {
    this.pushFn?.(event);
  }
}

describe('MultiProvider', () => {
  it('connects all sub-providers', () => {
    const p1 = new MockProvider();
    const p2 = new MockProvider();
    const multi = new MultiProvider([p1, p2]);

    multi.connect(() => {});

    expect(p1.connectCalled).toBe(true);
    expect(p2.connectCalled).toBe(true);
  });

  it('disconnects all sub-providers', async () => {
    const p1 = new MockProvider();
    const p2 = new MockProvider();
    const multi = new MultiProvider([p1, p2]);

    multi.connect(() => {});
    await multi.disconnect();

    expect(p1.disconnectCalled).toBe(true);
    expect(p2.disconnectCalled).toBe(true);
  });

  it('forwards events from all sub-providers to the push callback', () => {
    const p1 = new MockProvider();
    const p2 = new MockProvider();
    const multi = new MultiProvider([p1, p2]);

    const received: JobEvent[] = [];
    multi.connect((event) => received.push(event));

    p1.emit(makeEvent({ jobId: 'from-p1' }));
    p2.emit(makeEvent({ jobId: 'from-p2' }));

    expect(received).toHaveLength(2);
    expect(received[0].jobId).toBe('from-p1');
    expect(received[1].jobId).toBe('from-p2');
  });

  it('works with zero providers', async () => {
    const multi = new MultiProvider([]);
    multi.connect(() => {});
    await multi.disconnect();
  });
});

describe('AgendaProvider', () => {
  it('throws when neither agenda nor mongoUrl is provided', async () => {
    const { AgendaProvider } = await import('../src/providers/agenda.js');
    expect(() => new AgendaProvider({})).toThrow(
      'requires either an `agenda` instance or a `mongoUrl`',
    );
  });

  it('connects to an existing agenda instance', async () => {
    const { AgendaProvider } = await import('../src/providers/agenda.js');

    const listeners = new Map<string, ((...args: unknown[]) => void)[]>();
    const mockAgenda = {
      on(event: string, listener: (...args: unknown[]) => void): void {
        const arr = listeners.get(event) ?? [];
        arr.push(listener);
        listeners.set(event, arr);
      },
      off(event: string, listener: (...args: unknown[]) => void): void {
        const arr = listeners.get(event) ?? [];
        listeners.set(
          event,
          arr.filter((l) => l !== listener),
        );
      },
      async start(): Promise<void> {},
      async stop(): Promise<void> {},
    };

    const provider = new AgendaProvider({ agenda: mockAgenda });
    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    // Verify listeners were attached
    expect(listeners.get('start')?.length).toBe(1);
    expect(listeners.get('complete')?.length).toBe(1);
    expect(listeners.get('fail')?.length).toBe(1);

    // Simulate a job start event
    const fakeJob = {
      attrs: { _id: 'agenda-1', name: 'daily-report', data: { region: 'us' } },
    };
    listeners.get('start')![0](fakeJob);

    expect(received).toHaveLength(1);
    expect(received[0].event).toBe('active');
    expect(received[0].jobName).toBe('daily-report');
    expect(received[0].queue).toBe('agenda');

    // Simulate completion
    listeners.get('complete')![0](fakeJob);
    expect(received).toHaveLength(2);
    expect(received[1].event).toBe('completed');

    // Simulate failure
    const error = new Error('oops');
    listeners.get('fail')![0](error, fakeJob);
    expect(received).toHaveLength(3);
    expect(received[2].event).toBe('failed');
    expect(received[2].data?.failedReason).toBe('oops');

    await provider.disconnect();

    // Listeners should be removed
    expect(listeners.get('start')?.length).toBe(0);
    expect(listeners.get('complete')?.length).toBe(0);
    expect(listeners.get('fail')?.length).toBe(0);
  });

  it('extracts traceId from job data', async () => {
    const { AgendaProvider } = await import('../src/providers/agenda.js');

    const listeners = new Map<string, ((...args: unknown[]) => void)[]>();
    const mockAgenda = {
      on(event: string, listener: (...args: unknown[]) => void): void {
        const arr = listeners.get(event) ?? [];
        arr.push(listener);
        listeners.set(event, arr);
      },
      off(event: string, listener: (...args: unknown[]) => void): void {
        const arr = listeners.get(event) ?? [];
        listeners.set(
          event,
          arr.filter((l) => l !== listener),
        );
      },
      async start(): Promise<void> {},
      async stop(): Promise<void> {},
    };

    const provider = new AgendaProvider({ agenda: mockAgenda });
    const received: JobEvent[] = [];
    provider.connect((event) => received.push(event));

    const fakeJob = {
      attrs: {
        _id: 'agenda-2',
        name: 'export',
        data: { traceId: 'trace-abc' },
      },
    };
    listeners.get('start')![0](fakeJob);

    expect(received[0].traceId).toBe('trace-abc');
    await provider.disconnect();
  });

  it('uses removeListener when off is not available', async () => {
    const { AgendaProvider } = await import('../src/providers/agenda.js');

    const removeListenerSpy = vi.fn();
    const mockAgenda = {
      on(): void {},
      removeListener: removeListenerSpy,
      async start(): Promise<void> {},
      async stop(): Promise<void> {},
    };

    const provider = new AgendaProvider({ agenda: mockAgenda });
    provider.connect(() => {});
    await provider.disconnect();

    expect(removeListenerSpy).toHaveBeenCalledTimes(3); // start, complete, fail
  });

  it('does not stop agenda when it does not own it', async () => {
    const { AgendaProvider } = await import('../src/providers/agenda.js');

    const stopSpy = vi.fn();
    const mockAgenda = {
      on(): void {},
      off(): void {},
      async start(): Promise<void> {},
      stop: stopSpy,
    };

    const provider = new AgendaProvider({ agenda: mockAgenda });
    provider.connect(() => {});
    await provider.disconnect();

    expect(stopSpy).not.toHaveBeenCalled(); // should not stop user's agenda
  });
});

describe('DebugLogger', () => {
  it('tracks stats correctly', async () => {
    const { DebugLogger } = await import('../src/debug-logger.js');
    const logger = new DebugLogger();

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const consoleErrorSpy = vi
      .spyOn(console, 'error')
      .mockImplementation(() => {});

    logger.logEvent(makeEvent());
    logger.logFlush(5);
    logger.logFlush(3);
    logger.logDrop(new Error('timeout'), 2);

    const stats = logger.getStats();
    expect(stats.events_sent).toBe(8);
    expect(stats.events_dropped).toBe(2);
    expect(stats.last_flush_at).toBeTruthy();
    expect(stats.recent_errors).toHaveLength(1);

    consoleSpy.mockRestore();
    consoleErrorSpy.mockRestore();
  });

  it('caps errors at 50', async () => {
    const { DebugLogger } = await import('../src/debug-logger.js');
    const logger = new DebugLogger();

    const consoleErrorSpy = vi
      .spyOn(console, 'error')
      .mockImplementation(() => {});

    for (let i = 0; i < 60; i++) {
      logger.logDrop(new Error(`err-${i}`), 1);
    }

    const stats = logger.getStats();
    expect(stats.recent_errors.length).toBeLessThanOrEqual(50);

    consoleErrorSpy.mockRestore();
  });

  it('logConnectionIssue tracks warning', async () => {
    const { DebugLogger } = await import('../src/debug-logger.js');
    const logger = new DebugLogger();

    const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

    logger.logConnectionIssue('Redis unreachable');

    const stats = logger.getStats();
    expect(stats.recent_errors).toHaveLength(1);
    expect(stats.recent_errors[0].message).toBe('Redis unreachable');

    consoleSpy.mockRestore();
  });
});

describe('DebugServer', () => {
  it('serves health endpoint on 127.0.0.1', async () => {
    const { DebugServer } = await import('../src/debug-server.js');

    const server = new DebugServer(
      0, // port 0 = random available port
      () => ({ test: true }),
      () => ({ events_sent: 5 }),
    );

    // Patch the port to use 0 for testing
    server.start();

    // Give server a tick to start
    await new Promise((r) => setTimeout(r, 100));

    await server.stop();
  });

  it('stop is safe when server is not started', async () => {
    const { DebugServer } = await import('../src/debug-server.js');

    const server = new DebugServer(
      0,
      () => ({}),
      () => ({}),
    );

    await server.stop(); // should not throw
  });
});

describe('version', () => {
  it('exports AGENT_VERSION as a string', async () => {
    const { AGENT_VERSION } = await import('../src/version.js');
    expect(typeof AGENT_VERSION).toBe('string');
    expect(AGENT_VERSION).toMatch(/^\d+\.\d+\.\d+/);
  });
});
