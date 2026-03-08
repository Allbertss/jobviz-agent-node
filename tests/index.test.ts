import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { initJobviz, stopJobviz, jobviz, JobvizAgent } from '../src/index.js';
import type { QueueProvider } from '../src/providers/types.js';

class MockProvider implements QueueProvider {
  connect(): void {}
  async disconnect(): Promise<void> {}
}

describe('initJobviz / stopJobviz', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('OK', { status: 200 }),
    );
  });

  afterEach(async () => {
    await stopJobviz();
    vi.useRealTimers();
    fetchSpy.mockRestore();
  });

  it('initializes and returns a JobvizAgent', async () => {
    const agent = await initJobviz({
      apiKey: 'jv_test_key_1234',
      provider: new MockProvider(),
    } as Parameters<typeof initJobviz>[0]);

    expect(agent).toBeInstanceOf(JobvizAgent);
  });

  it('throws on double initialization', async () => {
    await initJobviz({
      apiKey: 'jv_test_key_1234',
      provider: new MockProvider(),
    } as Parameters<typeof initJobviz>[0]);

    await expect(
      initJobviz({
        apiKey: 'jv_test_key_1234',
        provider: new MockProvider(),
      } as Parameters<typeof initJobviz>[0]),
    ).rejects.toThrow('already initialized');
  });

  it('allows re-initialization after stopJobviz', async () => {
    await initJobviz({
      apiKey: 'jv_test_key_1234',
      provider: new MockProvider(),
    } as Parameters<typeof initJobviz>[0]);

    await stopJobviz();

    // Should not throw
    await initJobviz({
      apiKey: 'jv_test_key_1234',
      provider: new MockProvider(),
    } as Parameters<typeof initJobviz>[0]);
  });

  it('stopJobviz is safe to call when not initialized', async () => {
    await stopJobviz(); // should not throw
  });
});

describe('jobviz.log', () => {
  let fetchSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.useFakeTimers();
    fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('OK', { status: 200 }),
    );
  });

  afterEach(async () => {
    await stopJobviz();
    vi.useRealTimers();
    fetchSpy.mockRestore();
  });

  it('is a no-op when agent is not initialized', () => {
    // Should not throw
    jobviz.log({ id: '1', name: 'test', queueName: 'q' }, 'hello');
  });

  it('sends log events when agent is initialized', async () => {
    await initJobviz({
      apiKey: 'jv_test_key_1234',
      provider: new MockProvider(),
    } as Parameters<typeof initJobviz>[0]);

    jobviz.log({ id: 'j1', name: 'test', queueName: 'q' }, 'Step 1');

    await stopJobviz();

    expect(fetchSpy).toHaveBeenCalled();
    const body = JSON.parse(
      (fetchSpy.mock.calls[0] as [string, RequestInit])[1].body as string,
    );
    expect(body.events[0].data.log.message).toBe('Step 1');
  });
});

describe('exports', () => {
  it('exports JobvizAgent class', async () => {
    const mod = await import('../src/index.js');
    expect(mod.JobvizAgent).toBeDefined();
  });

  it('exports provider classes', async () => {
    const mod = await import('../src/index.js');
    expect(mod.BullMQProvider).toBeDefined();
    expect(mod.BeeQueueProvider).toBeDefined();
    expect(mod.AgendaProvider).toBeDefined();
    expect(mod.MultiProvider).toBeDefined();
  });
});
