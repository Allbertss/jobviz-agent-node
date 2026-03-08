import { Queue, Worker } from 'bullmq';
import type { ConnectionOptions } from 'bullmq';
import { EventBuffer, type JobEvent } from './buffer.js';
import { BullMQProvider } from './providers/bullmq.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const QUEUE_NAME = 'jobviz-test';

const connection: ConnectionOptions = { url: REDIS_URL };

const received: JobEvent[] = [];

const buffer = new EventBuffer(100, 1000, async (events) => {
  received.push(...events);
  for (const e of events) {
    console.log(
      `[buffer flush] ${e.event.padEnd(10)} queue=${e.queue} jobId=${e.jobId} name=${e.jobName}`,
    );
  }
});

const provider = new BullMQProvider({
  queues: [QUEUE_NAME],
  connection,
});

const queue = new Queue(QUEUE_NAME, { connection });

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    if (job.name === 'fail-me') throw new Error('intentional failure');
    return { ok: true };
  },
  { connection },
);

async function cleanup() {
  await worker.close();
  await queue.close();
  await buffer.stop();
  await provider.disconnect();
}

async function run() {
  provider.connect((event) => buffer.push(event));
  buffer.start();

  // give QueueEvents time to subscribe
  await new Promise((r) => setTimeout(r, 300));

  console.log('Adding jobs...');
  await queue.add('send-email', { to: 'user@example.com' });
  await queue.add('generate-pdf', { docId: 42 });
  await queue.add('fail-me', {});

  // wait for worker to process + flush interval to fire
  await new Promise((r) => setTimeout(r, 3000));

  console.log(`\nTotal events received: ${received.length}`);

  const types = received.map((e) => e.event);
  const hasFailed = types.includes('failed');
  const hasCompleted = types.includes('completed');

  console.log('failed event:', hasFailed ? '✓' : '✗');
  console.log('completed event:', hasCompleted ? '✓' : '✗');

  const hasJobNames = received.some((e) => e.jobName !== '');
  console.log('jobName populated:', hasJobNames ? '✓' : '✗');

  if (!hasFailed || !hasCompleted) {
    console.error('\nSome expected events are missing!');
    await cleanup();
    process.exit(1);
  }

  console.log('\nAll good!');
  await cleanup();
}

run().catch(async (err) => {
  console.error(err);
  await cleanup();
  process.exit(1);
});
