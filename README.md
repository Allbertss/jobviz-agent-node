# jobviz-agent

Lightweight SDK for streaming job lifecycle events from your Node.js application to [Jobviz](https://jobviz.dev) — real-time monitoring, alerting, and AI-powered debugging for background jobs.

Supports **BullMQ**, **bee-queue**, **Agenda.js**, and custom providers.

## Install

```bash
npm install jobviz-agent
```

Then install the queue library you use (if you haven't already):

```bash
# Pick one (or more)
npm install bullmq
npm install bee-queue
npm install agenda
```

## Quick Start

### BullMQ (shorthand)

```ts
import { initJobviz } from 'jobviz-agent';

initJobviz({
  apiKey: process.env.JOBVIZ_API_KEY!,
  queues: ['emails', 'reports', 'notifications'],
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
});
```

That's it. The agent subscribes to Redis pub/sub events for the listed queues and streams them to Jobviz in batches.

### BullMQ (explicit provider)

```ts
import { initJobviz, BullMQProvider } from 'jobviz-agent';

initJobviz({
  apiKey: process.env.JOBVIZ_API_KEY!,
  provider: new BullMQProvider({
    queues: ['emails', 'reports'],
    connection: { host: 'redis.internal', port: 6380, password: 'secret' },
  }),
});
```

### bee-queue

```ts
import { initJobviz, BeeQueueProvider } from 'jobviz-agent';

initJobviz({
  apiKey: process.env.JOBVIZ_API_KEY!,
  provider: new BeeQueueProvider({
    queues: ['notifications', 'sms'],
    redisUrl: process.env.REDIS_URL!,
  }),
});
```

### Agenda.js

```ts
import { initJobviz, AgendaProvider } from 'jobviz-agent';
import Agenda from 'agenda';

const agenda = new Agenda({ db: { address: process.env.MONGO_URL! } });

initJobviz({
  apiKey: process.env.JOBVIZ_API_KEY!,
  provider: new AgendaProvider({ agenda }),
});
```

### Multiple queue systems

```ts
import { initJobviz, MultiProvider, BullMQProvider, BeeQueueProvider } from 'jobviz-agent';

initJobviz({
  apiKey: process.env.JOBVIZ_API_KEY!,
  provider: new MultiProvider([
    new BullMQProvider({ queues: ['emails'], connection: { url: redisUrl } }),
    new BeeQueueProvider({ queues: ['notifications'], redisUrl }),
  ]),
});
```

## In-Job Logging

Attach structured log entries to a running job for step-by-step visibility in the Jobviz timeline:

```ts
import { jobviz } from 'jobviz-agent';
import { Worker } from 'bullmq';

const worker = new Worker('emails', async (job) => {
  jobviz.log(job, 'Fetching template');
  const tpl = await fetchTemplate(job.data.templateId);

  jobviz.log(job, 'Sending email', { recipients: job.data.to.length });
  await sendEmail(tpl, job.data);
});
```

## Deployment Tracking

Correlate deployments with job failures in the Jobviz dashboard:

```ts
import { initJobviz } from 'jobviz-agent';

const agent = initJobviz({ apiKey, queues: ['emails'], redisUrl });

agent.trackDeployment({
  version: '1.4.2',
  commitHash: 'abc123f',
  description: 'Fix email retry logic',
});
```

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `apiKey` | `string` | *required* | Your Jobviz project API key |
| `queues` | `string[]` | — | Queue names to monitor (BullMQ shorthand mode) |
| `redisUrl` | `string` | — | Redis connection URL (BullMQ shorthand mode) |
| `provider` | `QueueProvider` | — | Custom provider instance (instead of `queues`/`redisUrl`) |
| `endpoint` | `string` | `https://app.jobviz.dev` | Jobviz API endpoint |
| `env` | `'development' \| 'production'` | auto-detected | Environment tag |
| `batchSize` | `number` | `100` | Max events per HTTP batch |
| `flushInterval` | `number` | `3000` | Flush interval in ms |
| `captureInput` | `boolean` | `true` | Capture job input data (job.data) |
| `captureStackTraces` | `boolean` | `true` | Capture stack traces on failure |
| `redactKeys` | `boolean \| string[] \| RedactKeysConfig` | `false` | Redact sensitive keys from job data |
| `maxBufferSize` | `number` | `10000` | Max events buffered in memory before dropping oldest |
| `onError` | `(err, count) => void` | stderr (dev) | Called when events are dropped |
| `debug` | `boolean` | `false` | Enable verbose logging + health endpoint |
| `debugPort` | `number` | `9888` | Port for `/agent/health` endpoint |

### Internal buffering

The BullMQ provider maintains an in-memory cache of job metadata (name, traceId, parent) to avoid redundant Redis lookups. The cache uses **FIFO eviction** (oldest entries are dropped first when the 50 000-entry cap is reached) and a 30-minute TTL sweep. This is intentional — in a queue, the oldest cached jobs are the most likely to have already completed.

The event buffer holds up to `maxBufferSize` events (default 10 000) in memory. When the buffer is full, the **oldest events are dropped** to prevent unbounded memory growth.

## Privacy & Data Sanitization

By default, the agent captures job input data and error stack traces — this powers Jobviz's debugging and AI root-cause analysis features.

If your jobs handle sensitive data, you have several levels of control:

```ts
// 1. Disable input capture entirely
initJobviz({ apiKey, queues, redisUrl, captureInput: false });

// 2. Disable stack trace capture
initJobviz({ apiKey, queues, redisUrl, captureStackTraces: false });

// 3. Redact built-in sensitive keys (password, token, secret, etc.)
initJobviz({ apiKey, queues, redisUrl, redactKeys: true });

// 4. Redact custom keys (merged with built-in set)
initJobviz({ apiKey, queues, redisUrl, redactKeys: ['ssn', 'dob', 'bankAccount'] });

// 5. Fine-grained control — add and remove keys from defaults
initJobviz({
  apiKey, queues, redisUrl,
  redactKeys: {
    include: ['dob', 'bankAccount'],  // add to defaults
    exclude: ['token'],               // remove from defaults
  },
});

// 6. Use only your own keys, ignore built-in defaults
initJobviz({
  apiKey, queues, redisUrl,
  redactKeys: { only: ['mySecretField', 'internalId'] },
});
```

Built-in redacted keys: `password`, `secret`, `token`, `apiKey`, `api_key`, `authorization`, `creditCard`, `credit_card`, `ssn`, `accessToken`, `access_token`, `refreshToken`, `refresh_token`.

Key matching is **case-insensitive** — `Authorization`, `AUTHORIZATION`, and `authorization` are all redacted.

See our [Privacy Policy](https://jobviz.dev/privacy) for full details on data handling.

## Multi-Instance Usage

The `initJobviz()` / `stopJobviz()` helpers manage a global singleton. For advanced use cases (tests, multi-tenant), instantiate `JobvizAgent` directly:

```ts
import { JobvizAgent } from 'jobviz-agent';

const agent = new JobvizAgent({
  apiKey: process.env.JOBVIZ_API_KEY!,
  queues: ['emails'],
  redisUrl: process.env.REDIS_URL!,
});

agent.start();

// Later...
await agent.stop();
```

## Custom Providers

Implement the `QueueProvider` interface to monitor any queue system:

```ts
import type { QueueProvider, JobEvent } from 'jobviz-agent';

class MyQueueProvider implements QueueProvider {
  connect(push: (event: JobEvent) => void): void {
    // Subscribe to your queue system and call push() for each event
  }

  async disconnect(): Promise<void> {
    // Clean up connections
  }
}
```

## Debug Mode

Enable debug mode to troubleshoot connectivity issues:

```ts
initJobviz({
  apiKey,
  queues: ['emails'],
  redisUrl,
  debug: true,      // Verbose console logging
  debugPort: 9888,  // GET http://127.0.0.1:9888/agent/health
});
```

> **Security note:** The debug health endpoint binds to `127.0.0.1` (localhost only) and has no authentication. It is intended for local diagnostics only. In containerized or shared-host environments, other processes on the same network namespace may be able to reach it. Do not enable `debug: true` in production unless you understand the exposure.

## Graceful Shutdown

The agent does **not** register signal handlers automatically — your application owns its own lifecycle. Wire up `SIGTERM` / `SIGINT` to flush remaining events before exiting:

```ts
import { stopJobviz } from 'jobviz-agent';

async function shutdown() {
  await stopJobviz(); // Flushes remaining events before exiting
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
```

> **Tip:** In Kubernetes or Docker, the default `SIGTERM` grace period is 30 seconds. `stopJobviz()` flushes the in-memory buffer and closes provider connections — typically under 1 second.

## Requirements

- Node.js >= 18
- ESM only (`import` syntax) — CommonJS `require()` is not supported
- One of: BullMQ 5+, bee-queue 1.7+, or Agenda 5+

## License

[MIT](LICENSE)
