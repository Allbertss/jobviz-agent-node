# Changelog

## 0.1.0 (2026-03-08)

Initial release.

- **BullMQ** provider with job metadata caching and Redis pub/sub
- **bee-queue** provider (watcher mode)
- **Agenda.js** provider (MongoDB-backed)
- **MultiProvider** for monitoring multiple queue systems simultaneously
- Event batching with configurable batch size and flush interval
- Retry strategy with exponential backoff (no retry on 4xx)
- In-job logging via `jobviz.log()`
- Deployment tracking via `agent.trackDeployment()`
- Privacy controls: `captureInput`, `captureStackTraces`, `redactKeys`
- Debug mode with local `/agent/health` endpoint
- Singleton (`initJobviz`) and multi-instance (`new JobvizAgent`) modes
