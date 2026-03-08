import type { JobEvent } from './buffer.js';

interface ErrorEntry {
  at: number;
  message: string;
}

export class DebugLogger {
  private eventsSent = 0;
  private eventsDropped = 0;
  private lastFlushAt: number | null = null;
  private errors: ErrorEntry[] = [];

  logEvent(event: JobEvent): void {
    console.log(
      `[jobviz:debug] event: ${event.event} job=${event.jobId} queue=${event.queue} name=${event.jobName}`,
    );
  }

  logFlush(count: number): void {
    this.eventsSent += count;
    this.lastFlushAt = Date.now();
    console.log(
      `[jobviz:debug] flushed ${count} events (total sent: ${this.eventsSent})`,
    );
  }

  logDrop(err: Error, count: number): void {
    this.eventsDropped += count;
    this.pushError(err.message);
    console.error(`[jobviz:debug] dropped ${count} events: ${err.message}`);
  }

  logConnectionIssue(message: string): void {
    this.pushError(message);
    console.warn(`[jobviz:debug] connection: ${message}`);
  }

  getStats() {
    return {
      events_sent: this.eventsSent,
      events_dropped: this.eventsDropped,
      last_flush_at: this.lastFlushAt
        ? new Date(this.lastFlushAt).toISOString()
        : null,
      recent_errors: this.errors.slice(-10),
    };
  }

  private pushError(message: string): void {
    this.errors.push({ at: Date.now(), message });
    if (this.errors.length > 50) this.errors.shift();
  }
}
