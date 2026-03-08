import { createServer, type Server } from 'node:http';
import { AGENT_VERSION } from './version.js';

export interface DebugServerStatus {
  config: Record<string, unknown>;
  stats: ReturnType<DebugServerStatusFn>;
}

type DebugServerStatusFn = () => Record<string, unknown>;

const MAX_PORT_RETRIES = 3;

export class DebugServer {
  private server: Server | null = null;
  private startedAt: number = Date.now();
  private portRetries = 0;

  constructor(
    private readonly port: number,
    private readonly getConfig: () => Record<string, unknown>,
    private readonly getStats: () => Record<string, unknown>,
  ) {}

  start(): void {
    this.startedAt = Date.now();

    this.server = createServer((req, res) => {
      if (req.method !== 'GET') {
        res.writeHead(405);
        res.end('Method Not Allowed');
        return;
      }
      if (req.url === '/agent/health') {
        const body = JSON.stringify(
          {
            agent_version: AGENT_VERSION,
            uptime_seconds: Math.floor((Date.now() - this.startedAt) / 1000),
            config: this.getConfig(),
            stats: this.getStats(),
          },
          null,
          2,
        );
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(body);
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });

    this.server.timeout = 5_000;
    this.server.listen(this.port, '127.0.0.1', () => {
      console.log(
        `[jobviz:debug] health endpoint at http://127.0.0.1:${this.port}/agent/health`,
      );
    });

    this.server.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'EADDRINUSE' && this.portRetries < MAX_PORT_RETRIES) {
        this.portRetries++;
        const nextPort = this.port + this.portRetries;
        console.warn(
          `[jobviz:debug] port ${nextPort - 1} in use, trying ${nextPort}`,
        );
        this.server?.listen(nextPort, '127.0.0.1');
      } else if (err.code === 'EADDRINUSE') {
        console.error(
          `[jobviz:debug] could not find an open port after ${MAX_PORT_RETRIES} retries`,
        );
      } else {
        console.error(`[jobviz:debug] health server error: ${err.message}`);
      }
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.server) return resolve();
      this.server.close(() => resolve());
    });
  }
}
