import type { JobEvent } from '../buffer.js';

/**
 * Interface all queue providers must implement.
 *
 * A provider is responsible for listening to a queue system and pushing
 * normalised JobEvents into the agent's event buffer.
 *
 * To write your own provider:
 *
 *   import type { QueueProvider } from "jobviz-agent"
 *   import type { JobEvent }      from "jobviz-agent"
 *
 *   export class MyProvider implements QueueProvider {
 *     private push!: (event: JobEvent) => void
 *
 *     connect(push: (event: JobEvent) => void): void {
 *       this.push = push
 *       // subscribe to your queue system here and call push() for every event
 *     }
 *
 *     async disconnect(): Promise<void> {
 *       // clean up connections / timers
 *     }
 *   }
 *
 * Then pass it to initJobviz:
 *
 *   initJobviz({ apiKey, provider: new MyProvider() })
 */
export interface QueueProvider {
  /**
   * Start listening.  The agent supplies the `push` callback; call it once
   * per job event.  Must not throw — emit errors via `push` or log them.
   */
  connect(push: (event: JobEvent) => void): void | Promise<void>;

  /** Stop listening and release all resources. */
  disconnect(): Promise<void>;
}
