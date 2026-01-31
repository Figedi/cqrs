import { Observable } from "rxjs"

import type { IPersistedEvent } from "../types.js"

/** Event emitted through the stream */
export interface IStreamEvent {
  event: IPersistedEvent
  status: "PROCESSED" | "FAILED" | "ABORTED"
  error?: Error
}

/**
 * Controller for managing an AsyncIterator-based event stream.
 *
 * Provides a way to push events to consumers and control the stream lifecycle.
 * Events are only emitted after completion (success or failure after retries exhausted).
 */
export class StreamController {
  private queue: IStreamEvent[] = []
  private resolvers: Array<(value: IteratorResult<IStreamEvent, void>) => void> = []
  private closed = false
  private error: Error | null = null

  /**
   * Push an event to the stream.
   * Events are delivered to waiting consumers or queued for later.
   *
   * @param event - The stream event to push
   */
  push(event: IStreamEvent): void {
    if (this.closed) {
      return
    }

    const resolver = this.resolvers.shift()
    if (resolver) {
      resolver({ value: event, done: false })
    } else {
      this.queue.push(event)
    }
  }

  /**
   * Close the stream gracefully.
   * Pending consumers will receive done=true.
   */
  close(): void {
    if (this.closed) {
      return
    }
    this.closed = true

    // Resolve all pending consumers with done=true
    for (const resolver of this.resolvers) {
      resolver({ value: undefined, done: true })
    }
    this.resolvers = []
  }

  /**
   * Close the stream with an error.
   * Pending consumers will receive the error.
   *
   * @param error - The error that caused the stream to close
   */
  abort(error: Error): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.error = error

    // Reject all pending consumers
    // Note: Since we store resolvers, we'll throw on next() instead
    this.resolvers = []
  }

  /**
   * Check if the stream is closed.
   */
  isClosed(): boolean {
    return this.closed
  }

  /**
   * Get the AsyncIterator for consuming events.
   */
  getIterator(): AsyncIterableIterator<IStreamEvent> {
    const self = this

    return {
      [Symbol.asyncIterator]() {
        return this
      },

      async next(): Promise<IteratorResult<IStreamEvent, void>> {
        // Check for error first
        if (self.error) {
          throw self.error
        }

        // Return queued event if available
        const queued = self.queue.shift()
        if (queued) {
          return { value: queued, done: false }
        }

        // If closed and no queued events, we're done
        if (self.closed) {
          return { value: undefined, done: true }
        }

        // Wait for next event
        return new Promise(resolve => {
          self.resolvers.push(resolve)
        })
      },

      async return(): Promise<IteratorResult<IStreamEvent, void>> {
        self.close()
        return { value: undefined, done: true }
      },

      async throw(error?: Error): Promise<IteratorResult<IStreamEvent, void>> {
        const err = error || new Error("Stream aborted")
        self.abort(err)
        throw err
      },
    }
  }

  /**
   * Convert the stream to an RxJS Observable.
   * Convenience method for users who prefer RxJS.
   */
  stream$(): Observable<IStreamEvent> {
    const iterator = this.getIterator()

    return new Observable<IStreamEvent>(subscriber => {
      const consume = async () => {
        try {
          for await (const event of { [Symbol.asyncIterator]: () => iterator }) {
            if (subscriber.closed) {
              break
            }
            subscriber.next(event)
          }
          subscriber.complete()
        } catch (error) {
          subscriber.error(error)
        }
      }

      consume()

      return () => {
        iterator.return?.()
      }
    })
  }
}

/**
 * Create a new StreamController.
 */
export function createStreamController(): StreamController {
  return new StreamController()
}
