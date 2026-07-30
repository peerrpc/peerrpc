/**
 * AsyncQueue: a FIFO queue with async recv().
 *
 * push() delivers an item to a pending recv() or buffers it. close()
 * resolves all pending recv() with null (EOF). After close, recv()
 * returns null immediately once buffered items are drained.
 *
 * Replaces the setTimeout(poll, 1) busy-polling pattern used in
 * ServerStream.recv / ClientStream.recv / collectUnary, which spun at
 * 1kHz and leaked promises + timers when the channel closed.
 */

export class AsyncQueue<T> {
  private items: T[] = [];
  private resolvers: ((v: T | null) => void)[] = [];
  private closed = false;

  /** Enqueue one item, or hand it to a waiting recv(). */
  push(item: T): void {
    if (this.closed) return;
    if (this.resolvers.length > 0) {
      this.resolvers.shift()!(item);
    } else {
      this.items.push(item);
    }
  }

  /**
   * Signal that no more items will arrive. All pending recv() callers
   * resolve with null (EOF) once buffered items are drained.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    // Resolvers waiting for items beyond the buffer get EOF.
    while (this.resolvers.length > this.items.length) {
      this.resolvers.pop()!(null);
    }
  }

  /** Dequeue the next item, or null at EOF. */
  async recv(): Promise<T | null> {
    if (this.items.length > 0) {
      return this.items.shift()!;
    }
    if (this.closed) {
      return null;
    }
    return new Promise((resolve) => {
      this.resolvers.push(resolve);
    });
  }
}
