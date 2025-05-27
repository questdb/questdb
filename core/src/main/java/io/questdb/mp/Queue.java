package io.questdb.mp;

import io.questdb.std.Mutable;

/**
 * Interface for concurrent unbounded MPMC queue.
 */
public interface Queue<T> extends Mutable {

    void enqueue(T item);

    boolean tryDequeue(T result);
}
