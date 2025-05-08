package io.questdb.mp;

/**
 * Interface for concurrent unbounded MPMC queue.
 */
public interface Queue<T> {

    void enqueue(T item);

    boolean tryDequeue(T result);
}
