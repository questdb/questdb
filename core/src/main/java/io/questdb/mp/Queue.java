package io.questdb.mp;

public interface Queue<T> {
    void enqueue(T item);

    boolean tryDequeue(T result);
}
