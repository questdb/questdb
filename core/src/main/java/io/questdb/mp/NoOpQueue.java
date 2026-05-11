package io.questdb.mp;

public class NoOpQueue<T> implements Queue<T> {

    @Override
    public void clear() {
    }

    @Override
    public void enqueue(T item) {
    }

    @Override
    public boolean tryDequeue(T result) {
        return false;
    }
}
