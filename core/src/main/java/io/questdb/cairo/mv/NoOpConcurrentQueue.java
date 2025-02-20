package io.questdb.cairo.mv;

import io.questdb.mp.Queue;
import io.questdb.mp.ValueHolder;

// used in MatViewGraph to switch off notification for a replica node
public class NoOpConcurrentQueue<T extends ValueHolder<T>> implements Queue<T> {
    @Override
    public void enqueue(T item) {
    }

    @Override
    public boolean tryDequeue(T result) {
        return false;
    }
}
