package com.questdb.cairo.pool;

import java.io.Closeable;

@FunctionalInterface
public interface ResourcePool<T extends Closeable> {
    T get(CharSequence name);
}
