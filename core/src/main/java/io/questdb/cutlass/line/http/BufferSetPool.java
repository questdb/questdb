/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.line.http;

import io.questdb.cutlass.line.tcp.v4.IlpV4TableBuffer;
import io.questdb.std.ObjList;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe pool for reusing buffer sets after async flush completes.
 * <p>
 * This pool reduces GC pressure in high-throughput scenarios by reusing
 * Map and ObjList instances. The IlpV4TableBuffer instances themselves
 * are table-name-specific and can be reused via reset() when table names
 * match.
 * <p>
 * Thread safety: All methods are thread-safe. The pool uses a
 * ConcurrentLinkedQueue for lock-free access.
 */
public class BufferSetPool implements Closeable {

    /**
     * Represents a reusable set of buffers.
     */
    public static class BufferSet {
        private final Map<String, IlpV4TableBuffer> tableBuffers;
        private final ObjList<String> tableOrder;

        public BufferSet() {
            this.tableBuffers = new HashMap<>();
            this.tableOrder = new ObjList<>();
        }

        public BufferSet(Map<String, IlpV4TableBuffer> tableBuffers, ObjList<String> tableOrder) {
            this.tableBuffers = tableBuffers;
            this.tableOrder = tableOrder;
        }

        public Map<String, IlpV4TableBuffer> getTableBuffers() {
            return tableBuffers;
        }

        public ObjList<String> getTableOrder() {
            return tableOrder;
        }

        /**
         * Resets all buffers for reuse (keeps column definitions).
         */
        public void reset() {
            for (int i = 0, n = tableOrder.size(); i < n; i++) {
                String tableName = tableOrder.get(i);
                IlpV4TableBuffer buffer = tableBuffers.get(tableName);
                if (buffer != null) {
                    buffer.reset();
                }
            }
        }

        /**
         * Clears completely (removes all tables and column definitions).
         */
        public void clear() {
            for (int i = 0, n = tableOrder.size(); i < n; i++) {
                String tableName = tableOrder.get(i);
                IlpV4TableBuffer buffer = tableBuffers.get(tableName);
                if (buffer != null) {
                    buffer.clear();
                }
            }
            tableBuffers.clear();
            tableOrder.clear();
        }

        /**
         * Returns true if this buffer set is empty (no tables).
         */
        public boolean isEmpty() {
            return tableOrder.size() == 0;
        }

        /**
         * Returns the number of tables in this buffer set.
         */
        public int getTableCount() {
            return tableOrder.size();
        }
    }

    private final ConcurrentLinkedQueue<BufferSet> pool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger poolSize = new AtomicInteger(0);
    private final int maxPoolSize;
    private volatile boolean closed = false;

    /**
     * Creates a new buffer pool with the specified maximum size.
     *
     * @param maxPoolSize maximum number of buffer sets to keep in the pool
     */
    public BufferSetPool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Acquires a buffer set from the pool, or creates a new one if none available.
     * The returned buffer set is cleared and ready for use.
     *
     * @return a buffer set ready for use
     */
    public BufferSet acquire() {
        if (closed) {
            return new BufferSet();
        }

        BufferSet set = pool.poll();
        if (set == null) {
            set = new BufferSet();
        } else {
            // Decrement size counter when taking from pool
            poolSize.decrementAndGet();
            // Clear the set for fresh use
            set.clear();
        }
        return set;
    }

    /**
     * Returns a buffer set to the pool for reuse.
     * If the pool is full or closed, the buffer set is discarded.
     *
     * @param set the buffer set to return (will be reset before storing)
     */
    public void release(BufferSet set) {
        if (set == null || closed) {
            return;
        }

        // Reset for reuse
        set.reset();

        // Try to atomically reserve a slot in the pool
        int currentSize;
        do {
            currentSize = poolSize.get();
            if (currentSize >= maxPoolSize) {
                // Pool is full, discard (will be GC'd)
                return;
            }
        } while (!poolSize.compareAndSet(currentSize, currentSize + 1));

        // Successfully reserved a slot, add to pool
        pool.offer(set);
    }

    /**
     * Returns a PendingFlush's buffers to the pool.
     * This is a convenience method that extracts the buffers from a PendingFlush.
     *
     * @param pendingFlush the completed flush whose buffers should be returned
     */
    public void releaseFromPendingFlush(PendingFlush pendingFlush) {
        if (pendingFlush == null || closed) {
            return;
        }

        BufferSet set = new BufferSet(
                pendingFlush.getTableBuffers(),
                pendingFlush.getTableOrder()
        );
        release(set);
    }

    /**
     * Returns the current number of buffer sets in the pool.
     */
    public int size() {
        return poolSize.get();
    }

    /**
     * Returns true if the pool is closed.
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        pool.clear();
        poolSize.set(0);
    }
}
