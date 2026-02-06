/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Fixed pool of reusable native buffers for io_uring WAL writes.
 * <p>
 * Each buffer is {@code bufferSize} bytes, allocated via {@link Unsafe#malloc}.
 * Buffers are tracked by index and managed with a free-stack for O(1) acquire/release.
 * <p>
 * When the pool is exhausted, {@link #acquire()} applies backpressure by draining
 * io_uring CQEs (which releases completed swap-write buffers back to the pool).
 * <p>
 * Not thread-safe; intended for a single WAL writer thread.
 */
public class WalWriterBufferPool implements Closeable {

    private static final Log LOG = LogFactory.getLog(WalWriterBufferPool.class);
    private long[] addresses;
    private final long bufferSize;
    private int capacity;
    private int freeCount;
    private int[] freeStack;
    private final int memoryTag;
    private boolean registered;
    private WalWriterRingManager ringManager;

    public WalWriterBufferPool(long bufferSize, int capacity, int memoryTag) {
        this.bufferSize = bufferSize;
        this.memoryTag = memoryTag;
        this.capacity = capacity;
        this.addresses = new long[capacity];
        this.freeStack = new int[capacity];
        this.freeCount = capacity;
        for (int i = 0; i < capacity; i++) {
            addresses[i] = Unsafe.malloc(bufferSize, memoryTag);
            freeStack[i] = i;
        }
    }

    /**
     * Acquire a buffer from the pool. Returns the buffer index.
     * Use {@link #address(int)} to get the native address.
     * <p>
     * If the pool is empty, drains CQEs via the ring manager (backpressure).
     * If still empty after drain, grows the pool to accommodate more buffers.
     */
    public int acquire() {
        if (freeCount > 0) {
            return freeStack[--freeCount];
        }
        // Backpressure: drain CQEs to reclaim swap-write buffers.
        if (ringManager != null) {
            ringManager.drainCqes();
            if (freeCount > 0) {
                return freeStack[--freeCount];
            }
            ringManager.submitAndDrainAll();
            if (freeCount > 0) {
                return freeStack[--freeCount];
            }
            ringManager.waitForAll();
            if (freeCount > 0) {
                return freeStack[--freeCount];
            }
        }
        // All buffers are live (page buffers, not in-flight). Grow the pool.
        grow(capacity + Math.max(16, capacity / 2));
        return freeStack[--freeCount];
    }

    public long address(int bufferIndex) {
        return addresses[bufferIndex];
    }

    @Override
    public void close() {
        unregisterFromKernel();
        for (int i = 0; i < capacity; i++) {
            if (addresses[i] != 0) {
                Unsafe.free(addresses[i], bufferSize, memoryTag);
                addresses[i] = 0;
            }
        }
        freeCount = 0;
    }

    public long getBufferSize() {
        return bufferSize;
    }

    public int getFreeCount() {
        return freeCount;
    }

    /**
     * Grow the pool to accommodate more columns.
     * Existing buffers are preserved; new buffers are added to the free stack.
     */
    public void grow(int newCapacity) {
        if (newCapacity <= capacity) {
            return;
        }
        unregisterFromKernel();
        long[] newAddresses = new long[newCapacity];
        int[] newFreeStack = new int[newCapacity];
        System.arraycopy(addresses, 0, newAddresses, 0, capacity);
        System.arraycopy(freeStack, 0, newFreeStack, 0, freeCount);
        // Allocate new buffers and push them onto the free stack.
        for (int i = capacity; i < newCapacity; i++) {
            newAddresses[i] = Unsafe.malloc(bufferSize, memoryTag);
            newFreeStack[freeCount++] = i;
        }
        addresses = newAddresses;
        freeStack = newFreeStack;
        capacity = newCapacity;
        registerWithKernel();
    }

    public boolean isRegistered() {
        return registered;
    }

    /**
     * Build a native struct iovec[] array and register all pool buffers with the kernel.
     * sizeof(struct iovec) = 16 bytes on 64-bit Linux (8 for iov_base + 8 for iov_len).
     */
    public void registerWithKernel() {
        if (ringManager == null) {
            return;
        }
        long iovecSize = 16L;
        long arraySize = iovecSize * capacity;
        long iovsAddr = Unsafe.malloc(arraySize, memoryTag);
        try {
            for (int i = 0; i < capacity; i++) {
                long entryAddr = iovsAddr + i * iovecSize;
                Unsafe.getUnsafe().putLong(entryAddr, addresses[i]);
                Unsafe.getUnsafe().putLong(entryAddr + 8, bufferSize);
            }
            int ret = ringManager.registerBuffers(iovsAddr, capacity);
            if (ret < 0) {
                LOG.info().$("io_uring_register_buffers failed, falling back to IORING_OP_WRITE [errno=").$(- ret)
                        .$(", buffers=").$(capacity)
                        .$(", bufferSize=").$(bufferSize)
                        .I$();
                return;
            }
            registered = true;
        } finally {
            Unsafe.free(iovsAddr, arraySize, memoryTag);
        }
    }

    /**
     * Release a buffer back to the pool by index. O(1).
     */
    public void release(int bufferIndex) {
        freeStack[freeCount++] = bufferIndex;
    }

    /**
     * Release a buffer by its native address. O(n) scan — only used on close/evict paths.
     */
    public void releaseByAddress(long address) {
        if (address == 0) {
            return;
        }
        for (int i = 0; i < capacity; i++) {
            if (addresses[i] == address) {
                release(i);
                return;
            }
        }
        // Address not found in pool — it may have been allocated outside the pool
        // (e.g., via preadPage before pool integration). Free it directly.
        Unsafe.free(address, bufferSize, memoryTag);
    }

    public void setRingManager(WalWriterRingManager ringManager) {
        this.ringManager = ringManager;
    }

    public void unregisterFromKernel() {
        if (registered && ringManager != null) {
            ringManager.unregisterBuffers();
            registered = false;
        }
    }
}
