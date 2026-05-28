/*+*****************************************************************************
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

package io.questdb.std;

import java.io.Closeable;

/**
 * Tracks the native memory charged to a single bounded workload (a user SQL
 * query, a materialized view refresh attempt, or a WAL apply batch). Wraps a
 * 16-byte native {@code {used, limit}} block shared with Rust.
 * <p>
 * Concrete trackers are obtained from a {@link MemoryTrackerProvider} at
 * workload start and must be {@link #close() closed} exactly once at workload
 * end. The OSS implementation pools the Java skeleton and reuses the native
 * block across acquisitions so the steady state performs no native alloc/free
 * per workload invocation.
 */
public abstract class MemoryTracker implements Closeable {

    // Sparse cache of per-tag Rust-side QdbAllocator pointers pre-bound to
    // this tracker. Indexed by `memoryTag - NATIVE_DEFAULT`. Allocated lazily
    // by `getOrCreateNativeAllocator` and freed by `freeNativeAllocators`.
    private final long[] nativeAllocators = new long[MemoryTag.SIZE - MemoryTag.NATIVE_DEFAULT];

    @Override
    public abstract void close();

    /**
     * @return the configured byte limit for this tracker; {@code 0} means
     * unlimited.
     */
    public abstract long getLimit();

    /**
     * @return the workload identifier supplied at acquisition time. Used for
     * error reporting.
     */
    public abstract long getQueryId();

    /**
     * @return current bytes charged against this tracker. The reader sees the
     * latest committed value; the counter is updated by allocation sites and
     * may transiently exceed {@link #getLimit()} under concurrency.
     */
    public abstract long getUsed();

    /**
     * @return the workload class this tracker is bound to.
     */
    public abstract MemoryTrackerWorkload getWorkload();

    /**
     * @return the pointer to the native {@code {used, limit}} block. Used to
     * hand off the tracker to Rust via the tracker-aware
     * {@code Unsafe.getNativeAllocator(tag, tracker)} overload.
     */
    public abstract long nativeAddress();

    /**
     * Releases every per-tag Rust-side QdbAllocator block this tracker has
     * handed out. Called by the owning provider when the tracker is finally
     * disposed (not on pool return).
     */
    protected final void freeNativeAllocators() {
        for (int i = 0; i < nativeAllocators.length; i++) {
            if (nativeAllocators[i] != 0) {
                Unsafe.freeTrackerNativeAllocator(nativeAllocators[i]);
                nativeAllocators[i] = 0;
            }
        }
    }

    /**
     * Returns a Rust-side QdbAllocator pointer bound to this tracker for the
     * given memory tag. The first call for a tag allocates the underlying
     * block; subsequent calls return the cached pointer. Synchronized because
     * multiple worker threads sharing the tracker may race on the first call
     * for a given tag.
     */
    final synchronized long getOrCreateNativeAllocator(int memoryTag) {
        assert memoryTag >= MemoryTag.NATIVE_DEFAULT;
        final int idx = memoryTag - MemoryTag.NATIVE_DEFAULT;
        long addr = nativeAllocators[idx];
        if (addr == 0) {
            addr = Unsafe.constructTrackerNativeAllocator(this, memoryTag);
            nativeAllocators[idx] = addr;
        }
        return addr;
    }
}
