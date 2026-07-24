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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The engine-wide ceiling every {@link LiveViewCheckpointPageCache} allocates
 * against. One budget instance serves all live views, so N views cannot each
 * take the configured cap; a view that warms first simply gets more of it, and
 * the caches that lose the race keep serving misses rather than failing.
 * <p>
 * The counter is atomic because caches on different refresh workers acquire and
 * release concurrently. Only slab allocation and release touch it - a page probe
 * or admission into an already-owned slab does not - so the contention is per
 * {@link LiveViewCheckpointPageCache#SLAB_BYTES} of cache growth, not per page.
 */
public class LiveViewCheckpointPageCacheBudget {

    private final long capacityBytes;
    private final AtomicLong usedBytes = new AtomicLong();

    /**
     * @param capacityBytes the bytes all caches may hold together. Zero, or any
     *                      negative value, disables caching outright
     */
    public LiveViewCheckpointPageCacheBudget(long capacityBytes) {
        this.capacityBytes = Math.max(0, capacityBytes);
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public long getUsedBytes() {
        return usedBytes.get();
    }

    public boolean isEnabled() {
        return capacityBytes > 0;
    }

    /**
     * Gives {@code bytes} back to the budget. Releasing more than was acquired is
     * a wiring error the caches must not make, so it raises rather than letting
     * the counter drift negative and hand out capacity that does not exist.
     */
    public void release(long bytes) {
        if (bytes < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint page cache budget release is negative [bytes=").put(bytes).put(']');
        }
        final long remaining = usedBytes.addAndGet(-bytes);
        if (remaining < 0) {
            usedBytes.addAndGet(bytes);
            throw CairoException.critical(0)
                    .put("live view checkpoint page cache budget released more than acquired")
                    .put(" [bytes=").put(bytes).put(", used=").put(remaining + bytes).put(']');
        }
    }

    /**
     * @return true when {@code bytes} fit under the cap and are now charged to the
     * caller, false when the budget is exhausted. A refusal is not an error: the
     * cache stops admitting and keeps serving what it already holds.
     */
    public boolean tryAcquire(long bytes) {
        if (bytes <= 0) {
            return false;
        }
        long used = usedBytes.get();
        while (used + bytes <= capacityBytes) {
            if (usedBytes.compareAndSet(used, used + bytes)) {
                return true;
            }
            used = usedBytes.get();
        }
        return false;
    }
}
