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

package io.questdb.test.tools;

import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * A per-query memory tracker with a fixed limit, for tests that need to fail one specific
 * native allocation. The limit selects the failing allocation byte-exactly - the check is
 * {@code used + size > limit} - which the global RSS ceiling cannot do as reliably, since it
 * is process-wide and other threads move it.
 */
public final class LimitedMemoryTracker extends MemoryTracker {
    private long nativeAddress;

    public LimitedMemoryTracker(long limitBytes) {
        nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
        Vect.memset(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, 0);
        Unsafe.putLong(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
    }

    @Override
    public void close() {
        if (nativeAddress != 0) {
            freeNativeAllocators();
            nativeAddress = Unsafe.free(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
        }
    }

    @Override
    public long getLimit() {
        return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
    }

    @Override
    public long getQueryId() {
        return 1;
    }

    @Override
    public long getUsed() {
        return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET);
    }

    @Override
    public MemoryTrackerWorkload getWorkload() {
        return MemoryTrackerWorkload.QUERY;
    }

    @Override
    public long nativeAddress() {
        return nativeAddress;
    }

    /**
     * Raises or lowers the limit in place, so a test can lift it after asserting a breach and
     * then reuse the same tracker for the recovery leg.
     */
    public void setLimit(long limitBytes) {
        Unsafe.putLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
    }
}
