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

import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;

/**
 * A per-query memory tracker with a fixed limit, for tests that need to fail one specific
 * native allocation. The limit selects the failing allocation byte-exactly - the check is
 * {@code used + size > limit} - which the global RSS ceiling cannot do as reliably, since it
 * is process-wide and other threads move it.
 */
public final class LimitedMemoryTracker extends MemoryTracker {

    public LimitedMemoryTracker(long limitBytes) {
        setLimit(limitBytes);
    }

    @Override
    public void close() {
        if (nativeAddress() != 0) {
            destroyNativeBlock();
        }
    }

    @Override
    public long getQueryId() {
        return 1;
    }

    @Override
    public MemoryTrackerWorkload getWorkload() {
        return MemoryTrackerWorkload.QUERY;
    }

    /**
     * Raises or lowers the limit in place, so a test can lift it after asserting a breach and
     * then reuse the same tracker for the recovery leg.
     */
    public void setLimit(long limitBytes) {
        Unsafe.putLongVolatile(nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
    }
}
