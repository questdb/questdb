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

package io.questdb;

import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Not thread-safe: {@link #format(CharSink)} reuses internal scratch arrays
 * across calls. Drive each instance from a single thread (the production caller
 * is a {@link io.questdb.mp.SynchronizedJob} that already serializes entry).
 */
public final class MemoryUsageFormatter {
    public static final int MAX_LOGGED_TAGS = 10;

    private final int[] tagIndices = new int[MemoryTag.SIZE];
    private final long[] tagValues = new long[MemoryTag.SIZE];

    // Exposed for testing the sort + emit logic in isolation.
    public static void appendTopTagsByAbsoluteValue(
            @NotNull CharSink<?> sink,
            int[] tagIndices,
            long[] tagValues,
            int count,
            int maxTags
    ) {
        // Partial selection sort: bring top-N by absolute value to the front.
        final int limit = Math.min(maxTags, count);
        for (int k = 0; k < limit; k++) {
            int maxAt = k;
            long maxAbs = Math.abs(tagValues[k]);
            for (int j = k + 1; j < count; j++) {
                final long abs = Math.abs(tagValues[j]);
                if (abs > maxAbs) {
                    maxAbs = abs;
                    maxAt = j;
                }
            }
            if (maxAt != k) {
                final int tmpIdx = tagIndices[k];
                tagIndices[k] = tagIndices[maxAt];
                tagIndices[maxAt] = tmpIdx;
                final long tmpVal = tagValues[k];
                tagValues[k] = tagValues[maxAt];
                tagValues[maxAt] = tmpVal;
            }
            if (k > 0) {
                sink.putAscii(", ");
            }
            sink.putAscii(MemoryTag.nameOf(tagIndices[k])).putAscii('=').put(tagValues[k]);
        }
        if (count > limit) {
            sink.putAscii(", and ").put(count - limit).putAscii(" more");
        }
    }

    public void format(@NotNull CharSink<?> sink) {
        final Runtime runtime = Runtime.getRuntime();
        final long heapCommitted = runtime.totalMemory();
        final long heapUsed = heapCommitted - runtime.freeMemory();
        final long memRssAccounted = Unsafe.getRssMemUsed();
        final long memNonRssAccounted = Unsafe.getNonRssMemUsed();
        final long memAccounted = memRssAccounted + memNonRssAccounted;

        sink.putAscii("mem.accounted=").put(memAccounted)
                .putAscii(", mem.rss.accounted=").put(memRssAccounted)
                .putAscii(", mem.non.rss.accounted=").put(memNonRssAccounted)
                .putAscii(", mem.rss.limit=").put(Unsafe.getRssMemLimit())
                .putAscii(", rss.physical=").put(Os.getRss())
                .putAscii(", jvm.heap.used=").put(heapUsed)
                .putAscii(", jvm.heap.committed=").put(heapCommitted)
                .putAscii(", jvm.heap.max=").put(runtime.maxMemory())
                .putAscii(", malloc.count=").put(Unsafe.getMallocCount())
                .putAscii(", realloc.count=").put(Unsafe.getReallocCount())
                .putAscii(", free.count=").put(Unsafe.getFreeCount())
                .putAscii(", tags=[");

        int count = 0;
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            final long tagValue = Unsafe.getMemUsedByTag(i);
            if (tagValue != 0) {
                tagIndices[count] = i;
                tagValues[count] = tagValue;
                count++;
            }
        }
        appendTopTagsByAbsoluteValue(sink, tagIndices, tagValues, count, MAX_LOGGED_TAGS);
        sink.putAscii(']');
    }
}
