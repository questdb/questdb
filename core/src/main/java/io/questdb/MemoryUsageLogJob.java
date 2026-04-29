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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class MemoryUsageLogJob extends SynchronizedJob {
    private static final Log LOG = LogFactory.getLog(MemoryUsageLogJob.class);

    private final MicrosecondClock clock;
    private final long intervalMicros;
    private long nextRunMicros;

    public MemoryUsageLogJob(@NotNull MicrosecondClock clock, long intervalMillis) {
        this.clock = clock;
        this.intervalMicros = intervalMillis * 1000;
        this.nextRunMicros = clock.getTicks();
    }

    public static void appendMemoryUsage(@NotNull CharSink<?> sink) {
        final Runtime runtime = Runtime.getRuntime();
        final long heapCommitted = runtime.totalMemory();
        final long heapUsed = heapCommitted - runtime.freeMemory();
        final long memAccounted = Unsafe.getMemUsed();
        final long memRssAccounted = Unsafe.getRssMemUsed();

        sink.putAscii("mem.accounted=").put(memAccounted)
                .putAscii(", mem.rss.accounted=").put(memRssAccounted)
                .putAscii(", mem.non.rss.accounted=").put(memAccounted - memRssAccounted)
                .putAscii(", mem.rss.limit=").put(Unsafe.getRssMemLimit())
                .putAscii(", rss.physical=").put(Os.getRss())
                .putAscii(", jvm.heap.used=").put(heapUsed)
                .putAscii(", jvm.heap.committed=").put(heapCommitted)
                .putAscii(", jvm.heap.max=").put(runtime.maxMemory())
                .putAscii(", malloc.count=").put(Unsafe.getMallocCount())
                .putAscii(", realloc.count=").put(Unsafe.getReallocCount())
                .putAscii(", free.count=").put(Unsafe.getFreeCount())
                .putAscii(", tags=[");

        boolean tagWritten = false;
        for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
            final long tagValue = Unsafe.getMemUsedByTag(i);
            if (tagValue != 0) {
                if (tagWritten) {
                    sink.putAscii(", ");
                }
                sink.putAscii(MemoryTag.nameOf(i)).putAscii('=').put(tagValue);
                tagWritten = true;
            }
        }
        sink.putAscii(']');
    }

    @Override
    protected boolean runSerially() {
        final long now = clock.getTicks();
        if (now < nextRunMicros) {
            return false;
        }
        nextRunMicros = now + intervalMicros;
        final LogRecord record = LOG.info();
        if (record.isEnabled()) {
            record.$("memory usage [");
            appendMemoryUsage(record);
            record.I$();
        }
        return true;
    }
}
