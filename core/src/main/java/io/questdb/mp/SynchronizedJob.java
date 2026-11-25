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

package io.questdb.mp;

import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public abstract class SynchronizedJob extends SynchronizedJobRhsPadding implements Job {
    private static final long LOCKED_OFFSET = Unsafe.getFieldOffset(SynchronizedJobLock.class, "locked");

    @Override
    public boolean run(@NotNull WorkerContext workerContext) {
        if (tryAcquireRunLock()) {
            try {
                return runSerially();
            } finally {
                releaseRunLock();
            }
        }
        return false;
    }

    protected final void acquireRunLock() {
        while (!tryAcquireRunLock()) {
            Os.pause();
        }
    }

    protected final void releaseRunLock() {
        locked = 0;
    }

    protected abstract boolean runSerially();

    // A worker that reads the lock as held fails on its shared copy of the cache line, without
    // taking the line exclusively for a CAS that would fail.
    private boolean tryAcquireRunLock() {
        return locked == 0 && Unsafe.cas(this, LOCKED_OFFSET, 0L, 1L);
    }
}

// The workers poll the lock word, so SynchronizedJobLhsPadding and SynchronizedJobRhsPadding give it
// 128 bytes on each side, keeping it off the cache lines the lock holder writes. 128 bytes covers
// CPUs with 128-byte lines and CPUs that prefetch lines in adjacent pairs, wherever the 8-byte-aligned
// object lands. HotSpot lays out superclass fields before subclass fields, which puts the padding
// classes around the lock word.
@SuppressWarnings("unused")
abstract class SynchronizedJobLhsPadding {
    private long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15, p16;
}

abstract class SynchronizedJobLock extends SynchronizedJobLhsPadding {
    // HotSpot places a subclass field into any gap in its superclasses' layout that fits it, so the
    // lock word must fit no such gap and leave none behind it. A long does both: the only gap is the
    // 4 bytes after a 12-byte object header, and the long's 8-byte alignment leaves no gap after it.
    volatile long locked;
}

@SuppressWarnings("unused")
abstract class SynchronizedJobRhsPadding extends SynchronizedJobLock {
    private long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;
}
