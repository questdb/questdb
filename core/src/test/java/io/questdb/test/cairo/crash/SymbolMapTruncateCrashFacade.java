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

package io.questdb.test.cairo.crash;

import io.questdb.cairo.SymbolMapWriter;
import io.questdb.std.IntHashSet;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * A {@link CrashFaultFilesFacade} that tells which of its kill points {@link SymbolMapWriter#truncate()} takes.
 * <p>
 * {@code SymbolMapWriter.truncate()} zeroes the header of the offset file and of the index key file before it writes
 * them again, so a process killed in between leaves a symbol map that the reader and the next writer open reject.
 * TRUNCATE without KEEP SYMBOL MAPS empties the symbol maps only once it has published the empty {@code _txn}, so the
 * table is empty at those kill points too, but a sweep of the TRUNCATE kill points has to leave them out until
 * {@code SymbolMapWriter} empties a map atomically.
 */
public class SymbolMapTruncateCrashFacade extends CrashFaultFilesFacade {
    private static final String SYMBOL_MAP_WRITER = SymbolMapWriter.class.getName();
    private final IntHashSet points = new IntHashSet();
    private volatile boolean isRecording;

    @Override
    public boolean close(long fd) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            return super.close(fd);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    /**
     * True when kill point {@code point}, counted as {@link #killPointCount()} counts, was taken inside
     * {@code SymbolMapWriter.truncate()} while recording.
     */
    public synchronized boolean isSymbolMapTruncatePoint(int point) {
        return points.contains(point);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            super.munmap(address, size, memoryTag);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    @Override
    public boolean removeQuiet(LPSZ name) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            return super.removeQuiet(name);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    @Override
    public int rename(LPSZ from, LPSZ to) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            return super.rename(from, to);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    @Override
    public boolean rmdir(Path name, boolean haltOnError) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            return super.rmdir(name, haltOnError);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    /**
     * Starts recording the kill points that {@code SymbolMapWriter.truncate()} takes, and forgets the ones recorded
     * before.
     */
    public synchronized void startRecording() {
        points.clear();
        isRecording = true;
    }

    public void stopRecording() {
        isRecording = false;
    }

    public synchronized int symbolMapTruncatePointCount() {
        return points.size();
    }

    @Override
    public boolean truncate(long fd, long size) {
        final int before = killPointCount();
        final boolean isSymbolMapTruncate = isSymbolMapTruncate();
        try {
            return super.truncate(fd, size);
        } finally {
            record(before, isSymbolMapTruncate);
        }
    }

    private boolean isSymbolMapTruncate() {
        return isRecording && StackWalker.getInstance().walk(frames -> frames.anyMatch(
                frame -> SYMBOL_MAP_WRITER.equals(frame.getClassName()) && "truncate".equals(frame.getMethodName())
        ));
    }

    private synchronized void record(int before, boolean isSymbolMapTruncate) {
        if (isSymbolMapTruncate) {
            for (int point = before + 1, n = killPointCount(); point <= n; point++) {
                points.add(point);
            }
        }
    }
}
