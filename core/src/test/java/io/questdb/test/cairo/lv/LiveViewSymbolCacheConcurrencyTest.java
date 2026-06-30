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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrency unit coverage for {@link LiveViewSymbolCache}. The cache is a
 * single-writer (the refresh worker), multi-reader (query cursors over a Mode A
 * symbol lead) structure: the worker {@link LiveViewSymbolCache#intern interns}
 * new lead-symbol values while cursors resolve symbols by raw int key
 * ({@link LiveViewSymbolCache#newSymbolKeyOf}, the {@code WHERE sym = '...'} /
 * GROUP BY / static ORDER BY path) and by id
 * ({@link LiveViewSymbolCache#newSymbolValueOf}, the {@code getSymA} path).
 * <p>
 * The reader-churn soak in {@code LiveViewConcurrencyTest} never reaches these
 * paths: its Mode A lead view carries a SYMBOL passthrough that routes the read
 * disk-only, and the single-threaded symbol-lead tests in
 * {@code LiveViewInMemReadTest} never run a reader against a concurrently
 * interning worker. This test exercises the read methods against a worker that
 * is actively growing the cache's per-column id-to-string lists.
 */
public class LiveViewSymbolCacheConcurrencyTest {

    private static final int COL = 0;

    // A committed reader that finds nothing, so every interned value is new to the
    // lead and grows the cache's id-to-string list. intern only ever calls keyOf.
    private static final SymbolMapReader NOT_FOUND_READER = new SymbolMapReader() {
        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSymbolCount() {
            return 0;
        }

        @Override
        public MemoryR getSymbolOffsetsColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryR getSymbolValuesColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCached() {
            return false;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public int keyOf(CharSequence value) {
            return SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public StaticSymbolTable newSymbolTableView() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateSymbolCount(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence valueBOf(int key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence valueOf(int key) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    public void testConcurrentInternAndReadDoNotRace() throws Exception {
        // The worker interns a long run of distinct values into the cache's
        // id-to-string list (forcing many backing-array growths) while reader
        // threads resolve symbols by raw int key and by id - exactly what a
        // WHERE/GROUP BY filter and a getSymA print do against a live Mode A
        // symbol lead. A backing-array reallocation observed mid-scan must not
        // throw (ArrayIndexOutOfBounds / NPE) nor return a torn id; any id the
        // key lookup returns must round-trip back to the same string.
        //
        // newSymbolKeyOf scans up to the cache's live list size, which the worker
        // grows concurrently. On a TSO host (x86) the worker's array-store-then-
        // size-store ordering keeps an observed size paired with a backing array
        // at least that large, so the index stays in bounds and this passes; the
        // out-of-bounds hazard surfaces only on a weak-memory host (ARM). The test
        // therefore doubles as an ARM-CI canary and as a guard against any future
        // change that breaks the append-only id->string invariant the read path
        // relies on (which would also fail on x86).
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        final int internCount = 2_000_000;
        final int numReaders = 4;
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);

        final Thread writer = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < internCount; i++) {
                    cache.intern(COL, "v" + i, NOT_FOUND_READER);
                }
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                writerDone.set(true);
            }
        }, "lv-symbol-cache-writer");

        final Thread[] readers = new Thread[numReaders];
        for (int r = 0; r < numReaders; r++) {
            final int seed = r;
            readers[r] = new Thread(() -> {
                try {
                    barrier.await();
                    int probe = seed;
                    while (!writerDone.get()) {
                        // Raw int-key resolution over the whole lead band, the
                        // racy live-size scan the WHERE/GROUP BY path drives.
                        final int key = cache.newSymbolKeyOf(COL, "v" + probe, 0);
                        if (key != SymbolTable.VALUE_NOT_FOUND) {
                            // A found id must resolve back to the same string.
                            final CharSequence resolved = cache.newSymbolValueOf(COL, key);
                            if (resolved == null || !("v" + probe).contentEquals(resolved)) {
                                throw new AssertionError("torn key->value: key=" + key
                                        + ", probe=" + probe + ", resolved=" + resolved);
                            }
                        }
                        // Id-keyed resolution near the growing frontier (getSymA).
                        final int maxId = cache.newSymbolMaxIdExclusive(COL);
                        if (maxId > 0) {
                            cache.newSymbolValueOf(COL, maxId - 1);
                        }
                        probe += numReaders;
                        if (probe >= internCount) {
                            probe = seed;
                        }
                    }
                } catch (Throwable th) {
                    errors.add(th);
                }
            }, "lv-symbol-cache-reader-" + r);
        }

        writer.start();
        for (Thread t : readers) {
            t.start();
        }
        writer.join();
        for (Thread t : readers) {
            t.join();
        }

        if (!errors.isEmpty()) {
            throw new AssertionError("symbol cache read/write raced", errors.peek());
        }
        // Sanity: every distinct value was interned exactly once.
        Assert.assertEquals(internCount, cache.newSymbolMaxIdExclusive(COL));
    }
}
