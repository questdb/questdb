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
import io.questdb.cairo.lv.LiveViewSymbolTable;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * <p>
 * It also pins down the memory-safety contract of the bounded scan: a reader must
 * bound {@code newSymbolKeyOf} to a symbol horizon published with a happens-before
 * edge (in production the slot-pin CAS; here an {@link AtomicInteger} release /
 * acquire), never to the list's live {@code size()}. The horizon is at most the
 * list size when it was published, so the backing array the reader observes is at
 * least that long even while the worker concurrently grows and reallocates the
 * list - the index stays in bounds without a lock. Bounding to a live size read
 * instead is the bug this guards against: on a weak-memory host (ARM) the reader
 * could observe the bumped size paired with the old, shorter array and index out
 * of bounds. So this doubles as an ARM-CI canary for the bounded read path and as
 * a guard on the append-only {@code id -> string} invariant.
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
        // Each reader bounds its newSymbolKeyOf scan to a horizon the worker
        // publishes through an AtomicInteger release after the matching intern -
        // the unit-level stand-in for the production slot-pin CAS, which publishes
        // the slot's stamped symbol horizon to a reader. The horizon is at most the
        // list size when it was published, so the backing array the reader observes
        // is at least that long; every index stays in bounds even as the worker
        // reallocates the list under it. On a weak-memory host (ARM) a scan bounded
        // to the live size() instead could observe the bumped size with the old,
        // shorter array and go out of bounds - the regression this guards. So this
        // doubles as an ARM-CI canary and as a guard on the append-only id->string
        // invariant the read path relies on.
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        final int internCount = 2_000_000;
        final int numReaders = 4;
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        // The lead's symbol horizon, published with release semantics after each
        // intern; readers read it with acquire semantics and never scan past it.
        final AtomicInteger publishedHorizon = new AtomicInteger(0);
        final CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);

        final Thread writer = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < internCount; i++) {
                    cache.intern(COL, "v" + i, NOT_FOUND_READER);
                    // Publish the new horizon (id i was just assigned, so the list
                    // size is now i + 1). The release pairs with the readers'
                    // acquire so a reader bounding to this value observes a backing
                    // array at least this long.
                    publishedHorizon.set(i + 1);
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
                        // Acquire-read the published horizon, then bound the scan to
                        // it - the WHERE/GROUP BY raw-int-key path, memory-safe.
                        final int horizon = publishedHorizon.get();
                        final int key = cache.newSymbolKeyOf(COL, "v" + probe, 0, horizon);
                        if (key != SymbolTable.VALUE_NOT_FOUND) {
                            // A found id must resolve back to the same string.
                            final CharSequence resolved = cache.newSymbolValueOf(COL, key);
                            if (resolved == null || !("v" + probe).contentEquals(resolved)) {
                                throw new AssertionError("torn key->value: key=" + key
                                        + ", probe=" + probe + ", resolved=" + resolved);
                            }
                        }
                        // Id-keyed resolution at the published frontier (getSymA).
                        if (horizon > 0) {
                            cache.newSymbolValueOf(COL, horizon - 1);
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

    @Test
    public void testOverlayBoundsKeyScanToSlotHorizon() {
        // Deterministic guard that the overlay bounds its key scan to the pinned
        // slot's symbol horizon, not the cache's live list size. A value a later
        // refresh cycle interned (past the slot horizon) must resolve to
        // VALUE_NOT_FOUND for that slot, and the overlay's symbol count must reflect
        // the horizon, not the grown size. This is the over-scan a revert to the
        // live-size scan would reintroduce - caught here on any host (the
        // memory-safety angle of the same revert needs a weak-memory host; see
        // testConcurrentInternAndReadDoNotRace).
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        // A slot publishes after interning v0, v1, v2 -> ids 0, 1, 2; horizon = 3.
        Assert.assertEquals(0, cache.intern(COL, "v0", NOT_FOUND_READER));
        Assert.assertEquals(1, cache.intern(COL, "v1", NOT_FOUND_READER));
        Assert.assertEquals(2, cache.intern(COL, "v2", NOT_FOUND_READER));
        final int slotHorizon = cache.newSymbolMaxIdExclusive(COL);
        Assert.assertEquals(3, slotHorizon);

        final LiveViewSymbolTable overlay = new LiveViewSymbolTable()
                .of(NOT_FOUND_READER, cache, COL, slotHorizon, false);

        // In-band lead values resolve, and the count covers the horizon.
        Assert.assertEquals(0, overlay.keyOf("v0"));
        Assert.assertEquals(2, overlay.keyOf("v2"));
        Assert.assertEquals(3, overlay.getSymbolCount());

        // A later cycle interns v3, v4 -> ids 3, 4, growing the cache past the
        // slot's horizon.
        Assert.assertEquals(3, cache.intern(COL, "v3", NOT_FOUND_READER));
        Assert.assertEquals(4, cache.intern(COL, "v4", NOT_FOUND_READER));

        // The slot's overlay still only sees its own band: later-cycle values are
        // invisible and the count is unchanged - the bound is the slot horizon, not
        // the now-larger live size.
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, overlay.keyOf("v3"));
        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, overlay.keyOf("v4"));
        Assert.assertEquals(3, overlay.getSymbolCount());

        // Sanity: an in-band value still resolves after the cache grew.
        Assert.assertEquals(1, overlay.keyOf("v1"));
    }
}
