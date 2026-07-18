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
 * It also pins down both concurrent read structures. A reader bounds
 * {@code newSymbolKeyOf} to the slot's published symbol horizon, so the reverse
 * index cannot expose a later assignment. The matching {@code id -> string}
 * lookup runs while the worker repeatedly reallocates its append-only list; the
 * list's release/acquire publication must prevent torn or stale values. Production
 * uses the slot-pin CAS for the horizon publish; this test uses an
 * {@link AtomicInteger} release/acquire pair. This doubles as an ARM-CI canary for
 * the concurrent read paths and a guard on the append-only history invariant.
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
        // symbol lead. Concurrent map publication and backing-array reallocation
        // must not throw or return a torn id; any id the
        // key lookup returns must round-trip back to the same string.
        //
        // Each reader bounds its newSymbolKeyOf lookup to a horizon the worker
        // publishes through an AtomicInteger release after the matching intern -
        // the unit-level stand-in for the production slot-pin CAS, which publishes
        // the slot's stamped symbol horizon to a reader. The test races reverse-map
        // publication and id-list reallocation, then round-trips every found id.
        // This is one of several ARM-CI weak-memory guards for the read path: the
        // torn-VALUE angle is covered by testReaderPinnedToStaleHorizonSeesNoTornValue
        // and the single-threaded overlay-bounds angle by
        // testOverlayBoundsKeyScanToSlotHorizon. Here it also guards the append-only
        // id->string invariant the read path relies on.
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        final int internCount = 2_000_000;
        final int numReaders = 4;
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        // The lead's symbol horizon, published with release semantics after each
        // intern; readers read it with acquire semantics and never resolve past it.
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

        // Every reader's loop is bounded by writerDone, so a reader the scheduler never gets to until
        // the writer has finished runs zero iterations - and the terminal assertions below (no
        // errors, writer-side intern count) all still pass. Count each reader's probes and require
        // it actually raced the writer. Each reader owns its slot and the joins below publish it.
        final long[] probeCounts = new long[numReaders];

        final Thread[] readers = new Thread[numReaders];
        for (int r = 0; r < numReaders; r++) {
            final int seed = r;
            readers[r] = new Thread(() -> {
                try {
                    barrier.await();
                    int probe = seed;
                    long probes = 0;
                    while (!writerDone.get()) {
                        // Acquire-read the published horizon, then bound the lookup
                        // to it - the WHERE/GROUP BY raw-int-key path.
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
                        probes++;
                        probe += numReaders;
                        if (probe >= internCount) {
                            probe = seed;
                        }
                    }
                    probeCounts[seed] = probes;
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
        assertAllReadersProbed(probeCounts);
        // Sanity: every distinct value was interned exactly once.
        Assert.assertEquals(internCount, cache.newSymbolMaxIdExclusive(COL));
    }

    @Test
    public void testReaderPinnedToStaleHorizonSeesNoTornValue() throws Exception {
        // Torn-VALUE canary (companion to testConcurrentInternAndReadDoNotRace,
        // which races both read indexes). A reader pins its slot horizon ONCE, then keeps
        // resolving in-band values while the worker interns far past that horizon,
        // reallocating the backing array many times without the reader re-acquiring
        // a fresh publish. Every in-band value (id < the pinned horizon) was
        // assigned before the pin, so it must always resolve - never a spurious
        // VALUE_NOT_FOUND. The cache's id->string list is a ConcurrentCharSequenceList,
        // which release-stores each reallocated array and acquire-loads it on every
        // read, so a reader that picks up a new array also sees the copied elements.
        // With a plain ObjList the array store has no fence, so on a weak-memory host
        // (ARM) the reader could observe the new array before the copies and read a
        // stale null at an in-bounds id. x86/TSO cannot exhibit it - this is an
        // ARM-CI canary.
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        final int internCount = 2_000_000;
        final int warmup = 256;
        final int numReaders = 4;
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean writerDone = new AtomicBoolean(false);
        final AtomicInteger publishedHorizon = new AtomicInteger(0);
        final CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);

        final Thread writer = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < internCount; i++) {
                    cache.intern(COL, "v" + i, NOT_FOUND_READER);
                    publishedHorizon.set(i + 1); // release
                }
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                writerDone.set(true);
            }
        }, "lv-symbol-cache-writer");

        // As in testConcurrentInternAndReadDoNotRace, the reader loop is bounded by writerDone and so
        // can run zero times. Worse, the pin loop below used to bail out with a bare `return` when it
        // saw no horizon, which is the same silent pass with a comment claiming it cannot happen.
        // Count the probes and let the assertion decide.
        final long[] probeCounts = new long[numReaders];

        final Thread[] readers = new Thread[numReaders];
        for (int r = 0; r < numReaders; r++) {
            final int seed = r;
            readers[r] = new Thread(() -> {
                try {
                    barrier.await();
                    // Pin a warmup horizon once and never re-acquire, so the worker's
                    // later reallocations are not ordered to this reader by a fresh
                    // publish - only the list's own release/acquire can make them safe.
                    int pinned;
                    while ((pinned = publishedHorizon.get()) < warmup && !writerDone.get()) {
                        Thread.onSpinWait();
                    }
                    if (pinned <= 0) {
                        throw new AssertionError("reader pinned no horizon: the writer interned "
                                + internCount + " values, so the reader must have observed at least one");
                    }
                    int probe = seed % pinned;
                    long probes = 0;
                    while (!writerDone.get()) {
                        // id probe < pinned, so it was interned before the pin: must resolve.
                        final int key = cache.newSymbolKeyOf(COL, "v" + probe, 0, pinned);
                        if (key != probe) {
                            throw new AssertionError("torn in-band miss: probe=" + probe
                                    + ", pinned=" + pinned + ", key=" + key);
                        }
                        final CharSequence resolved = cache.newSymbolValueOf(COL, probe);
                        if (resolved == null || !("v" + probe).contentEquals(resolved)) {
                            throw new AssertionError("torn id->value: probe=" + probe
                                    + ", resolved=" + resolved);
                        }
                        probes++;
                        probe += numReaders;
                        if (probe >= pinned) {
                            probe -= pinned;
                        }
                    }
                    probeCounts[seed] = probes;
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
            throw new AssertionError("symbol cache torn-value race", errors.peek());
        }
        assertAllReadersProbed(probeCounts);
    }

    @Test
    public void testMissingKeyLookupDoesNotScanLeadOverlay() {
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);
        final int symbolCount = 4096;
        for (int i = 0; i < symbolCount; i++) {
            cache.intern(COL, "value-" + i, NOT_FOUND_READER);
        }

        final class CountingCharSequence implements CharSequence {
            private int accesses;
            private final String value;

            private CountingCharSequence(String value) {
                this.value = value;
            }

            @Override
            public char charAt(int index) {
                accesses++;
                return value.charAt(index);
            }

            @Override
            public int length() {
                accesses++;
                return value.length();
            }

            @Override
            public CharSequence subSequence(int start, int end) {
                return value.subSequence(start, end);
            }
        }

        final String missingValue = "absent-value";
        final CountingCharSequence probe = new CountingCharSequence(missingValue);
        Assert.assertEquals(
                SymbolTable.VALUE_NOT_FOUND,
                cache.newSymbolKeyOf(COL, probe, 0, symbolCount)
        );
        Assert.assertTrue(
                "a cache miss must hash the probe once, not compare it with every lead symbol [accesses="
                        + probe.accesses + ']',
                probe.accesses <= missingValue.length() * 4
        );
    }

    @Test
    public void testReverseIndexPreservesPinnedHorizonAcrossReassignment() {
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes);

        Assert.assertEquals(0, cache.intern(COL, "repeated", NOT_FOUND_READER));
        final int oldHorizon = cache.newSymbolMaxIdExclusive(COL);

        // O3 clears the current interning window. The same string can then be
        // provisionally assigned a later id while a cursor still holds a slot
        // stamped with the old horizon.
        cache.onO3();
        Assert.assertEquals(1, cache.intern(COL, "repeated", NOT_FOUND_READER));
        final int newHorizon = cache.newSymbolMaxIdExclusive(COL);

        // The reverse index must retain both assignments: an old slot resolves
        // the old id, while current and tail-only bands resolve the newer id.
        Assert.assertEquals(0, cache.newSymbolKeyOf(COL, "repeated", 0, oldHorizon));
        Assert.assertEquals(1, cache.newSymbolKeyOf(COL, "repeated", 0, newHorizon));
        Assert.assertEquals(1, cache.newSymbolKeyOf(COL, "repeated", oldHorizon, newHorizon));
        Assert.assertEquals(
                SymbolTable.VALUE_NOT_FOUND,
                cache.newSymbolKeyOf(COL, "repeated", newHorizon, newHorizon + 1)
        );
    }

    @Test
    public void testOverlayBoundsKeyScanToSlotHorizon() {
        // Deterministic guard that the overlay bounds its key lookup to the pinned
        // slot's symbol horizon, not the cache's live list size. A value a later
        // refresh cycle interned (past the slot horizon) must resolve to
        // VALUE_NOT_FOUND for that slot, and the overlay's symbol count must reflect
        // the horizon, not the grown size. A reverse lookup must not leak an
        // assignment published by a later refresh cycle.
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

    // Both soaks race their readers against a writer interning 2M values, and both bound the reader
    // loop by the writer's done flag. A reader that the scheduler starves until the writer finishes
    // therefore probes nothing, contributes no error, and leaves the test green while proving
    // nothing. Neither soak can mean anything unless every reader actually got in.
    private static void assertAllReadersProbed(long[] probeCounts) {
        for (int r = 0; r < probeCounts.length; r++) {
            Assert.assertTrue(
                    "reader " + r + " probed the cache zero times: its loop runs only while the writer"
                            + " is still interning, so it raced nothing and asserted nothing",
                    probeCounts[r] > 0
            );
        }
    }

    @Test
    public void testInternWindowFirstMatchesCommittedFirstIds() {
        // Equivalence: for the same sequence of interns (new + repeated symbols), the
        // primary window-first order (windowMapAuthoritative=true) and the committed-first
        // order (false) assign IDENTICAL ids. The optimization changes cost, never ids.
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        final String[] seq = {"a", "b", "a", "c", "b", "a", "d", "c"};
        final int[] idsTrue = new int[seq.length];
        final int[] idsFalse = new int[seq.length];
        try (LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes)) {
            final CountingReader reader = new CountingReader();
            for (int i = 0; i < seq.length; i++) {
                idsTrue[i] = cache.intern(COL, seq[i], reader, true);
            }
        }
        try (LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes)) {
            final CountingReader reader = new CountingReader();
            for (int i = 0; i < seq.length; i++) {
                idsFalse[i] = cache.intern(COL, seq[i], reader, false);
            }
        }
        Assert.assertArrayEquals(
                "window-first (primary) and committed-first must assign identical ids",
                idsFalse,
                idsTrue
        );
    }

    @Test
    public void testInternWindowFirstSkipsCommittedProbeForRepeatedNewSymbol() {
        // M5: a symbol new to the un-flushed lead recurs across many rows. With
        // windowMapAuthoritative=true (the primary, whose window map is reset on flush),
        // intern resolves the 2nd+ occurrence from the window map WITHOUT a committed
        // keyOf (a mmapped symbol-index probe that always misses for a not-yet-committed
        // value). The committed-first order (false) probes committed on every occurrence.
        final IntList columnTypes = new IntList();
        columnTypes.add(ColumnType.SYMBOL);
        try (LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes)) {
            final CountingReader reader = new CountingReader();
            // First occurrence: not in window -> committed keyOf (miss) -> intern id 0.
            Assert.assertEquals(0, cache.intern(COL, "new", reader, true));
            Assert.assertEquals(1, reader.keyOfCalls);
            // Repeated occurrences: window fast-path hit, no committed keyOf.
            Assert.assertEquals(0, cache.intern(COL, "new", reader, true));
            Assert.assertEquals(0, cache.intern(COL, "new", reader, true));
            Assert.assertEquals(
                    "primary window-first must not re-probe committed for a repeated new symbol",
                    1,
                    reader.keyOfCalls
            );
        }
        // Control: committed-first probes committed on every occurrence.
        try (LiveViewSymbolCache cache = new LiveViewSymbolCache(columnTypes)) {
            final CountingReader reader = new CountingReader();
            Assert.assertEquals(0, cache.intern(COL, "new", reader, false));
            Assert.assertEquals(0, cache.intern(COL, "new", reader, false));
            Assert.assertEquals(0, cache.intern(COL, "new", reader, false));
            Assert.assertEquals("committed-first probes committed on every occurrence", 3, reader.keyOfCalls);
        }
    }

    // A committed reader that finds nothing (every value is new to the lead) and counts
    // keyOf calls, so a test can observe how often intern probes committed storage.
    private static final class CountingReader implements SymbolMapReader {
        int keyOfCalls;

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
            keyOfCalls++;
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
        public CharSequence valueOf(int symbolKey) {
            throw new UnsupportedOperationException();
        }
    }
}
