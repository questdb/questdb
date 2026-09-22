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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.Chars;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IntHashJoinBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private static final Symbols POPULATED_SYMBOLS = new Symbols(
            "symbol-0", "symbol-1", "symbol-2", "symbol-3", "symbol-4", "symbol-5",
            "symbol-6", "symbol-7", "symbol-8", "symbol-9", "symbol-10", "symbol-11"
    );

    @Test
    public void testCompressedIncrementalHeapBoundAndGrowthCap() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 8)) {
                build.open(tracker, NOOP);
                build.append(17, 42);
                Field heapField = IntHashJoinBuild.class.getDeclaredField("heap");
                heapField.setAccessible(true);
                Object heap = heapField.get(build);
                Field rowsField = heap.getClass().getDeclaredField("rows");
                rowsField.setAccessible(true);
                Object rows = rowsField.get(heap);
                Field capacityField = rows.getClass().getDeclaredField("capacity");
                capacityField.setAccessible(true);
                Method ensure = rows.getClass().getDeclaredMethod("ensure", long.class, long.class);
                ensure.setAccessible(true);
                // Exercise the incremental path directly, without the known-size hint guard.
                for (long required : new long[]{-1, limit + 1, Long.MAX_VALUE}) {
                    InvocationTargetException error = Assert.assertThrows(InvocationTargetException.class,
                            () -> ensure.invoke(rows, required, 8L));
                    Assert.assertTrue(error.getCause() instanceof CairoException);
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), "buffer overflow");
                    Assert.assertEquals(32, tracker.getUsed());
                }
                final long realCapacity = capacityField.getLong(rows);
                try {
                    capacityField.setLong(rows, limit / 2 + 8);
                    InvocationTargetException error = Assert.assertThrows(InvocationTargetException.class,
                            () -> ensure.invoke(rows, limit / 2 + 16, 8L));
                    Assert.assertTrue(error.getCause() instanceof CairoException);
                    // The tiny tracker rejects before allocation/copy. The requested
                    // destination must be capped even though doubling exceeds the limit.
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), "query memory limit exceeded");
                    TestUtils.assertContains(((CairoException) error.getCause()).getFlyweightMessage(), ", size=" + limit + ",");
                } finally {
                    capacityField.setLong(rows, realCapacity);
                }
                build.append(17, 43);
                try (FrozenHashJoinBuild.IntProbe probe = build.freeze(new RowIdPayloadSource()).newProbe()) {
                    probe.find(17);
                    probe.next();
                    Assert.assertEquals(43, probe.getRecord().getInt(0));
                    probe.next();
                    Assert.assertEquals(42, probe.getRecord().getInt(0));
                    Assert.assertFalse(probe.hasNext());
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCompressedUnsignedReferencesSurviveRehashCollisions() throws Exception {
        assertMemoryLeak(() -> {
            int[] keys = new int[3];
            for (int key = 0, count = 0; count < keys.length; key++) {
                if (((int) Hash.hashInt64(key) & 7) == 7) {
                    keys[count++] = key;
                }
            }
            try (IntHashJoinBuild build = new IntHashJoinBuild(true, 4, 24)) {
                build.open(null, NOOP);
                build.append(keys[0], 0);
                build.append(keys[1], 1);
                Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                keysField.setAccessible(true);
                Object table = keysField.get(build);
                Field addressField = table.getClass().getDeclaredField("address");
                addressField.setAccessible(true);
                long address = addressField.getLong(table);
                long offset = 0x80000000L << 3;
                Unsafe.putInt(address + 3 * 8 + 4, CompressedOffsets.compressBiased8(offset));
                Unsafe.putInt(address + 4, CompressedOffsets.compressBiased8(offset + 16));
                build.append(keys[2], 2); // Rehash both negative references into colliding destination slots.
                try (FrozenHashJoinBuild.IntProbe probe = build.freeze(new RowIdPayloadSource()).newProbe()) {
                    Field rowsField = probe.getClass().getSuperclass().getDeclaredField("heapAddress");
                    rowsField.setAccessible(true);
                    long realRows = rowsField.getLong(probe);
                    for (int i = 0; i < keys.length; i++) {
                        rowsField.setLong(probe, i < 2 ? realRows - offset : realRows);
                        probe.findUnchecked(keys[i]);
                        Assert.assertTrue(probe.hasNext());
                        Assert.assertEquals(i < 2 ? offset + i * 16L : 32, probe.next());
                        // The probe reads the row id at the widened offset, which lands in the real row.
                        Assert.assertEquals(i, probe.getRecord().getInt(0));
                        Assert.assertFalse(probe.hasNext());
                        Assert.assertTrue(probe.findSingleUnchecked(keys[i]));
                        Assert.assertEquals(i, probe.getRecord().getInt(0));
                    }
                    rowsField.setLong(probe, realRows);
                }
            }
        });
    }

    @Test
    public void testCompressedUnsignedProbeReferences() throws Exception {
        assertMemoryLeak(() -> {
            for (int count : new int[]{1, 2}) {
                try (IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 8)) {
                    build.open(null, NOOP);
                    for (int i = 0; i < count; i++) {
                        build.append(17, 42);
                    }
                    try (FrozenHashJoinBuild.IntProbe probe = build.freeze(new RowIdPayloadSource()).newProbe()) {
                        Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                        keysField.setAccessible(true);
                        Object keys = keysField.get(build);
                        Field addressField = keys.getClass().getDeclaredField("address");
                        addressField.setAccessible(true);
                        long slot = addressField.getLong(keys) + ((int) Hash.hashInt64(17) & 1) * 8L;
                        Field rowsField = probe.getClass().getSuperclass().getDeclaredField("heapAddress");
                        rowsField.setAccessible(true);
                        long realRows = rowsField.getLong(probe);
                        // Simulate a large relative heap. Every dereference still lands
                        // in the real, owned rows; no 16/32 GiB allocation is needed.
                        for (long offset : new long[]{0, 0x80000000L << 3, CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - count * 16}) {
                            Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8(offset + (count - 1) * 16));
                            if (count == 2) {
                                Unsafe.putLong(realRows + 16, offset + 8);
                            }
                            rowsField.setLong(probe, realRows - offset);
                            probe.findUnchecked(17);
                            for (int i = count - 1; i >= 0; i--) {
                                Assert.assertTrue(probe.hasNext());
                                Assert.assertEquals(offset + i * 16L, probe.next());
                                Assert.assertEquals(42, probe.getRecord().getInt(0));
                            }
                            Assert.assertFalse(probe.hasNext());
                            if (count == 1) {
                                Assert.assertTrue(probe.findSingleUnchecked(17));
                                Assert.assertEquals(42, probe.getRecord().getInt(0));
                                Assert.assertFalse(probe.hasNext());
                                Assert.assertFalse(probe.findSingleUnchecked(-17));
                            }
                        }
                        Unsafe.putInt(slot + 4, CompressedOffsets.compressBiased8((count - 1) * 16L));
                        if (count == 2) {
                            Unsafe.putLong(realRows + 16, 8);
                        }
                        rowsField.setLong(probe, realRows);
                    }
                }
            }
        });
    }

    @Test
    public void testCompressedHeapBoundBeforeAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(true, 2, limit + 1));
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 8)) {
                for (long hint : new long[]{limit / 16 + 1, Long.MAX_VALUE}) {
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> build.reserve(hint, -1));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
                    build.open(tracker, NOOP);
                    build.reserve(1, -1);
                    build.append(17, 17);
                    try (FrozenHashJoinBuild.IntProbe probe = build.freeze(payloads).newProbe()) {
                        Assert.assertTrue(probe.findSingleUnchecked(17));
                        Assert.assertEquals(17, probe.getRecord().getInt(0));
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
                // The largest legal hint reaches tracked allocation and is rejected
                // by this tiny memory limit, rather than wrapping its compressed offset.
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.reserve(limit / 16, -1));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(error.getFlyweightMessage(), ", size=" + limit + ",");
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testUniqueDuplicateAndEmptyBuildReuseWithAndWithoutPayload() throws Exception {
        assertMemoryLeak(() -> {
            Symbols symbols = new Symbols();
            symbols.put(1, "Aa", "BB");
            // Column 1 is a SYMBOL payload whose key cycles through both values and null.
            RowIdPayloadSource payloads = new RowIdPayloadSource(symbols,
                    rowId -> rowId % 3 == 2 ? SymbolTable.VALUE_IS_NULL : (int) (rowId % 3), 1);
            for (boolean hasPayload : new boolean[]{false, true}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(32 * 1024 * 1024);
                     IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 8, true)) {
                    FrozenHashJoinBuild.IntProbe reusable = null;
                    try {
                        // Recompute uniqueness on empty -> unique -> late duplicate -> skew -> unique reuse.
                        for (int execution = 0; execution < 5; execution++) {
                            build.open(tracker, NOOP);
                            Map<Integer, List<Integer>> expected = new HashMap<>();
                            int count = execution == 0 ? 0 : 4097;
                            for (int r = 0; r < count; r++) {
                                int key = execution == 3 ? (r % 7) : execution == 2 && r == count - 1 ? 0 : r;
                                key = key == 1 ? Numbers.INT_NULL : key == 2 ? Integer.MAX_VALUE : key == 3 ? -1 : key;
                                expected.computeIfAbsent(key, ignored -> new ArrayList<>()).add(r);
                                build.append(key, r);
                            }
                            FrozenHashJoinBuild.IntKeyed frozen = hasPayload ? build.freeze(payloads) : build.freeze();
                            Assert.assertEquals(count, frozen.getRowCount());
                            Assert.assertEquals(expected.size(), frozen.getKeyCount());
                            Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                            if (reusable == null) {
                                reusable = frozen.newProbe();
                            } else {
                                reusable.reopen();
                            }
                            Assert.assertEquals(hasPayload, reusable.getRecord() != null);
                            List<Long> handles = new ArrayList<>();
                            List<Integer> payloadRows = new ArrayList<>();
                            for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet()) {
                                reusable.findUnchecked(entry.getKey());
                                List<Integer> matches = new ArrayList<>();
                                while (reusable.hasNext()) {
                                    long handle = reusable.next();
                                    int value = hasPayload ? reusable.getRecord().getInt(0) : 0;
                                    matches.add(value);
                                    handles.add(handle);
                                    payloadRows.add(value);
                                }
                                Assert.assertEquals(entry.getValue().size(), matches.size());
                                if (hasPayload) {
                                    // Duplicates come back in reverse input order.
                                    List<Integer> reversed = new ArrayList<>(entry.getValue());
                                    Collections.reverse(reversed);
                                    Assert.assertEquals(reversed, matches);
                                }
                                if (count == expected.size()) {
                                    Assert.assertTrue(reusable.findSingleUnchecked(entry.getKey()));
                                    if (hasPayload) {
                                        Assert.assertEquals(entry.getValue().get(0).intValue(), reusable.getRecord().getInt(0));
                                    }
                                    Assert.assertFalse(reusable.hasNext());
                                }
                            }
                            reusable.findUnchecked(-42);
                            Assert.assertFalse(reusable.hasNext());
                            if (count == expected.size()) {
                                Assert.assertFalse(reusable.findSingleUnchecked(-42));
                            }
                            for (int h = 0; h < handles.size(); h++) {
                                reusable.recordAt(handles.get(h));
                                if (hasPayload) {
                                    int row = payloadRows.get(h);
                                    Assert.assertEquals(row, reusable.getRecord().getInt(0));
                                    Assert.assertEquals(row + 0.25, reusable.getRecord().getDouble(2), 0);
                                    TestUtils.assertEquals(symbols.valueOf(1, row % 3 == 2 ? -1 : row % 3), reusable.getRecord().getSymA(1));
                                }
                            }
                            reusable.close();
                            build.close();
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    } finally {
                        Misc.free(reusable);
                    }
                }
            }
            // One reader for the one reusable probe that read payloads, reopened per non-empty execution too.
            Assert.assertEquals(1, payloads.readerCount);
            Assert.assertEquals(5, payloads.reopenCount);
        });
    }

    @Test
    public void testKnownBuildSizeReservesTrackedRowsAndOverflowReuses() throws Exception {
        assertMemoryLeak(() -> {
            // Enough for the exact rows and two key slots, but not a doubling copy.
            final long capacity = 10_000 * 16L + 16;
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(capacity);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                for (int execution = 0; execution < 2; execution++) {
                    build.open(tracker, NOOP);
                    build.reserve(10_000, -1);
                    for (int row = 1; row <= 10_000; row++) {
                        build.append(1, row);
                    }
                    FrozenHashJoinBuild.IntKeyed frozen = build.freeze(payloads);
                    Assert.assertEquals(10_000, frozen.getRowCount());
                    Assert.assertEquals(capacity, tracker.getUsed());
                    Assert.assertEquals(capacity, frozen.getSizeInBytes());
                    try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        probe.find(1);
                        probe.next();
                        Assert.assertEquals(10_000, probe.getRecord().getLong(0));
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> build.reserve(Long.MAX_VALUE, -1));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testKeyCountHintBoundsWithoutCapping() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(false, 64, 64);
                 RecordCursorFactory factory = select("SELECT x::INT k FROM long_sequence(1_000)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // A hint below the distinct keys presizes that far, to 256 slots, and the table
                // grows past it to 2_048 slots; the heap doubles from 64 bytes to 8_192 for 8_000.
                build.open(tracker, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, -1, 100, null);
                Assert.assertEquals(1_000, frozen.getKeyCount());
                Assert.assertEquals(2_048 * 8 + 8_192, frozen.getSizeInBytes());
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    for (int key = 0; key <= 1_001; key++) {
                        Assert.assertEquals(key > 0 && key <= 1_000, probe.findSingleUnchecked(key));
                    }
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                // A hint past the largest table asks for all of it, 8 GiB, which the limit rejects
                // before any allocation; the failure releases the build, which then reopens.
                cursor.toTop();
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, -1, Long.MAX_VALUE, null));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
                cursor.toTop();
                build.open(tracker, NOOP);
                Assert.assertEquals(1_000, build.build(cursor, 0, 1_000, 1_000, null).getKeyCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testKeyCountHintPresizesKeyTableAndLowersPeak() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 100_000;
            // 100_000 distinct keys fill 262_144 eight-byte slots up to half, and a build without
            // payload columns leaves an eight-byte link a row, presized from the row count below.
            final long keyTableBytes = 262_144 * 8L;
            final long heapBytes = rows * 8L;
            // The presize replaces the 64 initial slots, which stay charged while it allocates.
            final long initialKeyTableBytes = 64 * 8L;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(keyTableBytes + heapBytes + initialKeyTableBytes);
                 IntHashJoinBuild build = new IntHashJoinBuild(false, 64, 64);
                 RecordCursorFactory factory = select("SELECT x::INT k FROM long_sequence(" + rows + ")");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                for (int execution = 0; execution < 2; execution++) {
                    cursor.toTop();
                    SiteBreaker breaker = new SiteBreaker(null);
                    build.open(tracker, breaker);
                    FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, rows, rows, null);
                    Assert.assertEquals(rows, frozen.getKeyCount());
                    Assert.assertEquals(keyTableBytes + heapBytes, frozen.getSizeInBytes());
                    Assert.assertEquals(keyTableBytes + heapBytes, tracker.getUsed());
                    // A single rehash, of the 64 empty initial slots, before the first row.
                    Assert.assertEquals(1, breaker.keyRehashChecks);
                    try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        Assert.assertTrue(probe.findSingleUnchecked(1));
                        Assert.assertTrue(probe.findSingleUnchecked(rows));
                        Assert.assertFalse(probe.findSingleUnchecked(rows + 1));
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
                // Growing into the same table rehashes the previous one, half its size, while both
                // are charged, which the same limit does not hold.
                cursor.toTop();
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, rows, -1, null));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRowSizeMatchesHeapLayout() throws Exception {
        assertMemoryLeak(() -> {
            // An eight-byte link, plus the build row's id when the build has payload columns,
            // whatever their number and width.
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            for (boolean hasPayload : new boolean[]{false, true}) {
                final long rowSize = hasPayload ? 16 : 8;
                Assert.assertEquals(rowSize, FrozenHashJoinBuild.getRowSize(hasPayload));
                try (IntHashJoinBuild build = new IntHashJoinBuild(hasPayload, 2, 8)) {
                    build.open(null, NOOP);
                    build.reserve(3, -1);
                    for (int row = 0; row < 3; row++) {
                        build.append(1, row);
                    }
                    FrozenHashJoinBuild.IntKeyed frozen = hasPayload ? build.freeze(payloads) : build.freeze();
                    Assert.assertEquals(3, frozen.getRowCount());
                    // Two eight-byte key slots, and a heap presized to exactly three rows.
                    Assert.assertEquals(16 + 3 * rowSize, frozen.getSizeInBytes());
                }
            }
        });
    }

    @Test
    public void testCursorBuildLeavesRowChecksToCursorFrames() throws Exception {
        assertMemoryLeak(() -> {
            final int frameRows = 1024;
            AtomicInteger clockReads = new AtomicInteger();
            AtomicInteger consumed = new AtomicInteger();
            AtomicInteger interruptMode = new AtomicInteger();
            AtomicBoolean cancelled = new AtomicBoolean();
            AtomicBoolean expired = new AtomicBoolean();
            DefaultSqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                public int getCircuitBreakerThrottle() {
                    // Every consultation performs a real check, so clock reads count build checks.
                    return 1;
                }

                @Override
                public MillisecondClock getClock() {
                    return () -> {
                        clockReads.incrementAndGet();
                        return expired.get() ? 1002 : 1000;
                    };
                }
            };
            Record record = new Record() {
                @Override
                public int getInt(int col) {
                    return 1;
                }

                @Override
                public long getRowId() {
                    return consumed.get();
                }
            };
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, config);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 1_600_000);
                 RecordCursor cursor = new RecordCursor() {
                     @Override
                     public void close() {
                     }

                     @Override
                     public Record getRecord() {
                         return record;
                     }

                     @Override
                     public Record getRecordB() {
                         throw new UnsupportedOperationException();
                     }

                     @Override
                     public boolean hasNext() {
                         if (consumed.get() == 100_000) {
                             return false;
                         }
                         final int row = consumed.incrementAndGet();
                         if (row == 32) {
                             cancelled.set(interruptMode.get() == 1);
                             expired.set(interruptMode.get() == 2);
                         }
                         if (interruptMode.get() == 1 || interruptMode.get() == 2) {
                             // Table cursors check the breaker once per page frame.
                             if (row % frameRows == 0) {
                                 breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                             }
                         }
                         return true;
                     }

                     @Override
                     public long preComputedStateSize() {
                         return 0;
                     }

                     @Override
                     public void recordAt(Record record, long rowId) {
                         throw new UnsupportedOperationException();
                     }

                     @Override
                     public long size() {
                         return 100_000;
                     }

                     @Override
                     public void toTop() {
                         consumed.set(0);
                     }
                 }) {
                breaker.setCancelledFlag(cancelled);
                breaker.setTimeout(1);
                for (int mode = 0; mode < 4; mode++) {
                    interruptMode.set(mode);
                    cancelled.set(false);
                    expired.set(false);
                    cursor.toTop();
                    breaker.resetTimer();
                    clockReads.set(0);
                    build.open(tracker, breaker);
                    if (mode == 1 || mode == 2) {
                        CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, -1, -1, payloads));
                        Assert.assertEquals(mode == 1, error.isCancellation());
                        Assert.assertEquals("the build observes interruption at the cursor's frame check",
                                frameRows, consumed.get());
                    } else {
                        FrozenHashJoinBuild.IntKeyed frozen = build.build(cursor, 0, -1, -1, payloads);
                        Assert.assertEquals(100_000, frozen.getRowCount());
                        // Only open, the initial slot clear and freeze check; appended rows do not.
                        Assert.assertTrue("build checks must not scale with rows: " + clockReads.get(),
                                clockReads.get() <= 3);
                        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                            probe.find(1);
                            probe.next();
                            Assert.assertEquals(100_000, probe.getRecord().getLong(0));
                        }
                    }
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testCancellationAtEveryBuildCheckAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1_000_000);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                populate(build, tracker, counting);
                long checks = counting.getCheckCount();
                build.close();
                // Includes initialization, hash/row growth and freeze.
                for (long failAt = 1; failAt <= checks; failAt++) {
                    final long failureCheck = failAt;
                    CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                        @Override
                        public void statefulThrowExceptionIfTripped() {
                            super.statefulThrowExceptionIfTripped();
                            failAtCheck();
                        }

                        @Override
                        public void statefulThrowExceptionIfTrippedNoThrottle() {
                            super.statefulThrowExceptionIfTrippedNoThrottle();
                            failAtCheck();
                        }

                        @Override
                        public void statefulThrowExceptionIfTrippedTimeThrottled() {
                            super.statefulThrowExceptionIfTrippedTimeThrottled();
                            failAtCheck();
                        }

                        private void failAtCheck() {
                            if (getCheckCount() == failureCheck) {
                                throw CairoException.queryCancelled(1);
                            }
                        }
                    };
                    try {
                        populate(build, tracker, breaker);
                        Assert.fail("expected cancellation at check " + failAt);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    }
                    Assert.assertEquals("cancelled build releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                populate(build, tracker, NOOP);
                Assert.assertTrue(tracker.getUsed() > 0);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRehashChecksCancellationPerMiBOfSlots() throws Exception {
        assertMemoryLeak(() -> {
            // Row 131_072 rehashes 2 MiB of key slots.
            final int rows = 131_073;
            Symbols symbols = new Symbols("reused");
            RowIdPayloadSource payloads = new RowIdPayloadSource(symbols, rowId -> 0, 0);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                for (String failSite : new String[]{null, "growKeyTable"}) {
                    SiteBreaker breaker = new SiteBreaker(failSite);
                    build.open(tracker, breaker);
                    try {
                        for (int row = 0; row < rows; row++) {
                            breaker.row = row;
                            build.append(row, row);
                        }
                        Assert.assertNull("expected cancellation inside " + failSite, failSite);
                        Assert.assertEquals(rows, build.freeze(payloads).getRowCount());
                        Assert.assertEquals("appended rows must not check", 0, breaker.rowChecks);
                        // One check per rehash plus one per MiB of old slots: 19 key checks.
                        Assert.assertTrue("key rehash checks: " + breaker.keyRehashChecks,
                                breaker.keyRehashChecks > 0 && breaker.keyRehashChecks < 64);
                        build.close();
                    } catch (CairoException e) {
                        Assert.assertNotNull("unexpected interruption: " + e.getFlyweightMessage(), failSite);
                        Assert.assertTrue(e.isCancellation());
                        // The first rehash of more than 1 MiB of old slots checks again inside its loop.
                        Assert.assertEquals(131_072, breaker.failedRow);
                    }
                    Assert.assertEquals("rehash cancellation releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                build.open(tracker, NOOP);
                build.append(1, 0);
                try (FrozenHashJoinBuild.IntProbe probe = build.freeze(payloads).newProbe()) {
                    Assert.assertTrue(probe.findSingleUnchecked(1));
                    TestUtils.assertEquals("reused", probe.getRecord().getSymA(0));
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCancellationDuringRowHeapGrowthCopy() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 1024 * 1024)) {
                CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTripped() {
                        failWhileCopiesCoexist();
                    }

                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        // The copy loop checks once per MiB copied.
                        failWhileCopiesCoexist();
                    }

                    private void failWhileCopiesCoexist() {
                        if (tracker.getUsed() > 2 * 1024 * 1024) {
                            throw CairoException.queryCancelled(1);
                        }
                    }
                };
                build.open(tracker, breaker);
                // 65_536 sixteen-byte rows fill the 1 MiB heap.
                for (int i = 0; i < 65536; i++) {
                    build.append(1, i);
                }
                // Cancellation after allocating the destination, while source and destination coexist.
                Assert.assertThrows(CairoException.class, () -> build.append(1, 65536));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(1, 0);
                Assert.assertEquals(1, build.freeze(new RowIdPayloadSource()).getRowCount());
            }
        });
    }

    @Test
    public void testConcurrentLookupsAndIndependentSymbolFlyweights() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (k INT, s SYMBOL, d DOUBLE)");
            execute("""
                    INSERT INTO src
                    SELECT ((x - 1) % 100)::INT, CASE WHEN x % 2 = 1 THEN 'ES' ELSE 'IT' END, (x - 1) + 0.25
                    FROM long_sequence(10_000)
                    """);
            try (RecordCursorFactory factory = select("src");
                 // Probes read payload columns and symbols through the frames, so they stay open while probing.
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, indexes(1, 2), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                ExecutorService executor = Executors.newFixedThreadPool(4);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                ObjList<FrozenHashJoinBuild.IntProbe> probes = new ObjList<>();
                try {
                    for (int worker = 0; worker < 4; worker++) {
                        final int shift = worker;
                        // The owner binds probes and their symbol table views before publication.
                        FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                        probes.add(a);
                        FrozenHashJoinBuild.IntProbe b = frozen.newProbe();
                        probes.add(b);
                        Assert.assertNotSame(a.getSymbolTable(0), b.getSymbolTable(0));
                        futures.add(executor.submit(() -> {
                            // Task submission and the latch publish the completed build.
                            start.await();
                            for (int i = 0; i < 1000; i++) {
                                int key = (i + shift) % 100;
                                a.find(key);
                                b.find((key + 1) % 100);
                                for (int row = 9900 + key; row >= 0; row -= 100) {
                                    Assert.assertTrue(a.hasNext());
                                    a.next();
                                    CharSequence first = a.getRecord().getSymA(0);
                                    b.next();
                                    TestUtils.assertEquals((key & 1) == 0 ? "ES" : "IT", first);
                                    Assert.assertEquals(row + 0.25, a.getRecord().getDouble(1), 0);
                                    TestUtils.assertEquals((key & 1) == 0 ? "IT" : "ES", b.getRecord().getSymB(0));
                                    // Another view's flyweight does not overwrite this one.
                                    TestUtils.assertEquals((key & 1) == 0 ? "ES" : "IT", first);
                                }
                                Assert.assertFalse(a.hasNext());
                                Assert.assertFalse(b.hasNext());
                                a.find(-1);
                                Assert.assertFalse(a.hasNext());
                            }
                            return null;
                        }));
                    }
                    start.countDown();
                    for (Future<?> future : futures) {
                        future.get(30, TimeUnit.SECONDS);
                    }
                } finally {
                    start.countDown();
                    executor.shutdownNow();
                    // Do not release native backing until readers have stopped even on failure.
                    Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                    Misc.freeObjList(probes);
                }
            }
        });
    }

    @Test
    public void testEmptyBuildAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            RowIdPayloadSource payloads = new RowIdPayloadSource(new Symbols("fresh"), 0);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
                Assert.assertThrows(IllegalStateException.class, build::freeze);
                build.open(tracker, NOOP);
                // Payload columns cannot be read without a source; the failure closes the build.
                Assert.assertThrows(IllegalArgumentException.class, build::freeze);
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze(payloads);
                Assert.assertEquals(0, frozen.getRowCount());
                Assert.assertEquals(0, frozen.getKeyCount());
                Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    probe.find(Numbers.INT_NULL);
                    Assert.assertFalse(probe.hasNext());
                    // An empty build still hands out the symbol tables that null-extended rows resolve through.
                    Assert.assertNull(probe.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                    Assert.assertNull(probe.newSymbolTable(0).valueBOf(SymbolTable.VALUE_IS_NULL));
                    Assert.assertTrue(probe.getSymbolTable(0).supportsKeyValueAccess());
                }
                Assert.assertThrows(IllegalStateException.class, () -> build.append(1, 0));
                Assert.assertThrows(IllegalStateException.class, () -> build.open(tracker, NOOP));
                build.close();
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, frozen::newProbe);
                build.open(tracker, NOOP);
                build.append(1, 0);
                try (FrozenHashJoinBuild.IntProbe probe = build.freeze(payloads).newProbe()) {
                    probe.find(1);
                    probe.next();
                    Assert.assertEquals(0, probe.getRecord().getInt(0));
                    TestUtils.assertEquals("fresh", probe.getRecord().getSymA(0));
                }
            }
            // Every probe closed its reader.
            Assert.assertEquals(payloads.readerCount, payloads.closeCount);
        });
    }

    @Test
    public void testEmptyPayloadPreservesDuplicateCounts() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = new IntHashJoinBuild(false, 2, 8)) {
                build.open(null, NOOP);
                for (int i = 0; i < 100; i++) {
                    build.append(Numbers.INT_NULL, i);
                }
                try (FrozenHashJoinBuild.IntProbe probe = build.freeze().newProbe()) {
                    // A build without payload columns stores no row ids and has no record to read.
                    Assert.assertNull(probe.getRecord());
                    probe.find(Numbers.INT_NULL);
                    for (int i = 0; i < 100; i++) {
                        Assert.assertTrue(probe.hasNext());
                        probe.next();
                    }
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testHashCollisionsZeroNegativeAndNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = new IntHashJoinBuild(true, 16, 16)) {
                build.open(null, NOOP);
                IntList keys = new IntList();
                keys.add(0);
                keys.add(-1);
                keys.add(Numbers.INT_NULL);
                keys.add(Integer.MAX_VALUE);
                // Force collision and wraparound at the last bucket, below the resize threshold.
                for (int key = 1; keys.size() < 8; key++) {
                    if ((Hash.hashInt64(key) & 15) == 15) {
                        keys.add(key);
                    }
                }
                for (int i = 0; i < keys.size(); i++) {
                    for (int j = 0; j < 3; j++) {
                        build.append(keys.getQuick(i), i * 10 + j);
                    }
                }
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze(new RowIdPayloadSource());
                Assert.assertEquals(8, frozen.getKeyCount());
                Assert.assertEquals(24, frozen.getRowCount());
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    for (int i = 0; i < keys.size(); i++) {
                        probe.find(keys.getQuick(i));
                        long handle = -1;
                        for (int j = 2; j >= 0; j--) {
                            Assert.assertTrue(probe.hasNext());
                            handle = probe.next();
                            Assert.assertEquals(i * 10 + j + 0.25, probe.getRecord().getDouble(0), 0);
                        }
                        Assert.assertFalse(probe.hasNext());
                        probe.find(-123456);
                        Assert.assertFalse(probe.hasNext());
                        probe.recordAt(handle);
                        Assert.assertEquals(i * 10 + 0.25, probe.getRecord().getDouble(0), 0);
                    }
                }
            }
        });
    }

    @Test
    public void testInvalidLayout() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(true, 3, 16));
        Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(true, 2, 0));
        Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(false, 1, 16));
    }

    @Test
    public void testHighSymbolCardinalityAndDuplicateGrowthAccounting() throws Exception {
        assertMemoryLeak(() -> {
            String[] values = new String[8192];
            for (int i = 0; i < values.length; i++) {
                values[i] = "country-with-a-long-name-" + i;
            }
            RowIdPayloadSource payloads = new RowIdPayloadSource(new Symbols(values), 0);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                for (long limit : new long[]{4096, 16384, 65536, 0}) {
                    tracker.setLimit(limit);
                    try {
                        build.open(tracker, NOOP);
                        for (int i = 0; i < values.length; i++) {
                            build.append(i % 257, i);
                            Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                            Assert.assertEquals(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline, tracker.getUsed());
                        }
                        Assert.assertEquals(0, limit);
                        try (FrozenHashJoinBuild.IntProbe probe = build.freeze(payloads).newProbe()) {
                            probe.find(0);
                            int matches = 0;
                            while (probe.hasNext()) {
                                probe.next();
                                TestUtils.assertEquals("country-with-a-long-name-" + (31 - matches) * 257, probe.getRecord().getSymA(0));
                                matches++;
                            }
                            Assert.assertEquals(32, matches);
                        }
                        // A row costs its link and its row id, whatever the dictionary holds.
                        Assert.assertEquals(8192L * 16, build.getSizeInBytes() - 1024 * 8);
                    } catch (CairoException ex) {
                        Assert.assertTrue(ex.isOutOfMemory());
                        Assert.assertNotEquals(0, limit);
                    } finally {
                        build.close();
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                }
            }
        });
    }

    @Test
    public void testMemoryLimitAtEveryAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                // Sweep byte limits through all small hash and row allocations.
                int failures = 0;
                long peak = 0;
                for (long limit = 1; limit <= 16_384; limit++) {
                    tracker.setLimit(limit);
                    try {
                        populate(build, tracker, NOOP);
                        Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                        build.close();
                        peak = limit;
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        failures++;
                        Assert.assertEquals(0, build.getSizeInBytes());
                    }
                    Assert.assertEquals("all partial build allocations must be released", 0, tracker.getUsed());
                }
                // Every limit below the peak fails, and the peak covers growth of both buffers.
                Assert.assertEquals(peak - 1, failures);
                Assert.assertTrue("peak: " + peak, peak > 256);
                tracker.setLimit(1_000_000);
                populate(build, tracker, NOOP);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesHashRehashPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Two slots (16 bytes), 32-byte row capacity. Rehash needs old 16 + new 32 + rows 32 = 80.
            // The final map and two rows fit in 64 bytes, which must still fail during rehash.
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 32)) {
                build.open(tracker, NOOP);
                build.append(1, 0);
                Assert.assertEquals(48, tracker.getUsed());
                Assert.assertThrows(CairoException.class, () -> build.append(2, 1));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(80);
                build.open(tracker, NOOP);
                build.append(1, 0);
                build.append(2, 1);
                Assert.assertEquals(64, build.freeze(payloads).getSizeInBytes());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesRowHeapGrowthPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Unique key count stays at one: only the duplicate row buffer grows.
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(48);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                build.open(tracker, NOOP);
                build.append(1, 0);
                // Final 16-byte hash + 32-byte rows fits, old 16-byte rows are still live.
                Assert.assertThrows(CairoException.class, () -> build.append(1, 1));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(64);
                build.open(tracker, NOOP);
                build.append(1, 0);
                build.append(1, 1);
                Assert.assertEquals(48, build.freeze(payloads).getSizeInBytes());
            }
        });
    }

    @Test
    public void testNullKeyMatchesExistingHashJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table l (k int)");
            execute("create table r (k int, v double)");
            execute("insert into l values (null), (0), (-1)");
            execute("insert into r values (null, 1.0), (null, 2.0), (0, 3.0), (-1, 4.0)");
            assertQuery("select l.k, r.v from l join r on l.k = r.k order by l.k, r.v")
                    .noLeakCheck().returns("k\tv\nnull\t1.0\nnull\t2.0\n-1\t4.0\n0\t3.0\n");
            try (RecordCursorFactory factory = select("r");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, indexes(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    probe.find(Numbers.INT_NULL);
                    probe.next();
                    Assert.assertEquals(2.0, probe.getRecord().getDouble(0), 0);
                    probe.next();
                    Assert.assertEquals(1.0, probe.getRecord().getDouble(0), 0);
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testRandomizedGrowthAgainstMultimap() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                Map<Integer, List<Integer>> expected = new HashMap<>();
                build.open(null, NOOP);
                Rnd rnd = new Rnd(130, 131);
                for (int i = 0; i < 20_000; i++) {
                    int key = rnd.nextInt(2000) - 1000;
                    build.append(key, i);
                    expected.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
                }
                FrozenHashJoinBuild.IntKeyed frozen = build.freeze(new RowIdPayloadSource());
                Assert.assertEquals(expected.size(), frozen.getKeyCount());
                Assert.assertEquals(20_000, frozen.getRowCount());
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    for (int key = -1100; key <= 1100; key++) {
                        probe.find(key);
                        List<Integer> values = expected.get(key);
                        if (values != null) {
                            for (int i = values.size() - 1; i >= 0; i--) {
                                Assert.assertTrue(probe.hasNext());
                                probe.next();
                                Assert.assertEquals(values.get(i) + 0.25, probe.getRecord().getDouble(0), 0);
                            }
                        }
                        Assert.assertFalse(probe.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testSourceFailureClosesPartialBuild() throws Exception {
        assertMemoryLeak(() -> {
            RowIdPayloadSource payloads = new RowIdPayloadSource();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                // The second row's id fails after the first row was appended.
                KeyCursor cursor = new KeyCursor(new Symbols(), new int[]{1, 2}) {
                    @Override
                    long rowId(int row) {
                        if (row == 1) {
                            throw new IllegalStateException("source getter failed");
                        }
                        return row;
                    }
                };
                build.open(tracker, NOOP);
                Assert.assertThrows(IllegalStateException.class, () -> build.build(cursor, 0, -1, -1, payloads));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(2, 0);
                Assert.assertEquals(1, build.freeze(payloads).getRowCount());
            }
        });
    }

    @Test
    public void testReusableBuildGrowthDoesNotAllocateHeap() throws Exception {
        assertMemoryLeak(() -> {
            com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            org.junit.Assume.assumeTrue(bean.isThreadAllocatedMemorySupported());
            bean.setThreadAllocatedMemoryEnabled(true);
            String[] values = new String[65_536];
            for (int i = 0; i < values.length; i++) {
                values[i] = Integer.toString(i);
            }
            // The source hands out one retained view, so only the build itself can allocate.
            Symbols symbols = new Symbols(values);
            symbols.isViewShared = true;
            RowIdPayloadSource payloads = new RowIdPayloadSource(symbols, 0);
            FrozenHashJoinBuild.IntProbe probe = null;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16, true)) {
                long allocated = 0;
                // Keep the setup cardinality fixed while warming the JVM's loop backedges.
                // A short setup can charge VM String/byte[] allocations
                // at the probe-loop backedge to the first measured native growth.
                final int warmupExecutions = 200;
                for (int execution = 0; execution < warmupExecutions + 2; execution++) {
                    long before = bean.getCurrentThreadAllocatedBytes();
                    build.open(tracker, NOOP);
                    int rows = execution < warmupExecutions ? 512 : 65_536;
                    for (int row = 0; row < rows; row++) {
                        build.append(row, row);
                    }
                    FrozenHashJoinBuild.IntKeyed snapshot = build.freeze(payloads);
                    if (probe == null) probe = snapshot.newProbe();
                    else probe.reopen();
                    for (int row = 0; row < rows; row++) {
                        Assert.assertTrue(probe.findSingleUnchecked(row));
                        probe.getRecord().getSymA(0).length();
                        probe.find(row);
                        probe.next();
                        probe.getRecord().getSymA(0).length();
                    }
                    probe.close();
                    build.close();
                    long bytes = bean.getCurrentThreadAllocatedBytes() - before;
                    if (execution >= warmupExecutions) allocated += bytes;
                    Assert.assertEquals(0, tracker.getUsed());
                }
                Assert.assertEquals("fresh builds, unseen symbols and forced native growth", 0, allocated);
            } finally {
                Misc.free(probe);
            }
        });
    }

    @Test
    public void testReusableSnapshotRequiresExplicitProbeRebinding() throws Exception {
        assertMemoryLeak(() -> {
            Symbols oldSymbols = new Symbols("old");
            Symbols newSymbols = new Symbols("new");
            RowIdPayloadSource oldPayloads = new RowIdPayloadSource(oldSymbols, rowId -> 0, 0);
            RowIdPayloadSource newPayloads = new RowIdPayloadSource(newSymbols, rowId -> 0, 0);
            FrozenHashJoinBuild.IntProbe probe = null;
            FrozenHashJoinBuild.IntProbe peer = null;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16, true)) {
                build.open(tracker, NOOP);
                build.append(1, 0);
                FrozenHashJoinBuild.IntKeyed snapshot = build.freeze(oldPayloads);
                probe = snapshot.newProbe();
                peer = snapshot.newProbe();
                probe.find(1);
                long oldHandle = probe.next();
                TestUtils.assertEquals("old", probe.getRecord().getSymA(0));
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, probe::reopen);

                build.open(tracker, NOOP);
                for (int row = 0; row < 4096; row++) {
                    build.append(row, row);
                }
                Assert.assertSame(snapshot, build.freeze(newPayloads));
                final FrozenHashJoinBuild.IntProbe stale = probe;
                Assert.assertThrows(AssertionError.class, () -> stale.find(1));
                Assert.assertThrows(AssertionError.class, stale::next);
                Assert.assertThrows(AssertionError.class, () -> stale.findUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> stale.findSingleUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> stale.getSymbolTable(0));
                Assert.assertThrows(AssertionError.class, () -> stale.newSymbolTable(0));
                probe.reopen();
                Assert.assertThrows(AssertionError.class, () -> stale.recordAt(oldHandle));
                // Both table capacity and native backing grew in the new execution.
                probe.findUnchecked(4095);
                Assert.assertTrue(probe.hasNext());
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                probe.find(4095);
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                TestUtils.assertEquals("new", probe.newSymbolTable(0).valueOf(0));
                // Only the two original bindings used the previous execution's source; the new
                // source hands the rebound probe a reader of its own.
                Assert.assertEquals(2, oldSymbols.newSymbolTableCalls);
                Assert.assertEquals(2, newSymbols.newSymbolTableCalls);
                Assert.assertEquals(2, oldPayloads.readerCount);
                Assert.assertEquals(1, newPayloads.readerCount);
                // Rebinding one acquired slot must not revive another slot's view.
                final FrozenHashJoinBuild.IntProbe stalePeer = peer;
                Assert.assertThrows(AssertionError.class, () -> stalePeer.find(1));
                peer.reopen();
                peer.find(1);
                peer.next();
                TestUtils.assertEquals("new", peer.getRecord().getSymA(0));
                probe.close();
                peer.close();
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                Misc.free(probe);
                Misc.free(peer);
            }
        });
    }

    @Test
    public void testSymbolKeyTranslationCachesDistinctKeysAndKeepsEveryBuildRow() throws Exception {
        assertMemoryLeak(() -> {
            // Equal text has different keys in the two dictionaries; IT and FR are absent from the
            // probe, and PT from the build. The build keeps every row: it stores its own keys now.
            Symbols buildSymbols = new Symbols("ES", "IT", "FR", "DE");
            Symbols probeSymbols = new Symbols("DE", "ES", "PT");
            final int nil = SymbolTable.VALUE_IS_NULL;
            FrozenHashJoinBuild.IntProbe probe = null;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16, true);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                // The second execution has no duplicate key, leaving a unique build.
                int[][] executions = {{0, 1, 0, nil, 3, 2, 0, 3, nil}, {1, 0, 2, 3, nil}};
                for (int[] keys : executions) {
                    buildSymbols.resetCounts();
                    probeSymbols.resetCounts();
                    // The payload is the key column itself, read back through the row id.
                    RowIdPayloadSource payloads = new RowIdPayloadSource(buildSymbols, rowId -> keys[(int) rowId], 0);
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild.IntKeyed frozen = build.build(new KeyCursor(buildSymbols, keys), 0, keys.length, -1, payloads);
                    Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                    // The build translates nothing, so no lookup happened while it read its rows.
                    Assert.assertEquals(0, buildSymbols.keyOfCalls);
                    Assert.assertEquals(0, probeSymbols.valueOfCalls);
                    // Every row is kept, including the ones whose symbol the probe lacks.
                    Assert.assertEquals(keys.length, frozen.getRowCount());
                    Assert.assertEquals(5, frozen.getKeyCount());
                    Assert.assertEquals(keys.length == 5, frozen.getRowCount() == frozen.getKeyCount());

                    translator.of(3, tracker, NOOP);
                    Assert.assertEquals(3 * Integer.BYTES, translator.getSizeInBytes());
                    view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                    if (probe == null) {
                        probe = frozen.newProbe();
                    } else {
                        probe.reopen();
                    }
                    // Probe keys: DE is 0 against the build's 3, ES is 1 against 0, PT is absent
                    // from the build, a probe key past the dictionary reads as null, and null
                    // matches null. Two passes, so the second reads the cache.
                    for (int pass = 0; pass < 2; pass++) {
                        for (int probeKey : new int[]{0, 1, 2, 3, nil}) {
                            final int buildKey = view.translate(probeKey);
                            Assert.assertEquals(probeKey == 0 ? 3 : probeKey == 1 ? 0
                                    : probeKey == 2 ? SymbolTable.VALUE_NOT_FOUND : nil, buildKey);
                            probe.find(buildKey);
                            int matches = 0;
                            while (probe.hasNext()) {
                                probe.next();
                                // The payload keeps the build key, which resolves to the same text.
                                Assert.assertEquals(buildKey, probe.getRecord().getInt(0));
                                TestUtils.assertEquals(buildSymbols.valueOf(0, buildKey), probe.getRecord().getSymA(0));
                                matches++;
                            }
                            int expected = 0;
                            for (int key : keys) {
                                if (key == buildKey) {
                                    expected++;
                                }
                            }
                            Assert.assertEquals(expected, matches);
                        }
                    }
                    // One lookup per distinct cached probe key, none per probed row and none for
                    // null; the key past the dictionary is the only one that resolves every time.
                    Assert.assertEquals(5, buildSymbols.keyOfCalls);
                    Assert.assertEquals(5, probeSymbols.valueOfCalls);
                    view.close();
                    translator.close();
                    probe.close();
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            } finally {
                Misc.free(probe);
            }
        });
    }

    @Test
    public void testSymbolKeyTranslationFailureLeavesTheEntryUnresolved() throws Exception {
        assertMemoryLeak(() -> {
            Symbols buildSymbols = new Symbols("ES", "IT");
            Symbols probeSymbols = new Symbols("IT", "ES");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(2, tracker, NOOP);
                view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                buildSymbols.failKeyOfAt = 1;
                Assert.assertThrows(IllegalStateException.class, () -> view.translate(0));
                // The entry stays unresolved, so a later probe resolves it rather than reading
                // whatever the failed lookup would have left behind.
                buildSymbols.failKeyOfAt = 0;
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(2, buildSymbols.keyOfCalls);
                view.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSymbolKeyTranslatorBoundsMemoryAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(15);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                Symbols buildSymbols = new Symbols("a", "b", "c", "d");
                Symbols probeSymbols = new Symbols("d", "c", "b", "a");
                // The cache holds four INT keys: 16 bytes exceed the limit and nothing stays charged.
                CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(4, tracker, NOOP));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, translator.getSizeInBytes());
                translator.close();

                // Cancellation while clearing the cache releases it as well.
                tracker.setLimit(1 << 20);
                CountingSqlExecutionCircuitBreaker cancelled = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        throw CairoException.queryCancelled(1);
                    }
                };
                Assert.assertThrows(CairoException.class, () -> translator.of(4, tracker, cancelled));
                Assert.assertEquals(0, tracker.getUsed());

                translator.of(4, tracker, NOOP);
                Assert.assertEquals(16, tracker.getUsed());
                view.of(translator, probeSymbols.newTable(0), buildSymbols.newTable(0));
                for (int pass = 0; pass < 2; pass++) {
                    for (int key = 0; key < 4; key++) {
                        Assert.assertEquals(3 - key, view.translate(key));
                    }
                }
                Assert.assertEquals(4, buildSymbols.keyOfCalls);
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(SymbolTable.VALUE_IS_NULL));
                Assert.assertEquals(4, buildSymbols.keyOfCalls);
                // A key outside the probe dictionary resolves as the ordinary join resolves it, uncached.
                for (int key : new int[]{4, -1}) {
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(key));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(key));
                }
                Assert.assertEquals(8, buildSymbols.keyOfCalls);

                // Rebinding releases the old cache and resolves every key again for the new dictionaries.
                Symbols grownProbe = new Symbols("a", "b", "c", "d", "e", "f", "g", "h");
                Symbols smallBuild = new Symbols("h", "a");
                translator.of(8, tracker, NOOP);
                Assert.assertEquals(32, tracker.getUsed());
                view.of(translator, grownProbe.newTable(0), smallBuild.newTable(0));
                Assert.assertEquals(1, view.translate(0));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(3));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(3));
                Assert.assertEquals(0, view.translate(7));
                Assert.assertEquals(3, grownProbe.valueOfCalls);

                // An empty probe dictionary needs no cache.
                translator.of(0, tracker, NOOP);
                Assert.assertEquals(0, tracker.getUsed());
                view.of(translator, new Symbols().newTable(0), buildSymbols.newTable(0));
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(SymbolTable.VALUE_IS_NULL));
                view.close();
                view.close();
                translator.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    private static IntList indexes(int... columns) {
        IntList result = new IntList();
        for (int column : columns) {
            result.add(column);
        }
        return result;
    }

    private static void populate(IntHashJoinBuild build, LimitedMemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        build.open(tracker, breaker);
        for (int i = 0; i < 12; i++) {
            build.append(i, i);
        }
        FrozenHashJoinBuild.IntKeyed frozen = build.freeze(new RowIdPayloadSource(POPULATED_SYMBOLS, 0));
        Assert.assertEquals(12, frozen.getRowCount());
    }

    /**
     * Attributes each build check to the IntHashJoinBuild method that issued it. It cancels
     * on a second consecutive check from the fail site, i.e. inside a single rehash loop.
     */
    private static class SiteBreaker extends CountingSqlExecutionCircuitBreaker {
        // The build spans three classes of the join package - the build itself, its row heap
        // and their shared native buffer - so attribution takes the first frame of any of them.
        private static final String BUILD_PACKAGE = "io.questdb.griffin.engine.join.";
        private final String failSite;
        private final StackWalker walker = StackWalker.getInstance();
        private int failedRow = -1;
        private int keyRehashChecks;
        private String previousSite = "";
        private int row;
        private int rowChecks;

        private SiteBreaker(String failSite) {
            super(NOOP);
            this.failSite = failSite;
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            super.statefulThrowExceptionIfTripped();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            super.statefulThrowExceptionIfTrippedNoThrottle();
            onCheck();
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            onCheck();
        }

        private void onCheck() {
            final String site = walker.walk(frames -> frames
                    .filter(frame -> frame.getClassName().startsWith(BUILD_PACKAGE))
                    .findFirst()
                    .map(StackWalker.StackFrame::getMethodName)
                    .orElse(""));
            switch (site) {
                case "append", "appendRow", "appendFrame", "build" -> rowChecks++;
                case "growKeyTable" -> keyRehashChecks++;
                default -> {
                }
            }
            final boolean isRepeatedLoopCheck = site.equals(previousSite);
            previousSite = site;
            if (isRepeatedLoopCheck && site.equals(failSite)) {
                failedRow = row;
                throw CairoException.queryCancelled(1);
            }
        }
    }

    /**
     * Replays INT build keys from column 0, identifies each row by its index, and resolves SYMBOL
     * columns through the given source.
     */
    private static class KeyCursor implements RecordCursor {
        private final int[] keys;
        private final Record record = new Record() {
            @Override
            public int getInt(int col) {
                return keys[row];
            }

            @Override
            public long getRowId() {
                return rowId(row);
            }
        };
        private final Symbols symbols;
        private int row = -1;

        private KeyCursor(Symbols symbols, int[] keys) {
            this.symbols = symbols;
            this.keys = keys;
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return symbols.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return ++row < keys.length;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return symbols.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return keys.length;
        }

        @Override
        public void toTop() {
            row = -1;
        }

        long rowId(int row) {
            return row;
        }
    }

    /**
     * Static dictionaries per source column. Column 0 is the default. Counts lookups and view
     * requests, and returns independent views unless a test asks for one retained view.
     */
    private static class Symbols implements SymbolTableSource {
        private final ObjList<ObjList<String>> dictionaries = new ObjList<>();
        private final IntList requestedColumns = new IntList();
        private int failKeyOfAt;
        private boolean isViewShared;
        private int keyOfCalls;
        private int newSymbolTableCalls;
        private Table sharedView;
        private int valueOfCalls;

        private Symbols(String... values) {
            put(0, values);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return newTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            newSymbolTableCalls++;
            requestedColumns.add(columnIndex);
            if (isViewShared) {
                if (sharedView == null) {
                    sharedView = newTable(columnIndex);
                }
                return sharedView;
            }
            return newTable(columnIndex);
        }

        private Table newTable(int columnIndex) {
            return new Table(dictionaries.getQuick(columnIndex));
        }

        private void put(int columnIndex, String... values) {
            ObjList<String> dictionary = new ObjList<>();
            for (String value : values) {
                dictionary.add(value);
            }
            dictionaries.extendAndSet(columnIndex, dictionary);
        }

        private void resetCounts() {
            keyOfCalls = 0;
            valueOfCalls = 0;
        }

        private String valueOf(int columnIndex, int key) {
            ObjList<String> dictionary = dictionaries.getQuick(columnIndex);
            return key >= 0 && key < dictionary.size() ? dictionary.getQuick(key) : null;
        }

        private class Table implements StaticSymbolTable {
            private final ObjList<String> dictionary;

            private Table(ObjList<String> dictionary) {
                this.dictionary = dictionary;
            }

            @Override
            public boolean containsNullValue() {
                return false;
            }

            @Override
            public int getSymbolCount() {
                return dictionary.size();
            }

            @Override
            public int keyOf(CharSequence value) {
                if (++keyOfCalls == failKeyOfAt) {
                    throw new IllegalStateException("injected symbol lookup failure");
                }
                if (value == null) {
                    return SymbolTable.VALUE_IS_NULL;
                }
                for (int i = 0, n = dictionary.size(); i < n; i++) {
                    if (Chars.equals(value, dictionary.getQuick(i))) {
                        return i;
                    }
                }
                return SymbolTable.VALUE_NOT_FOUND;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                valueOfCalls++;
                return key >= 0 && key < dictionary.size() ? dictionary.getQuick(key) : null;
            }
        }
    }
}
