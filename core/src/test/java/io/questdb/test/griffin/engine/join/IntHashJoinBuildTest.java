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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
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

    @Test
    public void testCompressedIncrementalHeapBoundAndGrowthCap() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            Record source = new Record() {
                @Override
                public int getInt(int column) {
                    return 42;
                }
            };
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT)) {
                build.open(tracker, NOOP);
                build.append(17, source);
                Field rowsField = IntHashJoinBuild.class.getDeclaredField("rows");
                rowsField.setAccessible(true);
                Object rows = rowsField.get(build);
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
                build.append(17, source);
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                probe.find(17);
                probe.next();
                Assert.assertEquals(42, probe.getRecord().getInt(0));
                probe.next();
                Assert.assertEquals(42, probe.getRecord().getInt(0));
                Assert.assertFalse(probe.hasNext());
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
            AtomicInteger value = new AtomicInteger();
            Record source = new Record() {
                @Override
                public int getInt(int col) {
                    return value.get();
                }
            };
            try (IntHashJoinBuild build = newBuild(4, 24, ColumnType.INT)) {
                build.open(null, NOOP);
                build.append(keys[0], source);
                value.set(1);
                build.append(keys[1], source);
                Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                keysField.setAccessible(true);
                Object table = keysField.get(build);
                Field addressField = table.getClass().getDeclaredField("address");
                addressField.setAccessible(true);
                long address = addressField.getLong(table);
                long offset = 0x80000000L << 3;
                Unsafe.putInt(address + 3 * 8 + 4, CompressedOffsets.compressBiased8(offset));
                Unsafe.putInt(address + 4, CompressedOffsets.compressBiased8(offset + 16));
                value.set(2);
                build.append(keys[2], source); // Rehash both negative references into colliding destination slots.
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                Field rowsField = probe.getClass().getDeclaredField("payloadRowsAddress");
                rowsField.setAccessible(true);
                long realRows = rowsField.getLong(probe);
                for (int i = 0; i < keys.length; i++) {
                    rowsField.setLong(probe, i < 2 ? realRows - offset : realRows);
                    probe.findUnchecked(keys[i]);
                    Assert.assertTrue(probe.hasNext());
                    Assert.assertEquals(i < 2 ? offset + i * 16L : 32, probe.next());
                    Assert.assertEquals(i, probe.getRecord().getInt(0));
                    Assert.assertFalse(probe.hasNext());
                    Assert.assertTrue(probe.findSingleUnchecked(keys[i]));
                    Assert.assertEquals(i, probe.getRecord().getInt(0));
                }
                rowsField.setLong(probe, realRows);
            }
        });
    }

    @Test
    public void testCompressedUnsignedProbeReferences() throws Exception {
        assertMemoryLeak(() -> {
            for (int count : new int[]{1, 2}) {
                try (IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT)) {
                    build.open(null, NOOP);
                    Record source = new Record() {
                        @Override
                        public int getInt(int col) {
                            return 42;
                        }
                    };
                    for (int i = 0; i < count; i++) {
                        build.append(17, source);
                    }
                    FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                    Field keysField = IntHashJoinBuild.class.getDeclaredField("keys");
                    keysField.setAccessible(true);
                    Object keys = keysField.get(build);
                    Field addressField = keys.getClass().getDeclaredField("address");
                    addressField.setAccessible(true);
                    long slot = addressField.getLong(keys) + ((int) Hash.hashInt64(17) & 1) * 8L;
                    Field rowsField = probe.getClass().getDeclaredField("payloadRowsAddress");
                    rowsField.setAccessible(true);
                    long realRows = rowsField.getLong(probe);
                    // Simulate a large relative heap. Every dereference still lands
                    // in the real, owned payload rows; no 16/32 GiB allocation is needed.
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
        });
    }

    @Test
    public void testCompressedHeapBoundBeforeAllocationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            final long limit = CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE;
            Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, limit + 1, ColumnType.INT));
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 8, ColumnType.INT);
                 RecordCursorFactory factory = select("SELECT 17::INT k, 42::INT v FROM long_sequence(1)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                for (long hint : new long[]{limit / 16 + 1, Long.MAX_VALUE}) {
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, hint));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
                    cursor.toTop();
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild.Probe probe = build.build(cursor, 0, 1).newProbe();
                    Assert.assertTrue(probe.findSingleUnchecked(17));
                    Assert.assertEquals(17, probe.getRecord().getInt(0));
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    cursor.toTop();
                }
                // The largest legal hint reaches tracked allocation and is rejected
                // by this tiny memory limit, rather than wrapping its compressed offset.
                build.open(tracker, NOOP);
                CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0, limit / 16));
                TestUtils.assertContains(error.getFlyweightMessage(), "query memory limit exceeded");
                TestUtils.assertContains(error.getFlyweightMessage(), ", size=" + limit + ",");
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testUniqueDuplicateAndEmptyBuildReuseAcrossPayloadWidths() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger row = new AtomicInteger();
            Record source = new Record() {
                @Override
                public int getInt(int column) {
                    return row.get();
                }

                @Override
                public double getDouble(int column) {
                    return row.get() * 0.5;
                }

                @Override
                public CharSequence getSymA(int column) {
                    return switch (row.get() % 3) {
                        case 0 -> "Aa";
                        case 1 -> "BB";
                        default -> null;
                    };
                }
            };
            // Empty, narrow and wide layouts, with growth through page/cache-line boundaries.
            for (int width : new int[]{0, 1, 3, 17}) {
                ArrayColumnTypes types = new ArrayColumnTypes();
                IntList columns = new IntList();
                for (int i = 0; i < width; i++) {
                    types.add(i == 0 ? ColumnType.INT : i == 1 ? ColumnType.SYMBOL : ColumnType.DOUBLE);
                    columns.add(i);
                }
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(32 * 1024 * 1024);
                     IntHashJoinBuild build = new IntHashJoinBuild(types, columns, 2, 8, true)) {
                    FrozenHashJoinBuild.Probe reusable = null;
                    // Recompute uniqueness on empty -> unique -> late duplicate -> skew -> unique reuse.
                    for (int execution = 0; execution < 5; execution++) {
                        build.open(tracker, NOOP);
                        Map<Integer, List<Integer>> expected = new HashMap<>();
                        int count = execution == 0 ? 0 : 4097;
                        for (int r = 0; r < count; r++) {
                            row.set(r);
                            int key = execution == 3 ? (r % 7) : execution == 2 && r == count - 1 ? 0 : r;
                            key = key == 1 ? Numbers.INT_NULL : key == 2 ? Integer.MAX_VALUE : key == 3 ? -1 : key;
                            expected.computeIfAbsent(key, ignored -> new ArrayList<>()).add(r);
                            build.append(key, source);
                        }
                        FrozenHashJoinBuild frozen = build.freeze();
                        Assert.assertEquals(count, frozen.getRowCount());
                        Assert.assertEquals(expected.size(), frozen.getKeyCount());
                        Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                        if (reusable == null) {
                            reusable = frozen.newProbe();
                        } else {
                            reusable.reopen();
                        }
                        List<Long> handles = new ArrayList<>();
                        List<Integer> payloadRows = new ArrayList<>();
                        for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet()) {
                            reusable.findUnchecked(entry.getKey());
                            List<Integer> matches = new ArrayList<>();
                            while (reusable.hasNext()) {
                                long handle = reusable.next();
                                int value = width == 0 ? 0 : reusable.getRecord().getInt(0);
                                matches.add(value);
                                handles.add(handle);
                                payloadRows.add(value);
                            }
                            Assert.assertEquals(entry.getValue().size(), matches.size());
                            if (width > 0) {
                                matches.sort(Integer::compare);
                                Assert.assertEquals(entry.getValue(), matches);
                            }
                            if (count == expected.size()) {
                                Assert.assertTrue(reusable.findSingleUnchecked(entry.getKey()));
                                if (width > 0) {
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
                            int value = payloadRows.get(h);
                            if (width > 0) {
                                Assert.assertEquals(value, reusable.getRecord().getInt(0));
                            }
                            for (int column = 2; column < width; column++) {
                                Assert.assertEquals(value * 0.5, reusable.getRecord().getDouble(column), 0);
                            }
                            if (width > 1) {
                                row.set(value);
                                TestUtils.assertEquals(source.getSymA(1), reusable.getRecord().getSymA(1));
                            }
                        }
                        build.close();
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                }
            }
        });
    }

    @Test
    public void testAllPayloadTypesAndPrunedMappingAfterCursorClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table payload (b boolean, by byte, sh short, ch char, i int, l long, d date, ts timestamp, "
                    + "ns timestamp_ns, f float, dbl double, s symbol, unused string)");
            execute("insert into payload values (true, 12, 123, 'Q', 42, 9876543210L, 100, 123456789, "
                    + "123456789123456789L, 1.25, 2.5, 'ES', 'ignored'), "
                    + "(null, null, null, null, null, null, null, null, null, null, null, null, 'ignored')");
            String[] expressions = {"s", "dbl", "f", "ns", "ts", "d", "l", "i", "ch", "sh", "by", "b"};
            ArrayColumnTypes types = new ArrayColumnTypes();
            IntList mapping = new IntList();
            try (RecordCursorFactory source = select("payload")) {
                for (String name : expressions) {
                    int index = source.getMetadata().getColumnIndex(name);
                    types.add(source.getMetadata().getColumnType(index));
                    mapping.add(index);
                }
                try (IntHashJoinBuild build = new IntHashJoinBuild(types, mapping, 2, 16)) {
                    build.open(null, NOOP);
                    try (RecordCursor cursor = source.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(ColumnType.FLOAT, types.getColumnType(2));
                        Assert.assertEquals(1.25f, cursor.getRecord().getFloat(mapping.getQuick(2)), 0);
                        build.append(42, cursor.getRecord());
                        Assert.assertTrue(cursor.hasNext());
                        build.append(43, cursor.getRecord());
                    }
                    FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                    probe.find(42);
                    Assert.assertTrue(probe.hasNext());
                    probe.next();
                    Record record = probe.getRecord();
                    TestUtils.assertEquals("ES", record.getSymA(0));
                    Assert.assertEquals(2.5, record.getDouble(1), 0);
                    Assert.assertEquals(1.25f, record.getFloat(2), 0);
                    Assert.assertEquals(123456789123456789L, record.getTimestamp(3));
                    Assert.assertEquals(123456789L, record.getTimestamp(4));
                    Assert.assertEquals(100L, record.getDate(5));
                    Assert.assertEquals(9876543210L, record.getLong(6));
                    Assert.assertEquals(42, record.getInt(7));
                    Assert.assertEquals('Q', record.getChar(8));
                    Assert.assertEquals(123, record.getShort(9));
                    Assert.assertEquals(12, record.getByte(10));
                    Assert.assertTrue(record.getBool(11));
                    Assert.assertFalse(probe.hasNext());
                    probe.find(43);
                    probe.next();
                    Assert.assertNull(record.getSymA(0));
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, record.getInt(0));
                    Assert.assertTrue(Double.isNaN(record.getDouble(1)));
                    Assert.assertTrue(Float.isNaN(record.getFloat(2)));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getTimestamp(3));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getTimestamp(4));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getDate(5));
                    Assert.assertEquals(Numbers.LONG_NULL, record.getLong(6));
                    Assert.assertEquals(Numbers.INT_NULL, record.getInt(7));
                    Assert.assertEquals(0, record.getChar(8));
                    Assert.assertEquals(0, record.getShort(9));
                    Assert.assertEquals(0, record.getByte(10));
                    Assert.assertFalse(record.getBool(11));
                    Assert.assertFalse(probe.hasNext());
                }
            }
        });
    }

    @Test
    public void testKnownBuildSizeReservesTrackedRowsAndOverflowReuses() throws Exception {
        assertMemoryLeak(() -> {
            // Enough for the exact payload and two key slots, but not a doubling copy.
            final long capacity = 10_000 * 16L + 16;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(capacity);
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.DOUBLE), indexes(1), 2, 16);
                 RecordCursorFactory factory = select("select 1::int k, x*0.5 v from long_sequence(10000)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(10_000, cursor.size());
                for (int execution = 0; execution < 2; execution++) {
                    cursor.toTop();
                    build.open(tracker, NOOP);
                    FrozenHashJoinBuild frozen = build.build(cursor, 0, cursor.size());
                    Assert.assertEquals(10_000, frozen.getRowCount());
                    Assert.assertEquals(capacity, tracker.getUsed());
                    Assert.assertEquals(capacity, frozen.getSizeInBytes());
                    FrozenHashJoinBuild.Probe probe = frozen.newProbe();
                    probe.find(1);
                    probe.next();
                    Assert.assertEquals(5_000, probe.getRecord().getDouble(0), 0);
                    build.close();
                    Assert.assertEquals(0, tracker.getUsed());
                    build.open(tracker, NOOP);
                    CairoException error = Assert.assertThrows(CairoException.class,
                            () -> build.build(cursor, 0, Long.MAX_VALUE));
                    TestUtils.assertContains(error.getFlyweightMessage(), "hash join build buffer overflow");
                    Assert.assertEquals(0, tracker.getUsed());
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
                public double getDouble(int col) {
                    return consumed.get();
                }

                @Override
                public int getInt(int col) {
                    return 1;
                }
            };
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, config);
                 IntHashJoinBuild build = newBuild(2, 1_600_000, ColumnType.DOUBLE);
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
                        CairoException error = Assert.assertThrows(CairoException.class, () -> build.build(cursor, 0));
                        Assert.assertEquals(mode == 1, error.isCancellation());
                        Assert.assertEquals("the build observes interruption at the cursor's frame check",
                                frameRows, consumed.get());
                    } else {
                        FrozenHashJoinBuild frozen = build.build(cursor, 0);
                        Assert.assertEquals(100_000, frozen.getRowCount());
                        // Only open, the initial slot clear and freeze check; appended rows do not.
                        Assert.assertTrue("build checks must not scale with rows: " + clockReads.get(),
                                clockReads.get() <= 3);
                        FrozenHashJoinBuild.Probe probe = frozen.newProbe();
                        probe.find(1);
                        probe.next();
                        Assert.assertEquals(100_000, probe.getRecord().getDouble(0), 0);
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
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                Source record = new Source();
                CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                populate(build, record, tracker, counting);
                long checks = counting.getCheckCount();
                build.close();
                // Includes initialization, copied rows, hash/row/dictionary growth and freeze.
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
                        populate(build, record, tracker, breaker);
                        Assert.fail("expected cancellation at check " + failAt);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    }
                    Assert.assertEquals("cancelled build releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                populate(build, record, tracker, NOOP);
                Assert.assertTrue(tracker.getUsed() > 0);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRehashChecksCancellationPerMiBOfSlots() throws Exception {
        assertMemoryLeak(() -> {
            // Row 65_536 rehashes 2 MiB of symbol slots and row 131_072 rehashes 2 MiB of key slots.
            final int rows = 131_073;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL)) {
                io.questdb.std.str.StringSink text = new io.questdb.std.str.StringSink();
                Source source = new Source();
                source.text = text;
                for (String failSite : new String[]{null, "growKeyTable", "growSymbolTable"}) {
                    SiteBreaker breaker = new SiteBreaker(failSite);
                    build.open(tracker, breaker);
                    try {
                        for (int row = 0; row < rows; row++) {
                            breaker.row = row;
                            text.clear();
                            text.put(row);
                            build.append(row, source);
                        }
                        Assert.assertNull("expected cancellation inside " + failSite, failSite);
                        Assert.assertEquals(rows, build.freeze().getRowCount());
                        Assert.assertEquals("appended and interned rows must not check", 0, breaker.rowChecks);
                        // One check per rehash plus one per MiB of old slots: 19 key and 22 symbol checks.
                        Assert.assertTrue("key rehash checks: " + breaker.keyRehashChecks,
                                breaker.keyRehashChecks > 0 && breaker.keyRehashChecks < 64);
                        Assert.assertTrue("symbol rehash checks: " + breaker.symbolRehashChecks,
                                breaker.symbolRehashChecks > 0 && breaker.symbolRehashChecks < 64);
                        build.close();
                    } catch (CairoException e) {
                        Assert.assertNotNull("unexpected interruption: " + e.getFlyweightMessage(), failSite);
                        Assert.assertTrue(e.isCancellation());
                        // The first rehash of more than 1 MiB of old slots checks again inside its loop.
                        Assert.assertEquals(failSite.equals("growKeyTable") ? 131_072 : 65_536, breaker.failedRow);
                    }
                    Assert.assertEquals("rehash cancellation releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                build.open(tracker, NOOP);
                text.clear();
                text.put("reused");
                build.append(1, source);
                Assert.assertEquals(1, build.freeze().getRowCount());
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCancellationDuringPayloadCopy() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 IntHashJoinBuild build = newBuild(2, 1024 * 1024, ColumnType.DOUBLE)) {
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
                Source record = new Source();
                for (int i = 0; i < 65536; i++) {
                    build.append(1, record);
                }
                // Cancellation after allocating the destination, while source and destination coexist.
                Assert.assertThrows(CairoException.class, () -> build.append(1, record));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(1, record);
                Assert.assertEquals(1, build.freeze().getRowCount());
            }
        });
    }

    @Test
    public void testConcurrentLookupsAndIndependentSymbolFlyweights() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                build.open(null, NOOP);
                Source source = new Source();
                for (int i = 0; i < 10_000; i++) {
                    source.row = i;
                    source.text = (i & 1) == 0 ? "ES" : "IT";
                    build.append(i % 100, source);
                }
                FrozenHashJoinBuild frozen = build.freeze();
                ExecutorService executor = Executors.newFixedThreadPool(4);
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                try {
                    for (int worker = 0; worker < 4; worker++) {
                        final int shift = worker;
                        futures.add(executor.submit(() -> {
                            // Task submission and the latch publish the completed build.
                            FrozenHashJoinBuild.Probe a = frozen.newProbe();
                            FrozenHashJoinBuild.Probe b = frozen.newProbe();
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
                }
            }
        });
    }

    @Test
    public void testEmptyBuildAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL)) {
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
                Assert.assertThrows(IllegalStateException.class, build::freeze);
                build.open(tracker, NOOP);
                FrozenHashJoinBuild frozen = build.freeze();
                Assert.assertEquals(0, frozen.getRowCount());
                Assert.assertEquals(0, frozen.getKeyCount());
                Assert.assertEquals(tracker.getUsed(), frozen.getSizeInBytes());
                FrozenHashJoinBuild.Probe probe = frozen.newProbe();
                probe.find(Numbers.INT_NULL);
                Assert.assertFalse(probe.hasNext());
                Assert.assertNull(probe.getSymbolTable(0).valueOf(SymbolTable.VALUE_IS_NULL));
                Assert.assertNull(probe.newSymbolTable(0).valueBOf(SymbolTable.VALUE_IS_NULL));
                Assert.assertTrue(probe.getSymbolTable(0).supportsKeyValueAccess());
                Assert.assertThrows(IllegalStateException.class, () -> build.append(1, new Source()));
                Assert.assertThrows(IllegalStateException.class, () -> build.open(tracker, NOOP));
                build.close();
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, () -> frozen.newProbe());
                build.open(tracker, NOOP);
                Source source = new Source();
                source.text = "fresh";
                build.append(1, source);
                probe = build.freeze().newProbe();
                probe.find(1);
                probe.next();
                Assert.assertEquals(0, probe.getRecord().getInt(0));
                TestUtils.assertEquals("fresh", probe.getRecord().getSymA(0));
            }
        });
    }

    @Test
    public void testEmptyPayloadPreservesDuplicateCounts() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 8)) {
                build.open(null, NOOP);
                for (int i = 0; i < 100; i++) {
                    build.append(Numbers.INT_NULL, new Source());
                }
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                probe.find(Numbers.INT_NULL);
                for (int i = 0; i < 100; i++) {
                    Assert.assertTrue(probe.hasNext());
                    probe.next();
                }
                Assert.assertFalse(probe.hasNext());
            }
        });
    }

    @Test
    public void testHashCollisionsZeroNegativeAndNullKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(16, 16, ColumnType.DOUBLE)) {
                build.open(null, NOOP);
                Source source = new Source();
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
                        source.row = i * 10 + j;
                        build.append(keys.getQuick(i), source);
                    }
                }
                FrozenHashJoinBuild frozen = build.freeze();
                Assert.assertEquals(8, frozen.getKeyCount());
                Assert.assertEquals(24, frozen.getRowCount());
                FrozenHashJoinBuild.Probe probe = frozen.newProbe();
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
        });
    }

    @Test
    public void testInvalidLayout() {
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(3, 16, ColumnType.INT));
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, 0, ColumnType.INT));
        Assert.assertThrows(IllegalArgumentException.class, () -> newBuild(2, 16, ColumnType.STRING));
        Assert.assertThrows(IllegalArgumentException.class, () -> new IntHashJoinBuild(new ArrayColumnTypes(), indexes(0), 2, 16));
    }

    @Test
    public void testHighSymbolCardinalityAndDuplicateGrowthAccounting() throws Exception {
        assertMemoryLeak(() -> {
            StringBuilder symbol = new StringBuilder();
            Record source = new Record() {
                @Override
                public CharSequence getSymA(int col) {
                    return symbol;
                }
            };
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL)) {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                for (long limit : new long[]{4096, 16384, 65536, 0}) {
                    tracker.setLimit(limit);
                    try {
                        build.open(tracker, NOOP);
                        for (int i = 0; i < 8192; i++) {
                            symbol.setLength(0);
                            symbol.append("country-with-a-long-name-").append(i);
                            build.append(i % 257, source);
                            Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                            Assert.assertEquals(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) - baseline, tracker.getUsed());
                        }
                        Assert.assertEquals(0, limit);
                        FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                        probe.find(0);
                        int matches = 0;
                        while (probe.hasNext()) {
                            probe.next();
                            TestUtils.assertEquals("country-with-a-long-name-" + (31 - matches) * 257, probe.getRecord().getSymA(0));
                            matches++;
                        }
                        Assert.assertEquals(32, matches);
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
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                // Sweep byte limits through all small hash, row, symbol-index and text allocations.
                int failures = 0;
                for (long limit = 1; limit <= 16_384; limit++) {
                    tracker.setLimit(limit);
                    try {
                        populate(build, new Source(), tracker, NOOP);
                        Assert.assertEquals(build.getSizeInBytes(), tracker.getUsed());
                        build.close();
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        failures++;
                        Assert.assertEquals(0, build.getSizeInBytes());
                    }
                    Assert.assertEquals("all partial build allocations must be released", 0, tracker.getUsed());
                }
                Assert.assertTrue(failures > 1000);
                tracker.setLimit(1_000_000);
                populate(build, new Source(), tracker, NOOP);
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
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 32, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                Assert.assertEquals(48, tracker.getUsed());
                Assert.assertThrows(CairoException.class, () -> build.append(2, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(80);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(2, new Source());
                Assert.assertEquals(64, build.freeze().getSizeInBytes());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesPayloadGrowthPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Unique key count stays at one: only the duplicate payload buffer grows.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(48);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                // Final 16-byte hash + 32-byte payload fits, old 16-byte payload is still live.
                Assert.assertThrows(CairoException.class, () -> build.append(1, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(64);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(1, new Source());
                Assert.assertEquals(48, build.freeze().getSizeInBytes());
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
                 IntHashJoinBuild build = new IntHashJoinBuild(new ArrayColumnTypes().add(ColumnType.DOUBLE), indexes(1), 2, 16)) {
                build.open(null, NOOP);
                FrozenHashJoinBuild frozen;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    frozen = build.build(cursor, 0);
                }
                FrozenHashJoinBuild.Probe probe = frozen.newProbe();
                probe.find(Numbers.INT_NULL);
                probe.next();
                Assert.assertEquals(2.0, probe.getRecord().getDouble(0), 0);
                probe.next();
                Assert.assertEquals(1.0, probe.getRecord().getDouble(0), 0);
                Assert.assertFalse(probe.hasNext());
            }
        });
    }

    @Test
    public void testRandomizedGrowthAgainstMultimap() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                Map<Integer, List<Integer>> expected = new HashMap<>();
                build.open(null, NOOP);
                Source record = new Source();
                Rnd rnd = new Rnd(130, 131);
                for (int i = 0; i < 20_000; i++) {
                    int key = rnd.nextInt(2000) - 1000;
                    record.row = i;
                    build.append(key, record);
                    expected.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
                }
                FrozenHashJoinBuild frozen = build.freeze();
                Assert.assertEquals(expected.size(), frozen.getKeyCount());
                Assert.assertEquals(20_000, frozen.getRowCount());
                FrozenHashJoinBuild.Probe probe = frozen.newProbe();
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
        });
    }

    @Test
    public void testSourceGetterFailureClosesPartialBuild() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(4096);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                Source source = new Source();
                source.text = "ES";
                build.append(1, source);
                Record broken = new Record() {
                    @Override
                    public CharSequence getSymA(int col) {
                        return "allocated before failure";
                    }

                    @Override
                    public double getDouble(int col) {
                        throw new IllegalStateException("source getter failed");
                    }
                };
                Assert.assertThrows(IllegalStateException.class, () -> build.append(2, broken));
                Assert.assertEquals(0, tracker.getUsed());
                build.open(tracker, NOOP);
                build.append(2, source);
                Assert.assertEquals(1, build.freeze().getRowCount());
            }
        });
    }

    @Test
    public void testSymbolOwnershipHashCollisionsAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                build.open(null, NOOP);
                StringBuilder text = new StringBuilder();
                String[] values = {null, "", "Aa", "BB", "AaAa", "BBBB", "AaBB", "BBAa", "ES", "IT", "国家🌞", "ES"};
                Record source = new Record() {
                    @Override
                    public CharSequence getSymA(int col) {
                        return text;
                    }

                    @Override
                    public double getDouble(int col) {
                        return Double.NaN;
                    }
                };
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        build.append(i, new Source());
                    } else {
                        text.setLength(0);
                        text.append(values[i]);
                        build.append(i, source);
                    }
                }
                text.setLength(0);
                text.append("source overwritten");
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe();
                int es = -1;
                for (int i = 0; i < values.length; i++) {
                    probe.find(i);
                    probe.next();
                    Record record = probe.getRecord();
                    TestUtils.assertEquals(values[i], record.getSymA(0));
                    TestUtils.assertEquals(values[i], record.getSymB(1));
                    Assert.assertEquals(record.getInt(0), record.getInt(1));
                    if (values[i] == null) {
                        Assert.assertEquals(SymbolTable.VALUE_IS_NULL, record.getInt(0));
                    } else {
                        Assert.assertTrue(Double.isNaN(record.getDouble(2)));
                        Assert.assertTrue(record.getInt(0) >= 0);
                    }
                    if ("ES".equals(values[i])) {
                        if (es >= 0) {
                            Assert.assertEquals(es, record.getInt(0));
                        }
                        es = record.getInt(0);
                    }
                }
                SymbolTable a = probe.getSymbolTable(0);
                SymbolTable b = probe.newSymbolTable(0);
                CharSequence first = a.valueOf(1); // Aa
                a.valueBOf(2); // BB: independent B flyweight
                b.valueOf(3);
                probe.getSymbolTable(1).valueOf(4);
                TestUtils.assertEquals("Aa", first);
                // Ten distinct values: keys outside [0, 10) resolve to null, as in SymbolMapReaderImpl.
                TestUtils.assertEquals("国家🌞", a.valueOf(9));
                for (int key : new int[]{-1, SymbolTable.VALUE_NOT_FOUND, 10, Integer.MAX_VALUE}) {
                    Assert.assertNull(a.valueOf(key));
                    Assert.assertNull(b.valueBOf(key));
                }
            }
        });
    }

    @Test
    public void testReusableBuildGrowthDoesNotAllocateHeap() throws Exception {
        assertMemoryLeak(() -> {
            com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            org.junit.Assume.assumeTrue(bean.isThreadAllocatedMemorySupported());
            bean.setThreadAllocatedMemoryEnabled(true);
            ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.SYMBOL);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(0), 2, 16, true)) {
                Source source = new Source();
                io.questdb.std.str.StringSink text = new io.questdb.std.str.StringSink(64);
                source.text = text;
                FrozenHashJoinBuild.Probe probe = null;
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
                        text.clear();
                        text.put(row);
                        build.append(row, source);
                    }
                    FrozenHashJoinBuild snapshot = build.freeze();
                    if (probe == null) probe = snapshot.newProbe();
                    else probe.reopen();
                    for (int row = 0; row < rows; row++) {
                        Assert.assertTrue(probe.findSingleUnchecked(row));
                        probe.getRecord().getSymA(0).length();
                        probe.find(row);
                        probe.next();
                        probe.getRecord().getSymA(0).length();
                    }
                    build.close();
                    long bytes = bean.getCurrentThreadAllocatedBytes() - before;
                    if (execution >= warmupExecutions) allocated += bytes;
                    Assert.assertEquals(0, tracker.getUsed());
                }
                Assert.assertEquals("fresh builds, unseen symbols and forced native growth", 0, allocated);
            }
        });
    }

    @Test
    public void testReusableSnapshotRequiresExplicitProbeRebinding() throws Exception {
        assertMemoryLeak(() -> {
            ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.SYMBOL);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 IntHashJoinBuild build = new IntHashJoinBuild(types, indexes(0), 2, 16, true)) {
                Source source = new Source();
                source.text = "old";
                build.open(tracker, NOOP);
                build.append(1, source);
                FrozenHashJoinBuild snapshot = build.freeze();
                FrozenHashJoinBuild.Probe probe = snapshot.newProbe();
                FrozenHashJoinBuild.Probe peer = snapshot.newProbe();
                SymbolTable symbols = probe.newSymbolTable(0);
                probe.find(1);
                long oldHandle = probe.next();
                TestUtils.assertEquals("old", symbols.valueOf(0));
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertThrows(IllegalStateException.class, probe::reopen);

                build.open(tracker, NOOP);
                source.text = "new";
                for (int row = 0; row < 4096; row++) {
                    build.append(row, source);
                }
                Assert.assertSame(snapshot, build.freeze());
                Assert.assertThrows(AssertionError.class, () -> probe.find(1));
                Assert.assertThrows(AssertionError.class, probe::next);
                Assert.assertThrows(AssertionError.class, () -> probe.findUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> probe.findSingleUnchecked(1));
                Assert.assertThrows(AssertionError.class, () -> symbols.valueOf(0));
                probe.reopen();
                Assert.assertThrows(AssertionError.class, () -> probe.recordAt(oldHandle));
                // Both table capacity and native backing grew in the new execution.
                probe.findUnchecked(4095);
                Assert.assertTrue(probe.hasNext());
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                probe.find(4095);
                probe.next();
                TestUtils.assertEquals("new", probe.getRecord().getSymA(0));
                // Rebinding one acquired slot must not revive another slot's view.
                Assert.assertThrows(AssertionError.class, () -> peer.find(1));
                peer.reopen();
                peer.find(1);
                peer.next();
                TestUtils.assertEquals("new", peer.getRecord().getSymA(0));
                io.questdb.std.Misc.freeIfCloseable(symbols);
                SymbolTable reused = probe.newSymbolTable(0);
                Assert.assertSame(symbols, reused);
                TestUtils.assertEquals("new", reused.valueOf(0));
                io.questdb.std.Misc.freeIfCloseable(reused);
                build.close();
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

    private static IntHashJoinBuild newBuild(int slots, long rowCapacity, int... columnTypes) {
        ArrayColumnTypes types = new ArrayColumnTypes();
        IntList columns = new IntList();
        for (int i = 0; i < columnTypes.length; i++) {
            types.add(columnTypes[i]);
            columns.add(i);
        }
        return new IntHashJoinBuild(types, columns, slots, rowCapacity);
    }

    private static void populate(IntHashJoinBuild build, Source record, LimitedMemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        build.open(tracker, breaker);
        for (int i = 0; i < 12; i++) {
            record.row = i;
            record.text = "symbol-" + i;
            build.append(i, record);
        }
        FrozenHashJoinBuild frozen = build.freeze();
        Assert.assertEquals(12, frozen.getRowCount());
    }

    /**
     * Attributes each build check to the IntHashJoinBuild method that issued it. It cancels
     * on a second consecutive check from the fail site, i.e. inside a single rehash loop.
     */
    private static class SiteBreaker extends CountingSqlExecutionCircuitBreaker {
        private final String failSite;
        private final StackWalker walker = StackWalker.getInstance();
        private int failedRow = -1;
        private int keyRehashChecks;
        private String previousSite = "";
        private int row;
        private int rowChecks;
        private int symbolRehashChecks;

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
                    .filter(frame -> frame.getClassName().startsWith(IntHashJoinBuild.class.getName()))
                    .findFirst()
                    .map(StackWalker.StackFrame::getMethodName)
                    .orElse(""));
            switch (site) {
                case "append", "appendRow", "build", "intern", "symbolEquals" -> rowChecks++;
                case "growKeyTable" -> keyRehashChecks++;
                case "growSymbolTable" -> symbolRehashChecks++;
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

    private static class Source implements Record {
        private int row;
        private CharSequence text;

        @Override
        public double getDouble(int col) {
            return row + 0.25;
        }

        @Override
        public CharSequence getSymA(int col) {
            return text;
        }
    }
}
