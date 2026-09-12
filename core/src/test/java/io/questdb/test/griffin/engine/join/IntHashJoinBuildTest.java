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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IntHashJoinBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;

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
                    FrozenHashJoinBuild.Probe probe = build.freeze().newProbe(NOOP);
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
    public void testCancellationAtEveryBuildCheckAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1_000_000);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.SYMBOL, ColumnType.DOUBLE)) {
                Source record = new Source();
                CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                populate(build, record, tracker, counting);
                long checks = counting.getCheckCount();
                build.close();
                // Includes initialization, hash/row/dictionary growth, collisions and freeze.
                for (long failAt = 1; failAt <= checks; failAt++) {
                    final long failureCheck = failAt;
                    CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                        @Override
                        public void statefulThrowExceptionIfTripped() {
                            super.statefulThrowExceptionIfTripped();
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
    public void testCancellationDuringPayloadCopy() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(8 * 1024 * 1024);
                 IntHashJoinBuild build = newBuild(2, 1024 * 1024, ColumnType.DOUBLE)) {
                CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTripped() {
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
    public void testCancellationInsideDuplicateIteration() throws Exception {
        assertMemoryLeak(() -> {
            try (IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                build.open(null, NOOP);
                Source source = new Source();
                for (int i = 0; i < 100_000; i++) {
                    source.row = i;
                    build.append(1, source);
                }
                CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
                    @Override
                    public void statefulThrowExceptionIfTripped() {
                        super.statefulThrowExceptionIfTripped();
                        if (getCheckCount() == 10) {
                            throw CairoException.queryCancelled(1);
                        }
                    }
                };
                FrozenHashJoinBuild frozen = build.freeze();
                FrozenHashJoinBuild.Probe probe = frozen.newProbe(breaker);
                probe.find(1);
                int visited = 0;
                try {
                    while (probe.hasNext()) {
                        probe.next();
                        visited++;
                    }
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                Assert.assertEquals(8, visited);
                // Probe cancellation must not free storage still visible to other slots.
                probe = frozen.newProbe(NOOP);
                probe.find(1);
                Assert.assertTrue(probe.hasNext());
                probe.next();
                Assert.assertEquals(99999.25, probe.getRecord().getDouble(0), 0);
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
                            FrozenHashJoinBuild.Probe a = frozen.newProbe(NOOP);
                            FrozenHashJoinBuild.Probe b = frozen.newProbe(NOOP);
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
                FrozenHashJoinBuild.Probe probe = frozen.newProbe(NOOP);
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
                Assert.assertThrows(IllegalStateException.class, () -> frozen.newProbe(NOOP));
                build.open(tracker, NOOP);
                Source source = new Source();
                source.text = "fresh";
                build.append(1, source);
                probe = build.freeze().newProbe(NOOP);
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
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe(NOOP);
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
                FrozenHashJoinBuild.Probe probe = frozen.newProbe(NOOP);
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
            // Two slots (32 bytes), 32-byte row capacity. Rehash needs old 32 + new 64 + rows 32 = 128.
            // The final map and two rows fit in 96 bytes, which must still fail during rehash.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(96);
                 IntHashJoinBuild build = newBuild(2, 32, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                Assert.assertEquals(64, tracker.getUsed());
                Assert.assertThrows(CairoException.class, () -> build.append(2, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(128);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(2, new Source());
                Assert.assertEquals(96, build.freeze().getSizeInBytes());
            }
        });
    }

    @Test
    public void testMemoryLimitIncludesPayloadGrowthPeak() throws Exception {
        assertMemoryLeak(() -> {
            // Unique key count stays at one: only the duplicate payload buffer grows.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64);
                 IntHashJoinBuild build = newBuild(2, 16, ColumnType.DOUBLE)) {
                build.open(tracker, NOOP);
                build.append(1, new Source());
                // Final 32-byte hash + 32-byte payload fits, old 16-byte payload is still live.
                Assert.assertThrows(CairoException.class, () -> build.append(1, new Source()));
                Assert.assertEquals(0, tracker.getUsed());
                tracker.setLimit(80);
                build.open(tracker, NOOP);
                build.append(1, new Source());
                build.append(1, new Source());
                Assert.assertEquals(64, build.freeze().getSizeInBytes());
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
                FrozenHashJoinBuild.Probe probe = frozen.newProbe(NOOP);
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
                FrozenHashJoinBuild.Probe probe = frozen.newProbe(NOOP);
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
                FrozenHashJoinBuild.Probe probe = build.freeze().newProbe(NOOP);
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
