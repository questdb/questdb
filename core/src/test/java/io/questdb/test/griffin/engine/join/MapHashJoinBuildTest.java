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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MapHashJoinBuildTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;

    @Test
    public void testAgreesWithTheIntKeyedBuild() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final int rows = 5_000;
            final int[] keys = new int[rows];
            for (int i = 0; i < rows; i++) {
                // A narrow key range makes most rows collide into duplicate chains.
                keys[i] = rnd.nextInt(64) - 32;
            }
            try (IntHashJoinBuild intBuild = new IntHashJoinBuild(new SingleColumnType(ColumnType.INT), indexes(1), 4, 64);
                 MapHashJoinBuild mapBuild = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                         new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64)) {
                intBuild.open(null, NOOP);
                mapBuild.open(null, NOOP);
                final LongKeySink sink = new LongKeySink();
                for (int i = 0; i < rows; i++) {
                    final Source row = source(keys[i], i);
                    intBuild.append(keys[i], row);
                    mapBuild.append(row, sink);
                }
                FrozenHashJoinBuild.IntKeyed intFrozen = intBuild.freeze();
                FrozenHashJoinBuild.RecordKeyed mapFrozen = mapBuild.freeze();
                Assert.assertEquals(intFrozen.getRowCount(), mapFrozen.getRowCount());
                Assert.assertEquals(intFrozen.getKeyCount(), mapFrozen.getKeyCount());
                FrozenHashJoinBuild.IntProbe intProbe = intFrozen.newProbe();
                FrozenHashJoinBuild.RecordProbe mapProbe = mapFrozen.newProbe(new LongKeySink());
                try {
                    for (int key = -40; key < 40; key++) {
                        final StringBuilder expected = new StringBuilder();
                        intProbe.findUnchecked(key);
                        while (intProbe.hasNext()) {
                            intProbe.next();
                            if (expected.length() > 0) {
                                expected.append(' ');
                            }
                            expected.append(intProbe.getRecord().getInt(0));
                        }
                        TestUtils.assertEquals("key " + key, expected.toString(), chainOf(mapProbe, key));
                    }
                } finally {
                    Misc.free(mapProbe);
                    Misc.free(intProbe);
                }
            }
        });
    }

    @Test
    public void testExpiredBuildRejectsProbes() throws Exception {
        assertMemoryLeak(() -> {
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                    new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64, true)) {
                build.open(null, NOOP);
                build.append(source(1, 10), new LongKeySink());
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    build.close();
                    // The reusable snapshot object outlives the execution, so the probe has to
                    // refuse it until the next freeze rebinds it.
                    Assert.assertThrows(IllegalStateException.class, probe::reopen);
                    Assert.assertThrows(IllegalStateException.class, () -> frozen.newProbe(new LongKeySink()));
                    build.open(null, NOOP);
                    Assert.assertThrows(IllegalStateException.class, probe::reopen);
                    build.append(source(1, 11), new LongKeySink());
                    build.freeze();
                    probe.reopen();
                    TestUtils.assertEquals("11", chainOf(probe, 1));
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testBuildIsNotMutableAfterFreeze() throws Exception {
        assertMemoryLeak(() -> {
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                Assert.assertThrows(IllegalStateException.class, () -> build.append(new Source(), new LongKeySink()));
                build.open(null, NOOP);
                Assert.assertThrows(IllegalStateException.class, () -> build.open(null, NOOP));
                build.append(source(1, 10), new LongKeySink());
                build.freeze();
                Assert.assertThrows(IllegalStateException.class, () -> build.append(new Source(), new LongKeySink()));
                Assert.assertThrows(IllegalStateException.class, build::freeze);
            }
        });
    }

    @Test
    public void testCancellationDuringBuildReleasesEverything() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 4_096;
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                final CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                build.open(tracker, counting);
                build.build(new SourceCursor(rows), new LongKeySink(), rows);
                final long checks = counting.getCheckCount();
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertTrue("checks: " + checks, checks > 0);
                for (long failAt = 1; failAt <= checks; failAt++) {
                    final long failureCheck = failAt;
                    final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(NOOP) {
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
                        // open() checks the breaker before its first allocation, so the whole
                        // execution, not only the build loop, has to be cancellable here.
                        build.open(tracker, breaker);
                        build.build(new SourceCursor(rows), new LongKeySink(), rows);
                        Assert.fail("expected cancellation at check " + failAt);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    }
                    Assert.assertEquals("cancelled build releases all allocations", 0, tracker.getUsed());
                    Assert.assertEquals(0, build.getSizeInBytes());
                }
                build.open(tracker, NOOP);
                build.build(new SourceCursor(rows), new LongKeySink(), rows);
                Assert.assertTrue(tracker.getUsed() > 0);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testCompositeKeyThroughGeneratedSink() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, i INT, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                      ('a', 1, 'x', 0),
                      ('b', 2, 'y', 1_000),
                      ('a', 1, 'x', 2_000),
                      ('c', 3, NULL, 3_000),
                      ('c', 3, NULL, 4_000)""");
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes()
                    .add(ColumnType.STRING)
                    .add(ColumnType.INT)
                    .add(ColumnType.VARCHAR);
            try (TableReader reader = newOffPoolReader(configuration, "t");
                 TestTableReaderRecordCursor buildCursor = new TestTableReaderRecordCursor().of(reader);
                 TestTableReaderRecordCursor probeCursor = new TestTableReaderRecordCursor().of(reader);
                 MapHashJoinBuild build = new MapHashJoinBuild(configuration, keyTypes,
                         new SingleColumnType(ColumnType.TIMESTAMP), indexes(3), 4, 1024, 64)) {
                final ListColumnFilter keyFilter = new ListColumnFilter();
                keyFilter.add(1);
                keyFilter.add(2);
                keyFilter.add(3);
                final BitSet symbolAsString = new BitSet();
                symbolAsString.set(0);
                final BytecodeAssembler asm = new BytecodeAssembler();
                // The build and the probe stage the same key layout through their own sinks,
                // as the two sides of a join do.
                final RecordSink buildSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), keyFilter, symbolAsString);
                final RecordSink probeSink = RecordSinkFactory.getInstance(configuration, asm, reader.getMetadata(), keyFilter, symbolAsString);
                build.open(sqlExecutionContext.getMemoryTracker(), NOOP);
                FrozenHashJoinBuild.RecordKeyed frozen = build.build(buildCursor, buildSink, 5);
                Assert.assertEquals(5, frozen.getRowCount());
                Assert.assertEquals(3, frozen.getKeyCount());
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(probeSink);
                try {
                    final Record record = probeCursor.getRecord();
                    final List<String> matches = new ArrayList<>();
                    while (probeCursor.hasNext()) {
                        probe.findUnchecked(record);
                        final StringBuilder line = new StringBuilder().append(record.getTimestamp(3)).append(" ->");
                        while (probe.hasNext()) {
                            probe.next();
                            line.append(' ').append(probe.getRecord().getTimestamp(0));
                        }
                        matches.add(line.toString());
                    }
                    TestUtils.assertEquals("[0 -> 2000 0, 1000 -> 1000, 2000 -> 2000 0, 3000 -> 4000 3000, 4000 -> 4000 3000]",
                            matches.toString());
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testConcurrentProbesOverOneFrozenBuild() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 10_000;
            final int workers = 4;
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(sqlExecutionContext.getMemoryTracker(), NOOP);
                final FrozenHashJoinBuild.RecordKeyed frozen = build.build(new SourceCursor(rows), new LongKeySink(), rows);
                final ExecutorService pool = Executors.newFixedThreadPool(workers);
                final CountDownLatch start = new CountDownLatch(1);
                final AtomicInteger errors = new AtomicInteger();
                final ObjList<Future<?>> futures = new ObjList<>();
                try {
                    for (int i = 0; i < workers; i++) {
                        futures.add(pool.submit(() -> {
                            // Each worker owns its probe and its key sink, as a slot does.
                            FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                            try {
                                start.await();
                                final Source probeRecord = new Source();
                                for (int row = 0; row < rows; row++) {
                                    probeRecord.longKey = row;
                                    probe.findUnchecked(probeRecord);
                                    if (!probe.hasNext()) {
                                        errors.incrementAndGet();
                                        continue;
                                    }
                                    probe.next();
                                    if (probe.getRecord().getInt(0) != row * 10) {
                                        errors.incrementAndGet();
                                    }
                                    if (probe.hasNext()) {
                                        errors.incrementAndGet();
                                    }
                                }
                            } catch (Throwable th) {
                                errors.incrementAndGet();
                            } finally {
                                Misc.free(probe);
                            }
                        }));
                    }
                    start.countDown();
                    for (int i = 0, n = futures.size(); i < n; i++) {
                        futures.getQuick(i).get(60, TimeUnit.SECONDS);
                    }
                } finally {
                    pool.shutdownNow();
                    Assert.assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
                }
                Assert.assertEquals(0, errors.get());
            }
        });
    }

    @Test
    public void testDuplicateChainIteratesInReverseInputOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(null, NOOP);
                final LongKeySink sink = new LongKeySink();
                build.append(source(7, 1), sink);
                build.append(source(8, 2), sink);
                build.append(source(7, 3), sink);
                build.append(source(7, 4), sink);
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                Assert.assertEquals(4, frozen.getRowCount());
                Assert.assertEquals(2, frozen.getKeyCount());
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    TestUtils.assertEquals("4 3 1", chainOf(probe, 7));
                    TestUtils.assertEquals("2", chainOf(probe, 8));
                    TestUtils.assertEquals("", chainOf(probe, 9));
                    // A miss also clears the payload record of the previous match.
                    probe.find(source(9, 0));
                    Assert.assertFalse(probe.hasNext());
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testFindSingleUncheckedOnUniqueBuild() throws Exception {
        assertMemoryLeak(() -> {
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(null, NOOP);
                final LongKeySink sink = new LongKeySink();
                build.append(source(0, 10), sink);
                build.append(source(Numbers.LONG_NULL, 20), sink);
                build.append(source(-3, 30), sink);
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                Assert.assertEquals(frozen.getRowCount(), frozen.getKeyCount());
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    final Source probeRecord = new Source();
                    for (long[] pair : new long[][]{{0, 10}, {Numbers.LONG_NULL, 20}, {-3, 30}}) {
                        probeRecord.longKey = pair[0];
                        Assert.assertTrue(probe.findSingleUnchecked(probeRecord));
                        Assert.assertEquals(pair[1], probe.getRecord().getInt(0));
                        Assert.assertFalse(probe.hasNext());
                    }
                    probeRecord.longKey = 17;
                    Assert.assertFalse(probe.findSingleUnchecked(probeRecord));
                    Assert.assertFalse(probe.hasNext());
                } finally {
                    Misc.free(probe);
                }
            }
            // An empty build answers every lookup without touching the payload.
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(null, NOOP);
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    Assert.assertFalse(probe.findSingleUnchecked(source(1, 1)));
                    Assert.assertFalse(probe.hasNext());
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testHandlesRoundTripAndStayUniqueAcrossExecutions() throws Exception {
        assertMemoryLeak(() -> {
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                    new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64, true)) {
                final LongList handles = new LongList();
                for (int execution = 0; execution < 2; execution++) {
                    build.open(null, NOOP);
                    final LongKeySink sink = new LongKeySink();
                    build.append(source(1, 10), sink);
                    build.append(source(1, 11), sink);
                    build.append(source(2, 20), sink);
                    FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                    FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                    try {
                        probe.findUnchecked(source(1, 0));
                        final long first = probe.next();
                        final long second = probe.next();
                        Assert.assertFalse(probe.hasNext());
                        // recordAt repositions the payload without disturbing the iterator.
                        probe.recordAt(first);
                        Assert.assertEquals(11, probe.getRecord().getInt(0));
                        probe.recordAt(second);
                        Assert.assertEquals(10, probe.getRecord().getInt(0));
                        Assert.assertEquals(-1, handles.indexOf(first));
                        Assert.assertEquals(-1, handles.indexOf(second));
                        handles.add(first);
                        handles.add(second);
                    } finally {
                        Misc.free(probe);
                    }
                    build.close();
                }
            }
        });
    }

    @Test
    public void testMapChoiceFollowsTheKey() throws Exception {
        assertMemoryLeak(() -> {
            // Unordered8Map admits exactly these three, and compares the full type int, so a
            // nanosecond timestamp is not one of them.
            for (int keyType : new int[]{ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE}) {
                assertMapChoice(new SingleColumnType(keyType), false);
            }
            for (int keyType : new int[]{ColumnType.INT, ColumnType.SHORT, ColumnType.DOUBLE,
                    ColumnType.TIMESTAMP_NANO, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.UUID}) {
                assertMapChoice(new SingleColumnType(keyType), true);
            }
            // Every multi-column key goes to the ordered map, eight-byte columns included.
            assertMapChoice(new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.LONG), true);
        });
    }

    @Test
    public void testMemoryLimitDuringBuildReleasesEverything() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1024);
                 MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(tracker, NOOP);
                try {
                    build.build(new SourceCursor(100_000), new LongKeySink(), -1);
                    Assert.fail("expected the tracker to reject an allocation");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, build.getSizeInBytes());
            }
        });
    }

    @Test
    public void testNullAndZeroKeysMatchEachOther() throws Exception {
        assertMemoryLeak(() -> {
            // Zero marks an empty slot in the unordered map, so its entry lives past the table;
            // NULL is an ordinary entry. Both have to match themselves and nothing else.
            for (long key : new long[]{0, Numbers.LONG_NULL}) {
                try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                    build.open(null, NOOP);
                    final LongKeySink sink = new LongKeySink();
                    build.append(source(key, 1), sink);
                    build.append(source(key, 2), sink);
                    build.append(source(key == 0 ? 5 : 0, 3), sink);
                    FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                    Assert.assertEquals(2, frozen.getKeyCount());
                    FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                    try {
                        TestUtils.assertEquals("2 1", chainOf(probe, key));
                        TestUtils.assertEquals("", chainOf(probe, 7));
                    } finally {
                        Misc.free(probe);
                    }
                }
            }
            // The same on the ordered map, where a NULL string key is a length-prefixed entry.
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64)) {
                build.open(null, NOOP);
                final StrKeySink sink = new StrKeySink();
                build.append(source(null, 1), sink);
                build.append(source(null, 2), sink);
                build.append(source("a", 3), sink);
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                Assert.assertEquals(2, frozen.getKeyCount());
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new StrKeySink());
                try {
                    TestUtils.assertEquals("2 1", chainOfStr(probe, null));
                    TestUtils.assertEquals("3", chainOfStr(probe, "a"));
                    TestUtils.assertEquals("", chainOfStr(probe, "b"));
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testPayloadColumnsRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes payload = new ArrayColumnTypes()
                    .add(ColumnType.BOOLEAN).add(ColumnType.BYTE).add(ColumnType.SHORT).add(ColumnType.CHAR)
                    .add(ColumnType.INT).add(ColumnType.FLOAT).add(ColumnType.LONG).add(ColumnType.DATE)
                    .add(ColumnType.TIMESTAMP).add(ColumnType.DOUBLE);
            final IntList columns = new IntList();
            for (int i = 0; i < payload.getColumnCount(); i++) {
                columns.add(1);
            }
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                    payload, columns, 4, 1024, 64)) {
                build.open(null, NOOP);
                build.append(source(3, 42), new LongKeySink());
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    Assert.assertTrue(probe.findSingleUnchecked(source(3, 0)));
                    final Record record = probe.getRecord();
                    Assert.assertTrue(record.getBool(0));
                    Assert.assertEquals(42, record.getByte(1));
                    Assert.assertEquals(42, record.getShort(2));
                    Assert.assertEquals('*', record.getChar(3));
                    Assert.assertEquals(42, record.getInt(4));
                    Assert.assertEquals(42f, record.getFloat(5), 0.0001);
                    Assert.assertEquals(42, record.getLong(6));
                    Assert.assertEquals(42, record.getDate(7));
                    Assert.assertEquals(42, record.getTimestamp(8));
                    Assert.assertEquals(42, record.getDouble(9), 0.0001);
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testProbeReleasesStagingBufferAndReopens() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.STRING),
                         new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64, true)) {
                build.open(tracker, NOOP);
                build.append(source("a", 1), new StrKeySink());
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                final long buildBytes = tracker.getUsed();
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new StrKeySink());
                try {
                    // The ordered map's view stages keys in native memory of its own.
                    Assert.assertTrue(tracker.getUsed() > buildBytes);
                    TestUtils.assertEquals("1", chainOfStr(probe, "a"));
                    probe.close();
                    Assert.assertEquals(buildBytes, tracker.getUsed());
                    // A closed probe comes back for the next execution.
                    build.close();
                    build.open(tracker, NOOP);
                    build.append(source("a", 2), new StrKeySink());
                    build.append(source("b", 3), new StrKeySink());
                    build.freeze();
                    probe.reopen();
                    TestUtils.assertEquals("2", chainOfStr(probe, "a"));
                    TestUtils.assertEquals("3", chainOfStr(probe, "b"));
                } finally {
                    Misc.free(probe);
                }
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testRowCountHintPresizesTheBuild() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 20_000;
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(sqlExecutionContext.getMemoryTracker(), NOOP);
                final FrozenHashJoinBuild.RecordKeyed frozen = build.build(new SourceCursor(rows), new LongKeySink(), rows);
                Assert.assertEquals(rows, frozen.getRowCount());
                Assert.assertEquals(rows, frozen.getKeyCount());
            }
            // A hint the row heap cannot hold is an error before the map ever sees it.
            try (MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                build.open(sqlExecutionContext.getMemoryTracker(), NOOP);
                try {
                    build.build(new SourceCursor(8), new LongKeySink(), Long.MAX_VALUE / 64);
                    Assert.fail("expected the row heap to reject the hint");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "buffer overflow");
                }
            }
        });
    }

    @Test
    public void testSizeInBytesSumsMapAndRowHeap() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 MapHashJoinBuild build = newLongKeyBuild(ColumnType.INT)) {
                Assert.assertEquals(0, build.getSizeInBytes());
                build.open(tracker, NOOP);
                build.build(new SourceCursor(1_000), new LongKeySink(), 1_000);
                // The map and the row heap carry different memory tags, so the build's own
                // figure has to sum them; it can only undercount by unused map capacity.
                Assert.assertEquals(tracker.getUsed(), build.getSizeInBytes());
                Assert.assertTrue(build.getSizeInBytes() > 1_000 * 16L);
                build.close();
                Assert.assertEquals(0, build.getSizeInBytes());
                Assert.assertEquals(0, tracker.getUsed());
            }
            // The same for the ordered map, whose bytes are a key heap plus an offset table.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024);
                 MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.STRING),
                         new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64)) {
                build.open(tracker, NOOP);
                final StrKeySink sink = new StrKeySink();
                for (int i = 0; i < 1_000; i++) {
                    build.append(source("key-" + i, i), sink);
                }
                build.freeze();
                Assert.assertEquals(tracker.getUsed(), build.getSizeInBytes());
                build.close();
                Assert.assertEquals(0, build.getSizeInBytes());
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSymbolPayloadResolvesThroughTheSource() throws Exception {
        assertMemoryLeak(() -> {
            final Symbols symbols = new Symbols("zero", "one", "two");
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                    new SingleColumnType(ColumnType.SYMBOL), indexes(1), 4, 1024, 64)) {
                build.open(null, NOOP);
                build.append(source(1, 2), new LongKeySink());
                Assert.assertThrows(IllegalArgumentException.class, build::freeze);
            }
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG),
                    new SingleColumnType(ColumnType.SYMBOL), indexes(1), 4, 1024, 64)) {
                build.open(null, NOOP);
                build.append(source(1, 2), new LongKeySink());
                build.append(source(1, 0), new LongKeySink());
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze(symbols);
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new LongKeySink());
                try {
                    probe.findUnchecked(source(1, 0));
                    probe.next();
                    TestUtils.assertEquals("zero", probe.getRecord().getSymA(0));
                    probe.next();
                    TestUtils.assertEquals("two", probe.getRecord().getSymA(0));
                    // Each probe takes its own flyweights, which only the source can hand out.
                    Assert.assertEquals(1, symbols.newSymbolTableCalls);
                    TestUtils.assertEquals("one", probe.newSymbolTable(0).valueOf(1));
                    Assert.assertEquals(2, symbols.newSymbolTableCalls);
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    @Test
    public void testVarSizeKeyOutgrowsTheStagingBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final StringBuilder longKey = new StringBuilder();
            for (int i = 0; i < 4_096; i++) {
                longKey.append((char) ('a' + (i % 26)));
            }
            try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.STRING),
                    new SingleColumnType(ColumnType.INT), indexes(1), 4, 64, 64)) {
                build.open(sqlExecutionContext.getMemoryTracker(), NOOP);
                final StrKeySink sink = new StrKeySink();
                build.append(source(longKey.toString(), 1), sink);
                build.append(source(longKey.substring(0, 4_095), 2), sink);
                FrozenHashJoinBuild.RecordKeyed frozen = build.freeze();
                FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(new StrKeySink());
                try {
                    // The staging buffer starts at 64 bytes and has to grow for each of these.
                    TestUtils.assertEquals("1", chainOfStr(probe, longKey.toString()));
                    TestUtils.assertEquals("2", chainOfStr(probe, longKey.substring(0, 4_095)));
                    TestUtils.assertEquals("", chainOfStr(probe, longKey + "tail"));
                } finally {
                    Misc.free(probe);
                }
            }
        });
    }

    private static void assertMapChoice(ColumnTypes keyTypes, boolean expectOrdered) throws Exception {
        try (MapHashJoinBuild build = new MapHashJoinBuild(configuration, keyTypes,
                new SingleColumnType(ColumnType.INT), indexes(1), 4, 1024, 64)) {
            Assert.assertEquals(keyTypes.getColumnType(0) + " ordered", expectOrdered, mapOf(build, "orderedMap") != null);
            Assert.assertEquals(keyTypes.getColumnType(0) + " unordered8", !expectOrdered, mapOf(build, "unordered8Map") != null);
        }
    }

    private static String chainOf(FrozenHashJoinBuild.RecordProbe probe, long key) {
        final Source probeRecord = new Source();
        probeRecord.longKey = key;
        return drain(probe, probeRecord);
    }

    private static String chainOfStr(FrozenHashJoinBuild.RecordProbe probe, CharSequence key) {
        final Source probeRecord = new Source();
        probeRecord.strKey = key;
        return drain(probe, probeRecord);
    }

    private static String drain(FrozenHashJoinBuild.RecordProbe probe, Source probeRecord) {
        probe.findUnchecked(probeRecord);
        final StringBuilder sink = new StringBuilder();
        while (probe.hasNext()) {
            probe.next();
            if (sink.length() > 0) {
                sink.append(' ');
            }
            sink.append(probe.getRecord().getInt(0));
        }
        return sink.toString();
    }

    private static IntList indexes(int... columns) {
        final IntList list = new IntList();
        for (int column : columns) {
            list.add(column);
        }
        return list;
    }

    private static Object mapOf(MapHashJoinBuild build, String name) throws Exception {
        final Field field = MapHashJoinBuild.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(build);
    }

    private static MapHashJoinBuild newLongKeyBuild(int... payloadTypes) {
        final ArrayColumnTypes types = new ArrayColumnTypes();
        final IntList columns = new IntList();
        for (int payloadType : payloadTypes) {
            types.add(payloadType);
            columns.add(1);
        }
        return new MapHashJoinBuild(configuration, new SingleColumnType(ColumnType.LONG), types, columns, 4, 1024, 64);
    }

    private static Source source(long key, int payload) {
        final Source source = new Source();
        source.longKey = key;
        source.payload = payload;
        return source;
    }

    private static Source source(CharSequence key, int payload) {
        final Source source = new Source();
        source.strKey = key;
        source.payload = payload;
        return source;
    }

    /** Stages a single LONG key column, as a generated sink over one LONG column would. */
    private static class LongKeySink implements RecordSink {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putLong(r.getLong(0));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    }

    /** Column 0 carries every key flavour, column 1 the payload value. */
    private static class Source implements Record {
        private long longKey;
        private int payload;
        private CharSequence strKey;

        @Override
        public boolean getBool(int col) {
            return payload != 0;
        }

        @Override
        public byte getByte(int col) {
            return (byte) payload;
        }

        @Override
        public char getChar(int col) {
            return (char) payload;
        }

        @Override
        public long getDate(int col) {
            return payload;
        }

        @Override
        public double getDouble(int col) {
            return payload;
        }

        @Override
        public float getFloat(int col) {
            return payload;
        }

        @Override
        public int getInt(int col) {
            return payload;
        }

        @Override
        public long getLong(int col) {
            return col == 0 ? longKey : payload;
        }

        @Override
        public short getShort(int col) {
            return (short) payload;
        }

        @Override
        public CharSequence getStrA(int col) {
            return strKey;
        }

        @Override
        public long getTimestamp(int col) {
            return payload;
        }
    }

    /** Replays rows whose LONG key is the row number and whose INT payload is ten times it. */
    private static class SourceCursor implements RecordCursor {
        private final Source record = new Source();
        private final int rows;
        private int row = -1;

        private SourceCursor(int rows) {
            this.rows = rows;
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
            throw new UnsupportedOperationException();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public boolean hasNext() {
            if (++row < rows) {
                record.longKey = row;
                record.payload = row * 10;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return rows;
        }

        @Override
        public void toTop() {
            row = -1;
        }
    }

    /** Stages a single STRING key column. */
    private static class StrKeySink implements RecordSink {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            w.putStr(r.getStrA(0));
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
        }
    }

    /** One static dictionary, handing out an independent flyweight per request. */
    private static class Symbols implements SymbolTableSource {
        private final ObjList<String> dictionary = new ObjList<>();
        private int newSymbolTableCalls;

        private Symbols(String... values) {
            for (String value : values) {
                dictionary.add(value);
            }
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return newSymbolTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            newSymbolTableCalls++;
            return new Table();
        }

        private class Table implements StaticSymbolTable {
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
                final int index = dictionary.indexOf(value);
                return index < 0 ? SymbolTable.VALUE_NOT_FOUND : index;
            }

            @Override
            public CharSequence valueBOf(int key) {
                return valueOf(key);
            }

            @Override
            public CharSequence valueOf(int key) {
                return key == SymbolTable.VALUE_IS_NULL ? null : dictionary.getQuick(key);
            }
        }
    }
}
