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

import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.std.BinarySequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * The build's payload source over its input's page frames: probes read payload columns at the
 * row ids the build keeps, each through a reader of its own, or a copy of those columns that the
 * owner makes once the build froze.
 */
public class HashJoinBuildFramesTest extends AbstractCairoTest {

    @Test
    public void testAllPayloadTypesAndPrunedMapping() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE payload (k INT, b BOOLEAN, by BYTE, sh SHORT, ch CHAR, i INT, l LONG, d DATE, ts TIMESTAMP, "
                    + "ns TIMESTAMP_NS, f FLOAT, dbl DOUBLE, s SYMBOL, unused STRING)");
            execute("INSERT INTO payload VALUES (42, true, 12, 123, 'Q', 42, 9876543210L, 100, 123456789, "
                    + "123456789123456789L, 1.25, 2.5, 'ES', 'ignored'), "
                    + "(43, null, null, null, null, null, null, null, null, null, null, null, null, 'ignored')");
            // The payload order reverses the table's and skips a column, so a reader that ignored
            // the mapping would read the wrong column.
            String[] names = {"s", "dbl", "f", "ns", "ts", "d", "l", "i", "ch", "sh", "by", "b"};
            try (RecordCursorFactory factory = select("payload")) {
                IntList mapping = new IntList();
                for (String name : names) {
                    mapping.add(factory.getMetadata().getColumnIndex(name));
                }
                try (HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, mapping, factory.getMetadata());
                     IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                    FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                    try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        probe.find(42);
                        Assert.assertTrue(probe.hasNext());
                        probe.next();
                        Record record = probe.getRecord();
                        TestUtils.assertEquals("ES", record.getSymA(0));
                        TestUtils.assertEquals("ES", record.getSymB(0));
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
                        Assert.assertTrue(probe.getSymbolTable(0) instanceof StaticSymbolTable);
                        Assert.assertEquals(0, ((StaticSymbolTable) probe.getSymbolTable(0)).keyOf("ES"));
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
            }
        });
    }

    @Test
    public void testCopiedPayloadReadsAsTheFrames() throws Exception {
        assertMemoryLeak(() -> {
            // Every type the copy holds, over six daily partitions: two of them Parquet, a NULL row
            // every seventh row, and half the columns added after the first two days, so that their
            // early rows sit below column tops.
            execute("CREATE TABLE every (k INT, b BOOLEAN, by BYTE, sh SHORT, ch CHAR, i INT, l LONG, d DATE, "
                    + "ns TIMESTAMP_NS, f FLOAT, dbl DOUBLE, s SYMBOL, ip IPV4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO every
                    SELECT x::INT,
                        CASE WHEN x % 7 = 0 THEN null ELSE x % 2 = 0 END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x % 100)::BYTE END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 3)::SHORT END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_char() END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 11)::INT END,
                        CASE WHEN x % 7 = 0 THEN null ELSE x * 1_000_003 END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 86_400_000)::DATE END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 1_000_000_007)::TIMESTAMP_NS END,
                        CASE WHEN x % 7 = 0 THEN null ELSE x / 4.0 END::FLOAT,
                        CASE WHEN x % 7 = 0 THEN null ELSE x / 8.0 END,
                        CASE WHEN x % 7 = 0 THEN null ELSE 's' || (x % 5) END::SYMBOL,
                        CASE WHEN x % 7 = 0 THEN null ELSE ('10.0.0.' || (x % 250 + 1))::IPV4 END,
                        timestamp_sequence('2020-01-01', 7_200_000_000L)
                    FROM long_sequence(24)
                    """);
            execute("ALTER TABLE every ADD COLUMN u UUID");
            execute("ALTER TABLE every ADD COLUMN l256 LONG256");
            execute("ALTER TABLE every ADD COLUMN g1 GEOHASH(1c)");
            execute("ALTER TABLE every ADD COLUMN g3 GEOHASH(3c)");
            execute("ALTER TABLE every ADD COLUMN g6 GEOHASH(6c)");
            execute("ALTER TABLE every ADD COLUMN g12 GEOHASH(12c)");
            execute("ALTER TABLE every ADD COLUMN dec8 DECIMAL(2,1)");
            execute("ALTER TABLE every ADD COLUMN dec16 DECIMAL(4,1)");
            execute("ALTER TABLE every ADD COLUMN dec32 DECIMAL(9,2)");
            execute("ALTER TABLE every ADD COLUMN dec64 DECIMAL(18,2)");
            execute("ALTER TABLE every ADD COLUMN dec128 DECIMAL(38,2)");
            execute("ALTER TABLE every ADD COLUMN dec256 DECIMAL(50,2)");
            execute("ALTER TABLE every ADD COLUMN s2 SYMBOL");
            execute("""
                    INSERT INTO every
                    SELECT (24 + x)::INT, x % 2 = 0, x::BYTE, x::SHORT, 'Z', x::INT, x, x::DATE, x::TIMESTAMP_NS,
                        x::FLOAT, x::DOUBLE, 's' || (x % 5), ('10.0.1.' || x)::IPV4,
                        timestamp_sequence('2020-01-03', 7_200_000_000L),
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_uuid4() END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_long256() END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(5) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(15) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(30) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(60) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x % 9)::DECIMAL(2,1) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 7)::DECIMAL(4,1) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 1_234)::DECIMAL(9,2) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 1_234_567_890L)::DECIMAL(18,2) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * 123_456_789_012_345_678L)::DECIMAL(38,2) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE (x * -123_456_789_012_345_678L)::DECIMAL(50,2) END,
                        CASE WHEN x % 7 = 0 THEN null ELSE 't' || (x % 3) END
                    FROM long_sequence(48)
                    """);
            execute("ALTER TABLE every CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-02'");
            execute("ALTER TABLE every CONVERT PARTITION TO PARQUET WHERE ts IN '2020-01-04'");
            sqlExecutionContext.changePageFrameSizes(5, 5);
            final MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 RecordCursorFactory factory = select("every")) {
                sqlExecutionContext.setMemoryTracker(tracker);
                final RecordMetadata metadata = factory.getMetadata();
                // Every column but the key and the designated timestamp, in reverse table order, so
                // that a copy that ignored the payload mapping would read the wrong column.
                IntList mapping = new IntList();
                for (int i = metadata.getColumnCount() - 1; i > 0; i--) {
                    if (i != metadata.getTimestampIndex()) {
                        mapping.add(i);
                    }
                }
                final StringSink expected = new StringSink();
                final StringSink actual = new StringSink();
                try (HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, mapping, metadata);
                     IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                    // BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP_NS, FLOAT, DOUBLE,
                    // SYMBOL, IPV4, UUID, LONG256, four GEOHASH widths, six DECIMAL widths and a
                    // second SYMBOL, each aligned to its size up to eight bytes.
                    Assert.assertEquals(200, frames.getCopyRowSize());
                    FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                    Assert.assertEquals(72, frozen.getRowCount());
                    try (FrozenHashJoinBuild.IntProbe before = frozen.newProbe()) {
                        for (int key = 1; key <= 72; key++) {
                            printRow(before, key, metadata, mapping, expected);
                        }
                        final long used = tracker.getUsed();
                        frames.copyPayload(frozen, sqlExecutionContext.getCircuitBreaker());
                        // The copy is charged to the execution, and the pass releases what it decoded.
                        Assert.assertEquals(used + 72 * 200, tracker.getUsed());
                        // A reader that existed before the copy reads the copy from its next match on,
                        // and so does one that the copy precedes.
                        try (FrozenHashJoinBuild.IntProbe after = frozen.newProbe()) {
                            for (FrozenHashJoinBuild.IntProbe probe : new FrozenHashJoinBuild.IntProbe[]{before, after}) {
                                actual.clear();
                                for (int key = 1; key <= 72; key++) {
                                    printRow(probe, key, metadata, mapping, actual);
                                }
                                TestUtils.assertEquals(expected, actual);
                            }
                        }
                    }
                    build.close();
                    frames.clear();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            } finally {
                sqlExecutionContext.setMemoryTracker(previous);
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testCopyFailuresReleaseTheCopy() throws Exception {
        assertMemoryLeak(() -> {
            // 70_000 rows, so that the pass checks the breaker twice, over four partitions, two of
            // them Parquet, which the pass decodes as it goes.
            execute("CREATE TABLE big (k INT, v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO big
                    SELECT x::INT, x, 's' || (x % 3), timestamp_sequence('2020-01-01', 4_000_000L)
                    FROM long_sequence(70_000)
                    """);
            execute("ALTER TABLE big CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            final MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 RecordCursorFactory factory = select("big");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                // A LONG and a SYMBOL: twelve bytes, aligned to sixteen.
                Assert.assertEquals(16, frames.getCopyRowSize());
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                final long buildBytes = tracker.getUsed();

                // A memory limit one byte short of the copy fails the allocation.
                tracker.setLimit(buildBytes + 70_000 * 16 - 1);
                Assert.assertThrows(CairoException.class, () -> frames.copyPayload(frozen, sqlExecutionContext.getCircuitBreaker()));
                Assert.assertEquals(buildBytes, tracker.getUsed());

                // Cancellation before the copy, and inside the pass with Parquet buffers decoded.
                tracker.setLimit(0);
                for (int trip = 0; trip < 3; trip++) {
                    final int tripAt = trip;
                    final CountingSqlExecutionCircuitBreaker cancelled = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER) {
                        private int checks;

                        @Override
                        public void statefulThrowExceptionIfTrippedNoThrottle() {
                            if (tripAt == 0) {
                                throw CairoException.queryCancelled(1);
                            }
                        }

                        @Override
                        public void statefulThrowExceptionIfTrippedTimeThrottled() {
                            if (++checks == tripAt) {
                                throw CairoException.queryCancelled(1);
                            }
                        }
                    };
                    Assert.assertThrows(CairoException.class, () -> frames.copyPayload(frozen, cancelled));
                    Assert.assertEquals(buildBytes, tracker.getUsed());
                }

                // The build survives both failures, and a copy after them reads every row.
                frames.copyPayload(frozen, sqlExecutionContext.getCircuitBreaker());
                Assert.assertEquals(buildBytes + 70_000 * 16, tracker.getUsed());
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    for (int key = 1; key <= 70_000; key += 997) {
                        probe.find(key);
                        probe.next();
                        Assert.assertEquals(key, probe.getRecord().getLong(0));
                        TestUtils.assertEquals("s" + (key % 3), probe.getRecord().getSymA(1));
                    }
                }
                build.close();
                frames.clear();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                sqlExecutionContext.setMemoryTracker(previous);
            }
        });
    }

    @Test
    public void testCursorOverloadReadsAndBuildsAsTheFactoryOverload() throws Exception {
        assertMemoryLeak(() -> {
            // 600 rows over six daily partitions, the first two of them Parquet, read in frames of
            // seven rows at most, and 40 keys, so that every key's duplicates span frames,
            // partitions and formats.
            execute("CREATE TABLE spread (k INT, v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO spread
                    SELECT (x % 40)::INT, x, 's' || (x % 7), timestamp_sequence('2020-01-01', 864_000_000L)
                    FROM long_sequence(600)
                    """);
            execute("ALTER TABLE spread CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            sqlExecutionContext.changePageFrameSizes(7, 7);
            // A factory per frames object: a query's factory serves one open page frame cursor at a time.
            try (RecordCursorFactory factory = select("spread");
                 RecordCursorFactory cursorFactory = select("spread");
                 RecordCursorFactory backwardFactory = select("spread");
                 HashJoinBuildFrames byFactory = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 HashJoinBuildFrames byCursor = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 HashJoinBuildFrames byBackwardCursor = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 IntHashJoinBuild factoryBuild = new IntHashJoinBuild(true, 2, 16);
                 IntHashJoinBuild cursorBuild = new IntHashJoinBuild(true, 2, 16);
                 IntHashJoinBuild partitionedBuild = new IntHashJoinBuild(true, 2, 16)) {
                // The frames adopt the cursor they are handed: it serves the build input's symbol
                // tables until clear() closes it, which releases its table reader.
                final PageFrameCursor cursor = cursorFactory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                byCursor.open(cursor, cursorFactory.getMetadata(), sqlExecutionContext);
                Assert.assertSame(cursor, byCursor.getSymbolTableSource());
                Assert.assertEquals(1, engine.getBusyReaderCount());
                byCursor.clear();
                Assert.assertNull(byCursor.getSymbolTableSource());
                Assert.assertEquals(0, engine.getBusyReaderCount());

                final FrozenHashJoinBuild.IntKeyed fromFactory = FrameBuilds.buildInt(configuration, factoryBuild, byFactory, factory, 0, sqlExecutionContext);
                byCursor.open(cursorFactory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC), cursorFactory.getMetadata(), sqlExecutionContext);
                final FrozenHashJoinBuild.IntKeyed fromCursor = FrameBuilds.buildIntOverOpenFrames(configuration, cursorBuild, byCursor, 0, sqlExecutionContext);
                Assert.assertTrue(byCursor.hasParquetFrames());
                Assert.assertTrue("frames: " + byCursor.getFrameCount(), byCursor.getFrameCount() > 50);
                Assert.assertEquals(600, byCursor.getRowCount());
                // Both overloads walk the same frames, with the same rows.
                final String factoryFrames = printFrames(byFactory, false, true);
                TestUtils.assertEquals(factoryFrames, printFrames(byCursor, false, true));

                // The serial builds over both, and a parallel build over the cursor's frames, chain
                // each key's rows alike: newest first, as the serial hash join lists them.
                final FrozenHashJoinBuild.IntKeyed partitioned = FrameBuilds.buildIntPartitioned(configuration, partitionedBuild,
                        byCursor, 0, 64, -1, null, null, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                Assert.assertTrue("partitions: " + partitionedBuild.getPartitionCount(), partitionedBuild.getPartitionCount() > 1);
                for (FrozenHashJoinBuild.IntKeyed frozen : new FrozenHashJoinBuild.IntKeyed[]{fromFactory, fromCursor, partitioned}) {
                    try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        for (int key = 0; key < 40; key++) {
                            probe.find(key);
                            for (long v = 600 - ((600 - key) % 40); v > 0; v -= 40) {
                                Assert.assertTrue("key " + key, probe.hasNext());
                                probe.next();
                                Assert.assertEquals(v, probe.getRecord().getLong(0));
                                TestUtils.assertEquals("s" + (v % 7), probe.getRecord().getSymA(1));
                            }
                            Assert.assertFalse("key " + key, probe.hasNext());
                        }
                    }
                }

                // A cursor of a backward scan hands the frames over in its order: the same rows,
                // the last frame first.
                byBackwardCursor.open(backwardFactory.getPageFrameCursor(sqlExecutionContext, ORDER_DESC), backwardFactory.getMetadata(), sqlExecutionContext);
                byBackwardCursor.loadFrames();
                Assert.assertEquals(600, byBackwardCursor.getRowCount());
                Assert.assertTrue(byBackwardCursor.hasParquetFrames());
                TestUtils.assertEquals(printFrames(byFactory, false, false), printFrames(byBackwardCursor, true, false));
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testEveryPayloadTypeReadsAsTheScan() throws Exception {
        assertMemoryLeak(() -> {
            // Every payload type, the var-size ones included, over six daily partitions of twelve
            // rows, the first and the fourth of them Parquet, a NULL row every seventh row, and the
            // columns from u on added after the first two days, so that their early rows sit below
            // column tops, which the first day's Parquet file holds as NULLs. Every key is a row of
            // its own. The var-size columns cycle through NULL, empty, a 4-byte UTF-8 character, a
            // value long enough to leave a VARCHAR's inline bytes, and a short ASCII value.
            execute("CREATE TABLE every (k INT, b BOOLEAN, by BYTE, sh SHORT, ch CHAR, i INT, l LONG, d DATE, "
                    + "ns TIMESTAMP_NS, f FLOAT, dbl DOUBLE, s SYMBOL, ip IPV4, str STRING, vc VARCHAR, bin BINARY, "
                    + "arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO every SELECT " + fixedSizeValues("x") + ", " + varSizeValues("x", false)
                    + ", timestamp_sequence('2020-01-01', 7_200_000_000L) FROM long_sequence(24)");
            execute("ALTER TABLE every ADD COLUMN u UUID");
            execute("ALTER TABLE every ADD COLUMN l256 LONG256");
            execute("ALTER TABLE every ADD COLUMN g1 GEOHASH(1c)");
            execute("ALTER TABLE every ADD COLUMN g3 GEOHASH(3c)");
            execute("ALTER TABLE every ADD COLUMN g6 GEOHASH(6c)");
            execute("ALTER TABLE every ADD COLUMN g12 GEOHASH(12c)");
            execute("ALTER TABLE every ADD COLUMN dec8 DECIMAL(2,1)");
            execute("ALTER TABLE every ADD COLUMN dec16 DECIMAL(4,1)");
            execute("ALTER TABLE every ADD COLUMN dec32 DECIMAL(9,2)");
            execute("ALTER TABLE every ADD COLUMN dec64 DECIMAL(18,2)");
            execute("ALTER TABLE every ADD COLUMN dec128 DECIMAL(38,2)");
            execute("ALTER TABLE every ADD COLUMN dec256 DECIMAL(50,2)");
            execute("ALTER TABLE every ADD COLUMN s2 SYMBOL");
            execute("ALTER TABLE every ADD COLUMN str2 STRING");
            execute("ALTER TABLE every ADD COLUMN vc2 VARCHAR");
            execute("ALTER TABLE every ADD COLUMN bin2 BINARY");
            execute("ALTER TABLE every ADD COLUMN arr2 DOUBLE[][]");
            execute("INSERT INTO every SELECT " + fixedSizeValues("(24 + x)") + ", " + varSizeValues("(24 + x)", false)
                    + ", timestamp_sequence('2020-01-03', 7_200_000_000L), "
                    + """
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_uuid4() END,
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_long256() END,
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(5) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(15) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(30) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE rnd_geohash(60) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x % 9)::DECIMAL(2,1) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x * 7)::DECIMAL(4,1) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x * 1_234)::DECIMAL(9,2) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x * 1_234_567_890L)::DECIMAL(18,2) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x * 123_456_789_012_345_678L)::DECIMAL(38,2) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE (x * -123_456_789_012_345_678L)::DECIMAL(50,2) END,
                    CASE WHEN x % 7 = 0 THEN null ELSE 't' || (x % 3) END,
                    """
                    + varSizeValues("(24 + x)", true) + " FROM long_sequence(48)");
            execute("ALTER TABLE every CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-02'");
            execute("ALTER TABLE every CONVERT PARTITION TO PARQUET WHERE ts IN '2020-01-04'");
            sqlExecutionContext.changePageFrameSizes(5, 5);
            // A factory per open scan: the frames' and the reference cursor's.
            try (RecordCursorFactory factory = select("every");
                 RecordCursorFactory scanFactory = select("every")) {
                final RecordMetadata metadata = factory.getMetadata();
                // Every column but the key, in reverse table order, so that a reader that ignored the
                // payload mapping would read the wrong column.
                final IntList mapping = new IntList();
                final IntList payloadColumns = new IntList();
                for (int i = metadata.getColumnCount() - 1; i > 0; i--) {
                    payloadColumns.add(mapping.size());
                    mapping.add(i);
                }
                // The reference: each key's row as the table's own record cursor reads it.
                final Map<Integer, String> expected = new HashMap<>();
                try (RecordCursor cursor = scanFactory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final StringSink sink = new StringSink();
                        printPayload(record, mapping, metadata, mapping, sink);
                        expected.put(record.getInt(0), sink.toString());
                    }
                }
                Assert.assertEquals(72, expected.size());
                final VarSizeColumns varSize = new VarSizeColumns(metadata, mapping);
                try (HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, mapping, metadata);
                     IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                    Assert.assertEquals(0, frames.getCopyRowSize());
                    final FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                    Assert.assertTrue(frames.hasParquetFrames());
                    // More frames than partitions, so that frames switch inside partitions too.
                    Assert.assertTrue("frames: " + frames.getFrameCount(), frames.getFrameCount() > 6);
                    try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                         FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                        // Two probes take turns at the keys in random order, so that nearly every lookup
                        // switches its reader to another frame, native or Parquet, and each reads its
                        // own row whatever the other did.
                        final Rnd rnd = TestUtils.generateRandom(LOG);
                        final int[] keys = new int[72];
                        for (int i = 0; i < keys.length; i++) {
                            keys[i] = i + 1;
                        }
                        for (int round = 0; round < 3; round++) {
                            shuffle(keys, rnd);
                            int keyA = -1;
                            int keyB = -1;
                            for (int i = 0; i < keys.length; i++) {
                                final FrozenHashJoinBuild.IntProbe probe = i % 2 == 0 ? a : b;
                                final int key = keys[i];
                                probe.find(key);
                                Assert.assertTrue(probe.hasNext());
                                probe.next();
                                Assert.assertFalse(probe.hasNext());
                                if (probe == a) {
                                    keyA = key;
                                } else {
                                    keyB = key;
                                }
                                varSize.assertRow("key " + key, probe.getRecord(), key);
                                for (FrozenHashJoinBuild.IntProbe p : new FrozenHashJoinBuild.IntProbe[]{a, b}) {
                                    final int current = p == a ? keyA : keyB;
                                    if (current > 0) {
                                        final StringSink sink = new StringSink();
                                        printPayload(p.getRecord(), payloadColumns, metadata, mapping, sink);
                                        TestUtils.assertEquals("key " + current, expected.get(current), sink);
                                    }
                                }
                            }
                        }

                        // A/B independence: a value read through the A getters of one row survives
                        // the reader moving to the next row of its frame and reading it through the
                        // B getters, in native and in Parquet frames.
                        final HashJoinPayloadSource.Reader reader = (HashJoinPayloadSource.Reader) a.getRecord();
                        int nativePairs = 0;
                        int parquetPairs = 0;
                        for (int key = 1; key < 72; key++) {
                            a.find(key);
                            a.next();
                            final int frameIndex = Rows.toPartitionIndex(reader.locator());
                            final CharSequence strA = reader.getStrA(varSize.str);
                            final Utf8Sequence vcA = reader.getVarcharA(varSize.vc);
                            final CharSequence symA = reader.getSymA(varSize.sym);
                            final Long256 long256A = reader.getLong256A(varSize.long256);
                            final long long256Word = long256A.getLong0();
                            a.find(key + 1);
                            a.next();
                            if (Rows.toPartitionIndex(reader.locator()) != frameIndex) {
                                continue;
                            }
                            if (frames.getAddressCache().getFrameFormat(frameIndex) == PartitionFormat.PARQUET) {
                                parquetPairs++;
                            } else {
                                nativePairs++;
                            }
                            final String label = "keys " + key + " and " + (key + 1);
                            final CharSequence strB = reader.getStrB(varSize.str);
                            final Utf8Sequence vcB = reader.getVarcharB(varSize.vc);
                            final CharSequence symB = reader.getSymB(varSize.sym);
                            final Long256 long256B = reader.getLong256B(varSize.long256);
                            TestUtils.assertEquals(label, varSizeText(key, "s"), strA);
                            TestUtils.assertEquals(label, varSizeText(key + 1, "s"), strB);
                            Assert.assertEquals(label, varSizeText(key, "v"), vcA == null ? null : Utf8s.stringFromUtf8Bytes(vcA));
                            Assert.assertEquals(label, varSizeText(key + 1, "v"), vcB == null ? null : Utf8s.stringFromUtf8Bytes(vcB));
                            TestUtils.assertEquals(label, key % 7 == 0 ? null : "s" + (key % 5), symA);
                            TestUtils.assertEquals(label, (key + 1) % 7 == 0 ? null : "s" + ((key + 1) % 5), symB);
                            Assert.assertNotSame(label, long256A, long256B);
                            Assert.assertEquals(label, long256Word, long256A.getLong0());
                        }
                        Assert.assertTrue("native pairs: " + nativePairs, nativePairs > 10);
                        Assert.assertTrue("parquet pairs: " + parquetPairs, parquetPairs > 5);
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testLocatorsPositionReadersAtTheirRows() throws Exception {
        assertMemoryLeak(() -> {
            // 600 rows over six daily partitions, the first two of them Parquet, read in frames of
            // 20 rows; seven keys repeat within frames, across frames, partitions and formats, and v
            // numbers the rows from one.
            execute("CREATE TABLE loc (k INT, v LONG, s SYMBOL, vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO loc
                    SELECT (x % 7)::INT, x, 's' || (x % 5),
                           CASE WHEN x % 11 = 0 THEN NULL ELSE 'row ' || x || ' \uD83D\uDE00' END,
                           timestamp_sequence('2020-01-01', 864_000_000L)
                    FROM long_sequence(600)
                    """);
            execute("ALTER TABLE loc CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            sqlExecutionContext.changePageFrameSizes(20, 20);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory factory = select("loc");
                 // v and s, which copy, sixteen bytes a row, and v and vc, which never do.
                 HashJoinBuildFrames fixedSize = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 HashJoinBuildFrames varSize = new HashJoinBuildFrames(configuration, ints(1, 3), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                Assert.assertEquals(16, fixedSize.getCopyRowSize());
                Assert.assertEquals(0, varSize.getCopyRowSize());
                final Rnd rnd = TestUtils.generateRandom(LOG);
                for (HashJoinBuildFrames frames : new HashJoinBuildFrames[]{fixedSize, varSize}) {
                    for (boolean isPartitioned : new boolean[]{false, true}) {
                        final String label = (frames == fixedSize ? "fixed-size" : "var-size") + (isPartitioned ? ", partitioned" : ", serial");
                        frames.open(factory, ORDER_ASC, sqlExecutionContext);
                        frames.loadFrames();
                        Assert.assertTrue(label, frames.hasParquetFrames());
                        final FrozenHashJoinBuild.IntKeyed frozen = isPartitioned
                                ? FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 64, -1, null, tracker, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER)
                                : FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_ASC, tracker, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        Assert.assertEquals(label, isPartitioned, build.getPartitionCount() > 1);
                        // A reader that the copy finds already open follows the copy's locators too.
                        try (HashJoinPayloadSource.Reader early = frames.newReader()) {
                            early.reopen();
                            assertLocators(label, frames, frozen, early, false, frames == varSize, rnd);
                            if (frames == fixedSize) {
                                frames.copyPayload(frozen, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                                assertLocators(label + ", copied", frames, frozen, early, true, false, rnd);
                            }
                        }
                        build.close();
                        // The copy holds the rows of that build, so the next build takes frames without one.
                        frames.clear();
                        Assert.assertEquals(label, 0, tracker.getUsed());
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testOpenInScanOrderBuildsChainsAsLongChain() throws Exception {
        assertMemoryLeak(() -> {
            for (String sql : FrameBuilds.SCAN_ORDER_TABLES) {
                execute(sql);
            }
            sqlExecutionContext.changePageFrameSizes(FrameBuilds.SCAN_ORDER_FRAME_ROWS, FrameBuilds.SCAN_ORDER_FRAME_ROWS);
            // A factory per open scan: a query's factory serves one open scan at a time.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(Long.MAX_VALUE);
                 RecordCursorFactory forward = select("t");
                 RecordCursorFactory backward = select("t ORDER BY ts DESC");
                 RecordCursorFactory serialBackward = select("t ORDER BY ts DESC");
                 HashJoinBuildFrames forwardFrames = new HashJoinBuildFrames(configuration, ints(1, 2), forward.getMetadata());
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1, 2), backward.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                Assert.assertEquals(ORDER_DESC, FrameBuilds.scanOrder(backward));
                // The serial hash join's chains over the backward scan list each key's rows from its
                // first in the table to its last.
                final Map<Integer, List<Long>> expected = FrameBuilds.longChains(serialBackward, record -> record.getInt(0), 1,
                        null, sqlExecutionContext);
                Assert.assertEquals(7, expected.size());
                for (List<Long> chain : expected.values()) {
                    for (int i = 1, n = chain.size(); i < n; i++) {
                        Assert.assertTrue(chain.get(i - 1) < chain.get(i));
                    }
                }

                forwardFrames.open(forward, ORDER_ASC, sqlExecutionContext);
                forwardFrames.loadFrames();
                frames.open(backward, ORDER_DESC, sqlExecutionContext);
                // The open scan serves the build input's symbol tables before it walks a frame.
                Assert.assertNotNull(frames.getSymbolTableSource().getSymbolTable(2));
                frames.loadFrames();
                Assert.assertEquals(600, frames.getRowCount());
                Assert.assertTrue(frames.hasParquetFrames());
                Assert.assertTrue("frames: " + frames.getFrameCount(), frames.getFrameCount() > 20);
                // The backward scan hands the frames of the forward one over, the last first.
                TestUtils.assertEquals(printFrames(forwardFrames, true, false), printFrames(frames, false, false));

                // The owner's build, which meets each frame's rows from the last, then reads the
                // payload through the frames and through a copy of it.
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildIntSerial(configuration, build, frames, 0, null, ORDER_DESC,
                        tracker, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                FrameBuilds.assertIntChains(frozen, expected, -1, 9);
                assertSymbolPayloads(frozen);
                frames.copyPayload(frozen, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                FrameBuilds.assertIntChains(frozen, expected, -1, 9);
                assertSymbolPayloads(frozen);
                build.close();
                // The copy holds the rows of that build, so the next build takes frames without one.
                frames.clear();
                frames.open(backward, ORDER_DESC, sqlExecutionContext);
                frames.loadFrames();

                // A parallel build, whose frame tasks place their frames' rows in the heap from the last.
                frozen = FrameBuilds.buildIntPartitioned(configuration, build, frames, 0, 7, -1, null, ORDER_DESC,
                        tracker, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                Assert.assertTrue("partitions: " + build.getPartitionCount(), build.getPartitionCount() > 1);
                Assert.assertTrue(build.hasFrameRegions());
                FrameBuilds.assertIntChains(frozen, expected, -1, 9);
                assertSymbolPayloads(frozen);
                build.close();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testPositionsAcrossFramesAndPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // 2_000 rows over 20 daily partitions, read in frames of three rows at most, and 50
            // keys, so every key's duplicates span frames and partitions.
            execute("CREATE TABLE spread (k INT, v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO spread
                    SELECT (x % 50)::INT, x, 's' || (x % 7), timestamp_sequence('2020-01-01', 864_000_000L)
                    FROM long_sequence(2_000)
                    """);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            try (RecordCursorFactory factory = select("spread");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                // Far more frames than partitions, so frame switches happen inside partitions too.
                Assert.assertTrue("frames: " + frames.getFrameCount(), frames.getFrameCount() > 100);
                Assert.assertEquals(2_000, frames.getRowCount());
                try (FrozenHashJoinBuild.IntProbe a = frozen.newProbe();
                     FrozenHashJoinBuild.IntProbe b = frozen.newProbe()) {
                    Rnd rnd = new Rnd();
                    // Two probes interleave their lookups in random key order; each positions its
                    // own reader, so neither moves the other's row.
                    for (int i = 0; i < 200; i++) {
                        final int keyA = rnd.nextInt(50);
                        final int keyB = rnd.nextInt(50);
                        a.find(keyA);
                        b.find(keyB);
                        // Duplicates come back in reverse input order: the latest row first.
                        long expectedA = 2_000 - ((2_000 - keyA) % 50);
                        long expectedB = 2_000 - ((2_000 - keyB) % 50);
                        while (a.hasNext() || b.hasNext()) {
                            if (a.hasNext()) {
                                a.next();
                            }
                            if (b.hasNext()) {
                                b.next();
                            }
                            if (expectedA > 0) {
                                Assert.assertEquals(expectedA, a.getRecord().getLong(0));
                                TestUtils.assertEquals("s" + (expectedA % 7), a.getRecord().getSymA(1));
                            }
                            if (expectedB > 0) {
                                Assert.assertEquals(expectedB, b.getRecord().getLong(0));
                                TestUtils.assertEquals("s" + (expectedB % 7), b.getRecord().getSymA(1));
                            }
                            expectedA -= 50;
                            expectedB -= 50;
                        }
                    }
                }
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testColumnTopsReadAsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tops (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tops VALUES (1, '2020-01-01'), (2, '2020-01-01T01')");
            execute("ALTER TABLE tops ADD COLUMN v DOUBLE");
            execute("ALTER TABLE tops ADD COLUMN s SYMBOL");
            execute("INSERT INTO tops VALUES (1, '2020-01-01T02', 1.5, 'x'), (2, '2020-01-02', 2.5, 'y')");
            try (RecordCursorFactory factory = select("tops");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(2, 3), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    // Rows written before the columns existed sit below their column tops.
                    for (int key = 1; key <= 2; key++) {
                        probe.find(key);
                        probe.next();
                        Assert.assertEquals(key + 0.5, probe.getRecord().getDouble(0), 0);
                        TestUtils.assertEquals(key == 1 ? "x" : "y", probe.getRecord().getSymA(1));
                        probe.next();
                        Assert.assertTrue(Double.isNaN(probe.getRecord().getDouble(0)));
                        Assert.assertNull(probe.getRecord().getSymA(1));
                        Assert.assertFalse(probe.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testVarSizePayloadNeverCopies() throws Exception {
        assertMemoryLeak(() -> {
            // 3_000 rows over three daily partitions of 1_000 rows, the first two of them Parquet, a
            // row group each; v is the one fixed-size column besides the key.
            execute("CREATE TABLE vs (k INT, v LONG, str STRING, vc VARCHAR, bin BINARY, arr DOUBLE[], arr2 DOUBLE[][], ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO vs
                    SELECT (x % 100)::INT, x, 's' || x, 'v' || x, rnd_bin(1, 8, 0), ARRAY[x::DOUBLE], ARRAY[[x::DOUBLE]],
                           timestamp_sequence('2020-01-01', 86_400_000L)
                    FROM long_sequence(3_000)
                    """);
            execute("ALTER TABLE vs CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            try (RecordCursorFactory factory = select("vs")) {
                final RecordMetadata metadata = factory.getMetadata();
                // Each var-size type alone, and next to v before or after it, and all of them
                // together; v alone, and v twice, are the payloads that copy.
                final int[][] payloads = {{1}, {1, 1}, {2}, {3}, {4}, {5}, {6}, {1, 2}, {3, 1}, {1, 4, 1}, {5, 1}, {1, 6}, {6, 5, 4, 3, 2, 1}};
                for (int[] payload : payloads) {
                    boolean isVarSize = false;
                    final StringSink names = new StringSink();
                    for (int column : payload) {
                        isVarSize |= column != 1;
                        names.put(metadata.getColumnName(column)).put(' ');
                    }
                    final String label = "payload " + names;
                    // Four probe readers share the decode cache, a quarter of it each.
                    try (HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(payload), metadata, 4);
                         IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                        // A copied LONG takes eight bytes; a payload with a var-size column has no copy row.
                        Assert.assertEquals(label, isVarSize ? 0 : Long.BYTES * payload.length, frames.getCopyRowSize());
                        final FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                        final long buildRows = frozen.getRowCount();
                        Assert.assertEquals(label, 3_000, buildRows);
                        Assert.assertEquals(label, 100, frozen.getKeyCount());
                        // What a stage counts once its build froze: it keeps every row here.
                        long parquetFrameRows = 0;
                        int parquetFrameCount = 0;
                        for (int frameIndex = 0, n = frames.getFrameCount(); frameIndex < n; frameIndex++) {
                            if (frames.getAddressCache().getFrameFormat(frameIndex) == PartitionFormat.PARQUET) {
                                parquetFrameRows += frames.getFrameRowCount(frameIndex);
                                parquetFrameCount++;
                            }
                        }
                        Assert.assertEquals(label, 2_000, parquetFrameRows);
                        Assert.assertEquals(label, 2, parquetFrameCount);
                        // Decode caches whose quarter holds nothing, less than a row group, and all of it.
                        boolean hasOverReaderCache = false;
                        boolean hasUnderReaderCache = false;
                        for (long budget : new long[]{0, 1_024, 1L << 40}) {
                            setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
                            Assert.assertEquals(label, budget / 4, frames.getReaderCacheSize());
                            final boolean isOverReaderCache = frames.isReaderCacheExceeded(parquetFrameRows);
                            hasOverReaderCache |= isOverReaderCache;
                            hasUnderReaderCache |= !isOverReaderCache;
                            for (long probeRows : new long[]{0, 1, 2, buildRows, 1_000 * buildRows, Long.MAX_VALUE}) {
                                final String caseLabel = label + ", budget " + budget + ", probe rows " + probeRows;
                                final boolean isParquetReached = HashJoinBuildStage.isParquetInProbeReach(probeRows, buildRows,
                                        frozen.getKeyCount(), parquetFrameCount);
                                // Without a byte bound and a probe ratio every payload that the copy can
                                // hold copies, for any probe, and no payload that it cannot.
                                Assert.assertEquals(caseLabel, !isVarSize, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows,
                                        isParquetReached, isOverReaderCache, frames.getCopyRowSize(), probeRows, Long.MAX_VALUE, 0));
                                // Nor under the configured bound and either configured probe ratio.
                                if (isVarSize) {
                                    for (double ratio : new double[]{
                                            configuration.getSqlParallelHashJoinGroupByPayloadCopyMinProbeRatio(),
                                            configuration.getSqlParallelHashJoinGroupByPayloadCopyParallelMinProbeRatio()
                                    }) {
                                        Assert.assertFalse(caseLabel, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows,
                                                isParquetReached, isOverReaderCache, frames.getCopyRowSize(), probeRows,
                                                configuration.getSqlParallelHashJoinGroupByPayloadCopyMaxSize(), ratio));
                                    }
                                }
                            }
                        }
                        Assert.assertTrue(label, hasOverReaderCache && hasUnderReaderCache);
                        if (!isVarSize) {
                            // What keeps a payload that the copy holds from copying: a byte bound one
                            // byte short of the copy, and a build without rows; at the bound it copies.
                            final long copySize = buildRows * frames.getCopyRowSize();
                            Assert.assertFalse(label, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows, true, true,
                                    frames.getCopyRowSize(), buildRows, copySize - 1, 0));
                            Assert.assertTrue(label, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows, true, true,
                                    frames.getCopyRowSize(), buildRows, copySize, 0));
                            Assert.assertFalse(label, HashJoinBuildStage.isPayloadCopyWorthIt(0, 0, true, true,
                                    frames.getCopyRowSize(), buildRows, Long.MAX_VALUE, 0));
                            // A probe that does not repay the copy under a probe ratio, in reach of the
                            // Parquet frames or not, while they fit a reader's share.
                            Assert.assertFalse(label, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows, false, false,
                                    frames.getCopyRowSize(), buildRows - 1, Long.MAX_VALUE, 1));
                            Assert.assertFalse(label, HashJoinBuildStage.isPayloadCopyWorthIt(buildRows, parquetFrameRows, true, false,
                                    frames.getCopyRowSize(), buildRows - parquetFrameRows - 1, Long.MAX_VALUE, 1));
                        }
                        if (isVarSize) {
                            // The frames refuse a copy they cannot hold, and the readers keep reading
                            // the columns where they live.
                            Assert.assertThrows(AssertionError.class, () -> frames.copyPayload(frozen, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                        } else {
                            frames.copyPayload(frozen, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        }
                        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                            probe.find(42);
                            long v = 3_000 - 58;
                            while (probe.hasNext()) {
                                probe.next();
                                // The owner's build appends the rows in scan order, so the row with
                                // this v is the (v - 1)-th: the copy's ordinal, or a row of the frames.
                                Assert.assertEquals(label, isVarSize ? scanRowId(frames, v - 1) : v - 1,
                                        ((HashJoinPayloadSource.Reader) probe.getRecord()).locator());
                                for (int col = 0; col < payload.length; col++) {
                                    switch (payload[col]) {
                                        case 1 -> Assert.assertEquals(label, v, probe.getRecord().getLong(col));
                                        case 2 -> TestUtils.assertEquals(label, "s" + v, probe.getRecord().getStrA(col));
                                        case 3 -> TestUtils.assertEquals(label, "v" + v, Utf8s.stringFromUtf8Bytes(probe.getRecord().getVarcharA(col)));
                                        case 4 -> Assert.assertTrue(label, probe.getRecord().getBinLen(col) > 0);
                                        default -> Assert.assertEquals(label, v, probe.getRecord().getArrayDouble1d2d(col,
                                                metadata.getColumnType(payload[col]), 0, 0), 0);
                                    }
                                }
                                v -= 100;
                            }
                            Assert.assertEquals(label, 42 - 100, v);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWideFixedSizePayloadTypesRoundTripAndNullExtend() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wide (k INT, ip IPV4, u UUID, l256 LONG256, "
                    + "g1 GEOHASH(1c), g3 GEOHASH(3c), g6 GEOHASH(6c), g12 GEOHASH(12c), "
                    + "dec8 DECIMAL(2,1), dec16 DECIMAL(4,1), dec32 DECIMAL(9,2), "
                    + "dec64 DECIMAL(18,2), dec128 DECIMAL(38,2), dec256 DECIMAL(50,2))");
            execute("""
                    INSERT INTO wide VALUES
                    (7, '10.0.0.7', '11111111-2222-3333-4444-555555555555',
                     '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                     'q', 'sp0', 'sp052w', 'sp052w92p1p8',
                     1.5::DECIMAL(2,1), 12.5::DECIMAL(4,1), 1234.56::DECIMAL(9,2),
                     1234567890.12::DECIMAL(18,2), 123456789012345678.90::DECIMAL(38,2),
                     12345678901234567890123456.78::DECIMAL(50,2)),
                    (8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)""");
            String[] names = {"dec256", "dec8", "dec128", "dec16", "l256", "dec32", "u", "dec64",
                    "g1", "g3", "g6", "g12", "ip"};
            try (RecordCursorFactory factory = select("wide")) {
                IntList mapping = new IntList();
                for (String name : names) {
                    mapping.add(factory.getMetadata().getColumnIndex(name));
                }
                try (HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, mapping, factory.getMetadata());
                     IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                    FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                    try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                        probe.find(7);
                        probe.next();
                        Record record = probe.getRecord();
                        Decimal256 decimal256 = new Decimal256();
                        Decimal128 decimal128 = new Decimal128();
                        // A record reads raw words; the scale rides in the column's type, as for
                        // every other fixed-size record.
                        record.getDecimal256(0, decimal256);
                        decimal256.of(decimal256.getHh(), decimal256.getHl(), decimal256.getLh(), decimal256.getLl(),
                                ColumnType.getDecimalScale(factory.getMetadata().getColumnType(mapping.getQuick(0))));
                        TestUtils.assertEquals("12345678901234567890123456.78", decimal256.toString());
                        Assert.assertEquals(15, record.getDecimal8(1));
                        record.getDecimal128(2, decimal128);
                        decimal128.of(decimal128.getHigh(), decimal128.getLow(),
                                ColumnType.getDecimalScale(factory.getMetadata().getColumnType(mapping.getQuick(2))));
                        TestUtils.assertEquals("123456789012345678.90", decimal128.toString());
                        Assert.assertEquals(125, record.getDecimal16(3));
                        TestUtils.assertEquals("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                                record.getLong256A(4).toString());
                        // getLong256B() must survive a getLong256A() on the same column.
                        Long256 a = record.getLong256A(4);
                        Long256 b = record.getLong256B(4);
                        Assert.assertNotSame(a, b);
                        Assert.assertEquals(a.getLong0(), b.getLong0());
                        Assert.assertEquals(123456, record.getDecimal32(5));
                        Assert.assertEquals(0x1111111122223333L, record.getLong128Hi(6));
                        Assert.assertEquals(0x4444555555555555L, record.getLong128Lo(6));
                        Assert.assertEquals(123456789012L, record.getDecimal64(7));
                        Assert.assertEquals(GeoHashes.fromString("q", 0, 1), record.getGeoByte(8));
                        Assert.assertEquals(GeoHashes.fromString("sp0", 0, 3), record.getGeoShort(9));
                        Assert.assertEquals(GeoHashes.fromString("sp052w", 0, 6), record.getGeoInt(10));
                        Assert.assertEquals(GeoHashes.fromString("sp052w92p1p8", 0, 12), record.getGeoLong(11));
                        Assert.assertEquals(Numbers.parseIPv4("10.0.0.7"), record.getIPv4(12));
                        Assert.assertFalse(probe.hasNext());
                        // The NULL row keeps every type's own sentinel, which is what a LEFT join's
                        // null-extended row has to match.
                        probe.find(8);
                        probe.next();
                        record.getDecimal256(0, decimal256);
                        Assert.assertTrue(decimal256.isNull());
                        Assert.assertEquals(Decimals.DECIMAL8_NULL, record.getDecimal8(1));
                        record.getDecimal128(2, decimal128);
                        Assert.assertTrue(decimal128.isNull());
                        Assert.assertEquals(Decimals.DECIMAL16_NULL, record.getDecimal16(3));
                        Assert.assertEquals(Long256Impl.NULL_LONG256, record.getLong256A(4));
                        Assert.assertEquals(Decimals.DECIMAL32_NULL, record.getDecimal32(5));
                        Assert.assertEquals(Numbers.LONG_NULL, record.getLong128Hi(6));
                        Assert.assertEquals(Numbers.LONG_NULL, record.getLong128Lo(6));
                        Assert.assertEquals(Decimals.DECIMAL64_NULL, record.getDecimal64(7));
                        Assert.assertEquals(GeoHashes.BYTE_NULL, record.getGeoByte(8));
                        Assert.assertEquals(GeoHashes.SHORT_NULL, record.getGeoShort(9));
                        Assert.assertEquals(GeoHashes.INT_NULL, record.getGeoInt(10));
                        Assert.assertEquals(GeoHashes.NULL, record.getGeoLong(11));
                        Assert.assertEquals(Numbers.IPv4_NULL, record.getIPv4(12));
                        Assert.assertFalse(probe.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testReadersTakeOwnSymbolTablesAndReleaseParquetMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pq (k INT, v DOUBLE, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO pq
                    SELECT (x % 10)::INT, x, 's' || (x % 3), timestamp_sequence('2020-01-01', 3_600_000_000L)
                    FROM long_sequence(96)
                    """);
            // Two of the four days go to Parquet, which each reader decodes into a pool of its own.
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            final MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
            final ObjList<FrozenHashJoinBuild.IntProbe> probes = new ObjList<>();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 RecordCursorFactory factory = select("pq");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1, 2), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                final long buildBytes = tracker.getUsed();
                for (int i = 0; i < 2; i++) {
                    probes.add(frozen.newProbe());
                }
                FrozenHashJoinBuild.IntProbe a = probes.getQuick(0);
                FrozenHashJoinBuild.IntProbe b = probes.getQuick(1);
                // Each reader holds its own symbol table; a fresh one is a third.
                Assert.assertNotSame(a.getSymbolTable(1), b.getSymbolTable(1));
                Assert.assertNotSame(a.getSymbolTable(1), a.newSymbolTable(1));
                for (int key = 0; key < 10; key++) {
                    a.find(key);
                    long expected = 96 - ((96 - key) % 10);
                    while (a.hasNext()) {
                        a.next();
                        Assert.assertEquals(expected, a.getRecord().getDouble(0), 0);
                        TestUtils.assertEquals("s" + (expected % 3), a.getRecord().getSymA(1));
                        expected -= 10;
                    }
                    Assert.assertEquals(key == 0 ? 0 : key - 10, expected);
                }
                // Reading Parquet rows charged decode buffers to the execution; closing the
                // reader's probe releases them, and a reopened probe reads again.
                Assert.assertTrue(tracker.getUsed() >= buildBytes);
                a.close();
                b.close();
                Assert.assertEquals(buildBytes, tracker.getUsed());
                a.reopen();
                Assert.assertEquals(91, probeFirst(a, 1));
                a.close();
                Assert.assertEquals(buildBytes, tracker.getUsed());
                build.close();
                frames.clear();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                Misc.freeObjList(probes);
                sqlExecutionContext.setMemoryTracker(previous);
            }
        });
    }

    @Test
    public void testReadersDecodeNoFilterOnlyColumnOfParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            // msg stands in for a column that only the build's filters read: it stays in the build
            // scan, yet no reader reads it. Its 300 bytes a row compress well, so a Parquet row group
            // decodes it to a 16-byte slice entry a row and to the decompressed pages that the entries
            // point into. The INT key k and the timestamp are not payload either.
            execute("CREATE TABLE pq (k INT, v DOUBLE, msg VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO pq
                    SELECT (x % 10)::INT, x, rpad(x::VARCHAR, 300, ' compressible log line'), timestamp_sequence('2020-01-01', 86_400_000L)
                    FROM long_sequence(3_000)
                    """);
            // The first two days, 1_000 rows each, turn into a Parquet row group each; the third stays native.
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            final MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 RecordCursorFactory factory = select("pq");
                 // The payload is v alone.
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 IntHashJoinBuild build = new IntHashJoinBuild(true, 2, 16)) {
                sqlExecutionContext.setMemoryTracker(tracker);
                FrozenHashJoinBuild.IntKeyed frozen = FrameBuilds.buildInt(configuration, build, frames, factory, 0, sqlExecutionContext);
                final long buildBytes = tracker.getUsed();
                try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
                    final long readerBytes = tracker.getUsed();
                    // Key 3 has 100 rows a day, so the probe reader decodes both Parquet row groups.
                    probe.find(3);
                    long count = 0;
                    long sum = 0;
                    while (probe.hasNext()) {
                        probe.next();
                        sum += (long) probe.getRecord().getDouble(0);
                        count++;
                    }
                    Assert.assertEquals(300, count);
                    Assert.assertEquals(449_400, sum);
                    assertPayloadOnlyDecode(tracker.getUsed() - readerBytes);
                }
                Assert.assertEquals(buildBytes, tracker.getUsed());
                assertCopyReaderDecodesPayloadOnly(frames, frozen, tracker, buildBytes);
                build.close();
                frames.clear();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                sqlExecutionContext.setMemoryTracker(previous);
            }
        });
    }

    @Test
    public void testReadersDecodeNoVarcharKeyOfParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            // The build keys its rows by k, 300 bytes of text a row that compress well, and the
            // payload is v alone. A reader never reads the key, which the key table holds.
            execute("CREATE TABLE pk (k VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO pk
                    SELECT rpad((x % 1_000)::VARCHAR, 300, ' compressible log line'), x, timestamp_sequence('2020-01-01', 86_400_000L)
                    FROM long_sequence(3_000)
                    """);
            execute("ALTER TABLE pk CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
            execute("CREATE TABLE pp (k VARCHAR)");
            execute("INSERT INTO pp VALUES (rpad('5', 300, ' compressible log line'))");
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.VARCHAR);
            final ListColumnFilter keyFilter = new ListColumnFilter();
            keyFilter.add(1);
            final BytecodeAssembler asm = new BytecodeAssembler();
            final MemoryTracker previous = sqlExecutionContext.getMemoryTracker();
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 RecordCursorFactory factory = select("pk");
                 RecordCursorFactory probeFactory = select("pp");
                 HashJoinBuildFrames frames = new HashJoinBuildFrames(configuration, ints(1), factory.getMetadata());
                 MapHashJoinBuild build = new MapHashJoinBuild(configuration, keyTypes, true, 4, 1024, 64)) {
                final RecordSink buildSink = RecordSinkFactory.getInstance(configuration, asm, factory.getMetadata(), keyFilter, null);
                final RecordSink probeSink = RecordSinkFactory.getInstance(configuration, asm, probeFactory.getMetadata(), keyFilter, null);
                sqlExecutionContext.setMemoryTracker(tracker);
                FrozenHashJoinBuild.RecordKeyed frozen = FrameBuilds.buildMap(configuration, build, frames, factory, buildSink, sqlExecutionContext);
                final long buildBytes = tracker.getUsed();
                try (RecordCursor probeCursor = probeFactory.getCursor(sqlExecutionContext);
                     FrozenHashJoinBuild.RecordProbe probe = frozen.newProbe(probeSink)) {
                    Assert.assertTrue(probeCursor.hasNext());
                    final long readerBytes = tracker.getUsed();
                    // The key's rows are rows 5, 1_005 and 2_005, one a day, so the probe reader
                    // decodes both Parquet row groups.
                    probe.find(probeCursor.getRecord());
                    long count = 0;
                    long sum = 0;
                    while (probe.hasNext()) {
                        probe.next();
                        sum += (long) probe.getRecord().getDouble(0);
                        count++;
                    }
                    Assert.assertEquals(3, count);
                    Assert.assertEquals(3_015, sum);
                    assertPayloadOnlyDecode(tracker.getUsed() - readerBytes);
                }
                Assert.assertEquals(buildBytes, tracker.getUsed());
                assertCopyReaderDecodesPayloadOnly(frames, frozen, tracker, buildBytes);
                build.close();
                frames.clear();
                Assert.assertEquals(0, tracker.getUsed());
            } finally {
                sqlExecutionContext.setMemoryTracker(previous);
            }
        });
    }

    // Copies the payload, the DOUBLE v, of every build row through one copy reader, which walks the
    // rows in build order and keeps the row groups it decoded until the copy publishes, and checks
    // that it decoded the payload of the two Parquet row groups alone.
    private static void assertCopyReaderDecodesPayloadOnly(
            HashJoinBuildFrames frames,
            FrozenHashJoinBuild frozen,
            LimitedMemoryTracker tracker,
            long buildBytes
    ) {
        final long copyBytes = frozen.getRowCount() * Double.BYTES;
        frames.beginCopy(frozen, 1, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        frames.copyRows(0, frozen, 0, frozen.getRowCount(), SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        assertPayloadOnlyDecode(tracker.getUsed() - buildBytes - copyBytes);
        frames.publishCopy();
        Assert.assertEquals(buildBytes + copyBytes, tracker.getUsed());
    }

    /**
     * Walks every match of the seven keys of table loc through a probe of the frozen build and takes
     * each match's locator from the probe's reader. The locator names the row that the match read:
     * the build row's id while the readers read the frames, and once the copy is published the
     * copied row's ordinal, whose build row the frozen build names. Every row matches once, under a
     * locator of its own. Then the probe's reader and another reader of the frames follow every
     * locator, in random order, and read the payload of the row it names: v, and s or vc.
     */
    private static void assertLocators(
            String label,
            HashJoinBuildFrames frames,
            FrozenHashJoinBuild.IntKeyed frozen,
            HashJoinPayloadSource.Reader other,
            boolean isCopied,
            boolean isVarcharPayload,
            Rnd rnd
    ) {
        final LongList locators = new LongList();
        final LongList values = new LongList();
        final LongHashSet seenLocators = new LongHashSet();
        final LongHashSet seenValues = new LongHashSet();
        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe();
             PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            final HashJoinPayloadSource.Reader reader = (HashJoinPayloadSource.Reader) probe.getRecord();
            for (int key = 0; key < 7; key++) {
                probe.find(key);
                while (probe.hasNext()) {
                    probe.next();
                    final long locator = reader.locator();
                    final long v = reader.getLong(0);
                    if (isCopied) {
                        Assert.assertTrue(label, locator >= 0 && locator < frozen.getRowCount());
                    }
                    // The table's own row at the row id that the locator names holds the match.
                    final long rowId = isCopied ? frozen.getRowId(locator) : locator;
                    record.init(pool.navigateTo(Rows.toPartitionIndex(rowId)));
                    record.setRowIndex(Rows.toLocalRowID(rowId));
                    Assert.assertEquals(label, key, record.getInt(0));
                    Assert.assertEquals(label, v, record.getLong(1));
                    Assert.assertTrue(label, seenLocators.add(locator));
                    Assert.assertTrue(label, seenValues.add(v));
                    locators.add(locator);
                    values.add(v);
                }
            }
            Assert.assertEquals(label, 600, locators.size());
            for (long v = 1; v <= 600; v++) {
                Assert.assertTrue(label, seenValues.contains(v));
            }
            final int[] order = new int[locators.size()];
            for (int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            shuffle(order, rnd);
            for (int i : order) {
                final long locator = locators.getQuick(i);
                final long v = values.getQuick(i);
                for (HashJoinPayloadSource.Reader r : new HashJoinPayloadSource.Reader[]{other, reader}) {
                    r.positionLocator(locator);
                    Assert.assertEquals(label, locator, r.locator());
                    Assert.assertEquals(label, v, r.getLong(0));
                    if (isVarcharPayload) {
                        final String text = v % 11 == 0 ? null : "row " + v + " \uD83D\uDE00";
                        final Utf8Sequence value = r.getVarcharA(1);
                        Assert.assertEquals(label, text, value == null ? null : Utf8s.stringFromUtf8Bytes(value));
                    } else {
                        TestUtils.assertEquals(label, "s" + (v % 5), r.getSymA(1));
                    }
                }
            }
        }
    }

    // A reader that decodes the two Parquet row groups of 1_000 rows each, and of their build scan
    // columns only the DOUBLE payload, decodes 8 bytes a row and the decoder's bookkeeping, which
    // measured 832 bytes a row group, 17_664 bytes in all. The upper bound, 12 bytes a row, leaves
    // room for that bookkeeping and fails a reader that also decodes the narrowest other column of a
    // build scan here, the 4-byte INT key k, which measured 25_664 bytes, or the 16-byte slice entry
    // a row of a VARCHAR column.
    private static void assertPayloadOnlyDecode(long decodedBytes) {
        final long parquetRows = 2_000;
        Assert.assertTrue("decoded " + decodedBytes, decodedBytes >= parquetRows * Double.BYTES);
        Assert.assertTrue("decoded " + decodedBytes, decodedBytes < parquetRows * (Double.BYTES + Integer.BYTES));
    }

    // Every match of every key of SCAN_ORDER_TABLES' t resolves its SYMBOL payload, the second
    // payload column, to the text that t's row of that v holds.
    private static void assertSymbolPayloads(FrozenHashJoinBuild.IntKeyed frozen) {
        long matchCount = 0;
        try (FrozenHashJoinBuild.IntProbe probe = frozen.newProbe()) {
            for (int key = -1; key < 6; key++) {
                probe.find(key < 0 ? Numbers.INT_NULL : key);
                while (probe.hasNext()) {
                    probe.next();
                    final long v = probe.getRecord().getLong(0);
                    TestUtils.assertEquals(v % 17 == 0 ? null : "s" + (v % 5), probe.getRecord().getSymA(1));
                    matchCount++;
                }
            }
        }
        Assert.assertEquals(600, matchCount);
    }

    // The values of table every's columns from k to ip for the row whose key the expression computes:
    // a NULL row every seventh key.
    private static String fixedSizeValues(String key) {
        return """
                #K::INT,
                CASE WHEN #K % 7 = 0 THEN null ELSE #K % 2 = 0 END,
                CASE WHEN #K % 7 = 0 THEN null ELSE (#K % 100)::BYTE END,
                CASE WHEN #K % 7 = 0 THEN null ELSE (#K * 3)::SHORT END,
                CASE WHEN #K % 7 = 0 THEN null ELSE rnd_char() END,
                CASE WHEN #K % 7 = 0 THEN null ELSE (#K * 11)::INT END,
                CASE WHEN #K % 7 = 0 THEN null ELSE #K * 1_000_003 END,
                CASE WHEN #K % 7 = 0 THEN null ELSE (#K * 86_400_000)::DATE END,
                CASE WHEN #K % 7 = 0 THEN null ELSE (#K * 1_000_000_007)::TIMESTAMP_NS END,
                CASE WHEN #K % 7 = 0 THEN null ELSE #K / 4.0 END::FLOAT,
                CASE WHEN #K % 7 = 0 THEN null ELSE #K / 8.0 END,
                CASE WHEN #K % 7 = 0 THEN null ELSE 's' || (#K % 5) END,
                CASE WHEN #K % 7 = 0 THEN null ELSE ('10.0.0.' || (#K % 250 + 1))::IPV4 END
                """.replace("#K", key);
    }

    private static IntList ints(int... values) {
        IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    // Prints every getter that the payload column's type answers, for the build row with this key.
    private static void printRow(FrozenHashJoinBuild.IntProbe probe, int key, RecordMetadata metadata, IntList mapping, StringSink sink) {
        probe.find(key);
        Assert.assertTrue(probe.hasNext());
        probe.next();
        Assert.assertFalse(probe.hasNext());
        final Record record = probe.getRecord();
        final Decimal128 decimal128 = new Decimal128();
        final Decimal256 decimal256 = new Decimal256();
        sink.put(key);
        for (int col = 0, n = mapping.size(); col < n; col++) {
            sink.put('|');
            switch (ColumnType.tagOf(metadata.getColumnType(mapping.getQuick(col)))) {
                case ColumnType.BOOLEAN -> sink.put(record.getBool(col));
                case ColumnType.BYTE -> sink.put(record.getByte(col));
                case ColumnType.SHORT -> sink.put(record.getShort(col));
                case ColumnType.CHAR -> sink.put((int) record.getChar(col));
                case ColumnType.INT -> sink.put(record.getInt(col));
                case ColumnType.LONG -> sink.put(record.getLong(col));
                case ColumnType.DATE -> sink.put(record.getDate(col));
                case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(col));
                case ColumnType.FLOAT -> sink.put(record.getFloat(col));
                case ColumnType.DOUBLE -> sink.put(record.getDouble(col));
                case ColumnType.SYMBOL ->
                        sink.put(record.getInt(col)).put(':').put(record.getSymA(col)).put(':').put(record.getSymB(col));
                case ColumnType.IPv4 -> sink.put(record.getIPv4(col));
                case ColumnType.UUID -> sink.put(record.getLong128Lo(col)).put(':').put(record.getLong128Hi(col));
                case ColumnType.LONG256 -> {
                    record.getLong256(col, sink);
                    final Long256 a = record.getLong256A(col);
                    final Long256 b = record.getLong256B(col);
                    Assert.assertNotSame(a, b);
                    sink.put(':').put(a.getLong0()).put(':').put(b.getLong3());
                }
                case ColumnType.GEOBYTE -> sink.put(record.getGeoByte(col));
                case ColumnType.GEOSHORT -> sink.put(record.getGeoShort(col));
                case ColumnType.GEOINT -> sink.put(record.getGeoInt(col));
                case ColumnType.GEOLONG -> sink.put(record.getGeoLong(col));
                case ColumnType.DECIMAL8 -> sink.put(record.getDecimal8(col));
                case ColumnType.DECIMAL16 -> sink.put(record.getDecimal16(col));
                case ColumnType.DECIMAL32 -> sink.put(record.getDecimal32(col));
                case ColumnType.DECIMAL64 -> sink.put(record.getDecimal64(col));
                case ColumnType.DECIMAL128 -> {
                    record.getDecimal128(col, decimal128);
                    sink.put(decimal128.getHigh()).put(':').put(decimal128.getLow());
                }
                case ColumnType.DECIMAL256 -> {
                    record.getDecimal256(col, decimal256);
                    sink.put(decimal256.getHh()).put(':').put(decimal256.getHl()).put(':')
                            .put(decimal256.getLh()).put(':').put(decimal256.getLl());
                }
                default -> Assert.fail("unexpected payload type in column " + col);
            }
        }
        sink.put('\n');
    }

    // Prints the rows of the open frames, the last frame first when reversed, each frame's rows in
    // storage order, with a header per frame when asked.
    private static String printFrames(HashJoinBuildFrames frames, boolean isReversed, boolean hasHeaders) {
        final StringSink sink = new StringSink();
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            final int frameCount = frames.getFrameCount();
            for (int i = 0; i < frameCount; i++) {
                final int frameIndex = isReversed ? frameCount - 1 - i : i;
                final long frameRows = frames.getFrameRowCount(frameIndex);
                if (hasHeaders) {
                    sink.put('#').put(frameIndex).put(" rows=").put(frameRows).put('\n');
                }
                record.init(pool.navigateTo(frameIndex));
                for (long row = 0; row < frameRows; row++) {
                    record.setRowIndex(row);
                    sink.put(record.getInt(0)).put(',').put(record.getLong(1)).put(',')
                            .put(record.getSymA(2)).put(',').put(record.getTimestamp(3)).put('\n');
                }
            }
        }
        return sink.toString();
    }

    // Prints every getter that each payload column's type answers, column i of the record being
    // columns[i] and of the type of the build scan's column mapping[i].
    private static void printPayload(Record record, IntList columns, RecordMetadata metadata, IntList mapping, StringSink sink) {
        final Decimal128 decimal128 = new Decimal128();
        final Decimal256 decimal256 = new Decimal256();
        for (int i = 0, n = mapping.size(); i < n; i++) {
            final int col = columns.getQuick(i);
            final int columnType = metadata.getColumnType(mapping.getQuick(i));
            sink.put(metadata.getColumnName(mapping.getQuick(i))).put('=');
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN -> sink.put(record.getBool(col));
                case ColumnType.BYTE -> sink.put(record.getByte(col));
                case ColumnType.SHORT -> sink.put(record.getShort(col));
                case ColumnType.CHAR -> sink.put((int) record.getChar(col));
                case ColumnType.INT -> sink.put(record.getInt(col));
                case ColumnType.LONG -> sink.put(record.getLong(col));
                case ColumnType.DATE -> sink.put(record.getDate(col));
                case ColumnType.TIMESTAMP -> sink.put(record.getTimestamp(col));
                case ColumnType.FLOAT -> sink.put(record.getFloat(col));
                case ColumnType.DOUBLE -> sink.put(record.getDouble(col));
                case ColumnType.SYMBOL -> {
                    sink.put(record.getInt(col)).put(':');
                    putNullable(sink, record.getSymA(col));
                    sink.put(':');
                    putNullable(sink, record.getSymB(col));
                }
                case ColumnType.IPv4 -> sink.put(record.getIPv4(col)).put(':').put(record.getLongIPv4(col));
                case ColumnType.UUID -> sink.put(record.getLong128Lo(col)).put(':').put(record.getLong128Hi(col));
                case ColumnType.LONG256 -> {
                    record.getLong256(col, sink);
                    final Long256 a = record.getLong256A(col);
                    final Long256 b = record.getLong256B(col);
                    Assert.assertNotSame(a, b);
                    sink.put(':').put(a.getLong0()).put(':').put(a.getLong3()).put(':').put(b.getLong0()).put(':').put(b.getLong3());
                }
                case ColumnType.GEOBYTE -> sink.put(record.getGeoByte(col));
                case ColumnType.GEOSHORT -> sink.put(record.getGeoShort(col));
                case ColumnType.GEOINT -> sink.put(record.getGeoInt(col));
                case ColumnType.GEOLONG -> sink.put(record.getGeoLong(col));
                case ColumnType.DECIMAL8 -> sink.put(record.getDecimal8(col));
                case ColumnType.DECIMAL16 -> sink.put(record.getDecimal16(col));
                case ColumnType.DECIMAL32 -> sink.put(record.getDecimal32(col));
                case ColumnType.DECIMAL64 -> sink.put(record.getDecimal64(col));
                case ColumnType.DECIMAL128 -> {
                    record.getDecimal128(col, decimal128);
                    sink.put(decimal128.getHigh()).put(':').put(decimal128.getLow());
                }
                case ColumnType.DECIMAL256 -> {
                    record.getDecimal256(col, decimal256);
                    sink.put(decimal256.getHh()).put(':').put(decimal256.getHl()).put(':')
                            .put(decimal256.getLh()).put(':').put(decimal256.getLl());
                }
                case ColumnType.STRING -> {
                    sink.put(record.getStrLen(col)).put(':');
                    putNullable(sink, record.getStrA(col));
                    sink.put(':');
                    putNullable(sink, record.getStrB(col));
                }
                case ColumnType.VARCHAR -> {
                    sink.put(record.getVarcharSize(col)).put(':');
                    final Utf8Sequence a = record.getVarcharA(col);
                    putNullable(sink, a == null ? null : Utf8s.stringFromUtf8Bytes(a));
                    sink.put(':');
                    final Utf8Sequence b = record.getVarcharB(col);
                    putNullable(sink, b == null ? null : Utf8s.stringFromUtf8Bytes(b));
                    if (a != null) {
                        sink.put(":'");
                        record.getVarchar(col, sink);
                        sink.put('\'');
                    }
                }
                case ColumnType.BINARY -> {
                    sink.put(record.getBinLen(col)).put(':');
                    final BinarySequence bin = record.getBin(col);
                    if (bin == null) {
                        sink.put("<null>");
                    } else {
                        sink.put(bin.length()).put('[');
                        for (long j = 0, len = bin.length(); j < len; j++) {
                            sink.put(bin.byteAt(j)).put(' ');
                        }
                        sink.put(']');
                    }
                }
                case ColumnType.ARRAY -> {
                    ArrayTypeDriver.arrayToJson(record.getArray(col, columnType), sink, NoopArrayWriteState.INSTANCE);
                    for (int dim = 1, dims = ColumnType.decodeArrayDimensionality(columnType); dim <= dims; dim++) {
                        sink.put(':').put(record.getArrayDimLen(col, columnType, dim));
                    }
                    sink.put(':').put(record.getArrayDouble1d2d(col, columnType, 0, 0));
                }
                default -> Assert.fail("unexpected payload type in column " + col);
            }
            sink.put('|');
        }
    }

    private static long probeFirst(FrozenHashJoinBuild.IntProbe probe, int key) {
        probe.find(key);
        probe.next();
        return (long) probe.getRecord().getDouble(0);
    }

    private static void putNullable(StringSink sink, @Nullable CharSequence value) {
        if (value == null) {
            sink.put("<null>");
        } else {
            sink.put('\'').put(value).put('\'');
        }
    }

    // The row id of the row that a forward scan of the frames meets ordinal-th, counting from zero.
    private static long scanRowId(HashJoinBuildFrames frames, long ordinal) {
        for (int frameIndex = 0, n = frames.getFrameCount(); frameIndex < n; frameIndex++) {
            final long frameRows = frames.getFrameRowCount(frameIndex);
            if (ordinal < frameRows) {
                return Rows.toRowID(frameIndex, ordinal);
            }
            ordinal -= frameRows;
        }
        throw new AssertionError("no such row in the frames");
    }

    private static void shuffle(int[] values, Rnd rnd) {
        for (int i = values.length - 1; i > 0; i--) {
            final int j = rnd.nextInt(i + 1);
            final int value = values[i];
            values[i] = values[j];
            values[j] = value;
        }
    }

    // The text of a STRING or VARCHAR column of table every for the row with this key, as
    // varSizeValues() writes it: NULL, empty, a 4-byte UTF-8 character, a value long enough to leave
    // a VARCHAR's inline bytes, and a short ASCII value with the given prefix, by turns.
    @Nullable
    private static String varSizeText(int key, String prefix) {
        return switch (key % 5) {
            case 0 -> null;
            case 1 -> "";
            case 2 -> "\uD83D\uDE00" + key;
            case 3 -> "a longer text, row " + key + ", with \u00e9 and \uD83D\uDE00";
            default -> prefix + key;
        };
    }

    // The values of table every's STRING, VARCHAR, BINARY and ARRAY columns, the first or the second
    // set of them, for the row whose key the expression computes; see varSizeText() and
    // VarSizeColumns.assertRow().
    private static String varSizeValues(String key, boolean isSecondSet) {
        final String array = isSecondSet
                ? """
                CASE WHEN #K % 5 = 0 THEN NULL::DOUBLE[][] WHEN #K % 5 = 1 THEN '{}'::DOUBLE[][]
                     WHEN #K % 5 = 2 THEN ARRAY[[#K::DOUBLE]] WHEN #K % 5 = 3 THEN ARRAY[[#K::DOUBLE, 0.5], [1.5, (-#K)::DOUBLE]]
                     ELSE ARRAY[[(-#K)::DOUBLE], [#K::DOUBLE]] END"""
                : """
                CASE WHEN #K % 5 = 0 THEN NULL::DOUBLE[] WHEN #K % 5 = 1 THEN ARRAY[]::DOUBLE[]
                     WHEN #K % 5 = 2 THEN ARRAY[#K::DOUBLE] WHEN #K % 5 = 3 THEN ARRAY[#K::DOUBLE, #K / 2.0, (-#K)::DOUBLE]
                     ELSE ARRAY[(-#K)::DOUBLE, #K::DOUBLE] END""";
        return ("""
                CASE WHEN #K % 5 = 0 THEN NULL WHEN #K % 5 = 1 THEN '' WHEN #K % 5 = 2 THEN '\uD83D\uDE00' || #K
                     WHEN #K % 5 = 3 THEN 'a longer text, row ' || #K || ', with \u00e9 and \uD83D\uDE00' ELSE 's' || #K END::STRING,
                CASE WHEN #K % 5 = 0 THEN NULL WHEN #K % 5 = 1 THEN '' WHEN #K % 5 = 2 THEN '\uD83D\uDE00' || #K
                     WHEN #K % 5 = 3 THEN 'a longer text, row ' || #K || ', with \u00e9 and \uD83D\uDE00' ELSE 'v' || #K END::VARCHAR,
                CASE WHEN #K % 5 = 0 THEN NULL WHEN #K % 5 = 1 THEN from_base64('') WHEN #K % 5 = 2 THEN from_base64('8J+YgA==')
                     WHEN #K % 5 = 3 THEN rnd_bin(50, 100, 0) ELSE from_base64('AAEC') END,
                """ + array).replace("#K", key);
    }

    /**
     * The payload columns of table every that {@link #varSizeText}, {@link #varSizeValues} and the
     * A/B checks name, by payload index, and the checks of their values against what
     * varSizeValues() wrote, independent of any record that reads the table.
     */
    private static final class VarSizeColumns {
        final int arr;
        final int arr2;
        final int arr2Type;
        final int arrType;
        final int bin;
        final int bin2;
        final int long256;
        final int str;
        final int str2;
        final int sym;
        final int vc;
        final int vc2;

        VarSizeColumns(RecordMetadata metadata, IntList mapping) {
            str = payloadIndex(metadata, mapping, "str");
            vc = payloadIndex(metadata, mapping, "vc");
            bin = payloadIndex(metadata, mapping, "bin");
            arr = payloadIndex(metadata, mapping, "arr");
            str2 = payloadIndex(metadata, mapping, "str2");
            vc2 = payloadIndex(metadata, mapping, "vc2");
            bin2 = payloadIndex(metadata, mapping, "bin2");
            arr2 = payloadIndex(metadata, mapping, "arr2");
            sym = payloadIndex(metadata, mapping, "s");
            long256 = payloadIndex(metadata, mapping, "l256");
            arrType = metadata.getColumnType("arr");
            arr2Type = metadata.getColumnType("arr2");
        }

        private static void assertArray(String label, Record record, int col, int columnType, int key, boolean isTop) {
            final int dims = ColumnType.decodeArrayDimensionality(columnType);
            final ArrayView array = record.getArray(col, columnType);
            if (isTop || key % 5 == 0) {
                Assert.assertTrue(label, array.isNull());
                for (int dim = 1; dim <= dims; dim++) {
                    Assert.assertEquals(label, Numbers.INT_NULL, record.getArrayDimLen(col, columnType, dim));
                }
                Assert.assertTrue(label, Double.isNaN(record.getArrayDouble1d2d(col, columnType, 0, 0)));
                return;
            }
            Assert.assertFalse(label, array.isNull());
            Assert.assertEquals(label, dims, array.getDimCount());
            // The shape and first element of each value, by key, as varSizeValues() writes them.
            final int[] shape;
            final double first;
            switch (key % 5) {
                case 1 -> {
                    shape = dims == 1 ? new int[]{0} : new int[]{0, 0};
                    first = Double.NaN;
                }
                case 2 -> {
                    shape = dims == 1 ? new int[]{1} : new int[]{1, 1};
                    first = key;
                }
                case 3 -> {
                    shape = dims == 1 ? new int[]{3} : new int[]{2, 2};
                    first = key;
                }
                default -> {
                    shape = dims == 1 ? new int[]{2} : new int[]{2, 1};
                    first = -key;
                }
            }
            for (int dim = 1; dim <= dims; dim++) {
                Assert.assertEquals(label + ", dim " + dim, shape[dim - 1], record.getArrayDimLen(col, columnType, dim));
            }
            Assert.assertEquals(label, first, record.getArrayDouble1d2d(col, columnType, 0, 0), 0);
        }

        private static void assertBinary(String label, Record record, int col, int key, boolean isTop) {
            final BinarySequence bin = record.getBin(col);
            if (isTop || key % 5 == 0) {
                Assert.assertNull(label, bin);
                Assert.assertEquals(label, -1, record.getBinLen(col));
                return;
            }
            Assert.assertNotNull(label, bin);
            Assert.assertEquals(label, bin.length(), record.getBinLen(col));
            final byte[] expected = switch (key % 5) {
                case 1 -> new byte[0];
                // The UTF-8 bytes of U+1F600.
                case 2 -> new byte[]{(byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80};
                case 4 -> new byte[]{0, 1, 2};
                default -> null;
            };
            if (expected == null) {
                // rnd_bin(50, 100, 0)
                Assert.assertTrue(label, bin.length() >= 50 && bin.length() <= 100);
                return;
            }
            Assert.assertEquals(label, expected.length, bin.length());
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals(label, expected[i], bin.byteAt(i));
            }
        }

        private static void assertString(String label, Record record, int col, @Nullable String expected) {
            final CharSequence a = record.getStrA(col);
            final CharSequence b = record.getStrB(col);
            if (expected == null) {
                Assert.assertNull(label, a);
                Assert.assertNull(label, b);
                Assert.assertEquals(label, -1, record.getStrLen(col));
                return;
            }
            Assert.assertNotSame(label, a, b);
            TestUtils.assertEquals(label, expected, a);
            TestUtils.assertEquals(label, expected, b);
            // UTF-16 chars: U+1F600 takes two.
            Assert.assertEquals(label, expected.length(), record.getStrLen(col));
        }

        private static void assertVarchar(String label, Record record, int col, @Nullable String expected) {
            final Utf8Sequence a = record.getVarcharA(col);
            final Utf8Sequence b = record.getVarcharB(col);
            if (expected == null) {
                Assert.assertNull(label, a);
                Assert.assertNull(label, b);
                Assert.assertEquals(label, -1, record.getVarcharSize(col));
                return;
            }
            Assert.assertNotNull(label, a);
            Assert.assertNotSame(label, a, b);
            Assert.assertEquals(label, expected, Utf8s.stringFromUtf8Bytes(a));
            Assert.assertEquals(label, expected, Utf8s.stringFromUtf8Bytes(b));
            // UTF-8 bytes: U+1F600 takes four, and U+00E9 two.
            Assert.assertEquals(label, expected.getBytes(StandardCharsets.UTF_8).length, record.getVarcharSize(col));
            final StringSink sink = new StringSink();
            record.getVarchar(col, sink);
            TestUtils.assertEquals(label, expected, sink);
        }

        // The payload index of the build scan's column with this name.
        private static int payloadIndex(RecordMetadata metadata, IntList mapping, String name) {
            final int index = mapping.indexOf(metadata.getColumnIndex(name), 0, mapping.size());
            Assert.assertTrue(name, index >= 0);
            return index;
        }

        // Checks the var-size payload columns of the row with this key, which a reader is positioned at.
        void assertRow(String label, Record record, int key) {
            // The second set of columns came after the first two days, keys 1 to 24.
            final boolean isTop = key <= 24;
            assertString(label + ", str", record, str, varSizeText(key, "s"));
            assertVarchar(label + ", vc", record, vc, varSizeText(key, "v"));
            assertBinary(label + ", bin", record, bin, key, false);
            assertArray(label + ", arr", record, arr, arrType, key, false);
            assertString(label + ", str2", record, str2, isTop ? null : varSizeText(key, "s"));
            assertVarchar(label + ", vc2", record, vc2, isTop ? null : varSizeText(key, "v"));
            assertBinary(label + ", bin2", record, bin2, key, isTop);
            assertArray(label + ", arr2", record, arr2, arr2Type, key, isTop);
        }
    }
}
