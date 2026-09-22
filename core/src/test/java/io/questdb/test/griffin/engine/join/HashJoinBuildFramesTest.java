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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The build's payload source over its input's page frames: probes read payload columns at the
 * row ids the build keeps, each through a reader of its own.
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

    private static IntList ints(int... values) {
        IntList list = new IntList();
        for (int value : values) {
            list.add(value);
        }
        return list;
    }

    private static long probeFirst(FrozenHashJoinBuild.IntProbe probe, int key) {
        probe.find(key);
        probe.next();
        return (long) probe.getRecord().getDouble(0);
    }
}
