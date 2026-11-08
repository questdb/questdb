/*******************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzConvertPartitionToParquetOperation;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Generator-coverage pin for the replace-over-Parquet crash fuzzing (CLAUDE.md #11).
 * <p>
 * {@link RandomizedAdaptiveCrashFuzzTest#configureFuzz()} keeps {@code replaceInsert = 0.0} in the shared adaptive
 * crash sweep, so that sweep never draws a replace-range commit over a converted Parquet partition. This pin drives
 * the SAME full-library config through the extracted statics
 * ({@link RandomizedAdaptiveCrashFuzzTest#setFullLibraryFuzzProbabilities} /
 * {@link RandomizedAdaptiveCrashFuzzTest#setFullLibrarySweepFuzzCounts}) with replace enabled at 0.1 and asserts the
 * generator actually EMITS the shape: a {@link FuzzConvertPartitionToParquetOperation} that converts a day, followed
 * by a later transaction whose replace range overlaps that day. The convert's filter {@code ts > C - 1 day AND ts < C}
 * selects partitions by their floor timestamp, so on the fuzz table's DAY partitions it converts the day
 * {@code [floorDay(C), floorDay(C) + 1 day)} that contains the cutoff {@code C}: none when {@code C} is midnight, and
 * none when no committed row reached that day before the convert, as the table starts empty.
 * <p>
 * It fails first at {@code replaceInsert = 0.0} (no transaction carries a replace range, so the count is exactly 0)
 * and passes at 0.1 (the generator draws the shape). If a future change stops the generator emitting a replace range
 * over a day it converts, the count drops to 0 at 0.1 and this pin goes red. The pin reads the workloads, not a
 * table: an operation that empties the day before the convert, or drops it or converts it back before the replace
 * (TRUNCATE, DROP PARTITION, CONVERT TO NATIVE, a replace, a TTL), still counts.
 */
public class ReplaceOverParquetShapeCoverageTest extends AbstractCairoTest {

    // Fixed seed: a generator-coverage pin must be reproducible, so a run that finds no shape reproduces.
    private static final long SEED0 = 0x5f3759dfL;
    private static final long SEED1 = 0x9e3779b9L;
    private static final String SHAPE_TABLE = "rop_shapes";
    private static final int WORKLOADS = 1500;

    private final FuzzRunner fuzzer = new FuzzRunner();

    @Before
    public void setUpFuzzer() {
        fuzzer.withDb(engine, sqlExecutionContext);
        fuzzer.clearSeeds();
    }

    @After
    public void tearDownFuzzer() {
        fuzzer.after();
    }

    @Test
    public void testConvertTurnsTheDayContainingTheCutoffIntoParquet() throws Exception {
        // The engine shows which day hasReplaceOverParquetShape must treat as converted. Each expectation follows from
        // the convert's filter ts > C - 1 day AND ts < C evaluated on the DAY partitions' floor timestamps: the day
        // that contains C, none when C is midnight, and none when that day has no partition.
        assertMemoryLeak(() -> {
            final String day26IsParquet = """
                    name\tisParquet
                    2022-02-24\tfalse
                    2022-02-25\tfalse
                    2022-02-26\ttrue
                    """;
            final String day25IsParquet = """
                    name\tisParquet
                    2022-02-24\tfalse
                    2022-02-25\ttrue
                    2022-02-26\tfalse
                    """;
            final String noDayIsParquet = """
                    name\tisParquet
                    2022-02-24\tfalse
                    2022-02-25\tfalse
                    2022-02-26\tfalse
                    """;
            int tableIndex = 0;
            for (TestTimestampType timestampType : TestTimestampType.values()) {
                final String typeName = timestampType.getTypeName();
                final TimestampDriver driver = timestampType.getDriver();
                final long day26 = driver.parseFloorLiteral("2022-02-26");
                final long hour = driver.fromHours(1);
                // mid-day
                assertConvertedDays(typeName, "cv" + tableIndex++, day26 + 13 * hour, day26IsParquet);
                // one unit after midnight
                assertConvertedDays(typeName, "cv" + tableIndex++, day26 + 1, day26IsParquet);
                // one unit before midnight
                assertConvertedDays(typeName, "cv" + tableIndex++, day26 - 1, day25IsParquet);
                // midnight
                assertConvertedDays(typeName, "cv" + tableIndex++, day26, noDayIsParquet);
                // 2022-02-28T01:00, a day without rows
                assertConvertedDays(typeName, "cv" + tableIndex++, day26 + driver.fromDays(2) + hour, noDayIsParquet);
            }
        });
    }

    @Test
    public void testGeneratorEmitsReplaceOverParquetShapes() throws Exception {
        assertMemoryLeak(() -> {
            // Fail-first: with replace disabled (exactly configureFuzz()'s shared setting) NO workload carries a
            // replace range, so the generator cannot emit a single replace-over-Parquet shape.
            final int disabled = countReplaceOverParquetShapes(0.0);
            Assert.assertEquals("replace disabled (0.0) must emit zero replace-over-Parquet shapes", 0, disabled);

            // With replace enabled at 0.1 the generator draws replace-over-Parquet shapes. The test asserts presence,
            // not a count: the count is the generator's own output for the fixed seed and moves with any change to its
            // draws, while presence is what lets a sweep with replace enabled fuzz the shape. Every seed tried gave 66
            // or more shapes of WORKLOADS, so the count reaches 0 only when the shape becomes rare or unreachable.
            final int enabled = countReplaceOverParquetShapes(0.1);
            LOG.info().$("[rop-shapes] replace-over-parquet shapes at 0.1: ").$(enabled).$(" of ").$(WORKLOADS).$();
            Assert.assertTrue("replace enabled (0.1) must emit replace-over-Parquet shapes, got " + enabled, enabled > 0);
        });
    }

    @Test
    public void testShapeCountsOnlyAReplaceOverTheConvertedDay() throws Exception {
        // A convert with cutoff c turns the day [f, f + day) that contains c into Parquet when a committed row reached
        // that day before the convert (testConvertTurnsTheDayContainingTheCutoffIntoParquet).
        final long day = Micros.DAY_MICROS;
        final long hour = Micros.HOUR_MICROS;
        final long f = MicrosTimestampDriver.floor("2022-02-26");
        final long c = f + 13 * hour;

        assertShape("the converted day after the cutoff", true, insert(f), convert(c), replace(c, f + day));
        assertShape("the day before the converted day", false, insert(f), convert(c), replace(f - hour, f));
        assertShape("the converted day's first unit", true, insert(f), convert(c), replace(f - hour, f + 1));
        assertShape(
                "the converted day's last unit",
                true,
                insert(f), convert(c), replace(f + day - 1, f + day + hour)
        );
        assertShape("the next day", false, insert(f), convert(c), replace(f + day, f + day + hour));
        assertShape("a midnight cutoff converts nothing", false, insert(f), convert(f), replace(f - day, f + day));
        assertShape("the only row comes after the convert", false, convert(c), insert(f), replace(f, f + day));
        assertShape(
                "a rolled-back row creates no partition",
                false,
                rolledBack(insert(f)), convert(c), replace(f, f + day)
        );
        assertShape(
                "rows one unit outside the converted day",
                false,
                insert(f - 1), insert(f + day), convert(c), replace(f, f + day)
        );
        assertShape(
                "a row in the converted day's last unit",
                true,
                insert(f + day - 1), convert(c), replace(f, f + day)
        );
        assertShape("a replace before the convert", false, insert(f), replace(f, f + day), convert(c));
        assertShape(
                "a convert after a midnight convert",
                true,
                insert(f), convert(f), convert(c), replace(f, f + day)
        );
        assertShape(
                "a convert after a convert of a day without rows",
                true,
                insert(f), convert(f + day + 13 * hour), convert(c), replace(f, f + day)
        );
        assertShape(
                "a replace after a replace over the next day",
                true,
                insert(f), convert(c), replace(f + day, f + day + hour), replace(f, f + day)
        );
        assertShape(
                "a replace after a transaction without a replace",
                true,
                insert(f), convert(c), insert(f + day), replace(f, f + day)
        );
    }

    private void assertConvertedDays(
            String timestampTypeName,
            String tableName,
            long cutoff,
            String expectedPartitions
    ) throws Exception {
        execute("CREATE TABLE " + tableName + " (ts " + timestampTypeName + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // a row every 6 hours from 2022-02-24T00:00 to 2022-02-26T18:00
        execute(
                "INSERT INTO " + tableName + " SELECT dateadd('h', 6 * (x::int - 1), '2022-02-24T00:00:00.000000Z'::"
                        + timestampTypeName + ") ts, x v FROM long_sequence(12)"
        );
        drainWalQueue();
        final TableToken token = engine.verifyTableName(tableName);
        try (WalWriter walWriter = engine.getWalWriter(token)) {
            // as FuzzRunner.applyToWal applies the operation
            final FuzzConvertPartitionToParquetOperation convert = new FuzzConvertPartitionToParquetOperation(cutoff);
            Assert.assertTrue(convert.apply(new Rnd(), engine, walWriter, -1, null));
        }
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        assertQuery("SELECT name, isParquet FROM table_partitions('" + tableName + "')")
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .returns(expectedPartitions);
    }

    // Generates WORKLOADS full-library workloads with replaceInsert at replaceInsertProb and counts those that carry a
    // convert-to-Parquet followed by a later replace range over the day it converts. Deterministic: one master Rnd on
    // the fixed seed seeds every workload's generation.
    private int countReplaceOverParquetShapes(double replaceInsertProb) throws SqlException {
        fuzzer.clearSeeds();
        RandomizedAdaptiveCrashFuzzTest.setFullLibraryFuzzProbabilities(fuzzer, replaceInsertProb);
        RandomizedAdaptiveCrashFuzzTest.setFullLibrarySweepFuzzCounts(fuzzer);

        execute("drop table if exists " + SHAPE_TABLE);
        fuzzer.createInitialTableWal(SHAPE_TABLE, 0);
        drainWalQueue();

        final TableToken token = engine.verifyTableName(SHAPE_TABLE);
        final long oneDay;
        try (TableMetadata md = engine.getTableMetadata(token)) {
            Assert.assertEquals(
                    "the converted-day predicate reads DAY partitions",
                    PartitionBy.DAY,
                    md.getPartitionBy()
            );
            oneDay = ColumnType.getTimestampDriver(md.getTimestampType()).fromDays(1);
        }

        final Rnd master = new Rnd(SEED0, SEED1);
        int shapes = 0;
        for (int i = 0; i < WORKLOADS; i++) {
            final ObjList<FuzzTransaction> txns = fuzzer.generateTransactions(SHAPE_TABLE, new Rnd(master.nextLong(), master.nextLong()));
            try {
                if (hasReplaceOverParquetShape(txns, oneDay)) {
                    shapes++;
                }
            } finally {
                Misc.freeObjListAndClear(txns);
            }
        }
        return shapes;
    }

    private static void assertShape(String message, boolean expected, FuzzTransaction... transactions) {
        final ObjList<FuzzTransaction> txns = new ObjList<>();
        try {
            for (FuzzTransaction txn : transactions) {
                txns.add(txn);
            }
            Assert.assertEquals(message, expected, hasReplaceOverParquetShape(txns, Micros.DAY_MICROS));
        } finally {
            Misc.freeObjListAndClear(txns);
        }
    }

    private static FuzzTransaction convert(long cutoff) {
        final FuzzTransaction txn = new FuzzTransaction();
        txn.operationList.add(new FuzzConvertPartitionToParquetOperation(cutoff));
        return txn;
    }

    // The day txns[c] converts to Parquet, or Long.MIN_VALUE if it converts none. The convert's filter
    // ts > C - 1 day AND ts < C (FuzzTransactionOperation.partitionDayBeforeFilter) selects the partitions whose floor
    // timestamp lies in (C - 1 day, C) (SqlCompilerImpl.filterPartitions): on DAY partitions, the day that contains C,
    // none when C is midnight. The table starts empty, so that day has a partition to convert only if an earlier
    // committed transaction inserted a row into it.
    private static long convertedDay(ObjList<FuzzTransaction> txns, int c, long oneDay) {
        final FuzzTransaction txn = txns.getQuick(c);
        for (int i = 0, n = txn.operationList.size(); i < n; i++) {
            if (txn.operationList.getQuick(i) instanceof FuzzConvertPartitionToParquetOperation convert) {
                final long cutoff = convert.getCutoffTimestamp();
                final long day = cutoff - Math.floorMod(cutoff, oneDay);
                return day != cutoff && hasCommittedRow(txns, c, day, day + oneDay) ? day : Long.MIN_VALUE;
            }
        }
        return Long.MIN_VALUE;
    }

    // True if a transaction before txns[end] that is not rolled back inserts a row with a timestamp in [lo, hi).
    private static boolean hasCommittedRow(ObjList<FuzzTransaction> txns, int end, long lo, long hi) {
        for (int t = 0; t < end; t++) {
            final FuzzTransaction txn = txns.getQuick(t);
            if (!txn.rollback) {
                for (int i = 0, n = txn.operationList.size(); i < n; i++) {
                    if (txn.operationList.getQuick(i) instanceof FuzzInsertOperation insert
                            && insert.getTimestamp() >= lo && insert.getTimestamp() < hi) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // True if a transaction commits a replace range [lo, hi) overlapping a day an earlier transaction converts to
    // Parquet: lo < day + oneDay and hi > day.
    private static boolean hasReplaceOverParquetShape(ObjList<FuzzTransaction> txns, long oneDay) {
        for (int c = 0, n = txns.size(); c < n; c++) {
            final long day = convertedDay(txns, c, oneDay);
            if (day != Long.MIN_VALUE) {
                for (int t = c + 1; t < n; t++) {
                    final FuzzTransaction txn = txns.getQuick(t);
                    if (txn.hasReplaceRange() && txn.getReplaceLoTs() < day + oneDay && txn.getReplaceHiTs() > day) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static FuzzTransaction insert(long timestamp) {
        final FuzzTransaction txn = new FuzzTransaction();
        txn.operationList.add(new FuzzInsertOperation(0, 0, timestamp, 0, 0, 0, 0, null));
        return txn;
    }

    private static FuzzTransaction replace(long lo, long hi) {
        final FuzzTransaction txn = new FuzzTransaction();
        txn.setReplaceRange(lo, hi);
        return txn;
    }

    private static FuzzTransaction rolledBack(FuzzTransaction txn) {
        txn.rollback = true;
        return txn;
    }
}
