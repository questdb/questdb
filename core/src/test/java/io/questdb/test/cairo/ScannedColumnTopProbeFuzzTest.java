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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.ScannedColumnTopProbe;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Randomised cross-check of {@link ScannedColumnTopProbe} against a slow, obvious simulation of
 * the query it is standing in for.
 * <p>
 * Each round builds a {@code _txn} and a {@code _cv} directly, so it can reach states a sequence of
 * SQL statements would take a long time to stumble into: a random number of partitions across a
 * 30-day window, some of them split to start in the middle of a day, random column tops on a random
 * subset, and a random add time for the column.
 * <p>
 * The simulation walks interval by interval, finds the partitions each one overlaps, and decides
 * every partition from its own {@code _cv} record or, where it has none, from the column's add
 * time. The two answers must agree exactly, so both directions fail the round.
 * <p>
 * They fail for different reasons. Answering false where the simulation says true sends a NULL key
 * down a covering scan that holds nothing for those rows, which is wrong rows, silently. Answering
 * true where the simulation says false runs the backup plan for nothing, which is a slower query on
 * a shape that did not need it.
 * <p>
 * The simulation is not fully independent: it reproduces {@code partitionEndTimestamp}, so a
 * mistake in that one expression would appear in both and this test could not see it.
 */
public class ScannedColumnTopProbeFuzzTest extends AbstractCairoTest {

    private static final int COLUMN_COUNT = 4;
    private static final long DAY = Micros.DAY_MICROS;
    private static final int PROBED_COLUMN = 2;
    private static final int ROUNDS = 400;
    private static final long START = 0;
    private static final int WINDOW_DAYS = 30;

    @Test
    public void testProbeMatchesTheSimulatedQuery() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            int scannedTops = 0;
            for (int round = 0; round < ROUNDS; round++) {
                final LongList partitions = randomPartitions(rnd);
                final LongList tops = randomTops(rnd, partitions);
                final long addedAt = randomAddedAt(rnd, partitions);
                final LongList intervals = randomIntervals(rnd);

                final String tableName = "t_probe_fuzz_" + round;
                final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);
                final TableToken tableToken = engine.verifyTableName(tableName);
                try (Path path = new Path()) {
                    final FilesFacade ff = configuration.getFilesFacade();
                    writeTxn(ff, path, tableToken, partitions);
                    writeCv(path, tableToken, tops, addedAt);
                    try (
                            TxReader tx = new TxReader(ff);
                            ColumnVersionReader cv = new ColumnVersionReader().ofRO(
                                    ff, path.of(configuration.getDbRoot()).concat(tableToken).concat("_cv").$())
                    ) {
                        tx.ofRO(path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$(),
                                ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                        tx.unsafeLoadAll();
                        cv.readUnsafe();

                        final boolean probed = ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, intervals);
                        final boolean simulated = simulateQuery(tx, cv, addedAt, intervals);

                        if (simulated) {
                            scannedTops++;
                        }
                        Assert.assertEquals(
                                (simulated ? "under-reported, which returns wrong rows" : "over-reported")
                                        + describe(partitions, tops, addedAt, intervals),
                                simulated,
                                probed
                        );
                    }
                }
            }
            LOG.info().$("probe fuzz [rounds=").$(ROUNDS).$(", scannedTops=").$(scannedTops).I$();
            Assert.assertTrue(
                    "the generator never produced a round the probe answers true on; it is not"
                            + " reaching the shapes this test exists for",
                    scannedTops > 0
            );
        });
    }

    private static String describe(LongList partitions, LongList tops, long addedAt, LongList intervals) {
        return " [partitions=" + partitions + ", tops=" + tops + ", addedAt=" + addedAt
                + ", intervals=" + intervals + "]";
    }

    /**
     * Column top for {@code partitionTimestamp}, or -1 when it owns no record. {@code tops} holds
     * flat (partitionTimestamp, columnTop) pairs.
     */
    private static long lookupTop(LongList tops, long partitionTimestamp) {
        for (int i = 0, n = tops.size() / 2; i < n; i++) {
            if (tops.getQuick(2 * i) == partitionTimestamp) {
                return tops.getQuick(2 * i + 1);
            }
        }
        return -1;
    }

    /**
     * The column's add time: before every partition, on one of them, or after all of them. The
     * middle case is the interesting one, and is drawn most often.
     */
    private static long randomAddedAt(Rnd rnd, LongList partitions) {
        final int roll = rnd.nextInt(10);
        if (roll == 0) {
            return ColumnVersionReader.COL_TOP_DEFAULT_PARTITION;
        }
        if (roll == 1) {
            return START + (WINDOW_DAYS + 2L) * DAY;
        }
        return partitions.getQuick(rnd.nextInt(partitions.size()));
    }

    /**
     * Ascending, disjoint, closed (lo, hi) pairs over a window wider than the data, so intervals
     * that start before the first partition and end after the last one both occur. Sometimes null
     * (whole table) and sometimes empty (admits nothing).
     */
    private static LongList randomIntervals(Rnd rnd) {
        final int roll = rnd.nextInt(12);
        if (roll == 0) {
            return null;
        }
        final LongList intervals = new LongList();
        if (roll == 1) {
            return intervals;
        }
        long cursor = START - 2 * DAY + rnd.nextLong(DAY);
        final int count = 1 + rnd.nextInt(4);
        for (int i = 0; i < count; i++) {
            final long lo = cursor + rnd.nextLong(3 * DAY);
            final long hi = lo + rnd.nextLong(3 * DAY);
            intervals.add(lo);
            intervals.add(hi);
            cursor = hi + 1 + rnd.nextLong(DAY);
        }
        return intervals;
    }

    /**
     * A random number of partitions inside a 30-day window, ascending and unique. Some days are
     * split, i.e. carry a second partition starting in the middle of the day, so partition
     * timestamps are not all day floors.
     */
    private static LongList randomPartitions(Rnd rnd) {
        final LongList partitions = new LongList();
        for (int day = 0; day < WINDOW_DAYS; day++) {
            if (rnd.nextInt(3) == 0) {
                continue; // a gap: no partition for this day at all
            }
            final long floor = START + day * DAY;
            partitions.add(floor);
            if (rnd.nextInt(4) == 0) {
                // split: a second partition starting inside the same day
                partitions.add(floor + 1 + rnd.nextLong(DAY - 2));
            }
        }
        if (partitions.size() == 0) {
            partitions.add(START);
        }
        return partitions;
    }

    /**
     * Flat (partitionTimestamp, columnTop) pairs for a random subset of partitions. Zero tops are
     * drawn deliberately often: a zero-top record on a partition that came before the add is the
     * shape the probe over-reports on.
     */
    private static LongList randomTops(Rnd rnd, LongList partitions) {
        final LongList tops = new LongList();
        for (int i = 0, n = partitions.size(); i < n; i++) {
            final int roll = rnd.nextInt(3);
            if (roll == 0) {
                continue; // no record at all
            }
            tops.add(partitions.getQuick(i));
            tops.add(roll == 1 ? 0 : 1 + rnd.nextLong(1000));
        }
        return tops;
    }

    /**
     * The slow, obvious answer: walk interval by interval, take every partition the interval
     * overlaps, and decide it from its record or from the add time. Deliberately written the long
     * way round rather than sharing anything with the code under test.
     */
    private static boolean simulateQuery(TxReader tx, ColumnVersionReader cv, long addedAt, LongList intervals) {
        final int partitionCount = tx.getPartitionCount();
        final int intervalCount = intervals == null ? 1 : intervals.size() / 2;
        for (int i = 0; i < intervalCount; i++) {
            final long lo = intervals == null ? Long.MIN_VALUE : intervals.getQuick(2 * i);
            final long hi = intervals == null ? Long.MAX_VALUE : intervals.getQuick(2 * i + 1);
            for (int p = 0; p < partitionCount; p++) {
                final long partitionLo = tx.getPartitionTimestampByIndex(p);
                final long partitionHi = p + 1 < partitionCount
                        ? Math.min(tx.getPartitionTimestampByIndex(p + 1), tx.getNextLogicalPartitionTimestamp(partitionLo)) - 1
                        : tx.getNextLogicalPartitionTimestamp(partitionLo);
                if (lo > partitionHi || hi < partitionLo) {
                    continue; // this interval does not reach this partition
                }
                final int record = cv.getRecordIndex(partitionLo, PROBED_COLUMN);
                if (record > -1) {
                    if (cv.getColumnTopByIndex(record) > 0) {
                        return true;
                    }
                } else if (addedAt != ColumnVersionReader.COL_TOP_DEFAULT_PARTITION
                        && addedAt > partitionLo
                        && tx.getPartitionSize(p) > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void writeCv(Path path, TableToken tableToken, LongList tops, long addedAt) {
        try (ColumnVersionWriter w = new ColumnVersionWriter(
                configuration, path.of(configuration.getDbRoot()).concat(tableToken).concat("_cv").$(), true)) {
            for (int c = 0; c < COLUMN_COUNT; c++) {
                w.upsertDefaultTxnName(c, 1, c == PROBED_COLUMN ? addedAt : START);
            }
            for (int i = 0, n = tops.size() / 2; i < n; i++) {
                // Neighbouring columns share the partition timestamps, so the probed column's
                // records are never the only ones at a given timestamp.
                w.upsert(tops.getQuick(2 * i), PROBED_COLUMN - 1, 1, 0);
                w.upsert(tops.getQuick(2 * i), PROBED_COLUMN, 1, tops.getQuick(2 * i + 1));
                w.upsert(tops.getQuick(2 * i), PROBED_COLUMN + 1, 1, 0);
            }
            w.commit();
        }
    }

    private static void writeTxn(FilesFacade ff, Path path, TableToken tableToken, LongList partitions) {
        try (TxWriter tw = new TxWriter(ff, configuration).ofRW(
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$(),
                ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY)) {
            for (int i = 0, n = partitions.size(); i < n; i++) {
                tw.updatePartitionSizeByTimestamp(partitions.getQuick(i), 1 + i);
            }
            tw.updateMaxTimestamp(partitions.getQuick(partitions.size() - 1) + 1);
            tw.finishPartitionSizeUpdate();
            tw.commit(new ObjList<>());
        }
    }
}
