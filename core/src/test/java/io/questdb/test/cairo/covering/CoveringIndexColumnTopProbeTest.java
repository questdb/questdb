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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ScannedColumnTopProbe;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code CoveringIndexRecordCursorFactory.hasAnyColumnTop()} answers, per open of a NULL key,
 * whether the covering plan has to step aside for its backup. It reads {@code _cv} and the
 * reader's partition list, and opens no partition.
 * <p>
 * The probe is driven by {@code _cv}, not by the partition list: it visits this column's records
 * that carry a non-zero top and asks whether the scan reads that partition, then settles the
 * partitions that came before the column from the column's add time alone. This class pins its
 * answer against a per-partition {@link ColumnVersionReader#getRecordIndex} walk, over the shapes
 * that decide it -- a partition carrying a zero top record in front of one that carries a real
 * top, a partition wholly predating the column, a dropped partition whose records
 * {@code ColumnVersionWriter.removePartition} erased outright, a split partition whose timestamp
 * is not a day floor, Parquet partitions, a non-partitioned table whose single partition sits at
 * timestamp 0 just after the pseudo-partition records, and a partition that owns records for
 * columns on BOTH sides of the probed one.
 * <p>
 * The two answers agree everywhere, including the shape that most easily breaks that: a partition
 * that came before the column and was later back-filled by an O3 write owns a zero-top record, so
 * the column is there in full. {@link #testDroppedPartitionMatchesSearchWalk} and
 * {@link #testRecordsEitherSideOfProbedColumnMatchSearchWalk} build it, and the probe has to read
 * that record rather than settle the answer from the column's add time alone.
 * <p>
 * Under-reporting is the worse direction -- a false negative sends a NULL key down the covering
 * plan over a partition the posting chain holds nothing for, which is wrong rows, not a slow
 * query -- but the probe is exact, so both directions fail here.
 * <p>
 * Every case states the decision it expects as {@code BRANCH@partitionIndex} (see
 * {@link #searchWalkDecision}), so a fixture that drifts into short-circuiting somewhere else --
 * or into answering before it reaches the partition the case is named for -- turns red instead of
 * quietly testing a shape it no longer builds.
 */
public class CoveringIndexColumnTopProbeTest extends AbstractCairoTest {

    private static final int WIDE_PARTITION_COUNT = 128;

    @Test
    public void testDroppedPartitionMatchesSearchWalk() throws Exception {
        // DROP PARTITION erases the dropped timestamp's _cv records outright
        // (ColumnVersionWriter.removePartition) and pulls the default record's added-at
        // timestamp back to the new last partition, so the walk has to answer from what is left:
        // the partition that carried the only real top is gone, and the two zero-top records in
        // front of it now carry the walk all the way to the end.
        assertMemoryLeak(() -> {
            createColumnTopTable("t_probe_dropped");
            assertProbeMatchesSearchWalk("t_probe_dropped", "TOP_RECORD@2");

            execute("ALTER TABLE t_probe_dropped DROP PARTITION LIST '2024-01-03'");
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_dropped", "NO_TOP@2");
        });
    }

    @Test
    public void testFlatTableMatchesSearchWalkOnWalAndNonWal() throws Exception {
        // The control, on both commit paths: sym has existed since CREATE TABLE and no partition
        // carries a record for it, so the walk falls off the end of both lists.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe_flat_nowal (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_flat_nowal VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-02T00:00:00', 20.0, 'A'),
                    ('2024-01-03T00:00:00', 30.0, NULL)
                    """);
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_flat_nowal", "NO_TOP@3");

            execute("CREATE TABLE t_probe_flat_wal (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t_probe_flat_wal VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-02T00:00:00', 20.0, 'A'),
                    ('2024-01-03T00:00:00', 30.0, NULL)
                    """);
            drainWalQueue();
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_flat_wal", "NO_TOP@3");
        });
    }

    @Test
    public void testNonPartitionedTableMatchesSearchWalk() throws Exception {
        // A non-partitioned table has exactly one partition, and it sits at timestamp 0 -- above
        // the pseudo-partition records _cv keeps at COL_TOP_DEFAULT_PARTITION (Long.MIN_VALUE)
        // and SYMBOL_TABLE_VERSION_PARTITION, which the merge has to step over before it reads
        // anything real.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe_none (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_none VALUES
                    ('2024-01-01T00:00:00', 10.0),
                    ('2024-01-01T01:00:00', 20.0)
                    """);
            execute("ALTER TABLE t_probe_none ADD COLUMN sym SYMBOL");
            execute("INSERT INTO t_probe_none VALUES ('2024-01-01T02:00:00', 30.0, 'A')");
            execute("ALTER TABLE t_probe_none ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            releaseAll();

            try (TableReader reader = engine.getReader("t_probe_none")) {
                Assert.assertEquals(1, reader.getPartitionCount());
                Assert.assertEquals(0L, reader.getPartitionTimestampByIndex(0));
                final int wi = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
                final LongList scan = new LongList();
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                scan.add(1L, 2L);
                scan.add(Long.MAX_VALUE - 1, Long.MAX_VALUE);
                Assert.assertTrue(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
            }
            assertProbeMatchesSearchWalk("t_probe_none", "TOP_RECORD@0");
        });
    }

    @Test
    public void testParquetPartitionMatchesSearchWalk() throws Exception {
        // The probe reads _cv and the transaction file only, so a Parquet partition must answer
        // exactly as its native twin did. Both branches have to meet one: the record branch walks
        // past a converted partition and then decides on a second converted one, and the predates
        // branch reads a converted partition's row count out of the transaction file.
        assertMemoryLeak(() -> {
            createColumnTopTable("t_probe_parquet");
            assertProbeMatchesSearchWalk("t_probe_parquet", "TOP_RECORD@2");

            // CONVERT PARTITION returns success without converting the ACTIVE partition of a
            // non-WAL table (TableWriter.convertPartitionNativeToParquet), and 2024-01-03 -- the
            // partition whose record decides this answer -- is the active one. Append a later
            // partition first, so the deciding partition really does convert; the appended
            // partition sits past the record the walk returns on and cannot move the answer.
            execute("INSERT INTO t_probe_parquet VALUES ('2024-01-04T12:00:00', 40.0, 'A')");
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_parquet", "TOP_RECORD@2");

            execute("ALTER TABLE t_probe_parquet CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'");
            releaseAll();
            assertPartitionFormats(
                    "t_probe_parquet",
                    PartitionFormat.PARQUET,
                    PartitionFormat.NATIVE,
                    PartitionFormat.PARQUET,
                    PartitionFormat.NATIVE
            );
            assertProbeMatchesSearchWalk("t_probe_parquet", "TOP_RECORD@2");

            createPredatingPartitionTable("t_probe_parquet_pre");
            assertProbeMatchesSearchWalk("t_probe_parquet_pre", "PREDATES@0");

            execute("ALTER TABLE t_probe_parquet_pre CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            releaseAll();
            assertPartitionFormats(
                    "t_probe_parquet_pre",
                    PartitionFormat.PARQUET,
                    PartitionFormat.NATIVE,
                    PartitionFormat.NATIVE
            );
            assertProbeMatchesSearchWalk("t_probe_parquet_pre", "PREDATES@0");
        });
    }

    @Test
    public void testPartitionPredatingColumnMatchesSearchWalk() throws Exception {
        // ADD COLUMN writes the default record and puts an explicit top on the LAST partition
        // (TableWriter.openNewColumnFiles), so the earlier partitions get no record at all. They
        // hold no value for any of their rows, which the walk has to read off the default
        // record's added-at timestamp plus the partition's row count.
        assertMemoryLeak(() -> {
            createPredatingPartitionTable("t_probe_predates");
            assertProbeMatchesSearchWalk("t_probe_predates", "PREDATES@0");
        });
    }

    @Test
    public void testProbeAnswerAndPlanSurviveEveryKeyShape() throws Exception {
        // The probe is the same walk whatever drives the open, so a table with no top has to keep
        // the covering plan for the single key, the IN-list, LATEST ON (which iterates the
        // partitions backwards) and an interval-filtered scan alike. force_use_covering makes the
        // answer observable: it suppresses the backup, then throws on any open where the probe
        // says true. The plan assertion pins that each shape really is served by the covering
        // factory -- with the backup suppressed, "backup: true" could never appear anyway.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe_shapes (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_shapes VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-01T01:00:00', 20.0, 'A'),
                    ('2024-01-02T00:00:00', 30.0, NULL),
                    ('2024-01-03T00:00:00', 40.0, 'A')
                    """);
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_shapes", "NO_TOP@3");

            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_probe_shapes WHERE sym = null")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-02T00:00:00.000000Z\t\t30.0
                            """);

            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_probe_shapes WHERE sym IN (null, 'A')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\tA\t20.0
                            2024-01-02T00:00:00.000000Z\t\t30.0
                            2024-01-03T00:00:00.000000Z\tA\t40.0
                            """);

            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_probe_shapes"
                    + " WHERE sym = null AND ts IN '2024-01-02'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-02T00:00:00.000000Z\t\t30.0
                            """);

            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_probe_shapes"
                    + " WHERE sym = null LATEST ON ts PARTITION BY sym")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-02T00:00:00.000000Z\t\t30.0
                            """);
        });
    }

    @Test
    public void testRecordsEitherSideOfProbedColumnMatchSearchWalk() throws Exception {
        // The only fixture here that puts more than one _cv record on a real partition timestamp,
        // and so the only one that drives the merge's intra-timestamp advance
        // (CoveringIndexRecordCursorFactory's second inner while loop).
        //
        // "below" is added before sym, so on 2024-01-01..03 its record sits IN FRONT of sym's and
        // the merge has to step over it. Without that step the pointer stops on "below", the
        // == writerIndex guard rejects it, and the walk answers true off the predates branch --
        // the wrong answer, since the O3 back-fill left sym a zero top in every one of those
        // partitions.
        //
        // "above" is added after sym and takes an explicit top on 2024-01-04, where sym owns no
        // record at all. There the merge has to STOP in front of that record: reading it as sym's
        // would answer true off another column's top.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe_neighbours (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_neighbours VALUES
                    ('2024-01-01T12:00:00', 10.0),
                    ('2024-01-02T12:00:00', 20.0),
                    ('2024-01-03T12:00:00', 30.0)
                    """);
            execute("ALTER TABLE t_probe_neighbours ADD COLUMN below SYMBOL");
            execute("ALTER TABLE t_probe_neighbours ADD COLUMN sym SYMBOL");
            // An O3 row into each of the three partitions rewrites them with both new columns
            // present from their first row, which upserts a zero-top record per column per
            // partition -- the runs the advance has to walk through.
            execute("""
                    INSERT INTO t_probe_neighbours VALUES
                    ('2024-01-01T06:00:00', 11.0, 'B', 'A'),
                    ('2024-01-02T06:00:00', 21.0, 'B', 'A'),
                    ('2024-01-03T06:00:00', 31.0, 'B', 'A')
                    """);
            // A fourth partition, appended in order, and only then a column added on top of it:
            // "above" takes its explicit top there while sym, which already covered the whole
            // partition, takes no record.
            execute("INSERT INTO t_probe_neighbours VALUES ('2024-01-04T12:00:00', 40.0, 'B', 'A')");
            execute("ALTER TABLE t_probe_neighbours ADD COLUMN above SYMBOL");
            execute("ALTER TABLE t_probe_neighbours ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            releaseAll();

            try (TableReader reader = engine.getReader("t_probe_neighbours")) {
                final ColumnVersionReader cv = reader.getColumnVersionReader();
                final int writerIndex = symWriterIndex(reader);
                Assert.assertEquals(4, reader.getPartitionCount());
                Assert.assertEquals(
                        "2024-01-01..03 must each own a _cv record for a column BELOW sym, or the"
                                + " intra-timestamp advance never runs",
                        3,
                        countCvRecordsAtPartitions(reader, 0, writerIndex)
                );
                Assert.assertEquals(
                        "sym must own a zero-top record in each of those three partitions, or the"
                                + " advance is not what decides the answer there",
                        3,
                        countCvRecordsAtPartitions(reader, writerIndex, writerIndex + 1)
                );
                Assert.assertEquals(
                        "2024-01-04 must own exactly one _cv record for a column ABOVE sym, or the"
                                + " advance is never asked to stop in front of one",
                        1,
                        countCvRecordsAtPartitions(reader, writerIndex + 1, Integer.MAX_VALUE)
                );
                Assert.assertEquals(
                        "sym must own no record on 2024-01-04, or the walk stops on its own record"
                                + " before it ever reaches the one above it",
                        -1,
                        cv.getRecordIndex(reader.getPartitionTimestampByIndex(3), writerIndex)
                );
                Assert.assertTrue(
                        "the record above sym must carry a real top, or reading it as sym's would"
                                + " not change the answer",
                        cv.getColumnTopQuick(
                                reader.getPartitionTimestampByIndex(3),
                                reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("above"))
                        ) > 0
                );
                Assert.assertTrue(
                        "sym must have been added after 2024-01-01, or the predates branch cannot"
                                + " fire and a missing advance would go unnoticed",
                        cv.getColumnTopPartitionTimestamp(writerIndex) > reader.getPartitionTimestampByIndex(0)
                );
            }
            assertProbeMatchesSearchWalk("t_probe_neighbours", "NO_TOP@4");
        });
    }

    @Test
    public void testSplitPartitionsMatchSearchWalk() throws Exception {
        // An O3 write splits the last partition, so the reader's list gains a partition whose
        // timestamp is not the day's floor. The merge only assumes the list ascends, so the split
        // must not move the answer. A second symbol column added afterwards gives _cv a record on
        // the split half; its writer index sits ABOVE sym's, so the merge has to stop in front of
        // that record and let the == writerIndex guard reject it rather than read its top as
        // sym's. The split half can never be the partition that decides a true answer -- a column
        // top always covers a partition's first rows, so it stays in the prefix -- which is why
        // this case pins a false answer: only then does the walk reach the split at all.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            execute("CREATE TABLE t_probe_split (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_split VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-02T00:00:00', 20.0, 'A'),
                    ('2024-01-03T00:00:00', 30.0, NULL),
                    ('2024-01-03T01:00:00', 40.0, 'A')
                    """);
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_split", "NO_TOP@3");

            execute("ALTER TABLE t_probe_split ADD COLUMN sym2 SYMBOL");
            execute("INSERT INTO t_probe_split VALUES ('2024-01-03T00:30:00', 90.0, 'B', 'X')");
            releaseAll();
            try (TableReader reader = engine.getReader("t_probe_split")) {
                Assert.assertEquals(4, reader.getPartitionCount());
                Assert.assertEquals(
                        "partition 3 must be the split half, a microsecond past 2024-01-03's floor"
                                + " rather than a floor of its own",
                        reader.getPartitionTimestampByIndex(2) + 1,
                        reader.getPartitionTimestampByIndex(3)
                );
                Assert.assertTrue(
                        "the split half must own a _cv record, or the walk stops in front of nothing there",
                        reader.getColumnVersionReader().getRecordIndex(
                                reader.getPartitionTimestampByIndex(3),
                                reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym2"))
                        ) > -1
                );
            }
            assertProbeMatchesSearchWalk("t_probe_split", "NO_TOP@4");
            try (TableReader reader = engine.getReader("t_probe_split")) {
                final int wi = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym2"));
                final long prefix = reader.getPartitionTimestampByIndex(2);
                final long split = reader.getPartitionTimestampByIndex(3);
                final LongList scan = new LongList();
                scan.add(split, split);
                scan.add(split + 1, split + 2);
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                scan.clear();
                scan.add(prefix, split - 1);
                Assert.assertTrue(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                scan.clear();
                scan.add(prefix + 86_400_000_000L, prefix + 2 * 86_400_000_000L);
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
            }
        });
    }

    @Test
    public void testOrderedIntervalsExcludeMiddleTopAcrossTimestampAndWalModes() throws Exception {
        assertMemoryLeak(() -> {
            for (int mode = 0; mode < 4; mode++) {
                final boolean isNano = (mode & 1) != 0;
                final boolean isWal = (mode & 2) != 0;
                final String table = "t_interval_modes_" + mode;
                execute("CREATE TABLE " + table + " (ts " + (isNano ? "TIMESTAMP_NS" : "TIMESTAMP")
                        + ", val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
                execute("INSERT INTO " + table + " VALUES ('2024-01-02T12:00:00', 2.0)");
                drainWalQueue();
                execute("ALTER TABLE " + table + " ADD COLUMN sym SYMBOL");
                execute("INSERT INTO " + table + " VALUES ('2024-01-01T12:00:00', 1.0, NULL), ('2024-01-03T12:00:00', 3.0, NULL)");
                drainWalQueue();
                execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE(val)");
                drainWalQueue();
                releaseAll();
                try (TableReader reader = engine.getReader(table)) {
                    final int wi = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
                    final long first = reader.getPartitionTimestampByIndex(0);
                    final long day = 86_400_000_000L * (isNano ? 1_000 : 1);
                    final LongList scan = new LongList();
                    scan.add(first, first + day - 1);
                    scan.add(first + 2 * day, first + 3 * day - 1);
                    Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                    scan.clear();
                    scan.add(first, first + 3 * day - 1);
                    Assert.assertTrue(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                    scan.clear();
                    scan.add(first + 2 * day, first + 2 * day);
                    Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                    scan.clear();
                    scan.add(first + 2 * day - 1, first + 2 * day - 1);
                    Assert.assertTrue(ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, scan));
                }
                for (int hintMode = 0; hintMode < 2; hintMode++) {
                    final String hint = hintMode == 0 ? "" : "/*+ force_use_covering */ ";
                    assertQuery("SELECT " + hint + "val FROM " + table
                            + " WHERE sym=NULL AND (ts IN '2024-01-01' OR ts IN '2024-01-03')")
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("CoveringIndex")
                            .returns("val\n1.0\n3.0\n");
                }
                assertQuery("SELECT val FROM " + table + " WHERE sym=NULL AND ts IN '2024-01-02'")
                        .noRandomAccess()
                        // The covering factory declares no random access, but returns the index
                        // backup's cursor on this top; that cursor implements recordAt().
                        .skipRandomAccessProbe()
                        .returns("val\n2.0\n");
            }
        });
    }

    @Test
    public void testTopBehindZeroTopRecordsMatchesSearchWalk() throws Exception {
        // The record that decides sits behind two partitions that carry a record of their own
        // with a zero top, so the walk cannot answer until it has stepped over both. Each real
        // timestamp owns exactly one record here -- sym's -- so the pointer advances on timestamp
        // alone and the intra-timestamp advance never runs; the answer comes off the third
        // record's top.
        assertMemoryLeak(() -> {
            createColumnTopTable("t_probe_behind");
            assertProbeMatchesSearchWalk("t_probe_behind", "TOP_RECORD@2");
        });
    }

    @Test
    public void testTopOnlyInLastPartitionMatchesSearchWalk() throws Exception {
        // The record that decides the answer sits at the very end of both lists, so the merge has
        // to hold its pointer all the way through the partitions that have nothing to say.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_probe_last (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_probe_last VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-02T00:00:00', 20.0, 'A'),
                    ('2024-01-03T00:00:00', 30.0, 'A')
                    """);
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_last", "NO_TOP@3");

            // A second symbol column added now takes a top on the last partition only, which
            // leaves sym's own answer alone but puts a _cv run in front of it.
            execute("ALTER TABLE t_probe_last ADD COLUMN sym2 SYMBOL");
            execute("INSERT INTO t_probe_last VALUES ('2024-01-03T01:00:00', 40.0, 'A', 'X')");
            releaseAll();
            assertProbeMatchesSearchWalk("t_probe_last", "NO_TOP@3");
        });
    }

    @Test
    public void testWideColumnVersionListMatchesSearchWalk() throws Exception {
        // The wide case: many partitions and a _cv list long enough that a per-partition binary
        // search really does halve before it scans. sym carries no top anywhere, so the walk
        // cannot return early and has to agree with the oracle over the whole list.
        assertMemoryLeak(() -> {
            createWideColumnVersionTable("t_probe_wide");

            try (TableReader reader = engine.getReader("t_probe_wide")) {
                final int cvBlockCount = reader.getColumnVersionReader()
                        .getCachedColumnVersionList().size() / ColumnVersionReader.BLOCK_SIZE;
                Assert.assertEquals(WIDE_PARTITION_COUNT, reader.getPartitionCount());
                Assert.assertTrue(
                        "_cv must hold more than LongList.binarySearchBlock's 65-block linear-scan"
                                + " threshold, or a per-partition search never halves; got " + cvBlockCount,
                        cvBlockCount > 65
                );
            }
            assertProbeMatchesSearchWalk("t_probe_wide", "NO_TOP@" + WIDE_PARTITION_COUNT);
        });
    }

    /**
     * Asserts the table's partitions are in exactly the formats given, in partition order.
     * CONVERT PARTITION TO PARQUET reports success without converting the active partition of a
     * non-WAL table, so a case that relies on a converted partition has to say so out loud.
     */
    private static void assertPartitionFormats(String tableName, byte... expectedFormats) {
        try (TableReader reader = engine.getReader(tableName)) {
            Assert.assertEquals(
                    "the fixture no longer builds the partitions this case converts",
                    expectedFormats.length,
                    reader.getPartitionCount()
            );
            for (int i = 0; i < expectedFormats.length; i++) {
                Assert.assertEquals(
                        "partition " + i + " is not in the format this case relies on; CONVERT"
                                + " PARTITION skips the active partition of a non-WAL table"
                                + " without failing",
                        expectedFormats[i],
                        reader.getPartitionFormatFromMetadata(i)
                );
            }
        }
    }

    private static void assertProbeMatchesSearchWalk(String tableName, String expectedDecision) {
        try (TableReader reader = engine.getReader(tableName)) {
            final int writerIndex = symWriterIndex(reader);
            final String decision = searchWalkDecision(reader, writerIndex);
            Assert.assertEquals(
                    "the fixture no longer produces the shape this case is about",
                    expectedDecision,
                    decision
            );
            // Under-reporting is the dangerous direction: a false negative sends a NULL key down
            // the covering plan over a partition the posting chain holds nothing for. Over-reporting
            // only runs the backup for nothing, but the probe is exact, so both fail here.
            Assert.assertEquals(
                    "the probe disagrees with a per-partition binary search",
                    !decision.startsWith("NO_TOP@"),
                    CoveringIndexRecordCursorFactory.hasAnyColumnTopForTesting(reader, writerIndex)
            );
        }
    }

    /**
     * Counts the {@code _cv} records that sit at a timestamp the reader lists as a real partition
     * and carry a column index in {@code [fromColumnIndex, toColumnIndex)}. Records below a
     * partition's own timestamp -- the two pseudo-partition runs -- do not count: the merge steps
     * over those on timestamp alone, and it is only records AT a real timestamp that make its
     * intra-timestamp advance run at all.
     */
    private static int countCvRecordsAtPartitions(TableReader reader, int fromColumnIndex, int toColumnIndex) {
        final LongList records = reader.getColumnVersionReader().getCachedColumnVersionList();
        int count = 0;
        for (int i = 0, n = records.size(); i < n; i += ColumnVersionReader.BLOCK_SIZE) {
            final long columnIndex = records.getQuick(i + ColumnVersionReader.COLUMN_INDEX_OFFSET);
            if (columnIndex >= fromColumnIndex && columnIndex < toColumnIndex
                    && isPartitionTimestamp(reader, records.getQuick(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Builds a table whose first two partitions carry a {@code _cv} record for {@code sym} with a
     * zero top, and whose third carries a real one. ADD COLUMN puts its explicit top on the last
     * partition, 2024-01-03; the O3 insert that follows then rewrites 2024-01-01 and 2024-01-02
     * with {@code sym} present from their first row, which upserts a zero-top record for each.
     * The walk therefore has to reach index 2 before it can answer.
     */
    private static void createColumnTopTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T12:00:00', 10.0),
                ('2024-01-02T12:00:00', 20.0),
                ('2024-01-03T12:00:00', 30.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T06:00:00', 11.0, 'A'),
                ('2024-01-02T06:00:00', 21.0, 'A')
                """.formatted(name));
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        releaseAll();
    }

    /**
     * Builds a table whose first partition is written entirely before {@code sym} exists and
     * never rewritten, so {@code _cv} holds no record for it and the walk answers from the
     * default record's added-at timestamp at index 0.
     */
    private static void createPredatingPartitionTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T12:00:00', 10.0),
                ('2024-01-02T12:00:00', 20.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("INSERT INTO " + name + " VALUES ('2024-01-03T12:00:00', 50.0, 'A')");
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        releaseAll();
    }

    private static void createWideColumnVersionTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE,"
                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        // One partition per day, sym left NULL: it has existed since CREATE TABLE, so it takes no
        // column top and the probe has to reach the last partition before it can answer false.
        execute("INSERT INTO " + name + " (ts, val)"
                + " SELECT dateadd('d', (x - 1)::int, '2024-01-01T12:00:00.000000Z'::timestamp), x::double"
                + " FROM long_sequence(" + WIDE_PARTITION_COUNT + ")");
        // A column added now and then back-filled out of order into every partition. Each rewrite
        // upserts a _cv record for it, which is what makes the list long enough to binary-search
        // -- without giving sym a top.
        execute("ALTER TABLE " + name + " ADD COLUMN extra DOUBLE");
        execute("INSERT INTO " + name + " (ts, val, extra)"
                + " SELECT dateadd('d', (x - 1)::int, '2024-01-01T06:00:00.000000Z'::timestamp), x::double, x::double"
                + " FROM long_sequence(" + WIDE_PARTITION_COUNT + ")");
        releaseAll();
    }

    private static boolean isPartitionTimestamp(TableReader reader, long timestamp) {
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            if (reader.getPartitionTimestampByIndex(i) == timestamp) {
                return true;
            }
        }
        return false;
    }

    private static void releaseAll() {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    /**
     * The straightforward walk the merge replaced: one {@link ColumnVersionReader#getRecordIndex}
     * binary search per partition, kept byte-for-byte as the implementation stood before the
     * rewrite. It is the oracle for the merge's answer, and it reports which branch decided and
     * at which partition index -- {@code TOP_RECORD@i} (a {@code _cv} record with a top above
     * zero), {@code PREDATES@i} (no record, and the partition predates the column) or
     * {@code NO_TOP@partitionCount} (the walk fell off the end). The first two mean true, the
     * last means false, which is how {@link #assertProbeMatchesSearchWalk} derives the boolean it
     * holds the merge to without keeping a second copy of this loop.
     */
    private static String searchWalkDecision(TableReader reader, int writerIndex) {
        final ColumnVersionReader cv = reader.getColumnVersionReader();
        final long addedAtPartition = cv.getColumnTopPartitionTimestamp(writerIndex);
        for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
            final long partitionTimestamp = reader.getPartitionTimestampByIndex(i);
            final int recordIndex = cv.getRecordIndex(partitionTimestamp, writerIndex);
            if (recordIndex > -1) {
                if (cv.getColumnTopByIndex(recordIndex) > 0) {
                    return "TOP_RECORD@" + i;
                }
            } else if (addedAtPartition > partitionTimestamp && reader.getPartitionRowCountFromMetadata(i) > 0) {
                return "PREDATES@" + i;
            }
        }
        return "NO_TOP@" + reader.getPartitionCount();
    }

    private static int symWriterIndex(TableReader reader) {
        return reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
    }
}
