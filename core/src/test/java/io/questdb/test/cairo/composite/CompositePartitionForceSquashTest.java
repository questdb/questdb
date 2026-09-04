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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A FORCED squash - what {@code detachPartition}, {@code SQUASH PARTITIONS} and parquet conversion all
 * run - must leave a logical day as exactly one plain directory, composite siblings included. The
 * opportunistic squash gives up on a composite partition and lets compaction resolve it later; the forced
 * one cannot, because its callers act on the result immediately. It flattens each composite partition
 * with {@code compactPartitionToPlain} right before reading it instead.
 */
public class CompositePartitionForceSquashTest extends AbstractCairoTest {

    @Before
    public void setUpSplits() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
    }

    @Test
    public void testDetachFoldsEverySiblingOfACompositeDay() throws Exception {
        assertMemoryLeak(() -> {
            createSplitCompositeDay();
            final long rowsOfDay = rowsOfDay("2024-01-01");

            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();

            Assert.assertEquals(
                    "detach left siblings of the day attached",
                    0,
                    partitionCountOfDay("2024-01-01")
            );
            // One detached directory holding every row the day had: proof the forced squash folded the
            // composite sibling in rather than detaching the front partition on its own.
            Assert.assertEquals(
                    "the detached partition does not hold the whole day",
                    rowsOfDay,
                    scalar("SELECT coalesce(sum(numRows), 0) FROM table_partitions('x')" +
                            " WHERE name = '2024-01-01.detached'")
            );
        });
    }

    /**
     * The shape {@code squashPartitionForce} alone cannot reach: a logical day with NO split siblings
     * never enters {@code squashSplitPartitions}, so nothing there folds it. Detach hard-links the
     * directory as it stands, and attach reads every column file as one flat {@code [0, liveRows)} range
     * from byte 0 while {@code setPartitionFormat} drops the composite flag - so an unfolded composite
     * directory comes back serving dead rows in no particular timestamp order, with no error raised.
     */
    @Test
    public void testDetachAndAttachAnUnsplitCompositeDay() throws Exception {
        assertMemoryLeak(() -> {
            createUnsplitCompositeDay();
            final long rowsOfDay = rowsOfDay("2024-01-01");
            final String before = fingerprintOfDay("2024-01-01");

            execute("ALTER TABLE x DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals("detach left the day attached", 0, partitionCountOfDay("2024-01-01"));

            renameDetachedToAttachable("2024-01-01");
            execute("ALTER TABLE x ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();

            Assert.assertEquals("the round trip lost rows", rowsOfDay, rowsOfDay("2024-01-01"));
            Assert.assertEquals("the round trip changed the day's data", before, fingerprintOfDay("2024-01-01"));
            Assert.assertEquals(
                    "the round trip left the day's timestamps unordered",
                    rowsOfDay,
                    scalar("SELECT count() FROM (SELECT ts FROM x WHERE ts IN '2024-01-01' ORDER BY ts)")
            );
        });
    }

    @Test
    public void testSquashPartitionsFoldsACompositeDayIntoOne() throws Exception {
        assertMemoryLeak(() -> {
            createSplitCompositeDay();
            final String before = fingerprintOfDay("2024-01-01");

            execute("ALTER TABLE x SQUASH PARTITIONS");
            drainWalQueue();

            Assert.assertEquals(
                    "forced squash left the day split",
                    1,
                    partitionCountOfDay("2024-01-01")
            );
            Assert.assertFalse("forced squash left the day composite", isComposite("2024-01-01"));
            Assert.assertEquals("forced squash changed the day's data", before, fingerprintOfDay("2024-01-01"));
        });
    }

    /**
     * The third forced-squash caller. A parquet conversion encodes ONE native directory, so the squash it
     * runs first has to fold the day's composite siblings into that directory. Coverage, not a regression
     * test for the guard: this path folds the day either way, because the conversion walks the day's
     * siblings itself. It pins the outcome the other two tests pin - one directory, every row - for the
     * remaining force caller.
     */
    @Test
    public void testConvertToParquetFoldsACompositeDayFirst() throws Exception {
        assertMemoryLeak(() -> {
            createSplitCompositeDay();
            final long rowsOfDay = rowsOfDay("2024-01-01");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertEquals(
                    "the conversion left the day split",
                    1,
                    partitionCountOfDay("2024-01-01")
            );
            final TableToken tt = engine.verifyTableName("x");
            try (TableReader reader = engine.getReader(tt)) {
                final TxReader txReader = reader.getTxFile();
                final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor("2024-01-01T00:00:00.000000Z"));
                Assert.assertTrue("day was not converted to parquet", txReader.isPartitionParquet(partitionIndex));
                Assert.assertEquals(
                        "the parquet partition does not hold the whole day",
                        rowsOfDay,
                        txReader.getPartitionSize(partitionIndex)
                );
            }
            Assert.assertEquals("the conversion lost rows", rowsOfDay, rowsOfDay("2024-01-01"));
        });
    }

    /**
     * Builds a logical day split into siblings with at least one of them composite - the exact shape the
     * unconditional composite guard used to bail out on.
     */
    private static void createSplitCompositeDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        // Split the day while it is still the last one - only the last partition ever splits. An O3 write
        // landing well inside it cuts a sibling off the tail.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        execute("INSERT INTO x SELECT cast(x AS int) + 200_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01T05:00:00', 1_000L) ts FROM long_sequence(200)");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        Assert.assertTrue("fixture did not split the day", partitionCountOfDay("2024-01-01") > 1);

        // Two narrow backdated strides over the same rows: merge-append rewrites the owning piece at the
        // shared file tail and abandons its previous copy, leaving the partition composite.
        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
        Assert.assertTrue("fixture produced no composite partition", isComposite("2024-01-01"));

        // A later day, so the split day is no longer the last one: the last partition can never be
        // detached. The composite flag is what keeps the opportunistic squash off the day in the
        // meantime, so the siblings survive this commit.
        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();
        Assert.assertTrue("fixture lost the split before the forced squash", partitionCountOfDay("2024-01-01") > 1);
    }

    /**
     * Builds a logical day that is COMPOSITE but NOT split - the shape a forced squash walks straight
     * past. Only the last partition ever splits, so the day is pushed off the end before the backdated
     * strides that make it composite.
     */
    private static void createUnsplitCompositeDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        // A later day, so 2024-01-01 is no longer the last one and cannot split.
        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();

        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
        Assert.assertEquals("fixture split the day", 1, partitionCountOfDay("2024-01-01"));
        Assert.assertTrue("fixture produced no composite partition", isComposite("2024-01-01"));
    }

    private static void renameDetachedToAttachable(String day) {
        final TableToken tt = engine.verifyTableName("x");
        try (Path from = new Path(); Path to = new Path()) {
            from.of(configuration.getDbRoot()).concat(tt).concat(day).put(TableUtils.DETACHED_DIR_MARKER).$();
            to.of(configuration.getDbRoot()).concat(tt).concat(day).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(from.$(), to.$()) > -1);
        }
    }

    /** Content fingerprint of one day, so unrelated partitions cannot move it. */
    private static String fingerprintOfDay(String day) throws Exception {
        long count = 0;
        long sum = 0;
        long strLen = 0;
        try (RecordCursorFactory f = select("SELECT i, s FROM x WHERE ts IN '" + day + "'")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    sum += c.getRecord().getInt(0);
                    final CharSequence s = c.getRecord().getStrA(1);
                    strLen += s == null ? 0 : s.length();
                }
            }
        }
        return count + "/" + sum + "/" + strLen;
    }

    /** Whether the day's OWN (front) partition is flagged composite. */
    private static boolean isComposite(String day) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    /** Attached partitions of one logical day; a detached directory still shows up here, so exclude it. */
    private static long partitionCountOfDay(String day) throws Exception {
        return scalar("SELECT count() FROM table_partitions('x') WHERE name LIKE '" + day + "%'" +
                " AND NOT name LIKE '%.detached'");
    }

    private static long rowsOfDay(String day) throws Exception {
        return scalar("SELECT count() FROM x WHERE ts IN '" + day + "'");
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }
}
