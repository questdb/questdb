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
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Turning {@code cairo.o3.partition.merge.append.enabled} OFF on a database that already holds composite
 * partitions must not lose or reorder their rows.
 * <p>
 * The flag guards the WRITE path only, so with it off an O3 write into an existing composite partition
 * falls through to the legacy native merge - which reads every column file as one flat
 * {@code [0, liveRows)} range from file row 0. A composite directory can hold dead space at row 0 and
 * live rows above the live count, so that read serves the wrong rows with no error raised. Since
 * production defaults the flag to {@code false}, reverting it IS the default state, and a fresh
 * {@link io.questdb.cairo.TableWriter} folds every composite partition back to plain before it processes
 * a row.
 */
public class CompositePartitionMergeAppendDisabledTest extends AbstractCairoTest {

    @Test
    public void testO3IntoACompositePartitionAfterTurningTheFlagOff() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            final String beforeDay = fingerprintOfDay("2024-01-01");
            final long beforeRows = rowsOfDay("2024-01-01");

            turnMergeAppendOff();
            Assert.assertFalse("the fold left a composite partition behind", isComposite("2024-01-01"));
            Assert.assertEquals("the fold lost rows", beforeRows, rowsOfDay("2024-01-01"));
            Assert.assertEquals("the fold changed the day's data", beforeDay, fingerprintOfDay("2024-01-01"));

            // The write the legacy merge would have got wrong: out-of-order rows into the folded day.
            execute("INSERT INTO x SELECT cast(x AS int) + 500_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T02:30:00', 1_000_000L) ts FROM long_sequence(300)");
            drainWalQueue();

            Assert.assertEquals("the O3 write lost rows", beforeRows + 300, rowsOfDay("2024-01-01"));
            // The rows that were already there still read back exactly as they were written.
            Assert.assertEquals(
                    "the O3 write changed rows it did not write",
                    beforeDay,
                    fingerprintOf("SELECT i, s FROM x WHERE ts IN '2024-01-01' AND i < 500_000")
            );
        });
    }

    @Test
    public void testTheFoldRunsOnceAndLeavesLaterWritesAlone() throws Exception {
        assertMemoryLeak(() -> {
            createCompositeMiddleDay();
            turnMergeAppendOff();
            final long rowsBefore = rowsOfDay("2024-01-01");

            for (int i = 0; i < 3; i++) {
                execute("INSERT INTO x SELECT cast(x AS int) + 600_000 i, rnd_str(5, 16, 2) s," +
                        " timestamp_sequence('2024-01-01T03:00:00', 1_000_000L) ts FROM long_sequence(100)");
                drainWalQueue();
                Assert.assertFalse("a write with the flag off made the day composite again", isComposite("2024-01-01"));
            }
            Assert.assertEquals(rowsBefore + 300, rowsOfDay("2024-01-01"));
        });
    }

    /**
     * A composite day that is NOT the last partition - the one the legacy merge path actually reaches.
     * Built with the flag ON, which is the test-suite default.
     */
    private static void createCompositeMiddleDay() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();

        // Two narrow backdated strides: merge-append rewrites the owning piece at the shared file tail
        // and abandons its previous copy, so the directory keeps dead space below its live rows.
        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
        Assert.assertTrue("fixture produced no composite partition", isComposite("2024-01-01"));
    }

    private static String fingerprintOfDay(String day) throws Exception {
        return fingerprintOf("SELECT i, s FROM x WHERE ts IN '" + day + "'");
    }

    /** Content fingerprint over (i, s), so a reordered or dead-row read cannot match. */
    private static String fingerprintOf(String sql) throws Exception {
        long count = 0;
        long sum = 0;
        long strLen = 0;
        try (RecordCursorFactory f = select(sql)) {
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

    private static boolean isComposite(String day) {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(MicrosTimestampDriver.floor(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
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

    /**
     * What an operator does: flip the key and restart. The flag is not reloadable, so a restart is
     * required; releasing the pooled writer is this suite's stand-in for one - the next write opens a
     * fresh {@link io.questdb.cairo.TableWriter}, which is where the fold runs.
     */
    private static void turnMergeAppendOff() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        engine.releaseInactive();
        engine.releaseAllWriters();
        // The restart itself: opening a writer under the new setting is what runs the fold.
        engine.getWriter(engine.verifyTableName("x"), "test").close();
    }
}
