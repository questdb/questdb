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

import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code CoveringIndexRecordCursorFactory.hasAnyColumnTop()} decides, per open, whether a NULL
 * key has to leave the covering plan for the backup. It answers from {@code _cv} through two
 * independent tests, and each one is the only thing standing between a table and a wrong answer
 * for some shape:
 * <ul>
 *     <li>a partition with a record whose column top is above zero -- part of the partition
 *     predates the column;</li>
 *     <li>a partition with no record at all that sits below the column's default partition --
 *     the whole partition predates it.</li>
 * </ul>
 * Most fixtures answer true from both at once, which leaves either one free to be broken
 * unnoticed. Each test here isolates one, and asserts the {@code _cv} shape that makes it the
 * only test that can fire.
 * <p>
 * The third producer -- ATTACH PARTITION, which writes a per-partition top and no default
 * record at all -- lives in {@link CoveringIndexAttachPartitionTest}.
 */
public class CoveringIndexColumnTopDetectionTest extends AbstractCairoTest {

    @Test
    public void testNoColumnTopAnywhereKeepsCoveringPlan() throws Exception {
        // The control. sym has existed since CREATE TABLE and nothing has ever attached to the
        // table, so no partition carries a record for it and the walk falls off the end.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_ct_flat (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_ct_flat VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-01T01:00:00', 20.0, 'A'),
                    ('2024-01-02T00:00:00', 30.0, NULL)
                    """);
            assertNoColumnVersionRecords("t_ct_flat");

            // A literal NULL key always gets a backup FACTORY -- codegen cannot know what _cv
            // holds -- so the plan says so either way. The hint is what pins the answer: it
            // suppresses the backup outright, and then throws on any open where
            // hasAnyColumnTop() says true. No throw here means it said false.
            final String sql = "SELECT /*+ force_use_covering */ ts, sym, val FROM t_ct_flat WHERE sym = null";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanNotContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-02T00:00:00.000000Z\t\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("/*+ force_use_covering */", "/*+ no_covering */"));
            assertSqlCursors(sql, sql.replace("/*+ force_use_covering */", ""));
        });
    }

    @Test
    public void testPartitionWhollyPredatingColumnForcesBackup() throws Exception {
        // Only the second test can answer true here: 2024-01-01 has no record of its own and
        // sits below the column's default partition, and 2024-01-02 -- the one partition that
        // does have a record -- had its top rewritten to zero by an O3 insert. Read the first
        // test alone and this table looks top-free, and every row of 2024-01-01 disappears from
        // a NULL-key scan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_ct_predate (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_ct_predate VALUES ('2024-01-01T00:00:00', 10.0)");
            execute("INSERT INTO t_ct_predate VALUES ('2024-01-02T02:00:00', 20.0)");
            execute("ALTER TABLE t_ct_predate ADD COLUMN sym SYMBOL");
            // O3 into the newest partition rewrites it, and sym goes into every one of its rows.
            execute("INSERT INTO t_ct_predate VALUES ('2024-01-02T01:00:00', 30.0, 'A')");
            execute("ALTER TABLE t_ct_predate ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t_ct_predate")) {
                final ColumnVersionReader cv = reader.getColumnVersionReader();
                final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(
                        "the oldest partition must carry no record, or the other test decides",
                        -1,
                        cv.getRecordIndex(reader.getPartitionTimestampByIndex(0), writerIndex)
                );
                final int newest = cv.getRecordIndex(reader.getPartitionTimestampByIndex(1), writerIndex);
                Assert.assertTrue(newest > -1);
                Assert.assertEquals(
                        "the O3 rewrite must have zeroed the newest partition's top",
                        0,
                        cv.getColumnTopByIndex(newest)
                );
            }

            final String sql = "SELECT ts, sym, val FROM t_ct_predate WHERE sym = null";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .timestamp("ts")
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-02T02:00:00.000000Z\t\t20.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testPartitionWithPartialColumnTopForcesBackup() throws Exception {
        // The mirror image: one partition, so nothing can wholly predate the column, and the
        // record the ADD COLUMN left behind is the only thing that says two of its three rows
        // hold no sym.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_ct_partial (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_ct_partial VALUES
                    ('2024-01-01T00:00:00', 10.0),
                    ('2024-01-01T01:00:00', 20.0)
                    """);
            execute("ALTER TABLE t_ct_partial ADD COLUMN sym SYMBOL");
            execute("INSERT INTO t_ct_partial VALUES ('2024-01-01T02:00:00', 30.0, 'A')");
            execute("ALTER TABLE t_ct_partial ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t_ct_partial")) {
                final ColumnVersionReader cv = reader.getColumnVersionReader();
                final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
                Assert.assertEquals(1, reader.getPartitionCount());
                final long partitionTimestamp = reader.getPartitionTimestampByIndex(0);
                Assert.assertEquals(
                        "no partition may sit below the column's default partition, or the other test decides",
                        partitionTimestamp,
                        cv.getColumnTopPartitionTimestamp(writerIndex)
                );
                final int recordIndex = cv.getRecordIndex(partitionTimestamp, writerIndex);
                Assert.assertTrue(recordIndex > -1);
                Assert.assertEquals(2, cv.getColumnTopByIndex(recordIndex));
            }

            final String sql = "SELECT ts, sym, val FROM t_ct_partial WHERE sym = null";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .timestamp("ts")
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    private static void assertNoColumnVersionRecords(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final ColumnVersionReader cv = reader.getColumnVersionReader();
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex("sym"));
            Assert.assertEquals(Long.MIN_VALUE, cv.getColumnTopPartitionTimestamp(writerIndex));
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                Assert.assertEquals(
                        "partition " + i + " must carry no column version record for sym",
                        -1,
                        cv.getRecordIndex(reader.getPartitionTimestampByIndex(i), writerIndex)
                );
            }
        }
    }
}
