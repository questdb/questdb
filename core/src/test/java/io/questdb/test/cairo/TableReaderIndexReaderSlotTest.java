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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pooled reader keeps the index reader objects of a partition it closes. A column dropped while the partition is
 * closed moves the next columns into other slots, and a changed index type or column type gives a slot a column that
 * needs another kind of reader. The partition opens again with the reader objects of the slots. The two symbol columns
 * of the scan tests hold different values, so that a reader left bound to the other column returns the wrong rows.
 */
public class TableReaderIndexReaderSlotTest extends AbstractCairoTest {

    @Test
    public void testBitmapColumnInSlotOfDroppedColumnOpensIndexOnlyWhenScanned() throws Exception {
        testIndexOpenedOnlyByScanAfterColumnDrop("BITMAP", "s.k");
    }

    @Test
    public void testBitmapColumnTakesSlotOfBitmapColumnInOpenPartition() throws Exception {
        // s2 becomes a new column instance in the slot of s1, and takes over the reader objects s1 left there
        testIndexScansAfterDdl("BITMAP", "BITMAP", Partition.OPEN, false, "s2", "ALTER TABLE t DROP COLUMN s1");
    }

    @Test
    public void testBitmapColumnTakesSlotOfPostingColumnInParquetPartition() throws Exception {
        testIndexScansAfterDdl("POSTING", "BITMAP", Partition.REWRITTEN, true, "s2", "ALTER TABLE t DROP COLUMN x");
    }

    @Test
    public void testBitmapColumnTakesSlotOfPostingColumnInPartitionClosedByPool() throws Exception {
        testIndexScansAfterDdl("POSTING", "BITMAP", Partition.CLOSED_BY_POOL, false, "s2", "ALTER TABLE t DROP COLUMN x");
    }

    @Test
    public void testBitmapColumnTakesSlotOfPostingColumnWhilePartitionClosed() throws Exception {
        // a rewrite of the partition gives it a new directory, so that the pooled reader closes it
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        x LONG,
                        p SYMBOL INDEX TYPE POSTING,
                        b SYMBOL INDEX TYPE BITMAP,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, 'p1', 'b1', '2024-01-01T10:00:00.000000Z'),
                        (2, 'p2', 'b2', '2024-01-02T10:00:00.000000Z')
                    """);
            drainWalQueue();

            // the pooled reader opens the index readers of both columns in the first partition
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, reader.getMetadata().getColumnIndex("p"), IndexReader.DIR_FORWARD));
                Assert.assertNotNull(reader.getIndexReader(0, reader.getMetadata().getColumnIndex("b"), IndexReader.DIR_FORWARD));
            }

            // an out-of-order row rewrites the first partition, and x goes, so p and b move one slot left
            execute("INSERT INTO t VALUES (3, 'p3', 'b3', '2024-01-01T09:00:00.000000Z')");
            execute("ALTER TABLE t DROP COLUMN x");
            drainWalQueue();

            // the index scan of b runs on the pooled reader
            assertQuery("SELECT p, b, ts FROM t WHERE b = 'b3'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("p\tb\tts\np3\tb3\t2024-01-01T09:00:00.000000Z\n");
        });
    }

    @Test
    public void testColumnTypeChangeGivesSlotAnotherIndexTypeWhilePartitionClosed() throws Exception {
        testIndexScansAfterDdl(
                "POSTING",
                "POSTING",
                Partition.REWRITTEN,
                false,
                "s1",
                "ALTER TABLE t ALTER COLUMN s1 TYPE VARCHAR",
                "ALTER TABLE t ALTER COLUMN s1 TYPE SYMBOL",
                "ALTER TABLE t ALTER COLUMN s1 ADD INDEX TYPE BITMAP"
        );
    }

    @Test
    public void testFailedBuildOfReplacementReaderIsRetried() throws Exception {
        final AtomicInteger failCounter = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (
                        failCounter.get() > 0
                                && Utf8s.containsAscii(name, "2024-01-01")
                                && Utf8s.containsAscii(name, "s.k")
                                && failCounter.decrementAndGet() >= 0
                ) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (s SYMBOL INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('a', '2024-01-01T10:00:00.000000Z'), ('b', '2024-01-01T11:00:00.000000Z')");
            try (TableReader reader = getReader("t")) {
                final int s = reader.getMetadata().getColumnIndex("s");
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, s, IndexReader.DIR_FORWARD));
                Assert.assertNotNull(reader.getIndexReader(0, s, IndexReader.DIR_BACKWARD));
                reader.closePartitionByIndex(0);

                execute("ALTER TABLE t ALTER COLUMN s DROP INDEX");
                execute("ALTER TABLE t ALTER COLUMN s ADD INDEX TYPE BITMAP");
                Assert.assertTrue(reader.reload());
                reader.openPartition(0);
                for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                    // the POSTING reader is gone, and the open of the BITMAP key file fails once
                    failCounter.set(1);
                    try {
                        reader.getIndexReader(0, s, direction);
                        Assert.fail("the key file cannot be opened");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                    }
                    Assert.assertEquals(0, failCounter.get());

                    final IndexReader indexReader = reader.getIndexReader(0, s, direction);
                    Assert.assertTrue(indexReader.isOpen());
                    // index key 1 is 'a' in row 0, index key 2 is 'b' in row 1
                    for (int key = 1; key <= 2; key++) {
                        try (RowCursor cursor = indexReader.getCursor(key, 0, 1)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(key - 1, cursor.next());
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                    Assert.assertSame(indexReader, reader.getIndexReader(0, s, direction));
                }
            }
        });
    }

    @Test
    public void testIndexLookupSaysNotIndexedAfterIndexDroppedWhilePartitionClosed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX TYPE BITMAP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('a', '2024-01-01T10:00:00.000000Z')");
            try (TableReader reader = getReader("t")) {
                final int s = reader.getMetadata().getColumnIndex("s");
                reader.openPartition(0);
                Assert.assertNotNull(reader.getIndexReader(0, s, IndexReader.DIR_FORWARD));
                reader.closePartitionByIndex(0);

                execute("ALTER TABLE t ALTER COLUMN s DROP INDEX");
                Assert.assertTrue(reader.reload());
                try {
                    reader.getIndexReader(0, s, IndexReader.DIR_FORWARD);
                    Assert.fail("the column has no index");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Not indexed: s");
                }
            }
        });
    }

    @Test
    public void testIndexReaderObjectsReusedWhileKindFits() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        b SYMBOL INDEX TYPE BITMAP,
                        p SYMBOL INDEX TYPE POSTING,
                        d SYMBOL INDEX TYPE POSTING DELTA,
                        e SYMBOL INDEX TYPE POSTING EF,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t VALUES ('a', 'a', 'a', 'a', '2024-01-01T10:00:00.000000Z')");
            // the partition does not hold n, its index readers are stand-ins
            execute("ALTER TABLE t ADD COLUMN n SYMBOL INDEX");
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                for (String column : new String[]{"b", "p", "d", "e", "n"}) {
                    final int columnIndex = reader.getMetadata().getColumnIndex(column);
                    for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                        final IndexReader indexReader = reader.getIndexReader(0, columnIndex, direction);
                        final boolean isStandIn = indexReader instanceof IndexFwdNullReader || indexReader instanceof IndexBwdNullReader;
                        Assert.assertEquals(column, "n".equals(column), isStandIn);
                        Assert.assertSame(column, indexReader, reader.getIndexReader(0, columnIndex, direction));
                        if (!isStandIn) {
                            // the partition keeps the reader objects it closes, and opens with them again
                            reader.closePartitionByIndex(0);
                            reader.openPartition(0);
                            // the partition opens the index files only when a lookup needs them
                            Assert.assertFalse(column, indexReader.isOpen());
                            Assert.assertSame(column, indexReader, reader.getIndexReader(0, columnIndex, direction));
                            Assert.assertTrue(column, indexReader.isOpen());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIndexTypeChangesFromBitmapToPostingWhilePartitionClosed() throws Exception {
        testIndexScansAfterDdl(
                "BITMAP",
                "BITMAP",
                Partition.REWRITTEN,
                false,
                "s1",
                "ALTER TABLE t ALTER COLUMN s1 DROP INDEX",
                "ALTER TABLE t ALTER COLUMN s1 ADD INDEX TYPE POSTING"
        );
    }

    @Test
    public void testIndexTypeChangesFromPostingToBitmapWhilePartitionClosed() throws Exception {
        testIndexScansAfterDdl(
                "POSTING",
                "POSTING",
                Partition.REWRITTEN,
                false,
                "s1",
                "ALTER TABLE t ALTER COLUMN s1 DROP INDEX",
                "ALTER TABLE t ALTER COLUMN s1 ADD INDEX TYPE BITMAP"
        );
    }

    @Test
    public void testPostingColumnInSlotOfDroppedColumnOpensIndexOnlyWhenScanned() throws Exception {
        testIndexOpenedOnlyByScanAfterColumnDrop("POSTING", "s.pk");
    }

    @Test
    public void testPostingColumnTakesSlotOfBitmapColumnWhilePartitionClosed() throws Exception {
        testIndexScansAfterDdl("BITMAP", "POSTING", Partition.REWRITTEN, false, "s2", "ALTER TABLE t DROP COLUMN x");
    }

    @Test
    public void testPostingColumnTakesSlotOfPostingColumnInOpenPartition() throws Exception {
        // s2 becomes a new column instance in the slot of s1, and takes over the reader objects s1 left there
        testIndexScansAfterDdl("POSTING", "POSTING", Partition.OPEN, false, "s2", "ALTER TABLE t DROP COLUMN s1");
    }

    @Test
    public void testPostingColumnTakesSlotOfPostingColumnWhilePartitionClosed() throws Exception {
        testIndexScansAfterDdl("POSTING", "POSTING", Partition.REWRITTEN, false, "s2", "ALTER TABLE t DROP COLUMN x");
    }

    @Test
    public void testStandInReaderReplacedWhenOpenPartitionGainsColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01T10:00:00.000000Z')");
            execute("ALTER TABLE t ADD COLUMN n SYMBOL INDEX");
            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                final int n = reader.getMetadata().getColumnIndex("n");
                Assert.assertTrue(reader.getIndexReader(0, n, IndexReader.DIR_FORWARD) instanceof IndexFwdNullReader);
                Assert.assertTrue(reader.getIndexReader(0, n, IndexReader.DIR_BACKWARD) instanceof IndexBwdNullReader);
            }
            // an in-order append keeps the partition open in the pooled reader; the DDL makes the reload reshuffle
            execute("INSERT INTO t VALUES (2, '2024-01-01T11:00:00.000000Z', 'a')");
            execute("ALTER TABLE t ADD COLUMN m INT");
            try (TableReader reader = getReader("t")) {
                Assert.assertEquals(2, reader.openPartition(0));
                final int n = reader.getMetadata().getColumnIndex("n");
                for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                    final IndexReader indexReader = reader.getIndexReader(0, n, direction);
                    Assert.assertFalse("stand-in survived, direction=" + direction,
                            indexReader instanceof IndexFwdNullReader || indexReader instanceof IndexBwdNullReader);
                }
            }
            assertQuery("SELECT x FROM t WHERE n = 'a'").noLeakCheck().returns("x\n2\n");
            assertQuery("SELECT x FROM t WHERE n IS NULL").noLeakCheck().returns("x\n1\n");
        });
    }

    /**
     * A backward index scan of x leaves its backward readers in both open partitions of a pooled reader. DROP COLUMN x
     * makes s a new column instance in the slot of x. The reload must not open the index of s, which no query asked
     * for, and a later index scan of s must open it with the kept reader objects.
     */
    private void testIndexOpenedOnlyByScanAfterColumnDrop(String indexType, String keyFileName) throws Exception {
        final AtomicInteger keyFileOpens = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // the symbol map of a BITMAP column has a key file of the same name, outside the partitions
                if (Utf8s.containsAscii(name, "2024-01-0") && Utf8s.containsAscii(name, keyFileName)) {
                    keyFileOpens.incrementAndGet();
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (x SYMBOL INDEX TYPE " + indexType + ", s SYMBOL INDEX TYPE " + indexType
                    + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // x and s give 'a' the same key and other rows, so that a reader left bound to x returns the wrong rows
            execute("""
                    INSERT INTO t VALUES
                        ('a', 'a', '2024-01-01T10:00:00.000000Z'),
                        ('b', 'b', '2024-01-01T11:00:00.000000Z'),
                        ('a', 'b', '2024-01-01T12:00:00.000000Z'),
                        ('a', 'a', '2024-01-02T10:00:00.000000Z')
                    """);
            drainWalQueue();
            assertQuery("SELECT ts, x FROM t WHERE x = 'a' ORDER BY ts DESC")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .withPlanContaining("Index backward scan on: x")
                    .returns("""
                            ts\tx
                            2024-01-02T10:00:00.000000Z\ta
                            2024-01-01T12:00:00.000000Z\ta
                            2024-01-01T10:00:00.000000Z\ta
                            """);

            execute("ALTER TABLE t DROP COLUMN x");
            drainWalQueue();
            keyFileOpens.set(0);
            assertQuery("SELECT count() FROM t")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("count\n4\n");
            Assert.assertEquals(0, keyFileOpens.get());

            assertQuery("SELECT ts, s FROM t WHERE s = 'a' ORDER BY ts DESC")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .withPlanContaining("Index backward scan on: s")
                    .returns("""
                            ts\ts
                            2024-01-02T10:00:00.000000Z\ta
                            2024-01-01T10:00:00.000000Z\ta
                            """);
            // one key file per partition
            Assert.assertEquals(2, keyFileOpens.get());
        });
    }

    /**
     * A pooled reader opens the index readers of s1 and s2 in the first partition, in both directions. The DDL then
     * runs with the partition in the given state in that reader, and index scans of {@code column} run on it.
     */
    private void testIndexScansAfterDdl(
            String s1IndexType,
            String s2IndexType,
            Partition partition,
            boolean isParquet,
            String column,
            String... ddl
    ) throws Exception {
        switch (partition) {
            // a reader that returns to the pool keeps only its last partition open
            case CLOSED_BY_POOL -> setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, 1);
            // a rewrite of the partition gives it a new directory, so that the pooled reader closes it
            case REWRITTEN -> setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            case OPEN -> {
            }
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, s1 SYMBOL INDEX TYPE " + s1IndexType + ", s2 SYMBOL INDEX TYPE " + s2IndexType
                    + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, 'a', 'a', '2024-01-01T10:00:00.000000Z'),
                        (2, 'a', 'b', '2024-01-01T11:00:00.000000Z'),
                        (3, 'b', 'a', '2024-01-01T12:00:00.000000Z'),
                        (4, 'a', 'a', '2024-01-02T10:00:00.000000Z')
                    """);
            if (isParquet) {
                execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            drainWalQueue();

            try (TableReader reader = getReader("t")) {
                reader.openPartition(0);
                reader.openPartition(1);
                for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                    if (reader.getMetadata().isColumnIndexed(i)) {
                        Assert.assertNotNull(reader.getIndexReader(0, i, IndexReader.DIR_FORWARD));
                        Assert.assertNotNull(reader.getIndexReader(0, i, IndexReader.DIR_BACKWARD));
                    }
                }
            }

            if (partition == Partition.REWRITTEN) {
                execute("INSERT INTO t VALUES (5, 'a', 'b', '2024-01-01T09:00:00.000000Z')");
            }
            for (String statement : ddl) {
                execute(statement);
            }
            drainWalQueue();

            // Both columns give 'a' and 'b' the same keys, and 'a' other rows: s1 at 09:00, 10:00 and 11:00, s2 at 10:00
            // and 12:00. The index of the other column answers the same key with other rows.
            final boolean isS1 = "s1".equals(column);
            final String rewrittenRow = isS1 && partition == Partition.REWRITTEN ? "2024-01-01T09:00:00.000000Z\ta\n" : "";
            final String secondRow = isS1 ? "2024-01-01T11:00:00.000000Z\ta\n" : "2024-01-01T12:00:00.000000Z\ta\n";
            assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'a'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: " + column)
                    .returns("ts\t" + column + "\n"
                            + rewrittenRow
                            + "2024-01-01T10:00:00.000000Z\ta\n"
                            + secondRow
                            + "2024-01-02T10:00:00.000000Z\ta\n");
            assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'a' ORDER BY ts DESC")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .withPlanContaining("Index backward scan on: " + column)
                    .returns("ts\t" + column + "\n"
                            + "2024-01-02T10:00:00.000000Z\ta\n"
                            + secondRow
                            + "2024-01-01T10:00:00.000000Z\ta\n"
                            + rewrittenRow);
            assertQuery("SELECT isParquet FROM table_partitions('t') WHERE name = '2024-01-01'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("isParquet\n" + isParquet + "\n");
        });
    }

    private enum Partition {
        CLOSED_BY_POOL, OPEN, REWRITTEN
    }
}
