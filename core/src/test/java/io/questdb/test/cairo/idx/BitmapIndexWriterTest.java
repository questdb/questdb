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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Files;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every table writer path that opens a bitmap index of a partition and adds keys to it: the opens write nothing to the
 * key file, and neither do new keys over entries that hold zeros. Each table {@code t} indexes its symbol column, and
 * {@code ref} holds the same rows in a column without an index, which every query of {@code t} must match.
 */
public class BitmapIndexWriterTest extends AbstractCairoTest {

    @Test
    public void testColumnAddedAfterDataWritesNoSkippedKeys() throws Exception {
        // ADD COLUMN ... INDEX on a table with rows gives the partition an empty index above a column top. The rows
        // written after it, rows rolled back and the rows of a commit with a sparse key write nothing to the key file
        // but the entries of their keys.
        assertMemoryLeak(() -> {
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            for (String table : new String[]{"t", "ref"}) {
                execute("CREATE TABLE " + table + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " SELECT 'a' || x, (x * 1_000_000)::TIMESTAMP FROM long_sequence(1_000)");
            }
            execute("ALTER TABLE t ADD COLUMN s2 SYMBOL INDEX");
            execute("ALTER TABLE ref ADD COLUMN s2 SYMBOL");
            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());

            final String rows = "SELECT 'a' || x, ((1_000 + x) * 1_000_000)::TIMESTAMP, 'b' || (x % 100) FROM long_sequence(500)";
            execute("INSERT INTO t " + rows);
            execute("INSERT INTO ref " + rows);
            try (TableWriter writer = getWriter("t")) {
                for (int i = 0; i < 10; i++) {
                    final TableWriter.Row row = writer.newRow((2_000 + i) * 1_000_000L);
                    row.putSym(0, "a" + i);
                    row.putSym(2, i % 2 == 0 ? "b" + i : "rolledBack" + i);
                    row.append();
                }
                writer.rollback();
            }
            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());

            // out of order: the index adds c4 first, past c1 to c3, then b7 that the partition holds, c1 to c3, and c5
            // right past c4
            final String sparseRows = """
                    VALUES ('a1', 2_002_000_000::TIMESTAMP, 'b7'), ('a1', 2_003_000_000::TIMESTAMP, 'c1'),
                    ('a1', 2_004_000_000::TIMESTAMP, 'c2'), ('a1', 2_005_000_000::TIMESTAMP, 'c3'),
                    ('a1', 2_001_000_000::TIMESTAMP, 'c4'), ('a1', 2_006_000_000::TIMESTAMP, 'c5')
                    """;
            execute("INSERT INTO t " + sparseRows);
            execute("INSERT INTO ref " + sparseRows);
            assertIndexReadsAsRef(engine, sqlExecutionContext, "s2", "'b0', 'b7', 'b99', 'c1', 'c4', 'c5'");
            assertSqlCursors("SELECT * FROM ref WHERE s2 IS NULL", "SELECT * FROM t WHERE s2 IS NULL");
            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
        });
    }

    @Test
    public void testNonWalRollbackWritesNoSkippedKeys() throws Exception {
        // A rollback with rows in flight closes the index writers of the last partition without cutting their key
        // files, which keep a whole key append page past the last entry, and opens them again. The opens write
        // nothing, and the commit after them, with an existing key, a dense new key and a sparse one, writes nothing to
        // the key file but the entries of its keys, and reads only the entries the sparse key skips.
        assertMemoryLeak(() -> {
            for (long keyAppendPageSize : new long[]{Files.PAGE_SIZE, 16 * 1024, 512 * 1024, 4 * 1024 * 1024}) {
                setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE, keyAppendPageSize);
                for (String partitionBy : new String[]{"DAY", "NONE"}) {
                    BitmapIndexWriter.testZeroedKeyBytes.set(0);
                    execute("CREATE TABLE t (s SYMBOL CAPACITY 32_768 INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
                    execute("CREATE TABLE ref (s SYMBOL CAPACITY 32_768, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
                    for (String table : new String[]{"t", "ref"}) {
                        // the map takes p1 to p9 past s1 to s20001, and keeps them when TRUNCATE empties the table
                        execute("INSERT INTO " + table + " SELECT 's' || x, x::TIMESTAMP FROM long_sequence(20_001)");
                        execute("INSERT INTO " + table + " SELECT 'p' || x, (20_001 + x)::TIMESTAMP FROM long_sequence(9)");
                        engine.releaseAllReaders();
                        execute("TRUNCATE TABLE " + table + " KEEP SYMBOL MAPS");
                        execute("INSERT INTO " + table + " SELECT 's' || x, x::TIMESTAMP FROM long_sequence(20_001)");
                    }
                    try (TableWriter writer = getWriter("t")) {
                        for (int round = 0; round < 3; round++) {
                            for (int i = 0; i < 10; i++) {
                                final TableWriter.Row row = writer.newRow(100_000 + round * 100 + i);
                                row.putSym(0, i % 2 == 0 ? "s" + (i + 1) : "rolledBack" + i);
                                row.append();
                            }
                            writer.rollback();
                            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
                        }
                    }

                    // s7, which the partition holds, p1 right past its keys, p9 past p2 to p8, which the map holds and
                    // the partition does not, and n1, new to the map, right past p9
                    final String rows = """
                            VALUES ('s7', 20_002::TIMESTAMP), ('p1', 20_003::TIMESTAMP), ('p9', 20_004::TIMESTAMP),
                            ('n1', 20_005::TIMESTAMP)
                            """;
                    BitmapIndexWriter.isScannedKeyBytesCounterEnabled = true;
                    try {
                        BitmapIndexWriter.testScannedKeyBytes.set(0);
                        execute("INSERT INTO t " + rows);
                        // the entries of p2 to p8
                        Assert.assertEquals(224, BitmapIndexWriter.testScannedKeyBytes.get());
                    } finally {
                        BitmapIndexWriter.isScannedKeyBytesCounterEnabled = false;
                    }
                    execute("INSERT INTO ref " + rows);
                    assertIndexReadsAsRef(engine, sqlExecutionContext, "s", "'s1', 's7', 's20001', 'p1', 'p5', 'p9', 'n1'");
                    Assert.assertEquals("key append page " + keyAppendPageSize + ", " + partitionBy, 0, BitmapIndexWriter.testZeroedKeyBytes.get());
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    execute("DROP TABLE t");
                    execute("DROP TABLE ref");
                }
            }
        });
    }

    @Test
    public void testO3IntoOlderPartitionsWritesNoSkippedKeys() throws Exception {
        // An O3 commit that appends to older partitions opens the index of each through the file descriptors the O3
        // jobs pass, and adds an existing key, and a new key, dense in the first partition and sparse in the others.
        // The opens write nothing, and neither do the keys.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                execute("CREATE TABLE t (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("CREATE TABLE ref (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                // 6 day partitions with the symbols s0 to s999 each, in the first 12 hours of the day
                for (String table : new String[]{"t", "ref"}) {
                    execute("INSERT INTO " + table + " SELECT 's' || ((x - 1) % 1_000), ((x - 1) / 1_000 * 86_400_000_000 + (x - 1) % 1_000 * 40_000_000)::TIMESTAMP FROM long_sequence(6_000)");
                }
                drainWalQueue();

                // at hour 23 of the 5 older days: s7, and n0 to n4, whose keys grow with the day
                final StringBuilder rows = new StringBuilder("VALUES ");
                for (int day = 0; day < 5; day++) {
                    final long hour23 = day * 86_400_000_000L + 23 * 3_600_000_000L;
                    rows.append(day > 0 ? ", " : "")
                            .append("('s7', ").append(hour23).append("::TIMESTAMP), ")
                            .append("('n").append(day).append("', ").append(hour23 + 1_000_000).append("::TIMESTAMP)");
                }
                execute("INSERT INTO t " + rows);
                execute("INSERT INTO ref " + rows);
                drainWalQueue();
                assertSqlCursors("SELECT count() n FROM ref", "SELECT count() n FROM t");
                assertIndexReadsAsRef(engine, sqlExecutionContext, "s", "'s7', 's999', 'n0', 'n2', 'n4'");
                Assert.assertEquals(walMode, 0, BitmapIndexWriter.testZeroedKeyBytes.get());
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testParallelIndexingWritesNoSkippedKeys() throws Exception {
        // A commit of more rows than the parallel index threshold indexes all symbol columns but the last on the
        // ColumnIndexerJob workers, which add keys to the index writers the table writer opened.
        setProperty(PropertyKey.CAIRO_PARALLEL_INDEXING_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_PARALLEL_INDEX_THRESHOLD, 1);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4);
            executeWithPool(pool, (engine, compiler, context) -> {
                BitmapIndexWriter.testZeroedKeyBytes.set(0);
                engine.execute("CREATE TABLE t (s SYMBOL INDEX, s2 SYMBOL INDEX, s3 SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
                engine.execute("CREATE TABLE ref (s SYMBOL, s2 SYMBOL, s3 SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
                // every commit spans two days, and opens the index of the second
                for (int commit = 0; commit < 3; commit++) {
                    final String rows = "SELECT 's' || (x % (100 * " + (commit + 1) + ")), 'k' || (x * " + (commit + 1) + " % 997), 'm' || (1_000 - x % 1_000), "
                            + "(" + commit + " * 172_800_000_000 + x * 60_000_000)::TIMESTAMP FROM long_sequence(2_000)";
                    engine.execute("INSERT INTO t " + rows, context);
                    engine.execute("INSERT INTO ref " + rows, context);
                }
                for (String column : new String[]{"s", "s2", "s3"}) {
                    assertIndexReadsAsRef(engine, context, column, "'s1', 's150', 's299', 'k5', 'k996', 'm1', 'm1000'");
                }
                Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
            });
        });
    }

    @Test
    public void testPartitionSwitchWritesNoSkippedKeys() throws Exception {
        // In-order rows that move to a new day open the index of its partition, whose first key is a sparse one: the
        // symbol with the highest key so far. The opens write nothing, and neither do the keys.
        assertMemoryLeak(() -> {
            BitmapIndexWriter.testZeroedKeyBytes.set(0);
            execute("CREATE TABLE t (s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            for (String table : new String[]{"t", "ref"}) {
                execute("INSERT INTO " + table + " SELECT 's' || x, (x * 1_000_000)::TIMESTAMP FROM long_sequence(1_000)");
            }
            // days 1 to 3, each opened by the symbol with the highest key so far, then an old symbol and a new one
            for (int day = 1; day <= 3; day++) {
                final long midnight = day * 86_400_000_000L;
                final String rows = "VALUES ('s" + (1_000 + day - 1) + "', " + midnight + "::TIMESTAMP), "
                        + "('s" + (day * 7) + "', " + (midnight + 1) + "::TIMESTAMP), "
                        + "('s" + (1_000 + day) + "', " + (midnight + 2) + "::TIMESTAMP)";
                execute("INSERT INTO t " + rows);
                execute("INSERT INTO ref " + rows);
            }
            assertIndexReadsAsRef(engine, sqlExecutionContext, "s", "'s7', 's14', 's1000', 's1001', 's1003'");
            Assert.assertEquals(0, BitmapIndexWriter.testZeroedKeyBytes.get());
        });
    }

    // The rows the index of t finds, against the rows of ref, which holds the same rows without an index
    private static void assertIndexReadsAsRef(CairoEngine engine, SqlExecutionContext context, String column, String symbols) throws Exception {
        final String[] sqls = {
                "SELECT * FROM %s WHERE " + column + " IN (" + symbols + ") ORDER BY ts",
                "SELECT * FROM (SELECT * FROM %s LATEST ON ts PARTITION BY " + column + ") ORDER BY " + column,
                "SELECT * FROM (SELECT * FROM %s WHERE " + column + " IN (" + symbols + ") LATEST ON ts PARTITION BY " + column + ") ORDER BY " + column
        };
        for (String sql : sqls) {
            TestUtils.assertSqlCursors(engine, context, String.format(sql, "ref"), String.format(sql, "t"), LOG);
        }
        for (String symbol : symbols.split(", ")) {
            final String sql = "SELECT * FROM %s WHERE " + column + " = " + symbol;
            TestUtils.assertSqlCursors(engine, context, String.format(sql, "ref"), String.format(sql, "t"), LOG);
        }
    }
}
