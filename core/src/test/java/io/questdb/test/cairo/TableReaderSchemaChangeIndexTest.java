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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A pooled reader keeps the index readers of a partition it has closed, to reuse them when the partition opens
 * again. A schema change that moves or replaces an indexed column must hand each of them to the column it was made
 * for, or the column that takes its place reuses a reader of the wrong index type and the index scan fails. The
 * first partition is closed, or rewritten, before the reader sees the schema change; the second stays open.
 */
@RunWith(Parameterized.class)
public class TableReaderSchemaChangeIndexTest extends AbstractCairoTest {
    private final FirstPartition firstPartition;
    private final String otherIndexType;
    private final String startIndexType;
    private final String walClause;
    private final WalMode walMode;

    public TableReaderSchemaChangeIndexTest(WalMode walMode, FirstPartition firstPartition, String startIndexType, String otherIndexType) {
        this.walMode = walMode;
        this.firstPartition = firstPartition;
        this.startIndexType = startIndexType;
        this.otherIndexType = otherIndexType;
        this.walClause = walMode == WalMode.WITH_WAL ? " WAL" : " BYPASS WAL";
    }

    @Parameterized.Parameters(name = "{0} {1} {2}->{3}")
    public static Collection<Object[]> data() {
        final Collection<Object[]> data = new ArrayList<>();
        for (WalMode walMode : WalMode.values()) {
            for (FirstPartition firstPartition : FirstPartition.values()) {
                data.add(new Object[]{walMode, firstPartition, "POSTING", "BITMAP"});
                data.add(new Object[]{walMode, firstPartition, "BITMAP", "POSTING"});
            }
        }
        return data;
    }

    @Test
    public void testAddIndexedColumnAfterDroppingLastIndexedColumn() throws Exception {
        // The dropped column leaves its closed index readers behind in its slot, which the added column takes.
        assertMemoryLeak(() -> {
            createTable("ts TIMESTAMP, c INT, a SYMBOL INDEX TYPE " + startIndexType);
            insert("('2024-01-01T12:00:00', 1, 'x'), ('2024-01-02T12:00:00', 2, 'x')");
            prepareFirstPartition();
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");

            ddl("ALTER TABLE t DROP COLUMN a");
            assertQuery("SELECT ts, c FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tc
                            2024-01-01T12:00:00.000000Z\t1
                            2024-01-02T12:00:00.000000Z\t2
                            """);
            ddl("ALTER TABLE t ADD COLUMN b SYMBOL INDEX TYPE " + otherIndexType);
            // gives the new column data in both partitions, which rewrites them
            insert("('2024-01-01T06:00:00', 3, 'x'), ('2024-01-02T06:00:00', 4, 'x')");
            assertIndexScans("b", "2024-01-01T06:00:00.000000Z", "2024-01-02T06:00:00.000000Z");
        });
    }

    @Test
    public void testChangeIndexTypeOfColumn() throws Exception {
        // The column keeps its slot but becomes a new column instance with another index type.
        assertMemoryLeak(() -> {
            createTable("ts TIMESTAMP, a SYMBOL INDEX TYPE " + startIndexType);
            insert("('2024-01-01T12:00:00', 'x'), ('2024-01-02T12:00:00', 'x')");
            prepareFirstPartition();
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");

            closeOrRewriteFirstPartition("('2024-01-01T06:00:00', 'y')");
            ddl("ALTER TABLE t ALTER COLUMN a DROP INDEX");
            ddl("ALTER TABLE t ALTER COLUMN a ADD INDEX TYPE " + otherIndexType);
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
        });
    }

    @Test
    public void testDropColumnBeforeColumnsOfEveryKind() throws Exception {
        // Each column after c takes the slot of the column before it. The first partition reopens each one in the
        // memory that column left there: a variable-size column in a fixed-size column's and the other way round,
        // an array in a STRING's, a column with a column top in one without, and a column the partition lacks in a
        // column's it has.
        assertMemoryLeak(() -> {
            createTable("ts TIMESTAMP, c INT, v VARCHAR, i INT, s STRING, arr DOUBLE[], sym SYMBOL, a SYMBOL INDEX TYPE "
                    + startIndexType + ", l LONG");
            insert("('2024-01-01T12:00:00', 1, 'v1', 10, 's1', ARRAY[1.0, 2.0], 'p', 'x', 100)");
            // the first partition's first row predates the column
            ddl("ALTER TABLE t ADD COLUMN late LONG");
            insert("('2024-01-01T13:00:00', 2, 'v2', 20, 's2', ARRAY[3.0], 'q', 'z', 200, 5), "
                    + "('2024-01-02T12:00:00', 3, 'v3', 30, 's3', ARRAY[4.0, 5.0], 'q', 'x', 300, NULL)");
            // the first partition lacks the column
            ddl("ALTER TABLE t ADD COLUMN late2 VARCHAR");
            insert("('2024-01-02T13:00:00', 4, 'v4', 40, 's4', ARRAY[6.0], 'r', 'z', 400, 8, 'w')");
            prepareFirstPartition();
            // opens every partition and every column, so the first partition's slots hold the memory of each
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tc\tv\ti\ts\tarr\tsym\ta\tl\tlate\tlate2
                            2024-01-01T12:00:00.000000Z\t1\tv1\t10\ts1\t[1.0,2.0]\tp\tx\t100\tnull\t
                            2024-01-01T13:00:00.000000Z\t2\tv2\t20\ts2\t[3.0]\tq\tz\t200\t5\t
                            2024-01-02T12:00:00.000000Z\t3\tv3\t30\ts3\t[4.0,5.0]\tq\tx\t300\tnull\t
                            2024-01-02T13:00:00.000000Z\t4\tv4\t40\ts4\t[6.0]\tr\tz\t400\t8\tw
                            """);

            closeOrRewriteFirstPartition("('2024-01-01T06:00:00', 0, 'v0', 0, 's0', ARRAY[0.5], 'o', 'y', 0, 6, 'u')");
            ddl("ALTER TABLE t DROP COLUMN c");
            final String rewrittenRow = firstPartition == FirstPartition.REWRITTEN
                    ? "2024-01-01T06:00:00.000000Z\tv0\t0\ts0\t[0.5]\to\ty\t0\t6\tu\n"
                    : "";
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\ti\ts\tarr\tsym\ta\tl\tlate\tlate2\n"
                            + rewrittenRow
                            + "2024-01-01T12:00:00.000000Z\tv1\t10\ts1\t[1.0,2.0]\tp\tx\t100\tnull\t\n"
                            + "2024-01-01T13:00:00.000000Z\tv2\t20\ts2\t[3.0]\tq\tz\t200\t5\t\n"
                            + "2024-01-02T12:00:00.000000Z\tv3\t30\ts3\t[4.0,5.0]\tq\tx\t300\tnull\t\n"
                            + "2024-01-02T13:00:00.000000Z\tv4\t40\ts4\t[6.0]\tr\tz\t400\t8\tw\n");
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
        });
    }

    @Test
    public void testDropIndexedColumnBeforeIndexedColumn() throws Exception {
        // Dropping a SYMBOL column renumbers the symbol columns after it, so b becomes a new column instance in a's slot.
        assertMemoryLeak(() -> {
            createTable("ts TIMESTAMP, a SYMBOL INDEX TYPE " + startIndexType + ", b SYMBOL INDEX TYPE " + otherIndexType);
            insert("('2024-01-01T12:00:00', 'x', 'x'), ('2024-01-02T12:00:00', 'x', 'x')");
            prepareFirstPartition();
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
            assertIndexScans("b", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");

            closeOrRewriteFirstPartition("('2024-01-01T06:00:00', 'y', 'y')");
            ddl("ALTER TABLE t DROP COLUMN a");
            assertIndexScans("b", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
        });
    }

    @Test
    public void testDropNonSymbolColumnBeforeIndexedColumns() throws Exception {
        // Both indexed columns stay the same column instances and move one slot left, b into a's slot.
        assertMemoryLeak(() -> {
            createTable("ts TIMESTAMP, c INT, a SYMBOL INDEX TYPE " + startIndexType + ", b SYMBOL INDEX TYPE " + otherIndexType);
            insert("('2024-01-01T12:00:00', 1, 'x', 'x'), ('2024-01-02T12:00:00', 2, 'x', 'x')");
            prepareFirstPartition();
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
            assertIndexScans("b", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");

            closeOrRewriteFirstPartition("('2024-01-01T06:00:00', 3, 'y', 'y')");
            ddl("ALTER TABLE t DROP COLUMN c");
            assertIndexScans("a", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
            assertIndexScans("b", "2024-01-01T12:00:00.000000Z", "2024-01-02T12:00:00.000000Z");
        });
    }

    private void assertIndexScans(String column, String firstTs, String secondTs) throws Exception {
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x'")
                .noLeakCheck()
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: " + column)
                .returns("ts\t" + column + "\n" + firstTs + "\tx\n" + secondTs + "\tx\n");
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x' ORDER BY ts DESC")
                .noLeakCheck()
                .timestampDesc("ts")
                .withPlanContaining("Index backward scan on: " + column)
                .returns("ts\t" + column + "\n" + secondTs + "\tx\n" + firstTs + "\tx\n");
    }

    private void closeOrRewriteFirstPartition(String o3Row) throws Exception {
        if (firstPartition == FirstPartition.REWRITTEN) {
            // a row before the partition's only row merges into a new version of the partition
            insert(o3Row);
        }
        // otherwise the pooled reader closed the first partition when the last query returned it to the pool
    }

    private void createTable(String columns) throws Exception {
        if (firstPartition == FirstPartition.REWRITTEN) {
            // the server default: an out-of-order insert into a WAL table rewrites the partition, as it does
            // without WAL, instead of appending to it in place
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        } else {
            // the pooled reader keeps only the latest partition open while it sits in the pool
            node1.setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, 1);
        }
        execute("CREATE TABLE t (" + columns + ") TIMESTAMP(ts) PARTITION BY DAY" + walClause);
    }

    private void ddl(String sql) throws Exception {
        execute(sql);
        drainWalIfNeeded();
    }

    private void drainWalIfNeeded() {
        if (walMode == WalMode.WITH_WAL) {
            drainWalQueue();
        }
    }

    private void insert(String rows) throws Exception {
        execute("INSERT INTO t VALUES " + rows);
        drainWalIfNeeded();
    }

    private void prepareFirstPartition() throws Exception {
        if (firstPartition == FirstPartition.PARQUET_CLOSED) {
            ddl("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        }
    }

    public enum FirstPartition {
        // closed by the pooled reader when a query returns it to the pool
        CLOSED,
        // rewritten by an out-of-order insert, which the reader finds in the same reload as the schema change
        REWRITTEN,
        // a parquet partition, closed by the pooled reader when a query returns it to the pool
        PARQUET_CLOSED
    }
}
