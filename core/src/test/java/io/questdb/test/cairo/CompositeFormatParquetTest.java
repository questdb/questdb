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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * An all-parquet composite table -- {@code FORMAT PARQUET}, where every partition is born parquet
 * rather than converted -- against its plain {@code FORMAT PARQUET} twin.
 * <p>
 * This is the audit the writer-side gate's own comment asked for. The per-cell parquet machinery
 * exists and is well covered (CONVERT per cell, O3 into a parquet cell, dedup into a parquet cell,
 * cold storage over parquet cells), and the fresh-parquet write path was made cell-aware alongside
 * it; what had never been checked is whether a table where EVERY partition starts as parquet behaves,
 * which is a different lifecycle from converting a native partition after the fact.
 * <p>
 * The oracle throughout is the plain twin over identical rows, so a divergence can only come from the
 * cell split. Suspension is asserted separately from row content: a suspended composite table and a
 * correct one look alike to a comparison that only reads what it can see.
 */
public class CompositeFormatParquetTest extends AbstractCairoTest {

    private static final String GENERATOR = "SELECT"
            + " timestamp_sequence(1672531200000000L, 3600000000L) ts,"
            + " rnd_symbol('E0','E1','E2') exch, rnd_int() c_int, rnd_double() c_double,"
            + " rnd_varchar(4, 12, 1) c_varchar"
            + " FROM long_sequence(240)";

    /**
     * The plainest thing that can be asked of it: create, load, read back.
     */
    @Test
    public void testFreshParquetLoadMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            assertNotSuspended("c");
            assertTwins("after a fresh all-parquet load");
        });
    }

    /**
     * An out-of-order merge into a day that is already parquet, per cell.
     */
    @Test
    public void testO3MergeIntoFreshParquetMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src WHERE ts >= '2023-01-04'");
            execute("INSERT INTO p SELECT * FROM src WHERE ts >= '2023-01-04'");
            drainWalQueue();

            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-04' ORDER BY c_int");
            execute("INSERT INTO p SELECT * FROM src WHERE ts < '2023-01-04' ORDER BY c_int");
            drainWalQueue();

            assertNotSuspended("c");
            assertTwins("after an out-of-order merge into born-parquet cells");
        });
    }

    /**
     * Partition lifecycle over born-parquet cells.
     */
    @Test
    public void testDropPartitionOnFreshParquetMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            execute("ALTER TABLE c DROP PARTITION LIST '2023-01-02'");
            execute("ALTER TABLE p DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            assertNotSuspended("c");
            assertTwins("after dropping a born-parquet day");
        });
    }

    /**
     * DEDUP over born-parquet cells: an upsert has to merge into a parquet cell, not a native one.
     */
    @Test
    public void testDedupUpsertOnFreshParquetMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (" + GENERATOR + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_double DOUBLE, c_varchar VARCHAR)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL FORMAT PARQUET DEDUP UPSERT KEYS(ts, exch)");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_double DOUBLE, c_varchar VARCHAR)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL FORMAT PARQUET DEDUP UPSERT KEYS(ts, exch)");
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            assertNotSuspended("c");
            assertTwins("after a dedup upsert into born-parquet cells");
        });
    }

    /**
     * Converting born-parquet cells back to native, per cell.
     */
    @Test
    public void testConvertFreshParquetToNativeMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            execute("ALTER TABLE c CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            assertNotSuspended("c");
            assertTwins("after converting a born-parquet day back to native");
        });
    }

    /**
     * Column DDL over born-parquet cells.
     */
    @Test
    public void testAddColumnOnFreshParquetMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            execute("ALTER TABLE c ADD COLUMN added LONG");
            execute("ALTER TABLE p ADD COLUMN added LONG");
            drainWalQueue();

            execute("INSERT INTO c SELECT ts, exch, c_int, c_double, c_varchar, c_int FROM src WHERE ts < '2023-01-02'");
            execute("INSERT INTO p SELECT ts, exch, c_int, c_double, c_varchar, c_int FROM src WHERE ts < '2023-01-02'");
            drainWalQueue();

            assertNotSuspended("c");
            assertTwins("after ADD COLUMN over born-parquet cells");
        });
    }

    private void assertNotSuspended(String table) throws Exception {
        try (RecordCursorFactory factory = select("SELECT name, suspended, errorMessage FROM wal_tables()");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                final CharSequence name = cursor.getRecord().getStrA(0);
                if (name != null && io.questdb.std.Chars.equals(table, name)) {
                    Assert.assertFalse(
                            "table " + table + " suspended: " + cursor.getRecord().getStrA(2),
                            cursor.getRecord().getBool(1)
                    );
                    return;
                }
            }
        }
        Assert.fail("table " + table + " not found in wal_tables()");
    }

    private void assertTwins(String when) throws Exception {
        try {
            assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
        } catch (AssertionError e) {
            throw new AssertionError("composite and plain FORMAT PARQUET twins diverged " + when, e);
        }
    }

    private void createTwins() throws Exception {
        execute("CREATE TABLE src AS (" + GENERATOR + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_double DOUBLE, c_varchar VARCHAR)"
                + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL FORMAT PARQUET");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_double DOUBLE, c_varchar VARCHAR)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL FORMAT PARQUET");
    }
}
