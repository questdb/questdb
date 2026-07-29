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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * A COVERING posting index converted to Parquet and then back to native must
 * keep returning its covered values. {@code convertPartitionParquetToNative}
 * previously rebuilt the posting index as non-covering and dropped the covering
 * sidecars (.pci/.pc*), so a covering scan over the reconverted partition read
 * NULL for every covered column while a non-covering scan of the same data was
 * correct. The reconvert path now links the existing index files (including the
 * covering sidecars) across, mirroring the native->parquet direction.
 */
public class CoveringIndexParquetNativeRoundTripTest extends AbstractCairoTest {

    @Test
    public void testBitmapIndexSurvivesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX");
            drainWalQueue();

            roundTrip();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // Indexed scan resolves the same rows as a full scan of the same
            // predicate (the cast on the reference side defeats index usage).
            assertSqlCursors(
                    "SELECT ts, sym, price FROM t_rt WHERE sym = 'A0' ORDER BY ts",
                    "SELECT ts, sym, price FROM t_rt WHERE sym::string = 'A0' ORDER BY ts"
            );
            assertQuery("SELECT count() c, sum(price) s FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("c\ts\n25\t1300.0\n");
        });
    }

    @Test
    public void testColumnTopCoveringSurvivesConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_top (
                        ts TIMESTAMP,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_top
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), x::DOUBLE
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            // sym and tag get columnTop = 50 in 2024-01-01. price keeps
            // columnTop = 0, so the sidecar build covers both cases, over a
            // fixed-size and a var-size column.
            execute("ALTER TABLE t_top ADD COLUMN sym SYMBOL");
            execute("ALTER TABLE t_top ADD COLUMN tag VARCHAR");
            drainWalQueue();
            execute("""
                    INSERT INTO t_top
                    SELECT dateadd('m', (x + 100)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                           x::DOUBLE, 'A' || (x % 4), 'V' || (x % 4)
                    FROM long_sequence(50)
                    """);
            // A second partition keeps 2024-01-01 out of the active slot.
            execute("""
                    INSERT INTO t_top
                    SELECT dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                           x::DOUBLE, 'A' || (x % 4), 'V' || (x % 4)
                    FROM long_sequence(4)
                    """);
            execute("ALTER TABLE t_top ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            execute("ALTER TABLE t_top CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_top", true);

            // Control: the parquet data itself is intact, so any difference below
            // comes from the rebuilt index alone.
            assertQuery("SELECT /*+ no_covering */ sum(price) sum_price, count() c, first(tag) first_tag FROM t_top WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tc\tfirst_tag\n316.0\t13\tV0\n");
            assertQuery("SELECT sum(price) sum_price, count() c, first(tag) first_tag FROM t_top WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("sum_price\tc\tfirst_tag\n316.0\t13\tV0\n");
        });
    }

    @Test
    public void testHistoricPartitionMultiColumnRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hist (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_hist
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                           'A' || (x % 4), x::DOUBLE
                    FROM long_sequence(40)
                    """);
            drainWalQueue();
            // sym2 gets columnTop = 40 in 2024-01-01, then rows below it.
            execute("ALTER TABLE t_hist ADD COLUMN sym2 SYMBOL");
            execute("ALTER TABLE t_hist ADD COLUMN qty LONG");
            drainWalQueue();
            execute("""
                    INSERT INTO t_hist
                    SELECT dateadd('m', (x + 100)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                           'A' || (x % 4), x::DOUBLE, 'B' || (x % 3), x
                    FROM long_sequence(40)
                    """);
            // Two later partitions leave 2024-01-01 firmly historic.
            execute("""
                    INSERT INTO t_hist
                    SELECT dateadd('m', x::INT, '2024-01-0' || (2 + (x % 2))::INT || 'T00:00:00Z'::TIMESTAMP),
                           'A' || (x % 4), x::DOUBLE, 'B' || (x % 3), x
                    FROM long_sequence(10)
                    """);
            execute("ALTER TABLE t_hist ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            execute("ALTER TABLE t_hist ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (price)");
            drainWalQueue();

            execute("ALTER TABLE t_hist CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_hist", true);
            execute("ALTER TABLE t_hist CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_hist", false);

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_hist'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // Both covering indexes must still serve their covered columns, and
            // must agree with the same read taken without covering.
            assertQuery("SELECT count() c FROM t_hist WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    // 10 A0 rows in each of the two 2024-01-01 inserts, 2 more in
                    // the later-partition insert (x % 4 == 0 for x in 1..10).
                    .returns("c\n22\n");
            assertSqlCursors(
                    "SELECT /*+ no_covering */ sym, price, qty FROM t_hist WHERE sym = 'A0'",
                    "SELECT sym, price, qty FROM t_hist WHERE sym = 'A0'"
            );
            // sym2 needs its own plan pin: without it a covering regression that
            // hit only sym2 would leave both sides of the comparison below on the
            // same plain index scan, and this method would stay green on the
            // strength of the sym assertions alone.
            assertQuery("SELECT count() c FROM t_hist WHERE sym2 = 'B1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym2")
                    // x % 3 == 1 gives 14 of the 40 second-insert rows and 4 of
                    // the 10 later-partition rows ({1, 4, 7, 10}).
                    .returns("c\n18\n");
            assertSqlCursors(
                    "SELECT /*+ no_covering */ sym2, price FROM t_hist WHERE sym2 = 'B1'",
                    "SELECT sym2, price FROM t_hist WHERE sym2 = 'B1'"
            );
        });
    }

    @Test
    public void testMissingParquetKeyFileRebuildKeepsCovering() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            // A second partition keeps 2024-01-01 out of the active slot: the
            // writer holds the last partition's index files open, and Windows
            // refuses to unlink an open file.
            execute("""
                    INSERT INTO t_rt
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(4)
                    """);
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            execute("ALTER TABLE t_rt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rt", true);

            removeIndexFiles("sym.pk");

            execute("ALTER TABLE t_rt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rt", false);

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // Control: the decoded native data is intact, so any difference below
            // comes from the rebuilt index alone. 25 day-1 rows summing to 1300,
            // plus the single day-2 A0 row (price 4).
            assertQuery("SELECT /*+ no_covering */ sum(price) sum_price, count() c, first(tag) first_tag FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tc\tfirst_tag\n1304.0\t26\tV0\n");
            // The rebuilt index must serve the same values through a covering
            // scan. Covered fixed-size column first: it reads back silently
            // wrong rather than failing.
            assertQuery("SELECT sum(price) sum_price, count() c FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("sum_price\tc\n1304.0\t26\n");
            assertQuery("SELECT sum(price) sum_price, count() c, first(tag) first_tag FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("sum_price\tc\tfirst_tag\n1304.0\t26\tV0\n");
        });
    }

    @Test
    public void testMissingParquetValueFileFailsConversion() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("""
                    INSERT INTO t_rt
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(4)
                    """);
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            execute("ALTER TABLE t_rt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            removeIndexFiles("sym.pv");

            execute("ALTER TABLE t_rt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();

            assertQuery("SELECT suspended, errorMessage LIKE '%index files do not exist%' matched FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\tmatched\ntrue\ttrue\n");
        });
    }

    @Test
    public void testMultiKeyCoveringSurvivesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            roundTrip();

            // Multi-key (IN-list) covering scan, plus a row-by-row
            // covered-vs-uncovered cursor comparison. Both sides must keep the
            // sym predicate: with no WHERE, intrinsicModel.keyColumn is null, no
            // CoveringIndexRecordCursorFactory is ever constructed, the
            // no_covering hint is inert and the two sides compile to the same
            // plain page-frame scan.
            assertQuery("SELECT sum(price) sum_price, count() c FROM t_rt WHERE sym IN ('A0', 'A2')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym with: price")
                    .returns("sum_price\tc\n2550.0\t50\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2') ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2') ORDER BY ts"
            );
        });
    }

    @Test
    public void testO3IntoParquetThenReconvertKeepsCovering() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            // A second partition keeps 2024-01-01 out of the active slot.
            execute("""
                    INSERT INTO t_rt
                    SELECT
                        dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                        'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                    FROM long_sequence(4)
                    """);
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            execute("ALTER TABLE t_rt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rt", true);

            // O3 into the parquet partition: rewrites it and rebuilds the index
            // via a different producer than the convert path.
            execute("""
                    INSERT INTO t_rt VALUES
                        ('2024-01-01T00:00:30Z', 'A0', 1000.0, 'V0'),
                        ('2024-01-01T00:10:30Z', 'A2', 2000.0, 'V2')
                    """);
            drainWalQueue();
            assertPartitionFormat("t_rt", true);

            execute("ALTER TABLE t_rt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rt", false);

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // The O3 row must be present and every covered value must match the
            // same read taken without covering.
            assertQuery("SELECT count() c FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("c\n27\n");
            assertSqlCursors(
                    "SELECT /*+ no_covering */ sym, price, tag FROM t_rt WHERE sym = 'A0'",
                    "SELECT sym, price, tag FROM t_rt WHERE sym = 'A0'"
            );
            assertSqlCursors(
                    "SELECT /*+ no_covering */ sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2')",
                    "SELECT sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2')"
            );
        });
    }

    @Test
    public void testPostingCoveringSurvivesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertCovered("native (pre-parquet)");
            roundTrip();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // Control: the reconverted native data is intact regardless of the
            // covering index.
            assertQuery("SELECT /*+ no_covering */ sum(price) sum_price, first(tag) first_tag FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
            // The covering scan must agree (it returned NULL before the fix).
            assertCovered("native (post round-trip)");
        });
    }

    @Test
    public void testRepeatedRoundTripsKeepCovering() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            for (int i = 0; i < 3; i++) {
                roundTrip();
                assertCovered("round-trip #" + i);
            }
        });
    }

    @Test
    public void testRowlessPostingColumnSkippedOnReconvert() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rowless (
                        ts TIMESTAMP,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_rowless
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), x::DOUBLE
                    FROM long_sequence(30)
                    """);
            drainWalQueue();
            // columnTop for sym in 2024-01-01 is 30 == the partition row count:
            // the column exists but owns no row there.
            execute("ALTER TABLE t_rowless ADD COLUMN sym SYMBOL");
            drainWalQueue();
            execute("""
                    INSERT INTO t_rowless
                    SELECT dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),
                           x::DOUBLE, 'A' || (x % 4)
                    FROM long_sequence(8)
                    """);
            execute("ALTER TABLE t_rowless ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            drainWalQueue();

            execute("ALTER TABLE t_rowless CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rowless", true);

            execute("ALTER TABLE t_rowless CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_rowless", false);

            // The guard held: no "index files do not exist", table still live.
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rowless'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertQuery("SELECT count() c FROM t_rowless")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("c\n38\n");
            // The rows that do carry sym still read back through the covering index.
            assertQuery("SELECT count() c FROM t_rowless WHERE sym = 'A1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex on: sym")
                    .returns("c\n2\n");
            assertSqlCursors(
                    "SELECT /*+ no_covering */ sym, price FROM t_rowless WHERE sym = 'A1'",
                    "SELECT sym, price FROM t_rowless WHERE sym = 'A1'"
            );
        });
    }

    private void assertCovered(String stage) throws Exception {
        assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_rt WHERE sym = 'A0' -- " + stage)
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .withPlanContaining("CoveringIndex on: sym with: price, tag")
                .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
    }

    private void assertPartitionFormat(String tableName, boolean parquet) throws Exception {
        assertQuery("SELECT isParquet FROM table_partitions('" + tableName + "') WHERE name = '2024-01-01'")
                .noRandomAccess()
                .noLeakCheck()
                .returns("isParquet\n" + parquet + "\n");
    }

    private void createAndSeed() throws Exception {
        execute("""
                CREATE TABLE t_rt (
                    ts TIMESTAMP,
                    sym SYMBOL,
                    price DOUBLE,
                    tag VARCHAR
                ) TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
        execute("""
                INSERT INTO t_rt
                SELECT
                    dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                    'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                FROM long_sequence(100)
                """);
        drainWalQueue();
    }

    private void removeIndexFiles(String prefix) {
        final TableToken token = engine.verifyTableName("t_rt");
        final long partitionTs;
        final long nameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
            nameTxn = reader.getTxFile().getPartitionNameTxn(0);
        }
        // Release pooled readers so no mapping keeps the file alive on Windows.
        engine.releaseInactive();

        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
            final File[] files = new File(path.toString()).listFiles((_, name) -> name.startsWith(prefix));
            Assert.assertNotNull(files);
            Assert.assertTrue("no index file matching " + prefix + " under " + path, files.length > 0);
            for (File file : files) {
                Assert.assertTrue("could not remove " + file, file.delete());
            }
        }
    }

    private void roundTrip() throws Exception {
        execute("ALTER TABLE t_rt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        assertPartitionFormat("t_rt", true);
        execute("ALTER TABLE t_rt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
        drainWalQueue();
        assertPartitionFormat("t_rt", false);
    }
}
