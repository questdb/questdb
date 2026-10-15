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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    // one column of each type a covering index stores, added to t_cov after s
    private static final String[] TXN_ZERO_INCLUDE_COLUMNS = {
            "d DOUBLE", "i INT", "l LONG", "vc VARCHAR", "st STRING", "ch CHAR", "bo BOOLEAN", "bt BYTE", "sh SHORT", "u UUID", "ip IPv4",
            "l256 LONG256", "g GEOHASH(4c)", "d18 DECIMAL(18, 2)", "d38 DECIMAL(38, 4)", "sy SYMBOL", "tn TIMESTAMP_NS"
    };

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
    public void testConvertToNativeCoveredTopChangesLinkedIndex() throws Exception {
        // s exists from the start (top 0), d is added after 5 of 8 rows. With row groups of 4 rows, CONVERT
        // TO NATIVE gives d top 4 and links the covering index of s, whose top does not change. The covered
        // values of d then read through the native column with its new top, also after an insert into
        // the partition, the last one, whose seal reads the covered tops from _cv.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        final ConcurrentLinkedQueue<String> linkedFileNames = new ConcurrentLinkedQueue<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                final String path = Utf8s.stringFromUtf8Bytes(src);
                linkedFileNames.add(path.substring(path.lastIndexOf(Files.SEPARATOR) + 1));
                return super.hardLink(src, hardLink);
            }
        };
        assertMemoryLeak(ff, () -> {
            // t_twin gets the same statements without the CONVERT
            for (String table : new String[]{"t_cov", "t_twin"}) {
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + table + " SELECT dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), CASE WHEN x % 2 = 1 THEN 'x' ELSE 'y' END FROM long_sequence(5)");
                execute("ALTER TABLE " + table + " ADD COLUMN d DOUBLE");
                execute("""
                        INSERT INTO {t} VALUES ('2024-01-01T05:00:00.000000Z', 'x', 5.5), ('2024-01-01T06:00:00.000000Z', 'y', 6.5),
                        ('2024-01-01T07:00:00.000000Z', 'x', 7.5)""".replace("{t}", table));
                execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (d)");
            }
            drainWalQueue();
            execute("ALTER TABLE t_cov CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_cov", true);
            linkedFileNames.clear();
            execute("ALTER TABLE t_cov CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_cov", false);
            Assert.assertTrue(linkedFileNames.toString(), linkedFileNames.contains("s.pk") || linkedFileNames.stream().anyMatch(name -> name.startsWith("s.pk.")));
            Assert.assertEquals(0, getColumnTop("t_cov", "s"));
            Assert.assertEquals(4, getColumnTop("t_cov", "d"));
            assertQuery("SELECT ts, d FROM t_cov WHERE s = 'x'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            ts\td
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T04:00:00.000000Z\tnull
                            2024-01-01T05:00:00.000000Z\t5.5
                            2024-01-01T07:00:00.000000Z\t7.5
                            """);
            assertCoveredMatchesTwin("converted");

            for (String table : new String[]{"t_cov", "t_twin"}) {
                execute("INSERT INTO " + table + " VALUES ('2024-01-01T08:00:00.000000Z', 'x', 8.5), ('2024-01-01T09:00:00.000000Z', 'y', NULL)");
            }
            drainWalQueue();
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_cov'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            assertCoveredMatchesTwin("after insert");
            assertQuery("SELECT ts, d FROM t_cov WHERE s = 'x' AND ts > '2024-01-01T04:00:00.000000Z'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\td
                            2024-01-01T05:00:00.000000Z\t5.5
                            2024-01-01T07:00:00.000000Z\t7.5
                            2024-01-01T08:00:00.000000Z\t8.5
                            """);
        });
    }

    @Test
    public void testConvertToParquetLinkFailureTxnZero() throws Exception {
        // The hard link of a .pc file of s, whose column name txn is 0, fails: CONVERT TO PARQUET
        // fails with it and leaves the partition native, with its covering index intact.
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                final String path = Utf8s.stringFromUtf8Bytes(src);
                if (path.contains(Files.SEPARATOR + "s.pc") && !path.contains(".pci")) {
                    return -1;
                }
                return super.hardLink(src, hardLink);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTxnZeroTables("BYPASS WAL");
            final String[] partitionDirsBefore = listPartitionDirs("t_cov");
            try {
                execute("ALTER TABLE t_cov CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                Assert.fail("CONVERT TO PARQUET must fail when it cannot link a covering sidecar of s");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not create hard link");
                TestUtils.assertContains(e.getFlyweightMessage(), "s.pc");
            }
            assertPartitionFormat("t_cov", false);
            Assert.assertArrayEquals(partitionDirsBefore, listPartitionDirs("t_cov"));
            assertTxnZeroCoveredMatchesTwin("after the failed convert");
        });
    }

    @Test
    public void testCoveredValuesSurviveConvertWhenSymbolNameTxnIsZero() throws Exception {
        // s is the first column added to the empty table t_cov, so its column name txn is 0 and its
        // .pc files are named s.pc<N>.0.<C>.<S>, the token that column name txn NONE also writes. The
        // twin declares s in CREATE TABLE (txn NONE) and is never converted. CONVERT PARTITION in
        // both directions links the .pc files of s along with the rest of its index.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                final String context = walMode;
                execute("DROP TABLE IF EXISTS t_cov");
                execute("DROP TABLE IF EXISTS t_twin");
                createTxnZeroTables(walMode);
                Assert.assertEquals(context, 0, getColumnNameTxn("t_cov", "s"));
                Assert.assertEquals(context, TableUtils.COLUMN_NAME_TXN_NONE, getColumnNameTxn("t_twin", "s"));
                assertTxnZeroCoveredMatchesTwin(context + ", native");

                execute("ALTER TABLE t_cov CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
                drainWalQueue();
                assertPartitionFormat("t_cov", true);
                // one .pc file per INCLUDE column and the designated timestamp, linked into the parquet partition
                Assert.assertEquals(context, TXN_ZERO_INCLUDE_COLUMNS.length + 1, countCoverDataFiles("t_cov", 0));
                assertTxnZeroCoveredMatchesTwin(context + ", parquet");

                execute("ALTER TABLE t_cov CONVERT PARTITION TO NATIVE LIST '2024-01-01', '2024-01-02'");
                drainWalQueue();
                assertPartitionFormat("t_cov", false);
                Assert.assertEquals(context, TXN_ZERO_INCLUDE_COLUMNS.length + 1, countCoverDataFiles("t_cov", 0));
                assertTxnZeroCoveredMatchesTwin(context + ", native again");

                // readers reopen every file from disk
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                assertTxnZeroCoveredMatchesTwin(context + ", reopened");
                assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_cov'")
                        .noRandomAccess()
                        .noLeakCheck()
                        .returns(walMode.equals("WAL") ? "suspended\nfalse\n" : "suspended\n");
            }
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
                    "SELECT sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2')",
                    "SELECT /*+ no_covering */ sym, price, tag FROM t_rt WHERE sym IN ('A0', 'A2')"
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
    public void testParquetPartitionPredatingIndexedColumnServesNoFabricatedValues() throws Exception {
        // A Parquet partition that predates the indexed column keeps its full column
        // top (TableWriter converts it with zeroAllColumns=false), so the reader sees
        // the column as absent there. The posting chain holds no posting for a row
        // below a top, so the sidecar has nothing to decode for the NULL key -- the one
        // combination the covering scan cannot answer. The factory hands that open to
        // its backup plan, which is the ordinary reader path and already decodes
        // Parquet, so the partition's rows come back carrying their real timestamps and
        // prices. The regression this pins is the alternative: rows fabricated from
        // NULL sentinels, designated timestamp included.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pq_absent (
                        ts TIMESTAMP,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // 2024-01-01 and 2024-01-02 both predate sym. 2024-01-02 is the active
            // partition when ADD COLUMN runs, so it gets a sym column top and index
            // files; 2024-01-01 is already historic and gets no sym files at all.
            execute("""
                    INSERT INTO t_pq_absent
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), x::DOUBLE
                    FROM long_sequence(30)
                    """);
            execute("""
                    INSERT INTO t_pq_absent
                    SELECT dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP), (100 + x)::DOUBLE
                    FROM long_sequence(10)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_pq_absent ADD COLUMN sym SYMBOL");
            drainWalQueue();
            execute("""
                    INSERT INTO t_pq_absent
                    SELECT dateadd('m', x::INT, '2024-01-03T00:00:00Z'::TIMESTAMP), x::DOUBLE, 'A' || (x % 4)
                    FROM long_sequence(8)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_pq_absent ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            drainWalQueue();
            execute("ALTER TABLE t_pq_absent CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("t_pq_absent", true);
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // Both partitions that predate sym are served: 30 Parquet rows plus 10
            // native ones. Every row carries its real timestamp and price, so
            // count(ts) and count(price) both equal count().
            assertQuery("SELECT count() c, count(ts) nn_ts, count(price) nn_price, min(ts) min_ts, sum(price) s FROM t_pq_absent WHERE sym = null")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .withPlanContaining("CoveringIndex backup: true on: sym with: ts, price")
                    .returns("""
                            c\tnn_ts\tnn_price\tmin_ts\ts
                            40\t40\t40\t2024-01-01T00:01:00.000000Z\t1520.0
                            """);
            assertQuery("SELECT ts, price FROM t_pq_absent WHERE sym = null ORDER BY ts LIMIT 3")
                    .timestamp("ts")
                    .noRandomAccess()
                    // The backup is a PageFrameRecordCursorFactory: it declares no random
                    // access but its cursor implements getRecordB() anyway.
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("""
                            ts\tprice
                            2024-01-01T00:01:00.000000Z\t1.0
                            2024-01-01T00:02:00.000000Z\t2.0
                            2024-01-01T00:03:00.000000Z\t3.0
                            """);

            // The oracle the covered scan must match: the same query with covering off.
            assertQuery("SELECT /*+ no_covering */ count() c, count(ts) nn_ts, sum(price) s FROM t_pq_absent WHERE sym = null")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            c\tnn_ts\ts
                            40\t40\t1520.0
                            """);
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

    private static int countCoverDataFiles(String tableName, int partitionIndex) {
        final TableToken token = engine.verifyTableName(tableName);
        final long partitionTs;
        final long nameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(partitionIndex);
            nameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxn);
            final String[] names = new File(path.toString()).list((_, name) -> name.startsWith("s.pc") && !name.startsWith("s.pci"));
            Assert.assertNotNull(names);
            return names.length;
        }
    }

    /**
     * Creates t_cov, where s is the first column added to the empty table (column name txn 0), and
     * t_twin, which declares every column in CREATE TABLE. Both get the same covering index and rows:
     * partition 2024-01-01 has more rows than one parquet row group, NULL, empty and repeated keys;
     * in 2024-01-02 s is NULL in every row; 2024-01-03 is the last partition.
     */
    private static void createTxnZeroTables(String walMode) throws Exception {
        execute("CREATE TABLE t_cov (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("ALTER TABLE t_cov ADD COLUMN s SYMBOL");
        for (String column : TXN_ZERO_INCLUDE_COLUMNS) {
            execute("ALTER TABLE t_cov ADD COLUMN " + column);
        }
        execute("CREATE TABLE t_twin (v INT, ts TIMESTAMP, s SYMBOL, " + String.join(", ", TXN_ZERO_INCLUDE_COLUMNS)
                + ") TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        final StringBuilder include = new StringBuilder();
        for (String column : TXN_ZERO_INCLUDE_COLUMNS) {
            include.append(include.isEmpty() ? "" : ", ").append(column, 0, column.indexOf(' '));
        }
        for (String table : new String[]{"t_cov", "t_twin"}) {
            execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (" + include + ")");
            final StringBuilder rows = new StringBuilder();
            final String[] keys = {"x", "y", null, "", "x", "y", "x", null, "x"};
            for (int v = 1; v <= keys.length; v++) {
                rows.append(v > 1 ? ", " : "").append(txnZeroRow(v, "2024-01-01T0" + v + ":00:00.000000Z", keys[v - 1]));
            }
            for (int v = 11; v <= 13; v++) {
                rows.append(", ").append(txnZeroRow(v, "2024-01-02T0" + (v - 10) + ":00:00.000000Z", null));
            }
            rows.append(", ").append(txnZeroRow(21, "2024-01-03T01:00:00.000000Z", "x"));
            execute("INSERT INTO " + table + " VALUES " + rows);
        }
        drainWalQueue();
    }

    private static long getColumnNameTxn(String tableName, String columnName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnNameTxn(reader.getPartitionTimestampByIndex(0), writerIndex);
        }
    }

    private static String[] listPartitionDirs(String tableName) {
        final String[] names = new File(configuration.getDbRoot().toString(), engine.verifyTableName(tableName).getDirName())
                .list((_, name) -> name.startsWith("2024-"));
        Assert.assertNotNull(names);
        Arrays.sort(names);
        return names;
    }

    /**
     * A row of t_cov and t_twin with a value derived from {@code v} in every INCLUDE column, or NULL in
     * all of them when {@code s} is null.
     */
    private static String txnZeroRow(int v, String timestamp, String s) {
        if (s == null) {
            return "(" + v + ", '" + timestamp + "', NULL" + ", NULL".repeat(TXN_ZERO_INCLUDE_COLUMNS.length) + ")";
        }
        return "(" + v + ", '" + timestamp + "', '" + s + "', " + v + ".5, " + v + ", " + v + "000000000, 'vc" + v + "', 'st\u00e9" + v + "', 'q', true, "
                + v + "::BYTE, " + v + "::SHORT, '00000000-0000-0000-0000-" + String.format("%012d", v) + "', '10.0.0." + v + "', '0x"
                + String.format("%02x", v) + "'::LONG256, #9v1s, '" + v + ".25'::DECIMAL(18, 2), '" + v + ".1234'::DECIMAL(38, 4), 'k"
                + (v % 3) + "', '2024-01-01T00:00:00." + String.format("%09d", v) + "Z'::TIMESTAMP_NS)";
    }

    /**
     * The covering scans of t_cov return what the never-converted twin returns, and what a scan of t_cov
     * that bypasses the index returns.
     */
    private void assertTxnZeroCoveredMatchesTwin(String context) throws Exception {
        final StringBuilder projection = new StringBuilder("ts, s");
        for (String column : TXN_ZERO_INCLUDE_COLUMNS) {
            projection.append(", ").append(column, 0, column.indexOf(' '));
        }
        assertQuery("SELECT " + projection + " FROM t_cov WHERE s = 'x'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .timestamp("ts")
                .withPlanContaining("CoveringIndex on: s with: ts, d, i, l, vc, st, ch, bo, bt, sh, u, ip, l256, g, d18, d38, sy, tn")
                .returns(txnZeroExpectedRowsOfX());
        for (String where : new String[]{
                "s = 'x'",
                "s = 'y'",
                "s = ''",
                "s IS NULL",
                "s IN ('x', 'y')",
                "s = 'x' ORDER BY ts DESC",
                "s = 'x' LATEST ON ts PARTITION BY s",
                "s = 'x' AND ts IN '2024-01-01'"
        }) {
            final String sql = "SELECT " + projection + " FROM {t} WHERE " + where;
            try {
                assertSqlCursors(sql.replace("{t}", "t_twin"), sql.replace("{t}", "t_cov"));
                assertSqlCursors(sql.replace("{t}", "t_cov").replace("WHERE s ", "WHERE s::VARCHAR "), sql.replace("{t}", "t_cov"));
            } catch (AssertionError e) {
                throw new AssertionError(context + ": " + sql, e);
            }
        }
    }

    private static String txnZeroExpectedRowsOfX() {
        return """
                ts\ts\td\ti\tl\tvc\tst\tch\tbo\tbt\tsh\tu\tip\tl256\tg\td18\td38\tsy\ttn
                """
                + txnZeroExpectedRow(1, "2024-01-01T01:00:00.000000Z")
                + txnZeroExpectedRow(5, "2024-01-01T05:00:00.000000Z")
                + txnZeroExpectedRow(7, "2024-01-01T07:00:00.000000Z")
                + txnZeroExpectedRow(9, "2024-01-01T09:00:00.000000Z")
                + txnZeroExpectedRow(21, "2024-01-03T01:00:00.000000Z");
    }

    private static String txnZeroExpectedRow(int v, String timestamp) {
        return timestamp + "\tx\t" + v + ".5\t" + v + "\t" + v + "000000000\tvc" + v + "\tst\u00e9" + v + "\tq\ttrue\t" + v + "\t" + v
                + "\t00000000-0000-0000-0000-" + String.format("%012d", v) + "\t10.0.0." + v + "\t0x" + String.format("%02x", v)
                + "\t9v1s\t" + v + ".25\t" + v + ".1234\tk" + (v % 3) + "\t2024-01-01T00:00:00." + String.format("%09d", v) + "Z\n";
    }

    private void assertCovered(String stage) throws Exception {
        assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_rt WHERE sym = 'A0' -- " + stage)
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .withPlanContaining("CoveringIndex on: sym with: price, tag")
                .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
    }

    private static long getColumnTop(String tableName, String columnName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(0), columnIndex);
        }
    }

    /**
     * The covering scans of t_cov return what the twin returns, and what a scan that reads d from the
     * column returns.
     */
    private void assertCoveredMatchesTwin(String context) throws Exception {
        for (String sql : new String[]{
                "SELECT ts, d FROM {t} WHERE s = 'x'",
                "SELECT ts, d FROM {t} WHERE s = 'y'",
                "SELECT ts, s, d FROM {t} WHERE s IN ('x', 'y')",
                "SELECT count() c, sum(d) sd FROM {t} WHERE s = 'x'"
        }) {
            try {
                assertSqlCursors(sql.replace("{t}", "t_twin"), sql.replace("{t}", "t_cov"));
                assertSqlCursors(sql.replace("SELECT ", "SELECT /*+ no_covering */ ").replace("{t}", "t_cov"), sql.replace("{t}", "t_cov"));
            } catch (AssertionError e) {
                throw new AssertionError(context + ": " + sql, e);
            }
        }
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
