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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join across the storage modes of its inputs and the lifecycle of a compiled
 * factory: native, Parquet and mixed partitions on either side and in either orientation of the
 * runtime input swap, WAL and non-WAL tables, indexed symbols, and one factory reused while its
 * tables gain rows, lose them to TRUNCATE and change their columns, which compiles the query again.
 * Every result compares with the ordinary plan of the same SQL, in order.
 */
public class AsyncHashJoinModesTest extends AbstractCairoTest {
    // Probe-side and build-side shapes over t and d, which both storage tests create: an INT key
    // whose matches carry var-size payloads, a translated SYMBOL key whose INNER join may swap its
    // inputs, stolen filters on both inputs with a residual over a var-size payload, a staged LONG
    // key under a backward probe scan, a post-join filter, and an INNER join whose left input is the
    // smaller one, which hashes the left input and streams the right one.
    // The shapes name the tables #t and #d.
    private static final String[] STORAGE_SHAPES = {
            "SELECT t.k, t.v, t.ts, d.w, d.s, d.vc, d.str FROM #t t LEFT JOIN #d d ON t.k = d.k",
            "SELECT t.v, t.sym, d.w, d.s, d.vc FROM #t t JOIN #d d ON t.sym = d.s",
            "SELECT t.v, d.w, d.vc FROM (SELECT * FROM #t WHERE v % 3 <> 0) t LEFT JOIN (SELECT * FROM #d WHERE w > 3) d ON t.k = d.k AND d.vc <> t.sym",
            "SELECT t.v, t.ts, d.w, d.str FROM #t t LEFT JOIN #d d ON t.lk = d.lk ORDER BY t.ts DESC",
            "SELECT t.v, d.w, d.vc FROM #t t LEFT JOIN #d d ON t.k = d.k WHERE d.vc IS NULL OR d.w > t.v % 7",
            "SELECT d.w, d.vc, t.v, t.sym FROM #d d JOIN #t t ON d.k = t.k",
    };
    private static final String[] STORAGES = {"native", "mixed", "parquet"};

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        // Several row groups, so several Parquet frames, per partition.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testFactoryReuseAcrossInsertTruncateAndAlter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, sym SYMBOL, spare INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE d (k INT, w INT, s SYMBOL, dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY HOUR");
            insertProbe(40, "2024-01-01", "sym");
            insertBuild(10, "2024-01-01");
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // The INNER join swaps its inputs while t holds fewer rows than d; the LEFT join never does.
            final String inner = "SELECT t.k, t.v, t.sym, d.w, d.s FROM t JOIN d ON t.k = d.k";
            final String left = "SELECT t.k, t.v, t.sym, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k";
            RecordCursorFactory innerFactory = select(inner);
            RecordCursorFactory leftFactory = select(left);
            try {
                assertExecution(innerFactory, inner, false);
                assertExecution(leftFactory, left, false);
                // d outgrows t: the INNER join hashes t from now on, without a new compile.
                insertBuild(100, "2024-01-01T01");
                assertExecution(innerFactory, inner, true);
                assertExecution(leftFactory, left, false);
                // An empty d reports a size of zero, which the swap rule does not swap by.
                execute("TRUNCATE TABLE d");
                assertExecution(innerFactory, inner, false);
                assertExecution(leftFactory, left, false);
                Assert.assertEquals("", rows(result(innerFactory, sqlExecutionContext)));
                insertBuild(5, "2024-01-02");
                execute("TRUNCATE TABLE t");
                assertExecution(innerFactory, inner, false);
                assertExecution(leftFactory, left, false);
                Assert.assertEquals("", rows(result(leftFactory, sqlExecutionContext)));
                insertProbe(3, "2024-01-02", "sym");
                assertExecution(innerFactory, inner, true);
                assertExecution(leftFactory, left, false);
                // A changed column list makes the cached factories stale, and a new compile reads the
                // same rows.
                for (String alter : new String[]{
                        "ALTER TABLE d ADD COLUMN extra INT",
                        "ALTER TABLE t DROP COLUMN spare",
                        "ALTER TABLE d ALTER COLUMN w TYPE LONG",
                        "ALTER TABLE t RENAME COLUMN sym TO sym2",
                }) {
                    execute(alter);
                    final boolean isRenamed = alter.contains("RENAME");
                    final String innerSql = isRenamed ? inner.replace("t.sym", "t.sym2") : inner;
                    final String leftSql = isRenamed ? left.replace("t.sym", "t.sym2") : left;
                    innerFactory = recompile(innerFactory, innerSql, alter);
                    leftFactory = recompile(leftFactory, leftSql, alter);
                    assertExecution(innerFactory, innerSql, true);
                    assertExecution(leftFactory, leftSql, false);
                    // Both orientations stay reachable after the new compile.
                    insertBuild(1, "2024-01-03");
                }
                insertProbe(50, "2024-01-03", "sym2");
                final String innerSql = inner.replace("t.sym", "t.sym2");
                assertExecution(innerFactory, innerSql, false);
                assertExecution(leftFactory, left.replace("t.sym", "t.sym2"), false);
            } finally {
                innerFactory.close();
                leftFactory.close();
            }
        });
    }

    @Test
    public void testIndexedSymbols() throws Exception {
        assertMemoryLeak(() -> {
            for (String index : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                execute("DROP TABLE IF EXISTS t");
                execute("DROP TABLE IF EXISTS d");
                execute("CREATE TABLE t (k INT, v LONG, sym SYMBOL " + index + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
                execute("CREATE TABLE d (k INT, w LONG, s SYMBOL " + index + ", dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY HOUR");
                execute("INSERT INTO t SELECT (x % 7)::INT, x, CASE WHEN x % 11 = 0 THEN NULL ELSE 's' || (x % 5) END, timestamp_sequence('2024-01-01', 600_000_000) FROM long_sequence(60)");
                execute("INSERT INTO d SELECT (x % 5)::INT, x, CASE WHEN x % 9 = 0 THEN NULL ELSE 's' || (x % 4) END, timestamp_sequence('2024-01-01', 900_000_000) FROM long_sequence(25)");
                sqlExecutionContext.changePageFrameSizes(4, 4);
                // The indexed columns as keys and as payloads: the scans read the frames, not the index.
                assertParallelHashJoin("SELECT t.v, t.sym, d.w, d.s FROM t LEFT JOIN d ON t.sym = d.s", sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, t.sym, d.w, d.s FROM t JOIN d ON t.sym = d.s", sqlExecutionContext);
                assertParallelHashJoin("SELECT t.v, t.sym, d.w, d.s FROM t LEFT JOIN d ON t.k = d.k ORDER BY t.ts DESC", sqlExecutionContext);
                // A filter on an indexed symbol that the plan reads through the index exposes no page
                // frames, so the input keeps the ordinary plan, as it keeps it for the fused GROUP BY.
                assertOrdinaryPlan("SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k WHERE t.sym = 's1'", sqlExecutionContext);
                assertOrdinaryPlan("SELECT t.v, d.w FROM t LEFT JOIN (SELECT * FROM d WHERE s = 's2') d ON t.k = d.k", sqlExecutionContext);
                // A filter on another column scans the frames and keeps the operator.
                assertParallelHashJoin("SELECT t.v, d.w, d.s FROM t LEFT JOIN (SELECT * FROM d WHERE w % 2 = 0) d ON t.sym = d.s WHERE t.v > 5", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testNativeParquetAndMixedPartitions() throws Exception {
        assertMemoryLeak(() -> {
            for (String storage : STORAGES) {
                createStorageTables(storage);
            }
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // Serial builds, builds in rounds, and builds in rounds whose payload the probe reads from
            // a copy rather than from the build's frames.
            for (int buildMode = 0; buildMode < 3; buildMode++) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, buildMode == 0 ? Long.MAX_VALUE : 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 8);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, buildMode == 2 ? "0" : "1000000");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
                for (String probe : STORAGES) {
                    for (String build : STORAGES) {
                        for (String shape : STORAGE_SHAPES) {
                            final String sql = shape.replace("#t", "t_" + probe).replace("#d", "d_" + build);
                            // The probe tasks would read d.vc, a VARCHAR, for the ON residual and the
                            // post-join WHERE, from a build with Parquet frames, through a share of the
                            // decode budget: such a join keeps the ordinary plan.
                            if (!build.equals("native") && (shape.contains("AND d.vc <> ") || shape.contains("WHERE d.vc "))) {
                                assertOrdinaryPlan(sql, sqlExecutionContext);
                            } else {
                                assertParallelHashJoin(sql, sqlExecutionContext);
                            }
                        }
                    }
                }
            }
            // The INNER join of a smaller left input hashes that input, whatever its storage.
            for (String storage : STORAGES) {
                try (RecordCursorFactory factory = select("SELECT d.w, t.v FROM d_" + storage + " d JOIN t_" + storage + " t ON d.k = t.k")) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(storage, join.getAtom().isSwapped());
                    }
                }
            }
        });
    }

    @Test
    public void testWalTables() throws Exception {
        assertMemoryLeak(() -> {
            for (String suffix : new String[]{"_wal", "_direct"}) {
                final String wal = suffix.equals("_wal") ? " WAL" : " BYPASS WAL";
                execute("CREATE TABLE t" + suffix + " (k INT, v LONG, sym SYMBOL, vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR" + wal);
                execute("CREATE TABLE d" + suffix + " (k INT, w LONG, s SYMBOL, vc VARCHAR, dts TIMESTAMP) TIMESTAMP(dts) PARTITION BY HOUR" + wal);
                // In-order rows, then rows that land between them, out of order, in earlier partitions.
                execute("INSERT INTO t" + suffix + " SELECT (x % 7)::INT, x, 's' || (x % 5), 'tv' || x, timestamp_sequence('2024-01-01', 600_000_000) FROM long_sequence(30)");
                execute("INSERT INTO t" + suffix + " SELECT (x % 9)::INT, 100 + x, 's' || (x % 3), NULL, timestamp_sequence('2024-01-01T00:05', 1_800_000_000) FROM long_sequence(8)");
                execute("INSERT INTO d" + suffix + " SELECT (x % 5)::INT, x, 's' || (x % 4), 'dv' || x, timestamp_sequence('2024-01-01', 900_000_000) FROM long_sequence(12)");
                execute("INSERT INTO d" + suffix + " SELECT (x % 6)::INT, 50 + x, 's' || (x % 2), 'late' || x, timestamp_sequence('2024-01-01T00:07', 1_200_000_000) FROM long_sequence(6)");
            }
            drainWalQueue();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            for (String[] pair : new String[][]{{"_wal", "_direct"}, {"_direct", "_wal"}, {"_wal", "_wal"}}) {
                final String t = "t" + pair[0];
                final String d = "d" + pair[1];
                for (String sql : new String[]{
                        "SELECT t.k, t.v, t.ts, t.vc, d.w, d.s, d.vc FROM " + t + " t LEFT JOIN " + d + " d ON t.k = d.k",
                        "SELECT t.v, t.sym, d.w, d.s FROM " + t + " t JOIN " + d + " d ON t.sym = d.s",
                        "SELECT d.w, t.v FROM " + d + " d JOIN " + t + " t ON d.k = t.k",
                        "SELECT t.v, d.w, d.vc FROM " + t + " t LEFT JOIN " + d + " d ON t.k = d.k AND d.vc <> t.vc WHERE t.ts IN '2024-01-01T01' ORDER BY t.ts DESC",
                }) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                }
            }
            // A cached factory over WAL tables reads what the WAL apply job committed since.
            final String sql = "SELECT t.k, t.v, d.w, d.vc FROM t_wal t LEFT JOIN d_wal d ON t.k = d.k";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertNotNull(find(factory));
                Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                execute("INSERT INTO t_wal VALUES (1, 1000, 's1', 'new', '2024-01-01T00:30'), (2, 1001, 's2', 'newer', '2024-01-01T09')");
                execute("INSERT INTO d_wal VALUES (2, 2000, 's2', 'dnew', '2024-01-01T00:45')");
                drainWalQueue();
                final String expected = ordinary(sql, sqlExecutionContext);
                TestUtils.assertContains(expected, "1000");
                TestUtils.assertContains(expected, "2000");
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            }
        });
    }

    private static void assertExecution(RecordCursorFactory factory, String sql, boolean isSwapped) throws SqlException {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(sql, join);
        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
            // The cursor builds on its first read.
            cursor.hasNext();
        }
    }

    private static void createStorageTables(String storage) throws Exception {
        execute("CREATE TABLE t_" + storage + " AS (" +
                "SELECT CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 9)::INT END k, x v, (x % 6)::LONG lk, " +
                "('s' || (x % 5))::SYMBOL sym, timestamp_sequence('2024-01-01', 1_800_000_000) ts " +
                "FROM long_sequence(160)) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE d_" + storage + " AS (" +
                "SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k, (x % 20)::INT w, (x % 4)::LONG lk, " +
                "('s' || (x % 3))::SYMBOL s, CASE WHEN x % 5 = 0 THEN NULL WHEN x % 5 = 1 THEN '' ELSE 'vé😀' || x END vc, " +
                "CASE WHEN x % 6 = 0 THEN NULL ELSE 'str' || x END str, timestamp_sequence('2024-01-01', 4_320_000_000) dts " +
                "FROM long_sequence(60)) TIMESTAMP(dts) PARTITION BY DAY");
        switch (storage) {
            case "mixed" -> {
                execute("ALTER TABLE t_mixed CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'");
                execute("ALTER TABLE d_mixed CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            }
            case "parquet" -> {
                execute("ALTER TABLE t_parquet CONVERT PARTITION TO PARQUET WHERE ts >= 0");
                execute("ALTER TABLE d_parquet CONVERT PARTITION TO PARQUET WHERE dts >= 0");
            }
            default -> {
            }
        }
        for (String table : new String[]{"t_" + storage, "d_" + storage}) {
            final long parquet = count("SELECT count() FROM table_partitions('" + table + "') WHERE isParquet");
            final long partitions = count("SELECT count() FROM table_partitions('" + table + "')");
            Assert.assertTrue(table, partitions >= 3);
            // Every partition but the active one converts.
            switch (storage) {
                case "native" -> Assert.assertEquals(table, 0, parquet);
                case "mixed" -> Assert.assertTrue(table + ": " + parquet, parquet > 0 && parquet < partitions - 1);
                default -> Assert.assertEquals(table, partitions - 1, parquet);
            }
        }
    }

    // Names the columns it writes, which the ALTER statements leave in place.
    private static void insertBuild(int rows, String from) throws Exception {
        execute("INSERT INTO d (k, w, s, dts) SELECT (x % 6)::INT, x::INT, 'd' || (x % 3), timestamp_sequence('" + from + "', 60_000_000) FROM long_sequence(" + rows + ")");
    }

    private static void insertProbe(int rows, String from, String symColumn) throws Exception {
        execute("INSERT INTO t (k, v, " + symColumn + ", ts) SELECT (x % 8)::INT, x, 's' || (x % 4), timestamp_sequence('" + from + "', 60_000_000) FROM long_sequence(" + rows + ")");
    }

    private static long count(String sql) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    // A cached factory whose tables changed their columns refuses to open, as the ordinary plan's
    // does, and the caller compiles the query again.
    private static RecordCursorFactory recompile(RecordCursorFactory factory, String sql, String alter) throws SqlException {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("expected a stale factory after " + alter);
        } catch (TableReferenceOutOfDateException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
        } finally {
            factory.close();
        }
        Assert.assertEquals(0, engine.getBusyReaderCount());
        return select(sql);
    }

    // A result's rows, without its metadata lines.
    private static String rows(String result) {
        return result.substring(result.indexOf('\n', result.indexOf('\n') + 1) + 1);
    }
}
