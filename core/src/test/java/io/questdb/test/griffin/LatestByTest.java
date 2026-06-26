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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.BindVarTuple;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LatestByTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public LatestByTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testLatestByAllFilteredReentrant() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table zyzy as (\n" +
                            "  select \n" +
                            "  timestamp_sequence(1,1000)::" + timestampType.getTypeName() + " ts,\n" +
                            "  rnd_int(0,5,0) a,\n" +
                            "  rnd_int(0,5,0) b,\n" +
                            "  rnd_int(0,5,0) c,\n" +
                            "  rnd_int(0,5,0) x,\n" +
                            "  rnd_int(0,5,0) y,\n" +
                            "  rnd_int(0,5,0) z,\n" +
                            "  from long_sequence(100)\n" +
                            ") timestamp(ts);\n"
            );
            assertQuery("select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 3 latest on ts partition by x order by x;")
                    .expectSize()
                    .returns("""
                            x\tohoh
                            7\t25
                            9\t29
                            15\t29
                            17\t26
                            """);
        });
    }

    @Test
    public void testLatestByAllFilteredResolvesSymbol() throws Exception {
        executeWithRewriteTimestamp(
                """
                        CREATE TABLE history_P4v (
                          devid SYMBOL,
                          address SHORT,
                          value SHORT,
                          value_decimal BYTE,
                          created_at DATE,
                          ts #TIMESTAMP
                        ) timestamp(ts) PARTITION BY DAY;""",
                timestampType.getTypeName()
        );

        assertQuery("""
                SELECT * FROM history_P4v
                WHERE
                  devid = 'LLLAHFZHYA'
                LATEST ON ts PARTITION BY address""")
                .timestamp("ts")
                .returns("devid\taddress\tvalue\tvalue_decimal\tcreated_at\tts\n");
    }

    @Test
    public void testLatestByAllIndexedIndexReaderGetsReloaded() throws Exception {
        final int iterations = 100;
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("""
                            CREATE TABLE e (\s
                              ts #TIMESTAMP,\s
                              sym SYMBOL CAPACITY 32768 INDEX CAPACITY 4\s
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    timestampType.getTypeName()
            );
            executeWithRewriteTimestamp("""
                            CREATE TABLE p (\s
                              ts #TIMESTAMP,\s
                              sym SYMBOL CAPACITY 32768 CACHE INDEX CAPACITY 4,\s
                              lon FLOAT,\s
                              lat FLOAT,\s
                              g3 geohash(3c)\s
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    timestampType.getTypeName()
            );

            long timestamp = 1625853700000000L;
            for (int i = 0; i < iterations; i++) {
                LOG.info().$("Iteration: ").$(i).$();

                execute("INSERT INTO e VALUES(CAST(" + timestamp + " as TIMESTAMP), '42')");
                execute("INSERT INTO p VALUES(CAST(" + timestamp + " as TIMESTAMP), '42', 142.31, 42.31, #xpt)");

                String query = "SELECT count() FROM \n" +
                        "( \n" +
                        "  ( \n" +
                        "    SELECT ts ts_p, sym, lon, lat, g3 \n" +
                        "    FROM p \n" +
                        "    WHERE ts >= cast(" + timestamp + " AS timestamp) \n" +
                        "      AND g3 within(#xpk, #xpm, #xps, #xpt) \n" +
                        "    LATEST ON ts PARTITION BY sym \n" +
                        "  ) \n" +
                        "  WHERE lon >= 142.0 AND lon <= 143.0 \n" +
                        "    AND lat >= 42.0 AND lat <= 43.0 \n" +
                        ") \n" +
                        "JOIN \n" +
                        "( \n" +
                        "  SELECT ts ts_e, sym \n" +
                        "  FROM e \n" +
                        "  WHERE ts >= cast(" + timestamp + " AS timestamp) \n" +
                        "  LATEST ON ts PARTITION BY sym \n" +
                        ") \n" +
                        "ON (sym)";
                assertQuery(query)
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);

                timestamp += 10000L;
            }
        });
    }

    @Test
    public void testLatestByAllIndexedWithPrefixes() throws Exception {
        configOverrideUseWithinLatestByOptimisation();

        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            create table pos_test
                            (\s
                              ts #TIMESTAMP,
                              device_id symbol index,
                              g8c geohash(8c)
                            ) timestamp(ts) partition by day;""",
                    timestampType.getTypeName()
            );

            execute(
                    "insert into pos_test values " +
                            "('2021-09-02T00:00:00.000000', 'device_1', #46swgj10)," +
                            "('2021-09-02T00:00:00.000001', 'device_2', #46swgj10)," +
                            "('2021-09-02T00:00:00.000002', 'device_1', #46swgj12)"
            );

            String query = """
                    SELECT *
                    FROM pos_test
                    WHERE g8c within(#46swgj10)
                    and ts in '2021-09-02'
                    LATEST ON ts\s
                    PARTITION BY device_id""";

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("LatestByAllIndexed\n" +
                            "    Async index backward scan on: device_id workers: 2\n" +
                            "      filter: g8c within(\"0010000110110001110001111100010000100000\")\n" +
                            "    Interval backward scan on: pos_test\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"2021-09-02T00:00:00.000000Z\",\"2021-09-02T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"2021-09-02T00:00:00.000000000Z\",\"2021-09-02T23:59:59.999999999Z\")]\n"));

            // prefix filter is applied AFTER latest on
            assertQuery(query)
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tdevice_id\tg8c\n" +
                            "2021-09-02T00:00:00.000001" + getTimestampSuffix(timestampType.getTypeName()) + "\tdevice_2\t46swgj10\n");
        });
    }

    @Test
    public void testLatestByIndexedSymbolFilterNotDropped() throws Exception {
        // A WHERE predicate over an INDEXED SYMBOL combined with LATEST ON ... PARTITION BY
        // a non-symbol key used to be silently dropped. WhereClauseParser extracted the
        // indexed-symbol predicate into a key-column intrinsic (expecting an index scan to
        // serve it), but the LatestByAllFiltered path -- chosen because the partition key is
        // not a symbol -- ignores that intrinsic and applies only the residual filter, which
        // was then empty. The indexed table enumerated rows the WHERE should have removed and
        // diverged from its non-indexed sibling. Cross-check the two and pin the SQL-correct
        // answer for several predicate shapes and both index families.
        assertMemoryLeak(() -> {
            for (String indexDdl : new String[]{"INDEX", "INDEX TYPE POSTING"}) {
                execute("DROP TABLE IF EXISTS t_idx;");
                execute("DROP TABLE IF EXISTS t_plain;");
                for (String name : new String[]{"t_idx", "t_plain"}) {
                    String symDecl = "t_idx".equals(name) ? "sym SYMBOL " + indexDdl : "sym SYMBOL";
                    executeWithRewriteTimestamp(
                            "CREATE TABLE " + name + " (ts #TIMESTAMP, " + symDecl + ", c1 DOUBLE)" +
                                    " TIMESTAMP(ts) PARTITION BY DAY;",
                            timestampType.getTypeName()
                    );
                    // partition 1.0 has both rows non-null (latest unaffected by filter),
                    // partition 2.0 has a NULL latest row (filter must skip back one row),
                    // partition 3.0 is entirely NULL (filter must drop the whole partition).
                    executeWithRewriteTimestamp(
                            "INSERT INTO " + name + " VALUES " +
                                    "(1::#TIMESTAMP, 'a', 1.0)," +
                                    "(2::#TIMESTAMP, 'b', 1.0)," +
                                    "(3::#TIMESTAMP, 'c', 2.0)," +
                                    "(4::#TIMESTAMP, null, 2.0)," +
                                    "(5::#TIMESTAMP, null, 3.0)," +
                                    "(6::#TIMESTAMP, null, 3.0);",
                            timestampType.getTypeName()
                    );
                }

                for (String pred : new String[]{
                        "sym IS NOT NULL",
                        "sym = 'c'",
                        "sym IN ('a', 'b')",
                        "sym != 'b'",
                        "sym NOT IN ('a')",
                }) {
                    String suffix = " WHERE " + pred + " LATEST ON ts PARTITION BY c1 ORDER BY c1, sym";
                    StringSink expected = new StringSink();
                    StringSink actual = new StringSink();
                    printSql("SELECT sym, c1 FROM t_plain" + suffix, expected);
                    printSql("SELECT sym, c1 FROM t_idx" + suffix, actual);
                    org.junit.Assert.assertEquals(
                            "predicate=[" + pred + "] index=[" + indexDdl + "]",
                            expected.toString(),
                            actual.toString()
                    );
                }

                // The indexed table must compute the same latest-by rows as the full scan does;
                // pin the canonical answers (no timestamp in the projection, so format-independent).
                assertQuery("SELECT sym, c1 FROM t_idx WHERE sym IS NOT NULL LATEST ON ts PARTITION BY c1 ORDER BY c1, sym")
                        .expectSize()
                        .returns("sym\tc1\nb\t1.0\nc\t2.0\n");
                assertQuery("SELECT sym, c1 FROM t_idx WHERE sym = 'c' LATEST ON ts PARTITION BY c1 ORDER BY c1, sym")
                        .expectSize()
                        .returns("sym\tc1\nc\t2.0\n");
                assertQuery("SELECT sym, c1 FROM t_idx WHERE sym IN ('a', 'b') LATEST ON ts PARTITION BY c1 ORDER BY c1, sym")
                        .expectSize()
                        .returns("sym\tc1\nb\t1.0\n");
            }
        });
    }

    @Test
    public void testLatestByConstantFalseWhere() throws Exception {
        // A LATEST ON whose WHERE the optimiser folds to a compile-time constant-false
        // predicate (a col<col / col>col / ts>ts self-comparison, or an AND of them)
        // used to trip 'assert nested != null' in generateLatestBy (NPE in production).
        // The SQL-correct answer is an empty result set, the same as any query over a
        // constant-false filter. See also testLatestByConstantFalseWhereBindMatches below.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE t AS (\n" +
                            "  SELECT\n" +
                            "    timestamp_sequence(1, 1000)::#TIMESTAMP ts,\n" +
                            "    rnd_double() c2,\n" +
                            "    rnd_symbol('a','b','c') c3\n" +
                            "  FROM long_sequence(100)\n" +
                            ") TIMESTAMP(ts);\n",
                    timestampType.getTypeName()
            );

            final String empty = "ts\tc2\tc3\n";
            // self-comparison on a non-key column
            assertQuery("SELECT * FROM t WHERE c2 < c2 LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            assertQuery("SELECT * FROM t WHERE c2 > c2 LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            // self-comparison on the designated timestamp
            assertQuery("SELECT * FROM t WHERE ts > ts LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            // self-comparison on the partition key
            assertQuery("SELECT * FROM t WHERE c3 != c3 LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            // AND of self-comparisons
            assertQuery("SELECT * FROM t WHERE c2 < c2 AND ts > ts LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            // a folded literal-only constant-false term
            assertQuery("SELECT * FROM t WHERE 1 > 2 LATEST ON ts PARTITION BY c3").timestamp("ts").returns(empty);
            // with ORDER BY and LIMIT riding along (the fuzzer shapes)
            assertQuery("SELECT * FROM t WHERE c2 < c2 LATEST ON ts PARTITION BY c3 ORDER BY ts ASC").timestamp("ts").returns(empty);
            assertQuery("SELECT * FROM t WHERE c2 < c2 LATEST ON ts PARTITION BY c3 LIMIT 10").timestamp("ts").returns(empty);
        });
    }

    @Test
    public void testLatestByConstantFalseWhereBindMatches() throws Exception {
        // The runtime-constant bind variant of the same predicate is a runtime no-op (it
        // does not fold), so it always compiled; cross-check that the folded literal form
        // now matches it, both empty.
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE t AS (\n" +
                            "  SELECT\n" +
                            "    timestamp_sequence(1, 1000)::#TIMESTAMP ts,\n" +
                            "    rnd_double() c2,\n" +
                            "    rnd_symbol('a','b','c') c3\n" +
                            "  FROM long_sequence(100)\n" +
                            ") TIMESTAMP(ts);\n",
                    timestampType.getTypeName()
            );

            bindVariableService.clear();
            bindVariableService.setBoolean("b0", false);
            // a boolean bind variable is a runtime constant; it does not fold, so it takes
            // the generateLatestByTableQuery path (which already clears latestBy on a
            // constant-false runtime filter). It must agree with the folded literal form.
            assertQuery("SELECT * FROM t WHERE :b0 LATEST ON ts PARTITION BY c3")
                    .timestamp("ts")
                    .returns("ts\tc2\tc3\n");
        });
    }

    @Test
    public void testLatestByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s in ('a', 'b') " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tb\n");
        });
    }

    @Test
    public void testLatestByInsertNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp, s symbol, s2 symbol) timestamp (ts) partition by month");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            assertQuery("select ts, s2, s from t " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts2\ts
                            2025-01-03T00:00:00.000000Z\t\t
                            2025-01-04T00:00:00.000000Z\tsymS2A\tsymSA
                            """);
        });
    }

    @Test
    public void testLatestByInsertNullSymbolsOnWal() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp, s symbol, s2 symbol) timestamp (ts) partition by month wal");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            drainWalQueue();
            assertQuery("select ts, s2, s from t " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts2\ts
                            2025-01-03T00:00:00.000000Z\t\t
                            2025-01-04T00:00:00.000000Z\tsymS2A\tsymSA
                            """);
        });
    }

    @Test
    public void testLatestByLightSubQueryOrderByTimestampNotElided() throws Exception {
        // A LATEST ON ... over a derived sub-query compiles to LatestByLightRecordCursorFactory,
        // which emits one row per partition key in map order, NOT in designated-timestamp order.
        // It must report SCAN_DIRECTION_OTHER so an explicit ORDER BY timestamp is honored with a
        // real sort instead of being elided as already-sorted. Here key-insertion order (A, B) is
        // the reverse of latest-timestamp order, so without the sort the rows come back descending.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (i INT, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                    (1, 'A', '2024-01-01T00:00:00.000000Z'),
                    (1, 'B', '2024-01-01T00:00:10.000000Z'),
                    (1, 'B', '2024-01-01T00:00:50.000000Z'),
                    (1, 'A', '2024-01-01T00:01:40.000000Z')
                    """);
            assertQuery("SELECT ts FROM (SELECT ts, sym, i AS i1 FROM a) WHERE i1 > 0 LATEST ON ts PARTITION BY sym ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts
                            2024-01-01T00:00:50.000000Z
                            2024-01-01T00:01:40.000000Z
                            """);
        });
    }

    @Test
    public void testLatestByMultipleChangedColSymbols() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp, s string, s2 string) timestamp (ts)" +
                    " partition by month"
            );
            execute("insert into t values('2025-01-01', null, null), " +
                    "('2025-01-02', null, null)," +
                    " ('2025-01-03', null, null), " +
                    "('2025-01-04', 'symSA', 'symS2A')");
            execute("alter table t alter column s type symbol");
            execute("alter table t alter column s2 type symbol");
            assertQuery("select ts, s2, s from t " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts2\ts
                            2025-01-03T00:00:00.000000Z\t\t
                            2025-01-04T00:00:00.000000Z\tsymS2A\tsymSA
                            """);
        });
    }

    @Test
    public void testLatestByMultipleColTopSymbols() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp) timestamp (ts)" +
                    " partition by month"
            );
            execute("insert into t values('2025-01-01'), " +
                    "('2025-01-02')," +
                    " ('2025-01-03'), " +
                    "('2025-01-04')");
            execute("alter table t add column s symbol, s2 symbol");
            execute("insert into t values('2025-01-05', 'symSA', 'symS2A');");
            assertQuery("select ts, s2, s from t " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts2\ts
                            2025-01-04T00:00:00.000000Z\t\t
                            2025-01-05T00:00:00.000000Z\tsymS2A\tsymSA
                            """);
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan1() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('e', 'f', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s2, s from t " +
                    "where s = 'a' and s2 in ('c', 'd') " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n");
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan2() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s2, s from t " +
                    "where s2 = 'c' " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts2\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\tb\n");
        });
    }

    @Test
    public void testLatestByMultipleSymbolsDoesNotNeedFullScan3() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select * from t where s2 = 'c' latest on ts partition by s, s2 " +
                    "union all " +
                    "select * from t where s2 = 'd' latest on ts partition by s, s2")
                    .noRandomAccess()
                    .expectSize()
                    .returns("s\ts2\tts\n" +
                            "a\tc\t1970-01-02T23:00:00.000000" + suffix + "\n" +
                            "b\tc\t1970-01-03T00:00:00.000000" + suffix + "\n" +
                            "a\td\t1970-01-02T18:00:00.000000" + suffix + "\n" +
                            "b\td\t1970-01-02T19:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestByMultipleSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s2, s from t " +
                    "latest on ts partition by s, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts2\ts\n" +
                            "1970-01-02T18:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\td\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\tb\n");
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("t " +
                    "where s in ('a', 'b', null) " +
                    "latest on ts partition by s3, s2, s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-04T05:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000" + suffix + "\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-05T00:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestByMultipleSymbolsWithNullInSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(100)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("t " +
                    "latest on ts partition by s3, s2, s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T19:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-04T00:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-04T05:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-04T07:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-04T11:00:00.000000" + suffix + "\n" +
                            "a\tc\t\t1970-01-04T17:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-04T20:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-04T23:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-05T00:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-05T01:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-05T02:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-05T03:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestByPartitionByByte() throws Exception {
        testLatestByPartitionBy("byte", "1", "2");
    }

    @Test
    public void testLatestByPartitionByDate() throws Exception {
        testLatestByPartitionBy("date", "'2020-05-05T00:00:00.000Z'", "'2020-05-06T00:00:00.000Z'");
    }

    @Test
    public void testLatestByPartitionByDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts (when #TIMESTAMP, ts #TIMESTAMP, temperature double) timestamp(ts) partition by day", timestampType.getTypeName());

            // forecasts for 2020-05-05
            execute("insert into forecasts values " +
                    "  ('2020-05-05', '2020-05-02', 40), " +
                    "  ('2020-05-05', '2020-05-03', 41), " +
                    "  ('2020-05-05', '2020-05-04', 42)"
            );

            // forecasts for 2020-05-06
            execute("insert into forecasts values " +
                    "  ('2020-05-06', '2020-05-01', 140), " +
                    "  ('2020-05-06', '2020-05-03', 141), " +
                    "  ('2020-05-06', '2020-05-05', 142), " +// this row has the same ts as following one and will be de-duped
                    "  ('2020-05-07', '2020-05-05', 143)"
            );

            // PARTITION BY <DESIGNATED_TIMESTAMP> is perhaps a bit silly, but it is a valid query. so let's check it's working as expected
            String query = "select when, ts, temperature from forecasts latest on ts partition by ts";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tts\ttemperature\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-01T00:00:00.000000" + suffix + "\t140.0\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-02T00:00:00.000000" + suffix + "\t40.0\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-03T00:00:00.000000" + suffix + "\t141.0\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    "2020-05-07T00:00:00.000000" + suffix + "\t2020-05-05T00:00:00.000000" + suffix + "\t143.0\n";

            assertQuery(query)
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testLatestByPartitionByDouble() throws Exception {
        testLatestByPartitionBy("double", "0.0", "1.0");
    }

    @Test
    public void testLatestByPartitionByFloat() throws Exception {
        testLatestByPartitionBy("float", "0.0", "1.0");
    }

    @Test
    public void testLatestByPartitionByGeoByte() throws Exception {
        testLatestByPartitionBy("geohash(1c)", "#u", "#v");
    }

    @Test
    public void testLatestByPartitionByGeoInt() throws Exception {
        testLatestByPartitionBy("geohash(4c)", "#uuuu", "#vvvv");
    }

    @Test
    public void testLatestByPartitionByGeoLong() throws Exception {
        testLatestByPartitionBy("geohash(7c)", "#uuuuuuu", "#vvvvvvv");
    }

    @Test
    public void testLatestByPartitionByGeoShort() throws Exception {
        testLatestByPartitionBy("geohash(2c)", "#uu", "#vv");
    }

    @Test
    public void testLatestByPartitionByTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts (when  #TIMESTAMP, version #TIMESTAMP, temperature double) timestamp(version) partition by day", timestampType.getTypeName());

            // forecasts for 2020-05-05
            execute("insert into forecasts values " +
                    "  ('2020-05-05', '2020-05-02', 40), " +
                    "  ('2020-05-05', '2020-05-03', 41), " +
                    "  ('2020-05-05', '2020-05-04', 42)"
            );

            // forecasts for 2020-05-06
            execute("insert into forecasts values " +
                    "  ('2020-05-06', '2020-05-01', 140), " +
                    "  ('2020-05-06', '2020-05-03', 141), " +
                    "  ('2020-05-06', '2020-05-05', 142)"
            );

            String query = "select when, version, temperature from forecasts latest on version partition by when";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tversion\ttemperature\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-05T00:00:00.000000" + suffix + "\t142.0\n";

            assertQuery(query)
                    .timestamp("version")
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testLatestBySubQueryInitializesSymbolTables() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE 'offer_exchanges' (" +
                    "pair SYMBOL CAPACITY 100000 INDEX, " +
                    "rate DOUBLE, " +
                    "volume_a DOUBLE, " +
                    "volume_b DOUBLE, " +
                    "buyer STRING, " +
                    "seller STRING, " +
                    "taker STRING, " +
                    "provider STRING, " +
                    "autobridged STRING, " +
                    "tx_hash STRING, " +
                    "ledger_index INT, " +
                    "sequence INT, " +
                    "ts #TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH;", timestampType.getTypeName());
            execute("insert into offer_exchanges values ('abc', 1.1, 1.1, 1.1, 'abc', 'def', 'zxy', 'a', 'some hash', 'foo', 123, 5, '2024-01-29T15:00:00.000Z')");
            execute("insert into offer_exchanges values ('abc', 1.1, 1.1, 1.1, 'abc', 'def', 'zxy', 'a', 'some hash', 'foo', 123, 5, '2024-01-30T15:01:00.000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("WITH first_selection as (" +
                    "  SELECT pair, first(rate) AS open, last(rate) AS close, min(rate) AS low, max(rate) AS high, " +
                    "         sum(volume_a) AS base_volume, sum(volume_b) AS counter_volume, count(*) AS exchanges " +
                    "  FROM 'offer_exchanges' " +
                    "  WHERE ts >= '2024-01-30T15:00:00.000Z'" +
                    "), " +
                    "second_selection as (" +
                    "  SELECT pair, rate as prev_rate, ts as prev_ts " +
                    "  FROM 'offer_exchanges' " +
                    "  WHERE ts < '2024-01-30T15:00:00.000Z' and pair in (SELECT pair FROM first_selection) " +
                    "  LATEST ON ts PARTITION BY pair " +
                    ") " +
                    "SELECT first_selection.pair, first_selection.open, first_selection.close, first_selection.low, first_selection.high," +
                    "       first_selection.base_volume, first_selection.counter_volume, first_selection.exchanges, second_selection.prev_rate, " +
                    "       second_selection.prev_ts " +
                    "FROM first_selection " +
                    "JOIN second_selection on (pair);")
                    .noRandomAccess()
                    .returns("pair\topen\tclose\tlow\thigh\tbase_volume\tcounter_volume\texchanges\tprev_rate\tprev_ts\n" +
                            "abc\t1.1\t1.1\t1.1\t1.1\t1.1\t1.1\t1\t1.1\t2024-01-29T15:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestBySymbolDifferentBindingService() throws Exception {
        // Test that a parametrized latest-by <symbol_column> is re-initialized to a different parameter value
        // when the query is re-executed with a different binding variable service

        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            final String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Each case re-executes the held factory under its OWN execution context with its OWN
            // bind-variable service, verifying the parametrized latest-by re-reads :sym from the
            // execution context's service - not a value cached at compile time.
            try (
                    SqlExecutionContextImpl contextC = new SqlExecutionContextImpl(engine, 1);
                    SqlExecutionContextImpl contextA = new SqlExecutionContextImpl(engine, 1)
            ) {
                contextC.with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(configuration));
                contextA.with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(configuration));

                final ObjList<BindVarTuple> cases = new ObjList<>();
                // sanity check: same value as compiled with, via a different service
                cases.add(BindVarTuple.ok(
                        "different service, sym=c",
                        "ts\ts\n1970-01-03T00:00:00.000000" + suffix + "\tc\n",
                        bindVariableService -> bindVariableService.setStr("sym", "c")
                ).withContext(contextC));
                // different value via a different service must yield a different result
                cases.add(BindVarTuple.ok(
                        "different service, sym=a",
                        "ts\ts\n1970-01-02T23:00:00.000000" + suffix + "\ta\n",
                        bindVariableService -> bindVariableService.setStr("sym", "a")
                ).withContext(contextA));

                assertQuery("select ts, s from t where s = :sym latest on ts partition by s")
                        .noLeakCheck()
                        .timestamp("ts")
                        .assertBinds(cases);
            }
        });
    }

    @Test
    public void testLatestBySymbolEmpty() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan any partition, searched symbol values don't exist in symbol table
                    if (Utf8s.containsAscii(name, "1970-01-01") || Utf8s.containsAscii(name, "1970-01-02")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('g', 'd', 'f') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(40)" +
                    ") timestamp(ts) partition by DAY");

            assertQuery("t where s in ('a', 'b') latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("x\ts\tts\n");
        });
    }

    @Test
    public void testLatestBySymbolManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol(10000, 1, 15, 1000) s, " +
                    "timestamp_sequence(0, 1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(1000000)" +
                    ") timestamp(ts) partition by DAY");

            String distinctSymbols = selectDistinctSym();

            engine.releaseInactive();

            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in other partitions
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select min(ts), max(ts) from (select ts, x, s from t latest on ts partition by s)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("min\tmax\n" +
                            "1970-01-11T15:33:16.000000" + suffix + "\t1970-01-12T13:46:39.000000" + suffix + "\n");

            assertQuery("select min(ts), max(ts) from (" +
                    "select ts, x, s " +
                    "from t " +
                    "where s in (" + distinctSymbols + ") " +
                    "latest on ts partition by s" +
                    ")")
                    .noRandomAccess()
                    .expectSize()
                    .returns("min\tmax\n" +
                            "1970-01-11T16:57:53.000000" + suffix + "\t1970-01-12T13:46:05.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestBySymbolUnfilteredDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, x, s from t latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\t\n");
        });
    }

    @Test
    public void testLatestBySymbolWithNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "  select " +
                    "    x, " +
                    "    rnd_symbol('a', 'b', 'c', 'd', 'e', 'f') s, " +
                    "    timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "  from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, x, s from t latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\ts\n" +
                            "1970-01-02T17:00:00.000000" + suffix + "\t42\td\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\t44\te\n" +
                            "1970-01-02T21:00:00.000000" + suffix + "\t46\tc\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\tf\n");

            assertQuery("select ts, x, s from t latest on ts partition by s order by s desc")
                    .expectSize()
                    .returns("ts\tx\ts\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\t49\tf\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\t44\te\n" +
                            "1970-01-02T17:00:00.000000" + suffix + "\t42\td\n" +
                            "1970-01-02T21:00:00.000000" + suffix + "\t46\tc\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\t47\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\t48\ta\n");
        });
    }

    @Test
    public void testLatestByValueEmptyTableExcludedValueFilter() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( sym symbol, ts #TIMESTAMP ) timestamp(ts) partition by day",
                timestampType.getTypeName()
        );
        assertQuery("select sym, ts from a where sym != 'x' latest on ts partition by sym")
                .timestamp("ts")
                .returns("sym\tts\n");
    }

    @Test
    public void testLatestByValueEmptyTableNoFilter() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( sym symbol, ts #TIMESTAMP ) timestamp(ts) partition by day",
                timestampType.getTypeName()
        );
        assertQuery("select sym, ts from a latest on ts partition by sym")
                .timestamp("ts")
                .returns("sym\tts\n");
    }

    @Test
    public void testLatestByValuesFilteredResolvesSymbol() throws Exception {
        executeWithRewriteTimestamp(
                "create table a ( i int, s symbol, ts #TIMESTAMP ) timestamp(ts)",
                timestampType.getTypeName()
        );
        assertQuery("select s, i, ts " +
                "from a " +
                "where s in (select distinct s from a) " +
                "and s = 'ABC' " +
                "latest on ts partition by s")
                .timestamp("ts")
                .returns("s\ti\tts\n");
    }

    @Test
    public void testLatestByWithDeferredNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE tab (ts #TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n", timestampType.getTypeName());
            execute("""
                    insert into tab
                    select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)
                    from long_sequence(1000);""");

            assertQuery("""
                    with r as (select id, value v from tab where id = 'apc' || rnd_int() LATEST ON ts PARTITION BY id),
                         rr as (select id, value v from tab where id = 'apc' || rnd_int() and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)
                            select r.id, r.v, cast((r.v - rr.v) as float) r_1M
                            from r
                            join rr on id
                    """)
                    .noRandomAccess()
                    .returns("id\tv\tr_1M\n");
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s in (:sym1, :sym2) and s != :sym3 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "a");
            assertQuery("select ts, s from t " +
                    "where s = :sym1 and s != :sym2 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .returns("ts\ts\n");
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s in (:sym1, :sym2) and s != :sym3 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithInAndNotInAllBindVariablesNonEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s = :sym1 and s != :sym2 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .returns("ts\ts\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithInAndNotInBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s in ('a', 'b', 'c') and s != :sym " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T14:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-02T16:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\tc\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", null);
            bindVariableService.setStr("sym2", "a");
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-03T00:00:00.000000" + suffix + "\tc\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesMultipleValuesIndexedFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s), index(s2) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "d");
            bindVariableService.setStr("sym2", null);
            bindVariableService.setStr("sym3", "d");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s not in (:sym1, :sym2) and s2 = :sym3 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T14:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-02T16:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T19:00:00.000000" + suffix + "\tc\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s <> :sym " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithNotInAllBindVariablesSingleValueIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (" +
                    "select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)" +
                    "), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("select ts, s from t " +
                    "where s <> :sym " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\ts\n" +
                            "1970-01-02T22:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T23:00:00.000000" + suffix + "\ta\n");
        });
    }

    @Test
    public void testLatestByWithStaticNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");
            execute("""
                    insert into tab
                    select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0)
                    from long_sequence(1000);""");

            assertQuery("""
                    with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),
                         rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)
                            select r.id, r.v, cast((r.v - rr.v) as float) r_1M
                            from r
                            join rr on id
                    """)
                    .noRandomAccess()
                    .returns("id\tv\tr_1M\n");
        });
    }

    @Test
    public void testLatestByWithSymbolOnEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");

            assertQuery("""
                    with r as (select id, value v from tab where id = 'apc' LATEST ON ts PARTITION BY id),
                            rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now())  LATEST ON ts PARTITION BY id)
                            select r.id, r.v, cast((r.v - rr.v) as float) r_1M
                            from r
                            join rr on id
                    """)
                    .noRandomAccess()
                    .returns("id\tv\tr_1M\n");
        });
    }

    @Test
    public void testLatestOnVarchar() throws Exception {
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery("t " +
                "where v in ('a', 'b', 'd') and x%2 = 0 " +
                "latest on ts partition by v")
                .ddl("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_varchar('a', 'b', 'c', null) v, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) partition by DAY")
                .mutateWith("insert into t values (1000, 'd', '1970-01-02T20:00')")
                .timestamp("ts")
                .expectSize()
                .returns("x\tv\tts\n" +
                        "42\tb\t1970-01-02T17:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n", "x\tv\tts\n" +
                        "42\tb\t1970-01-02T17:00:00.000000" + suffix + "\n" +
                        "1000\td\t1970-01-02T20:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n");
    }

    @Test
    public void testLatestOnVarcharNonAscii() throws Exception {
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery("select * " +
                "from t " +
                "latest on ts partition by v")
                .ddl("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_varchar('раз', 'два', 'три', null) v, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(20)" +
                        ") timestamp(ts) partition by DAY")
                .timestamp("ts")
                .expectSize()
                .returns("x\tv\tts\n" +
                        "14\t\t1970-01-01T13:00:00.000000" + suffix + "\n" +
                        "17\tраз\t1970-01-01T16:00:00.000000" + suffix + "\n" +
                        "19\tдва\t1970-01-01T18:00:00.000000" + suffix + "\n" +
                        "20\tтри\t1970-01-01T19:00:00.000000" + suffix + "\n");
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("t " +
                    "where s in ('a', 'b') and x%2 = 0 " +
                    "latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("x\ts\tts\n" +
                            "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                            "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestWithFilterByDoesNotNeedFullScanValueNotInSymbolTable() throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Query should not scan the first partition
                // all the latest values are in the second, third partition
                if (Utf8s.containsAscii(name, "1970-01-01")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery("t " +
                "where s in ('a', 'b', 'c') and x%2 = 0 " +
                "latest on ts partition by s")
                .ddl("create table t as (" +
                        "select " +
                        "x, " +
                        "rnd_symbol('a', 'b', null) s, " +
                        "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                        "from long_sequence(49)" +
                        ") timestamp(ts) partition by DAY")
                .mutateWith("insert into t values (1000, 'c', '1970-01-02T20:00')")
                .timestamp("ts")
                .expectSize()
                .returns("x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n", "x\ts\tts\n" +
                        "44\tb\t1970-01-02T19:00:00.000000" + suffix + "\n" +
                        "1000\tc\t1970-01-02T20:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n");
    }

    @Test
    public void testLatestWithJoinIndexed() throws Exception {
        testLatestByWithJoin(true);
    }

    @Test
    public void testLatestWithJoinNonIndexed() throws Exception {
        testLatestByWithJoin(false);
    }

    @Test
    public void testLatestWithNullInSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("t where s in ('a', null) latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("x\ts\tts\n" +
                            "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testLatestWithoutSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // Query should not scan the first partition
                    // all the latest values are in the second, third partition
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (" +
                    "select " +
                    "x, " +
                    "rnd_symbol('a', 'b', null) s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(49)" +
                    ") timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery("t where x%2 = 1 latest on ts partition by s")
                    .timestamp("ts")
                    .expectSize()
                    .returns("x\ts\tts\n" +
                            "35\ta\t1970-01-02T10:00:00.000000" + suffix + "\n" +
                            "47\tb\t1970-01-02T22:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n");
        });
    }

    @Test
    public void testSymbolInPredicate_singleElement() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE table trades(symbol symbol, side symbol, timestamp #TIMESTAMP) timestamp(timestamp);", timestampType.getTypeName());
            execute("insert into trades VALUES ('BTC', 'buy', 1609459199000000::timestamp);");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "symbol\tside\ttimestamp\n" +
                    "BTC\tbuy\t2020-12-31T23:59:59.000000" + suffix + "\n";
            String query = """
                    SELECT * FROM trades
                    WHERE symbol in ('BTC') and side in 'buy'
                    LATEST ON timestamp PARTITION BY symbol;""";
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .returns(expected);
        });
    }

    private String selectDistinctSym() throws SqlException {
        StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select("select distinct s from t order by s limit " + 500)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    if (i++ > 0) {
                        sink.put(',');
                    }
                    sink.put('\'').put(record.getSymA(0)).put('\'');
                }
            }
        }
        return sink.toString();
    }

    private void testLatestByPartitionBy(String partitionByType, String valueA, String valueB) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts " +
                    "( when " + partitionByType + ", " +
                    "version #TIMESTAMP, " +
                    "temperature double) timestamp(version) partition by day", timestampType.getTypeName());
            execute("insert into forecasts values " +
                    "  (" + valueA + ", '2020-05-02', 40), " +
                    "  (" + valueA + ", '2020-05-03', 41), " +
                    "  (" + valueA + ", '2020-05-04', 42), " +
                    "  (" + valueB + ", '2020-05-01', 140), " +
                    "  (" + valueB + ", '2020-05-03', 141), " +
                    "  (" + valueB + ", '2020-05-05', 142)"
            );

            String query = "select when, version, temperature from forecasts latest on version partition by when";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tversion\ttemperature\n" +
                    valueA.replaceAll("['#]", "") + "\t2020-05-04T00:00:00.000000" + suffix + "\t42.0\n" +
                    valueB.replaceAll("['#]", "") + "\t2020-05-05T00:00:00.000000" + suffix + "\t142.0\n";

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("version")
                    .expectSize()
                    .returns(expected);
        });
    }

    private void testLatestByWithJoin(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table r (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into r values ('xyz', 1, '2022-11-02T01:01:01')");
            executeWithRewriteTimestamp("create table t (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into t values ('xyz', 42, '2022-11-02T01:01:01')");

            String query = """
                    with r as (select symbol, value v from r where symbol = 'xyz' latest on ts partition by symbol),
                     t as (select symbol, value v from t where symbol = 'xyz' latest on ts partition by symbol)
                    select r.symbol, r.v subscribers, t.v followers
                    from r
                    join t on symbol""";
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            symbol\tsubscribers\tfollowers
                            xyz\t1\t42
                            """);
        });
    }
}
