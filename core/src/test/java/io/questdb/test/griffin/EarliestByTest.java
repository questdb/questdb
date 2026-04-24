/*+****************************************************************************
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class EarliestByTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public EarliestByTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testEarliestByAllFilteredReentrant() throws Exception {
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
            assertQuery(
                    "x\tohoh\n" +
                            "15\t29\n" +
                            "17\t26\n" +
                            "9\t29\n" +
                            "7\t27\n",
                    "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 3 earliest on ts partition by x;",
                    true
            );
        });
    }

    @Test
    public void testEarliestByAllFilteredResolvesSymbol() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE history_P4v (\n" +
                            "  devid SYMBOL,\n" +
                            "  address SHORT,\n" +
                            "  value SHORT,\n" +
                            "  value_decimal BYTE,\n" +
                            "  created_at DATE,\n" +
                            "  ts #TIMESTAMP\n" +
                            ") timestamp(ts) PARTITION BY DAY;",
                    timestampType.getTypeName()
            );

            assertQuery(
                    "devid\taddress\tvalue\tvalue_decimal\tcreated_at\tts\n",
                    "SELECT * FROM history_P4v WHERE devid = 'LLLAHFZHYA' EARLIEST ON ts PARTITION BY address",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByAllIndexedIndexReaderGetsReloaded() throws Exception {
        final int iterations = 100;
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "CREATE TABLE e (ts #TIMESTAMP, sym SYMBOL CAPACITY 32_768 INDEX CAPACITY 4) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE p (ts #TIMESTAMP, sym SYMBOL CAPACITY 32_768 CACHE INDEX CAPACITY 4, lon FLOAT, lat FLOAT, g3 geohash(3c)) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );

            long timestamp = 1_625_853_700_000_000L;
            for (int i = 0; i < iterations; i++) {
                execute("INSERT INTO e VALUES(CAST(" + timestamp + " as TIMESTAMP), '42')");
                execute("INSERT INTO p VALUES(CAST(" + timestamp + " as TIMESTAMP), '42', 142.31, 42.31, #xpt)");

                String query = "SELECT count() FROM (" +
                        "  (SELECT ts ts_p, sym, lon, lat, g3 FROM p WHERE ts >= cast(" + timestamp + " AS timestamp) AND g3 within(#xpk, #xpm, #xps, #xpt) EARLIEST ON ts PARTITION BY sym) " +
                        "  WHERE lon >= 142.0 AND lon <= 143.0 AND lat >= 42.0 AND lat <= 43.0) " +
                        "JOIN (SELECT ts ts_e, sym FROM e WHERE ts >= cast(" + timestamp + " AS timestamp) EARLIEST ON ts PARTITION BY sym) ON (sym)";
                assertQuery("count\n1\n", query, null, false, true);
                timestamp += 10_000L;
            }
        });
    }

    @Test
    public void testEarliestByAllIndexedWithPrefixes() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table pos_test (ts #TIMESTAMP, device_id symbol index, g8c geohash(8c)) timestamp(ts) partition by day;",
                    timestampType.getTypeName()
            );

            execute("insert into pos_test values " +
                    "('2021-09-02T00:00:00.000000', 'device_1', #46swgj10)," +
                    "('2021-09-02T00:00:00.000001', 'device_2', #46swgj10)," +
                    "('2021-09-02T00:00:00.000002', 'device_1', #46swgj12)");

            String query = "SELECT * FROM pos_test WHERE g8c within(#46swgj10) and ts in '2021-09-02' EARLIEST ON ts PARTITION BY device_id";

            assertPlanNoLeakCheck(
                    query,
                    "EarliestByAllIndexed\n" +
                            "    Async index forward scan on: device_id workers: 2\n" +
                            "      filter: g8c within(\"0010000110110001110001111100010000100000\")\n" +
                            "    Interval forward scan on: pos_test\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"2021-09-02T00:00:00.000000Z\",\"2021-09-02T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"2021-09-02T00:00:00.000000000Z\",\"2021-09-02T23:59:59.999999999Z\")]\n")
            );

            assertQuery(
                    "ts\tdevice_id\tg8c\n" +
                            "2021-09-02T00:00:00.000000" + getTimestampSuffix(timestampType.getTypeName()) + "\tdevice_1\t46swgj10\n" +
                            "2021-09-02T00:00:00.000001" + getTimestampSuffix(timestampType.getTypeName()) + "\tdevice_2\t46swgj10\n",
                    query,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "select ts, s from t where s in ('a', 'b') earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByInsertNullSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts " + timestampType.getTypeName() + ", s symbol, s2 symbol) timestamp (ts) partition by month");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000" + suffix + "\t\t\n" +
                            "2025-01-04T00:00:00.000000" + suffix + "\tsymS2A\tsymSA\n",
                    "select ts, s2, s from t earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByInsertNullSymbolsOnWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts " + timestampType.getTypeName() + ", s symbol, s2 symbol) timestamp (ts) partition by month wal");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            drainWalQueue();
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000" + suffix + "\t\t\n" +
                            "2025-01-04T00:00:00.000000" + suffix + "\tsymS2A\tsymSA\n",
                    "select ts, s2, s from t earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    // Guards against reading uninitialised column-top regions as non-null keys when a column's
    // type changes. Forward-scan EARLIEST visits the NULL-key partitions first, so the earliest
    // row per key in the post-conversion data must be selected correctly.
    @Test
    public void testEarliestByMultipleChangedColSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts " + timestampType.getTypeName() + ", s string, s2 string) timestamp (ts) partition by month");
            execute("insert into t values('2025-01-01', null, null), " +
                    "('2025-01-02', null, null), " +
                    "('2025-01-03', null, null), " +
                    "('2025-01-04', 'symSA', 'symS2A')");
            execute("alter table t alter column s type symbol");
            execute("alter table t alter column s2 type symbol");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000" + suffix + "\t\t\n" +
                            "2025-01-04T00:00:00.000000" + suffix + "\tsymS2A\tsymSA\n",
                    "select ts, s2, s from t earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    // Guards against reading uninitialised column-top regions as non-null keys when a column is
    // added to an existing table. Forward-scan EARLIEST lands on the pre-column-top partition
    // first and must treat it as NULL keys, not as garbage.
    @Test
    public void testEarliestByMultipleColTopSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts " + timestampType.getTypeName() + ") timestamp (ts) partition by month");
            execute("insert into t values('2025-01-01'), " +
                    "('2025-01-02'), " +
                    "('2025-01-03'), " +
                    "('2025-01-04')");
            execute("alter table t add column s symbol, s2 symbol");
            execute("insert into t values('2025-01-05', 'symSA', 'symS2A');");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000" + suffix + "\t\t\n" +
                            "2025-01-05T00:00:00.000000" + suffix + "\tsymS2A\tsymSA\n",
                    "select ts, s2, s from t earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByMultipleSymbolsDoesNotNeedFullScan1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");
            execute("insert into t values ('e', 'f', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-01T03:00:00.000000" + suffix + "\td\ta\n",
                    "select ts, s2, s from t where s = 'a' and s2 in ('c', 'd') earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByMultipleSymbolsDoesNotNeedFullScan2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-01T07:00:00.000000" + suffix + "\tc\tb\n",
                    "select ts, s2, s from t where s2 = 'c' earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByMultipleSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\td\tb\n" +
                            "1970-01-01T03:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-01T07:00:00.000000" + suffix + "\tc\tb\n",
                    "select ts, s2, s from t earliest on ts partition by s, s2",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByMultipleSymbolsWithNullInSymbolsDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(100)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "a\tc\t\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-01T03:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-01T04:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-01T08:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-01T09:00:00.000000" + suffix + "\n" +
                            "\tc\t\t1970-01-01T11:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-01T13:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-01T16:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-01T23:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-02T09:00:00.000000" + suffix + "\n",
                    "t where s in ('a', 'b', null) earliest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByMultipleSymbolsWithNullInSymbolsUnfilteredDoesNotNeedFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(100)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "a\tc\t\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "\t\t\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "b\tc\td\t1970-01-01T03:00:00.000000" + suffix + "\n" +
                            "b\t\t\t1970-01-01T04:00:00.000000" + suffix + "\n" +
                            "a\tc\td\t1970-01-01T08:00:00.000000" + suffix + "\n" +
                            "b\t\td\t1970-01-01T09:00:00.000000" + suffix + "\n" +
                            "\tc\t\t1970-01-01T11:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-01T13:00:00.000000" + suffix + "\n" +
                            "\tc\td\t1970-01-01T16:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-01T23:00:00.000000" + suffix + "\n" +
                            "a\t\td\t1970-01-02T09:00:00.000000" + suffix + "\n",
                    "t earliest on ts partition by s3, s2, s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestBySymbolDifferentBindingService() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "c");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 final RecordCursorFactory factory = CairoEngine.select(compiler, "select ts, s from t where s = :sym earliest on ts partition by s", sqlExecutionContext)) {

                try (SqlExecutionContextImpl localContext = new SqlExecutionContextImpl(engine, 1)) {
                    BindVariableServiceImpl localBindings = new BindVariableServiceImpl(configuration);
                    localContext.with(AllowAllSecurityContext.INSTANCE, localBindings);
                    localBindings.setStr("sym", "c");
                    assertFactoryCursor(
                            "ts\ts\n" +
                                    "1970-01-01T03:00:00.000000" + suffix + "\tc\n",
                            "ts",
                            factory,
                            true,
                            localContext,
                            false,
                            false
                    );
                }

                try (SqlExecutionContextImpl localContext = new SqlExecutionContextImpl(engine, 1)) {
                    BindVariableServiceImpl localBindings = new BindVariableServiceImpl(configuration);
                    localContext.with(AllowAllSecurityContext.INSTANCE, localBindings);
                    localBindings.setStr("sym", "a");

                    assertFactoryCursor(
                            "ts\ts\n" +
                                    "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                            "ts",
                            factory,
                            true,
                            localContext,
                            false,
                            false
                    );
                }
            }
        });
    }

    @Test
    public void testEarliestBySymbolEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, rnd_symbol('g', 'd', 'f') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(40)) timestamp(ts) partition by DAY");

            assertQuery(
                    "x\ts\tts\n",
                    "t where s in ('a', 'b') earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByValueEmptyTableExcludedValueFilter() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table a (sym symbol, ts #TIMESTAMP) timestamp(ts) partition by day",
                    timestampType.getTypeName()
            );
            assertQuery(
                    "sym\tts\n",
                    "select sym, ts from a where sym != 'x' earliest on ts partition by sym",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByValueEmptyTableNoFilter() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table a (sym symbol, ts #TIMESTAMP) timestamp(ts) partition by day",
                    timestampType.getTypeName()
            );
            assertQuery(
                    "sym\tts\n",
                    "select sym, ts from a earliest on ts partition by sym",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByWithInAndNotInAllBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t where s in (:sym1, :sym2) and s != :sym3 earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithInAndNotInAllBindVariablesEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "a");
            assertQuery(
                    "ts\ts\n",
                    "select ts, s from t where s = :sym1 and s != :sym2 earliest on ts partition by s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByWithInAndNotInAllBindVariablesIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)), index(s) timestamp(ts) partition by DAY");

            bindVariableService.clear();
            bindVariableService.setStr("sym1", "a");
            bindVariableService.setStr("sym2", "b");
            bindVariableService.setStr("sym3", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "select ts, s from t where s in (:sym1, :sym2) and s != :sym3 earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithStaticNonExistingSymbolOnNonEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");
            execute("insert into tab select dateadd('h', -x::int, now()), rnd_symbol('ap', 'btc'), rnd_int(1,1000,0) from long_sequence(1000);");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' earliest on ts partition by id), " +
                            "rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now()) earliest on ts partition by id) " +
                            "select r.id, r.v, cast((r.v - rr.v) as float) r_1M from r join rr on id", null, false, false
            );
        });
    }

    @Test
    public void testEarliestByWithSymbolOnEmptyTableDoesNotThrowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, id SYMBOL, value INT) timestamp (ts) PARTITION BY MONTH;\n");

            assertQuery("id\tv\tr_1M\n",
                    "with r as (select id, value v from tab where id = 'apc' earliest on ts partition by id), " +
                            "rr as (select id, value v from tab where id = 'apc' and ts <= dateadd('d', -7, now()) earliest on ts partition by id) " +
                            "select r.id, r.v, cast((r.v - rr.v) as float) r_1M from r join rr on id", null, false, false
            );
        });
    }

    @Test
    public void testEarliestOnVarchar() throws Exception {
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        assertQuery(
                "x\tv\tts\n" +
                        "10\ta\t1970-01-01T09:00:00.000000" + suffix + "\n" +
                        "18\tb\t1970-01-01T17:00:00.000000" + suffix + "\n",
                "t where v in ('a', 'b', 'd') and x%2 = 0 earliest on ts partition by v",
                "create table t as (select x, rnd_varchar('a', 'b', 'c', null) v, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY",
                "ts",
                "insert into t values (1000, 'd', '1970-01-02T20:00')",
                "x\tv\tts\n" +
                        "10\ta\t1970-01-01T09:00:00.000000" + suffix + "\n" +
                        "18\tb\t1970-01-01T17:00:00.000000" + suffix + "\n" +
                        "1000\td\t1970-01-02T20:00:00.000000" + suffix + "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testEarliestWithNullInSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, rnd_symbol('a', 'b', null) s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "1\ta\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "4\t\t1970-01-01T03:00:00.000000" + suffix + "\n",
                    "t where s in ('a', null) earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestWithoutSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, rnd_symbol('a', 'b', null) s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "1\ta\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "3\tb\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "5\t\t1970-01-01T04:00:00.000000" + suffix + "\n",
                    "t where x%2 = 1 earliest on ts partition by s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithJoinIndexed() throws Exception {
        testEarliestByWithJoin(true);
    }

    @Test
    public void testEarliestByWithJoinNonIndexed() throws Exception {
        testEarliestByWithJoin(false);
    }

    private void testEarliestByWithJoin(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table r (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into r values ('xyz', 1, '2022-11-02T01:01:01')");
            executeWithRewriteTimestamp("create table t (symbol symbol, value long, ts #TIMESTAMP)" +
                    (indexed ? ", index(symbol) " : " ") + "timestamp(ts) partition by day", timestampType.getTypeName());
            execute("insert into t values ('xyz', 42, '2022-11-02T01:01:01')");

            String query = "with r as (select symbol, value v from r where symbol = 'xyz' earliest on ts partition by symbol), " +
                    "t as (select symbol, value v from t where symbol = 'xyz' earliest on ts partition by symbol) " +
                    "select r.symbol, r.v subscribers, t.v followers from r join t on symbol";
            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "symbol\tsubscribers\tfollowers\n" +
                                "xyz\t1\t42\n",
                        factory,
                        false,
                        false
                );
            }
        });
    }

    @Test
    public void testEarliestByPartitionByTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table forecasts (when  #TIMESTAMP, version #TIMESTAMP, temperature double) timestamp(version) partition by day", timestampType.getTypeName());

            execute("insert into forecasts values " +
                    "  ('2020-05-05', '2020-05-02', 40), " +
                    "  ('2020-05-05', '2020-05-03', 41), " +
                    "  ('2020-05-05', '2020-05-04', 42)");

            execute("insert into forecasts values " +
                    "  ('2020-05-06', '2020-05-01', 140), " +
                    "  ('2020-05-06', '2020-05-03', 141), " +
                    "  ('2020-05-06', '2020-05-05', 142)");

            String query = "select when, version, temperature from forecasts earliest on version partition by when";
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "when\tversion\ttemperature\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-01T00:00:00.000000" + suffix + "\t140.0\n" +
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-02T00:00:00.000000" + suffix + "\t40.0\n";

            assertQuery(expected, query, "version", true, true);
        });
    }

    @Test
    public void testEarliestOnWithExplicitColumnList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b', 'c') s, x val, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(49)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\tval\tts\n" +
                            "a\t1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t3\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "c\t4\t1970-01-01T03:00:00.000000" + suffix + "\n",
                    "SELECT s, val, ts FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnWithOrderByAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b', 'c') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(49)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // ORDER BY ts DESC LIMIT 1 returns the earliest row with the latest timestamp among earliest rows
            assertSql(
                    "ts\ts\n" +
                            "1970-01-01T03:00:00.000000" + suffix + "\tc\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s ORDER BY ts DESC LIMIT 1"
            );
        });
    }

    @Test
    public void testEarliestOnWithLatestOnSameQueryErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertException(
                    "SELECT * FROM t LATEST ON ts PARTITION BY s EARLIEST ON ts PARTITION BY s",
                    44,
                    "cannot use both LATEST and EARLIEST"
            );
        });
    }

    @Test
    public void testEarliestBySingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT 'a'::symbol s, '2024-01-01T00:00:00'::" + timestampType.getTypeName() + " ts) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\tts\n" +
                            "a\t2024-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestBySinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T00:02:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByDeprecatedSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t EARLIEST BY s"
            );
        });
    }

    @Test
    public void testEarliestByPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('b', '1970-01-01T00:00:00'), ('b', '1970-01-01T12:00:00'), ('a', '1970-01-01T23:00:00'), ('a', '1970-01-02T01:00:00'), ('b', '1970-01-02T02:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-01T23:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnDoesNotNeedFullScanForward() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    // EARLIEST ON scans forward; it should find all symbol
                    // combinations in the first partitions without touching
                    // the newest one.
                    if (Utf8s.containsAscii(name, "1970-01-03")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(49)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', 'b') EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (SELECT rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts FROM long_sequence(10)) TIMESTAMP(ts)");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnSubQueryOrdered() throws Exception {
        // Triggers EarliestByLightRecordCursorFactory with orderedByTimestampAsc=true
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T02:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T03:00:00')");

            // Subquery preserves user column order: s, ts
            // Map iteration order: a first (first key encountered), then b
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnSubQueryUnordered() throws Exception {
        // Triggers EarliestByLightRecordCursorFactory with orderedByTimestampAsc=false
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T02:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT s, ts FROM t ORDER BY ts DESC) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnSubQueryEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    "s\tts\n",
                    "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnSubQueryReentrant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T01:00:00'), ('b', '1970-01-01T00:00:00'), ('a', '1970-01-01T00:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "s\tts\n" +
                    "b\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                    "a\t1970-01-01T00:00:00.000000" + suffix + "\n";

            // Execute twice to test cursor close+reopen
            assertSql(expected, "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s");
            assertSql(expected, "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s");
        });
    }

    @Test
    public void testEarliestOnSubQueryMultiplePartitionCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 'x', '1970-01-01T02:00:00'), ('a', 'y', '1970-01-01T01:00:00'), " +
                    "('a', 'x', '1970-01-01T00:00:00'), ('b', 'x', '1970-01-01T03:00:00')");

            // Multi-key map iteration order is non-deterministic, so wrap in an ORDER BY
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s1\ts2\tts\n" +
                            "a\tx\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "a\ty\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "b\tx\t1970-01-01T03:00:00.000000" + suffix + "\n",
                    "SELECT s1, s2, ts FROM (SELECT s1, s2, ts FROM (SELECT s1, s2, ts FROM t) EARLIEST ON ts PARTITION BY s1, s2) ORDER BY s1, s2, ts"
            );
        });
    }

    @Test
    public void testEarliestOnGroupBySubQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T02:00:00'), ('b', 20, '1970-01-01T01:00:00'), " +
                    "('a', 30, '1970-01-01T01:00:00'), ('b', 40, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\ttotal\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\t20\n" +
                            "a\t1970-01-01T01:00:00.000000" + suffix + "\t30\n",
                    "SELECT s, ts, total FROM (" +
                            "SELECT s, ts, sum(v) total FROM t GROUP BY s, ts" +
                            ") EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnGroupByEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    "s\tts\ttotal\n",
                    "SELECT s, ts, total FROM (" +
                            "SELECT s, ts, sum(v) total FROM t GROUP BY s, ts" +
                            ") EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnGroupByReentrant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T02:00:00'), ('b', 20, '1970-01-01T01:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "s\tts\ttotal\n" +
                    "b\t1970-01-01T01:00:00.000000" + suffix + "\t20\n" +
                    "a\t1970-01-01T02:00:00.000000" + suffix + "\t10\n";

            String query = "SELECT s, ts, total FROM (" +
                    "SELECT s, ts, sum(v) total FROM t GROUP BY s, ts" +
                    ") EARLIEST ON ts PARTITION BY s";

            assertSql(expected, query);
            assertSql(expected, query);
        });
    }

    @Test
    public void testEarliestOnUnionAllSubQuery() throws Exception {
        // UNION ALL does not support random access, triggering EarliestByRecordCursorFactory
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES ('a', '1970-01-01T02:00:00'), ('b', '1970-01-01T01:00:00')");
            execute("CREATE TABLE t2 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t2 VALUES ('a', '1970-01-01T00:00:00'), ('c', '1970-01-01T03:00:00')");

            String query = "SELECT s, ts FROM (" +
                    "SELECT s, ts FROM t1 UNION ALL SELECT s, ts FROM t2" +
                    ") EARLIEST ON ts PARTITION BY s";
            // UNION ALL path (EarliestByRecordCursorFactory) has non-deterministic map iteration order,
            // so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "c\t1970-01-01T03:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (" + query + ") ORDER BY s, ts"
            );
        });
    }

    @Test
    public void testEarliestOnUnionAllEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    "s\tts\n",
                    "SELECT s, ts FROM (" +
                            "SELECT s, ts FROM t1 UNION ALL SELECT s, ts FROM t2" +
                            ") EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnUnionAllReentrant() throws Exception {
        // Tests EarliestByRecordCursorFactory cursor reuse (close+reopen)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T01:00:00'), ('b', '1970-01-01T00:00:00')");

            String query = "SELECT s, ts FROM (" +
                    "SELECT s, ts FROM t UNION ALL SELECT s, ts FROM t WHERE 1 = 0" +
                    ") EARLIEST ON ts PARTITION BY s";

            // Execute twice to exercise cursor close+reopen path
            assertSql("count\n2\n", "SELECT count() FROM (" + query + ")");
            assertSql("count\n2\n", "SELECT count() FROM (" + query + ")");
        });
    }

    @Test
    public void testEarliestOnUnionAllManyPartitions() throws Exception {
        // Tests with more distinct symbols to exercise the DirectLongList sorting path
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T05:00:00'), ('b', '1970-01-01T04:00:00'), " +
                    "('c', '1970-01-01T03:00:00'), ('d', '1970-01-01T02:00:00'), " +
                    "('e', '1970-01-01T01:00:00'), ('a', '1970-01-01T00:00:00')");

            // UNION ALL forces non-random-access, exercises row index sorting in EarliestByRecordCursorFactory.
            // Map iteration order is non-deterministic, so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T04:00:00.000000" + suffix + "\n" +
                            "c\t1970-01-01T03:00:00.000000" + suffix + "\n" +
                            "d\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "e\t1970-01-01T01:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT s, ts FROM (" +
                            "SELECT s, ts FROM t UNION ALL SELECT s, ts FROM t WHERE 1 = 0" +
                            ") EARLIEST ON ts PARTITION BY s) ORDER BY s, ts"
            );
        });
    }

    @Test
    public void testEarliestOnUnionAllMultiplePartitionCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 'x', '1970-01-01T02:00:00'), ('a', 'y', '1970-01-01T01:00:00'), " +
                    "('b', 'x', '1970-01-01T00:00:00')");

            // UNION ALL with multi-column partition by. Map iteration order is non-deterministic,
            // so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s1\ts2\tts\n" +
                            "a\tx\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "a\ty\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "b\tx\t1970-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s1, s2, ts FROM (SELECT s1, s2, ts FROM (" +
                            "SELECT s1, s2, ts FROM t UNION ALL SELECT s1, s2, ts FROM t WHERE 1 = 0" +
                            ") EARLIEST ON ts PARTITION BY s1, s2) ORDER BY s1, s2, ts"
            );
        });
    }

    @Test
    public void testEarliestParserInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t EARLIEST something",
                    29,
                    "'on' or 'by' expected"
            );
        });
    }

    @Test
    public void testEarliestParserMixOldAndNewSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t WHERE s = 'a' EARLIEST BY s EARLIEST ON ts PARTITION BY s",
                    43,
                    "'on' expected"
            );
        });
    }

    @Test
    public void testEarliestAndLatestMixed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t LATEST ON ts PARTITION BY s EARLIEST ON ts PARTITION BY s",
                    48,
                    "cannot use both LATEST and EARLIEST in the same query"
            );
        });
    }

    // Symmetric with testEarliestAndLatestMixed: EARLIEST ON followed by LATEST ON must be
    // rejected with the same dedicated message, not a generic "unexpected token".
    @Test
    public void testEarliestAndLatestMixedReversed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t EARLIEST ON ts PARTITION BY s LATEST ON ts PARTITION BY s",
                    50,
                    "cannot use both LATEST and EARLIEST in the same query"
            );
        });
    }

    // Guards the reverse of testEarliestAndLatestMixed: deprecated EARLIEST BY followed by
    // new-syntax LATEST ON must also be rejected. See SqlParser's new-latest block guard.
    @Test
    public void testEarliestByThenLatestOnRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t EARLIEST BY s LATEST ON ts PARTITION BY s",
                    34,
                    "cannot use both LATEST and EARLIEST in the same query"
            );
        });
    }

    @Test
    public void testEarliestOnNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a','b') s, rnd_int(0,100,0) v, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            assertException(
                    "SELECT s, v FROM (SELECT s, v FROM t) EARLIEST ON ts PARTITION BY s",
                    50,
                    "Invalid column: ts"
            );
        });
    }

    @Test
    public void testEarliestWhereAfterEarliestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts)");

            assertException(
                    "SELECT s, ts FROM t EARLIEST ON ts PARTITION BY s WHERE s = 'a'",
                    50,
                    "unexpected where clause after 'earliest on'"
            );
        });
    }

    @Test
    public void testEarliestByMultiplePartitionColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s1, rnd_symbol('x', 'y') s2, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");

            // partial row assertion: input data uses rnd_symbol so specific ts values per (s1,s2)
            // depend on the PRNG seed. Assert that all 4 combinations of (s1, s2) appear exactly
            // once in ORDER BY s1, s2 order, which is stronger than a count().
            assertSql(
                    "s1\ts2\n" +
                            "a\tx\n" +
                            "a\ty\n" +
                            "b\tx\n" +
                            "b\ty\n",
                    "SELECT s1, s2 FROM (SELECT ts, s1, s2 FROM t EARLIEST ON ts PARTITION BY s1, s2) ORDER BY s1, s2"
            );
        });
    }

    @Test
    public void testEarliestByWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c') s, rnd_int(0,100,0) v, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // WHERE + EARLIEST ON triggers the table query with filter path.
            // partial row assertion: input uses rnd_symbol/rnd_int so exact ts per symbol depends
            // on the PRNG seed. Assert that all 3 symbols appear once in ORDER BY s.
            String query = "SELECT ts, s FROM t WHERE v > 50 EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n" +
                            "c\n",
                    "SELECT s FROM (" + query + ") ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByWithIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s, " +
                    "timestamp_sequence('2024-01-01', 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(48)) TIMESTAMP(ts) PARTITION BY DAY");

            // Interval filter with earliest on.
            // partial row assertion: input uses rnd_symbol so exact ts per symbol depends on the
            // PRNG seed. Assert that both 'a' and 'b' appear once in ORDER BY s.
            String query = "SELECT ts, s FROM t WHERE ts >= '2024-01-01T12:00:00' AND ts < '2024-01-02' EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n",
                    "SELECT s FROM (" + query + ") ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByExcludedValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // WHERE s NOT IN triggers excludedKeyValues path.
            // partial row assertion: input uses rnd_symbol so exact ts per symbol depends on the
            // PRNG seed. Assert that 'a' and 'b' each appear once and 'c' is absent.
            String query = "SELECT ts, s FROM t WHERE s != 'c' EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n",
                    "SELECT s FROM (" + query + ") ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByMultipleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c', 'd') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // Multiple values in IN list triggers multi-value path.
            // partial row assertion: input uses rnd_symbol so exact ts per symbol depends on the
            // PRNG seed. Assert that 'a', 'b', 'c' each appear once and 'd' is absent.
            String query = "SELECT ts, s FROM t WHERE s IN ('a', 'b', 'c') EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n" +
                            "c\n",
                    "SELECT s FROM (" + query + ") ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByReentrantCursor() throws Exception {
        // Tests cursor reuse (close + reopen path) on direct table scan
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String expected = "ts\ts\n" +
                    "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                    "1970-01-01T02:00:00.000000" + suffix + "\tb\n";

            // Execute twice to test cursor reuse
            assertSql(expected, "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s");
            assertSql(expected, "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s");
        });
    }

    // Reuses the same factory across two cursor runs with a mid-scan failure on the first. The
    // EarliestByLight cursor carries a map whose contents survive close() and are reused on the
    // next of() call; a fix that only cleared the map on the happy path would let stale keys
    // poison the second run. This test guards that invariant.
    @Test
    public void testEarliestByLightReentrantAfterException() throws Exception {
        final AtomicBoolean failNext = new AtomicBoolean(true);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failNext.get() && Utf8s.containsAscii(name, "1970-01-02")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            // Three partitions. The earliest 'a' is at the start of partition 1, the earliest 'b'
            // is at the start of partition 1. The data in partition 2/3 is there to force the
            // subquery to try to open those partitions when no LIMIT prunes them.
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-02T00:00:00'), ('b', '1970-01-02T01:00:00'), " +
                    "('a', '1970-01-03T00:00:00'), ('b', '1970-01-03T01:00:00')");

            // Sub-query forces the EarliestByLight path rather than the direct table path.
            try (RecordCursorFactory factory = select(
                    "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s"
            )) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // consume until the FS layer throws
                    }
                    Assert.fail("Expected CairoException from injected FS failure");
                } catch (CairoException expected) {
                    // pass
                }

                failNext.set(false);
                String suffix = getTimestampSuffix(timestampType.getTypeName());
                assertCursor(
                        "s\tts\n" +
                                "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                                "b\t1970-01-01T01:00:00.000000" + suffix + "\n",
                        factory,
                        true,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testEarliestByConstantFilter() throws Exception {
        // Tests the constant filter optimization (filter always false)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    "s\tts\n",
                    "SELECT s, ts FROM t WHERE 1 = 0 EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestByNonIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T01:00:00'), ('b', '1970-01-01T00:00:00'), " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T02:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 1, '1970-01-01T01:00:00'), ('b', 2, '1970-01-01T00:00:00'), " +
                    "('a', 3, '1970-01-01T00:00:00'), ('b', 4, '1970-01-01T02:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Indexed symbol with single value filter.
            // The indexed fast path exits on first match so it cannot report a concrete size,
            // mirroring LatestByValueIndexedFilteredRecordCursor.size() == -1.
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t3\n",
                    "SELECT ts, s, v FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByIndexedSymbolSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T02:00:00'), ('b', '1970-01-01T00:00:00'), " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Indexed fast path exits on first match, no pre-computed size (matches LATEST).
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByNonSymbolColumn() throws Exception {
        // Non-symbol partition column goes through AllFiltered path
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '1970-01-01T02:00:00'), (2, '1970-01-01T00:00:00'), " +
                    "(1, '1970-01-01T00:00:00'), (2, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\t2\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\t1\n",
                    "SELECT ts, v FROM t EARLIEST ON ts PARTITION BY v"
            );
        });
    }

    @Test
    public void testEarliestByDeprecatedSyntaxMultiColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s1, rnd_symbol('x', 'y') s2, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");

            // Deprecated EARLIEST BY with multiple columns
            assertSql(
                    "count\n4\n",
                    "SELECT count() FROM (SELECT ts, s1, s2 FROM t EARLIEST BY s1, s2)"
            );
        });
    }

    @Test
    public void testEarliestByLargeDataset() throws Exception {
        // Larger dataset to exercise cursor iteration and map building more thoroughly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c', 'd', 'e') s, rnd_int(0,1000,0) v, " +
                    "timestamp_sequence(0, 1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10_000)) TIMESTAMP(ts) PARTITION BY HOUR");

            // partial row assertion: input uses rnd_symbol so exact ts per symbol depends on the
            // PRNG seed. Assert that all 5 symbols appear exactly once in ORDER BY s.
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n" +
                            "c\n" +
                            "d\n" +
                            "e\n",
                    "SELECT s FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s) ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByWithFilterAndSingleValue() throws Exception {
        // WHERE filter + single symbol value earliest by
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T02:00:00'), ('b', 20, '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T00:00:00'), ('b', 40, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t30\n",
                    "SELECT ts, s, v FROM t WHERE s = 'a' AND v > 15 EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestByManySymbolValues() throws Exception {
        // Tests with many distinct partition values to exercise map growth
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t') s, " +
                    "timestamp_sequence(0, 1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(500)) TIMESTAMP(ts) PARTITION BY HOUR");

            // Many distinct symbols through direct table scan.
            // partial row assertion: input uses rnd_symbol so exact ts per symbol depends on the
            // PRNG seed. Assert that all 20 symbols appear exactly once in ORDER BY s.
            assertSql(
                    "s\n" +
                            "a\n" +
                            "b\n" +
                            "c\n" +
                            "d\n" +
                            "e\n" +
                            "f\n" +
                            "g\n" +
                            "h\n" +
                            "i\n" +
                            "j\n" +
                            "k\n" +
                            "l\n" +
                            "m\n" +
                            "n\n" +
                            "o\n" +
                            "p\n" +
                            "q\n" +
                            "r\n" +
                            "s\n" +
                            "t\n",
                    "SELECT s FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s) ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestByExcludedSymbolNoFilter() throws Exception {
        // Triggers findRestrictedExcludedOnlyNoFilter in EarliestByValueListRecordCursor
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), ('a', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s != 'c' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByExcludedSymbolWithFilter() throws Exception {
        // Triggers findRestrictedExcludedOnlyWithFilter in EarliestByValueListRecordCursor
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T00:00:00'), ('b', 20, '1970-01-01T01:00:00'), " +
                    "('c', 30, '1970-01-01T02:00:00'), ('a', 40, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\tv\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\t20\n",
                    "SELECT ts, s, v FROM t WHERE s != 'c' AND s != 'a' AND v > 15 EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestByIncludedAndExcludedSymbols() throws Exception {
        // Triggers findRestrictedNoFilter/findRestrictedWithFilter paths
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), ('d', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // IN list with NOT EQUAL: includedSymbolKeys + excludedSymbolKeys
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tc\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', 'b', 'c') AND s != 'b' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByNonExistentSymbolValue() throws Exception {
        // Triggers VALUE_NOT_FOUND path in lookupDeferredSymbols
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', 'nonexistent') EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByNonExistentExcludedSymbol() throws Exception {
        // Triggers VALUE_NOT_FOUND path in excluded symbol lookup
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Exclude a symbol that doesn't exist in the table
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s != 'nonexistent' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithInSubquery() throws Exception {
        // Tests EARLIEST ON with WHERE s IN (SELECT ...)  -  exercises EarliestBySubQueryRecordCursorFactory
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T02:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T03:00:00'), ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T04:00:00')");

            execute("CREATE TABLE keys (k SYMBOL)");
            execute("INSERT INTO keys VALUES ('a'), ('b')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s IN (SELECT k FROM keys) EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByAllSymbolsEarlyTermination() throws Exception {
        // Tests early termination when all symbol combinations found in first partition
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            // All combinations appear in first day, second day should not be scanned
            execute("INSERT INTO t VALUES ('a', 'x', '1970-01-01T00:00:00'), ('a', 'y', '1970-01-01T01:00:00'), " +
                    "('b', 'x', '1970-01-01T02:00:00'), ('b', 'y', '1970-01-01T03:00:00'), " +
                    "('a', 'x', '1970-01-02T00:00:00'), ('b', 'y', '1970-01-02T01:00:00')");

            // All 4 combinations found in first partition - exercises early termination.
            // Map iteration order is non-deterministic, so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s1\ts2\tts\n" +
                            "a\tx\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "a\ty\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "b\tx\t1970-01-01T02:00:00.000000" + suffix + "\n" +
                            "b\ty\t1970-01-01T03:00:00.000000" + suffix + "\n",
                    "SELECT s1, s2, ts FROM (SELECT ts, s1, s2 FROM t EARLIEST ON ts PARTITION BY s1, s2) ORDER BY s1, s2, ts"
            );
        });
    }

    @Test
    public void testEarliestByAllSymbolsWithFilter() throws Exception {
        // Multi-symbol partition with WHERE filter
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 'x', 10, '1970-01-01T00:00:00'), ('a', 'y', 20, '1970-01-01T01:00:00'), " +
                    "('b', 'x', 30, '1970-01-01T02:00:00'), ('a', 'x', 40, '1970-01-01T03:00:00')");

            // Filter narrows results. Map iteration order is non-deterministic,
            // so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s1\ts2\tts\n" +
                            "a\tx\t1970-01-01T03:00:00.000000" + suffix + "\n" +
                            "a\ty\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "b\tx\t1970-01-01T02:00:00.000000" + suffix + "\n",
                    "SELECT s1, s2, ts FROM (SELECT ts, s1, s2 FROM t WHERE v > 15 EARLIEST ON ts PARTITION BY s1, s2) ORDER BY s1, s2, ts"
            );
        });
    }

    @Test
    public void testEarliestByIndexedSymbolNoFilter() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        // Triggers EarliestByAllIndexedRecordCursorFactory without filter
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T02:00:00'), ('b', 20, '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T00:00:00'), ('b', 40, '1970-01-01T03:00:00')");

            // No WHERE clause with indexed symbol column. Iteration order is non-deterministic,
            // so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tv\tts\n" +
                            "a\t30\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t20\t1970-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s, v, ts FROM (SELECT ts, s, v FROM t EARLIEST ON ts PARTITION BY s) ORDER BY s, ts"
            );
        });
    }

    @Test
    public void testEarliestByIndexedSymbolMultiplePartitions() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        // Tests indexed scan across multiple table partitions
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-02T00:00:00'), " +
                    "('a', '1970-01-02T00:00:00'), ('b', '1970-01-01T00:00:00')");

            // Indexed symbol scan across partition boundaries. Iteration order is non-deterministic,
            // so wrap in ORDER BY to assert exact rows.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s) ORDER BY s, ts"
            );
        });
    }

    @Test
    public void testEarliestByWithIntervalFilterDeterministic() throws Exception {
        // Triggers IntervalPartitionFrameCursorFactory path in generateEarliestByTableQuery
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T06:00:00'), " +
                    "('a', '1970-01-01T12:00:00'), ('b', '1970-01-01T18:00:00'), " +
                    "('a', '1970-01-02T00:00:00'), ('b', '1970-01-02T06:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T12:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T18:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE ts >= '1970-01-01T12:00:00' AND ts < '1970-01-02' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s LIMIT 1"
            );
        });
    }

    @Test
    public void testEarliestByConfigurationRowCountOverride() throws Exception {
        // Pins that cairo.sql.earliest.by.row.count actually drives the rows DirectLongList
        // initial capacity for EARLIEST paths: set a small value and verify the factory
        // still produces correct results when the list has to grow past the seeded size.
        setProperty(PropertyKey.CAIRO_SQL_EARLIEST_BY_ROW_COUNT, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (" +
                    "SELECT rnd_symbol('a', 'b', 'c', 'd', 'e') s, " +
                    "timestamp_sequence(0, 60 * 60 * 1_000_000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(25)) TIMESTAMP(ts) PARTITION BY DAY");

            // Expect one row per distinct symbol (more than the configured initial capacity of 1).
            assertQueryNoLeakCheck(
                    "count\n5\n",
                    "SELECT count() FROM (SELECT s, ts FROM t EARLIEST ON ts PARTITION BY s)",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnTableWithoutDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ")");

            assertException(
                    "SELECT * FROM t EARLIEST ON ts PARTITION BY s",
                    28,
                    "earliest by over a table requires designated TIMESTAMP"
            );
        });
    }

    @Test
    public void testEarliestOnWithSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 10, '1970-01-01T00:00:00'), " +
                    "('b', 20, '1970-01-01T01:00:00'), " +
                    "('a', 30, '1970-01-01T02:00:00'), " +
                    "('b', 40, '1970-01-01T03:00:00'), " +
                    "('a', 50, '1970-01-02T00:00:00'), " +
                    "('b', 60, '1970-01-02T01:00:00')");

            // EARLIEST ON + SAMPLE BY should work (consistent with LATEST ON + SAMPLE BY).
            // Row ordering from SAMPLE BY hashmap is non-deterministic; wrap in ORDER BY to
            // assert exact (sym, sum) pairs, which is stronger than a plain count().
            assertSql(
                    "sym\tsum_v\n" +
                            "a\t10\n" +
                            "b\t20\n",
                    "SELECT sym, sum_v FROM (" +
                            "SELECT s sym, sum(v) sum_v FROM t EARLIEST ON ts PARTITION BY s SAMPLE BY 1d" +
                            ") ORDER BY sym"
            );
        });
    }

    @Test
    public void testEarliestOnNonTimestampColumnErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");

            assertException(
                    "SELECT * FROM t EARLIEST ON v PARTITION BY s",
                    28,
                    "not a TIMESTAMP"
            );
        });
    }

    @Test
    public void testEarliestOnMixedSymbolAndIntPartitionKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('a', 2, '1970-01-01T01:00:00'), " +
                    "('b', 1, '1970-01-01T02:00:00'), " +
                    "('a', 1, '1970-01-01T03:00:00'), " +
                    "('b', 2, '1970-01-01T04:00:00'), " +
                    "('b', 1, '1970-01-01T05:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t1\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\t2\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\t1\n" +
                            "1970-01-01T04:00:00.000000" + suffix + "\tb\t2\n",
                    "SELECT ts, s, v FROM t EARLIEST ON ts PARTITION BY s, v",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnMixedSymbolAndVarcharPartitionKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, name VARCHAR, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 'alice', '1970-01-01T00:00:00'), " +
                    "('a', 'bob',   '1970-01-01T01:00:00'), " +
                    "('b', 'alice', '1970-01-01T02:00:00'), " +
                    "('a', 'alice', '1970-01-01T03:00:00'), " +
                    "('b', 'bob',   '1970-01-01T04:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\tname\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\talice\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\tbob\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\talice\n" +
                            "1970-01-01T04:00:00.000000" + suffix + "\tb\tbob\n",
                    "SELECT ts, s, name FROM t EARLIEST ON ts PARTITION BY s, name",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnWalOutOfOrderInserts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T03:00:00'), " +
                    "('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-01T01:00:00'), " +
                    "('b', '1970-01-01T04:00:00'), " +
                    "('a', '1970-01-01T05:00:00'), " +
                    "('b', '1970-01-01T00:00:00')");
            drainWalQueue();

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnWalMultipleBatchesOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            // First batch
            execute("INSERT INTO t VALUES ('a', '1970-01-01T10:00:00'), ('b', '1970-01-01T10:00:00')");
            drainWalQueue();

            // Second batch with earlier timestamps  -  WAL must reorder
            execute("INSERT INTO t VALUES ('a', '1970-01-01T01:00:00'), ('b', '1970-01-01T02:00:00')");
            drainWalQueue();

            // Third batch spanning two partitions
            execute("INSERT INTO t VALUES ('c', '1970-01-02T00:00:00'), ('c', '1970-01-01T00:30:00')");
            drainWalQueue();

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\tc\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnWithOrderByDescending() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), " +
                    "('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), " +
                    "('a', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tc\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s ORDER BY ts DESC"
            );
        });
    }

    @Test
    public void testEarliestOnOverGroupBySubquery() throws Exception {
        // Inner subquery has implicit GROUP BY s, ts (since both are non-aggregated alongside sum(v)).
        // Each row becomes its own group, so sum(v) == v. EARLIEST ON picks the first row per symbol.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 10, '1970-01-01T00:00:00'), " +
                    "('b', 20, '1970-01-01T01:00:00'), " +
                    "('a', 30, '1970-01-01T02:00:00'), " +
                    "('b', 40, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\ttotal\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t10\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\t20\n",
                    "SELECT ts, s, total FROM (SELECT s, sum(v) total, ts FROM t TIMESTAMP(ts)) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnDuplicateTimestampsReturnsDeterministicResult() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('a', 2, '1970-01-01T00:00:00'), " +
                    "('a', 3, '1970-01-01T00:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // The first inserted row should win since all timestamps are equal
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t1\n",
                    "SELECT ts, s, v FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnManyDistinctKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT ('s' || x)::symbol s, " +
                    "timestamp_sequence(0, 60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(1000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            // Each of the 1000 symbols appears exactly once, so EARLIEST ON returns all rows.
            // Map iteration order is non-deterministic, so ORDER BY and assert the full count
            // plus the first and last rows by symbol name length, then by ts.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "cnt\tmin_ts\tmax_ts\n" +
                            "1000\t1970-01-01T00:00:00.000000" + suffix
                            + "\t1970-01-01T16:39:00.000000" + suffix + "\n",
                    "SELECT count() cnt, min(ts) min_ts, max(ts) max_ts FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s)"
            );
            // Additionally verify ORDER BY works over the result so row identity is observable.
            assertSql(
                    "s\tts\n" +
                            "s1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "s10\t1970-01-01T00:09:00.000000" + suffix + "\n" +
                            "s100\t1970-01-01T01:39:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s) ORDER BY s, ts LIMIT 3"
            );
        });
    }

    @Test
    public void testEarliestOnInUnionAllBranches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t2 (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t1 VALUES ('a', '1970-01-01T00:00:00'), ('a', '1970-01-01T01:00:00'), ('b', '1970-01-01T02:00:00')");
            execute("INSERT INTO t2 VALUES ('c', '1970-01-02T00:00:00'), ('c', '1970-01-02T01:00:00'), ('d', '1970-01-02T02:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n" +
                            "1970-01-02T00:00:00.000000" + suffix + "\tc\n" +
                            "1970-01-02T02:00:00.000000" + suffix + "\td\n",
                    "SELECT ts, s FROM t1 EARLIEST ON ts PARTITION BY s " +
                            "UNION ALL " +
                            "SELECT ts, s FROM t2 EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnWithFilterThatMatchesNothing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00')");

            assertQuery(
                    "ts\ts\n",
                    "SELECT ts, s FROM t WHERE s = 'nonexistent' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestOnPartitionByBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(true, '1970-01-01T00:00:00'), " +
                    "(false, '1970-01-01T01:00:00'), " +
                    "(true, '1970-01-01T02:00:00'), " +
                    "(false, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\tb\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ttrue\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tfalse\n",
                    "SELECT ts, b FROM t EARLIEST ON ts PARTITION BY b",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnPartitionByLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id LONG, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(100, '1970-01-01T00:00:00'), " +
                    "(200, '1970-01-01T01:00:00'), " +
                    "(100, '1970-01-01T02:00:00'), " +
                    "(200, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\tid\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\t100\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\t200\n",
                    "SELECT ts, id FROM t EARLIEST ON ts PARTITION BY id",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnPartitionByDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.5, '1970-01-01T00:00:00'), " +
                    "(2.5, '1970-01-01T01:00:00'), " +
                    "(1.5, '1970-01-01T02:00:00'), " +
                    "(2.5, '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\t1.5\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\t2.5\n",
                    "SELECT ts, v FROM t EARLIEST ON ts PARTITION BY v",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlan() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");

            // Verify EXPLAIN output exercises the EarliestBy plan sink
            assertSql(
                    "QUERY PLAN\n" +
                            "EarliestByDeferredListValuesFiltered\n" +
                            "    Frame forward scan on: t\n",
                    "EXPLAIN SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanAllFiltered() throws Exception {
        // Routes to EarliestByAllFilteredRecordCursorFactory: non-symbol partition column.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, x INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT * FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY x",
                    "EarliestByAllFiltered\n" +
                            "    Row forward scan\n" +
                            "      filter: s='a'\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanAllSymbolsFiltered() throws Exception {
        // Routes to EarliestByAllSymbolsFilteredRecordCursorFactory: multi-column symbol partition with filter.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, x INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT * FROM t WHERE x > 0 EARLIEST ON ts PARTITION BY s1, s2",
                    "EarliestByAllSymbolsFiltered\n" +
                            "  filter: 0<x\n" +
                            "    Row forward scan\n" +
                            "      expectedSymbolsCount: 4611686014132420609\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    // Guards the NO_OP_FILTER cleanup in EarliestByAllSymbolsFilteredRecordCursor.getFilter():
    // a query without a WHERE clause must NOT render a misleading 'filter: true' line.
    @Test
    public void testEarliestOnExplainPlanAllSymbolsFilteredNoFilter() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT * FROM t EARLIEST ON ts PARTITION BY s1, s2",
                    "EarliestByAllSymbolsFiltered\n" +
                            "    Row forward scan\n" +
                            "      expectedSymbolsCount: 4611686014132420609\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanSubQuery() throws Exception {
        // Routes to EarliestBySubQueryRecordCursorFactory: WHERE s IN (subquery) with single-symbol partition.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE keys (s SYMBOL)");
            assertPlanNoLeakCheck(
                    "SELECT * FROM t WHERE s IN (SELECT s FROM keys) EARLIEST ON ts PARTITION BY s",
                    "EarliestBySubQuery\n" +
                            "    Subquery\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: keys\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanSubQueryIndexed() throws Exception {
        // Routes to EarliestBySubQueryRecordCursorFactory with indexed=true: WHERE s IN (subquery)
        // on an indexed SYMBOL column switches to per-key bitmap lookups.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE keys (s SYMBOL)");
            assertPlanNoLeakCheck(
                    "SELECT * FROM t WHERE s IN (SELECT s FROM keys) EARLIEST ON ts PARTITION BY s",
                    "EarliestBySubQuery\n" +
                            "    Subquery\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: keys\n" +
                            "    Index forward scan on: s\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestOnInSubQueryIndexedReturnsCorrectRows() throws Exception {
        // Exercises the indexed path in EarliestBySubQueryRecordCursorFactory. The data has two
        // partitions, so the earliest row per matched symbol lives in the first partition and must
        // be returned by the per-symbol bitmap lookup.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE keys (s SYMBOL)");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), ('a', '1970-01-02T03:00:00'), " +
                    "('b', '1970-01-02T04:00:00'), ('c', '1970-01-02T05:00:00')");
            execute("INSERT INTO keys VALUES ('a'), ('c')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tc\n",
                    "SELECT ts, s FROM t WHERE s IN (SELECT s FROM keys) EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnInSubQueryIndexedWithFilter() throws Exception {
        // Indexed subquery path with a WHERE filter: uses EarliestByValuesIndexedFilteredRecordCursor.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE keys (s SYMBOL)");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), ('b', 5, '1970-01-01T01:00:00'), " +
                    "('c', 1, '1970-01-01T02:00:00'), ('a', 5, '1970-01-02T03:00:00'), " +
                    "('b', 5, '1970-01-02T04:00:00'), ('c', 5, '1970-01-02T05:00:00')");
            execute("INSERT INTO keys VALUES ('a'), ('c')");

            // Filter excludes the earliest 'a' (v=1) and the earliest 'c' (v=1), so the
            // earliest-matching rows land in the second partition.
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-02T03:00:00.000000" + suffix + "\ta\t5\n" +
                            "1970-01-02T05:00:00.000000" + suffix + "\tc\t5\n",
                    "SELECT ts, s, v FROM t WHERE s IN (SELECT s FROM keys) AND v = 5 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanLight() throws Exception {
        // Routes to EarliestByLightRecordCursorFactory: subselect that supports random access.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT * FROM (SELECT s, ts FROM t LIMIT 100) EARLIEST ON ts PARTITION BY s",
                    "SelectedRecord\n" +
                            "    EarliestBy light order_by_timestamp: true\n" +
                            "        Limit value: 100 skip-rows: 0 take-rows: 0\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanNoRandomAccess() throws Exception {
        // Routes to EarliestByRecordCursorFactory: UNION ALL strips random-access support.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (s SYMBOL, ts " + timestampType.getTypeName() + ")");
            execute("CREATE TABLE t2 (s SYMBOL, ts " + timestampType.getTypeName() + ")");
            assertPlanNoLeakCheck(
                    "SELECT s, ts FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2) EARLIEST ON ts PARTITION BY s",
                    "SelectedRecord\n" +
                            "    EarliestBy\n" +
                            "        Union All\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t1\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t2\n"
            );
        });
    }

    @Test
    public void testEarliestOnWithInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE keys (s SYMBOL)");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), ('a', '1970-01-01T03:00:00')");
            execute("INSERT INTO keys VALUES ('a'), ('c')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tc\n",
                    "SELECT ts, s FROM t WHERE s IN (SELECT s FROM keys) EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnWithNotInBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00')");

            bindVariableService.clear();
            bindVariableService.setStr("excluded", "b");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tc\n",
                    "SELECT ts, s FROM t WHERE s != :excluded EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByWithInSubqueryNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00'), ('b', '1970-01-01T01:00:00')");
            execute("CREATE TABLE keys (k SYMBOL)");
            execute("INSERT INTO keys VALUES ('x'), ('y')");
            // Subquery returns symbols not present in t  -  should return empty result
            assertSql(
                    "ts\ts\n",
                    "SELECT ts, s FROM t WHERE s IN (SELECT k FROM keys) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnSubQueryNullTimestamps() throws Exception {
        // Exercises the buildMapForUnorderedSubQuery path where NULL timestamps
        // should not win over valid timestamps as "earliest"
        assertMemoryLeak(() -> {
            // Tables without designated timestamp allow NULL in the ts column
            execute("CREATE TABLE t1 (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ")");
            execute("INSERT INTO t1 VALUES ('a', 1, NULL), ('a', 2, '1970-01-01T01:00:00'), ('b', 3, '1970-01-01T00:00:00')");
            execute("CREATE TABLE t2 (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ")");
            execute("INSERT INTO t2 VALUES ('a', 4, '1970-01-01T02:00:00'), ('b', 5, NULL)");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // UNION ALL removes random access, forcing EarliestByRecordCursorFactory path.
            // 'a' has rows at NULL, 01:00, 02:00  -  earliest non-NULL is 01:00 (v=2)
            // 'b' has rows at 00:00, NULL  -  earliest non-NULL is 00:00 (v=3)
            assertSql(
                    "s\tv\tts\n" +
                            "a\t2\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "b\t3\t1970-01-01T00:00:00.000000" + suffix + "\n",
                    "SELECT s, v, ts FROM (" +
                            "SELECT * FROM t1 UNION ALL SELECT * FROM t2" +
                            ") EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    @Test
    public void testEarliestOnPreservedThroughArtificialStarSubquery() throws Exception {
        // Verifies that the parser's subquery-collapse optimisation does not
        // strip EARLIEST ON when it sees an artificial SELECT * wrapper.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), " +
                    "('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-01T02:00:00'), " +
                    "('b', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Without the fix, the collapse would turn
            //   SELECT * FROM (t EARLIEST ON ts PARTITION BY s)
            // into just "t"  -  returning all 4 rows instead of 2.
            assertQuery(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\n",
                    "SELECT * FROM (SELECT * FROM t EARLIEST ON ts PARTITION BY s)",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByIndexedMultipleFramesPerPartition() throws Exception {
        // Forces a single partition to span multiple page frames so that
        // EarliestByAllIndexedRecordCursor encounters frames with partitionLo > 0.
        // The bitmap index reader returns frame-local row ids (it subtracts minValue
        // == partitionLo internally); this test guards against any future caller that
        // accidentally re-subtracts partitionLo.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            // 12 rows in a single partition, split into 3 frames of 4 rows each.
            // Earliest 'a' is v=1 at 00:00, earliest 'b' is v=2 at 00:01 (both in frame 0).
            // Earliest 'c' is v=9 at 00:08 (in frame 2, partitionLo=8). The revert-guard
            // matters most for 'c', which lives in a non-zero-partitionLo frame.
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('b', 2, '1970-01-01T00:00:01'), " +
                    "('a', 3, '1970-01-01T00:00:02'), " +
                    "('b', 4, '1970-01-01T00:00:03'), " +
                    "('a', 5, '1970-01-01T00:00:04'), " +
                    "('b', 6, '1970-01-01T00:00:05'), " +
                    "('a', 7, '1970-01-01T00:00:06'), " +
                    "('b', 8, '1970-01-01T00:00:07'), " +
                    "('c', 9, '1970-01-01T00:00:08'), " +
                    "('c', 10, '1970-01-01T00:00:09'), " +
                    "('c', 11, '1970-01-01T00:00:10'), " +
                    "('c', 12, '1970-01-01T00:00:11')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\tv\tts\n" +
                            "a\t1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t2\t1970-01-01T00:00:01.000000" + suffix + "\n" +
                            "c\t9\t1970-01-01T00:00:08.000000" + suffix + "\n",
                    "SELECT s, v, ts FROM t EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnInvalidPartitionByColumnErrorMessage() throws Exception {
        // EARLIEST ON validation errors must say "EARLIEST ON", not "LATEST ON".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BINARY, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            try {
                select("SELECT * FROM t EARLIEST ON ts PARTITION BY b").close();
                org.junit.Assert.fail("expected SqlException");
            } catch (SqlException e) {
                String msg = e.getFlyweightMessage().toString();
                org.junit.Assert.assertTrue(
                        "error message should mention EARLIEST ON but was: " + msg,
                        msg.contains("EARLIEST ON")
                );
                org.junit.Assert.assertFalse(
                        "error message must not mention LATEST ON but was: " + msg,
                        msg.contains("LATEST ON")
                );
            }
        });
    }

    @Test
    public void testEarliestOnSubQueryOrderedNullTimestamps() throws Exception {
        // Drives the buildMapForOrderedSubQuery path (random-access sub-query whose
        // timestamp matches the designated one) with NULL timestamps. The ordered path
        // must skip NULL-timestamp rows instead of pinning them as "earliest", and must
        // produce the same row set as the unordered path for keys that have at least
        // one non-NULL row.
        assertMemoryLeak(() -> {
            // No designated timestamp so the ts column can hold NULLs.
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ")");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, NULL), " +
                    "('a', 2, '1970-01-01T02:00:00'), " +
                    "('a', 3, '1970-01-01T01:00:00'), " +
                    "('b', 4, NULL), " +
                    "('b', 5, '1970-01-01T00:00:00'), " +
                    "('c', 6, NULL), " +
                    "('c', 7, NULL)");

            // ORDER BY ts places NULLs first in ascending order. The earliest
            // non-NULL for 'a' is 01:00 (v=3), for 'b' is 00:00 (v=5). 'c' has
            // only NULL rows and must be omitted.
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (" +
                            "SELECT s, v, ts FROM (SELECT * FROM t ORDER BY ts) EARLIEST ON ts PARTITION BY s" +
                            ")"
            );
            assertSql(
                    "s\tv\n" +
                            "a\t3\n" +
                            "b\t5\n",
                    "SELECT s, v FROM (" +
                            "SELECT s, v, ts FROM (SELECT * FROM t ORDER BY ts) EARLIEST ON ts PARTITION BY s" +
                            ") ORDER BY s"
            );
        });
    }

    @Test
    public void testEarliestOnValueListExcludedSymbolsNotInTable() throws Exception {
        // Verifies that EarliestByValueListRecordCursor's excluded-only scan correctly
        // subtracts only excluded keys that actually exist in the symbol table. A
        // previous iteration subtracted the full excluded set, which underestimated the
        // distinct count and terminated the scan before every reachable row was found.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T00:00:00'), " +
                    "('b', '1970-01-01T01:00:00'), " +
                    "('c', '1970-01-01T02:00:00'), " +
                    "('a', '1970-01-01T03:00:00'), " +
                    "('b', '1970-01-01T04:00:00'), " +
                    "('c', '1970-01-01T05:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // Exclude a mix: one real symbol ('a') and two that don't exist in the
            // table. Expected: earliest of 'b' and 'c' only.
            assertQuery(
                    "s\tts\n" +
                            "b\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "c\t1970-01-01T02:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM t WHERE s NOT IN ('a', 'zzz', 'missing') EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByIndexedSingleLiteralWithFilter() throws Exception {
        // Indexed symbol, single literal key value, extra WHERE filter -> routes to
        // EarliestByValueIndexedFilteredRecordCursorFactory + EarliestByValueIndexedFilteredRecordCursor.
        // The cursor must walk the bitmap index forward and skip rows that fail the
        // additional filter, returning the earliest surviving row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 5,  '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T01:00:00'), " +
                    "('a', 10, '1970-01-01T02:00:00'), " +
                    "('b', 50, '1970-01-01T00:30:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // v=5 is earliest by ts for 'a' but fails v >= 20, so v=30 wins.
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\t30\n",
                    "SELECT ts, s, v FROM t WHERE s = 'a' AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            assertPlanNoLeakCheck(
                    "SELECT ts, s, v FROM t WHERE s = 'a' AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "Index forward scan on: s\n" +
                            "  filter: v>=20\n" +
                            "  symbolFilter: s=1\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestByIndexedSingleBindVarNoFilter() throws Exception {
        // Indexed symbol, single runtime-constant (bind variable) key value, no extra
        // filter -> routes to EarliestByValueDeferredIndexedRowCursorFactory wrapped in
        // PageFrameRecordCursorFactory. The symbol key is unknown at compile time, so
        // the factory must resolve it against the symbol table at cursor-prepare time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T02:00:00'), " +
                    "('a', '1970-01-01T00:00:00'), " +
                    "('b', '1970-01-01T00:30:00'), " +
                    "('b', '1970-01-01T03:00:00')");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "a");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s = :sym EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            // Re-running with a different bind value must pick the earliest row for 'b'.
            bindVariableService.setStr("sym", "b");
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s = :sym EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            // Unknown symbol must return an empty cursor without throwing.
            bindVariableService.setStr("sym", "zzz");
            assertQuery(
                    "ts\ts\n",
                    "SELECT ts, s FROM t WHERE s = :sym EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByIndexedSingleBindVarWithFilter() throws Exception {
        // Indexed symbol, single runtime-constant key value AND an extra filter ->
        // routes to EarliestByValueDeferredIndexedFilteredRecordCursorFactory. The
        // factory must resolve the bind value and build an
        // EarliestByValueIndexedFilteredRecordCursor on top of the bitmap index.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 5,  '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T01:00:00'), " +
                    "('a', 10, '1970-01-01T02:00:00'), " +
                    "('b', 50, '1970-01-01T00:30:00'), " +
                    "('b', 80, '1970-01-01T02:30:00')");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "a");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // v=5 has earliest ts but fails v >= 20, so v=30 wins for 'a'.
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\t30\n",
                    "SELECT ts, s, v FROM t WHERE s = :sym AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            // Change the bind value and re-run the same factory: 'b' passes the filter
            // already at its earliest row.
            bindVariableService.setStr("sym", "b");
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\tb\t50\n",
                    "SELECT ts, s, v FROM t WHERE s = :sym AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByIndexedMultiValueInNoFilter() throws Exception {
        // Indexed symbol, multi-value IN list (two literals), no extra filter -> routes
        // to EarliestByValuesIndexedFilteredRecordCursorFactory with filter == null,
        // which instantiates EarliestByValuesIndexedRecordCursor (the non-filtered
        // multi-value bitmap forward scan). Each distinct requested key must land its
        // own earliest row via the index.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('c', '1970-01-01T00:00:00'), " +
                    "('a', '1970-01-01T00:30:00'), " +
                    "('b', '1970-01-01T01:00:00'), " +
                    "('a', '1970-01-01T02:00:00'), " +
                    "('b', '1970-01-01T03:00:00'), " +
                    "('c', '1970-01-01T04:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', 'b') EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );

            // Mixing known and unknown literals: the unknown one is registered as a
            // deferred key and contributes no rows, while the known ones still resolve.
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', 'zzz') EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    "SELECT ts, s FROM t WHERE s IN ('a', 'b') EARLIEST ON ts PARTITION BY s",
                    "Index forward scan on: s\n" +
                            "  symbolFilter: s in [2,3]\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestByIndexedSingleLiteralAcrossEmptyPartition() throws Exception {
        // Indexed symbol, single literal, no extra filter, but the first partition
        // contains no rows for the requested symbol. This forces
        // EarliestByValueIndexedRowCursorFactory.getCursor() to take the
        // indexReaderCursor.hasNext() == false branch (returning EmptyRowCursor)
        // for the first partition, then resolve successfully in the next one.
        // Also pins the toPlan output so the Index forward scan plan stays stable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('b', '1970-01-01T00:00:00'), " +
                    "('b', '1970-01-01T06:00:00'), " +
                    "('a', '1970-01-02T01:00:00'), " +
                    "('b', '1970-01-02T02:00:00'), " +
                    "('a', '1970-01-02T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // 'a' exists only on 1970-01-02. The 1970-01-01 partition frame must
            // hand back an EmptyRowCursor before the scan advances to 1970-01-02.
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T01:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByIndexedSingleLiteralExplainPlan() throws Exception {
        // Covers EarliestByValueIndexedRowCursorFactory.toPlan. The existing
        // behavioural tests only iterate the cursor; without a plan assertion the
        // toPlan method stayed uncovered and any accidental rewording of the
        // "Index forward scan ... filter: s=N" line would go unnoticed.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', '1970-01-01T00:00:00')");
            assertPlanNoLeakCheck(
                    "SELECT ts, s FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "PageFrame\n" +
                            "    Index forward scan on: s\n" +
                            "      filter: s=1\n" +
                            "    Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testEarliestByNonIndexedSingleLiteralNoFilter() throws Exception {
        // Non-indexed symbol, single literal, no extra WHERE filter -> routes to
        // EarliestByValueFilteredRecordCursorFactory with filter == null, which
        // instantiates EarliestByValueRecordCursor (the non-filtered single-value
        // cursor). Previously existing tests always supplied an additional filter,
        // so the filter == null constructor branch went uncovered.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', '1970-01-01T02:00:00'), " +
                    "('b', '1970-01-01T00:00:00'), " +
                    "('a', '1970-01-01T00:30:00'), " +
                    "('a', '1970-01-01T03:00:00')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:30:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestByNonIndexedSingleBindVarWithFilter() throws Exception {
        // Non-indexed symbol, runtime-constant bind variable key value, extra WHERE
        // filter -> routes to EarliestByValueDeferredFilteredRecordCursorFactory.
        // Its createCursorFor() returns EarliestByValueFilteredRecordCursor on the
        // filter != null branch. The existing non-indexed deferred tests do not
        // pass an extra filter, so that branch went uncovered.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 5,  '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T01:00:00'), " +
                    "('a', 10, '1970-01-01T02:00:00'), " +
                    "('b', 50, '1970-01-01T00:30:00')");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "a");
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            // v=5 has the earliest ts but fails v >= 20, so v=30 wins for 'a'.
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\t30\n",
                    "SELECT ts, s, v FROM t WHERE s = :sym AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            // Unknown bind value: the factory keeps resolving at each cursor open
            // and must return an empty result without throwing.
            bindVariableService.setStr("sym", "zzz");
            assertQuery(
                    "ts\ts\tv\n",
                    "SELECT ts, s, v FROM t WHERE s = :sym AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testEarliestOnExplainPlanValueFilteredVariants() throws Exception {
        // Consolidated plan assertions for the single-value EARLIEST ON factories
        // whose toPlan methods were otherwise unreached. Covers:
        //   - EarliestByValueFilteredRecordCursorFactory (non-indexed literal +
        //     filter): type "EarliestByValueFiltered"
        //   - EarliestByValueFilteredRecordCursorFactory (non-indexed literal,
        //     no filter): chained child is EarliestByValueRecordCursor, not the
        //     filtered cursor
        //   - EarliestByValueDeferredFilteredRecordCursorFactory (non-indexed
        //     bind variable + filter): type "EarliestByValueDeferredFiltered"
        //   - EarliestByValueDeferredIndexedRowCursorFactory (indexed bind
        //     variable, no filter): "Index forward scan ... deferred: true"
        //   - EarliestByValueDeferredIndexedFilteredRecordCursorFactory
        //     (indexed bind variable + filter): deferred indexed filtered plan.
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE plain (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE idx (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO plain VALUES ('a', 1, '1970-01-01T00:00:00')");
            execute("INSERT INTO idx   VALUES ('a', 1, '1970-01-01T00:00:00')");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "a");

            assertPlanNoLeakCheck(
                    "SELECT * FROM plain WHERE s = 'a' AND v > 0 EARLIEST ON ts PARTITION BY s",
                    "EarliestByValueFiltered\n" +
                            "    Row forward scan\n" +
                            "      symbolFilter: s=0\n" +
                            "      filter: 0<v\n" +
                            "    Frame forward scan on: plain\n"
            );

            assertPlanNoLeakCheck(
                    "SELECT * FROM plain WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "EarliestByValueFiltered\n" +
                            "    Row forward scan\n" +
                            "      symbolFilter: s=0\n" +
                            "    Frame forward scan on: plain\n"
            );

            assertPlanNoLeakCheck(
                    "SELECT * FROM plain WHERE s = :sym AND v > 0 EARLIEST ON ts PARTITION BY s",
                    "EarliestByValueDeferredFiltered\n" +
                            "  filter: 0<v\n" +
                            "  symbolFilter: s=:sym::string\n" +
                            "    Frame forward scan on: plain\n"
            );

            assertPlanNoLeakCheck(
                    "SELECT * FROM idx WHERE s = :sym EARLIEST ON ts PARTITION BY s",
                    "PageFrame\n" +
                            "    Index forward scan on: s deferred: true\n" +
                            "      filter: s=:sym::string\n" +
                            "    Frame forward scan on: idx\n"
            );

            assertPlanNoLeakCheck(
                    "SELECT * FROM idx WHERE s = :sym AND v > 0 EARLIEST ON ts PARTITION BY s",
                    "Index forward scan on: s\n" +
                            "  filter: 0<v\n" +
                            "  symbolFilter: s=:sym::string\n" +
                            "    Frame forward scan on: idx\n"
            );
        });
    }

    @Test
    public void testEarliestByIndexedFilteredNoMatch() throws Exception {
        // Exercises the no-match end of EarliestByValueIndexedFilteredRecordCursor
        // .findRecord(): the bitmap index has rows for the requested symbol in
        // every frame, but the extra filter rejects all of them. The outer loop
        // must then exhaust every frame without ever setting isRecordFound.
        // Separately, the indexed multi-value path must produce an empty result
        // when no requested symbol passes the filter, covering the same exhaust
        // branch in EarliestByValuesIndexedFilteredRecordCursor.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('a', 2, '1970-01-01T01:00:00'), " +
                    "('a', 3, '1970-01-02T00:00:00'), " +
                    "('b', 4, '1970-01-02T01:00:00')");

            // Single-value indexed + filter that matches nothing.
            assertQuery(
                    "ts\ts\tv\n",
                    "SELECT ts, s, v FROM t WHERE s = 'a' AND v > 100 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    false
            );

            // Multi-value indexed + filter that matches nothing.
            assertQuery(
                    "ts\ts\tv\n",
                    "SELECT ts, s, v FROM t WHERE s IN ('a', 'b') AND v > 100 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestByIndexedBindVarInListPartialResolution() throws Exception {
        // Hits the deferredSymbolKeys branch of
        // EarliestByValuesIndexedRecordCursor.buildTreeMap() and the mirror
        // branch in EarliestByValuesIndexedFilteredRecordCursor.buildTreeMap().
        // A bind variable in an IN list is a runtime constant: it is NOT
        // resolved at compile time, so it ends up in deferredSymbolFuncs and,
        // once the cursor opens, the resolved key goes into deferredSymbolKeys.
        // Combined with a literal in the same IN list, both symbolKeys and
        // deferredSymbolKeys get populated, so both inner loops run.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 10, '1970-01-01T00:00:00'), " +
                    "('a', 20, '1970-01-01T01:00:00'), " +
                    "('b', 30, '1970-01-01T02:00:00'), " +
                    "('b', 40, '1970-01-01T03:00:00')");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "b");
            String suffix = getTimestampSuffix(timestampType.getTypeName());

            // No extra filter -> EarliestByValuesIndexedRecordCursor with both
            // literal 'a' (symbolKeys) and deferred :sym resolving to 'b'
            // (deferredSymbolKeys).
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\n",
                    "SELECT ts, s FROM t WHERE s IN ('a', :sym) EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );

            // Extra filter -> EarliestByValuesIndexedFilteredRecordCursor on the
            // same split, exercising the filtered deferred-keys inner loop.
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T01:00:00.000000" + suffix + "\ta\t20\n" +
                            "1970-01-01T02:00:00.000000" + suffix + "\tb\t30\n",
                    "SELECT ts, s, v FROM t WHERE s IN ('a', :sym) AND v >= 20 EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testEarliestOnLightUnorderedSubqueryWithNullTimestamps() throws Exception {
        // Drives EarliestByLightRecordCursorFactory.buildMapForUnorderedSubQuery()
        // down its NULL-timestamp continue branch. The sub-query preserves its
        // natural order (no ORDER BY ts), so the light factory picks the
        // unordered build path; rows with NULL ts must be skipped instead of
        // pinning the key, matching the ordered-subquery semantics.
        assertMemoryLeak(() -> {
            // No designated timestamp so the ts column can hold NULLs.
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ")");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, NULL), " +
                    "('a', 2, '1970-01-01T02:00:00'), " +
                    "('a', 3, '1970-01-01T01:00:00'), " +
                    "('b', 4, NULL), " +
                    "('b', 5, '1970-01-01T00:00:00'), " +
                    "('c', 6, NULL)");

            // A sub-select without ORDER BY ts is not timestamp-ascending, so
            // the EarliestBy light factory falls into the unordered build path.
            assertSql(
                    "s\tv\n" +
                            "a\t3\n" +
                            "b\t5\n",
                    "SELECT s, v FROM (SELECT s, v, ts FROM t LIMIT 100) EARLIEST ON ts PARTITION BY s ORDER BY s"
            );
        });
    }

    // Shared dataset builder for the filtered-indexed plan-pin tests below.
    // 12 rows in a single partition; PAGE_FRAME_MAX_ROWS=4 is set at call sites
    // to exercise the frame-max config path through the filtered indexed cursors.
    private void buildIndexedDataset() throws SqlException {
        execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('a', 1, '1970-01-01T00:00:00'), " +
                "('b', 2, '1970-01-01T00:00:01'), " +
                "('a', 3, '1970-01-01T00:00:02'), " +
                "('b', 4, '1970-01-01T00:00:03'), " +
                "('a', 5, '1970-01-01T00:00:04'), " +
                "('b', 6, '1970-01-01T00:00:05'), " +
                "('a', 7, '1970-01-01T00:00:06'), " +
                "('b', 8, '1970-01-01T00:00:07'), " +
                "('c', 9, '1970-01-01T00:00:08'), " +
                "('c', 10, '1970-01-01T00:00:09'), " +
                "('c', 11, '1970-01-01T00:00:10'), " +
                "('c', 12, '1970-01-01T00:00:11')");
    }

    @Test
    public void testEarliestByIndexedSingleValueFilteredPlanAssertion() throws Exception {
        // Pins EarliestByValueIndexedFilteredRecordCursorFactory: single indexed
        // symbol value with a generic filter. PAGE_FRAME_MAX_ROWS=4 exercises
        // the frame-max config path; the filter forces the cursor to read the
        // row payload via setRowIndex.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        assertMemoryLeak(() -> {
            buildIndexedDataset();

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String q = "SELECT s, v, ts FROM t WHERE s IN ('c') AND v > 5 EARLIEST ON ts PARTITION BY s";

            assertPlanNoLeakCheck(
                    q,
                    "Index forward scan on: s\n" +
                            "  filter: 5<v\n" +
                            "  symbolFilter: s=3\n" +
                            "    Frame forward scan on: t\n"
            );

            assertSql(
                    "s\tv\tts\n" +
                            "c\t9\t1970-01-01T00:00:08.000000" + suffix + "\n",
                    q
            );
        });
    }

    @Test
    public void testEarliestByIndexedValueListFilteredPlanAssertion() throws Exception {
        // Pins EarliestByValuesIndexedFilteredRecordCursorFactory: IN-list over
        // an indexed symbol with a generic filter. The two-value IN list
        // exercises the multi-key path.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        assertMemoryLeak(() -> {
            buildIndexedDataset();

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String q = "SELECT s, v, ts FROM t WHERE s IN ('a', 'c') AND v > 1 EARLIEST ON ts PARTITION BY s";

            assertPlanNoLeakCheck(
                    q,
                    "Index forward scan on: s\n" +
                            "  filter: 1<v\n" +
                            "  symbolFilter: s in [1,3]\n" +
                            "    Frame forward scan on: t\n"
            );

            assertSql(
                    "s\tv\tts\n" +
                            "a\t3\t1970-01-01T00:00:02.000000" + suffix + "\n" +
                            "c\t9\t1970-01-01T00:00:08.000000" + suffix + "\n",
                    q
            );
        });
    }

    @Test
    public void testEarliestByIndexedValueListPlanAssertion() throws Exception {
        // Pins EarliestByValuesIndexedRecordCursor: IN-list over an indexed
        // symbol, no generic filter. Same dataset as the filtered variant, to
        // guard the non-filtered multi-value indexed path.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4);
        assertMemoryLeak(() -> {
            buildIndexedDataset();

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String q = "SELECT s, v, ts FROM t WHERE s IN ('a', 'c') EARLIEST ON ts PARTITION BY s";

            assertPlanNoLeakCheck(
                    q,
                    "Index forward scan on: s\n" +
                            "  symbolFilter: s in [1,3]\n" +
                            "    Frame forward scan on: t\n"
            );

            assertSql(
                    "s\tv\tts\n" +
                            "a\t1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "c\t9\t1970-01-01T00:00:08.000000" + suffix + "\n",
                    q
            );
        });
    }

    @Test
    public void testEarliestByDeferredListValuesFilteredPlanAssertion() throws Exception {
        // Pins factory selection for EarliestByDeferredListValuesFilteredRecordCursorFactory:
        // PARTITION BY on a non-indexed symbol with only a generic filter routes
        // through the deferred-list values factory (literal/deferred symbol sets
        // both empty). Guards against a regression that silently selects a less
        // efficient factory. No ORDER BY, so output order is factory-defined.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('b', 2, '1970-01-01T00:00:01'), " +
                    "('a', 3, '1970-01-01T00:00:02'), " +
                    "('b', 4, '1970-01-01T00:00:03')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String q = "SELECT s, v, ts FROM t WHERE v > 1 EARLIEST ON ts PARTITION BY s";

            assertPlanNoLeakCheck(
                    q,
                    "EarliestByDeferredListValuesFiltered\n" +
                            "  filter: 1<v\n" +
                            "    Frame forward scan on: t\n"
            );

            assertSql(
                    "s\tv\tts\n" +
                            "b\t2\t1970-01-01T00:00:01.000000" + suffix + "\n" +
                            "a\t3\t1970-01-01T00:00:02.000000" + suffix + "\n",
                    q
            );
        });
    }

    @Test
    public void testEarliestByValueFilteredPlanAssertion() throws Exception {
        // Pins factory selection for EarliestByValueFilteredRecordCursorFactory:
        // single non-indexed value with a generic filter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('a', 1, '1970-01-01T00:00:00'), " +
                    "('b', 2, '1970-01-01T00:00:01'), " +
                    "('a', 3, '1970-01-01T00:00:02'), " +
                    "('b', 4, '1970-01-01T00:00:03')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            String q = "SELECT s, v, ts FROM t WHERE s = 'a' AND v > 1 EARLIEST ON ts PARTITION BY s";

            assertPlanNoLeakCheck(
                    q,
                    "EarliestByValueFiltered\n" +
                            "    Row forward scan\n" +
                            "      symbolFilter: s=0\n" +
                            "      filter: 1<v\n" +
                            "    Frame forward scan on: t\n"
            );

            assertSql(
                    "s\tv\tts\n" +
                            "a\t3\t1970-01-01T00:00:02.000000" + suffix + "\n",
                    q
            );
        });
    }

    // Shared dataset for the retry-via-getCursor() regression tests below.
    // Partitions 2 and 3 are the only ones that contain 'c'; queries for 'c'
    // (or IN-lists that include 'c') therefore must open partition 2. The
    // filesystem fault injector blocks that open, so the first cursor throws
    // mid-scan. The test then re-acquires a cursor from the same factory,
    // which invokes of() on the reused cursor instance, and verifies it
    // produces the full, correct result set. This pins the contract that
    // of() fully resets after an exception  -  the same contract LATEST BY
    // relies on.
    private void insertRetryIndexedDataset() throws SqlException {
        execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('a', 1, '1970-01-01T00:00:00'), ('b', 2, '1970-01-01T01:00:00'), " +
                "('a', 3, '1970-01-02T00:00:00'), ('b', 4, '1970-01-02T01:00:00'), ('c', 5, '1970-01-02T02:00:00'), " +
                "('a', 6, '1970-01-03T00:00:00'), ('b', 7, '1970-01-03T01:00:00'), ('c', 8, '1970-01-03T02:00:00')");
    }

    private void runRetryAfterMidScanException(String query, String expected) throws Exception {
        final AtomicBoolean failNext = new AtomicBoolean(true);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failNext.get() && Utf8s.containsAscii(name, "1970-01-02")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };
        assertMemoryLeak(() -> {
            insertRetryIndexedDataset();

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // Drain until the FS fault triggers.
                    }
                    Assert.fail("expected CairoException from injected FS failure");
                } catch (CairoException expected2) {
                    // pass  -  cursor threw mid-scan
                }

                // Allow partition 2 to open on the retry.
                failNext.set(false);
                assertCursor(expected, factory, true, true, true);
            }
        });
    }

    @Test
    public void testEarliestByValueIndexedFilteredReentrantAfterException() throws Exception {
        // Covers EarliestByValueIndexedFilteredRecordCursor: verifies that the
        // cursor fully resets after an exception, so the second getCursor()
        // returns correct results from a clean scan. Query is for 'c' which
        // only exists in partitions 2+; the SINGLE-value cursor must open
        // partition 2 to find its first match, triggering the injected fault.
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        runRetryAfterMidScanException(
                "SELECT s, v, ts FROM t WHERE s IN ('c') AND v > 0 EARLIEST ON ts PARTITION BY s",
                "s\tv\tts\n" +
                        "c\t5\t1970-01-02T02:00:00.000000" + suffix + "\n"
        );
    }

    @Test
    public void testEarliestByValuesIndexedReentrantAfterException() throws Exception {
        // Covers EarliestByValuesIndexedRecordCursor: IN-list over an indexed
        // symbol, no filter. 'c' only exists in partitions 2+, so the cursor
        // must open partition 2 to satisfy the full IN-list, triggering the
        // injected fault on the first pass.
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        runRetryAfterMidScanException(
                "SELECT s, v, ts FROM t WHERE s IN ('a', 'b', 'c') EARLIEST ON ts PARTITION BY s",
                "s\tv\tts\n" +
                        "a\t1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                        "b\t2\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                        "c\t5\t1970-01-02T02:00:00.000000" + suffix + "\n"
        );
    }

    @Test
    public void testEarliestByValuesIndexedFilteredReentrantAfterException() throws Exception {
        // Covers EarliestByValuesIndexedFilteredRecordCursor: IN-list over an
        // indexed symbol with a generic filter. The filter forces setRowIndex
        // plus a row read before each "found" record is locked in. 'c' is only
        // in partitions 2+, so the cursor opens partition 2 and trips the
        // injected fault on the first pass.
        String suffix = getTimestampSuffix(timestampType.getTypeName());
        runRetryAfterMidScanException(
                "SELECT s, v, ts FROM t WHERE s IN ('a', 'b', 'c') AND v > 0 EARLIEST ON ts PARTITION BY s",
                "s\tv\tts\n" +
                        "a\t1\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                        "b\t2\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                        "c\t5\t1970-01-02T02:00:00.000000" + suffix + "\n"
        );
    }

    @Test
    public void testLateralEarliestByInnerEqCorrelation() throws Exception {
        // Inner LATERAL with equality correlation exercises the fast path in
        // compensateEarliestBy: WHERE symbol = o.sym becomes a hash join and
        // symbol is appended to EARLIEST ON's PARTITION BY list.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, sym SYMBOL, ts " + timestampType.getTypeName()
                    + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (symbol SYMBOL, venue SYMBOL, price DOUBLE, ts "
                    + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'MSFT', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('AAPL', 'NYSE',   100.0, '2024-01-01T00:10:00.000000Z'),
                    ('AAPL', 'NYSE',   101.0, '2024-01-01T00:20:00.000000Z'),
                    ('AAPL', 'NASDAQ', 102.0, '2024-01-01T00:30:00.000000Z'),
                    ('AAPL', 'NASDAQ', 103.0, '2024-01-01T00:40:00.000000Z'),
                    ('MSFT', 'NYSE',   200.0, '2024-01-01T01:10:00.000000Z'),
                    ('MSFT', 'NASDAQ', 201.0, '2024-01-01T01:20:00.000000Z'),
                    ('MSFT', 'NASDAQ', 202.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // earliest trade per venue, scoped to each outer order's symbol
            // order 1 (AAPL): NYSE earliest = 100.0, NASDAQ earliest = 102.0
            // order 2 (MSFT): NYSE earliest = 200.0, NASDAQ earliest = 201.0
            assertQueryNoLeakCheck(
                    "id\tvenue\tprice\n" +
                            "1\tNASDAQ\t102.0\n" +
                            "1\tNYSE\t100.0\n" +
                            "2\tNASDAQ\t201.0\n" +
                            "2\tNYSE\t200.0\n",
                    """
                            SELECT o.id, e.venue, e.price
                            FROM orders o
                            JOIN LATERAL (
                                SELECT venue, price FROM trades
                                WHERE symbol = o.sym
                                EARLIEST ON ts PARTITION BY venue
                            ) e
                            ORDER BY o.id, e.venue
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testLateralEarliestByLeftEqCorrelation() throws Exception {
        // LEFT LATERAL with equality correlation. Unmatched outer rows must
        // get a NULL-filled right side. Also validates fast-path PARTITION BY
        // expansion for compensateEarliestBy.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, sym SYMBOL, ts " + timestampType.getTypeName()
                    + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (symbol SYMBOL, venue SYMBOL, price DOUBLE, ts "
                    + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, 'AAPL', '2024-01-01T00:00:00.000000Z'),
                    (2, 'MSFT', '2024-01-01T01:00:00.000000Z'),
                    (3, 'GOOG', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('AAPL', 'NYSE',   100.0, '2024-01-01T00:10:00.000000Z'),
                    ('AAPL', 'NYSE',   101.0, '2024-01-01T00:20:00.000000Z'),
                    ('AAPL', 'NASDAQ', 102.0, '2024-01-01T00:30:00.000000Z'),
                    ('MSFT', 'NYSE',   200.0, '2024-01-01T01:10:00.000000Z')
                    """);

            // order 1 (AAPL): NYSE earliest = 100.0, NASDAQ earliest = 102.0
            // order 2 (MSFT): NYSE earliest = 200.0
            // order 3 (GOOG): no trades -> NULL row
            assertQueryNoLeakCheck(
                    "id\tvenue\tprice\n" +
                            "1\tNASDAQ\t102.0\n" +
                            "1\tNYSE\t100.0\n" +
                            "2\tNYSE\t200.0\n" +
                            "3\t\tnull\n",
                    """
                            SELECT o.id, e.venue, e.price
                            FROM orders o
                            LEFT JOIN LATERAL (
                                SELECT venue, price FROM trades
                                WHERE symbol = o.sym
                                EARLIEST ON ts PARTITION BY venue
                            ) e
                            ORDER BY o.id, e.venue
                            """,
                    null, true, false
            );
        });
    }

    @Test
    public void testLateralEarliestByNonEqCorrelationFallback() throws Exception {
        // Non-eq correlation (ts > o.min_ts) forces compensateEarliestBy
        // onto the window-function fallback path: row_number() OVER
        // (PARTITION BY venue, __qdb_outer_ref__0_id ORDER BY ts ASC) = 1.
        // Validates that outer-reference rewriting and ORDER BY direction
        // (ASCENDING, inverse of LATEST BY) are correct.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE orders (id INT, min_ts " + timestampType.getTypeName()
                    + ", ts " + timestampType.getTypeName()
                    + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE trades (venue SYMBOL, price DOUBLE, ts "
                    + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO orders VALUES
                    (1, '2024-01-01T00:15:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:15:00.000000Z', '2024-01-01T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO trades VALUES
                    ('NYSE',   10.0, '2024-01-01T00:10:00.000000Z'),
                    ('NYSE',   20.0, '2024-01-01T00:20:00.000000Z'),
                    ('NYSE',   21.0, '2024-01-01T00:30:00.000000Z'),
                    ('NASDAQ', 30.0, '2024-01-01T00:40:00.000000Z'),
                    ('NASDAQ', 31.0, '2024-01-01T00:50:00.000000Z'),
                    ('NYSE',   40.0, '2024-01-01T01:20:00.000000Z'),
                    ('NASDAQ', 50.0, '2024-01-01T01:30:00.000000Z')
                    """);

            // order 1 (min_ts=00:15): trades after 00:15 -> NYSE {20,21,40}, NASDAQ {30,31,50}
            //   earliest per venue: NYSE=20, NASDAQ=30
            // order 2 (min_ts=01:15): trades after 01:15 -> NYSE {40}, NASDAQ {50}
            //   earliest per venue: NYSE=40, NASDAQ=50
            assertQueryNoLeakCheck(
                    "id\tvenue\tprice\n" +
                            "1\tNASDAQ\t30.0\n" +
                            "1\tNYSE\t20.0\n" +
                            "2\tNASDAQ\t50.0\n" +
                            "2\tNYSE\t40.0\n",
                    """
                            SELECT o.id, e.venue, e.price
                            FROM orders o
                            JOIN LATERAL (
                                SELECT venue, price FROM trades
                                WHERE ts > o.min_ts
                                EARLIEST ON ts PARTITION BY venue
                            ) e
                            ORDER BY o.id, e.venue
                            """,
                    null, true, true
            );
        });
    }
}

