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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
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
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

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
                    "CREATE TABLE e (ts #TIMESTAMP, sym SYMBOL CAPACITY 32768 INDEX CAPACITY 4) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );
            executeWithRewriteTimestamp(
                    "CREATE TABLE p (ts #TIMESTAMP, sym SYMBOL CAPACITY 32768 CACHE INDEX CAPACITY 4, lon FLOAT, lat FLOAT, g3 geohash(3c)) TIMESTAMP(ts) PARTITION BY DAY",
                    timestampType.getTypeName()
            );

            long timestamp = 1625853700000000L;
            for (int i = 0; i < iterations; i++) {
                execute("INSERT INTO e VALUES(CAST(" + timestamp + " as TIMESTAMP), '42')");
                execute("INSERT INTO p VALUES(CAST(" + timestamp + " as TIMESTAMP), '42', 142.31, 42.31, #xpt)");

                String query = "SELECT count() FROM (" +
                        "  (SELECT ts ts_p, sym, lon, lat, g3 FROM p WHERE ts >= cast(" + timestamp + " AS timestamp) AND g3 within(#xpk, #xpm, #xps, #xpt) EARLIEST ON ts PARTITION BY sym) " +
                        "  WHERE lon >= 142.0 AND lon <= 143.0 AND lat >= 42.0 AND lat <= 43.0) " +
                        "JOIN (SELECT ts ts_e, sym FROM e WHERE ts >= cast(" + timestamp + " AS timestamp) EARLIEST ON ts PARTITION BY sym) ON (sym)";
                assertQuery("count\n1\n", query, null, false, true);
                timestamp += 10000L;
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
                            "    Index forward scan on: device_id\n" +
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
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp, s symbol, s2 symbol) timestamp (ts) partition by month");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000Z\t\t\n" +
                            "2025-01-04T00:00:00.000000Z\tsymS2A\tsymSA\n",
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
            Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
            execute("create table t (ts timestamp, s symbol, s2 symbol) timestamp (ts) partition by month wal");
            execute("insert into t(ts) values ('2025-01-01'),('2025-01-02'),('2025-01-03')");
            execute("insert into t values ('2025-01-04', 'symSA', 'symS2A')");
            drainWalQueue();
            assertQuery(
                    "ts\ts2\ts\n" +
                            "2025-01-01T00:00:00.000000Z\t\t\n" +
                            "2025-01-04T00:00:00.000000Z\tsymS2A\tsymSA\n",
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
                            true,
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
                            true,
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

    // =====================================================================
    // Additional table-based tests for code coverage
    // =====================================================================

    // =====================================================================
    // Parser edge cases
    // =====================================================================

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

    // =====================================================================
    // Additional code path coverage
    // =====================================================================

    @Test
    public void testEarliestByMultiplePartitionColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s1, rnd_symbol('x', 'y') s2, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(20)) TIMESTAMP(ts) PARTITION BY DAY");

            // Multi-column partition by goes through the AllSymbolsFiltered path
            printSql("SELECT ts, s1, s2 FROM t EARLIEST ON ts PARTITION BY s1, s2");
        });
    }

    @Test
    public void testEarliestByWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c') s, rnd_int(0,100,0) v, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // WHERE + EARLIEST ON triggers the table query with filter path
            printSql("SELECT ts, s FROM t WHERE v > 50 EARLIEST ON ts PARTITION BY s");
        });
    }

    @Test
    public void testEarliestByWithIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s, " +
                    "timestamp_sequence('2024-01-01', 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(48)) TIMESTAMP(ts) PARTITION BY DAY");

            // Interval filter with earliest on
            printSql("SELECT ts, s FROM t WHERE ts >= '2024-01-01T12:00:00' AND ts < '2024-01-02' EARLIEST ON ts PARTITION BY s");
        });
    }

    @Test
    public void testEarliestByExcludedValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // WHERE s NOT IN triggers excludedKeyValues path
            printSql("SELECT ts, s FROM t WHERE s != 'c' EARLIEST ON ts PARTITION BY s");
        });
    }

    @Test
    public void testEarliestByMultipleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c', 'd') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(30)) TIMESTAMP(ts) PARTITION BY DAY");

            // Multiple values in IN list triggers multi-value path
            printSql("SELECT ts, s FROM t WHERE s IN ('a', 'b', 'c') EARLIEST ON ts PARTITION BY s");
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
            // Indexed symbol with single value filter
            assertQuery(
                    "ts\ts\tv\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\t3\n",
                    "SELECT ts, s, v FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
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
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-01T00:00:00.000000" + suffix + "\ta\n",
                    "SELECT ts, s FROM t WHERE s = 'a' EARLIEST ON ts PARTITION BY s",
                    "ts",
                    true,
                    true
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
            printSql("SELECT ts, s1, s2 FROM t EARLIEST BY s1, s2");
        });
    }

    @Test
    public void testEarliestByLargeDataset() throws Exception {
        // Larger dataset to exercise cursor iteration and map building more thoroughly
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b', 'c', 'd', 'e') s, rnd_int(0,1000,0) v, " +
                    "timestamp_sequence(0, 1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10000)) TIMESTAMP(ts) PARTITION BY HOUR");

            printSql("SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s");
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

            // Many distinct symbols through direct table scan
            printSql("SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s");
        });
    }
}

