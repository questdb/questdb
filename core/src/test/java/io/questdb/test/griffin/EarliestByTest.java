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
    // Tests for EarliestByLightRecordCursorFactory (subquery with random access)
    // =====================================================================

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

            // Multi-key map iteration order is non-deterministic, so wrap in a count query
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (SELECT s1, s2, ts FROM (SELECT s1, s2, ts FROM t) EARLIEST ON ts PARTITION BY s1, s2)"
            );
        });
    }

    // =====================================================================
    // Tests for EarliestByLightRecordCursorFactory (group by supports random access)
    // =====================================================================

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

    // =====================================================================
    // Tests for EarliestByRecordCursorFactory (no random access - UNION ALL)
    // =====================================================================

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
            // UNION ALL path (EarliestByRecordCursorFactory) has non-deterministic map iteration order
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (" + query + ")"
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

            // UNION ALL forces non-random-access, exercises row index sorting in EarliestByRecordCursorFactory
            assertSql(
                    "count\n5\n",
                    "SELECT count() FROM (SELECT s, ts FROM (" +
                            "SELECT s, ts FROM t UNION ALL SELECT s, ts FROM t WHERE 1 = 0" +
                            ") EARLIEST ON ts PARTITION BY s)"
            );
        });
    }

    @Test
    public void testEarliestOnUnionAllMultiplePartitionCols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 'x', '1970-01-01T02:00:00'), ('a', 'y', '1970-01-01T01:00:00'), " +
                    "('b', 'x', '1970-01-01T00:00:00')");

            // UNION ALL with multi-column partition by
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (SELECT s1, s2, ts FROM (" +
                            "SELECT s1, s2, ts FROM t UNION ALL SELECT s1, s2, ts FROM t WHERE 1 = 0" +
                            ") EARLIEST ON ts PARTITION BY s1, s2)"
            );
        });
    }

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
            assertSql(
                    "count\n4\n",
                    "SELECT count() FROM (SELECT ts, s1, s2 FROM t EARLIEST ON ts PARTITION BY s1, s2)"
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

            // WHERE + EARLIEST ON triggers the table query with filter path
            String query = "SELECT ts, s FROM t WHERE v > 50 EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (" + query + ")"
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

            // Interval filter with earliest on
            String query = "SELECT ts, s FROM t WHERE ts >= '2024-01-01T12:00:00' AND ts < '2024-01-02' EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (" + query + ")"
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

            // WHERE s NOT IN triggers excludedKeyValues path
            String query = "SELECT ts, s FROM t WHERE s != 'c' EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (" + query + ")"
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

            // Multiple values in IN list triggers multi-value path
            String query = "SELECT ts, s FROM t WHERE s IN ('a', 'b', 'c') EARLIEST ON ts PARTITION BY s";
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (" + query + ")"
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
                    "FROM long_sequence(10000)) TIMESTAMP(ts) PARTITION BY HOUR");

            assertSql(
                    "count\n5\n",
                    "SELECT count() FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s)"
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

            // Many distinct symbols through direct table scan
            assertSql(
                    "count\n20\n",
                    "SELECT count() FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s)"
            );
        });
    }

    // =====================================================================
    // Targeted coverage: excluded symbol values path (ValueListRecordCursor)
    // =====================================================================

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

    // =====================================================================
    // Targeted coverage: non-existent symbol values (DeferredListValues)
    // =====================================================================

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
        // Tests EARLIEST ON with WHERE s IN (SELECT ...) — exercises EarliestBySubQueryRecordCursorFactory
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

    // =====================================================================
    // Targeted coverage: AllSymbols early termination
    // =====================================================================

    @Test
    public void testEarliestByAllSymbolsEarlyTermination() throws Exception {
        // Tests early termination when all symbol combinations found in first partition
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s1 SYMBOL, s2 SYMBOL, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            // All combinations appear in first day, second day should not be scanned
            execute("INSERT INTO t VALUES ('a', 'x', '1970-01-01T00:00:00'), ('a', 'y', '1970-01-01T01:00:00'), " +
                    "('b', 'x', '1970-01-01T02:00:00'), ('b', 'y', '1970-01-01T03:00:00'), " +
                    "('a', 'x', '1970-01-02T00:00:00'), ('b', 'y', '1970-01-02T01:00:00')");

            // All 4 combinations found in first partition - exercises early termination
            assertSql(
                    "count\n4\n",
                    "SELECT count() FROM (SELECT ts, s1, s2 FROM t EARLIEST ON ts PARTITION BY s1, s2)"
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

            // Filter narrows results
            assertSql(
                    "count\n3\n",
                    "SELECT count() FROM (SELECT ts, s1, s2 FROM t WHERE v > 15 EARLIEST ON ts PARTITION BY s1, s2)"
            );
        });
    }

    // =====================================================================
    // Targeted coverage: indexed symbol without WHERE (AllIndexed path)
    // =====================================================================

    @Test
    public void testEarliestByIndexedSymbolNoFilter() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        // Triggers EarliestByAllIndexedRecordCursorFactory without filter
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL INDEX, v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('a', 10, '1970-01-01T02:00:00'), ('b', 20, '1970-01-01T00:00:00'), " +
                    "('a', 30, '1970-01-01T00:00:00'), ('b', 40, '1970-01-01T03:00:00')");

            // No WHERE clause with indexed symbol column
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT ts, s, v FROM t EARLIEST ON ts PARTITION BY s)"
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

            // Indexed symbol scan across partition boundaries
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s)"
            );
        });
    }

    // =====================================================================
    // Targeted coverage: interval-filtered EARLIEST ON
    // =====================================================================

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

    // =====================================================================
    // Targeted coverage: LIMIT/OFFSET with EARLIEST ON
    // =====================================================================

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

    // =====================================================================
    // Targeted coverage: DefaultCairoConfiguration
    // =====================================================================

    @Test
    public void testEarliestByConfigurationDefaults() throws Exception {
        // Just ensure the configuration method is called
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT rnd_symbol('a', 'b') s, " +
                    "timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY");

            // Execute query that uses earliest by row count configuration
            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000" + suffix + "\n" +
                            "b\t1970-01-01T02:00:00.000000" + suffix + "\n",
                    "SELECT s, ts FROM (SELECT s, ts FROM t) EARLIEST ON ts PARTITION BY s"
            );
        });
    }

    // =====================================================================
    // Missing scenarios: table without designated timestamp
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: EARLIEST ON + SAMPLE BY interaction
    // =====================================================================

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

            // EARLIEST ON + SAMPLE BY should work (consistent with LATEST ON + SAMPLE BY)
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM (SELECT s, sum(v) FROM t EARLIEST ON ts PARTITION BY s SAMPLE BY 1d)"
            );
        });
    }

    // =====================================================================
    // Missing scenarios: non-timestamp column in ON position
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: mixed symbol + non-symbol multi-key partition
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: WAL out-of-order inserts
    // =====================================================================

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

            // Second batch with earlier timestamps — WAL must reorder
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

    // =====================================================================
    // Missing scenarios: EARLIEST ON with ORDER BY
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: EARLIEST ON with GROUP BY in subquery
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: EARLIEST ON with multiple identical timestamps
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: EARLIEST ON with large number of distinct keys
    // =====================================================================

    @Test
    public void testEarliestOnManyDistinctKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t as (" +
                    "SELECT ('s' || x)::symbol s, " +
                    "timestamp_sequence(0, 60*1000*1000L)::" + timestampType.getTypeName() + " ts " +
                    "FROM long_sequence(1000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            // Each of the 1000 symbols appears exactly once, so EARLIEST ON returns all rows
            assertSql(
                    "count\n1000\n",
                    "SELECT count() FROM (SELECT ts, s FROM t EARLIEST ON ts PARTITION BY s)"
            );
        });
    }

    // =====================================================================
    // Missing scenarios: EARLIEST ON with UNION ALL
    // =====================================================================

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

    // =====================================================================
    // Missing scenarios: EARLIEST ON with empty result after filter
    // =====================================================================

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

    // =====================================================================
    // Additional partition key types
    // =====================================================================

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

    // =====================================================================
    // EXPLAIN PLAN output
    // =====================================================================

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

    // =====================================================================
    // IN (subquery) filter
    // =====================================================================

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

    // =====================================================================
    // Bind variables with NOT IN
    // =====================================================================

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
            // Subquery returns symbols not present in t — should return empty result
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
            // 'a' has rows at NULL, 01:00, 02:00 — earliest non-NULL is 01:00 (v=2)
            // 'b' has rows at 00:00, NULL — earliest non-NULL is 00:00 (v=3)
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

    // =====================================================================
    // Regression: subquery collapse must not strip EARLIEST ON
    // =====================================================================

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
            // into just "t" — returning all 4 rows instead of 2.
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
}

