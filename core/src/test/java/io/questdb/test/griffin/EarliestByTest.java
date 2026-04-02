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
                            "1\t29\n" +
                            "3\t26\n" +
                            "9\t29\n" +
                            "7\t25\n",
                    "select a+b*c x, sum(z)+25 ohoh from zyzy where a in (x,y) and b = 3 earliest on ts partition by x;",
                    true
            );
        });
    }

    @Test
    public void testEarliestByAllFilteredResolvesSymbol() throws Exception {
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
                            "    Async index backward scan on: device_id workers: 2\n" +
                            "      filter: g8c within(\"0010000110110001110001111100010000100000\")\n" +
                            "    Interval backward scan on: pos_test\n" +
                            (timestampType == TestTimestampType.MICRO ?
                                    "      intervals: [(\"2021-09-02T00:00:00.000000Z\",\"2021-09-02T23:59:59.999999Z\")]\n" :
                                    "      intervals: [(\"2021-09-02T00:00:00.000000000Z\",\"2021-09-02T23:59:59.999999999Z\")]\n")
            );

            assertQuery(
                    "ts\tdevice_id\tg8c\n" +
                            "2021-09-02T00:00:00.000000" + getTimestampSuffix(timestampType.getTypeName()) + "\tdevice_1\t46swgj10\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b') s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts\n" +
                            "1970-01-02T00:00:00.000000" + suffix + "\ta\n" +
                            "1970-01-02T01:00:00.000000" + suffix + "\tb\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");
            execute("insert into t values ('e', 'f', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-02T01:00:00.000000" + suffix + "\td\ta\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");
            execute("insert into t values ('a', 'e', '1970-01-01T01:01:01.000000Z')");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-02T01:00:00.000000" + suffix + "\tc\tb\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b') s, rnd_symbol('c', 'd') s2, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "ts\ts2\ts\n" +
                            "1970-01-02T00:00:00.000000" + suffix + "\tc\ta\n" +
                            "1970-01-02T01:00:00.000000" + suffix + "\tc\tb\n" +
                            "1970-01-02T02:00:00.000000" + suffix + "\td\ta\n" +
                            "1970-01-02T03:00:00.000000" + suffix + "\td\tb\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(100)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T04:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-03T10:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-03T16:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-03T20:00:00.000000" + suffix + "\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };
            execute("create table t as (select rnd_symbol('a', 'b', null) s, rnd_symbol('c', null) s2, rnd_symbol('d', null) s3, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(100)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "s\ts2\ts3\tts\n" +
                            "\tc\t\t1970-01-03T04:00:00.000000" + suffix + "\n" +
                            "b\tc\t\t1970-01-03T10:00:00.000000" + suffix + "\n" +
                            "\t\td\t1970-01-03T16:00:00.000000" + suffix + "\n" +
                            "a\t\t\t1970-01-03T20:00:00.000000" + suffix + "\n",
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
                                    "1970-01-01T01:00:00.000000" + suffix + "\tc\n",
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
                                    "1970-01-02T00:00:00.000000" + suffix + "\ta\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01") || Utf8s.containsAscii(name, "1970-01-02")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

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
    }

    @Test
    public void testEarliestByValueEmptyTableNoFilter() throws Exception {
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
                            "1970-01-02T00:00:00.000000" + suffix + "\ta\n",
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
                            "1970-01-02T00:00:00.000000" + suffix + "\ta\n",
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
                        "10\tb\t1970-01-02T01:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                "t where v in ('a', 'b', 'd') and x%2 = 0 earliest on ts partition by v",
                "create table t as (select x, rnd_varchar('a', 'b', 'c', null) v, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY",
                "ts",
                "insert into t values (1000, 'd', '1970-01-02T20:00')",
                "x\tv\tts\n" +
                        "10\tb\t1970-01-02T01:00:00.000000" + suffix + "\n" +
                        "1000\td\t1970-01-02T20:00:00.000000" + suffix + "\n" +
                        "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testEarliestWithNullInSymbolFilterDoesNotDoFullScan() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (select x, rnd_symbol('a', 'b', null) s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "48\ta\t1970-01-02T23:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n",
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
            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRO(LPSZ name) {
                    if (Utf8s.containsAscii(name, "1970-01-01")) {
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRO(name);
                }
            };

            execute("create table t as (select x, rnd_symbol('a', 'b', null) s, timestamp_sequence(0, 60*60*1000*1000L)::" + timestampType.getTypeName() + " ts from long_sequence(49)) timestamp(ts) partition by DAY");

            String suffix = getTimestampSuffix(timestampType.getTypeName());
            assertQuery(
                    "x\ts\tts\n" +
                            "1\ta\t1970-01-01T01:00:00.000000" + suffix + "\n" +
                            "2\tb\t1970-01-02T01:00:00.000000" + suffix + "\n" +
                            "49\t\t1970-01-03T00:00:00.000000" + suffix + "\n",
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
                    "2020-05-05T00:00:00.000000" + suffix + "\t2020-05-02T00:00:00.000000" + suffix + "\t40.0\n" +
                    "2020-05-06T00:00:00.000000" + suffix + "\t2020-05-01T00:00:00.000000" + suffix + "\t140.0\n";

            assertQuery(expected, query, "version", true, true);
        });
    }
}

