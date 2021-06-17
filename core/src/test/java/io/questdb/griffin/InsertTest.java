/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.TestBinarySequence;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InsertTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testInsertAllByDay() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L;
                    }
                },
                true,
                true
        );
    }

    @Test
    public void testInsertAllByDayUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L;
                    }
                },
                false,
                true
        );
    }

    @Test
    public void testInsertAllByDayUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L;
                    }
                },
                false,
                false
        );
    }

    @Test
    public void testInsertAllByMonth() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 30;
                    }
                },
                true,
                true
        );
    }

    @Test
    public void testInsertAllByMonthUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 30;
                    }
                },
                false,
                true
        );
    }

    @Test
    public void testInsertAllByMonthUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 30;
                    }
                },
                false,
                false
        );
    }

    @Test
    public void testInsertAllByNone() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, true, true);
    }

    @Test
    public void testInsertAllByNoneUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, true);
    }

    @Test
    public void testInsertAllByNoneUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, false);
    }

    @Test
    public void testInsertAllByYear() throws Exception {
        testBindVariableInsert(PartitionBy.YEAR, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 30 * 12;
                    }
                },
                true,
                true
        );
    }

    @Test
    public void testInsertAllByYearUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.YEAR, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 30 * 12;
                    }
                },
                false,
                true
        );
    }

    @Test
    public void testInsertContextSwitch() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            sqlExecutionContext.getBindVariableService().setDouble("bal", 150.4);
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', :bal)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insertStatement = cq.getInsertStatement();

            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllCairoSecurityContext.INSTANCE,
                            bindVariableService,
                            null,
                            -1,
                            null);

            bindVariableService.setDouble("bal", 56.4);

            try (InsertMethod method = insertStatement.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), insertStatement.getTableName())) {
                TestUtils.assertReader("cust_id\tccy\tbalance\n" +
                        "1\tGBP\t150.4\n" +
                        "1\tGBP\t56.4\n", reader, sink);
            }
        });
    }

    @Test
    public void testInsertExecutionAfterStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', 356.12)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertStatement insertStatement = cq.getInsertStatement();

                compiler.compile("alter table balances drop column ccy", sqlExecutionContext);

                insertStatement.createMethod(sqlExecutionContext);
                Assert.fail();
            } catch (WriterOutOfDateException ignored) {
            }
        });
    }

    @Test
    public void testInsertExplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'))", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            String expected = "timestamp\tfield\tvalue\n" +
                    "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReader(expected, insert.getTableName());
        });
    }

    @Test
    public void testInsertImplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("INSERT INTO TS values(to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'),'X',123.33d)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            String expected = "timestamp\tfield\tvalue\n" +
                    "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReader(expected, insert.getTableName());
        });
    }

    @Test
    public void testInsertInvalidColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("insert into balances(cust_id, ccy2, balance) values (1, 'GBP', 356.12)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(30, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testInsertNoSelfReference() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE trades_aapl (ts TIMESTAMP, px INT, qty int, side STRING) TIMESTAMP(ts)", sqlExecutionContext);
            try {
                compiler.compile("insert into trades_aapl (ts) values (ts)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(37, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testInsertNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'USD', 356.12)", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            String expected = "cust_id\tccy\tbalance\n" +
                    "1\tUSD\t356.12\n";

            assertReader(expected, insert.getTableName());
        });
    }

    @Test
    public void testInsertNotEnoughFields() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("insert into balances values (1, 'USD')", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(37, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "not enough values");
            }
        });
    }

    @Test
    public void testInsertSingleAndMultipleCharacterSymbols() throws Exception {
        final String expected = "sym\tid\tts\n" +
                "A\t315515118\t1970-01-03T00:00:00.000000Z\n" +
                "BB\t-727724771\t1970-01-03T00:06:00.000000Z\n" +
                "BB\t-948263339\t1970-01-03T00:12:00.000000Z\n" +
                "CC\t592859671\t1970-01-03T00:18:00.000000Z\n" +
                "CC\t-847531048\t1970-01-03T00:24:00.000000Z\n" +
                "A\t-2041844972\t1970-01-03T00:30:00.000000Z\n" +
                "CC\t-1575378703\t1970-01-03T00:36:00.000000Z\n" +
                "BB\t1545253512\t1970-01-03T00:42:00.000000Z\n" +
                "A\t1573662097\t1970-01-03T00:48:00.000000Z\n" +
                "BB\t339631474\t1970-01-03T00:54:00.000000Z\n";

        assertQuery(
                "sym\tid\tts\n",
                "x",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    id int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into x select * from (select rnd_symbol('A', 'BB', 'CC', 'DDD') sym, \n" +
                        "        rnd_int() id, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true,
                true,
                true
        );
    }

    @Test
    public void testInsertSingleCharacterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table ww (id int, sym symbol)", sqlExecutionContext);
            CompiledQuery cq = compiler.compile("insert into ww VALUES ( 2, 'A')", sqlExecutionContext);
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                method.execute();
                method.commit();
            }

            String expected = "id\tsym\n" +
                    "2\tA\n";

            assertReader(expected, insert.getTableName());
        });
    }

    @Test
    public void testInsertSymbolNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table symbols (sym symbol, isNewSymbol BOOLEAN)  partition by NONE", sqlExecutionContext);
            executeInsert("insert into symbols (sym, isNewSymbol) VALUES ('USDJPY', false);");
            executeInsert("insert into symbols (sym, isNewSymbol) VALUES ('USDFJD', true);");

            String expected = "sym\tisNewSymbol\n" +
                    "USDJPY\tfalse\n" +
                    "USDFJD\ttrue\n";

            assertReader(expected, "symbols");
        });
    }

    @Test
    public void testInsertSymbolPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;", sqlExecutionContext);
            executeInsert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            executeInsert("insert into trades VALUES ( 1262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" +
                    "2010-01-04T10:01:40.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReader(expected, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;", sqlExecutionContext);
            executeInsert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");

            String expected1 = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n";

            assertReader(expected1, "trades");

            try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "trades", "testing")) {
                w.truncate();
            }

            executeInsert("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected2 = "ts\tsym\tbid\task\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReader(expected2, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedFarApart() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;", sqlExecutionContext);
            executeInsert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            executeInsert("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReader(expected, "trades");
        });
    }

    @Test
    public void testInsertValueCannotReferenceTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)", sqlExecutionContext);
            try {
                compiler.compile("insert into balances values (1, ccy, 356.12)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(32, e.getPosition());
            }
        });
    }

    @Test
    public void testInsertValuesAsLambda() throws Exception {
        assertFailure(
                "insert into names values(select rnd_str('Tom', 'Anna', 'John', 'Tim', 'Kim', 'Jim'), rnd_str('Smith', 'Mason', 'Johnson', 'Thompson') from long_sequence(8))",
                null,
                25,
                "query is not allowed here"
        );
    }

    @Test
    public void testInsertWithLessColumnsThanExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(seq long, ts timestamp) timestamp(ts);", sqlExecutionContext);
            try {
                compiler.compile("insert into tab select x ac  from long_sequence(10)", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "select clause must provide timestamp column");
            }
        });
    }

    @Test
    public void testInsertWithWrongNominatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(seq long, ts timestamp) timestamp(ts);", sqlExecutionContext);
            try {
                compiler.compile("insert into tab select * from (select  timestamp_sequence(0, x) ts, x ac from long_sequence(10)) timestamp(ts)", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "nominated column of existing table");
            }
        });
    }

    @Test
    public void testInsertWithoutNominatedTimestamp() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t1970-01-01T00:00:00.000000Z\n" +
                "2\t1970-01-01T00:00:00.000001Z\n" +
                "3\t1970-01-01T00:00:00.000003Z\n" +
                "4\t1970-01-01T00:00:00.000006Z\n" +
                "5\t1970-01-01T00:00:00.000010Z\n" +
                "6\t1970-01-01T00:00:00.000015Z\n" +
                "7\t1970-01-01T00:00:00.000021Z\n" +
                "8\t1970-01-01T00:00:00.000028Z\n" +
                "9\t1970-01-01T00:00:00.000036Z\n" +
                "10\t1970-01-01T00:00:00.000045Z\n";

        assertQuery(
                "seq\tts\n",
                "tab",
                "create table tab(seq long, ts timestamp) timestamp(ts);",
                "ts",
                "insert into tab select x ac, timestamp_sequence(0, x) ts from long_sequence(10)",
                expected,
                true,
                true,
                true
        );
    }

    @Test
    public void testInsertWithoutNominatedTimestampAndTypeDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tab(seq long, ts timestamp) timestamp(ts);", sqlExecutionContext);
            try {
                compiler.compile("insert into tab select x ac, rnd_int() id from long_sequence(10)", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(12, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "expected timestamp column");
            }
        });
    }

    @Test
    public void testInsertWrongTypeConstant() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table test (a timestamp)", sqlExecutionContext);
            try {
                compiler.compile("insert into test values ('2013')", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(25, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types: STRING -> TIMESTAMP");
            }
        });
    }

    @Test
    public void testInsertAsSelectISODateStringToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab select 1, '2021-01-03'",
                null,
                false
        );
    }

    @Test
    public void testInsertAsSelectNumberStringToDesignatedTimestampColumn() throws Exception {
        assertInsertTimestamp(
                "Invalid timestamp: 123456",
                "insert into tab select 1, '123456'",
                "io.questdb.cairo.CairoException",
                false
        );
    }

    @Test
    public void testInsertISODateStringToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOSecondsDateStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T00:00:00Z')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T01:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T00:00:00+01')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColum2() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:30:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T02:00:00-01:30')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumFails() throws Exception {
        assertInsertTimestamp(
                "Invalid timestamp",
                "insert into tab values (1, '2021-01-03T02:00:00-:30')",
                "io.questdb.cairo.CairoException",
                true
        );
    }

    @Test
    public void testInsertISOMicroStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T00:00:00.000000Z')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMicroStringTimestampColumnNoTimezone() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T00:00:00.000000')",
                null,
                true
        );
    }

    @Test
    public void testInsertInvalidDateStringTimestampColumn() throws Exception {
        assertInsertTimestamp(
                "Invalid timestamp: 2021-23-03T00:00:00Z",
                "insert into tab values (1, '2021-23-03T00:00:00Z')",
                "io.questdb.cairo.CairoException",
                true
        );
    }

    private void assertInsertTimestamp(String expected, String ddl2, String exceptionType, boolean commitInsert) throws Exception {
        if (commitInsert) {
            compiler.compile("create table tab(seq long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            try {
                executeInsert(ddl2);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql("tab", expected);
            } catch (CairoException e) {
                if (exceptionType == null) throw e;
                Assert.assertEquals(exceptionType, e.getClass().getName());
                TestUtils.assertContains(e.getFlyweightMessage(), expected);
            }
        } else {
            compiler.compile("create table tab(seq long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            try {
                compiler.compile(ddl2, sqlExecutionContext);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql("tab", expected);
            } catch (CairoException e) {
                if (exceptionType == null) throw e;
                Assert.assertEquals(exceptionType, e.getClass().getName());
                TestUtils.assertContains(e.getFlyweightMessage(), expected);
            }
        }

        compiler.compile("drop table tab", sqlExecutionContext);

        if (commitInsert) {
            compiler.compile("create table tab(seq long, ts timestamp)", sqlExecutionContext);
            try {
                executeInsert(ddl2);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql("tab", expected);
            } catch (Throwable e) {
                if (exceptionType == null) throw e;
                Assert.assertEquals(exceptionType, e.getClass().getName());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        } else {
            compiler.compile("create table tab(seq long, ts timestamp)", sqlExecutionContext);
            try {
                compiler.compile(ddl2, sqlExecutionContext);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql("tab", expected);
            } catch (Throwable e) {
                if (exceptionType == null) throw e;
                Assert.assertEquals(exceptionType, e.getClass().getName());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        }
    }

    private void testBindVariableInsert(
            int partitionBy,
            TimestampFunction timestampFunction,
            boolean initBindVariables,
            boolean columnSet
    ) throws Exception {
        assertMemoryLeak(() -> {
            CairoTestUtils.createAllTableWithNewTypes(configuration, partitionBy);
            // this is BLOB
            byte[] blob = new byte[500];
            TestBinarySequence bs = new TestBinarySequence();
            bs.of(blob);
            Rnd rnd = new Rnd();

            if (initBindVariables) {
                // this is type declaration to have query compile correctly
                bindVariableService.setInt(0, 0);
                bindVariableService.setShort(1, (short) 10);
                bindVariableService.setByte(2, (byte) 91);
                bindVariableService.setDouble(3, 9.2);
                bindVariableService.setFloat(4, 5.6f);
                bindVariableService.setLong(5, 99901);
                bindVariableService.setStr(6, "hello kitty");
                bindVariableService.setStr(7, "sym?");
                bindVariableService.setBoolean(8, true);
                bindVariableService.setBin(9, bs);
                bindVariableService.setDate(10, 1234L);
                bindVariableService.setLong256(11, 1, 2, 3, 4);
                bindVariableService.setChar(12, 'A');
                bindVariableService.setTimestamp(13, timestampFunction.getTimestamp());
            }

            final String sql;
            if (columnSet) {
                sql = "insert into all2 (" +
                        "int, " +
                        "short, " +
                        "byte, " +
                        "double, " +
                        "float, " +
                        "long, " +
                        "str, " +
                        "sym, " +
                        "bool, " +
                        "bin, " +
                        "date, " +
                        "long256, " +
                        "chr, " +
                        "timestamp" +
                        ") values (" +
                        "$1, " +
                        "$2, " +
                        "$3, " +
                        "$4, " +
                        "$5, " +
                        "$6, " +
                        "$7, " +
                        "$8, " +
                        "$9, " +
                        "$10, " +
                        "$11, " +
                        "$12, " +
                        "$13, " +
                        "$14)";
            } else {
                sql = "insert into all2 values (" +
                        "$1, " +
                        "$2, " +
                        "$3, " +
                        "$4, " +
                        "$5, " +
                        "$6, " +
                        "$7, " +
                        "$8, " +
                        "$9, " +
                        "$10, " +
                        "$11, " +
                        "$12, " +
                        "$13, " +
                        "$14)";
            }

            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);

            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                for (int i = 0; i < 10_000; i++) {
                    bindVariableService.setInt(0, rnd.nextInt());
                    bindVariableService.setShort(1, rnd.nextShort());
                    bindVariableService.setByte(2, rnd.nextByte());
                    bindVariableService.setDouble(3, rnd.nextDouble());
                    bindVariableService.setFloat(4, rnd.nextFloat());
                    bindVariableService.setLong(5, rnd.nextLong());
                    bindVariableService.setStr(6, rnd.nextChars(6));
                    bindVariableService.setStr(7, rnd.nextChars(1));
                    bindVariableService.setBoolean(8, rnd.nextBoolean());
                    rnd.nextBytes(blob);
                    bindVariableService.setBin(9, bs);
                    bindVariableService.setDate(10, rnd.nextLong());
                    bindVariableService.setLong256(11, rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                    bindVariableService.setChar(12, rnd.nextChar());
                    bindVariableService.setTimestamp(13, timestampFunction.getTimestamp());
                    method.execute();
                }
                method.commit();
            }

            rnd.reset();
            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "all2")) {
                final TableReaderRecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextShort(), record.getShort(1));
                    Assert.assertEquals(rnd.nextByte(), record.getByte(2));
                    Assert.assertEquals(rnd.nextDouble(), record.getDouble(3), 0.0001);
                    Assert.assertEquals(rnd.nextFloat(), record.getFloat(4), 0.000001);
                    Assert.assertEquals(rnd.nextLong(), record.getLong(5));
                    TestUtils.assertEquals(rnd.nextChars(6), record.getStr(6));
                    TestUtils.assertEquals(rnd.nextChars(1), record.getSym(7));
                    Assert.assertEquals(rnd.nextBoolean(), record.getBool(8));
                    rnd.nextBytes(blob);
                    BinarySequence binarySequence = record.getBin(9);
                    Assert.assertEquals(blob.length, binarySequence.length());
                    for (int j = 0, m = blob.length; j < m; j++) {
                        Assert.assertEquals(blob[j], binarySequence.byteAt(j));
                    }
                    Assert.assertEquals(rnd.nextLong(), record.getDate(10));
                    Long256 long256 = record.getLong256A(11);
                    Assert.assertEquals(rnd.nextLong(), long256.getLong0());
                    Assert.assertEquals(rnd.nextLong(), long256.getLong1());
                    Assert.assertEquals(rnd.nextLong(), long256.getLong2());
                    Assert.assertEquals(rnd.nextLong(), long256.getLong3());
                    Assert.assertEquals(rnd.nextChar(), record.getChar(12));
                }
            }
        });
    }

    @FunctionalInterface
    private interface TimestampFunction {
        long getTimestamp();
    }
}
