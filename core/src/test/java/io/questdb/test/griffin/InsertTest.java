/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class InsertTest extends AbstractCairoTest {

    private final boolean walEnabled;

    public InsertTest(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false}, {true}
        });
    }

    public void assertReaderCheckWal(String expected, CharSequence tableName) {
        if (walEnabled) {
            drainWalQueue();
        }
        assertReader(expected, tableName);
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
    }

    @Test
    public void testAutoIncrementUniqueId_FirstColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table currencies(id long, ccy symbol, ts timestamp) timestamp(ts)");

            insert("insert into currencies values (1, 'USD', '2019-03-10T00:00:00.000000Z')");
            assertSql("id\tccy\tts\n" +
                    "1\tUSD\t2019-03-10T00:00:00.000000Z\n", "currencies");

            ddl("insert into currencies select max(id) + 1, 'EUR', '2019-03-10T01:00:00.000000Z' from currencies");
            assertSql("id\tccy\tts\n" +
                    "1\tUSD\t2019-03-10T00:00:00.000000Z\n" +
                    "2\tEUR\t2019-03-10T01:00:00.000000Z\n", "currencies");

            ddl("insert into currencies select max(id) + 1, 'GBP', '2019-03-10T02:00:00.000000Z' from currencies");
            assertSql("id\tccy\tts\n" +
                    "1\tUSD\t2019-03-10T00:00:00.000000Z\n" +
                    "2\tEUR\t2019-03-10T01:00:00.000000Z\n" +
                    "3\tGBP\t2019-03-10T02:00:00.000000Z\n", "currencies");
        });
    }

    @Test
    public void testAutoIncrementUniqueId_NotFirstColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table currencies(ccy symbol, id long, ts timestamp) timestamp(ts)");

            insert("insert into currencies values ('USD', 1, '2019-03-10T00:00:00.000000Z')");
            assertSql("ccy\tid\tts\n" +
                    "USD\t1\t2019-03-10T00:00:00.000000Z\n", "currencies");

            ddl("insert into currencies select 'EUR', max(id) + 1, '2019-03-10T01:00:00.000000Z' from currencies");
            assertSql("ccy\tid\tts\n" +
                    "USD\t1\t2019-03-10T00:00:00.000000Z\n" +
                    "EUR\t2\t2019-03-10T01:00:00.000000Z\n", "currencies");

            ddl("insert into currencies select 'GBP', max(id) + 1, '2019-03-10T02:00:00.000000Z' from currencies");
            assertSql("ccy\tid\tts\n" +
                    "USD\t1\t2019-03-10T00:00:00.000000Z\n" +
                    "EUR\t2\t2019-03-10T01:00:00.000000Z\n" +
                    "GBP\t3\t2019-03-10T02:00:00.000000Z\n", "currencies");
        });
    }

    @Test
    public void testGeoHash() throws Exception {
        final TimestampFunction timestampFunction = new TimestampFunction() {
            private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30 * 12;
            }
        };

        assertMemoryLeak(() -> {
            try (TableModel model = CreateTableTestUtils.getGeoHashTypesModelWithNewTypes(configuration, PartitionBy.YEAR)) {
                TestUtils.create(model, engine);
            }
            Rnd rnd = new Rnd();

            final String sql;
            sql = "insert into allgeo values (" +
                    "$1, " +
                    "$2, " +
                    "$3, " +
                    "$4, " +
                    "$5)";


            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insert = cq.getInsertOperation();
                try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    for (int i = 0; i < 10_000; i++) {
                        bindVariableService.setGeoHash(0, rnd.nextGeoHashByte(6), ColumnType.getGeoHashTypeWithBits(6));
                        bindVariableService.setGeoHash(1, rnd.nextGeoHashShort(12), ColumnType.getGeoHashTypeWithBits(12));
                        // truncate this one, target column is 27 bit
                        bindVariableService.setGeoHash(2, rnd.nextGeoHashInt(29), ColumnType.getGeoHashTypeWithBits(29));
                        bindVariableService.setGeoHash(3, rnd.nextGeoHashLong(44), ColumnType.getGeoHashTypeWithBits(44));
                        bindVariableService.setTimestamp(4, timestampFunction.getTimestamp());
                        method.execute();
                    }
                    method.commit();
                }
            }

            rnd.reset();
            try (TableReader reader = getReader("allgeo")) {
                final TableReaderRecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextGeoHashByte(6), record.getGeoByte(0));
                    Assert.assertEquals(rnd.nextGeoHashShort(12), record.getGeoShort(1));
                    Assert.assertEquals(GeoHashes.widen(rnd.nextGeoHashInt(29), 29, 27), record.getGeoInt(2));
                    Assert.assertEquals(rnd.nextGeoHashLong(44), record.getGeoLong(3));
                }
            }
        });
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
    public void testInsertAllByWeek() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 7;
                    }
                },
                true,
                true
        );
    }

    @Test
    public void testInsertAllByWeekUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 7;
                    }
                },
                false,
                true
        );
    }

    @Test
    public void testInsertAllByWeekUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
                    private long last = TimestampFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

                    @Override
                    public long getTimestamp() {
                        return last = last + 100000L * 7;
                    }
                },
                false,
                false
        );
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
                "seq\tts\n" +
                        "1\t1970-01-01T00:00:00.123456Z\n",
                "insert into tab select 1, '123456'",
                null,
                false
        );
    }

    @Test
    public void testInsertAsSelectTimestampAscOrder() throws Exception {
        testInsertAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testInsertAsSelectTimestampDescOrder() throws Exception {
        testInsertAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testInsertAsSelectTimestampNoOrder() throws Exception {
        testInsertAsSelectWithOrderBy("");
    }

    @Test
    public void testInsertAsWith() throws Exception {
        assertInsertTimestamp(
                "seq\tts\n" +
                        "1\t1970-01-01T00:00:00.123456Z\n",
                "with x as (select 1, '123456') insert into tab  select * from x",
                null,
                false
        );
    }

    @Test
    public void testInsertContextSwitch() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            sqlExecutionContext.getBindVariableService().setDouble("bal", 150.4);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', :bal)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insertOperation = cq.getInsertOperation();

                try (InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                    method.execute();
                    method.commit();
                }

                BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
                bindVariableService.setDouble("bal", 56.4);

                try (
                        SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, bindVariableService);
                        InsertMethod method = insertOperation.createMethod(sqlExecutionContext)
                ) {
                    method.execute();
                    method.commit();
                }
            }

            assertReaderCheckWal("cust_id\tccy\tbalance\n" +
                    "1\tGBP\t150.4\n" +
                    "1\tGBP\t56.4\n", "balances");
        });
    }

    @Test
    public void testInsertEmptyStringSelectEmptyStringColumnIndexed() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table tab (id int, val symbol index)");
                    insert("insert into tab values (1, '')");
                    assertSql("id\n1\n", "select id from tab where val = ''");
                }
        );
    }

    @Test
    public void testInsertEmptyStringSelectNullStringColumnIndexed() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table tab (id int, val symbol index)");
                    insert("insert into tab values (1, '')");
                    assertSql("id\n", "select id from tab where val = null");
                }
        );
    }

    @Test
    public void testInsertExecutionAfterStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', 356.12)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insertOperation = cq.getInsertOperation();

                ddl("alter table balances drop column ccy", sqlExecutionContext);

                insertOperation.createMethod(sqlExecutionContext);
                Assert.fail();
            } catch (TableReferenceOutOfDateException ignored) {
            }
        });
    }

    @Test
    public void testInsertExplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)");
            insert("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'))");
            String expected = "timestamp\tfield\tvalue\n" +
                    "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
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
    public void testInsertISOMilliWithTzDateStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-02T23:00:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T00:00:00+01')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumn2() throws Exception {
        final String expected = "seq\tts\n" +
                "1\t2021-01-03T03:30:00.000000Z\n";

        assertInsertTimestamp(
                expected,
                "insert into tab values (1, '2021-01-03T02:00:00-01:30')",
                null,
                true
        );
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumnFails() throws Exception {
        assertInsertTimestamp(
                "inconvertible value: `2021-01-03T02:00:00-:30` [STRING -> TIMESTAMP]",
                "insert into tab values (1, '2021-01-03T02:00:00-:30')",
                ImplicitCastException.class,
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
    public void testInsertImplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)");
            insert("INSERT INTO TS values(to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'),'X',123.33d)");
            String expected = "timestamp\tfield\tvalue\n" +
                    "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertInvalidColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            try {
                assertException("insert into balances(cust_id, ccy2, balance) values (1, 'GBP', 356.12)", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(30, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testInsertInvalidDateStringTimestampColumn() throws Exception {
        assertInsertTimestamp(
                "inconvertible value: `2021-23-03T00:00:00Z` [STRING -> TIMESTAMP]",
                "insert into tab values (1, '2021-23-03T00:00:00Z')",
                ImplicitCastException.class,
                true
        );
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp, sym symbol) timestamp(ts);");
            insert("insert into trades VALUES (1262599200000000, 'USDJPY'), (3262599300000000, 'USDFJD');");

            String expected = "ts\tsym\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp, sym symbol) timestamp(ts);");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final String sql = "insert into trades VALUES (1262599200000000, $1), (3262599300000000, $2);";
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insert = cq.getInsertOperation();
                try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    bindVariableService.setStr(0, "USDJPY");
                    bindVariableService.setStr(1, "USDFJD");
                    method.execute();
                    method.commit();
                }
            }
            String expected = "ts\tsym\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\n";
            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsExtraParentheses() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (i INT, sym symbol)");
            insert("insert into trades VALUES ((1), 'USD'), ((2), (('FJD')));");

            String expected = "i\tsym\n" +
                    "1\tUSD\n" +
                    "2\tFJD\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsFailInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (i int, sym symbol)");

            // No comma delimiter between rows
            assertException("insert into trades VALUES (1, 'USDJPY')(2, 'USDFJD');", 39, "',' expected");

            // Empty row
            assertException("insert into trades VALUES (1, 'USDJPY'), ();", 42, "Expression expected");

            // Empty row with comma delimiter inside
            assertException("insert into trades VALUES (1, 'USDJPY'), (2, 'USDFJD'), (,);", 57, "Expression expected");

            // Empty row column
            assertException("insert into trades VALUES (1, 'USDJPY'), (2, 'USDFJD'), (3,);", 59, "Expression expected");

            // Multi row insert can't end in comma token
            assertException("insert into trades VALUES (1, 'USDJPY'), (2, 'USDFJD'),;", 55, "'(' expected");
        });
    }

    @Test
    public void testInsertMultipleRowsFailRowWrongColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (i int, sym symbol)");
            assertException("insert into trades VALUES (1, 'USDJPY'), ('USDFJD');", 50, "row value count does not match column count [expected=2, actual=1, tuple=2]");
        });
    }

    @Test
    public void testInsertMultipleRowsFailTypeConversion() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (sym symbol)");
            assertException("insert into trades VALUES ('USDJPY'), (1), ('USDFJD');", 39, "inconvertible types: INT -> SYMBOL [from=1, to=sym]");
        });
    }

    @Test
    public void testInsertMultipleRowsMissingBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (ts timestamp, i int) timestamp(ts);");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final String sql = "insert into t VALUES (1262599200000000, $1), (3262599300000000, $2);";
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insert = cq.getInsertOperation();
                try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    bindVariableService.setInt(0, 1);
                    method.execute();
                    method.commit();
                }
            }
            String expected = "ts\ti\n" +
                    "2010-01-04T10:00:00.000000Z\t1\n" +
                    "2073-05-21T13:35:00.000000Z\tNaN\n";
            assertReaderCheckWal(expected, "t");
        });
    }

    @Test
    public void testInsertMultipleRowsOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp) timestamp(ts);");
            try {
                insert("insert into trades VALUES (1), (3), (2);");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot insert rows out of order to non-partitioned table.");
            }
        });
    }

    @Test
    public void testInsertNoSelfReference() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE trades_aapl (ts TIMESTAMP, px INT, qty int, side STRING) TIMESTAMP(ts)");
            assertException("insert into trades_aapl (ts) values (ts)", 37, "Invalid column");
        });
    }

    @Test
    public void testInsertNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            insert("insert into balances values (1, 'USD', 356.12)");
            String expected = "cust_id\tccy\tbalance\n" +
                    "1\tUSD\t356.12\n";

            assertReaderCheckWal(expected, "balances");
        });
    }

    @Test
    public void testInsertNotEnoughFields() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            assertException("insert into balances values (1, 'USD')", 37, "row value count does not match column count [expected=3, actual=2, tuple=1]");
        });
    }

    @Test
    public void testInsertNullStringSelectEmptyStringColumnIndexed() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table tab (id int, val symbol index)");
                    insert("insert into tab values (1, NULL)");
                    assertSql("id\n", "select id from tab where val = ''");
                }
        );
    }

    @Test
    public void testInsertNullStringSelectNullStringColumnIndexed() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table tab (id int, val symbol index)");
                    insert("insert into tab values (1, null)");
                    assertSql("id\n1\n", "select id from tab where val = null");
                }
        );
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
                false
        );
    }

    @Test
    public void testInsertSingleCharacterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table ww (id int, sym symbol)");
            insert("insert into ww VALUES ( 2, 'A')");
            String expected = "id\tsym\n" +
                    "2\tA\n";

            assertReaderCheckWal(expected, "ww");
        });
    }

    @Test
    public void testInsertSymbolNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table symbols (sym symbol, isNewSymbol BOOLEAN)");
            insert("insert into symbols (sym, isNewSymbol) VALUES ('USDJPY', false);");
            insert("insert into symbols (sym, isNewSymbol) VALUES ('USDFJD', true);");

            String expected = "sym\tisNewSymbol\n" +
                    "USDJPY\tfalse\n" +
                    "USDFJD\ttrue\n";

            assertReaderCheckWal(expected, "symbols");
        });
    }

    @Test
    public void testInsertSymbolPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            insert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            insert("insert into trades VALUES ( 1262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" +
                    "2010-01-04T10:01:40.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            insert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");

            String expected1 = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n";

            assertReaderCheckWal(expected1, "trades");

            try (TableWriter w = getWriter("trades")) {
                w.truncate();
            }

            insert("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected2 = "ts\tsym\tbid\task\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected2, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedFarApart() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            insert("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            insert("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" +
                    "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" +
                    "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertTimestampWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (timestamp timestamp) timestamp(timestamp);");
            insert("insert into t values (timestamp with time zone '2020-12-31 15:15:51.663+00:00')");

            String expected1 = "timestamp\n" +
                    "2020-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected1, "t");

            insert("insert into t values (cast('2021-12-31 15:15:51.663+00:00' as timestamp with time zone))");

            String expected2 = expected1 +
                    "2021-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected2, "t");

            assertException("insert into t values  (timestamp with time zone)", 47, "String literal expected after 'timestamp with time zone'");
        });
    }

    @Test
    public void testInsertValueCannotReferenceTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(cust_id int, ccy symbol, balance double)");
            assertException("insert into balances values (1, ccy, 356.12)", 32, "Invalid column: ccy");
        });
    }

    @Test
    public void testInsertValuesAsLambda() throws Exception {
        assertException(
                "insert into names values(select rnd_str('Tom', 'Anna', 'John', 'Tim', 'Kim', 'Jim'), rnd_str('Smith', 'Mason', 'Johnson', 'Thompson') from long_sequence(8))",
                25,
                "query is not allowed here"
        );
    }

    @Test
    public void testInsertWithLessColumnsThanExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select x ac  from long_sequence(10)", 12, "select clause must provide timestamp column");
        });
    }

    @Test
    public void testInsertWithWrongDesignatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select * from (select  timestamp_sequence(0, x) ts, x ac from long_sequence(10)) timestamp(ts)", 12, "designated timestamp of existing table");
        });
    }

    @Test
    public void testInsertWithoutDesignatedTimestamp() throws Exception {
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

        if (walEnabled) {
            drainWalQueue();
        }
        assertQuery(
                "seq\tts\n",
                "tab",
                "create table tab(seq long, ts timestamp) timestamp(ts);",
                "ts",
                "insert into tab select x ac, timestamp_sequence(0, x) ts from long_sequence(10)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testInsertWithoutDesignatedTimestampAndTypeDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select x ac, rnd_int() id from long_sequence(10)", 12, "expected timestamp column");
        });
    }

    @Test
    public void testInsertWrongTypeConstant() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test (a timestamp)", sqlExecutionContext);
            assertException("insert into test values ('foobar')", 0, "inconvertible value: `foobar` [STRING -> TIMESTAMP]");
        });
    }

    private void assertInsertTimestamp(String expected, String ddl2, Class<?> exceptionType, boolean commitInsert) throws Exception {
        if (commitInsert) {
            ddl("create table tab(seq long, ts timestamp) timestamp(ts)");
            try {
                insert(ddl2);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql(expected, "tab");
            } catch (Throwable e) {
                if (exceptionType == null) {
                    throw e;
                }
                Assert.assertSame(exceptionType, e.getClass());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        } else {
            ddl("create table tab(seq long, ts timestamp) timestamp(ts)");
            try {
                ddl(ddl2);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql(expected, "tab");
            } catch (Throwable e) {
                if (exceptionType == null) throw e;
                Assert.assertSame(exceptionType, e.getClass());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        }

        drop("drop table tab");

        if (commitInsert) {
            ddl("create table tab(seq long, ts timestamp)");
            try {
                insert(ddl2);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql(expected, "tab");
            } catch (Throwable e) {
                if (exceptionType == null) throw e;
                Assert.assertSame(exceptionType, e.getClass());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        } else {
            ddl("create table tab(seq long, ts timestamp)");
            try {
                ddl(ddl2, sqlExecutionContext);
                if (exceptionType != null) {
                    Assert.fail("SqlException expected");
                }
                assertSql(expected, "tab");
            } catch (Throwable e) {
                e.printStackTrace();
                if (exceptionType == null) throw e;
                Assert.assertSame(exceptionType, e.getClass());
                TestUtils.assertContains(e.getMessage(), expected);
            }
        }
    }

    private void assertQueryCheckWal(String expected) throws SqlException {
        if (walEnabled) {
            drainWalQueue();
        }

        assertQuery(
                expected,
                "dest",
                "ts",
                true,
                true
        );
    }

    private void testBindVariableInsert(
            int partitionBy,
            TimestampFunction timestampFunction,
            boolean initBindVariables,
            boolean columnSet
    ) throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithNewTypes(engine, partitionBy);
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

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);

                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertOperation insert = cq.getInsertOperation();
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
            }

            rnd.reset();
            try (TableReader reader = getReader("all2")) {
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

    private void testInsertAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table src (ts timestamp, v long) timestamp(ts) partition by day;");
            insert("insert into src values (0, 0);");
            insert("insert into src values (10000, 1);");
            insert("insert into src values (20000, 2);");
            insert("insert into src values (30000, 3);");
            insert("insert into src values (40000, 4);");

            ddl("create table dest (ts timestamp, v long) timestamp(ts) partition by day;");
            drainWalQueue();

            ddl("insert into dest select * from src where v % 2 = 0 " + orderByClause + ";");

            String expected = "ts\tv\n" +
                    "1970-01-01T00:00:00.000000Z\t0\n" +
                    "1970-01-01T00:00:00.020000Z\t2\n" +
                    "1970-01-01T00:00:00.040000Z\t4\n";

            assertQueryCheckWal(expected);
        });
    }

    @FunctionalInterface
    private interface TimestampFunction {
        long getTimestamp();
    }
}
