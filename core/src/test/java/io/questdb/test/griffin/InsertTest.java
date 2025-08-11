/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
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
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    public void assertReaderCheckWal(String expected, CharSequence tableName) {
        if (walEnabled) {
            drainWalQueue();
        }
        assertReader(expected, tableName);
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        engine.getMatViewStateStore().clear();
    }

    @Test
    public void testAutoIncrementUniqueId_FirstColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table currencies(id long, ccy symbol, ts timestamp) timestamp(ts)");

            execute("insert into currencies values (1, 'USD', '2019-03-10T00:00:00.000000Z')");
            assertSql("id\tccy\tts\n" + "1\tUSD\t2019-03-10T00:00:00.000000Z\n", "currencies");

            execute("insert into currencies select max(id) + 1, 'EUR', '2019-03-10T01:00:00.000000Z' from currencies");
            assertSql("id\tccy\tts\n" + "1\tUSD\t2019-03-10T00:00:00.000000Z\n" + "2\tEUR\t2019-03-10T01:00:00.000000Z\n", "currencies");

            execute("insert into currencies select max(id) + 1, 'GBP', '2019-03-10T02:00:00.000000Z' from currencies");
            assertSql("id\tccy\tts\n" + "1\tUSD\t2019-03-10T00:00:00.000000Z\n" + "2\tEUR\t2019-03-10T01:00:00.000000Z\n" + "3\tGBP\t2019-03-10T02:00:00.000000Z\n", "currencies");
        });
    }

    @Test
    public void testAutoIncrementUniqueId_NotFirstColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table currencies(ccy symbol, id long, ts timestamp) timestamp(ts)");

            execute("insert into currencies values ('USD', 1, '2019-03-10T00:00:00.000000Z')");
            assertSql("ccy\tid\tts\n" + "USD\t1\t2019-03-10T00:00:00.000000Z\n", "currencies");

            execute("insert into currencies select 'EUR', max(id) + 1, '2019-03-10T01:00:00.000000Z' from currencies");
            assertSql("ccy\tid\tts\n" + "USD\t1\t2019-03-10T00:00:00.000000Z\n" + "EUR\t2\t2019-03-10T01:00:00.000000Z\n", "currencies");

            execute("insert into currencies select 'GBP', max(id) + 1, '2019-03-10T02:00:00.000000Z' from currencies");
            assertSql("ccy\tid\tts\n" + "USD\t1\t2019-03-10T00:00:00.000000Z\n" + "EUR\t2\t2019-03-10T01:00:00.000000Z\n" + "GBP\t3\t2019-03-10T02:00:00.000000Z\n", "currencies");
        });
    }

    @Test
    public void testCannotInsertIntoMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table currencies(ccy symbol, id long, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into currencies values ('USD', 1, '2019-03-10T00:00:00.000000Z')");
            execute("insert into currencies select 'EUR', max(id) + 1, '2019-03-10T01:00:00.000000Z' from currencies");
            execute("insert into currencies select 'GBP', max(id) + 1, '2019-03-10T02:00:00.000000Z' from currencies");

            execute("create materialized view curr_view as (select ts, max(id) as id from currencies sample by 1h) partition by day");
            try {
                execute("insert into curr_view values ('SEK', 3, '2019-03-10T03:00:00.000000Z')");
                Assert.fail("INSERT should fail");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot modify materialized view [view=curr_view]");
                Assert.assertEquals(12, e.getPosition());
            }
        });
    }

    @Test
    public void testGeoHash() throws Exception {
        final TimestampFunction timestampFunction = new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30 * 12;
            }
        };

        assertMemoryLeak(() -> {
            TableModel model = CreateTableTestUtils.getGeoHashTypesModelWithNewTypes(configuration, PartitionBy.YEAR);
            TestUtils.createTable(engine, model);
            Rnd rnd = new Rnd();

            final String sql;
            sql = "insert into allgeo values (" + "$1, " + "$2, " + "$3, " + "$4, " + "$5)";


            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                try (InsertOperation insert = cq.popInsertOperation();
                     InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    for (int i = 0; i < 10_000; i++) {
                        bindVariableService.setGeoHash(0, rnd.nextGeoHashByte(6), ColumnType.getGeoHashTypeWithBits(6));
                        bindVariableService.setGeoHash(1, rnd.nextGeoHashShort(12), ColumnType.getGeoHashTypeWithBits(12));
                        // truncate this one, target column is 27 bit
                        bindVariableService.setGeoHash(2, rnd.nextGeoHashInt(29), ColumnType.getGeoHashTypeWithBits(29));
                        bindVariableService.setGeoHash(3, rnd.nextGeoHashLong(44), ColumnType.getGeoHashTypeWithBits(44));
                        bindVariableService.setTimestamp(4, timestampFunction.getTimestamp());
                        method.execute(sqlExecutionContext);
                    }
                    method.commit();
                }
            }

            rnd.reset();
            try (
                    TableReader reader = getReader("allgeo");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
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
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, true, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByDayUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, false, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByDayUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, false, false, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByDayUndefinedNoColumnSetWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, false, false, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByDayUndefinedWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, false, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByDayWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        }, true, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByMonth() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, true, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByMonthUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, false, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByMonthUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, false, false, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByMonthUndefinedNoColumnSetWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, false, false, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByMonthUndefinedWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, false, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByMonthWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        }, true, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByNone() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, true, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByNoneUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByNoneUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, false, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByNoneUndefinedNoColumnSetWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, false, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByNoneUndefinedWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, false, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByNoneWithNanoTs() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0, true, true, ColumnType.TIMESTAMP_NANO);
    }

    @Test
    public void testInsertAllByWeek() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 7;
            }
        }, true, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByWeekUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 7;
            }
        }, false, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByWeekUndefinedNoColumnSet() throws Exception {
        testBindVariableInsert(PartitionBy.WEEK, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 7;
            }
        }, false, false, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByYear() throws Exception {
        testBindVariableInsert(PartitionBy.YEAR, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30 * 12;
            }
        }, true, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAllByYearUndefined() throws Exception {
        testBindVariableInsert(PartitionBy.YEAR, new TimestampFunction() {
            private long last = MicrosFormatUtils.parseTimestamp("2019-03-10T00:00:00.000000Z");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30 * 12;
            }
        }, false, true, ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testInsertAsSelectISODateStringToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab select 1, '2021-01-03'", null, false, "timestamp");
    }

    @Test
    public void testInsertAsSelectISODateStringToDesignatedTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab select 1, '2021-01-03'", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsSelectISODateVarcharToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab select 1, '2021-01-03'::varchar", null, false, "timestamp");
    }

    @Test
    public void testInsertAsSelectISODateVarcharToDesignatedTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab select 1, '2021-01-03'::varchar", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsSelectNumberStringToDesignatedTimestampColumn() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456Z\n", "insert atomic into tab select 1, '123456'", null, false, "timestamp");
    }

    @Test
    public void testInsertAsSelectNumberStringToDesignatedTimestampNSColumn() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456789Z\n", "insert atomic into tab select 1, '123456789'", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsSelectNumberVarcharToDesignatedTimestampColumn() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456Z\n", "insert atomic into tab select 1, '123456'::varchar", null, false, "timestamp");
    }

    @Test
    public void testInsertAsSelectNumberVarcharToDesignatedTimestampNSColumn() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456789Z\n", "insert atomic into tab select 1, '123456789'::varchar", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsSelectTimestampAscOrder() throws Exception {
        testInsertAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testInsertAsSelectTimestampAscOrderWithNanoTS() throws Exception {
        testInsertAsSelectWithOrderByWithNanoTS("order by ts asc");
    }

    @Test
    public void testInsertAsSelectTimestampDescOrder() throws Exception {
        testInsertAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testInsertAsSelectTimestampDescOrderWithNanoTS() throws Exception {
        testInsertAsSelectWithOrderByWithNanoTS("order by ts desc");
    }

    @Test
    public void testInsertAsSelectTimestampNoOrder() throws Exception {
        testInsertAsSelectWithOrderBy("");
    }

    @Test
    public void testInsertAsSelectTimestampNoOrderWithNanoTS() throws Exception {
        testInsertAsSelectWithOrderByWithNanoTS("");
    }

    @Test
    public void testInsertAsWithNanos_string() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456789Z\n", "with x as (select 1, '123456789') insert atomic into tab select * from x", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsWithNanos_varchar() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456789Z\n", "with x as (select 1, '123456789'::varchar) insert atomic into tab select * from x", null, false, "timestamp_ns");
    }

    @Test
    public void testInsertAsWith_string() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456Z\n", "with x as (select 1, '123456'::string) insert atomic into tab select * from x", null, false, "timestamp");
    }

    @Test
    public void testInsertAsWith_varchar() throws Exception {
        assertInsertTimestamp("seq\tts\n" + "1\t1970-01-01T00:00:00.123456Z\n", "with x as (select 1, '123456'::varchar) insert atomic into tab select * from x", null, false, "timestamp");
    }

    @Test
    public void testInsertContextSwitch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            sqlExecutionContext.getBindVariableService().setDouble("bal", 150.4);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', :bal)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());

                try (InsertOperation insertOperation = cq.popInsertOperation()) {
                    try (InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                        method.execute(sqlExecutionContext);
                        method.commit();
                    }

                    BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
                    bindVariableService.setDouble("bal", 56.4);

                    try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, bindVariableService); InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                        method.execute(sqlExecutionContext);
                        method.commit();
                    }
                }
            }

            assertReaderCheckWal("cust_id\tccy\tbalance\n" + "1\tGBP\t150.4\n" + "1\tGBP\t56.4\n", "balances");
        });
    }

    @Test
    public void testInsertContextSwitch_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            sqlExecutionContext.getBindVariableService().setDouble("bal", 150.4);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP'::varchar, :bal)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                try (InsertOperation insertOperation = cq.popInsertOperation()) {
                    try (InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                        method.execute(sqlExecutionContext);
                        method.commit();
                    }
                    BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
                    bindVariableService.setDouble("bal", 56.4);

                    try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, bindVariableService); InsertMethod method = insertOperation.createMethod(sqlExecutionContext)) {
                        method.execute(sqlExecutionContext);
                        method.commit();
                    }
                }
            }

            assertReaderCheckWal("cust_id\tccy\tbalance\n" + "1\tGBP\t150.4\n" + "1\tGBP\t56.4\n", "balances");
        });
    }

    @Test
    public void testInsertEmptyStringSelectEmptyStringColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, '')");
            assertSql("id\n1\n", "select id from tab where val = ''");
        });
    }

    @Test
    public void testInsertEmptyStringSelectNullStringColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, '')");
            assertSql("id\n", "select id from tab where val = null");
        });
    }

    @Test
    public void testInsertEmptyVarcharSelectEmptyVarcharColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, ''::varchar)");
            assertSql("id\n1\n", "select id from tab where val = ''");
        });
    }

    @Test
    public void testInsertEmptyVarcharSelectNullVarcharColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, ''::varchar)");
            assertSql("id\n", "select id from tab where val = null");
        });
    }

    @Test
    public void testInsertExecutionAfterStructureChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile("insert into balances values (1, 'GBP', 356.12)", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                try (InsertOperation insertOperation = cq.popInsertOperation()) {
                    execute("alter table balances drop column ccy", sqlExecutionContext);
                    insertOperation.createMethod(sqlExecutionContext);
                }
                Assert.fail();
            } catch (TableReferenceOutOfDateException ignored) {
            }
        });
    }

    @Test
    public void testInsertExplicitTimestampNSPos1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP_NS, field STRING, value DOUBLE) TIMESTAMP(timestamp) partition by day");
            execute("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'))");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertExplicitTimestampNSPos1_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP_NS, field STRING, value DOUBLE) TIMESTAMP(timestamp) partition by day");
            execute("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49'::varchar, 'yyyy-MM-ddTHH:mm:ss'))");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertExplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)");
            execute("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'))");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertExplicitTimestampPos1_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)");
            execute("INSERT INTO TS(field, value, timestamp) values('X',123.33, to_timestamp('2019-12-04T13:20:49'::varchar, 'yyyy-MM-ddTHH:mm:ss'))");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertISODateStringToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03')", null, true, "timestamp");
    }

    @Test
    public void testInsertISODateStringToDesignatedTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISODateVarcharToDesignatedTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertISODateVarcharToDesignatedTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03'::varchar)", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMicroStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000Z')", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMicroStringTimestampColumnNSNoTimezone() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000001Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000001')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMicroStringTimestampColumnNoTimezone() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000')", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMicroStringTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000000Z')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMicroVarcharTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000Z'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMicroVarcharTimestampColumnNoTimezone() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMicroVarcharTimestampNSColumnNoTimezone() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000123Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00.000000123'::varchar)", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-02T23:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00+01')", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumn2() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T03:30:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T02:00:00-01:30')", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampColumnFails() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-01-03T02:00:00-:30` [STRING -> TIMESTAMP]", "insert into tab values (1, '2021-01-03T02:00:00-:30')", ImplicitCastException.class, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-02T23:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00+01')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampNSColumn2() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T03:30:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T02:00:00-01:30')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateStringTimestampNSColumnFails() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-01-03T02:00:00-:30` [STRING -> TIMESTAMP_NS]", "insert into tab values (1, '2021-01-03T02:00:00-:30')", ImplicitCastException.class, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-02T23:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00+01'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampColumn2() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T03:30:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T02:00:00-01:30'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampColumnFails() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-01-03T02:00:00-:30` [VARCHAR -> TIMESTAMP]", "insert into tab values (1, '2021-01-03T02:00:00-:30'::varchar)", ImplicitCastException.class, true, "timestamp");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-02T23:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00+01'::varchar)", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampNSColumn2() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T03:30:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T02:00:00-01:30'::varchar)", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOMilliWithTzDateVarcharTimestampNSColumnFails() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-01-03T02:00:00-:30` [VARCHAR -> TIMESTAMP_NS]", "insert into tab values (1, '2021-01-03T02:00:00-:30'::varchar)", ImplicitCastException.class, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOSecondsDateStringTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00Z')", null, true, "timestamp");
    }

    @Test
    public void testInsertISOSecondsDateStringTimestampNSColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00Z')", null, true, "timestamp_ns");
    }

    @Test
    public void testInsertISOSecondsDateVarcharTimestampColumn() throws Exception {
        final String expected = "seq\tts\n" + "1\t2021-01-03T00:00:00.000000Z\n";

        assertInsertTimestamp(expected, "insert into tab values (1, '2021-01-03T00:00:00Z'::varchar)", null, true, "timestamp");
    }

    @Test
    public void testInsertImplicitTimestampPos1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP, field STRING, value DOUBLE) TIMESTAMP(timestamp)");
            execute("INSERT INTO TS values(to_timestamp('2019-12-04T13:20:49', 'yyyy-MM-ddTHH:mm:ss'),'X',123.33d)");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertImplicitTimestampPos1_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE TS (timestamp TIMESTAMP, field VARCHAR, value DOUBLE) TIMESTAMP(timestamp)");
            execute("INSERT INTO TS values(to_timestamp('2019-12-04T13:20:49'::varchar, 'yyyy-MM-ddTHH:mm:ss'),'X',123.33d)");
            String expected = "timestamp\tfield\tvalue\n" + "2019-12-04T13:20:49.000000Z\tX\t123.33\n";

            assertReaderCheckWal(expected, "TS");
        });
    }

    @Test
    public void testInsertIntoNonExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("insert into tab values (1)");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=tab]");
                Assert.assertEquals(12, e.getPosition());
            }
        });
    }

    @Test
    public void testInsertInvalidColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            try {
                assertExceptionNoLeakCheck("insert into balances(cust_id, ccy2, balance) values (1, 'GBP', 356.12)", sqlExecutionContext);
            } catch (SqlException e) {
                Assert.assertEquals(30, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column");
            }
        });
    }

    @Test
    public void testInsertInvalidDateStringTimestampColumn() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-23-03T00:00:00Z` [STRING -> TIMESTAMP]", "insert into tab values (1, '2021-23-03T00:00:00Z')", ImplicitCastException.class, true, "timestamp");
    }

    @Test
    public void testInsertInvalidDateStringTimestampNSColumn() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-23-03T00:00:00Z` [STRING -> TIMESTAMP_NS]", "insert into tab values (1, '2021-23-03T00:00:00Z')", ImplicitCastException.class, true, "timestamp_ns");
    }

    @Test
    public void testInsertInvalidDateVarcharTimestampColumn() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-23-03T00:00:00Z` [VARCHAR -> TIMESTAMP]", "insert into tab values (1, '2021-23-03T00:00:00Z'::varchar)", ImplicitCastException.class, true, "timestamp");
    }

    @Test
    public void testInsertInvalidDateVarcharTimestampNSColumn() throws Exception {
        assertInsertTimestamp("inconvertible value: `2021-23-03T00:00:00Z` [VARCHAR -> TIMESTAMP_NS]", "insert into tab values (1, '2021-23-03T00:00:00Z'::varchar)", ImplicitCastException.class, true, "timestamp_ns");
    }

    @Test
    public void testInsertMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol) timestamp(ts);");
            execute("insert into trades VALUES (1262599200000000, 'USDJPY'), (3262599300000000, 'USDFJD');");

            String expected = "ts\tsym\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol) timestamp(ts);");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final String sql = "insert into trades VALUES (1262599200000000, $1), (3262599300000000, $2);";
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                try (InsertOperation insert = cq.popInsertOperation();
                     InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    bindVariableService.setStr(0, "USDJPY");
                    bindVariableService.setStr(1, "USDFJD");
                    method.execute(sqlExecutionContext);
                    method.commit();
                }
            }
            String expected = "ts\tsym\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\n";
            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsExtraParentheses() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (i INT, sym symbol)");
            execute("insert into trades VALUES ((1), 'USD'), ((2), (('FJD')));");

            String expected = "i\tsym\n" + "1\tUSD\n" + "2\tFJD\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertMultipleRowsFailInvalidSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (i int, sym symbol)");

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
            execute("create table trades (i int, sym symbol)");
            assertException("insert into trades VALUES (1, 'USDJPY'), ('USDFJD');", 50, "row value count does not match column count [expected=2, actual=1, tuple=2]");
        });
    }

    @Test
    public void testInsertMultipleRowsFailTypeConversion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (sym symbol)");
            assertException("insert into trades VALUES ('USDJPY'), (1), ('USDFJD');", 39, "inconvertible types: INT -> SYMBOL [from=1, to=sym]");
        });
    }

    @Test
    public void testInsertMultipleRowsMissingBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, i int) timestamp(ts);");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final String sql = "insert into t VALUES (1262599200000000, $1), (3262599300000000, $2);";
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());

                try (InsertOperation insert = cq.popInsertOperation();
                     InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                    bindVariableService.setInt(0, 1);
                    method.execute(sqlExecutionContext);
                    method.commit();
                }
            }
            String expected = "ts\ti\n" + "2010-01-04T10:00:00.000000Z\t1\n" + "2073-05-21T13:35:00.000000Z\tnull\n";
            assertReaderCheckWal(expected, "t");
        });
    }

    @Test
    public void testInsertMultipleRowsOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp) timestamp(ts) partition by day;");
            try {
                execute("insert into trades VALUES (1), (3), (2);");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot insert rows out of order to non-partitioned table.");
            }

            execute("create table trades_ns (ts timestamp_ns) timestamp(ts) partition by day;");
            try {
                execute("insert into trades VALUES (1), (3), (2);");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot insert rows out of order to non-partitioned table.");
            }
        });
    }

    @Test
    public void testInsertNoSelfReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades_aapl (ts TIMESTAMP, px INT, qty int, side STRING) TIMESTAMP(ts)");
            assertException("insert into trades_aapl (ts) values (ts)", 37, "Invalid column");
        });
    }

    @Test
    public void testInsertNoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            execute("insert into balances values (1, 'USD', 356.12)");
            String expected = "cust_id\tccy\tbalance\n" + "1\tUSD\t356.12\n";

            assertReaderCheckWal(expected, "balances");
        });
    }

    @Test
    public void testInsertNotEnoughFields() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException("insert into balances values (1, 'USD')", 37, "row value count does not match column count [expected=3, actual=2, tuple=1]");
        });
    }

    @Test
    public void testInsertNullStringSelectEmptyStringColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, NULL)");
            assertSql("id\n", "select id from tab where val = ''");
        });
    }

    @Test
    public void testInsertNullStringSelectNullStringColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, null)");
            assertSql("id\n1\n", "select id from tab where val = null");
        });
    }

    @Test
    public void testInsertNullVarcharSelectEmptyVarcharColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, NULL::varchar)");
            assertSql("id\n", "select id from tab where val = ''");
        });
    }

    @Test
    public void testInsertNullVarcharSelectNullVarcharColumnIndexed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (id int, val symbol index)");
            execute("insert into tab values (1, null::varchar)");
            assertSql("id\n1\n", "select id from tab where val = null");
        });
    }

    @Test
    public void testInsertSelectTwoWheres() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table result (r long)");

            assertExceptionNoLeakCheck(
                    "insert into result select * from long_sequence(1) where true where false;",
                    61,
                    "unexpected token [where]"
            );
        });
    }

    @Test
    public void testInsertSingleAndMultipleCharacterSymbols() throws Exception {
        final String expected = "sym\tid\tts\n" + "A\t315515118\t1970-01-03T00:00:00.000000Z\n" + "BB\t-727724771\t1970-01-03T00:06:00.000000Z\n" + "BB\t-948263339\t1970-01-03T00:12:00.000000Z\n" + "CC\t592859671\t1970-01-03T00:18:00.000000Z\n" + "CC\t-847531048\t1970-01-03T00:24:00.000000Z\n" + "A\t-2041844972\t1970-01-03T00:30:00.000000Z\n" + "CC\t-1575378703\t1970-01-03T00:36:00.000000Z\n" + "BB\t1545253512\t1970-01-03T00:42:00.000000Z\n" + "A\t1573662097\t1970-01-03T00:48:00.000000Z\n" + "BB\t339631474\t1970-01-03T00:54:00.000000Z\n";

        assertQuery("sym\tid\tts\n", "x", "create table x (\n" + "    sym symbol index,\n" + "    id int,\n" + "    ts timestamp\n" + ") timestamp(ts) partition by DAY", "ts", "insert into x select * from (select rnd_symbol('A', 'BB', 'CC', 'DDD') sym, \n" + "        rnd_int() id, \n" + "        timestamp_sequence(172800000000, 360000000) ts \n" + "    from long_sequence(10)) timestamp (ts)", expected, true, true, false);
    }

    @Test
    public void testInsertSingleCharacterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ww (id int, sym symbol)");
            execute("insert into ww VALUES ( 2, 'A')");
            String expected = "id\tsym\n" + "2\tA\n";

            assertReaderCheckWal(expected, "ww");
        });
    }

    @Test
    public void testInsertSymbolIntoVarcharCol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, sym symbol) timestamp(ts) partition by day;");
            execute("insert into src values (0, 'foo');");
            execute("insert into src values (20000, null);");
            execute("insert into src values (30000, 'bar');");

            execute("create table dest (ts timestamp, vch varchar) timestamp(ts) partition by day;");
            drainWalQueue();

            execute("insert into dest select ts, sym from src;");

            String expected = "ts\tvch\n" + "1970-01-01T00:00:00.000000Z\tfoo\n" + "1970-01-01T00:00:00.020000Z\t\n" + "1970-01-01T00:00:00.030000Z\tbar\n";
            assertQueryCheckWal(expected);

            // check symbol null was inserted as a null varch and not as an empty varchar
            assertQuery("ts\tvch\n" + "1970-01-01T00:00:00.020000Z\t\n", "select * from dest where vch is null", "ts", true, false);
        });
    }

    @Test
    public void testInsertSymbolNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table symbols (sym symbol, isNewSymbol BOOLEAN)");
            execute("insert into symbols (sym, isNewSymbol) VALUES ('USDJPY', false);");
            execute("insert into symbols (sym, isNewSymbol) VALUES ('USDFJD', true);");

            String expected = "sym\tisNewSymbol\n" + "USDJPY\tfalse\n" + "USDFJD\ttrue\n";

            assertReaderCheckWal(expected, "symbols");
        });
    }

    @Test
    public void testInsertSymbolNonPartitioned_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table symbols (sym symbol, isNewSymbol BOOLEAN)");
            execute("insert into symbols (sym, isNewSymbol) VALUES ('USDJPY'::varchar, false);");
            execute("insert into symbols (sym, isNewSymbol) VALUES ('USDFJD'::varchar, true);");

            String expected = "sym\tisNewSymbol\n" + "USDJPY\tfalse\n" + "USDFJD\ttrue\n";

            assertReaderCheckWal(expected, "symbols");
        });
    }

    @Test
    public void testInsertSymbolPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            execute("insert into trades VALUES ( 1262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" + "2010-01-04T10:01:40.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");

            String expected1 = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n";

            assertReaderCheckWal(expected1, "trades");

            try (TableWriter w = getWriter("trades")) {
                w.truncate();
            }

            execute("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected2 = "ts\tsym\tbid\task\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected2, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedAfterTruncate_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY'::varchar, 1, 2);");

            String expected1 = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n";

            assertReaderCheckWal(expected1, "trades");

            try (TableWriter w = getWriter("trades")) {
                w.truncate();
            }

            execute("insert into trades VALUES ( 3262599300000000, 'USDFJD'::varchar, 2, 4);");

            String expected2 = "ts\tsym\tbid\task\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected2, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedFarApart() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY', 1, 2);");
            execute("insert into trades VALUES ( 3262599300000000, 'USDFJD', 2, 4);");

            String expected = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitionedFarApart_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY'::varchar, 1, 2);");
            execute("insert into trades VALUES ( 3262599300000000, 'USDFJD'::varchar, 2, 4);");

            String expected = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" + "2073-05-21T13:35:00.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertSymbolPartitioned_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by DAY;");
            execute("insert into trades VALUES ( 1262599200000000, 'USDJPY'::varchar, 1, 2);");
            execute("insert into trades VALUES ( 1262599300000000, 'USDFJD'::varchar, 2, 4);");

            String expected = "ts\tsym\tbid\task\n" + "2010-01-04T10:00:00.000000Z\tUSDJPY\t1.0\t2.0\n" + "2010-01-04T10:01:40.000000Z\tUSDFJD\t2.0\t4.0\n";

            assertReaderCheckWal(expected, "trades");
        });
    }

    @Test
    public void testInsertTimestampNSOverflowException() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (t1 timestamp_ns) timestamp(t1) partition by day;");
            assertException("insert into tab values ('2300-01-03T00:00:00.000001123')", 24, "timestamp_ns before 1970-01-01 and beyond 2261-12-31 23:59:59.999999999 is not allowed");
        });
    }

    @Test
    public void testInsertTimestampNSOverflowException3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (t1 timestamp, t2 timestamp) timestamp(t2) partition by DAY;");
            execute("insert into tab values ('2321-01-03T00:00:00.123', '2021-01-03T00:00:00.456');");
            assertReaderCheckWal("t1\tt2\n" +
                    "2321-01-03T00:00:00.123000Z\t2021-01-03T00:00:00.456000Z\n", "tab");
            execute("create table tab1 (t1 timestamp_ns, t2 timestamp_ns) timestamp(t2) partition by DAY;");
            assertException("insert into tab1 select t1, t2 from tab", 0, "inconvertible value: 11076652800123000 [TIMESTAMP -> TIMESTAMP_NS]");
        });
    }

    @Test
    public void testInsertTimestampNSWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (timestamp timestamp_ns) timestamp(timestamp) partition by day;");
            execute("insert into t values (timestamp with time zone '2020-12-31 15:15:51.663+00:00')");

            String expected1 = "timestamp\n" + "2020-12-31T15:15:51.663000000Z\n";

            assertReaderCheckWal(expected1, "t");

            execute("insert into t values (cast('2021-12-31 15:15:51.663+00:00' as timestamp with time zone))");

            String expected2 = expected1 + "2021-12-31T15:15:51.663000000Z\n";

            assertReaderCheckWal(expected2, "t");

            assertException("insert into t values  (timestamp with time zone)", 47, "String literal expected after 'timestamp with time zone'");
        });
    }

    @Test
    public void testInsertTimestampNSWithTimeZone_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (timestamp timestamp_ns) timestamp(timestamp) partition by day;");
            execute("insert into t values (timestamp with time zone '2020-12-31 15:15:51.663+00:00')");

            String expected1 = "timestamp\n" + "2020-12-31T15:15:51.663000000Z\n";

            assertReaderCheckWal(expected1, "t");

            execute("insert into t values (cast('2021-12-31 15:15:51.663+00:00'::varchar as timestamp with time zone))");

            String expected2 = expected1 + "2021-12-31T15:15:51.663000000Z\n";

            assertReaderCheckWal(expected2, "t");

            assertException("insert into t values  (timestamp with time zone)", 47, "String literal expected after 'timestamp with time zone'");
        });
    }

    @Test
    public void testInsertTimestampNS_timestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (t1 timestamp, t2 timestamp) timestamp(t2) partition by DAY;");
            execute("insert into tab values ('2021-01-03T00:00:00.123', '2021-01-03T00:00:00.456');");
            assertReaderCheckWal("t1\tt2\n" +
                    "2021-01-03T00:00:00.123000Z\t2021-01-03T00:00:00.456000Z\n", "tab");
            execute("create table tab1 (t1 timestamp_ns, t2 timestamp_ns) timestamp(t2) partition by DAY;");
            execute("insert into tab1 select t1, t2 from tab;");
            assertReaderCheckWal("t1\tt2\n" +
                    "2021-01-03T00:00:00.123000000Z\t2021-01-03T00:00:00.456000000Z\n", "tab1");
        });
    }

    @Test
    public void testInsertTimestampWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (timestamp timestamp) timestamp(timestamp);");
            execute("insert into t values (timestamp with time zone '2020-12-31 15:15:51.663+00:00')");

            String expected1 = "timestamp\n" + "2020-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected1, "t");

            execute("insert into t values (cast('2021-12-31 15:15:51.663+00:00' as timestamp with time zone))");

            String expected2 = expected1 + "2021-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected2, "t");

            assertException("insert into t values  (timestamp with time zone)", 47, "String literal expected after 'timestamp with time zone'");
        });
    }

    @Test
    public void testInsertTimestampWithTimeZone_varchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (timestamp timestamp) timestamp(timestamp);");

            // We cannot cast '2020-12-31 15:15:51.663+00:00'::varchar,
            // because it will act as (timestamp with time zone '2020-12-31 15:15:51.663+00:00')::varchar
            // This creates a varchar constant whose value is the string representation of the timestamp in microseconds
            // since the epoch. And such string constants cannot be inserted as timestamps. Only actual string/varchar timestamps
            // can be inserted into a timestamp column.
            // If you cast '2020-12-31 15:15:51.663+00:00'::string then it fails too.
            // thus Varchar behaves the same as String in this case.
            execute("insert into t values (timestamp with time zone '2020-12-31 15:15:51.663+00:00')");

            String expected1 = "timestamp\n" + "2020-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected1, "t");

            execute("insert into t values (cast('2021-12-31 15:15:51.663+00:00'::varchar as timestamp with time zone))");

            String expected2 = expected1 + "2021-12-31T15:15:51.663000Z\n";

            assertReaderCheckWal(expected2, "t");

            assertException("insert into t values  (timestamp with time zone)", 47, "String literal expected after 'timestamp with time zone'");
        });
    }

    @Test
    public void testInsertTimestamp_timestampNS() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (t1 timestamp_ns, t2 timestamp_ns) timestamp(t2) partition by day;");
            execute("insert into tab values ('2021-01-03T00:00:00.000001123', '2021-01-03T00:00:00.000004456');");
            assertReaderCheckWal("t1\tt2\n" +
                    "2021-01-03T00:00:00.000001123Z\t2021-01-03T00:00:00.000004456Z\n", "tab");
            execute("create table tab1 (t1 timestamp, t2 timestamp) timestamp(t2) partition by day;");
            execute("insert into tab1 select t1, t2 from tab;");
            assertReaderCheckWal("t1\tt2\n" +
                    "2021-01-03T00:00:00.000001Z\t2021-01-03T00:00:00.000004Z\n", "tab1");
        });
    }

    @Test
    public void testInsertUUIDIntoVarcharCol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, u uuid) timestamp(ts) partition by day;");
            execute("insert into src values (0, '11111111-1111-1111-1111-111111111111');");
            execute("insert into src values (20000, null);");
            execute("insert into src values (30000, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');");

            execute("create table dest (ts timestamp, vch varchar) timestamp(ts) partition by day;");
            drainWalQueue();

            execute("insert into dest select ts, u from src;");

            String expected = "ts\tvch\n" + "1970-01-01T00:00:00.000000Z\t11111111-1111-1111-1111-111111111111\n" + "1970-01-01T00:00:00.020000Z\t\n" + "1970-01-01T00:00:00.030000Z\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n";
            assertQueryCheckWal(expected);

            // check symbol null was inserted as a null varch and not as an empty varchar
            assertQuery("ts\tvch\n" + "1970-01-01T00:00:00.020000Z\t\n", "select * from dest where vch is null", "ts", true, false);
        });
    }

    @Test
    public void testInsertValueCannotReferenceTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table balances(cust_id int, ccy symbol, balance double)");
            assertException("insert into balances values (1, ccy, 356.12)", 32, "Invalid column: ccy");
        });
    }

    @Test
    public void testInsertValuesAsLambda() throws Exception {
        assertException("insert into names values(select rnd_str('Tom', 'Anna', 'John', 'Tim', 'Kim', 'Jim'), rnd_str('Smith', 'Mason', 'Johnson', 'Thompson') from long_sequence(8))", 25, "query is not allowed here");
    }

    @Test
    public void testInsertVarcharToDifferentTypeCol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, vch varchar, vch2 varchar, vch3 varchar) timestamp(ts) partition by day;");
            execute("insert into src values (0, '1', '11111111-1111-1111-1111-111111111111', '2022-11-20T10:30:55.123Z');");
            execute("insert into src values (20000, null, null, null);");
            execute("insert into src values (30000, '2', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '-900');");

            execute("create table dest (ts timestamp, s string, l long, sh short, i int, b byte, c char, f float, d double, u uuid, dt date, ts2 timestamp, sym symbol) timestamp(ts) partition by day;");
            drainWalQueue();

            execute("insert into dest select ts, vch, vch, vch, vch, vch, vch, vch, vch, vch2, vch3, vch3, vch from src;");

            String expected = "ts\ts\tl\tsh\ti\tb\tc\tf\td\tu\tdt\tts2\tsym\n" +
                    "1970-01-01T00:00:00.000000Z\t1\t1\t1\t1\t1\t1\t1.0\t1.0\t11111111-1111-1111-1111-111111111111\t2022-11-20T10:30:55.123Z\t2022-11-20T10:30:55.123000Z\t1\n" +
                    "1970-01-01T00:00:00.020000Z\t\tnull\t0\tnull\t0\t\tnull\tnull\t\t\t\t\n" +
                    "1970-01-01T00:00:00.030000Z\t2\t2\t2\t2\t2\t2\t2.0\t2.0\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1969-12-31T23:59:59.100Z\t1969-12-31T23:59:59.999100Z\t2\n";
            assertQueryCheckWal(expected);

            // check varchar null was inserted as a null string and not as an empty string
            assertQuery("ts\ts\tl\tsh\ti\tb\tc\tf\td\tu\tdt\tts2\tsym\n" + "1970-01-01T00:00:00.020000Z\t\tnull\t0\tnull\t0\t\tnull\tnull\t\t\t\t\n", "select * from dest where s is null", "ts", true, false);
        });
    }

    @Test
    public void testInsertWithLessColumnsThanExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select x ac  from long_sequence(10)", 12, "select clause must provide timestamp column");
        });
    }

    @Test
    public void testInsertWithWrongDesignatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select * from (select  timestamp_sequence(0, x) ts, x ac from long_sequence(10)) timestamp(ts)", 12, "designated timestamp of existing table");
        });
    }

    @Test
    public void testInsertWithoutDesignatedTimestamp() throws Exception {
        final String expected = "seq\tts\n" + "1\t1970-01-01T00:00:00.000000Z\n" + "2\t1970-01-01T00:00:00.000001Z\n" + "3\t1970-01-01T00:00:00.000003Z\n" + "4\t1970-01-01T00:00:00.000006Z\n" + "5\t1970-01-01T00:00:00.000010Z\n" + "6\t1970-01-01T00:00:00.000015Z\n" + "7\t1970-01-01T00:00:00.000021Z\n" + "8\t1970-01-01T00:00:00.000028Z\n" + "9\t1970-01-01T00:00:00.000036Z\n" + "10\t1970-01-01T00:00:00.000045Z\n";

        if (walEnabled) {
            drainWalQueue();
        }
        assertQuery("seq\tts\n", "tab", "create table tab(seq long, ts timestamp) timestamp(ts);", "ts", "insert into tab select x ac, timestamp_sequence(0, x) ts from long_sequence(10)", expected, true, true, false);
    }

    @Test
    public void testInsertWithoutDesignatedTimestampAndTypeDoesNotMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(seq long, ts timestamp) timestamp(ts);");
            assertException("insert into tab select x ac, rnd_int() id from long_sequence(10)", 12, "expected timestamp column");
        });
    }

    @Test
    public void testInsertWrongTypeConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (a timestamp)", sqlExecutionContext);
            assertException("insert into test values ('foobar')", 0, "inconvertible value: `foobar` [STRING -> TIMESTAMP]");
            assertException("insert into test values ('foobar'::varchar)", 0, "inconvertible value: `foobar` [VARCHAR -> TIMESTAMP]");
        });
    }

    @Test
    public void testVarcharMixedAscii() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (a varchar, b varchar, ts timestamp) timestamp(ts) partition by day");
            execute("insert into test values ('a', 'b', 0)");
            execute("insert into test values ('2HEz*Dq', 'cVԕΖVq', 0)");
            execute("insert into test values ('Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:', 'C>Wy;', 0)");
            execute("insert into test values ('6tuU}+8mV', null, 0)");
            execute("insert into test values ('te', '葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*', 0)");
            execute("insert into test values ('+٘ˣ聉|凜-،W.ƣ', '1);86rU)', 0)");
            execute("insert into test values ('{[pG5d^fG>v [6', 'Ȕ\uDB75\uDF17ߚ`ŷ֪', 0)");

            drainWalQueue();

            assertSql(
                    "a\tb\tts\n" +
                            "a\tb\t1970-01-01T00:00:00.000000Z\n" +
                            "2HEz*Dq\tcVԕΖVq\t1970-01-01T00:00:00.000000Z\n" +
                            "Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:\tC>Wy;\t1970-01-01T00:00:00.000000Z\n" +
                            "6tuU}+8mV\t\t1970-01-01T00:00:00.000000Z\n" +
                            "te\t葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*\t1970-01-01T00:00:00.000000Z\n" +
                            "+٘ˣ聉|凜-،W.ƣ\t1);86rU)\t1970-01-01T00:00:00.000000Z\n" +
                            "{[pG5d^fG>v [6\tȔ\uDB75\uDF17ߚ`ŷ֪\t1970-01-01T00:00:00.000000Z\n",
                    "test"
            );

            execute("create table y as (select * from test) timestamp(ts) partition by day");

            drainWalQueue();

            assertSql(
                    "a\tb\tts\n" +
                            "a\tb\t1970-01-01T00:00:00.000000Z\n" +
                            "2HEz*Dq\tcVԕΖVq\t1970-01-01T00:00:00.000000Z\n" +
                            "Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:\tC>Wy;\t1970-01-01T00:00:00.000000Z\n" +
                            "6tuU}+8mV\t\t1970-01-01T00:00:00.000000Z\n" +
                            "te\t葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*\t1970-01-01T00:00:00.000000Z\n" +
                            "+٘ˣ聉|凜-،W.ƣ\t1);86rU)\t1970-01-01T00:00:00.000000Z\n" +
                            "{[pG5d^fG>v [6\tȔ\uDB75\uDF17ߚ`ŷ֪\t1970-01-01T00:00:00.000000Z\n",
                    "y"
            );

            // sort rows without using rowid
            assertSql(
                    "a\tb\tts\n" +
                            "+٘ˣ聉|凜-،W.ƣ\t1);86rU)\t1970-01-01T00:00:00.000000Z\n" +
                            "2HEz*Dq\tcVԕΖVq\t1970-01-01T00:00:00.000000Z\n" +
                            "6tuU}+8mV\t\t1970-01-01T00:00:00.000000Z\n" +
                            "a\tb\t1970-01-01T00:00:00.000000Z\n" +
                            "te\t葈ﾫ!\uD8F3\uDD99Ҧ\uDB8D\uDFC8R\uD988\uDCEEOa*\t1970-01-01T00:00:00.000000Z\n" +
                            "{[pG5d^fG>v [6\tȔ\uDB75\uDF17ߚ`ŷ֪\t1970-01-01T00:00:00.000000Z\n" +
                            "Ɨ\uDA83\uDD95\uD9ED\uDF4C눻D\uDBA8\uDFB6qٽUY⚂խ:\tC>Wy;\t1970-01-01T00:00:00.000000Z\n",
                    "'*!*y' order by a, b"
            );
        });
    }

    private void assertInsertTimestamp(String expected, String ddl2, Class<?> exceptionType, boolean commitInsert, String timestampType) throws Exception {
        assertMemoryLeak(() -> {
            if (commitInsert) {
                execute("create table tab(seq long, ts #TIMESTAMP_TYPE) timestamp(ts) partition by day".replace("#TIMESTAMP_TYPE", timestampType));
                try {
                    execute(ddl2);
                    if (exceptionType != null) {
                        Assert.fail("SqlException expected");
                    }
                    if (walEnabled) {
                        drainWalQueue();
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
                execute("create table tab(seq long, ts #TIMESTAMP_TYPE) timestamp(ts) partition by day".replace("#TIMESTAMP_TYPE", timestampType));
                try {
                    execute(ddl2);
                    if (exceptionType != null) {
                        Assert.fail("SqlException expected");
                    }
                    if (walEnabled) {
                        drainWalQueue();
                    }
                    assertSql(expected, "tab");
                } catch (Throwable e) {
                    if (exceptionType == null) throw e;
                    Assert.assertSame(exceptionType, e.getClass());
                    TestUtils.assertContains(e.getMessage(), expected);
                }
            }

            execute("drop table tab");

            if (commitInsert) {
                execute("create table tab(seq long, ts #TIMESTAMP_TYPE)".replace("#TIMESTAMP_TYPE", timestampType));
                try {
                    execute(ddl2);
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
                execute("create table tab(seq long, ts #TIMESTAMP_TYPE)".replace("#TIMESTAMP_TYPE", timestampType));
                try {
                    execute(ddl2, sqlExecutionContext);
                    if (exceptionType != null) {
                        Assert.fail("SqlException expected");
                    }
                    assertSql(expected, "tab");
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    if (exceptionType == null) throw e;
                    Assert.assertSame(exceptionType, e.getClass());
                    TestUtils.assertContains(e.getMessage(), expected);
                }
            }
        });
    }

    private void assertQueryCheckWal(String expected) throws Exception {
        if (walEnabled) {
            drainWalQueue();
        }

        assertQueryNoLeakCheck(expected, "dest", "ts", true, true);
    }

    private void testBindVariableInsert(int partitionBy, TimestampFunction timestampFunction, boolean initBindVariables, boolean columnSet, int timestampType) throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithNewTypes(engine, partitionBy, timestampType);
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
                bindVariableService.setStr(13, "a07934b2-a15d-48e0-9509-88dbaca49734");
                bindVariableService.setStr(14, "192.0.0.1");
                bindVariableService.setStr(15, "foo bar");
                bindVariableService.setTimestamp(16, timestampFunction.getTimestamp());
            }

            final String sql;
            if (columnSet) {
                sql = "insert into all2 (" + "int, " + "short, " + "byte, " + "double, " + "float, " + "long, " + "str, " + "sym, " + "bool, " + "bin, " + "date, " + "long256, " + "chr, " + "uuid, " + "ipv4, " + "varchar, " + "timestamp" + ") values (" + "$1, " + "$2, " + "$3, " + "$4, " + "$5, " + "$6, " + "$7, " + "$8, " + "$9, " + "$10, " + "$11, " + "$12, " + "$13, " + "$14, " + "$15, " + "$16, " + "$17)";
            } else {
                sql = "insert into all2 values (" + "$1, " + "$2, " + "$3, " + "$4, " + "$5, " + "$6, " + "$7, " + "$8, " + "$9, " + "$10, " + "$11, " + "$12, " + "$13, " + "$14, " + "$15, " + "$16, " + "$17)";
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                try (InsertOperation insert = cq.popInsertOperation();
                     InsertMethod method = insert.createMethod(sqlExecutionContext)) {
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
                        sink.clear();
                        Numbers.appendUuid(rnd.nextLong(), rnd.nextLong(), sink);
                        bindVariableService.setStr(13, sink);
                        sink.clear();
                        Numbers.intToIPv4Sink(sink, rnd.nextInt());
                        bindVariableService.setStr(14, sink);
                        bindVariableService.setStr(15, rnd.nextChars(16));
                        bindVariableService.setTimestamp(16, timestampFunction.getTimestamp());
                        method.execute(sqlExecutionContext);
                    }
                    method.commit();
                }
            }

            rnd.reset();
            try (
                    TableReader reader = getReader("all2");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                    Assert.assertEquals(rnd.nextShort(), record.getShort(1));
                    Assert.assertEquals(rnd.nextByte(), record.getByte(2));
                    Assert.assertEquals(rnd.nextDouble(), record.getDouble(3), 0.0001);
                    Assert.assertEquals(rnd.nextFloat(), record.getFloat(4), 0.000001);
                    Assert.assertEquals(rnd.nextLong(), record.getLong(5));
                    TestUtils.assertEquals(rnd.nextChars(6), record.getStrA(6));
                    TestUtils.assertEquals(rnd.nextChars(1), record.getSymA(7));
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
                    Assert.assertEquals(rnd.nextLong(), record.getLong128Lo(13));
                    Assert.assertEquals(rnd.nextLong(), record.getLong128Hi(13));
                    Assert.assertEquals(rnd.nextInt(), record.getInt(14));
                    TestUtils.assertEquals(rnd.nextChars(16), record.getVarcharA(15));
                }
            }
        });
    }

    private void testInsertAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, v long) timestamp(ts) partition by day;");
            execute("insert into src values (0, 0);");
            execute("insert into src values (10000, 1);");
            execute("insert into src values (20000, 2);");
            execute("insert into src values (30000, 3);");
            execute("insert into src values (40000, 4);");

            execute("create table dest (ts timestamp, v long) timestamp(ts) partition by day;");
            drainWalQueue();

            execute("insert into dest select * from src where v % 2 = 0 " + orderByClause + ";");

            String expected = "ts\tv\n" + "1970-01-01T00:00:00.000000Z\t0\n" + "1970-01-01T00:00:00.020000Z\t2\n" + "1970-01-01T00:00:00.040000Z\t4\n";

            assertQueryCheckWal(expected);
        });
    }

    private void testInsertAsSelectWithOrderByWithNanoTS(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp_ns, v long) timestamp(ts) partition by day;");
            execute("insert into src values (0, 0);");
            execute("insert into src values (10000000, 1);");
            execute("insert into src values (20000000, 2);");
            execute("insert into src values (30000000, 3);");
            execute("insert into src values (40000000, 4);");

            execute("create table dest (ts timestamp_ns, v long) timestamp(ts) partition by day;");
            drainWalQueue();

            execute("insert into dest select * from src where v % 2 = 0 " + orderByClause + ";");

            String expected = "ts\tv\n" + "1970-01-01T00:00:00.000000000Z\t0\n" + "1970-01-01T00:00:00.020000000Z\t2\n" + "1970-01-01T00:00:00.040000000Z\t4\n";

            assertQueryCheckWal(expected);
        });
    }

    @FunctionalInterface
    private interface TimestampFunction {
        long getTimestamp();
    }
}
