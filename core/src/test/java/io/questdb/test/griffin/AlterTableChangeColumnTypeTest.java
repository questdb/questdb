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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AlterTableChangeColumnTypeTest extends AbstractCairoTest {
    private final boolean partitioned;
    private final boolean walEnabled;

    public AlterTableChangeColumnTypeTest(Mode walMode) {
        this.walEnabled = (walMode == Mode.WITH_WAL);
        this.partitioned = (walMode != Mode.NON_PARTITIONED);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Mode.WITH_WAL}, {Mode.NO_WAL}, {Mode.NON_PARTITIONED}
        });
    }

    @Test
    public void testColumnDoesNotExist() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column non_existing", 27, "column 'non_existing' does not exists in table 'x'");
    }

    @Test
    public void testConversionInvalidToken() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column i type long abc", 39, "unexpected token [abc] while trying to change column type");
    }

    @Test
    public void testChangeIndexedSymbolToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            ddl("create table y as (select ik from x)", sqlExecutionContext);
            ddl("alter table x alter column ik type varchar", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select ik from y",
                    "select ik from x"
            );

            insert("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");

            insert("insert into y(ik) values('abc')", sqlExecutionContext);
            assertSqlCursorsConvertedStrings(
                    "select 'abc' as ik",
                    "select ik from x where ik = 'abc'"

            );
        });
    }

    @Test
    public void testChangeMultipleTimesReleaseWriters() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            engine.releaseInactive();
            ddl("alter table x alter column ik type varchar", sqlExecutionContext);
            ddl("alter table x alter column ik type string", sqlExecutionContext);
            ddl("alter table x alter column ik type symbol index", sqlExecutionContext);
            ddl("alter table x alter column ik type string", sqlExecutionContext);
            drainWalQueue();

            insert("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");
        });
    }

    @Test
    public void testChangeStringToIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            ddl("create table y as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type symbol index", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from y",
                    "select c from x"
            );

            insert("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            insert("insert into y(c) values('abc')", sqlExecutionContext);
            assertSqlCursorsConvertedStrings(
                    "select c from y where c = 'abc'",
                    "select c from x where c = 'abc'"

            );
        });
    }

    @Test
    public void testChangeStringToInt() throws Exception {
        assertFailure("alter table x alter column c type int", 34, "incompatible column type change [existing=STRING, new=INT]");
    }

    @Test
    public void testChangeStringToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            ddl("create table y as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type symbol", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from y",
                    "select c from x"
            );

            insert("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            ddl("create table z as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type string", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from z",
                    "select c from x"

            );
        });
    }

    @Test
    public void testChangeStringToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            ddl("create table y as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type varchar", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from x",
                    "select c from y"
            );

            insert("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            ddl("create table z as (select c from x)", sqlExecutionContext);
            ddl("alter table x alter column c type string", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from z",
                    "select c from x"
            );
        });
    }

    @Test
    public void testChangeSymbolToVarcharReleaseWriters() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            engine.releaseInactive();
            ddl("alter table x alter column ik type varchar", sqlExecutionContext);
            drainWalQueue();

            insert("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");
        });
    }

    @Test
    public void testChangeTypePreservesColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select * from x)", sqlExecutionContext);
            ddl("alter table x alter column c type symbol", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select * from y limit 10",
                    "select * from x limit 10"
            );
        });
    }

    @Test
    public void testChangeVarcharStringSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            ddl("create table y as (select v from x)", sqlExecutionContext);

            Rnd rnd = new Rnd();
            for (int i = 0; i < 10; i++) {
                String type;
                switch (rnd.nextPositiveInt() % 3) {
                    case 0:
                        type = "string";
                        break;
                    case 1:
                        type = "varchar";
                        break;
                    default:
                        type = "symbol";
                        break;
                }

                ddl("alter table x alter column v type " + type, sqlExecutionContext);

                assertSqlCursorsConvertedStrings(
                        "select v from y",
                        "select v from x"
                );
            }
        });
    }

    @Test
    public void testChangeVarcharToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            ddl("create table y as (select v from x)", sqlExecutionContext);
            ddl("alter table x alter column v type symbol", sqlExecutionContext);

            drainWalQueue();
            assertSqlCursorsConvertedStrings(
                    "select v from x",
                    "select v from y"
            );

            insert("insert into x(v, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("v\nabc\n", "select v from x limit -1");

            ddl("create table z as (select v from x)", sqlExecutionContext);
            ddl("alter table x alter column v type varchar", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select v from z",
                    "select v from x"
            );
        });
    }

    @Test
    public void testFixedSizeColumnEquivalentToCast() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            final String[] types = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP"};
            final char[] col_names = {'l', 'f', 'i', 'j', 'e', 'd', 'k'};
            char srcColName;
            String srcType;
            String dstType;

            for (int i = 0, n = types.length; i < n; i++) {
                for (int j = 0, m = types.length; j < m; j++) {
                    // skip unsupported noop conversion
                    if (i == j) {
                        continue;
                    }

                    srcType = types[i];
                    srcColName = col_names[i];
                    dstType = types[j];

                    LOG.info().$("checking `" + srcType + "` to `" + dstType + "` conversion");

                    ddl("create table y ( converted " + srcType + ", casted " + dstType + ", original " + srcType + ")", sqlExecutionContext);
                    insert("insert into y select " + srcColName + " as converted, " + srcColName + "::" + dstType + " as casted, " + srcColName + " as original from x", sqlExecutionContext);
                    drainWalQueue();
                    ddl("alter table y alter column converted type " + dstType, sqlExecutionContext);
                    drainWalQueue();

                    try {
                        assertQuery(
                                "count_distinct\tfirst\n1\ttrue\n",
                                "select count_distinct('equivalent'), first(equivalent) from (select (converted = casted) as equivalent from y)",
                                null,
                                false,
                                true
                        );
                        assertQuery("column\ttype\n" +
                                "converted\t" + dstType + "\n" +
                                "casted\t" + dstType + "\n" +
                                "original\t" + srcType + "\n", "select \"column\", type from table_columns('y')", null, false, false);
                    } catch (AssertionError e) {
                        // if the column wasn't converted
                        if (e.getMessage().contains("column")) {
                            throw e;
                        } else {
                            // dump the difference in data
                            assertSql("\nFailed equivalent conversion from `" + srcType + "` to `" + dstType + "`.\n", "select converted, casted, original from y");
                        }
                    } finally {
                        drop("drop table y", sqlExecutionContext);
                        drainWalQueue();
                    }
                }

            }
        });
    }

    @Test
    public void testFixedSizeColumnLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            ddl("create table y ( converted long, casted int, original long );", sqlExecutionContext);
            insert("insert into y (converted, casted, original) values (9999999999999, 9999999999999::int, 9999999999999)", sqlExecutionContext);
            drainWalQueue();
            ddl("alter table y alter column converted type int", sqlExecutionContext);
            drainWalQueue();

            assertQuery("converted\tcasted\toriginal\n" +
                    "1316134911\t1316134911\t9999999999999\n", "select * from y", null, true, true);

        });
    }

    @Test
    public void testFixedSizeColumnNullableBehaviour() throws Exception {
        assertMemoryLeak(() -> {
            drainWalQueue();
            final String[] types = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP"};
            String srcType;
            String dstType;

            for (int i = 0, n = types.length; i < n; i++) {
                for (int j = 0, m = types.length; j < m; j++) {
                    // skip unsupported noop conversion
                    if (i == j) {
                        continue;
                    }

                    srcType = types[i];
                    dstType = types[j];

                    LOG.info().$("checking `" + srcType + "` to `" + dstType + "` conversion");

                    ddl("create table y ( converted " + srcType + ", casted " + dstType + ", original " + srcType + ")", sqlExecutionContext);
                    insert("insert into y (converted, casted, original) values (null, cast(null as " + srcType + ")::" + dstType + ", null)", sqlExecutionContext);
                    drainWalQueue();
                    ddl("alter table y alter column converted type " + dstType, sqlExecutionContext);
                    drainWalQueue();

                    try {
                        assertQuery(
                                "count_distinct\tfirst\n1\ttrue\n",
                                "select count_distinct('equivalent'), first(equivalent) from (select (converted = casted) as equivalent from y)",
                                null,
                                false,
                                true
                        );
                        assertQuery("column\ttype\n" +
                                "converted\t" + dstType + "\n" +
                                "casted\t" + dstType + "\n" +
                                "original\t" + srcType + "\n", "select \"column\", type from table_columns('y')", null, false, false);
                    } catch (AssertionError e) {
                        // if the column wasn't converted
                        if (e.getMessage().contains("column")) {
                            throw e;
                        } else {
                            // dump the difference in data
                            assertSql("\nFailed equivalent conversion from `" + srcType + "` to `" + dstType + "`.\n", "select converted, casted, original from y");
                        }
                    } finally {
                        drop("drop table y", sqlExecutionContext);
                        drainWalQueue();
                    }
                }

            }
        });
    }

    @Test
    public void testNewTypeInvalid() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column c type abracadabra", 34, "invalid type");
    }

    @Test
    public void testNewTypeMissing() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column c type", 33, "column type expected");
    }

    @Test
    public void testTimestampConversionInvalid() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column timestamp type long", 42, "cannot change type of designated timestamp column");
    }

    private void createX() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str(5,1024,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_varchar(5,64,2) v" +
                        " from long_sequence(1000)" +
                        "), index(ik) timestamp (timestamp) " +
                        (partitioned ? "PARTITION BY DAY " : "PARTITION BY NONE ") +
                        (walEnabled ? "WAL" : (partitioned ? "BYPASS WAL" : ""))
        );
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataStringToSymbol() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.STRING, " rnd_str(5,1024,2) c,", "symbol"));
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataStringToVarchar() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.STRING, " rnd_str(5,1024,2) c,", "varchar"));
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataSymbolToString() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.SYMBOL, " rnd_symbol('a', 'b', 'c', null) c,", "string"));
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataSymbolToVarchar() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.SYMBOL, " rnd_symbol('a', 'b', 'c', null) c,", "varchar"));
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataVarcharToString() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.VARCHAR, " rnd_varchar(5,1024,2) c,", "string"));
    }

    @Test
    public void testWalWriterConvertsRowOnUncommittedDataVarcharToSymbol() throws Exception {
        assertMemoryLeak(() -> testWalRollUncommittedConversion(ColumnType.VARCHAR, " rnd_varchar(5,1024,2) c,", "symbol"));
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                ddl(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void assumeWal() {
        Assume.assumeTrue("Test disabled during WAL run.", walEnabled);
    }

    public enum Mode {
        WITH_WAL, NO_WAL, NON_PARTITIONED
    }

    private void testWalRollUncommittedConversion(int columnType, String columnCreateSql, String convertToTypeSql) throws SqlException, NumericException {
        assumeWal();
        ddl(
                "create table x as (" +
                        "select" +
                        columnCreateSql +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp," +
                        " from long_sequence(1000)" +
                        ") timestamp (timestamp) PARTITION BY HOUR WAL;"
        );

        try (WalWriter walWriter = getWalWriter("x")) {
            TableWriter.Row row = walWriter.newRow(IntervalUtils.parseFloorPartialTimestamp("2024-02-04"));
            switch (columnType) {
                case ColumnType.STRING:
                    row.putStr(0, "abc");
                    break;
                case ColumnType.SYMBOL:
                    row.putSym(0, "abc");
                    break;
                case ColumnType.VARCHAR:
                    row.putVarchar(0, new Utf8String("abc"));
                    break;
            }
            row.append();
            ddl("alter table x alter column c type " + convertToTypeSql, sqlExecutionContext);

            walWriter.commit();
        }

        drainWalQueue();

        assertSql("c\nabc\n", "select c from x limit -1");
    }

    protected static void assertSqlCursorsConvertedStrings(CharSequence expectedSql, CharSequence actualSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    actualSql,
                    LOG,
                    true
            );
        }
    }
}
