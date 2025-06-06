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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testCannotConvertToSameType() throws Exception {
        assumeNonWal();
        assertFailure("alter table x alter column d type double", 34, "column 'd' type is already 'DOUBLE'");
    }

    @Test
    public void testChangeDoubleToFloat() throws Exception {
        assertMemoryLeak(() -> {
            assumeWal();
            execute("create table x (ts timestamp, col double) timestamp(ts) partition by day wal", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:00.000000Z', 0.0)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:01.000000Z', 0.1)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', 3.1)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', -9223372036854775808.0)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', -3.4e38)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', 3.4e38)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', 1.80e300)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', -1.80e300)", sqlExecutionContext);
            drainWalQueue();

            execute("alter table x alter column col type float", sqlExecutionContext);
            drainWalQueue();

            assertSql("ts\tcol\n" +
                            "2024-05-14T16:00:00.000000Z\t0.0\n" +
                            "2024-05-14T16:00:01.000000Z\t0.1\n" +
                            "2024-05-14T16:00:02.000000Z\t3.1\n" +
                            "2024-05-14T16:00:02.000000Z\t-9.223372E18\n" +
                            "2024-05-14T16:00:02.000000Z\t-3.4E38\n" +
                            "2024-05-14T16:00:02.000000Z\t3.4E38\n" +
                            "2024-05-14T16:00:02.000000Z\tnull\n" +
                            "2024-05-14T16:00:02.000000Z\tnull\n",
                    "x");

            execute("alter table x alter column col type int", sqlExecutionContext);
            drainWalQueue();

            assertSql("ts\tcol\n" +
                    "2024-05-14T16:00:00.000000Z\t0\n" +
                    "2024-05-14T16:00:01.000000Z\t0\n" +
                    "2024-05-14T16:00:02.000000Z\t3\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n", "x");
        });
    }

    @Test
    public void testChangeFloatToDouble() throws Exception {
        assumeWal();
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, col float) timestamp(ts) partition by day wal", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:00.000000Z', 0.0)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:01.000000Z', 0.1)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', 3.1)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', -9223372036854775808.0)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', -3.4e38)", sqlExecutionContext);
            execute("insert into x values('2024-05-14T16:00:02.000000Z', 3.4e38)", sqlExecutionContext);
            drainWalQueue();

            execute("alter table x alter column col type double", sqlExecutionContext);
            drainWalQueue();

            assertSql("ts\tcol\n" +
                    "2024-05-14T16:00:00.000000Z\t0.0\n" +
                    "2024-05-14T16:00:01.000000Z\t0.10000000149011612\n" +
                    "2024-05-14T16:00:02.000000Z\t3.0999999046325684\n" +
                    "2024-05-14T16:00:02.000000Z\t-9.223372036854776E18\n" +
                    "2024-05-14T16:00:02.000000Z\t-3.3999999521443642E38\n" +
                    "2024-05-14T16:00:02.000000Z\t3.3999999521443642E38\n", "x");

            execute("alter table x alter column col type int", sqlExecutionContext);
            drainWalQueue();

            assertSql("ts\tcol\n" +
                    "2024-05-14T16:00:00.000000Z\t0\n" +
                    "2024-05-14T16:00:01.000000Z\t0\n" +
                    "2024-05-14T16:00:02.000000Z\t3\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n" +
                    "2024-05-14T16:00:02.000000Z\tnull\n", "x");
        });
    }

    @Test
    public void testChangeIndexedSymbolToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            execute("create table y as (select ik from x)", sqlExecutionContext);
            execute("alter table x alter column ik type varchar", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select ik from y",
                    "select ik from x"
            );

            execute("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");

            execute("insert into y(ik) values('abc')", sqlExecutionContext);
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
            execute("alter table x alter column ik type varchar", sqlExecutionContext);
            execute("alter table x alter column ik type string", sqlExecutionContext);
            execute("alter table x alter column ik type symbol index", sqlExecutionContext);
            execute("alter table x alter column ik type string", sqlExecutionContext);
            drainWalQueue();

            execute("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");
        });
    }

    @Test
    public void testChangeStringToBinary() throws Exception {
        assertFailure("alter table x alter column c type binary", 34, "incompatible column type change [existing=STRING, new=BINARY]");
    }

    @Test
    public void testChangeStringToIndexedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            execute("create table y as (select c from x)", sqlExecutionContext);
            execute("alter table x alter column c type symbol index", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from y",
                    "select c from x"
            );

            execute("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            execute("insert into y(c) values('abc')", sqlExecutionContext);
            assertSqlCursorsConvertedStrings(
                    "select c from y where c = 'abc'",
                    "select c from x where c = 'abc'"

            );
        });
    }

    @Test
    public void testChangeStringToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            execute("create table y as (select c from x)", sqlExecutionContext);
            execute("alter table x alter column c type symbol", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from y",
                    "select c from x"
            );

            execute("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            execute("create table z as (select c from x)", sqlExecutionContext);
            execute("alter table x alter column c type string", sqlExecutionContext);

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
            execute("create table y as (select c from x)", sqlExecutionContext);
            execute("alter table x alter column c type varchar", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select c from x",
                    "select c from y"
            );

            execute("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\nabc\n", "select c from x limit -1");

            execute("create table z as (select c from x)", sqlExecutionContext);
            execute("alter table x alter column c type string", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select c from z",
                    "select c from x"
            );
        });
    }

    @Test
    public void testChangeSymbolCannotChangeIndex() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            try {
                execute("alter table x alter column ik symbol capacity 512 index", sqlExecutionContext);
                Assert.fail("index syntax not supported when changing SYMBOL capacity");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "unexpected token [index] while trying to change symbol capacity");
            }
            drainWalQueue();
        });
    }

    @Test
    public void testChangeSymbolCapacity() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            execute("create table y as (select ik from x)", sqlExecutionContext);
            execute("alter table x alter column ik symbol capacity 512", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select ik from y",
                    "select ik from x"
            );

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ik\tSYMBOL\ttrue\t256\tfalse\t512\tfalse\tfalse\n", "(SHOW COLUMNS FROM x) WHERE column = 'ik'");


            execute("alter table x alter column ik symbol capacity 1000", sqlExecutionContext);

            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select ik from y",
                    "select ik from x"
            );

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ik\tSYMBOL\ttrue\t256\tfalse\t1024\tfalse\tfalse\n", "(SHOW COLUMNS FROM x) WHERE column = 'ik'");
        });
    }

    @Test
    public void testChangeSymbolToVarcharReleaseWriters() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();
            engine.releaseInactive();
            execute("alter table x alter column ik type varchar", sqlExecutionContext);
            drainWalQueue();

            execute("insert into x(ik, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();

            assertSql("ik\nabc\n", "select ik from x limit -1");
        });
    }

    @Test
    public void testChangeTypePreservesColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            execute("create table y as (select * from x)", sqlExecutionContext);
            execute("alter table x alter column c type symbol", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select * from y limit 10",
                    "select * from x limit 10"
            );
        });
    }

    @Test
    public void testChangeTypePreservesInsertColDefaultOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " rnd_str(5,5,2) c," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp," +
                            " from long_sequence(1)" +
                            ") timestamp (timestamp) PARTITION BY HOUR" + (walEnabled ? "  WAL" : " BYPASS WAL")
            );

            drainWalQueue();
            execute("alter table x alter column c type varchar", sqlExecutionContext);
            drainWalQueue();

            execute("insert into x values('abc', '2024-06-20T17:18:27.752076Z')", sqlExecutionContext);
            drainWalQueue();

            assertSql("c\nabc\n", "select c from x limit -1");

            execute("alter table x alter column c type string", sqlExecutionContext);
            drainWalQueue();
            engine.releaseInactive();

            execute("insert into x values('def', '2024-06-20T17:18:27.752076Z')", sqlExecutionContext);
            drainWalQueue();
            assertSql("c\ndef\n", "select c from x limit -1");

            execute("insert into x select * from x");
            drainWalQueue();

            assertSql("c\ttimestamp\n" +
                    "TJWCP\t2018-01-01T00:00:07.200000Z\n" +
                    "TJWCP\t2018-01-01T00:00:07.200000Z\n" +
                    "abc\t2024-06-20T17:18:27.752076Z\n" +
                    "abc\t2024-06-20T17:18:27.752076Z\n" +
                    "def\t2024-06-20T17:18:27.752076Z\n" +
                    "def\t2024-06-20T17:18:27.752076Z\n", "x order by timestamp, c");
        });
    }

    @Test
    public void testChangeVarcharStringSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            execute("create table y as (select v from x)", sqlExecutionContext);

            Rnd rnd = new Rnd();
            int currentType = ColumnType.VARCHAR;
            for (int i = 0; i < 10; i++) {

                int typeId = currentType;
                while (typeId == currentType) {
                    switch (rnd.nextPositiveInt() % 3) {
                        case 0:
                            typeId = ColumnType.STRING;
                            break;
                        case 1:
                            typeId = ColumnType.SYMBOL;
                            break;
                        default:
                            typeId = ColumnType.VARCHAR;
                            break;
                    }
                }
                String type = ColumnType.nameOf(typeId);
                currentType = typeId;
                execute("alter table x alter column v type " + type, sqlExecutionContext);

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
            execute("create table y as (select v from x)", sqlExecutionContext);
            execute("alter table x alter column v type symbol", sqlExecutionContext);

            drainWalQueue();
            assertSqlCursorsConvertedStrings(
                    "select v from x",
                    "select v from y"
            );

            execute("insert into x(v, timestamp) values('abc', now())", sqlExecutionContext);
            drainWalQueue();
            assertSql("v\nabc\n", "select v from x limit -1");

            execute("create table z as (select v from x)", sqlExecutionContext);
            execute("alter table x alter column v type varchar", sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select v from z",
                    "select v from x"
            );

            execute("alter table x add column sym_top symbol", sqlExecutionContext);
            execute("alter table z add column sym_top symbol", sqlExecutionContext);
            execute("insert into x(sym_top, timestamp) select rnd_symbol('a', 'b', 'c', null), timestamp_sequence(now(), 1) from long_sequence(123)", sqlExecutionContext);
            drainWalQueue();
            execute("insert into z(sym_top) select sym_top from x limit -123", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select sym_top from z",
                    "select sym_top from x"
            );

            execute("alter table x alter column sym_top type varchar", sqlExecutionContext);
            execute("alter table z alter column sym_top type varchar", sqlExecutionContext);
            drainWalQueue();

            assertSqlCursorsConvertedStrings(
                    "select sym_top from z",
                    "select sym_top from x"
            );
        });
    }

    @Test
    public void testColumnDoesNotExist() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column non_existing", 27, "column 'non_existing' does not exist in table 'x'");
    }

    @Test
    public void testConversionInvalidToken() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column i type long abc", 39, "unexpected token [abc] while trying to change column type");
    }

    @Test
    public void testConversionsFromArray() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table x (arr double[]);");
                assertException("alter table x alter column arr type uuid", 36, "incompatible column type change [existing=DOUBLE[], new=UUID]", sqlExecutionContext);
                assertException("alter table x alter column arr type int", 36, "incompatible column type change [existing=DOUBLE[], new=INT]", sqlExecutionContext);
                assertException("alter table x alter column arr type ipv4", 36, "incompatible column type change [existing=DOUBLE[], new=IPv4]", sqlExecutionContext);
                assertException("alter table x alter column arr type long", 36, "incompatible column type change [existing=DOUBLE[], new=LONG]", sqlExecutionContext);
                assertException("alter table x alter column arr type short", 36, "incompatible column type change [existing=DOUBLE[], new=SHORT]", sqlExecutionContext);
                assertException("alter table x alter column arr type byte", 36, "incompatible column type change [existing=DOUBLE[], new=BYTE]", sqlExecutionContext);
                assertException("alter table x alter column arr type double", 36, "incompatible column type change [existing=DOUBLE[], new=DOUBLE]", sqlExecutionContext);
                assertException("alter table x alter column arr type float", 36, "incompatible column type change [existing=DOUBLE[], new=FLOAT]", sqlExecutionContext);
                assertException("alter table x alter column arr type char", 36, "incompatible column type change [existing=DOUBLE[], new=CHAR]", sqlExecutionContext);
                assertException("alter table x alter column arr type boolean", 36, "incompatible column type change [existing=DOUBLE[], new=BOOLEAN]", sqlExecutionContext);
                assertException("alter table x alter column arr type timestamp", 36, "incompatible column type change [existing=DOUBLE[], new=TIMESTAMP]", sqlExecutionContext);
                assertException("alter table x alter column arr type date", 36, "incompatible column type change [existing=DOUBLE[], new=DATE]", sqlExecutionContext);
                assertException("alter table x alter column arr type symbol", 36, "incompatible column type change [existing=DOUBLE[], new=SYMBOL]", sqlExecutionContext);
                assertException("alter table x alter column arr type string", 36, "incompatible column type change [existing=DOUBLE[], new=STRING]", sqlExecutionContext);
                assertException("alter table x alter column arr type varchar", 36, "incompatible column type change [existing=DOUBLE[], new=VARCHAR]", sqlExecutionContext);
                assertException("alter table x alter column arr type binary", 36, "incompatible column type change [existing=DOUBLE[], new=BINARY]", sqlExecutionContext);
            } finally {
                execute("drop table if exists x;");
            }
        });
    }

    @Test
    public void testConversionsToArray() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute(
                        "create table x as (" +
                                "select" +
                                " rnd_uuid4() guid," +
                                " rnd_int() rint," +
                                " rnd_ipv4() ip," +
                                " rnd_long() i64," +
                                " rnd_short() i16," +
                                " rnd_byte() i8," +
                                " rnd_double() f64," +
                                " rnd_float() f32," +
                                " rnd_char() ch," +
                                " rnd_boolean() b," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 ts," +
                                " cast(to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 as date) dt," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp," +
                                " rnd_symbol('A', 'B', 'C') sym," +
                                " rnd_str('abc', 'def', 'ghi') str," +
                                " rnd_varchar('abc', 'def', 'ghi') var," +
                                " rnd_bin() bin" +
                                " from long_sequence(1000)" +
                                ")"
                );

                assertException("alter table x alter column guid type DOUBLE[]", 44, "incompatible column type change [existing=UUID, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column rint type DOUBLE[]", 44, "incompatible column type change [existing=INT, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column ip type DOUBLE[]", 42, "incompatible column type change [existing=IPv4, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column i64 type DOUBLE[]", 43, "incompatible column type change [existing=LONG, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column i16 type DOUBLE[]", 43, "incompatible column type change [existing=SHORT, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column i8 type DOUBLE[]", 42, "incompatible column type change [existing=BYTE, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column f64 type DOUBLE[]", 43, "incompatible column type change [existing=DOUBLE, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column f32 type DOUBLE[]", 43, "incompatible column type change [existing=FLOAT, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column ch type DOUBLE[]", 42, "incompatible column type change [existing=CHAR, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column b type DOUBLE[]", 41, "incompatible column type change [existing=BOOLEAN, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column ts type DOUBLE[]", 42, "incompatible column type change [existing=TIMESTAMP, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column dt type DOUBLE[]", 42, "incompatible column type change [existing=DATE, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column sym type DOUBLE[]", 43, "incompatible column type change [existing=SYMBOL, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column str type DOUBLE[]", 43, "incompatible column type change [existing=STRING, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column var type DOUBLE[]", 43, "incompatible column type change [existing=VARCHAR, new=DOUBLE[]]", sqlExecutionContext);
                assertException("alter table x alter column bin type DOUBLE[]", 43, "incompatible column type change [existing=BINARY, new=DOUBLE[]]", sqlExecutionContext);

            } finally {
                execute("drop table if exists x;");
            }
        });
    }

    @Test
    public void testConvertDedupKeyColumnKeepsItDedup() throws Exception {
        assumeWal();
        assertMemoryLeak(() -> {
            createX();

            execute("alter table x dedup enable upsert keys(timestamp, d)");
            drainWalQueue();
            checkDedupSet("d", true);

            execute("alter table x alter column d type float");
            drainWalQueue();
            checkDedupSet("d", true);

            engine.releaseInactive();
            checkDedupSet("d", true);

            execute("insert into x(d, timestamp) values(1.0, '2044-02-24')", sqlExecutionContext);
            execute("insert into x(d, timestamp) values(1.0, '2044-02-25')", sqlExecutionContext);
            execute("insert into x(d, timestamp) values(1.0, '2044-02-25')", sqlExecutionContext);
            execute("insert into x(d, timestamp) values(1.2, '2044-02-25')", sqlExecutionContext);

            drainWalQueue();

            assertSql(
                    "timestamp\td\n" +
                            "2044-02-24T00:00:00.000000Z\t1.0\n" +
                            "2044-02-25T00:00:00.000000Z\t1.0\n" +
                            "2044-02-25T00:00:00.000000Z\t1.2\n",
                    "select timestamp, d from x order by timestamp, d limit -3"
            );
        });
    }

    @Test
    public void testConvertFailsOnColumnFileOpen() throws Exception {
        assumeNonWal();
        AtomicReference<String> fail = new AtomicReference<>();
        AtomicBoolean failRead = new AtomicBoolean();

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failRead.get() && fail.get() != null && Misc.getThreadLocalUtf8Sink().put(name).toString().endsWith(fail.get())) {
                    fail.set(null);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, long opts) {
                if (!failRead.get() && fail.get() != null && Misc.getThreadLocalUtf8Sink().put(name).toString().endsWith(fail.get())) {
                    fail.set(null);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            createX();

            fail.set("c.d.1");
            try {
                execute("alter table x alter column c type varchar", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
            }

            fail.set("c.i.1");
            try {
                execute("alter table x alter column c type varchar", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
            }

            fail.set("c.d");
            failRead.set(true);
            try {
                execute("alter table x alter column c type varchar", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only");
            }

            fail.set("c.i");
            try {
                execute("alter table x alter column c type varchar", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only");
            }

            fail.set(null);
            execute("alter table x alter column c type varchar", sqlExecutionContext);

            execute("insert into x(c, timestamp) values('asdfadf', now())", sqlExecutionContext);
            assertSql("c\nasdfadf\n", "select c from x limit -1");
        });
    }

    @Test
    public void testConvertFailsWriterIsOk() throws Exception {
        assumeNonWal();
        assertMemoryLeak(() -> {
            createX();

            try (TableWriter writer = getWriter("x")) {
                writer.changeColumnType("timestamp", ColumnType.INT, 0, false, false, 0, false, null);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot change column type, column is the designated timestamp");
            }

            try (TableWriter writer = getWriter("x")) {
                writer.changeColumnType("d", ColumnType.DOUBLE, 0, false, false, 0, false, null);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot change column type, new type is the same as existing");
            }

            try (TableWriter writer = getWriter("x")) {
                writer.changeColumnType("ik", ColumnType.GEOBYTE, 0, false, false, 0, false, null);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "column conversion failed, see logs for details");
            }

            execute("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            assertSql("c\nabc\n", "select c from x limit -1");

            engine.releaseInactive();

            execute("insert into x(c, timestamp) values('def', now())", sqlExecutionContext);
            assertSql("c\ndef\n", "select c from x limit -1");
        });
    }

    @Test
    public void testConvertFromSymbolToStringDedupFlagIsAllowed() throws Exception {
        assumeWal();
        assertMemoryLeak(() -> {
            createX();

            execute("alter table x dedup enable upsert keys(timestamp, ik)");
            drainWalQueue();
            checkDedupSet("ik", true);
            checkDedupSet("f", false);

            execute("alter table x alter column ik type varchar");
            drainWalQueue();
            checkDedupSet("ik", true);

            execute("insert into x(ik, d, timestamp) values('abc', 2, '2044-02-24')", sqlExecutionContext);
            execute("insert into x(ik, d, timestamp) values('abc', 3, '2044-02-24')", sqlExecutionContext);
            execute("insert into x(ik, d, timestamp) values('abc', 4, '2044-02-25')", sqlExecutionContext);
            execute("insert into x(ik, d, timestamp) values('def', 5, '2044-02-25')", sqlExecutionContext);

            drainWalQueue();

            assertSql("timestamp\td\tik\n" +
                    "2018-01-01T02:00:00.000000Z\t0.04488373772232379\tCPSW\n" +
                    "2044-02-24T00:00:00.000000Z\t3.0\tabc\n" +
                    "2044-02-25T00:00:00.000000Z\t4.0\tabc\n" +
                    "2044-02-25T00:00:00.000000Z\t5.0\tdef\n", "select timestamp, d, ik from x limit -4");
        });
    }

    @Test
    public void testConvertInvalidColumnFailsWriterIsOk() throws Exception {
        assumeNonWal();
        assertMemoryLeak(() -> {
            createX();

            try (TableWriter writer = getWriter("x")) {
                writer.changeColumnType("non_existing", ColumnType.INT, 0, false, false, 0, false, null);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot change column type, column does not exist");
            }

            execute("insert into x(c, timestamp) values('abc', now())", sqlExecutionContext);
            assertSql("c\nabc\n", "select c from x limit -1");
        });
    }

    @Test
    public void testFixedSizeColumnEquivalentToCast() throws Exception {
        final String[] types = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", "BOOLEAN", "DATE"};
        final char[] col_names = {'l', 'f', 'i', 'j', 'e', 'd', 'k', 't', 'g'};

        testFixedToFixedConversions(types, col_names);
    }

    @Test
    public void testFixedSizeColumnLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            execute("create table y ( converted long, casted int, original long );", sqlExecutionContext);
            execute("insert into y (converted, casted, original) values (9999999999999, 9999999999999::int, 9999999999999)", sqlExecutionContext);
            drainWalQueue();
            execute("alter table y alter column converted type int", sqlExecutionContext);
            drainWalQueue();

            assertQuery("converted\tcasted\toriginal\n" +
                    "1316134911\t1316134911\t9999999999999\n", "select * from y", null, true, true);

        });
    }

    @Test
    public void testFixedSizeColumnNullableBehaviour() throws Exception {
        assumeNonWal();
        assertMemoryLeak(() -> {
            drainWalQueue();
            final String[] types = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", "BOOLEAN", "DATE"};
            String longMinValue = Long.toString(Long.MIN_VALUE + 1);
            final String[] minVals = {"-128", Short.toString(Short.MIN_VALUE), Integer.toString(Integer.MIN_VALUE + 1), longMinValue, -Float.MAX_VALUE + "f", Double.toString(-Double.MAX_VALUE), longMinValue, "false", longMinValue};
            String longMaxValue = Long.toString(Long.MAX_VALUE);
            final String[] maxVals = {"127", Short.toString(Short.MAX_VALUE), Integer.toString(Integer.MAX_VALUE), longMaxValue, Float.MAX_VALUE + "f", Double.toString(Double.MAX_VALUE), longMaxValue, "true", longMaxValue};

            execute("drop table if exists y", sqlExecutionContext);

            for (int i = 0, n = types.length; i < n; i++) {
                for (int j = 0, m = types.length; j < m; j++) {
                    // skip unsupported noop conversion
                    if (i == j) {
                        continue;
                    }

                    String srcType = types[i];
                    String dstType = types[j];

                    LOG.info().$("checking `" + srcType + "` to `" + dstType + "` conversion").$();

                    execute("create table y ( converted " + srcType + ", casted " + dstType + ", original " + srcType + ")", sqlExecutionContext);
                    execute("insert into y (converted, casted, original) values (null, cast(cast(null as " + srcType + ") as " + dstType + "), null)", sqlExecutionContext);
                    execute("insert into y (converted, casted, original) values (" + minVals[i] + ", cast(cast(" + minVals[i] + " as " + srcType + ") as " + dstType + "), " + minVals[i] + ")", sqlExecutionContext);
                    execute("insert into y (converted, casted, original) values (" + maxVals[i] + ", cast(cast(" + maxVals[i] + " as " + srcType + ") as " + dstType + "), " + maxVals[i] + ")", sqlExecutionContext);

                    execute("alter table y alter column converted type " + dstType, sqlExecutionContext);

                    try {
                        assertSql(
                                "count\n0\n",
                                "select count(*) from y where converted <> casted"
                        );
                    } catch (AssertionError e) {
                        LOG.error().$("failed, error: ").$(e).$();
                        // if the column wasn't converted
                        if (e.getMessage().contains("column")) {
                            throw e;
                        } else {
                            // dump the difference in data
                            assertSql("\nFailed equivalent conversion from `" + srcType + "` to `" + dstType + "`.\n", "select converted, casted, original from y");
                        }
                    }
                    assertSql(
                            "column\ttype\n" +
                                    "converted\t" + dstType + "\n" +
                                    "casted\t" + dstType + "\n" +
                                    "original\t" + srcType + "\n",
                            "select \"column\", type from table_columns('y')"
                    );
                    execute("drop table y", sqlExecutionContext);
                    drainWalQueue();
                }

            }
        });
    }

    @Test
    public void testFixedToStrConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertFixedToVar("string");
        });
    }

    @Test
    public void testFixedToSymbolConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertVarToFixed("symbol");
        });
    }

    @Test
    public void testFixedToVarcharConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertFixedToVar("varchar");
        });
    }

    @Test
    public void testIntOverflowConversions() throws SqlException {
        //assumeWal();
        execute("create table x (a long, timestamp timestamp) timestamp (timestamp) PARTITION BY HOUR" + (walEnabled ? " WAL" : " BYPASS WAL"));
        execute("insert into x(a, timestamp) values(-7178801693176412875L, '2024-02-04T00:00:00.000Z')", sqlExecutionContext);
        drainWalQueue();

        execute("alter table x alter column a type double", sqlExecutionContext);
        drainWalQueue();
        assertSql("a\n-7.1788016931764132E18\n", "select a from x");

        execute("alter table x alter column a type int", sqlExecutionContext);
        drainWalQueue();
        assertSql("cast\ta\n" +
                "null\tnull\n", "select cast(-7.1788016931764132E18 as int), a from x");
    }

    @Test
    public void testNewTypeInvalid() throws Exception {
        assumeNonWal();
        assertFailure("alter table x alter column c type abracadabra", 34, "unsupported column type: abracadabra");
    }

    @Test
    public void testNewTypeMissing() throws Exception {
        assumeNonWal();
        assertFailure("alter table x alter column c type", 33, "column type expected");
    }

    @Test
    public void testShouldTruncateConvertedColumns() throws Exception {
        assumeNonWal();
        assertMemoryLeak(() -> {
            // Create table with many partitions
            execute(
                    "create table x as (" +
                            "select" +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 72000000 timestamp," +
                            " x," +
                            " rnd_str(5,1024,2) c" +
                            " from long_sequence(1000)" +
                            ") timestamp (timestamp) PARTITION BY HOUR BYPASS WAL"
            );

            // One each platform the table size can be slightly different, query the size from QuestDB
            getFirstRowFirstColumn();
            long initialSize = Numbers.parseLong(sink);

            // 5-15Mb approx
            Assert.assertTrue(initialSize > 5E6 && initialSize < 15E6);

            // Test the size isn't ballooned after the conversion, it's no more than 25% larger than the initial size
            execute("alter table x alter column c type varchar", sqlExecutionContext);
            assertSql("column\ntrue\n", "select sum(diskSize) < " + (initialSize * 1.25) + " from table_partitions('x')");

            // Test the size back to the original
            execute("alter table x alter column c type string", sqlExecutionContext);
            assertSql("sum\n" + initialSize + "\n", "select sum(diskSize) from table_partitions('x')");

            // Test the size isn't ballooned after the conversion, it's no more than 50% larger than the initial size
            execute("alter table x alter column x type string", sqlExecutionContext);
            assertSql("column\ntrue\n", "select sum(diskSize) < " + (initialSize * 1.5) + " from table_partitions('x')");

            // Test the size back to the original
            execute("alter table x alter column x type int", sqlExecutionContext);
            assertSql("sum\n" + initialSize + "\n", "select sum(diskSize) from table_partitions('x')");
        });
    }

    @Test
    public void testStrToFixedConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertVarToFixed("string");
        });
    }

    @Test
    public void testSymbolToFixedConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertFixedToVar("symbol");
        });
    }

    @Test
    public void testTimestampConversionInvalid() throws Exception {
        Assume.assumeTrue(!walEnabled && partitioned);
        assertFailure("alter table x alter column timestamp type long", 42, "cannot change type of designated timestamp column");
    }

    @Test
    public void testVarcharToFixedConversions() throws Exception {
        assertMemoryLeak(() -> {
            assumeNonWal();
            testConvertVarToFixed("varchar");
        });
    }

    @Test
    public void testWalConversionFromVarToFixedDoesNotLeaveAuxFiles() throws Exception {
        assumeWal();
        assertMemoryLeak(() -> {
            execute("create table x (s string, timestamp timestamp) timestamp (timestamp) PARTITION BY HOUR WAL;");
            execute("alter table x alter column s type int;");

            TableToken xTbl = engine.verifyTableName("x");

            Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(xTbl).concat("wal1").concat("0").concat("s.d");
            Assert.assertTrue(Files.exists(path.$()));

            path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(xTbl).concat("wal1").concat("0").concat("s.i");
            Assert.assertFalse(Files.exists(path.$()));

            execute("insert into x(s, timestamp) values(1, '2024-02-04T00:00:00.000Z')", sqlExecutionContext);
            drainWalQueue();

            assertSql("s\n1\n", "select s from x limit -1");
        });
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

    private static void testConvertFixedToVar(String varTypeName) throws SqlException {
        try {
            execute(
                    "create table if not exists x as (" +
                            "select" +
                            " rnd_uuid4() guid," +
                            " rnd_int() rint," +
                            " rnd_ipv4() ip," +
                            " rnd_long() i64," +
                            " rnd_short() i16," +
                            " rnd_byte() i8," +
                            " rnd_double() f64," +
                            " rnd_float() f32," +
                            " rnd_char() ch," +
                            " rnd_boolean() b," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 ts," +
                            " cast(to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 as date) dt," +
                            " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp" +
                            " from long_sequence(1000)" +
                            ")"
            );
            // add nulls last line
            execute("insert into x(timestamp) values('2018-01-03T23:23')", sqlExecutionContext);

            execute("create table y as (" +
                    "select cast(cast(guid as string) as " + varTypeName + ") as guid," +
                    " cast(cast(rint as string) as " + varTypeName + ") as rint," +
                    " cast(cast(ip as string) as " + varTypeName + ") as ip," +
                    " cast(cast(i64 as string) as " + varTypeName + ") as i64," +
                    " cast(cast(i16 as string) as " + varTypeName + ") as i16," +
                    " cast(cast(i8 as string) as " + varTypeName + ") as i8," +
                    " cast(cast(f64 as string) as " + varTypeName + ") as f64," +
                    " cast(cast(f32 as string) as " + varTypeName + ") as f32," +
                    " cast(cast(ch as string) as " + varTypeName + ") as ch," +
                    " cast(cast(b as string) as " + varTypeName + ") as b," +
                    " cast(cast(ts as string) as " + varTypeName + ") as ts," +
                    " cast(cast(dt as string) as " + varTypeName + ") as dt," +
                    " timestamp from x) " +
                    "timestamp (timestamp) partition by DAY;", sqlExecutionContext);

            execute("alter table x alter column guid type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column rint type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column ip type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column i64 type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column i16 type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column i8 type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column f64 type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column f32 type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column ch type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column b type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column ts type " + varTypeName, sqlExecutionContext);
            execute("alter table x alter column dt type " + varTypeName, sqlExecutionContext);

            assertSqlCursorsConvertedStrings(
                    "select * from x",
                    "select * from y"
            );
        } finally {
            execute("drop table if exists x;");
            execute("drop table if exists y;");
        }
    }

    private static void testConvertVarToFixed(String varType) throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " rnd_uuid4() guid," +
                        " rnd_int() rint," +
                        " rnd_ipv4() ip," +
                        " rnd_long() i64," +
                        " rnd_short() i16," +
                        " rnd_byte() i8," +
                        " rnd_double() f64," +
                        " rnd_float() f32," +
                        " rnd_char() ch," +
                        " rnd_boolean() b," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 ts," +
                        " cast(to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 as date) dt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 7200000 timestamp" +
                        " from long_sequence(1000)" +
                        ")"
        );
        // add nulls last line
        execute("insert into x(timestamp) values('2018-01-03T23:23')", sqlExecutionContext);

        execute("create table y as (" +
                "select" +
                " cast(guid as " + varType + ") as guid," +
                " cast(cast(rint as string) as " + varType + ") as rint," +
                " cast(cast(ip as string) as " + varType + ") as ip," +
                " cast(cast(i64 as string) as " + varType + ") as i64," +
                " cast(cast(i16 as string) as " + varType + ") as i16," +
                " cast(cast(i8 as string) as " + varType + ") as i8," +
                " cast(cast(f64 as string) as " + varType + ") as f64," +
                " cast(cast(f32 as string) as " + varType + ") as f32," +
                " cast(cast(ch as string) as " + varType + ") as ch," +
                " cast(cast(b as string) as " + varType + ") as b," +
                " cast(cast(ts as string) as " + varType + ") as ts," +
                " cast(cast(dt as string) as " + varType + ") as dt," +
                " timestamp from x) " +
                "timestamp (timestamp) partition by DAY;", sqlExecutionContext);

        // Insert garbage data
        execute("insert into y(guid, rint, ip, i64, i8, i16, f64, f32, ch, ts, dt, timestamp) values('abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', '', 'abc', 'abc', '2018-01-03T23:23:10')", sqlExecutionContext);
        // Expect nulls
        execute("insert into x(timestamp) values('2018-01-03T23:23:10')", sqlExecutionContext);

        execute("alter table y alter column guid type uuid", sqlExecutionContext);
        execute("alter table y alter column rint type int", sqlExecutionContext);
        execute("alter table y alter column ip type ipv4", sqlExecutionContext);
        execute("alter table y alter column i64 type long", sqlExecutionContext);
        execute("alter table y alter column i16 type short", sqlExecutionContext);
        execute("alter table y alter column i8 type byte", sqlExecutionContext);
        execute("alter table y alter column f64 type double", sqlExecutionContext);
        execute("alter table y alter column f32 type float", sqlExecutionContext);
        execute("alter table y alter column ch type char", sqlExecutionContext);
        execute("alter table y alter column b type boolean", sqlExecutionContext);
        execute("alter table y alter column ts type timestamp", sqlExecutionContext);
        execute("alter table y alter column dt type date", sqlExecutionContext);

        assertSqlCursorsConvertedStrings(
                "select * from x",
                "select * from y"
        );
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                execute(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void assumeNonWal() {
        Assume.assumeTrue("Test disabled during WAL run.", !walEnabled && partitioned);
    }

    private void assumeWal() {
        Assume.assumeTrue("Test disabled during WAL run.", walEnabled);
    }

    private void checkDedupSet(String columnName, boolean value) {
        try (TableWriter writer = getWriter("x")) {
            int colIndex = writer.getMetadata().getColumnIndex(columnName);
            Assert.assertEquals("dedup key flag mismatch column:" + columnName, value, writer.getMetadata().isDedupKey(colIndex));
        }
    }

    private void createX() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " case WHEN x % 10 = 0 THEN NULL WHEN x % 10 = 1 THEN 0 ELSE cast(x as int) END i," +
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
                        " case WHEN x % 10 = 0 THEN NULL WHEN x % 10 = 1 THEN 0 ELSE rnd_long() END j," +
                        " case WHEN x % 10 = 0 THEN NULL WHEN x % 10 = 1 THEN CAST('1970-01-01' AS TIMESTAMP) ELSE timestamp_sequence(0, 1000000000) END k," +
                        " rnd_byte(2,50) l," +
                        " rnd_boolean() t," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_varchar(5,64,2) v" +
                        " from long_sequence(1000)" +
                        "), index(ik) timestamp (timestamp) " +
                        (partitioned ? "PARTITION BY DAY " : "PARTITION BY NONE ") +
                        (walEnabled ? "WAL" : (partitioned ? "BYPASS WAL" : ""))
        );
    }

    private void testFixedToFixedConversions(String[] types, char[] col_names) throws Exception {
        assumeNonWal();
        assertMemoryLeak(() -> {
            createX();
            drainWalQueue();

            for (int i = 0, n = types.length; i < n; i++) {
                for (int j = 0, m = types.length; j < m; j++) {
                    // skip unsupported noop conversion
                    if (i == j) {
                        continue;
                    }

                    String srcType = types[i];
                    char srcColName = col_names[i];
                    String dstType = types[j];

                    LOG.info().$("checking `" + srcType + "` to `" + dstType + "` conversion").$();

                    execute("create table y ( converted " + srcType + ", casted " + dstType + ", original " + srcType + ")", sqlExecutionContext);
                    execute("insert into y select " + srcColName + " as converted, cast(" + srcColName + " as " + dstType + ") as casted, " + srcColName + " as original from x", sqlExecutionContext);
                    execute("alter table y alter column converted type " + dstType, sqlExecutionContext);

                    try {
                        assertSql(
                                "count\n0\n",
                                "select count(*) from y where converted <> casted"
                        );
                    } catch (AssertionError e) {
                        LOG.error().$("failed, error: ").$(e).$();
                        // if the column wasn't converted
                        if (e.getMessage().contains("column")) {
                            throw e;
                        } else {
                            // dump the difference in data
                            assertSql("\nFailed equivalent conversion from `" + srcType + "` to `" + dstType + "`.\n", "select converted, casted, original from y");
                        }
                    }
                    assertSql(
                            "column\ttype\n" +
                                    "converted\t" + dstType + "\n" +
                                    "casted\t" + dstType + "\n" +
                                    "original\t" + srcType + "\n",
                            "select \"column\", type from table_columns('y')"
                    );
                    execute("drop table y", sqlExecutionContext);
                    drainWalQueue();

                }

            }
        });
    }

    private void testWalRollUncommittedConversion(int columnType, String columnCreateSql, String convertToTypeSql) throws SqlException, NumericException {
        assumeWal();
        execute(
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
            execute("alter table x alter column c type " + convertToTypeSql, sqlExecutionContext);

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

    protected static void getFirstRowFirstColumn() throws SqlException {
        try (RecordCursorFactory factory = select("select sum(diskSize) from table_partitions('x')")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                RecordMetadata metadata = factory.getMetadata();
                sink.clear();
                final Record record = cursor.getRecord();
                if (cursor.hasNext()) {
                    CursorPrinter.printColumn(record, metadata, 0, sink, false);
                }
            }
        }
    }

    public enum Mode {
        WITH_WAL, NO_WAL, NON_PARTITIONED
    }
}
