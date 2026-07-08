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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.ArgSwappingFunctionFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.griffin.engine.functions.array.ArrayCreateFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayAccessFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampIntervalFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InUuidFunctionFactory;
import io.questdb.griffin.engine.functions.bool.WithinGeohashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToRegClassFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToStrArrayFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.GlobFilesFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.StringToStringArrayFunction;
import io.questdb.griffin.engine.functions.catalogue.WalTransactionsFunctionFactory;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.RecordColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.conditional.CoalesceFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.functions.constants.ArrayTypeConstant;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.ByteConstant;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.FloatConstant;
import io.questdb.griffin.engine.functions.constants.GeoByteConstant;
import io.questdb.griffin.engine.functions.constants.GeoIntConstant;
import io.questdb.griffin.engine.functions.constants.GeoLongConstant;
import io.questdb.griffin.engine.functions.constants.GeoShortConstant;
import io.questdb.griffin.engine.functions.constants.IPv4Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.Long128Constant;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.constants.NullBinConstant;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.engine.functions.constants.ShortConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.functions.date.DateTruncFunctionFactory;
import io.questdb.griffin.engine.functions.date.ExtractFromTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampCeilFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.eq.ContainsEqIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.ContainsIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntStrCFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntervalFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLong256StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqTimestampCursorFunctionFactory;
import io.questdb.griffin.engine.functions.eq.NegContainsEqIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.NegContainsIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceArrayFunctionFactory;
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceFunctionFactory;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtStrIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.math.GreatestNumericFunctionFactory;
import io.questdb.griffin.engine.functions.math.LeastNumericFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndDoubleArrayFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndIPv4CCFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndSymbolListFunctionFactory;
import io.questdb.griffin.engine.functions.table.HydrateTableMetadataFunctionFactory;
import io.questdb.griffin.engine.functions.table.ReadParquetFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestSumXDoubleGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagDateFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagLongFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadDateFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadLongFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadTimestampFunctionFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.*;

public class ExplainPlanTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(ExplainPlanTest.class);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        testNanoClock = StationaryNanosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        engine.getMatViewStateStore().clear();
        inputRoot = root;
    }

    @Test
    public void test2686LeftJoinDoesNotMoveOtherInnerJoinPredicate() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) \
                join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Filter filter: a.ts>=dateadd('m',-1,b2.ts)
                                    Cross Join
                                        Filter filter: b.age=10
                                            Nested Loop Left Join
                                              filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table_1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table_2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinDoesNotMoveOtherLeftJoinPredicate() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) \
                left join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (a.ts>=dateadd('m',-1,b2.ts) and b.age=10)
                                    Nested Loop Left Join
                                      filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_1
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_2
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinDoesNotMoveOtherTwoTableEqJoinPredicate() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) \
                join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and a.age = b.age\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Filter filter: a.ts>=dateadd('m',-1,b2.ts)
                                    Cross Join
                                        Filter filter: a.age=b.age
                                            Nested Loop Left Join
                                              filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table_1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: table_2
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinDoesNotPushJoinPredicateToLeftTable() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and a.age = 10\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts and a.age=10)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_1
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinDoesNotPushJoinPredicateToRightTable() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts\s
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and b.age = 10\s""")
                .assertsPlan("""
                        SelectedRecord
                            Nested Loop Left Join
                              filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts and b.age=10)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table_1
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinDoesNotPushWherePredicateToRightTable() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)\
                where b.age = 10\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Filter filter: b.age=10
                                    Nested Loop Left Join
                                      filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_1
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftJoinCondition() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) \
                where a.age * b.age = 10""")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: a.age*b.age=10
                                Nested Loop Left Join
                                  filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_1
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_2
                        """);
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftTable() throws Exception {
        test2686Prepare();
        assertQuery("""
                select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)
                from table_1 as a\s
                left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)\
                where a.age = 10\s""")
                .assertsPlan("""
                        VirtualRecord
                          functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)
                                    Async JIT Filter workers: 1
                                      filter: age=10
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: table_1
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table_2
                        """);
    }

    @Test
    public void testArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][], arr3 DOUBLE[][][], a DOUBLE)");
            String commonPart1 = "VirtualRecord\n  functions: [";
            String commonPart2 = "]\n    PageFrame\n        Row forward scan\n        Frame forward scan on: tango\n";
            assertQuery("SELECT arr1[1] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "arr1[1]" + commonPart2);
            assertQuery("SELECT arr1[1:1] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "arr1[1:1]" + commonPart2);
            assertQuery("SELECT arr3[1:1, 2:3, 4:] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "arr3[1:1,2:3,4:]" + commonPart2);
            assertQuery("SELECT ARRAY[1.0, 2] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "ARRAY[1.0,2.0]" + commonPart2);
            assertQuery("SELECT ARRAY[[1.0, 2], [3.0, 4]] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]]" + commonPart2);
            assertQuery("SELECT ARRAY[a, a] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "ARRAY[a,a]" + commonPart2);
            assertQuery("SELECT ARRAY[arr1, arr1] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "ARRAY[arr1,arr1]" + commonPart2);
            assertQuery("SELECT ARRAY[arr1[1:2], arr2[1]] FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "ARRAY[arr1[1:2],arr2[1]]" + commonPart2);
            assertQuery("SELECT transpose(arr2) FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "transpose(arr2)" + commonPart2);
            assertQuery("SELECT arr2 * transpose(arr2) FROM tango")
                    .noLeakCheck()
                    .assertsPlan(commonPart1 + "arr2*transpose(arr2)" + commonPart2);
        });
    }

    @Test
    public void testAsOfJoin0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b on ts where a.i = b.ts::int")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.ts::int
                                    AsOf Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select ts, ts1, i, i1 from (select * from a asof join b on ts ) where i/10 = i1")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i/10=b.i
                                    AsOf Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join (select * from b limit 10) on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Encode sort light
                                      keys: [ts, i]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * " + "from a " + "asof join b on ts " + "asof join a c on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    AsOf Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test // where clause predicate can't be pushed to join clause because asof is and outer join
    public void testAsOfJoin5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * " + "from a " + "asof join b " + "where a.i = b.i")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.i
                                    AsOf Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoinFilterPushedThroughView() throws Exception {
        // Regression lock-in: a subquery wraps an ASOF join and the consumer filters by the master
        // (trades) key. The master is index 0 (inner), so the guard in
        // deriveTransitiveFiltersFromPushedPredicate passes and the constant propagates across the
        // equi-join key onto the ASOF slave (quotes), exactly as it does for the inline query. The
        // equi-join key q.sym = t.sym forces any matched quote to share the filtered symbol, so
        // pre-filtering the slave cannot change which row ASOF picks. This guards that behaviour and
        // its result.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL INDEX, px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE quotes (sym SYMBOL INDEX, bid DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO trades VALUES
                      ('AA', 10.0, '2024-01-01T00:01:00.000000Z'),
                      ('BB', 20.0, '2024-01-01T00:02:00.000000Z'),
                      ('AA', 11.0, '2024-01-01T00:04:00.000000Z')""");
            execute("""
                    INSERT INTO quotes VALUES
                      ('AA', 1.0, '2024-01-01T00:00:00.000000Z'),
                      ('BB', 2.0, '2024-01-01T00:00:00.000000Z'),
                      ('AA', 1.5, '2024-01-01T00:03:00.000000Z')""");
            // trades AA @00:01 -> latest AA quote at or before 00:01 is bid 1.0 @00:00
            // trades AA @00:04 -> latest AA quote at or before 00:04 is bid 1.5 @00:03
            // the BB trade is excluded by the filter
            assertQuery("""
                    SELECT * FROM (
                      SELECT t.sym AS k, t.px, q.bid
                      FROM trades t ASOF JOIN quotes q ON q.sym = t.sym
                    ) WHERE k = 'AA'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tpx\tbid
                            AA\t10.0\t1.0
                            AA\t11.0\t1.5
                            """);
        });
    }

    @Test
    public void testAsOfJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertQuery("select * " + "from a " + "asof join b on a.i = b.i")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    AsOf Join
                                      condition: b.i=a.i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                                """);
            }
        });
    }

    @Test
    public void testAsOfJoinNoKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b where a.i > 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    Async JIT Filter workers: 1
                                      filter: 0<i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
            assertQuery("select /*+ ENABLE_PRE_TOUCH(a) */ * from a asof join b where a.i > 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    Async JIT Filter workers: 1
                                      filter: 0<i [pre-touch]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoinNoKeyFast1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoinNoKeyFast2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b on(ts)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testAsOfJoinNoKeyFast3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, i int) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a asof join b on(ts)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testCachedWindowLightRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as ( " + "  select " + "    cast(x as int) i, " + "    rnd_symbol('a','b','c') sym, " + "    timestamp_sequence(0, 100000000) ts " + "   from long_sequence(100)" + ") timestamp(ts) partition by hour");

            String sql = "select i, " + "row_number() over (partition by sym), " + "avg(i) over (), " + "sum(i) over (), " + "first_value(i) over (), " + "from x limit 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows-max: 0 take-rows-max: 3
                                CachedWindowLight
                                  unorderedFunctions: [row_number() over (partition by [sym]),avg(i) over (),sum(i) over (),first_value(i) over ()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            i\trow_number\tavg\tsum\tfirst_value
                            1\t1\t50.5\t5050.0\t1
                            2\t2\t50.5\t5050.0\t1
                            3\t1\t50.5\t5050.0\t1
                            """);
        });
    }

    @Test
    public void testCastFloatToDouble() throws Exception {
        allowFunctionMemoization();
        assertMemoryLeak(() -> assertQuery("select rnd_float()::double ")
                .noLeakCheck()
                .assertsPlan("""
                        VirtualRecord
                          functions: [memoize(rnd_float()::double)]
                            long_sequence count: 1
                        """));
    }

    @Test
    public void testConstantReassociationBindVariable() throws Exception {
        assertQuery("select * from tab where d + $1 + 4 > 10")
                .ddl("create table tab (d double, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 10<d+$0::double+4
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testConstantReassociationFoldsAddition() throws Exception {
        assertQuery("select * from tab where d + 1 + 4 > 10")
                .ddl("create table tab (d double, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 10<d+5
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testConstantReassociationFoldsBitwiseAnd() throws Exception {
        assertQuery("select * from tab where l & 3 & 5 > 0")
                .ddl("create table tab (l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: 0<l&1
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testConstantReassociationFoldsCommutativePattern() throws Exception {
        assertQuery("select * from tab where 4 + (d + 1) > 10")
                .ddl("create table tab (d double, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 10<d+5
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testCountOfColumnsVectorized() throws Exception {
        assertQuery("select k, count(1) c1, " + "count(*) cstar, " + "count(i) ci, " + "count(l) cl, " + "count(d) cd, " + "count(dat) cdat, " + "count(ts) cts " + "from x")
                .ddl("create table x " + "(" + " k int, " + " i int, " + " l long, " + " f float, " + " d double, " + " dat date, " + " ts timestamp " + ")")
                .assertsPlan("""
                        VirtualRecord
                          functions: [k,c1,c1,ci,cl,cd,cdat,cts]
                            GroupBy vectorized: true workers: 1
                              keys: [k]
                              values: [count(*),count(i),count(l),count(d),count(dat),count(ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                        """);
    }

    @Test
    public void testCrossJoin0() throws Exception {
        assertQuery("select * from a cross join a b where length(a.s1) = length(b.s2)")
                .ddl("create table a ( i int, s1 string, s2 string)")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: length(a.s1)=length(b.s2)
                                Cross Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testCrossJoin0Output() throws Exception {
        assertQuery("select count(*) cnt from a cross join a b where length(a.s1) = length(b.s2)")
                .ddl("create table a as (select x, 's' || x as s1, 's' || (x%3) as s2 from long_sequence(3))")
                .noRandomAccess()
                .expectSize()
                .returns("cnt\n9\n");
    }

    @Test
    public void testCrossJoin1() throws Exception {
        assertQuery("select * from a cross join a b")
                .ddl("create table a ( i int)")
                .assertsPlan("""
                        SelectedRecord
                            Cross Join
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertQuery("select * from a cross join a b cross join a c")
                .ddl("create table a ( i int)")
                .assertsPlan("""
                        SelectedRecord
                            Cross Join
                                Cross Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testCrossJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 cross join t t2 order by t1.ts", "select * from (select * from t order by ts desc) t1 cross join t t2 order by t1.ts"};
            for (String query : queries) {
                assertQuery(query)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """);

                assertQuery(query)
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                x\tts\tx1\tts1
                                1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z
                                1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000002Z
                                2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z
                                2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z
                                """);
            }
        });
    }

    @Test
    public void testCrossJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " + "((select * from t order by ts desc) limit 10) t1 " + "cross join t t2 " + "order by t1.ts desc";

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Cross Join
                                    Limit value: 10 skip-rows: 0 take-rows: 2
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testCrossJoinWithSort3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");

            assertQuery("select * from " + "((select * from t order by ts asc) limit 10) t1 " + "cross join t t2 " + "order by t1.ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Cross Join
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testCrossJoinWithSort4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");

            assertQuery("select * from " + "((select * from t order by ts asc) limit 10) t1 " + "cross join t t2 " + "order by t1.ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [ts desc]
                                SelectedRecord
                                    Cross Join
                                        Limit value: 10 skip-rows: 0 take-rows: 0
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testCrossJoinWithSort5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");

            assertQuery("select * from " + "((select * from t order by ts asc) limit 10) t1 " + "cross join t t2 " + "order by t1.ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Cross Join
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testDateaddIntrinsic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (id long, ts timestamp, ts2 timestamp) timestamp(ts) partition by hour;");

            assertQuery("select * from x where id = 42 and dateadd('h', -1, ts) = '2020-01-01T00:01'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: id=42
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: x
                                      intervals: [("2020-01-01T01:01:00.000000Z","2020-01-01T01:01:00.000000Z")]
                            """);
            assertQuery("select * from x where id = 42 and dateadd('h', -1, ts2) = '2020-01-01T00:01'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: (id=42 and 2020-01-01T00:01:00.000000Z=dateadd('h',-1,ts2))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where dateadd('d', 1, ts) >= '2020-01-01T00:01' and id < 42")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: id<42
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: x
                                      intervals: [("2019-12-31T00:01:00.000000Z","294247-01-09T04:00:54.775807Z")]
                            """);
            assertQuery("select * from x where dateadd('d', 1, ts2) >= '2020-01-01T00:01' and id < 42")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: (dateadd('d',1,ts2)>=2020-01-01T00:01:00.000000Z and id<42)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select * from x where id in (1,2,3) and dateadd('m', -1, ts) between '2020-01-01T00:01' and '2020-01-01T00:02'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: id in [1,2,3]
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: x
                                      intervals: [("2020-01-01T00:02:00.000000Z","2020-01-01T00:03:00.000000Z")]
                            """);
            assertQuery("select * from x where id in (1,2,3) and dateadd('m', -1, ts2) between '2020-01-01T00:01' and '2020-01-01T00:02'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: (id in [1,2,3] and dateadd('m',-1,ts2) between 1577836860000000 and 1577836920000000)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testDistinctOverWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (event double )");
            execute("insert into test select x from long_sequence(3)");

            String query = "select * from ( SELECT DISTINCT avg(event) OVER (PARTITION BY 1) FROM test )";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Distinct
                              keys: avg
                                CachedWindowLight
                                  unorderedFunctions: [avg(event) over (partition by [1])]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: test
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("avg\n2.0\n");
            assertQuery("select * from ( " + query + " )")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("avg\n2.0\n");
        });
    }

    @Test
    public void testDistinctTsWithLimit1() throws Exception {
        assertQuery("select distinct ts from di order by 1 limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Long Top K lo: 10
                          keys: [ts asc]
                            Async Group By workers: 1
                              keys: [ts]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit2() throws Exception {
        assertQuery("select distinct ts from di order by 1 desc limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Long Top K lo: 10
                          keys: [ts desc]
                            Async Group By workers: 1
                              keys: [ts]
                              filter: null
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit3() throws Exception {
        assertQuery("select distinct ts from di limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [ts]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit4() throws Exception {
        assertQuery("select distinct ts from di limit -10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [ts]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit5a() throws Exception {
        assertQuery("select distinct ts from di where y = 5 limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [ts]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit5b() throws Exception {
        assertQuery("select distinct ts from di where y = 5 limit -10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [ts]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit6a() throws Exception {
        assertQuery("select distinct ts from di where abs(y) = 5 limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [ts]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit6b() throws Exception {
        assertQuery("select distinct ts from di where abs(y) = 5 limit -10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [ts]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctTsWithLimit7() throws Exception {
        assertQuery("select distinct ts from di where abs(y) = 5 limit 10, 20")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit left: 10 right: 20 skip-rows-max: 10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [ts]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit1() throws Exception {
        assertQuery("select distinct x from di order by 1 limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [x]
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit2() throws Exception {
        assertQuery("select distinct x from di order by 1 desc limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [x desc]
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit3() throws Exception {
        assertQuery("select distinct x from di limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit4() throws Exception {
        assertQuery("select distinct x from di limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit5a() throws Exception {
        assertQuery("select distinct x from di where y = 5 limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [x]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit5b() throws Exception {
        assertQuery("select distinct x from di where y = 5 limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [x]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit6a() throws Exception {
        assertQuery("select distinct x from di where abs(y) = 5 limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit6b() throws Exception {
        assertQuery("select distinct x from di where abs(y) = 5 limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testDistinctWithLimit7() throws Exception {
        assertQuery("select distinct x from di where abs(y) = 5 limit 10, 20")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit left: 10 right: 20 skip-rows-max: 10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testExcept() throws Exception {
        assertQuery("select * from a except select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Except
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testExceptAll() throws Exception {
        assertQuery("select * from a except all select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Except All
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testExceptAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Except
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExceptAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Except
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExceptAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Except
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExceptAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts desc]
                                Except
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExplainCreateMatView() throws Exception {
        assertQuery("create materialized view test as (select ts, k, avg(v) from tab sample by 30s) partition by day")
                .ddl("create table tab (ts timestamp, k symbol, v long) timestamp(ts) partition by day wal")
                .assertsPlan("""
                        Create materialized view: test
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  keyFunctions: [timestamp_floor_utc('30s',ts)]
                                  values: [avg(v)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                        """);
    }

    @Test
    public void testExplainCreateTable() throws Exception {
        assertQuery("create table a ( l long, d double)")
                .noLeakCheck()
                .assertsPlan("""
                        Create table: a
                        """);
    }

    @Test
    public void testExplainCreateTableAsSelect() throws Exception {
        assertMemoryLeak(() -> assertQuery("create table a as (select x, 1 from long_sequence(10))")
                .noLeakCheck()
                .assertsPlan("""
                        Create table: a
                            VirtualRecord
                              functions: [x,1]
                                long_sequence count: 10
                        """));
    }

    @Test
    public void testExplainDeferredSingleSymbolFilterPageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tab\s
                    (
                       id symbol index,
                       ts timestamp,
                       val double \s
                    ) timestamp(ts);""");
            execute("insert into tab values ( 'XXX', 0::timestamp, 1 );");

            assertQuery("""
                      select
                       ts,
                        id,\s
                        last(val)
                      from tab
                      where id = 'XXX'\s
                      sample by 15m ALIGN to CALENDAR
                    """)
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                GroupBy vectorized: false
                                  keys: [ts,id]
                                  values: [last(val)]
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: id
                                          filter: id=1
                                        Frame forward scan on: tab
                            """);

        });
    }

    @Test
    public void testExplainInReadOnlymode() throws Exception {
        assertMemoryLeak(() -> {
            // create a table, otherwise EXPLAIN UPDATE and EXPLAIN SELECT fails with "table doesn't exist"
            execute("""
                    CREATE TABLE reference_prices (
                      venue SYMBOL index ,
                      symbol SYMBOL index,
                      instrumentType SYMBOL index,
                      referencePriceType SYMBOL index,
                      ts TIMESTAMP,
                      referencePrice DOUBLE
                    ) timestamp (ts)""");

            try (SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)) {
                // use the read-only security context
                executionContext.with(ReadOnlySecurityContext.INSTANCE);

                assertWritePermissionDenied("""
                        EXPLAIN CREATE TABLE reference_prices (
                          venue SYMBOL index ,
                          symbol SYMBOL index,
                          instrumentType SYMBOL index,
                          referencePriceType SYMBOL index,
                          ts TIMESTAMP,
                          referencePrice DOUBLE
                        ) timestamp (ts)""", executionContext);

                assertNotSupportedByExplain("EXPLAIN DROP TABLE reference_prices", executionContext);

                assertNotSupportedByExplain("EXPLAIN ALTER TABLE reference_prices ADD COLUMN col1 LONG", executionContext);

                assertWritePermissionDenied("EXPLAIN CREATE MATERIALIZED VIEW prices_1h AS (\n" + "SELECT venue, symbol, avg(referencePrice) FROM reference_prices sample by 1h" + ") partition by day", executionContext);

                assertNotSupportedByExplain("EXPLAIN REFRESH MATERIALIZED VIEW prices_1h FULL", executionContext);

                assertWritePermissionDenied("""
                        EXPLAIN INSERT INTO reference_prices VALUES(
                        'venue1', 'sym1', 'FUT', 'Close', now(), 213.456
                        )""", executionContext);

                assertWritePermissionDenied("""
                        EXPLAIN INSERT INTO reference_prices
                        SELECT 'venue1', 'sym1', 'FUT', rnd_symbol('Open', 'Close'), x::timestamp, rnd_double()
                        FROM long_sequence(100)""", executionContext);

                assertWritePermissionDenied("EXPLAIN UPDATE reference_prices\n" + "SET referencePrice = 0 WHERE ts IN '2025-09-20'", executionContext);

                assertWritePermissionDenied("EXPLAIN UPDATE reference_prices\n" + "SET symbol = 'FDXS' WHERE symbol = 'FDAX'", executionContext);

                // SELECT is read-only, it is allowed
                assertReadOnlyOperationAllowed("EXPLAIN SELECT * FROM reference_prices", executionContext);
                assertReadOnlyOperationAllowed("EXPLAIN reference_prices", executionContext);
                assertReadOnlyOperationAllowed("EXPLAIN WITH v AS (select venue from reference_prices) SELECT * FROM v", executionContext);
            }
        });
    }

    @Test
    public void testExplainInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertQuery("insert into a values (1, 2.0)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Insert into table: a
                            """);
        });
    }

    @Test
    public void testExplainInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertQuery("insert into a select x, 1 from long_sequence(10)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Insert into table: a
                                VirtualRecord
                                  functions: [x,1]
                                    long_sequence count: 10
                            """);
        });
    }

    @Test
    public void testExplainPlanNoTrailingQuote() throws Exception {
        assertQuery("explain (format json) select * from long_sequence(1)")
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        QUERY PLAN
                        [
                          {
                            "Plan": {
                                "Node Type": "long_sequence",
                                "count":  1
                            }
                          }
                        ]
                        """);
    }

    @Test
    public void testExplainPlanWithEOLs1() throws Exception {
        assertQuery("select * from a where s = '\b\f\n\r\t\\u0013'")
                .ddl("create table a (s string)")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: s='\\b\\f\\n\\r\\t\\u0013'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testExplainSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertQuery("select * from a")
                    .noLeakCheck()
                    .assertsPlan("""
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExplainSelectWithCte1() throws Exception {
        assertQuery("with b as (select * from a where i = 0)" + "select * from a union all select * from b")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Union All
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Async JIT Filter workers: 1
                              filter: i=0
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testExplainSelectWithCte2() throws Exception {
        assertQuery("with b as (select i from a order by s)" + "select * from a join b on a.i = b.i")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        SelectedRecord
                            Hash Join Light
                              condition: b.i=a.i
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                Hash
                                    SelectedRecord
                                        Encode sort light
                                          keys: [s]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testExplainSelectWithCte3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertQuery("with b as (select * from a limit 10) select * from b")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows: 0 take-rows: 0
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExplainUpdate1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertQuery("update a set l = 1, d=10.1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: a
                                VirtualRecord
                                  functions: [1,10.1]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testExplainUpdate2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l1 long, d1 double)");
            execute("create table b ( l2 long, d2 double)");
            assertQuery("update a set l1 = 1, d1=d2 from b where l1=l2")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: a
                                VirtualRecord
                                  functions: [1,d1]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: l2=l1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testExplainUpdateWalTable() throws Exception {
        // Reproducer for https://github.com/questdb/questdb/issues/6194
        assertQuery("update trades set amount = 0 where ts in '2022-11-11'")
                .ddl("create table trades (" +
                        "symbol symbol, " +
                        "price double, " +
                        "amount int, " +
                        "ts timestamp" +
                        ") timestamp(ts) partition by day WAL")
                .assertsPlan("""
                        Update table: trades
                            VirtualRecord
                              functions: [0]
                                Filter filter: ts in [1668124800000000,1668211199999999]
                                    on: trades
                        """);
    }

    @Test
    public void testExplainUpdateWithFilter() throws Exception {
        allowFunctionMemoization();
        assertQuery("update a set l = 20, d = d+rnd_double() " + "where d < 100.0d and ts > dateadd('d', 1, now()  );")
                .ddl("create table a ( l long, d double, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Update table: a
                            VirtualRecord
                              functions: [20,memoize(d+rnd_double())]
                                Async Filter workers: 1
                                  filter: d<100.0
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: a
                                          intervals: [("1970-01-02T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testExplainWindowFunctionWithCharConstantFrameBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( key int, value double, ts timestamp) timestamp(ts)");

            assertQuery("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '1' MINUTES PRECEDING AND CURRENT ROW) from tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Window
                              functions: [avg(value) over (partition by [key] range between 60000000 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '4' MINUTES PRECEDING AND '3' MINUTES PRECEDING) from tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Window
                              functions: [avg(value) over (partition by [key] range between 240000000 preceding and 180000000 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND '10' MINUTES PRECEDING) from tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Window
                              functions: [avg(value) over (partition by [key] range between unbounded preceding and 600000000 preceding)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testExplainWithJsonFormat1() throws Exception {
        assertQuery("explain (format json) select count (*) from long_sequence(10)")
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        QUERY PLAN
                        [
                          {
                            "Plan": {
                                "Node Type": "Count",
                                "Plans": [
                                {
                                    "Node Type": "long_sequence",
                                    "count":  10
                                } ]
                            }
                          }
                        ]
                        """);
    }

    @Test
    public void testExplainWithJsonFormat2() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);

                String expected = """
                        QUERY PLAN
                        [
                          {
                            "Plan": {
                                "Node Type": "SelectedRecord",
                                "Plans": [
                                {
                                    "Node Type": "Filter",
                                    "filter": "0<a.l+b.l",
                                    "Plans": [
                                    {
                                        "Node Type": "Hash Join",
                                        "condition": "b.l=a.l",
                                        "Plans": [
                                        {
                                            "Node Type": "PageFrame",
                                            "Plans": [
                                            {
                                                "Node Type": "Row forward scan"
                                            },
                                            {
                                                "Node Type": "Frame forward scan",
                                                "on": "a"
                                            } ]
                                        },
                                        {
                                            "Node Type": "Hash",
                                            "Plans": [
                                            {
                                                "Node Type": "Async JIT Filter",
                                                "workers":  1,
                                                "limit":  4,
                                                "filter": "10<l",
                                                "Plans": [
                                                {
                                                    "Node Type": "PageFrame",
                                                    "Plans": [
                                                    {
                                                        "Node Type": "Row forward scan"
                                                    },
                                                    {
                                                        "Node Type": "Frame forward scan",
                                                        "on": "a"
                                                    } ]
                                                } ]
                                        } ]
                                    } ]
                                } ]
                            }
                          }
                        ]
                        """;

                if (!JitUtil.isJitSupported()) {
                    expected = expected.replace("JIT ", "");
                }

                execute("create table a ( l long)");
                assertQuery("explain (format json) select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ")
                        .noLeakCheck()
                        .withCompiler(compiler)
                        .withContext(sqlExecutionContext)
                        .noRandomAccess()
                        .expectSize()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testExplainWithJsonFormat3() throws Exception {
        assertQuery("explain (format json) select d, max(i) from (select * from a union select * from a)")
                .ddl("create table a ( i int, d double)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        QUERY PLAN
                        [
                          {
                            "Plan": {
                                "Node Type": "GroupBy",
                                "vectorized":  false,
                                "keys": "[d]",
                                "values": "[max(i)]",
                                "Plans": [
                                {
                                    "Node Type": "Union",
                                    "Plans": [
                                    {
                                        "Node Type": "PageFrame",
                                        "Plans": [
                                        {
                                            "Node Type": "Row forward scan"
                                        },
                                        {
                                            "Node Type": "Frame forward scan",
                                            "on": "a"
                                        } ]
                                    },
                                    {
                                        "Node Type": "PageFrame",
                                        "Plans": [
                                        {
                                            "Node Type": "Row forward scan"
                                        },
                                        {
                                            "Node Type": "Frame forward scan",
                                            "on": "a"
                                        } ]
                                    } ]
                                } ]
                            }
                          }
                        ]
                        """);
    }

    @Test
    public void testExplainWithJsonFormat4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            assertQuery(" explain (format json) select * from taba left join tabb on a1=b1  or a2=b2")
                    .noLeakCheck()
                    .ddl(null)
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            QUERY PLAN
                            [
                              {
                                "Plan": {
                                    "Node Type": "SelectedRecord",
                                    "Plans": [
                                    {
                                        "Node Type": "Nested Loop Left Join",
                                        "filter": "(taba.a1=tabb.b1 or taba.a2=tabb.b2)",
                                        "Plans": [
                                        {
                                            "Node Type": "PageFrame",
                                            "Plans": [
                                            {
                                                "Node Type": "Row forward scan"
                                            },
                                            {
                                                "Node Type": "Frame forward scan",
                                                "on": "taba"
                                            } ]
                                        },
                                        {
                                            "Node Type": "PageFrame",
                                            "Plans": [
                                            {
                                                "Node Type": "Row forward scan"
                                            },
                                            {
                                                "Node Type": "Frame forward scan",
                                                "on": "tabb"
                                            } ]
                                        } ]
                                    } ]
                                }
                              }
                            ]
                            """);
        });
    }

    @Test
    public void testExplainWithQueryInParentheses1() throws Exception {
        assertMemoryLeak(() -> assertQuery("(select 1)")
                .noLeakCheck()
                .assertsPlan("""
                        VirtualRecord
                          functions: [1]
                            long_sequence count: 1
                        """));
    }

    @Test
    public void testExplainWithQueryInParentheses2() throws Exception {
        assertQuery("(select * from x)")
                .ddl("create table x ( i int)")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: x
                        """);
    }

    @Test
    public void testExplainWithQueryInParentheses3() throws Exception {
        assertQuery("((select * from x))")
                .ddl("create table x ( i int)")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: x
                        """);
    }

    @Test
    public void testExplainWithQueryInParentheses4() throws Exception {
        assertQuery("((x))")
                .ddl("create table x ( i int)")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: x
                        """);
    }

    @Test
    public void testExplainWithQueryInParentheses5() throws Exception {
        assertQuery("((select last(timestamp) as x, last(price) as btcusd " + "from trades " + "where symbol = 'BTC-USD' " + "and timestamp > dateadd('m', -30, now())) " + "timestamp(x))")
                .ddl("""
                        CREATE TABLE trades (
                          symbol SYMBOL,
                          side SYMBOL,
                          price DOUBLE,
                          amount DOUBLE,
                          timestamp TIMESTAMP
                        ) timestamp (timestamp) PARTITION BY DAY""")
                .assertsPlan("""
                        SelectedRecord
                            Async JIT Group By workers: 1
                              vectorized: false
                              values: [last(timestamp),last(price)]
                              filter: symbol='BTC-USD'
                                PageFrame
                                    Row forward scan
                                    Interval forward scan on: trades
                                      intervals: [("1969-12-31T23:30:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testFilterOnExcludedIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("drop table if exists trips");
            execute("CREATE TABLE trips (l long, s symbol index capacity 5, ts TIMESTAMP) " + "timestamp(ts) partition by month");

            assertQuery("select s, count() from trips where s is not null order by count desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [count desc]
                                GroupBy vectorized: false
                                  keys: [s]
                                  values: [count(*)]
                                    FilterOnExcludedValues symbolOrder: desc
                                      symbolFilter: s not in [null]
                                        Cursor-order scan
                                        Frame forward scan on: trips
                            """);

            execute("insert into trips " + "  select x, 'A' || ( x%3000 )," + "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " + "  from long_sequence(10000);");
            execute("insert into trips " + "  select x, null," + "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " + "  from long_sequence(4000);");

            assertQuery("select s, count() from trips where s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: s is not null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null and s != 'A1000'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null and s!='A1000')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            assertQuery("select s, count() from trips where s is not null and s != :s1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null and s!=:s1::string)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null and l != 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (l!=0 and s is not null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null or l != 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null or l!=0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l != 0 and s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (l!=0 and s is not null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l != 0 or s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (l!=0 or s is not null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l > 100 or l != 0 and s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (100<l or (l!=0 and s is not null))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', 'A2000')")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (100<l or (l!=0 and not (s in [null,A1000,A2000])))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");

            assertQuery("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (100<l or (l!=0 and not (s in [null,A1000] or s in [:s1::string])))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);
        });
    }

    @Test
    public void testFilterOnExcludedNonIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("drop table if exists trips");
            execute("CREATE TABLE trips(l long, s symbol capacity 5, ts TIMESTAMP) " + "timestamp(ts) partition by month");

            assertQuery("select s, count() from trips where s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: s is not null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            execute("insert into trips " + "  select x, 'A' || ( x%3000 )," + "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " + "  from long_sequence(10000);");
            execute("insert into trips " + "  select x, null," + "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " + "  from long_sequence(4000);");

            assertQuery("select s, count() from trips where s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: s is not null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null and s != 'A1000'")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null and s!='A1000')
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            assertQuery("select s, count() from trips where s is not null and s != :s1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null and s!=:s1::string)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null and l != 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null and l!=0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where s is not null or l != 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (s is not null or l!=0)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l != 0 and s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (l!=0 and s is not null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l != 0 or s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (l!=0 or s is not null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            assertQuery("select s, count() from trips where l > 100 or l != 0 and s is not null")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (100<l or (l!=0 and s is not null))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");

            assertQuery("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Group By workers: 1
                              keys: [s]
                              values: [count(*)]
                              filter: (100<l or (l!=0 and not (s in [null,A1000] or s in [:s1::string])))
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trips
                            """);
        });
    }

    @Test
    public void testFiltersOnIndexedSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE reference_prices (
                      venue SYMBOL index ,
                      symbol SYMBOL index,
                      instrumentType SYMBOL index,
                      referencePriceType SYMBOL index,
                      resolutionType SYMBOL ,
                      ts TIMESTAMP,
                      referencePrice DOUBLE
                    ) timestamp (ts)""");

            execute("""
                    insert into reference_prices\s
                    select rnd_symbol('VENUE1', 'VENUE2', 'VENUE3'),\s
                              'symbol',\s
                              'instrumentType',\s
                              rnd_symbol('TYPE1', 'TYPE2'),\s
                              'resolutionType',\s
                              cast(x as timestamp),\s
                              rnd_double()
                    from long_sequence(10000)""");

            String query1 = "select referencePriceType,count(*) " + "from reference_prices " + "where referencePriceType not in ('TYPE1') " + "and venue in ('VENUE1', 'VENUE2')";
            String expectedResult = """
                    referencePriceType\tcount
                    TYPE2\t3344
                    """;
            String expectedPlan = """
                    GroupBy vectorized: false
                      keys: [referencePriceType]
                      values: [count(*)]
                        FilterOnValues symbolOrder: desc
                            Cursor-order scan
                                Index forward scan on: venue
                                  filter: venue=3 and not (referencePriceType in [TYPE1])
                                Index forward scan on: venue
                                  filter: venue=1 and not (referencePriceType in [TYPE1])
                            Frame forward scan on: reference_prices
                    """;

            assertQuery(query1)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan);
            assertQuery(query1)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedResult);

            String query2 = """
                    select referencePriceType, count(*)\s
                    from reference_prices\s
                    where venue in ('VENUE1', 'VENUE2')\s
                    and referencePriceType not in ('TYPE1')""";

            assertQuery(query2)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan);
            assertQuery(query2)
                    .noLeakCheck()
                    .expectSize()
                    .returns(expectedResult);
        });
    }

    @Test
    public void testFullFatHashJoin0() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                execute("create table a (l long)");
                assertQuery("select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    Filter filter: 0<a.l+b.l
                                        Hash Join
                                          condition: b.l=a.l
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                                            Hash
                                                Async JIT Filter workers: 1
                                                  limit: 4
                                                  filter: 10<l
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                """);
            }
        });
    }

    @Test
    public void testFullFatHashJoin1() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                execute("create table a ( l long)");
                assertQuery("select * from a join (select l from a limit 40) on l")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    Hash Join
                                      condition: _xQdbA1.l=a.l
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        Hash
                                            Limit value: 40 skip-rows: 0 take-rows: 0
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: a
                                """);
            }
        });
    }

    @Test
    public void testFullFatHashJoin2() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                execute("create table a (l long)");
                String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
                String[] joinFactoryTypes = {"Hash Left Outer Join", "Hash Right Outer Join", "Hash Full Outer Join"};

                for (int i = 0; i < joinTypes.length; i++) {
                    String joinType = joinTypes[i];
                    String factoryType = joinFactoryTypes[i];
                    assertQuery("select * from a " + joinType + " join a a1 on l")
                            .withCompiler(compiler)
                            .noLeakCheck()
                            .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: a1.l=a.l\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: a\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: a\n");
                }
            }
        });
    }

    @Test // FIXME:  abs(a2+1) = abs(b2) should be applied as left join filter  !
    public void testFullJoinWithEqualityAndExpressionsAhdWhere1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) " + "where a1+10 < b1 - 10")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    Filter filter: taba.a1+10<tabb.b1-10\n" + "        " + factoryType + "\n" + "          condition: b2=a2 and b1=a1\n" + "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testFullJoinWithEqualityAndExpressionsAhdWhere3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1 and abs(a2+1) = abs(b2) where a2=b2")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    Filter filter: taba.a2=tabb.b2\n" + "        " + factoryType + "\n" + "          condition: b1=a1\n" + "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testFunctions() throws Exception {
        assertMemoryLeak(() -> {
            // test table for show_columns
            execute("create table bbb (a int)");

            final StringSink sink = new StringSink();

            IntObjHashMap<ObjList<Function>> constFuncs = new IntObjHashMap<>();
            constFuncs.put(ColumnType.BOOLEAN, list(BooleanConstant.TRUE, BooleanConstant.FALSE));
            constFuncs.put(ColumnType.BYTE, list(new ByteConstant((byte) 1)));
            constFuncs.put(ColumnType.SHORT, list(new ShortConstant((short) 2)));
            constFuncs.put(ColumnType.CHAR, list(new CharConstant('a')));
            constFuncs.put(ColumnType.INT, list(new IntConstant(3)));
            constFuncs.put(ColumnType.IPv4, list(new IPv4Constant(3)));
            constFuncs.put(ColumnType.LONG, list(new LongConstant(4)));
            constFuncs.put(ColumnType.DATE, list(new DateConstant(0)));
            constFuncs.put(ColumnType.TIMESTAMP_MICRO, list(new TimestampConstant(86400000000L, ColumnType.TIMESTAMP_MICRO)));
            constFuncs.put(ColumnType.TIMESTAMP_NANO, list(new TimestampConstant(86400000000000L, ColumnType.TIMESTAMP_NANO)));
            constFuncs.put(ColumnType.FLOAT, list(new FloatConstant(5f)));
            constFuncs.put(ColumnType.DOUBLE, list(new DoubleConstant(1))); // has to be [0.0, 1.0] for approx_percentile
            constFuncs.put(ColumnType.STRING, list(new StrConstant("bbb"), new StrConstant("1"), new StrConstant("1.1.1.1"), new StrConstant("1.1.1.1/24")));
            constFuncs.put(ColumnType.VARCHAR, list(new VarcharConstant("bbb"), new VarcharConstant("1"), new VarcharConstant("1.1.1.1"), new VarcharConstant("1.1.1.1/24")));
            constFuncs.put(ColumnType.SYMBOL, list(new SymbolConstant("symbol", 0)));
            constFuncs.put(ColumnType.LONG256, list(new Long256Constant(0, 1, 2, 3)));
            constFuncs.put(ColumnType.GEOBYTE, list(new GeoByteConstant((byte) 1, ColumnType.getGeoHashTypeWithBits(5))));
            constFuncs.put(ColumnType.GEOSHORT, list(new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(10))));
            constFuncs.put(ColumnType.GEOINT, list(new GeoIntConstant(1, ColumnType.getGeoHashTypeWithBits(20))));
            constFuncs.put(ColumnType.GEOLONG, list(new GeoLongConstant(1, ColumnType.getGeoHashTypeWithBits(35))));
            constFuncs.put(ColumnType.GEOHASH, list(new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(15))));
            constFuncs.put(ColumnType.BINARY, list(new NullBinConstant()));
            constFuncs.put(ColumnType.LONG128, list(new Long128Constant(0, 1)));
            constFuncs.put(ColumnType.UUID, list(new UuidConstant(0, 1)));
            constFuncs.put(ColumnType.NULL, list(NullConstant.NULL));
            constFuncs.put(ColumnType.INTERVAL_RAW, list(IntervalConstant.RAW_NULL));
            constFuncs.put(ColumnType.INTERVAL_TIMESTAMP_NANO, list(IntervalConstant.TIMESTAMP_NANO_NULL));
            constFuncs.put(ColumnType.INTERVAL_TIMESTAMP_MICRO, list(IntervalConstant.TIMESTAMP_MICRO_NULL));
            constFuncs.put(ColumnType.ARRAY_STRING, list(new StringToStringArrayFunction(0, "{all}")));

            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("bbb", ColumnType.INT));
            constFuncs.put(ColumnType.RECORD, list(new RecordColumn(0, metadata)));

            GenericRecordMetadata cursorMetadata = new GenericRecordMetadata();
            cursorMetadata.add(new TableColumnMetadata("s", ColumnType.STRING));
            constFuncs.put(ColumnType.CURSOR, list(new CursorFunction(new EmptyTableRecordCursorFactory(cursorMetadata) {
                public boolean supportsPageFrameCursor() {
                    return true;
                }
            })));

            IntObjHashMap<Function> colFuncs = new IntObjHashMap<>();
            colFuncs.put(ColumnType.BOOLEAN, BooleanColumn.newInstance(1));
            colFuncs.put(ColumnType.BYTE, ByteColumn.newInstance(1));
            colFuncs.put(ColumnType.SHORT, ShortColumn.newInstance(2));
            colFuncs.put(ColumnType.CHAR, new CharColumn(1));
            colFuncs.put(ColumnType.INT, IntColumn.newInstance(1));
            colFuncs.put(ColumnType.IPv4, new IPv4Column(1));
            colFuncs.put(ColumnType.LONG, LongColumn.newInstance(1));
            colFuncs.put(ColumnType.DATE, DateColumn.newInstance(1));
            colFuncs.put(ColumnType.TIMESTAMP_MICRO, TimestampColumn.newInstance(1, ColumnType.TIMESTAMP_MICRO));
            colFuncs.put(ColumnType.TIMESTAMP_NANO, TimestampColumn.newInstance(1, ColumnType.TIMESTAMP_NANO));
            colFuncs.put(ColumnType.FLOAT, FloatColumn.newInstance(1));
            colFuncs.put(ColumnType.DOUBLE, DoubleColumn.newInstance(1));
            colFuncs.put(ColumnType.STRING, new StrColumn(1));
            colFuncs.put(ColumnType.VARCHAR, new VarcharColumn(1));
            colFuncs.put(ColumnType.SYMBOL, new SymbolColumn(1, true));
            colFuncs.put(ColumnType.LONG256, Long256Column.newInstance(1));
            colFuncs.put(ColumnType.GEOBYTE, GeoByteColumn.newInstance(1, ColumnType.getGeoHashTypeWithBits(5)));
            colFuncs.put(ColumnType.GEOSHORT, GeoShortColumn.newInstance(1, ColumnType.getGeoHashTypeWithBits(10)));
            colFuncs.put(ColumnType.GEOINT, GeoIntColumn.newInstance(1, ColumnType.getGeoHashTypeWithBits(20)));
            colFuncs.put(ColumnType.GEOLONG, GeoLongColumn.newInstance(1, ColumnType.getGeoHashTypeWithBits(35)));
            colFuncs.put(ColumnType.GEOHASH, GeoShortColumn.newInstance((short) 1, ColumnType.getGeoHashTypeWithBits(15)));
            colFuncs.put(ColumnType.BINARY, BinColumn.newInstance(1));
            colFuncs.put(ColumnType.LONG128, Long128Column.newInstance(1));
            colFuncs.put(ColumnType.UUID, UuidColumn.newInstance(1));
            colFuncs.put(ColumnType.ARRAY, new ArrayColumn(1, ColumnType.encodeArrayType(ColumnType.DOUBLE, 2)));
            colFuncs.put(ColumnType.INTERVAL_RAW, IntervalColumn.newInstance(1, ColumnType.INTERVAL_RAW));
            colFuncs.put(ColumnType.INTERVAL_TIMESTAMP_MICRO, IntervalColumn.newInstance(1, ColumnType.INTERVAL_TIMESTAMP_MICRO));
            colFuncs.put(ColumnType.INTERVAL_TIMESTAMP_NANO, IntervalColumn.newInstance(1, ColumnType.INTERVAL_TIMESTAMP_NANO));

            PlanSink planSink = new TextPlanSink() {
                @Override
                public PlanSink putColumnName(int columnIndex) {
                    val("column(").val(columnIndex).val(")");
                    return this;
                }
            };

            PlanSink tmpPlanSink = new TextPlanSink() {
                @Override
                public PlanSink putColumnName(int columnIndex) {
                    val("column(").val(columnIndex).val(")");
                    return this;
                }
            };

            ObjList<Function> args = new ObjList<>();
            IntList argPositions = new IntList();

            FunctionFactoryCache cache = engine.getFunctionFactoryCache();
            LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = cache.getFactories();
            factories.forEach((key, value) -> {
                FUNCTIONS:
                for (int i = 0, n = value.size(); i < n; i++) {
                    long memUsedBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ND_ARRAY);

                    planSink.clear();

                    FunctionFactoryDescriptor descriptor = value.get(i);
                    FunctionFactory factory = descriptor.getFactory();
                    if (factory instanceof ReadParquetFunctionFactory) {
                        continue;
                    }
                    int sigArgCount = descriptor.getSigArgCount();

                    sink.clear();
                    sink.put(factory.getSignature()).put(" types: ");

                    for (int p = 0; p < sigArgCount; p++) {
                        int typeWithFlags = descriptor.getArgTypeWithFlags(p);
                        final short sigArgType = FunctionFactoryDescriptor.toTypeTag(typeWithFlags);
                        boolean isArray = FunctionFactoryDescriptor.isArray(typeWithFlags);

                        if (p > 0) {
                            sink.put(',');
                        }
                        sink.put(ColumnType.nameOf(sigArgType));
                        if (isArray) {
                            sink.put("[]");
                        }
                    }
                    sink.put(" -> ");

                    int combinations = 1;

                    for (int p = 0; p < sigArgCount; p++) {
                        int typeWithFlags = descriptor.getArgTypeWithFlags(p);
                        boolean isConstant = FunctionFactoryDescriptor.isConstant(typeWithFlags);
                        short sigArgType = FunctionFactoryDescriptor.toTypeTag(typeWithFlags);
                        ObjList<Function> availableValues = constFuncs.get(sigArgType);
                        int constValues = availableValues != null ? availableValues.size() : 1;
                        combinations *= (constValues + (isConstant ? 0 : 1));
                    }

                    boolean goodArgsFound = false;
                    for (int no = 0; no < combinations; no++) {
                        args.clear();
                        argPositions.clear();
                        planSink.clear();

                        int tempNo = no;

                        try {
                            for (int p = 0; p < sigArgCount; p++) {
                                int typeWithFlags = descriptor.getArgTypeWithFlags(p);
                                short sigArgType = FunctionFactoryDescriptor.toTypeTag(typeWithFlags);
                                boolean isConstant = FunctionFactoryDescriptor.isConstant(typeWithFlags);
                                boolean isArray = FunctionFactoryDescriptor.isArray(typeWithFlags);
                                boolean useConst = isConstant || (tempNo & 1) == 1 || sigArgType == ColumnType.CURSOR || sigArgType == ColumnType.RECORD;
                                boolean isVarArg = sigArgType == ColumnType.VAR_ARG;

                                if (isVarArg) {
                                    if (factory instanceof LongSequenceFunctionFactory) {
                                        sigArgType = ColumnType.LONG;
                                    } else if (factory instanceof InCharFunctionFactory) {
                                        sigArgType = ColumnType.CHAR;
                                    } else if (factory instanceof InTimestampTimestampFunctionFactory) {
                                        sigArgType = ColumnType.TIMESTAMP;
                                    } else if (factory instanceof InDoubleFunctionFactory) {
                                        sigArgType = ColumnType.DOUBLE;
                                    } else if (factory instanceof LevelTwoPriceFunctionFactory) {
                                        sigArgType = ColumnType.DOUBLE;
                                    } else if (factory instanceof LagDoubleFunctionFactory || factory instanceof LeadDoubleFunctionFactory || factory instanceof LagLongFunctionFactory || factory instanceof LeadLongFunctionFactory || factory instanceof LagTimestampFunctionFactory || factory instanceof LeadTimestampFunctionFactory || factory instanceof LagDateFunctionFactory || factory instanceof LeadDateFunctionFactory) {
                                        sigArgType = ColumnType.INT;
                                        useConst = true;
                                    } else if (factory instanceof ArrayCreateFunctionFactory) {
                                        sigArgType = ColumnType.DOUBLE;
                                    } else if (factory instanceof DoubleArrayAccessFunctionFactory) {
                                        sigArgType = ColumnType.INT;
                                    } else if (factory instanceof RndDoubleArrayFunctionFactory) {
                                        sigArgType = ColumnType.INT;
                                        useConst = true;
                                    } else if (factory instanceof WithinGeohashFunctionFactory) {
                                        sigArgType = ColumnType.GEOBYTE;
                                    } else {
                                        sigArgType = ColumnType.STRING;
                                    }
                                }

                                if (factory instanceof LevelTwoPriceArrayFunctionFactory) {
                                    args.add(new DoubleConstant(2.0));
                                    args.add(new ArrayConstant(new double[]{1.0}));
                                    args.add(new ArrayConstant(new double[]{1.0}));
                                    break;
                                } else if (isArray && sigArgType == ColumnType.DOUBLE) {
                                    if (p == 1 && factory.getSignature().startsWith("cast(")) {
                                        args.add(new ArrayTypeConstant(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2)));
                                    } else {
                                        args.add(new ArrayConstant(new double[][]{{1}, {1}}));
                                    }
                                } else if (factory instanceof SwitchFunctionFactory) {
                                    args.add(new IntConstant(1));
                                    args.add(new IntConstant(2));
                                    args.add(new StrConstant("a"));
                                    args.add(new StrConstant("b"));
                                } else if (factory instanceof EqIntervalFunctionFactory) {
                                    args.add(IntervalConstant.RAW_NULL);
                                } else if (factory instanceof CoalesceFunctionFactory) {
                                    args.add(FloatColumn.newInstance(1));
                                    args.add(FloatColumn.newInstance(2));
                                    args.add(new FloatConstant(12f));
                                } else if (factory instanceof ExtractFromTimestampFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("day"));
                                } else if (factory instanceof RndSymbolListFunctionFactory) {
                                    args.add(new StrConstant("a"));
                                    args.add(new StrConstant("b"));
                                    args.add(new StrConstant("c"));
                                    args.add(new StrConstant("d"));
                                } else if (factory instanceof TimestampCeilFunctionFactory) {
                                    args.add(new StrConstant("d"));
                                } else if (sigArgType == ColumnType.STRING && isArray) {
                                    args.add(new StringToStringArrayFunction(0, "{'test'}"));
                                } else if (factory instanceof EqTimestampCursorFunctionFactory) {
                                    // 2nd arg for this function is a cursor, which is unclear how to test here
                                    // additionally, this function has separate tests
                                    continue FUNCTIONS;
                                } else if (factory instanceof ToTimezoneTimestampFunctionFactory && p == 1) {
                                    args.add(new StrConstant("CET"));
                                } else if (factory instanceof CastStrToRegClassFunctionFactory && useConst) {
                                    args.add(new StrConstant("pg_namespace"));
                                } else if (factory instanceof CastStrToStrArrayFunctionFactory) {
                                    args.add(new StrConstant("{'abc'}"));
                                } else if (factory instanceof TestSumXDoubleGroupByFunctionFactory && p == 1) {
                                    args.add(new StrConstant("123.456"));
                                } else if ((factory instanceof TimestampFloorFunctionFactory || factory instanceof TimestampFloorFromFunctionFactory || factory instanceof TimestampFloorFromOffsetFunctionFactory) && p == 0) {
                                    args.add(new StrConstant("d"));
                                } else if (factory instanceof TimestampFloorFromOffsetFunctionFactory && p == 3) {
                                    args.add(new StrConstant("00:30"));
                                    args.add(new StrConstant("UTC"));
                                } else if (factory instanceof DateTruncFunctionFactory && p == 0) {
                                    args.add(new StrConstant("year"));
                                } else if (factory instanceof ToUTCTimestampFunctionFactory && p == 1) {
                                    args.add(new StrConstant("CEST"));
                                } else if (factory instanceof TimestampAddFunctionFactory && p == 0) {
                                    args.add(new CharConstant('s'));
                                } else if (factory instanceof EqIntStrCFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("1"));
                                } else if (isLong256StrFactory(factory) && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9652"));
                                } else if (isIPv4StrFactory(factory) && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("10.8.6.5"));
                                } else if (factory instanceof ContainsIPv4StrFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("12.6.5.10/24"));
                                } else if (factory instanceof ContainsEqIPv4StrFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("12.6.5.10/24"));
                                } else if (factory instanceof NegContainsEqIPv4StrFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("34.56.22.11/12"));
                                } else if (factory instanceof NegContainsIPv4StrFunctionFactory && sigArgType == ColumnType.STRING) {
                                    args.add(new StrConstant("32.12.22.11/12"));
                                } else if (factory instanceof RndIPv4CCFunctionFactory) {
                                    args.add(new StrConstant("4.12.22.11/12"));
                                    args.add(new IntConstant(2));
                                } else if (isEqSymTimestampFactory(factory)) {
                                    continue FUNCTIONS;
                                } else if (factory instanceof InUuidFunctionFactory && p == 1) {
                                    // this factory requires valid UUID string, otherwise it will fail
                                    args.add(new StrConstant("11111111-1111-1111-1111-111111111111"));
                                } else if (factory instanceof GreatestNumericFunctionFactory) {
                                    args.add(new DoubleConstant(1.5));
                                    args.add(new DoubleConstant(3.2));
                                } else if (factory instanceof LeastNumericFunctionFactory) {
                                    args.add(new DoubleConstant(1.5));
                                    args.add(new DoubleConstant(3.2));
                                } else if (factory instanceof JsonExtractTypedFunctionFactory) {
                                    if (p == 0) {
                                        args.add(new VarcharConstant("{\"a\": 1}"));
                                        args.add(new VarcharConstant(".a"));
                                        args.add(new IntConstant(ColumnType.INT));
                                    }
                                } else if (factory instanceof HydrateTableMetadataFunctionFactory) {
                                    args.add(new StrConstant("*"));
                                } else if (factory instanceof InTimestampIntervalFunctionFactory) {
                                    args.add(new TimestampConstant(123141, ColumnType.TIMESTAMP_MICRO));
                                    args.add(new IntervalConstant(1231, 123146, ColumnType.INTERVAL_TIMESTAMP_MICRO));
                                } else if (Chars.equals(key, "approx_count_distinct") && sigArgCount == 2 && p == 1 && sigArgType == ColumnType.INT) {
                                    args.add(new IntConstant(4)); // precision has to be in the range of 4 to 18
                                } else if (!useConst) {
                                    args.add(colFuncs.get(sigArgType));
                                } else if (factory instanceof WalTransactionsFunctionFactory && sigArgType == ColumnType.STRING) {
                                    // Skip it, it requires a WAL table to exist
                                    break FUNCTIONS;
                                } else if (factory instanceof GlobFilesFunctionFactory) {
                                    args.add(new StrConstant("/tmp/*"));
                                } else {
                                    args.add(getConst(constFuncs, sigArgType, p, no));
                                }

                                if (!isConstant) {
                                    tempNo >>= 1;
                                }
                            }

                            argPositions.setAll(args.size(), 0);

                            // l2price requires an odd number of arguments
                            if (factory instanceof LevelTwoPriceFunctionFactory) {
                                if (args.size() % 2 == 0) {
                                    args.add(new DoubleConstant(1234));
                                }
                            }

                            // TODO: test with partition by, order by and various frame modes
                            if (factory.isWindow()) {
                                sqlExecutionContext.configureWindowContext(null, null, null, false, PageFrameRecordCursorFactory.SCAN_DIRECTION_FORWARD, -1, true, WindowExpression.FRAMING_RANGE, Long.MIN_VALUE, (char) 0, 10, 0, (char) 0, 20, WindowExpression.EXCLUDE_NO_OTHERS, 0, -1, ColumnType.NULL, false, 0);
                            }
                            Function function = null;
                            try {
                                try {
                                    function = factory.newInstance(0, args, argPositions, engine.getConfiguration(), sqlExecutionContext);
                                    function.toPlan(planSink);
                                } catch (Throwable th) {
                                    Misc.freeObjListAndClear(args);
                                } finally {
                                    sqlExecutionContext.clearWindowContext();
                                }

                                goodArgsFound = true;

                                assertFalse("function " + factory.getSignature() + " should serialize to text properly. current text: " + planSink.getSink(), Chars.contains(planSink.getSink(), "io.questdb"));
                                LOG.info().$safe(sink).$safe(planSink.getSink()).$();

                                if (function instanceof NegatableBooleanFunction && !((NegatableBooleanFunction) function).isNegated()) {
                                    ((NegatableBooleanFunction) function).setNegated();
                                    tmpPlanSink.clear();
                                    function.toPlan(tmpPlanSink);

                                    if (Chars.equals(planSink.getSink(), tmpPlanSink.getSink())) {
                                        throw new AssertionError("Same output generated regardless of " + "negatable flag! Factory: " + factory.getSignature() + " " + function);
                                    }

                                    assertFalse("function " + factory.getSignature() + " should serialize to text properly", Chars.contains(tmpPlanSink.getSink(), "io.questdb"));
                                }

                                if (function instanceof GroupByFunction) {
                                    assertFalse("group by function " + factory.getSignature() + " should not be marked as constant", function.isConstant());
                                }
                            } finally {
                                Misc.free(function);

                                long memUsedAfter = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ND_ARRAY);
                                if (memUsedAfter > memUsedBefore) {
                                    LOG.error().$("Memory leak detected in ").$safe(factory.getSignature()).$();
                                    fail("Memory leak detected in " + factory.getSignature());
                                }
                            }
                        } catch (Exception t) {
                            LOG.info().$(t).$();
                        }
                    }

                    if (!goodArgsFound) {
                        throw new RuntimeException("No good set of values found for " + factory);
                    }
                }
            });
        });
    }

    @Test
    public void testGroupByBoolean() throws Exception {
        assertQuery("select b, min(l)  from a group by b")
                .ddl("create table a (l long, b boolean)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [b]
                          values: [min(l)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByBooleanFunction() throws Exception {
        assertQuery("select b1||b2, min(l) from a group by b1||b2")
                .ddl("create table a (l long, b1 boolean, b2 boolean)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [concat]
                          keyFunctions: [concat([b1,b2])]
                          values: [min(l)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByBooleanWithFilter() throws Exception {
        assertQuery("select b, min(l)  from a where b = true group by b")
                .ddl("create table a (l long, b boolean)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [b]
                          values: [min(l)]
                          filter: b=true
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByDouble() throws Exception {
        assertQuery("select d, min(l) from a group by d")
                .ddl("create table a (l long, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [d]
                          values: [min(l)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByFloat() throws Exception {
        assertQuery("select f, min(l) from a group by f")
                .ddl("create table a (l long, f float)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [f]
                          values: [min(l)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // special case
    public void testGroupByHour() throws Exception {
        assertQuery("select hour(ts), min(d) from a group by hour(ts)")
                .ddl("create table a (ts timestamp, d double)")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [ts]
                          values: [min(d)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByHourAndFilterIsParallel() throws Exception {
        assertQuery("select hour(ts), min(d) from a where d > 0 group by hour(ts)")
                .ddl("create table a (ts timestamp, d double)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [hour]
                          keyFunctions: [hour(ts)]
                          values: [min(d)]
                          filter: 0<d
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByHourNonTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, d double)");
            assertQuery("select hour(d), min(d) from a")
                    .fails(12, "argument type mismatch for function `hour` at #1 expected: TIMESTAMP, actual: DOUBLE");
        });
    }

    @Test
    public void testGroupByHourUnorderedColumns() throws Exception {
        assertQuery("select min(d), hour(ts) from a group by hour(ts)")
                .ddl("create table a (ts timestamp, d double)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [min,hour]
                            GroupBy vectorized: true workers: 1
                              keys: [ts]
                              values: [min(d)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByInt1() throws Exception {
        assertQuery("select min(d), i from a group by i")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [min,i]
                            GroupBy vectorized: true workers: 1
                              keys: [i]
                              values: [min(d)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test // repeated group by keys get merged at group by level
    public void testGroupByInt2() throws Exception {
        assertQuery("select i, i, min(d) from a group by i, i")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [i,i,min]
                            GroupBy vectorized: true workers: 1
                              keys: [i]
                              values: [min(d)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByInt3() throws Exception {
        assertQuery("select i, max(l) - min(l) delta from a group by i")
                .ddl("create table a (i int, l long)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [i,max-min]
                            GroupBy vectorized: true workers: 1
                              keys: [i]
                              values: [min(l),max(l)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByIntOperation() throws Exception {
        assertQuery("select min(d), i * 42 from a group by i")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [min,i*42]
                            GroupBy vectorized: true workers: 1
                              keys: [i]
                              values: [min(d)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByKeyedAliased() throws Exception {
        assertQuery("select s as symbol, count() from a")
                .ddl("create table a (s symbol, ts timestamp) timestamp(ts) partition by year;")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByKeyedNoAlias() throws Exception {
        assertQuery("select s, count() from a")
                .ddl("create table a (s symbol, ts timestamp) timestamp(ts) partition by year;")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByKeyedOnExcept() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, d double)");

            assertQuery("select d, max(i) from (select * from a except select * from b)")
                    .ddl("create table b ( j int, e double)")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              keys: [d]
                              values: [max(i)]
                                Except
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testGroupByKeyedOnIntersect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, d double)");

            assertQuery("select d, max(i) from (select * from a intersect select * from b)")
                    .ddl("create table b ( j int, e double)")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              keys: [d]
                              values: [max(i)]
                                Intersect
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testGroupByKeyedOnUnion() throws Exception {
        assertQuery("select d, max(i) from (select * from a union select * from a)")
                .ddl("create table a ( i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          keys: [d]
                          values: [max(i)]
                            Union
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByKeyedOnUnionAll() throws Exception {
        assertQuery("select d, max(i) from (select * from a union all select * from a)")
                .ddl("create table a ( i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          keys: [d]
                          values: [max(i)]
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByLong() throws Exception {
        assertQuery("select l, min(d) from a group by l")
                .ddl("create table a ( l long, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [l]
                          values: [min(d)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions1() throws Exception {
        assertQuery("select i from a group by i")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [i]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions2() throws Exception {
        assertQuery("select i from a where d < 42 group by i")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [i]
                          filter: d<42
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions3() throws Exception {
        assertQuery("select i from a group by i")
                .ddl("create table a (i short, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [i]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions4() throws Exception {
        assertQuery("select i, j from a group by i, j")
                .ddl("create table a (i long, j long)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [i,j]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions5() throws Exception {
        assertQuery("select i, j from a where d > 42 group by i, j")
                .ddl("create table a (i long, j long, d double)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [i,j]
                          filter: 42<d
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions6() throws Exception {
        assertQuery("select s from a group by s")
                .ddl("create table a (s symbol)")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions7() throws Exception {
        assertQuery("select s from a where d = 42 group by s")
                .ddl("create table a (s symbol, d double)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [s]
                          filter: d=42
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions8() throws Exception {
        assertQuery("select s from a group by s")
                .ddl("create table a (s string)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [s]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNoFunctions9() throws Exception {
        assertQuery("select s from a where s like '%foobar%' group by s")
                .ddl("create table a (s string)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [s]
                          filter: s like %foobar%
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed1() throws Exception {
        assertQuery("select min(d) from a")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [min(d)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed10() throws Exception {
        assertQuery("select max(i) from (select * from a join a b on i )")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          values: [max(i)]
                            SelectedRecord
                                Hash Join Light
                                  condition: b.i=a.i
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed11() throws Exception {
        assertQuery("select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a")
                .ddl("create table a (gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b))")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [first(gb),last(gb),first(gs),last(gs),first(gi),last(gi),first(gl),last(gl)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed12() throws Exception {
        assertQuery("select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a where i > 42")
                .ddl("create table a (gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b), i int)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          vectorized: false
                          values: [first(gb),last(gb),first(gs),last(gs),first(gi),last(gi),first(gl),last(gl)]
                          filter: 42<i
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed13() throws Exception {
        assertQuery("select max(i) - min(i) from a")
                .ddl("create table a (i int)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [max-min]
                            Async Group By workers: 1
                              vectorized: true
                              values: [min(i),max(i)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed2() throws Exception {
        assertQuery("select min(d), max(d*d) from a")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [min(d),max(d*d)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed3() throws Exception {
        assertQuery("select max(d+1) from a")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: false
                          values: [max(d+1)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed4() throws Exception {
        assertQuery("select count(*), max(i), min(d) from a")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [count(*),max(i),min(d)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByNotKeyed5() throws Exception {
        assertQuery("select first(10), last(d), avg(10), min(10), max(10) from a")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [first(10),last(d),avg(10),min(10),max(10)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // group by on filtered data is not vectorized
    public void testGroupByNotKeyed6() throws Exception {
        assertQuery("select max(i) from a where i < 10")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          vectorized: false
                          values: [max(i)]
                          filter: i<10
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // order by is ignored and grouped by - vectorized
    public void testGroupByNotKeyed7() throws Exception {
        assertQuery("select max(i) from (select * from a order by d)")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [max(i)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // order by can't be ignored; group by is not vectorized
    public void testGroupByNotKeyed8() throws Exception {
        assertQuery("select max(i) from (select * from a order by d limit 10)")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          values: [max(i)]
                            Async Top K lo: 10 workers: 1
                              filter: null
                              keys: [d]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test // TODO: group by could be vectorized for union tables and result merged
    public void testGroupByNotKeyed9() throws Exception {
        assertQuery("select max(i) from (select * from a union all select * from a)")
                .ddl("create table a (i int, d double)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          values: [max(i)]
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByStringFunction() throws Exception {
        assertQuery("select s1||s2 s, avg(l) a from a")
                .ddl("create table a (l long, s1 string, s2 string)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [s]
                          keyFunctions: [concat([s1,s2])]
                          values: [avg(l)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByStringFunctionWithFilter() throws Exception {
        assertQuery("select s1||s2 s, avg(l) a from a where l > 42")
                .ddl("create table a (l long, s1 string, s2 string)")
                .assertsPlan("""
                        Async JIT Group By workers: 1
                          keys: [s]
                          keyFunctions: [concat([s1,s2])]
                          values: [avg(l)]
                          filter: 42<l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupBySymbol() throws Exception {
        assertQuery("select s, avg(l) a from a")
                .ddl("create table a (l long, s symbol)")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [avg(l)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupBySymbol2() throws Exception {
        assertQuery("select s, max(l) - min(l) a from a")
                .ddl("create table a (l long, s symbol)")
                .assertsPlan("""
                        VirtualRecord
                          functions: [s,max-min]
                            GroupBy vectorized: true workers: 1
                              keys: [s]
                              values: [min(l),max(l)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupBySymbolFunction() throws Exception {
        assertQuery("select s::symbol, avg(l) a from a")
                .ddl("create table a (l long, s string)")
                .assertsPlan("""
                        GroupBy vectorized: false
                          keys: [cast]
                          values: [avg(l)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupBySymbolWithSubQueryFilter() throws Exception {
        assertQuery("select s, avg(l) a from a where s in (select s from a where s = 'key')")
                .ddl("create table a (l long, s symbol)")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [s]
                          values: [avg(l)]
                          filter: s in cursor\s
                            Async JIT Filter workers: 1
                              filter: s='key'
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a [state-shared]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testGroupByWithLimit1() throws Exception {
        assertQuery("select x, count(*) from di group by x order by 1 limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [x]
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit10() throws Exception {
        assertQuery("select y, count(*) from di order by y desc limit 1")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Long Top K lo: 1
                          keys: [y desc]
                            Async Group By workers: 1
                              keys: [y]
                              values: [count(*)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit11() throws Exception {
        assertQuery("select y, count(*) c from di order by c limit 42")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Long Top K lo: 42
                          keys: [c asc]
                            Async Group By workers: 1
                              keys: [y]
                              values: [count(*)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit12() throws Exception {
        sqlExecutionContext.setParallelGroupByEnabled(false);
        try {
            assertQuery("select y, count(*) c from di order by c limit 42")
                    .ddl("create table di (x int, y long)")
                    .assertsPlan("""
                            Long Top K lo: 42
                              keys: [c asc]
                                GroupBy vectorized: false
                                  keys: [y]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: di
                            """);
        } finally {
            sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelGroupByEnabled());
        }
    }

    @Test
    public void testGroupByWithLimit13() throws Exception {
        assertQuery("select y, 42, count(*) c from di order by c limit 42")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Long Top K lo: 42
                          keys: [c asc]
                            VirtualRecord
                              functions: [y,42,c]
                                Async Group By workers: 1
                                  keys: [y]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit14() throws Exception {
        assertQuery("select y, c from (select y, z, count(*) c from di) order by c limit 42")
                .ddl("create table di (x int, y long, z double)")
                .assertsPlan("""
                        Long Top K lo: 42
                          keys: [c asc]
                            SelectedRecord
                                Async Group By workers: 1
                                  keys: [y,z]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit15() throws Exception {
        assertQuery("select y, c from (select ts, y, count(*) c from di) order by ts limit 13")
                .ddl("create table di (y long, ts timestamp)")
                .assertsPlan("""
                        SelectedRecord
                            Long Top K lo: 13
                              keys: [ts asc]
                                Async Group By workers: 1
                                  keys: [y,ts]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit16() throws Exception {
        assertQuery("select ts, 42, count(*) c from di order by ts limit 2")
                .ddl("create table di (ts timestamp)")
                .assertsPlan("""
                        Long Top K lo: 2
                          keys: [ts asc]
                            VirtualRecord
                              functions: [ts,42,c]
                                Async Group By workers: 1
                                  keys: [ts]
                                  values: [count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit17() throws Exception {
        assertQuery("select i, count(*) c from di order by c limit 2")
                .ddl("create table di (i int)")
                .assertsPlan("""
                        Long Top K lo: 2
                          keys: [c asc]
                            GroupBy vectorized: true workers: 1
                              keys: [i]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit2() throws Exception {
        assertQuery("select x, count(*) from di group by x order by 1 desc limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [x desc]
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit3() throws Exception {
        assertQuery("select x, count(*) from di group by x limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit4() throws Exception {
        assertQuery("select x, count(*) from di group by x limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            GroupBy vectorized: true workers: 1
                              keys: [x]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit5a() throws Exception {
        assertQuery("select x, count(*) from di where y = 5 group by x limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [x]
                              values: [count(*)]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit5b() throws Exception {
        assertQuery("select x, count(*) from di where y = 5 group by x limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async JIT Group By workers: 1
                              keys: [x]
                              values: [count(*)]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit6a() throws Exception {
        assertQuery("select x, count(*) from di where abs(y) = 5 group by x limit 10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              values: [count(*)]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit6b() throws Exception {
        assertQuery("select x, count(*) from di where abs(y) = 5 group by x limit -10")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit value: -10 skip-rows: baseRows-10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              values: [count(*)]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit7() throws Exception {
        assertQuery("select x, count(*) from di where abs(y) = 5 group by x limit 10, 20")
                .ddl("create table di (x int, y long)")
                .assertsPlan("""
                        Limit left: 10 right: 20 skip-rows-max: 10 take-rows-max: 10
                            Async Group By workers: 1
                              keys: [x]
                              values: [count(*)]
                              filter: abs(y)=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testGroupByWithLimit8() throws Exception {
        assertQuery("select ts, count(*) from di where y = 5 group by ts order by ts desc limit 10")
                .ddl("create table di (x int, y long, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Long Top K lo: 10
                          keys: [ts desc]
                            Async JIT Group By workers: 1
                              keys: [ts]
                              values: [count(*)]
                              filter: y=5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: di
                        """);
    }

    @Test
    public void testHashInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s1 string)");
            execute("create table b ( i int, s2 string)");

            assertQuery("select s1, s2 from (select a.s1, b.s2, b.i, a.i  from a join b on i) where i < i1 and s1 = s2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: (b.i<a.i and a.s1=b.s2)
                                    Hash Join Light
                                      condition: b.i=a.i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: b
                            """);
        });
    }

    @Test // inner hash join maintains order metadata and can be part of asof join
    public void testHashInnerJoinWithAsOf() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, ts1 timestamp) timestamp(ts1)");
            execute("create table tabb (b1 int, b2 long)");
            execute("create table tabc (c1 int, c2 long, ts3 timestamp) timestamp(ts3)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertQuery("select * " + "from taba " + "inner join tabb on a1=b1 " + "asof join tabc on b1=c1")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    AsOf Join
                                      condition: c1=b1
                                        Hash Join
                                          condition: b1=a1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tabc
                                """);
            }
        });
    }

    @Test
    public void testHashOuterJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int)");
            execute("create table b ( i int)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from a " + joinType + " join b on i")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b.i=a.i\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: a\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: b\n");
            }
        });
    }

    @Test
    public void testHashOuterJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int)");
            execute("create table b ( i int)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from a " + joinType + " join b on i where b.i is not null")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    Filter filter: b.i!=null\n" + "        " + factoryType + "\n" + "          condition: b.i=a.i\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: a\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: b\n");
            }
        });
    }

    @Ignore
    //FIXME
    //@Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testImplicitJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i1 int)");
            execute("create table b ( i2 int)");

            assertQuery("select * from a, b where a.i1 = b.i2")
                    .noLeakCheck()
                    .returns("");

            assertQuery("select * from a , b where a.i1 = b.i2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Cross Join
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testInUuid() throws Exception {
        assertQuery("select u, ts from a where u in ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')")
                .ddl("create table a (u uuid, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: u in ['33333333-3333-3333-3333-333333333333','11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222']
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testInnerJoinFilterOnSecondTablePushedThroughView() throws Exception {
        // A subquery wraps an INNER join. The WHERE filters by the SECOND (slave) table's join key.
        // The predicate reaches the slave scan (tb: index scan, filter bkey='x'), but the first table
        // stays a full scan - a constant pinned to the slave side is not propagated back to the
        // master. This matches the plan of the equivalent inline query: the optimiser behaves the
        // same with or without the enclosing view.
        assertQuery("""
                SELECT * FROM (
                  SELECT a.akey AS ka, b.bkey AS kb, b.bv
                  FROM ta a JOIN tb b ON b.bkey = a.akey
                ) WHERE kb = 'x'""")
                .ddl("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)", "CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)")
                .assertsPlan("""
                        SelectedRecord
                            Hash Join Light
                              condition: b.bkey=a.akey
                              symbolKeyJoin: true
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: ta
                                Hash
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: bkey deferred: true
                                          filter: bkey='x'
                                        Frame forward scan on: tb
                        """);
    }

    @Test
    public void testInnerJoinFilterPushedThroughView() throws Exception {
        // Same fix as the LEFT-join case, for an INNER join wrapped in a subquery. The WHERE filters
        // by the FIRST (master) table's key (aliased as k). The optimiser pushes it into the master
        // scan (ta) and derives the transitive filter for the second table (tb), so both sides become
        // keyed lookups instead of full scans.
        assertQuery("""
                SELECT * FROM (
                  SELECT a.akey AS k, a.av, b.bv
                  FROM ta a JOIN tb b ON b.bkey = a.akey
                ) WHERE k = 'x'""")
                .ddl("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)", "CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)")
                .assertsPlan("""
                        SelectedRecord
                            Hash Join Light
                              condition: b.bkey=a.akey
                              symbolKeyJoin: true
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: akey deferred: true
                                      filter: akey='x'
                                    Frame forward scan on: ta
                                Hash
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: bkey deferred: true
                                          filter: bkey='x'
                                        Frame forward scan on: tb
                        """);
    }

    @Test
    public void testInnerJoinFilterPushedThroughViewReturnsCorrectRows() throws Exception {
        // Row-level guard for testInnerJoinFilterPushedThroughView with distinct master/slave tables.
        // withPlanContaining is what proves the optimisation: a plan that forgot to derive the slave
        // filter would still return the right rows (the post-join WHERE k='x' filters correctly either
        // way), so the bkey fragment guards the transitive push, while .returns confirms exactly the
        // surviving master row is kept and the 'y' row excluded. With data present, 'x' resolves to
        // symbol key 1, so both filters render by resolved key (akey=1/bkey=1) rather than the
        // deferred ='x' form the no-data plan-only sibling shows - exercising the resolved-key path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'ax'), ('y', 'ay')");
            execute("INSERT INTO tb VALUES ('x', 'bx'), ('y', 'by')");
            assertQuery("""
                    SELECT * FROM (
                      SELECT a.akey AS k, a.av, b.bv
                      FROM ta a JOIN tb b ON b.bkey = a.akey
                    ) WHERE k = 'x'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("filter: akey=1", "filter: bkey=1")
                    .returns("""
                            k\tav\tbv
                            x\tax\tbx
                            """);
        });
    }

    @Test
    public void testIntersect1() throws Exception {
        assertQuery("select * from a intersect select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Intersect
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testIntersect2() throws Exception {
        assertQuery("select * from a intersect select * from a where i > 0")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Intersect
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Hash
                                Async JIT Filter workers: 1
                                  filter: 0<i
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testIntersectAll() throws Exception {
        assertQuery("select * from a intersect all select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Intersect All
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testIntersectAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Intersect
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testIntersectAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Intersect
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testIntersectAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Intersect
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testIntersectAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts desc]
                                Intersect
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testKSumNSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( k long, x double );");

            assertQuery("SELECT k, ksum(x), nsum(x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              keys: [k]
                              values: [ksum(x),nsum(x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testLatestByAllSymbolsFilteredFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table maps
                    (
                      timestamp timestamp,\s
                      cluster symbol,\s
                      alias symbol,\s
                      octets int,\s
                      packets int
                    ) timestamp(timestamp);""");

            execute("insert into maps values ('2023-09-01T09:41:00.000Z', 'cluster10', 'a', 1, 1), " + "('2023-09-01T09:42:00.000Z', 'cluster10', 'a', 2, 2)");

            String sql = """
                    select timestamp, cluster, alias, timestamp - timestamp1 interval, (octets-octets1)*8 bits, packets-packets1 packets from (
                      (select timestamp, cluster, alias, octets, packets
                      from maps
                      where cluster in ('cluster10') and timestamp BETWEEN '2023-09-01T09:40:27.286Z' AND '2023-09-01T10:40:27.286Z'
                      latest on timestamp partition by cluster,alias)
                      lt join maps on (cluster,alias)
                      ) order by bits desc
                    limit 10""";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                Encode sort
                                  keys: [bits desc]
                                    VirtualRecord
                                      functions: [timestamp,cluster,alias,timestamp-timestamp1,octets-octets1*8,packets-packets1]
                                        SelectedRecord
                                            Lt Join Light
                                              condition: maps.cluster=_xQdbA3.cluster and maps.alias=_xQdbA3.alias
                                                LatestByAllSymbolsFiltered
                                                  filter: cluster in [cluster10]
                                                    Row backward scan
                                                      expectedSymbolsCount: 2147483647
                                                    Interval backward scan on: maps
                                                      intervals: [("2023-09-01T09:40:27.286000Z","2023-09-01T10:40:27.286000Z")]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: maps
                            """);

            assertQuery(sql)
                    .noLeakCheck()
                    .returns("""
                            timestamp\tcluster\talias\tinterval\tbits\tpackets
                            2023-09-01T09:42:00.000000Z\tcluster10\ta\t60000000\t8\t1
                            """);
        });
    }

    @Test
    public void testLatestByRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as ( " + "  select " + "    rnd_str('a','b','c') s, " + "    timestamp_sequence(0, 100000000) ts " + "   from long_sequence(100)" + ") timestamp(ts) partition by hour");

            String sql = "with yy as (select ts, max(s) s from tab sample by 1h ALIGN TO FIRST OBSERVATION) " + "select * from yy latest on ts partition by s limit 10";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                LatestBy
                                    Sample By
                                      fill: none
                                      values: [max(s)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                            """);

            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .timestamp("ts")
                    .returns("""
                            ts\ts
                            1970-01-01T02:00:00.000000Z\tc
                            """);
        });
    }

    @Test
    public void testLatestOn0() throws Exception {
        assertQuery("select i from a latest on ts partition by i")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllFiltered
                            Row backward scan
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn0a() throws Exception {
        assertQuery("select i from (select * from a where i = 10 union select * from a where i =20) latest on ts partition by i")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        SelectedRecord
                            LatestBy
                                Union
                                    Async JIT Filter workers: 1
                                      filter: i=10
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    Async JIT Filter workers: 1
                                      filter: i=20
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testLatestOn0b() throws Exception {
        assertQuery("select ts,i from a where s in ('ABC') and i > 0 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        SelectedRecord
                            LatestByValueDeferredFiltered
                              filter: 0<i
                              symbolFilter: s='ABC'
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn0c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertQuery("select ts,i from a where s in ('a1') and i > 0 latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                LatestByValueFiltered
                                    Row backward scan
                                      symbolFilter: s=0
                                      filter: 0<i
                                    Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn0d() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertQuery("select ts,i from a where s in ('a1') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                LatestByValueFiltered
                                    Row backward scan
                                      symbolFilter: s=0
                                    Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn0e() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertQuery("select ts,i, s from a where s in ('a1') and i > 0 latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Index backward scan on: s
                              filter: 0<i
                              symbolFilter: s=1
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn1() throws Exception {
        assertQuery("select * from a latest on ts partition by i")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllFiltered
                            Row backward scan
                            Frame backward scan on: a
                        """);
    }

    @Test // TODO: should use index
    public void testLatestOn10() throws Exception {
        assertQuery("select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByDeferredListValuesFiltered
                          filter: (s='S1' or s='S2')
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn11() throws Exception {
        assertQuery("select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s")
                .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByDeferredListValuesFiltered
                          includedSymbols: ['S1','S2']
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn12() throws Exception {
        assertQuery("select s, i, ts from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestBySubQuery
                            Subquery
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Row backward scan on: s
                              filter: length(s)=2
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn12a() throws Exception {
        assertQuery("select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s")
                .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestBySubQuery
                            Subquery
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Row backward scan on: s
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn13() throws Exception {
        assertQuery("select i, ts, s from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestBySubQuery
                            Subquery
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Index backward scan on: s
                              filter: length(s)=2
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn13a() throws Exception {
        assertQuery("select i, ts, s from a where s in (select distinct s from a) latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestBySubQuery
                            Subquery
                                GroupBy vectorized: true workers: 1
                                  keys: [s]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Index backward scan on: s
                            Frame backward scan on: a
                        """);
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn14() throws Exception {
        assertQuery("select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2")
                .ddl("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllSymbolsFiltered
                          filter: (s1 in [S1,S2] and s2='S3' and 0<i)
                            Row backward scan
                              expectedSymbolsCount: 2
                            Frame backward scan on: a
                        """);
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn15() throws Exception {
        assertQuery("select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' latest on ts partition by s1,s2")
                .ddl("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllSymbolsFiltered
                          filter: (s1 in [S1,S2] and s2='S3')
                            Row backward scan
                              expectedSymbolsCount: 2
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn16() throws Exception {
        assertQuery("select s1, s2, i, ts from a where s1 = 'S1' and ts > 0::timestamp latest on ts partition by s1,s2")
                .ddl("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllSymbolsFiltered
                          filter: s1='S1'
                            Row backward scan
                              expectedSymbolsCount: 2147483647
                            Interval backward scan on: a
                              intervals: [("1970-01-01T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testLatestOn1a() throws Exception {
        assertQuery("select * from (select ts, i as i1, i as i2 from a ) where 0 < i1 and i2 < 10 latest on ts partition by i1")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestBy light order_by_timestamp: true
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  filter: (0<i and i<10)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testLatestOn1b() throws Exception {
        assertQuery("select ts, i as i1, i as i2 from a where 0 < i and i < 10 latest on ts partition by i")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        SelectedRecord
                            LatestByAllFiltered
                                Row backward scan
                                  filter: (0<i and i<10)
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn2() throws Exception {
        assertQuery("select ts, d from a latest on ts partition by i")
                .ddl("create table a ( i int, d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        SelectedRecord
                            LatestByAllFiltered
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn3() throws Exception {
        assertQuery("select * from a latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        LatestByAllIndexed
                            Async index backward scan on: s workers: 2
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn4() throws Exception {
        assertQuery("select s, i, ts from a where s  = 'S1' latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Index backward scan on: s deferred: true
                              filter: s='S1'
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn5a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertQuery("select s, i, ts from a where s  in ('def1', 'def2') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Index backward scan on: s
                              symbolFilter: s in ['def1','def2']
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn5b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertQuery("select s, i, ts from a where s  in ('1', 'deferred') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Index backward scan on: s
                              symbolFilter: s in [1] or s in ['deferred']
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn5c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertQuery("select s, i, ts from a where s  in ('1', '2') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Index backward scan on: s
                              symbolFilter: s in [1,2]
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLatestOn6() throws Exception {
        assertQuery("select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Index backward scan on: s
                          filter: 0<i
                          symbolFilter: s in ['S1','S2']
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn7() throws Exception {
        assertQuery("select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Index backward scan on: s
                          filter: length(s)<10
                          symbolFilter: s in ['S1','S2']
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn8() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertQuery("select s, i, ts from a where s  in ('s1') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            PageFrame
                                Index backward scan on: s
                                  filter: s=1
                                Frame backward scan on: a
                            """);
        });
    }

    @Test // key outside list of symbols
    public void testLatestOn8a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertQuery("select s, i, ts from a where s in ('bogus_key') latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            PageFrame
                                Index backward scan on: s deferred: true
                                  filter: s='bogus_key'
                                Frame backward scan on: a
                            """);
        });
    }

    @Test // columns in order different to table's
    public void testLatestOn9() throws Exception {
        assertQuery("select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Index backward scan on: s
                          filter: length(s)=10
                          symbolFilter: s='S1'
                            Frame backward scan on: a
                        """);
    }

    @Test // columns in table's order
    public void testLatestOn9a() throws Exception {
        assertQuery("select i, s, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Index backward scan on: s
                          filter: length(s)=10
                          symbolFilter: s='S1'
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testLatestOn9b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x::int, 'S' || x, x::timestamp from long_sequence(10)");

            assertQuery("select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Index backward scan on: s
                              filter: length(s)=10
                              symbolFilter: s=1
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterByAliasPushedThroughView() throws Exception {
        // Exercises alias rewriting on both sides: the join condition uses table aliases (a, b) and
        // the WHERE filters by a column alias (k), which the subquery projects from a.akey. The
        // optimiser resolves k back to a.akey, pushes it into the master scan (ta) and derives the
        // transitive filter on the slave (tb), so both sides become keyed lookups.
        assertQuery("""
                SELECT * FROM (
                  SELECT a.akey AS k, a.av, b.bv
                  FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                ) WHERE k = 'x'""")
                .ddl("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)", "CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)")
                .assertsPlan("""
                        SelectedRecord
                            Hash Left Outer Join Light
                              condition: b.bkey=a.akey
                              symbolKeyJoin: true
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: akey deferred: true
                                      filter: akey='x'
                                    Frame forward scan on: ta
                                Hash
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: bkey deferred: true
                                          filter: bkey='x'
                                        Frame forward scan on: tb
                        """);
    }

    @Test
    public void testLeftJoinFilterByBindVariablePushedThroughView() throws Exception {
        // The pushed-down predicate is a bind variable, not a literal constant. It must propagate the
        // same way: into the master scan (ta) and transitively onto the slave (tb), both rendered as
        // "filter: <key>=:kp::string". This confirms bind parameters reach the slave scans too.
        bindVariableService.clear();
        bindVariableService.setStr("kp", "x");
        assertQuery("""
                SELECT * FROM (
                  SELECT a.akey AS k, a.av, b.bv
                  FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                ) WHERE k = :kp""")
                .ddl("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)", "CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)")
                .assertsPlan("""
                        SelectedRecord
                            Hash Left Outer Join Light
                              condition: b.bkey=a.akey
                              symbolKeyJoin: true
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: akey deferred: true
                                      filter: akey=:kp::string
                                    Frame forward scan on: ta
                                Hash
                                    DeferredSingleSymbolFilterPageFrame
                                        Index forward scan on: bkey deferred: true
                                          filter: bkey=:kp::string
                                        Frame forward scan on: tb
                        """);
    }

    @Test
    public void testLeftJoinFilterByBindVariablePushedThroughViewReturnsCorrectRows() throws Exception {
        // Executes the bind-variable pushdown (the plan-only sibling is
        // testLeftJoinFilterByBindVariablePushedThroughView). The derived slave filter shares the bind
        // ExpressionNode by reference with the master predicate - one node feeds both scans - so this
        // proves both scans actually read the bound value at execution. The re-bind to 'y' then proves
        // the shared node re-reads rather than freezing on the first bound value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'ax'), ('y', 'ay')");
            execute("INSERT INTO tb VALUES ('x', 'bx'), ('y', 'by')");
            String query = """
                    SELECT * FROM (
                      SELECT a.akey AS k, a.av, b.bv
                      FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                    ) WHERE k = :kp""";
            bindVariableService.clear();
            bindVariableService.setStr("kp", "x");
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("filter: akey=:kp::string", "filter: bkey=:kp::string")
                    .returns("""
                            k\tav\tbv
                            x\tax\tbx
                            """);
            // re-bind: the shared const node must re-read, not return the stale 'x' row
            bindVariableService.setStr("kp", "y");
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tav\tbv
                            y\tay\tby
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterOnSecondTableStaysPostJoinThroughView() throws Exception {
        // A subquery wraps a LEFT join. The WHERE filters by the SECOND (slave) table's join key.
        // Unlike the INNER case, a WHERE predicate on a left-joined slave column must run AFTER the
        // join (it rejects the NULL-extended rows), so it stays a post-join "Filter" over the whole
        // join and is not pushed into either scan. This matches the equivalent inline query and is
        // intentionally left unchanged by the master-side pushdown.
        assertQuery("""
                SELECT * FROM (
                  SELECT a.akey AS ka, b.bkey AS kb, b.bv
                  FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                ) WHERE kb = 'x'""")
                .ddl("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)", "CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: b.bkey='x'
                                Hash Left Outer Join Light
                                  condition: b.bkey=a.akey
                                  symbolKeyJoin: true
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: ta
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tb
                        """);
    }

    @Test
    public void testLeftJoinFilterPushedThroughView() throws Exception {
        // A view encapsulates two LEFT JOINs onto 'events'. A consumer filters the view by the
        // master-table key (entity_id, which maps to entity_terminal.id). The filter reaches the
        // master scan (entity_terminal: Index forward scan, filter: id='x') AND is propagated across
        // the equi-join keys (events.entity_id = e.id) into both left-joined 'events' scans as index
        // lookups ("Index forward scan on: entity_id ... filter: entity_id='x'"), so the query does a
        // keyed lookup instead of reading every event row.
        // The resulting plan is identical to testLeftJoinFilterPushedWhenInlined, where the same
        // predicate is written directly in the join's query model rather than through a view.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE entity_terminal (
                      id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      status SYMBOL,
                      last_event_id LONG
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");
            execute("""
                    CREATE TABLE events (
                      entity_id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      event_type SYMBOL,
                      id LONG,
                      payload STRING
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");
            execute("""
                    CREATE VIEW entity_stats AS (
                      SELECT
                        e.id AS entity_id,
                        e.created_at,
                        e.status,
                        related_event.created_at AS terminal_at,
                        decision_event.payload AS decision_payload
                      FROM entity_terminal e
                      LEFT JOIN events decision_event
                        ON decision_event.entity_id = e.id
                        AND decision_event.created_at = e.created_at
                        AND decision_event.event_type = 'decision'
                      LEFT JOIN events related_event
                        ON related_event.entity_id = e.id
                        AND related_event.id = e.last_event_id
                        AND related_event.event_type = 'terminal'
                    )""");

            assertQuery("SELECT * FROM entity_stats WHERE entity_id = 'x'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Left Outer Join Light
                                  condition: related_event.id=e.last_event_id and related_event.entity_id=e.id
                                  symbolKeyJoin: true
                                  filter: related_event.event_type='terminal'
                                    Hash Left Outer Join Light
                                      condition: decision_event.created_at=e.created_at and decision_event.entity_id=e.id
                                      symbolKeyJoin: true
                                      filter: decision_event.event_type='decision'
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: id deferred: true
                                              filter: id='x'
                                            Frame forward scan on: entity_terminal
                                        Hash
                                            DeferredSingleSymbolFilterPageFrame
                                                Index forward scan on: entity_id deferred: true
                                                  filter: entity_id='x'
                                                Frame forward scan on: events
                                    Hash
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: entity_id deferred: true
                                              filter: entity_id='x'
                                            Frame forward scan on: events
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterPushedThroughViewReturnsCorrectRows() throws Exception {
        // Guards that pushing the filter into the left-joined 'events' scans (see
        // testLeftJoinFilterPushedThroughView) preserves LEFT JOIN semantics: every master row that
        // matches the filter survives, matched slave rows attach, and unmatched slaves stay NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE entity_terminal (
                      id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      status SYMBOL,
                      last_event_id LONG
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");
            execute("""
                    CREATE TABLE events (
                      entity_id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      event_type SYMBOL,
                      id LONG,
                      payload STRING
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");
            execute("""
                    CREATE VIEW entity_stats AS (
                      SELECT
                        e.id AS entity_id,
                        e.created_at,
                        e.status,
                        related_event.created_at AS terminal_at,
                        decision_event.payload AS decision_payload
                      FROM entity_terminal e
                      LEFT JOIN events decision_event
                        ON decision_event.entity_id = e.id
                        AND decision_event.created_at = e.created_at
                        AND decision_event.event_type = 'decision'
                      LEFT JOIN events related_event
                        ON related_event.entity_id = e.id
                        AND related_event.id = e.last_event_id
                        AND related_event.event_type = 'terminal'
                    )""");
            drainWalAndViewQueues();
            // two 'x' master rows (one fully matched, one with no matching terminal) plus a 'y' row
            // that the filter must exclude
            execute("""
                    INSERT INTO entity_terminal VALUES
                      ('x', '2024-01-01T00:00:00.000000Z', 'open', 100),
                      ('x', '2024-01-05T00:00:00.000000Z', 'reopened', 555),
                      ('y', '2024-01-02T00:00:00.000000Z', 'closed', 200)""");
            execute("""
                    INSERT INTO events VALUES
                      ('x', '2024-01-01T00:00:00.000000Z', 'decision', 1, 'dpx'),
                      ('x', '2024-01-03T00:00:00.000000Z', 'terminal', 100, 'tp'),
                      ('x', '2024-01-05T00:00:00.000000Z', 'decision', 7, 'dp5'),
                      ('x', '2024-01-09T00:00:00.000000Z', 'terminal', 999, 'tpw'),
                      ('y', '2024-01-02T00:00:00.000000Z', 'decision', 5, 'dpy')""");
            drainWalQueue();

            // row 1: decision matches (created_at=t1) and terminal matches (id=100 -> terminal_at=t3)
            // row 2: decision matches (created_at=t5) but no terminal with id=555 -> terminal_at NULL
            assertQuery("SELECT * FROM entity_stats WHERE entity_id = 'x' ORDER BY created_at")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .noRandomAccess()
                    .returns("""
                            entity_id\tcreated_at\tstatus\tterminal_at\tdecision_payload
                            x\t2024-01-01T00:00:00.000000Z\topen\t2024-01-03T00:00:00.000000Z\tdpx
                            x\t2024-01-05T00:00:00.000000Z\treopened\t\tdp5
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterPushedWhenInlined() throws Exception {
        // Contrast to testLeftJoinFilterPushedThroughView. With the same joins written inline -
        // so the WHERE filter lives in the same query model as the joins - the optimiser derives
        // events.entity_id='x' transitively from e.id='x' and the equi-join keys, and pushes it
        // into both left-joined 'events' scans as an index lookup
        // ("Index forward scan on: entity_id ... filter: entity_id='x'").
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE entity_terminal (
                      id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      status SYMBOL,
                      last_event_id LONG
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");
            execute("""
                    CREATE TABLE events (
                      entity_id SYMBOL INDEX,
                      created_at TIMESTAMP,
                      event_type SYMBOL,
                      id LONG,
                      payload STRING
                    ) TIMESTAMP(created_at) PARTITION BY DAY""");

            assertQuery("""
                    SELECT
                      e.id AS entity_id,
                      e.created_at,
                      e.status,
                      related_event.created_at AS terminal_at,
                      decision_event.payload AS decision_payload
                    FROM entity_terminal e
                    LEFT JOIN events decision_event
                      ON decision_event.entity_id = e.id
                      AND decision_event.created_at = e.created_at
                      AND decision_event.event_type = 'decision'
                    LEFT JOIN events related_event
                      ON related_event.entity_id = e.id
                      AND related_event.id = e.last_event_id
                      AND related_event.event_type = 'terminal'
                    WHERE e.id = 'x'""")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Left Outer Join Light
                                  condition: related_event.id=e.last_event_id and related_event.entity_id=e.id
                                  symbolKeyJoin: true
                                  filter: related_event.event_type='terminal'
                                    Hash Left Outer Join Light
                                      condition: decision_event.created_at=e.created_at and decision_event.entity_id=e.id
                                      symbolKeyJoin: true
                                      filter: decision_event.event_type='decision'
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: id deferred: true
                                              filter: id='x'
                                            Frame forward scan on: entity_terminal
                                        Hash
                                            DeferredSingleSymbolFilterPageFrame
                                                Index forward scan on: entity_id deferred: true
                                                  filter: entity_id='x'
                                                Frame forward scan on: events
                                    Hash
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: entity_id deferred: true
                                              filter: entity_id='x'
                                            Frame forward scan on: events
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterStacksWithSubQueryInternalWhere() throws Exception {
        // The subquery already carries its own internal WHERE (a.av='keep'), processed during
        // optimiseJoins. The consumer then adds WHERE k='x', which moveWhereInsideSubQueries pushes
        // into the same nested model. The pushed predicate must AND with the pre-existing internal
        // filter (concatFilters), not clobber it, while still deriving bkey='x' on the slave. Only the
        // ('x','keep') master row survives both filters, and it attaches the matching tb row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'keep'), ('x', 'drop'), ('y', 'keep')");
            execute("INSERT INTO tb VALUES ('x', 'bx'), ('y', 'by')");
            assertQuery("""
                    SELECT * FROM (
                      SELECT a.akey AS k, a.av, b.bv
                      FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                      WHERE a.av = 'keep'
                    ) WHERE k = 'x'""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            k\tav\tbv
                            x\tkeep\tbx
                            """);
        });
    }

    @Test
    public void testLeftJoinFilterWithNullConstantThroughView() throws Exception {
        // A NULL constant filter on the master key returns no rows ("col = NULL" is never true). The
        // subquery-wrapped form behaves exactly like the inline query: "k = NULL" is folded to a
        // deferred symbol filter (rendered as "=0") and pushed across the equi-join key onto the slave
        // scan just as it is inline, and both produce an empty result. Even with a NULL-extended slave,
        // the join condition (slave.key = master.key) cannot match when the master key is NULL, so the
        // pushdown is lossless. Guards the NULL-constant pushdown path against regressions.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ta (akey SYMBOL INDEX, av STRING)");
            execute("CREATE TABLE tb (bkey SYMBOL INDEX, bv STRING)");
            execute("INSERT INTO ta VALUES ('x', 'ax'), ('y', 'ay')");
            execute("INSERT INTO tb VALUES ('x', 'bx'), ('y', 'by')");
            assertQuery("""
                    SELECT * FROM (
                      SELECT a.akey AS k, a.av, b.bv
                      FROM ta a LEFT JOIN tb b ON b.bkey = a.akey
                    ) WHERE k = NULL""")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("k\tav\tbv\n");
        });
    }

    @Test // FIXME: there should be no separate filter
    public void testLeftJoinWithEquality3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            assertQuery("select * from taba left join tabb on a1=b1  or a2=b2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: taba
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tabb
                            """);
        });
    }

    @Test // FIXME: join and where clause filters should be separated
    public void testLeftJoinWithEquality4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            assertQuery("select * from taba left join tabb on a1=b1  or a2=b2 where a1 > b2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: tabb.b2<taba.a1
                                    Nested Loop Left Join
                                      filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: taba
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tabb
                            """);
        });
    }

    @Test
    public void testLeftJoinWithEquality7() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testHashAndAsOfJoin(compiler, true, true);
            }
        });
    }

    @Test
    public void testLeftJoinWithEquality8() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                testHashAndAsOfJoin(compiler, false, false);
            }
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            assertQuery("select * from taba left join tabb on abs(a2+1) = abs(b2)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: abs(taba.a2+1)=abs(tabb.b2)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: taba
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tabb
                            """);
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            assertQuery("select * from taba left join tabb on abs(a2+1) = abs(b2) or a2/2 = b2+1")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: (abs(taba.a2+1)=abs(tabb.b2) or taba.a2/2=tabb.b2+1)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: taba
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tabb
                            """);
        });
    }

    @Test
    public void testLeftJoinWithPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( created timestamp, value int ) timestamp(created)");

            String[] joinTypes = {"LEFT", "RIGHT", "FULL", "LT", "ASOF"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light", "Lt Join Fast", "AsOf Join Fast"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];

                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 " + (i < 3 ? " ON T1.created=T2.created " : "") + "WHERE not T2.value<>T2.value")
                        .noLeakCheck()
                        .assertsPlan("Count\n" + "    Filter filter: T2.value=T2.value\n" + "        " + factoryType + "\n" + (i < 3 ? "          condition: T2.created=T1.created\n" : "") + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + (i < 3 ? "            Hash\n" : "") + (i < 3 ? "    " : "") + "            PageFrame\n" + (i < 3 ? "    " : "") + "                Row forward scan\n" + (i < 3 ? "    " : "") + "                Frame forward scan on: tab\n");

                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 " + (i < 3 ? " ON T1.created=T2.created " : "") + "WHERE not T2.value=1")
                        .noLeakCheck()
                        .assertsPlan("Count\n" + "    Filter filter: T2.value!=1\n" + "        " + factoryType + "\n" + (i < 3 ? "          condition: T2.created=T1.created\n" : "") + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + (i < 3 ? "            Hash\n" : "") + (i < 3 ? "    " : "") + "            PageFrame\n" + (i < 3 ? "    " : "") + "                Row forward scan\n" + (i < 3 ? "    " : "") + "                Frame forward scan on: tab\n");

                // Push the predicate down to the 'left' table for joins that keep every left
                // row (LEFT OUTER, LT, ASOF). RIGHT and FULL OUTER NULL-extend the left table,
                // so the predicate must stay a post-join filter there instead.
                boolean isLeftNulled = "RIGHT".equals(joinType) || "FULL".equals(joinType);
                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 " + (i < 3 ? " ON T1.created=T2.created " : "") + "WHERE not T1.value=1")
                        .noLeakCheck()
                        .assertsPlan(isLeftNulled
                                ? "Count\n" + "    Filter filter: T1.value!=1\n" + "        " + factoryType + "\n" + "          condition: T2.created=T1.created\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n"
                                : "Count\n" + "    " + factoryType + "\n" + (i < 3 ? "      condition: T2.created=T1.created\n" : "") + "        Async JIT Filter workers: 1\n" + "          filter: value!=1\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + (i < 3 ? "        Hash\n" : "") + (i < 3 ? "    " : "") + "        PageFrame\n" + (i < 3 ? "    " : "") + "            Row forward scan\n" + (i < 3 ? "    " : "") + "            Frame forward scan on: tab\n");
            }

            // two joins
            for (int i = 0; i < 3; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                // RIGHT and FULL OUTER NULL-extend T1, so a T1 predicate stays a post-join filter.
                boolean isLeftNulled = "RIGHT".equals(joinType) || "FULL".equals(joinType);

                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 ON T1.created=T2.created " + "JOIN tab as T3 ON T2.created=T3.created " + "WHERE T1.value=1")
                        .noLeakCheck()
                        .assertsPlan(isLeftNulled
                                ? "Count\n" + "    Hash Join Light\n" + "      condition: T3.created=T2.created\n" + "        Filter filter: T1.value=1\n" + "            " + factoryType + "\n" + "              condition: T2.created=T1.created\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "                Hash\n" + "                    PageFrame\n" + "                        Row forward scan\n" + "                        Frame forward scan on: tab\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n"
                                : "Count\n" + "    Hash Join Light\n" + "      condition: T3.created=T2.created\n" + "        " + factoryType + "\n" + "          condition: T2.created=T1.created\n" + "            Async JIT Filter workers: 1\n" + "              filter: value=1\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n");

                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 ON T1.created=T2.created " + "JOIN tab as T3 ON T2.created=T3.created " + "WHERE T2.created=1")
                        .noLeakCheck()
                        .assertsPlan("Count\n" + "    Hash Join Light\n" + "      condition: T3.created=T2.created\n" + "        Filter filter: 1=T2.created\n" + "            " + factoryType + "\n" + "              condition: T2.created=T1.created\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "                Hash\n" + "                    PageFrame\n" + "                        Row forward scan\n" + "                        Frame forward scan on: tab\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n");

                assertQuery("SELECT count(1) " + "FROM tab as T1 " + joinType + " JOIN tab as T2 ON T1.created=T2.created " + "JOIN tab as T3 ON T2.created=T3.created " + "WHERE T3.value=1")
                        .noLeakCheck()
                        .assertsPlan("Count\n" + "    Hash Join Light\n" + "      condition: T3.created=T2.created\n" + "        " + factoryType + "\n" + "          condition: T2.created=T1.created\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "        Hash\n" + "            Async JIT Filter workers: 1\n" + "              filter: value=1\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n");

                // where clause in parent model
                assertQuery("SELECT count(1) " + "FROM ( " + "SELECT * " + "FROM tab as T1 " + joinType + " JOIN tab as T2 ON T1.created=T2.created ) e " + "WHERE not value1<>value1")
                        .noLeakCheck()
                        .assertsPlan("Count\n" + "    SelectedRecord\n" + "        Filter filter: T2.value=T2.value\n" + "            " + factoryType + "\n" + "              condition: T2.created=T1.created\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "                Hash\n" + "                    PageFrame\n" + "                        Row forward scan\n" + "                        Frame forward scan on: tab\n");

                // value is T1 (the master): RIGHT/FULL OUTER NULL-extend it, so the tautological
                // not value<>value (T1.value=T1.value) stays a post-join filter instead of pushing
                // into the master sub-query. The count is unchanged; only the plan moves.
                assertQuery("SELECT count(1) " + "FROM ( " + "SELECT * " + "FROM tab as T1 " + joinType + " JOIN tab as T2 ON T1.created=T2.created ) e " + "WHERE not value<>value")
                        .noLeakCheck()
                        .assertsPlan(isLeftNulled
                                ? "Count\n" + "    SelectedRecord\n" + "        Filter filter: T1.value=T1.value\n" + "            " + factoryType + "\n" + "              condition: T2.created=T1.created\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n" + "                Hash\n" + "                    PageFrame\n" + "                        Row forward scan\n" + "                        Frame forward scan on: tab\n"
                                : "Count\n" + "    SelectedRecord\n" + "        " + factoryType + "\n" + "          condition: T2.created=T1.created\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tab\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tab\n");
            }
        });
    }

    @Test
    public void testLikeFilters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (s1 string, s2 string, s3 string, s4 string, s5 string, s6 string);");

            assertQuery("select * from tab " + "where s1 like '%a'  and s2 ilike '%a' " + "  and s3 like 'a%'  and s4 ilike 'a%' " + "  and s5 like '%a%' and s6 ilike '%a%';")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Filter workers: 1
                              filter: ((s1 like %a and s2 ilike %a and s3 like a% and s4 ilike a%) and s5 like %a% and s6 ilike %a%)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testLtJoin0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select ts1, ts2, i1, i2 from (select a.i as i1, a.ts as ts1, b.i as i2, b.ts as ts2 from a lt join b on ts) where ts1::long*i1<ts2::long*i2")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.ts::long*a.i<b.ts::long*b.i
                                    Lt Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoin1a() throws Exception {
        // lt join guarantees that a.ts > b.ts [join cond is not an equality predicate]
        // CONCLUSION: a join b on X can't always be translated to a join b on a.X = b.X
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b on ts where a.i = b.ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.ts
                                    Lt Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoin1b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b on ts where a.i = b.ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.ts
                                    Lt Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoin1c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b where a.i = b.ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.ts
                                    Lt Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join (select * from b limit 10) on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertQuery("select * " + "from a " + "Lt Join b on a.i = b.i")
                        .withCompiler(compiler)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    Lt Join
                                      condition: b.i=a.i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                                """);
            }
        });
    }

    @Test
    public void testLtJoinNoKey1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b where a.i > 0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    Async JIT Filter workers: 1
                                      filter: 0<i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoinNoKey2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoinNoKey3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b on(ts)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtJoinNoKey4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, i int) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join b on(ts)")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Encode sort light
                                      keys: [ts, i]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testLtOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * " + "from a " + "lt join b on ts " + "lt join a c on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    Lt Join Fast
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testMultiExcept() throws Exception {
        assertQuery("select * from a except select * from a except select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Except
                            Except
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testMultiIntersect() throws Exception {
        assertQuery("select * from a intersect select * from a intersect select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Intersect
                            Intersect
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            Hash
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testMultiUnion() throws Exception {
        assertQuery("select * from a union select * from a union select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Union
                            Union
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testMultiUnionAll() throws Exception {
        assertQuery("select * from a union all select * from a union all select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Union All
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testNestedLoopLeftJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 left join t t2 on t1.x*t2.x>0 order by t1.ts", "select * from (select * from t order by ts desc) t1 left join t t2 on t1.x*t2.x>0 order by t1.ts"};
            for (String query : queries) {
                assertQuery(query)
                        .noLeakCheck()
                        .assertsPlan("""
                                SelectedRecord
                                    Nested Loop Left Join
                                      filter: 0<t1.x*t2.x
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                """);

                assertQuery(query)
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .returns("""
                                x\tts\tx1\tts1
                                1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z
                                1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000002Z
                                2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z
                                2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z
                                """);
            }
        });
    }

    @Test
    public void testNestedLoopLeftJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " + "((select * from t order by ts desc) limit 10) t1 " + "left join t t2 on t1.x*t2.x > 0 " + "order by t1.ts desc";

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Nested Loop Left Join
                                  filter: 0<t1.x*t2.x
                                    Limit value: 10 skip-rows: 0 take-rows: 2
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: t
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testNoArgFalseConstantExpressionUsedInJoinIsOptimizedAway() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (b boolean, ts timestamp)");
            // true
            assertQuery("update tab t1 set b=true from tab t2 where 1>2 and t1.b = t2.b")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: tab
                                VirtualRecord
                                  functions: [true]
                                    Empty table
                            """);
            // false
            assertQuery("update tab t1 set b=true from tab t2 where 1<2 and t1.b = t2.b")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: tab
                                VirtualRecord
                                  functions: [true]
                                    Hash Join Light
                                      condition: t2.b=t1.b
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testNoArgMixedConstAndRuntimeExprInJoin() throws Exception {
        // When constWhereClause mixes compile-time false with a runtime expression,
        // the optimizer keeps the compile-time false in constWhereClause and the
        // code generator folds it to Empty table.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (b BOOLEAN, ts TIMESTAMP)");
            assertQuery("SELECT * FROM tab T1 INNER JOIN tab T2 ON T1.b = T2.b WHERE 1 > 10 AND NOW() = NOW()")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Empty table
                            """);
        });
    }

    @Test
    public void testNoArgNonConstantExpressionUsedInJoinClauseIsUsedAsPostJoinFilter() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);

        assertQuery("update tab t1 set b=true from tab t2 where not sleep(60000) and t1.b = t2.b")
                .ddl("create table tab (b boolean, ts timestamp)")
                .assertsPlan("""
                        Update table: tab
                            VirtualRecord
                              functions: [true]
                                Filter filter: not (sleep(60000))
                                    Hash Join Light
                                      condition: t2.b=t1.b
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testNoArgRuntimeConstantExpressionUsedInJoinClauseIsUsedAsPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (b boolean, ts timestamp)");

            // true
            assertQuery("update tab t1 set b=true from tab t2 where now()::long > -1 and t1.b = t2.b")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: tab
                                VirtualRecord
                                  functions: [true]
                                    Filter filter: -1<now()::long
                                        Hash Join Light
                                          condition: t2.b=t1.b
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                            """);

            // false
            assertQuery("update tab t1 set b=true from tab t2 where now()::long < 0 and t1.b = t2.b")
                    .noLeakCheck()
                    .assertsPlan("""
                            Update table: tab
                                VirtualRecord
                                  functions: [true]
                                    Filter filter: now()::long<0
                                        Hash Join Light
                                          condition: t2.b=t1.b
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testNonKeyedGroupByMinMaxTimestamp() throws Exception {
        assertQuery("select min(ts), max(ts), min(ts1), max(ts1) from x")
                .ddl("create table x (ts timestamp, ts1 timestamp) timestamp(ts) partition by day;")
                .assertsPlan("""
                        Async Group By workers: 1
                          vectorized: true
                          values: [min_designated(ts),max_designated(ts),min(ts1),max(ts1)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: x
                        """);
    }

    @Test
    public void testOrderByAdvicePushdown() throws Exception {
        // TODO: improve :
        // - limit propagation to async filter factory
        // - negative limit rewrite
        // if order by is via alias of designated timestamp

        assertMemoryLeak(() -> {
            execute("create table device_data " + "( " + "  timestamp timestamp, " + "  val double, " + "  id symbol " + ") timestamp(timestamp)");

            execute("insert into device_data select x::timestamp, x, '12345678' from long_sequence(10)");

            // use column name in order by
            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp DESC " + "LIMIT 1")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            VirtualRecord
                              functions: [date,val,val+1]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      limit: 1
                                      filter: id='12345678'
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000010Z\t10.0\t11.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp  " + "LIMIT -1")
                    .noLeakCheck()
                    .timestamp("date")
                    .sizeMayVary()
                    .withPlan("""
                            VirtualRecord
                              functions: [date,val,val+1]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      limit: 1
                                      filter: id='12345678'
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000010Z\t10.0\t11.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp DESC " + "LIMIT -2")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            VirtualRecord
                              functions: [date,val,val+1]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      limit: 2
                                      filter: id='12345678'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000002Z\t2.0\t3.0
                            1970-01-01T00:00:00.000001Z\t1.0\t2.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp DESC " + "LIMIT 1,3")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            Limit left: 1 right: 3 skip-rows-max: 1 take-rows-max: 2
                                VirtualRecord
                                  functions: [date,val,val+1]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: id='12345678'
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000009Z\t9.0\t10.0
                            1970-01-01T00:00:00.000008Z\t8.0\t9.0
                            """);

            // with a virtual column
            assertQuery("SELECT timestamp, val, now() " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp DESC " + "LIMIT 1")
                    .noLeakCheck()
                    .timestampDesc("timestamp")
                    .sizeMayVary()
                    .withPlan("""
                            VirtualRecord
                              functions: [timestamp,val,now()]
                                Async JIT Filter workers: 1
                                  limit: 1
                                  filter: id='12345678'
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: device_data
                            """)
                    .returns("""
                            timestamp\tval\tnow
                            1970-01-01T00:00:00.000010Z\t10.0\t1970-01-01T00:00:00.000000Z
                            """);

            assertQuery("SELECT timestamp, val, now() " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY timestamp ASC " + "LIMIT -3")
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .sizeMayVary()
                    .withPlan("""
                            VirtualRecord
                              functions: [timestamp,val,now()]
                                Async JIT Filter workers: 1
                                  limit: 3
                                  filter: id='12345678'
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: device_data
                            """)
                    .returns("""
                            timestamp\tval\tnow
                            1970-01-01T00:00:00.000008Z\t8.0\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000009Z\t9.0\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000010Z\t10.0\t1970-01-01T00:00:00.000000Z
                            """);

            // use alias in order by
            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY date DESC " + "LIMIT 1")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                                VirtualRecord
                                  functions: [date,val,val+1]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: id='12345678'
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000010Z\t10.0\t11.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY date  " + "LIMIT -1")
                    .noLeakCheck()
                    .timestamp("date")
                    .sizeMayVary()
                    .withPlan("""
                            Limit value: -1 skip-rows: baseRows-1 take-rows-max: 1
                                VirtualRecord
                                  functions: [date,val,val+1]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: id='12345678'
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000010Z\t10.0\t11.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY date DESC " + "LIMIT -2")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            Limit value: -2 skip-rows: baseRows-2 take-rows-max: 2
                                VirtualRecord
                                  functions: [date,val,val+1]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: id='12345678'
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000002Z\t2.0\t3.0
                            1970-01-01T00:00:00.000001Z\t1.0\t2.0
                            """);

            assertQuery("SELECT timestamp AS date, val, val + 1 " + "FROM device_data " + "WHERE device_data.id = '12345678' " + "ORDER BY date DESC " + "LIMIT 1,3")
                    .noLeakCheck()
                    .timestampDesc("date")
                    .sizeMayVary()
                    .withPlan("""
                            Limit left: 1 right: 3 skip-rows-max: 1 take-rows-max: 2
                                VirtualRecord
                                  functions: [date,val,val+1]
                                    SelectedRecord
                                        Async JIT Filter workers: 1
                                          filter: id='12345678'
                                            PageFrame
                                                Row backward scan
                                                Frame backward scan on: device_data
                            """)
                    .returns("""
                            date\tval\tcolumn
                            1970-01-01T00:00:00.000009Z\t9.0\t10.0
                            1970-01-01T00:00:00.000008Z\t8.0\t9.0
                            """);
        });
    }

    @Test
    public void testOrderByIsMaintainedInLtAndAsofSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            for (String joinType : Arrays.asList("AsOf", "Lt")) {
                String query = "with gp as \n" + "(\n" + "selecT * from (\n" + "selecT * from gas_prices order by timestamp asc, galon_price desc\n" + ") timestamp(timestamp))\n" + "selecT * from gp gp1 \n" + joinType + " join gp gp2 \n" + "order by gp1.timestamp; ";

                String expectedPlan = "SelectedRecord\n" + "    " + joinType + " Join\n" + "        Encode sort light\n" + "          keys: [timestamp, galon_price desc]\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: gas_prices\n" + "        Encode sort light\n" + "          keys: [timestamp, galon_price desc]\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: gas_prices\n";

                assertQuery(query)
                        .noLeakCheck()
                        .assertsPlan(expectedPlan);
            }
        });
    }

    @Test
    public void testOrderByIsMaintainedInSpliceSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            String query = """
                    with gp as (
                    selecT * from (
                    selecT * from gas_prices order by timestamp asc, galon_price desc
                    ) timestamp(timestamp))
                    selecT * from gp gp1\s
                    splice join gp gp2\s
                    order by gp1.timestamp;\s""";

            String expectedPlan = """
                    Encode sort
                      keys: [timestamp]
                        SelectedRecord
                            Splice Join
                                Encode sort light
                                  keys: [timestamp, galon_price desc]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: gas_prices
                                Encode sort light
                                  keys: [timestamp, galon_price desc]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: gas_prices
                    """;

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan);
        });
    }

    @Test
    public void testOrderByIsMaintainedInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices " + "(timestamp TIMESTAMP, " + "galon_price DOUBLE) " + "timestamp (timestamp);");

            String query = """
                    WITH full_range AS ( \s
                      SELECT timestamp, galon_price FROM (
                        SELECT * FROM (
                          SELECT * FROM gas_prices
                          UNION
                          SELECT to_timestamp('1999-01-01', 'yyyy-MM-dd'), NULL as galon_price -- First Date
                          UNION\s
                          SELECT to_timestamp('2023-02-20', 'yyyy-MM-dd'), NULL  as galon_price -- Last Date
                        ) AS unordered_data\s
                        order by timestamp
                      ) TIMESTAMP(timestamp)
                    )\s
                    select * \
                    from full_range""";

            String expectedPlan = """
                    Encode sort
                      keys: [timestamp]
                        Union
                            Union
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: gas_prices
                                VirtualRecord
                                  functions: [1999-01-01T00:00:00.000000Z,null]
                                    long_sequence count: 1
                            VirtualRecord
                              functions: [2023-02-20T00:00:00.000000Z,null]
                                long_sequence count: 1
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan);
            assertQuery(query + " order by timestamp")
                    .noLeakCheck()
                    .assertsPlan(expectedPlan);
        });
    }

    @Test
    public void testOrderByTimestampAndOtherColumns1() throws Exception {
        assertQuery("select * from (select * from tab order by ts, i desc limit 10) order by ts")
                .ddl("create table tab (i int, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Encode sort light lo: 10 partiallySorted: true
                          keys: [ts, i desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testOrderByTimestampAndOtherColumns2() throws Exception {
        assertQuery("select * from (select * from tab order by ts desc, i asc limit 10) order by ts desc")
                .ddl("create table tab (i int, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Encode sort light lo: 10 partiallySorted: true
                          keys: [ts desc, i]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testOuterJoinWithEquality1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a int)");
            execute("create table tabb (b int)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a=b")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b=a\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testOuterJoinWithEquality2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=b2")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b2=a2 and b1=a1\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test // FIXME: ORed predicates should be applied as filter in hash join
    public void testOuterJoinWithEquality5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1 and (a2=b2+10 or a2=2*b2)")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b1=a1\n" + "      filter: (taba.a2=tabb.b2+10 or taba.a2=2*tabb.b2)\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    // left join conditions aren't transitive because left record + null right is produced if they fail
    // that means select * from a left join b on a.i = b.i and a.i=10 doesn't mean resulting records will have a.i = 10 !
    @Test
    public void testOuterJoinWithEquality6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            execute("create table tabc (c1 int, c2 long)");

            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1 and a1=5 " + "join tabc on a1=c1")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    Hash Join Light\n" + "      condition: c1=a1\n" + "        " + factoryType + "\n" + "          condition: b1=a1\n" + "          filter: taba.a1=5\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tabb\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabc\n");
            }
        });
    }

    @Test
    public void testOuterJoinWithEqualityAndExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2)")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b2=a2 and b1=a1\n" + "      filter: abs(taba.a2+1)=abs(tabb.b2)\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testOuterJoinWithEqualityAndExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=b2 and a2+5 = b2+10")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b2=a2 and b1=a1\n" + "      filter: taba.a2+5=tabb.b2+10\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testOuterJoinWithEqualityAndExpressions3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            assertQuery("select * from taba left join tabb on a1=b1 and a2=b2 and a2+5 = b2+10 and 1=0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Left Outer Join Light
                                  condition: b2=a2 and b1=a1
                                  filter: false
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: taba
                                    Hash
                                        Empty table
                            """);
            assertQuery("select * from taba right join tabb on a1=b1 and a2=b2 and a2+5 = b2+10 and 1=0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Right Outer Join Light
                                  condition: b2=a2 and b1=a1
                                  filter: false
                                    Empty table
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tabb
                            """);
            assertQuery("select * from taba full join tabb on a1=b1 and a2=b2 and a2+5 = b2+10 and 1=0")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Full Outer Join Light
                                  condition: b2=a2 and b1=a1
                                  filter: false
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: taba
                                    Hash
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tabb
                            """);
        });
    }

    // FIXME provably false predicate like x!=x in left join means we can skip join and return left + nulls or join with empty right table
    @Test
    public void testOuterJoinWithEqualityAndExpressions4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2!=a2")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b1=a1\n" + "      filter: taba.a2!=taba.a2\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test // FIXME: a2=a2 run as past of left join or be optimized away !
    public void testOuterJoinWithEqualityAndExpressions5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=a2")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b1=a1\n" + "      filter: taba.a2=taba.a2\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test // outer join filter must remain intact !
    public void testOuterJoinWithEqualityAndExpressions6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 string)");
            execute("create table tabb (b1 int, b2 string)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2 ~ 'a.*' and b2 ~ '.*z'")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    " + factoryType + "\n" + "      condition: b1=a1\n" + "      filter: (taba.a2 ~ a.* and tabb.b2 ~ .*z)\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: taba\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testOuterJoinWithEqualityAndExpressionsAhdWhere2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            String[] joinTypes = {"LEFT", "RIGHT", "FULL"};
            String[] joinFactoryTypes = {"Hash Left Outer Join Light", "Hash Right Outer Join Light", "Hash Full Outer Join Light"};

            for (int i = 0; i < joinTypes.length; i++) {
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];
                assertQuery("select * from taba " + joinType + " join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) where a1=b1")
                        .noLeakCheck()
                        .assertsPlan("SelectedRecord\n" + "    Filter filter: taba.a1=tabb.b1\n" + "        " + factoryType + "\n" + "          condition: b2=a2 and b1=a1\n" + "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tabb\n");
            }
        });
    }

    @Test
    public void testPostJoinConditionColumnsAreResolved() throws Exception {
        assertMemoryLeak(() -> {
            String query = """
                    SELECT count(*)
                    FROM test as T1
                    JOIN ( SELECT * FROM test ) as T2 ON T1.event < T2.event
                    JOIN test as T3 ON T2.created = T3.created""";

            execute("create table test (event int, created timestamp)");
            execute("insert into test values (1, 1), (2, 2)");

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Count
                                Filter filter: T1.event<T2.event
                                    Cross Join
                                        Hash Join Light
                                          condition: T3.created=T2.created
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: test
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: test
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: test
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testPredicatesArentPushedIntoWindowModel() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT * " + "FROM ( " + "  SELECT *, ROW_NUMBER() OVER ( PARTITION BY a ORDER BY b ) rownum " + "  FROM (" + "    SELECT 1 a, 2 b, 4 c " + "    UNION " + "    SELECT 1, 3, 5 " + "  ) o " + ") ra " + "WHERE ra.rownum = 1 " + "AND   c = 5";

            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: (rownum=1 and c=5)
                                CachedWindow
                                  orderedFunctions: [[b] => [row_number() over (partition by [a])]]
                                    Union
                                        VirtualRecord
                                          functions: [1,2,4]
                                            long_sequence count: 1
                                        VirtualRecord
                                          functions: [1,3,5]
                                            long_sequence count: 1
                            """);
            assertQuery(query)
                    .noLeakCheck()
                    .returns("a\tb\tc\trownum\n");

            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");

            assertQuery("SELECT *, ROW_NUMBER() OVER () FROM tab WHERE x = 10")
                    .noLeakCheck()
                    .assertsPlan("""
                            Window
                              functions: [row_number()]
                                Async JIT Filter workers: 1
                                  filter: x=10
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) WHERE x = 10")
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: x=10
                                Window
                                  functions: [row_number()]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab UNION ALL select 11, 11  ) WHERE x = 10")
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: x=10
                                Union All
                                    Window
                                      functions: [row_number()]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                    VirtualRecord
                                      functions: [11,11]
                                        long_sequence count: 1
                            """);

            assertQuery("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab cross join (select 11, 11)  ) WHERE x = 10")
                    .noLeakCheck()
                    .assertsPlan("""
                            Filter filter: x=10
                                Window
                                  functions: [row_number()]
                                    SelectedRecord
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                            VirtualRecord
                                              functions: [11,11]
                                                long_sequence count: 1
                            """);

            assertQuery("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) join (select 11L y, 11) on x=y WHERE x = 10")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Hash Join Light
                                  condition: y=x
                                    Filter filter: x=10
                                        Window
                                          functions: [row_number()]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                    Hash
                                        Filter filter: y=10
                                            VirtualRecord
                                              functions: [11L,11]
                                                long_sequence count: 1
                            """);
        });
    }

    @Test
    public void testReadParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" + " x as a_long," + " rnd_str(4,4,4,2) as a_str," + " rnd_timestamp('2015','2016',2) as a_ts" + " from long_sequence(3))");

            try (Path path = new Path(); PartitionDescriptor partitionDescriptor = new PartitionDescriptor(); TableReader reader = engine.getReader("x")) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertTrue(Files.exists(path.$()));

                assertQuery("select * from read_parquet('x.parquet') where a_long = 42;")
                        .noLeakCheck()
                        .assertsPlan("""
                                Async JIT Filter workers: 1
                                  filter: a_long=42
                                    parquet page frame scan
                                      columns: a_long,a_str,a_ts
                                """);

                assertQuery("select avg(a_long) from read_parquet('x.parquet');")
                        .noLeakCheck()
                        .assertsPlan("""
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [avg(a_long)]
                                  filter: null
                                    parquet page frame scan
                                      columns: a_long
                                """);

                assertQuery("select a_str, max(a_long) from read_parquet('x.parquet');")
                        .noLeakCheck()
                        .assertsPlan("""
                                Async Group By workers: 1
                                  keys: [a_str]
                                  values: [max(a_long)]
                                  filter: null
                                    parquet page frame scan
                                      columns: a_str,a_long
                                """);
            }
        });
    }

    @Test
    public void testRewriteAggregateWithAdditionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            assertQuery("SELECT sum(x), sum(x+10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(x+10)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10+x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(10+x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithAdditionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            assertQuery("SELECT sum(taba.x),sum(tabb.x), sum(taba.x+10), sum(tabb.x+10) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,sum+COUNT*10,sum1+COUNT1*10]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1),count(x),count(x1)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);

            assertQuery("SELECT sum(tabb.x),sum(taba.x),sum(10+taba.x), sum(10+tabb.x) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,COUNT*10+sum1,COUNT1*10+sum]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1),count(x1),count(x)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithIntAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            assertQuery("SELECT sum(x), sum(x+10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum+COUNT*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10+x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*10+sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithIntMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            assertQuery("SELECT sum(x), sum(x*10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10*x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,10*sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithIntSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            assertQuery("SELECT sum(x), sum(x-10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum-COUNT*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10-x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*10-sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithLongAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            assertQuery("SELECT sum(x), sum(x+2) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum+COUNT*2]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(2+x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*2+sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithLongMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            assertQuery("SELECT sum(x), sum(x*10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10*x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,10*sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithLongSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            assertQuery("SELECT sum(x), sum(x-10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum-COUNT*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10-x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*10-sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerColumnType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            assertQuery("SELECT sum(x), sum(x*10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(x*10)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10*x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(10*x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerConstantType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            assertQuery("SELECT sum(x), sum(x*10.0) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(x*10.0)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10.0*x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(10.0*x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            assertQuery("SELECT sum(taba.x),sum(tabb.x),sum(taba.x*10), sum(tabb.x*10) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,sum*10,sum1*10]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);

            assertQuery("SELECT sum(taba.x),sum(tabb.x),sum(10*taba.x), sum(10*tabb.x) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,10*sum,10*sum1]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithShortAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            assertQuery("SELECT sum(x), sum(x+42) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum+COUNT*42]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(42+x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*42+sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithShortMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            assertQuery("SELECT sum(x), sum(x*10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10*x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,10*sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithShortSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            assertQuery("SELECT sum(x), sum(x-10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum-COUNT*10]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10-x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,COUNT*10-sum]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(x),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithSubtractionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            assertQuery("SELECT sum(x), sum(x-10) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(x-10)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("SELECT sum(x), sum(10-x) FROM tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async Group By workers: 1
                              vectorized: true
                              values: [sum(x),sum(10-x)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);
        });
    }

    @Test
    public void testRewriteAggregateWithSubtractionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            assertQuery("SELECT sum(taba.x),sum(tabb.x),sum(taba.x-10), sum(tabb.x-10) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,sum-COUNT*10,sum1-COUNT1*10]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1),count(x),count(x1)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);

            assertQuery("SELECT sum(taba.x),sum(tabb.x),sum(10-taba.x), sum(10-tabb.x) " + "FROM taba " + "join tabb on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,sum1,COUNT*10-sum,COUNT1*10-sum1]
                                GroupBy vectorized: false
                                  values: [sum(x),sum(x1),count(x),count(x1)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: tabb.id=taba.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: taba
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tabb
                            """);
        });
    }

    @Test
    public void testRewriteAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE hits
                    (
                        EventTime timestamp,
                        ResolutionWidth int,
                        ResolutionHeight int
                    ) TIMESTAMP(EventTime) PARTITION BY DAY;""");

            assertQuery("SELECT sum(resolutIONWidth), count(resolutionwIDTH), SUM(ResolutionWidth), sum(ResolutionWidth) + count(), " + "SUM(ResolutionWidth+1),SUM(ResolutionWidth*2),sUM(ResolutionWidth), count()\n" + "FROM hits")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,count,sum,sum+count1,sum+count*1,sum*2,sum,count1]
                                Async Group By workers: 1
                                  vectorized: true
                                  values: [sum(ResolutionWidth),count(ResolutionWidth),count(*)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: hits
                            """);
        });
    }

    @Test
    public void testRewriteAggregatesOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hits1" + "(" + "    EventTime timestamp, " + "    ResolutionWidth int, " + "    ResolutionHeight int, " + "    id int" + ")");
            execute("create table hits2 as (select * from hits1)");

            assertQuery("SELECT sum(h1.resolutIONWidth), count(h1.resolutionwIDTH), SUM(h2.ResolutionWidth), sum(h2.ResolutionWidth) + count(), " + "SUM(h1.ResolutionWidth+1),SUM(h2.ResolutionWidth*2),sUM(h1.ResolutionWidth), count()\n" + "FROM hits1 h1 " + "join hits2 h2 on (id)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [sum,count,SUM1,SUM1+count1,sum+count*1,SUM1*2,sum,count1]
                                GroupBy vectorized: false
                                  values: [sum(resolutIONWidth),count(resolutIONWidth),sum(ResolutionWidth1),count(*)]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: h2.id=h1.id
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: hits1
                                            Hash
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: hits2
                            """);
        });
    }

    @Test
    public void testRewriteSelectCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test(s string, x long, ts timestamp, substring string) timestamp(ts) partition by day");
            execute("insert into test " + "select 's' || (x%10), " + " x, " + " (x*86400000000)::timestamp, " + " 'substring' " + "from long_sequence(10)");

            // multiple count_distinct, no re-write
            String expected = """
                    GroupBy vectorized: false
                      values: [count_distinct(s),count_distinct(x)]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(s), count_distinct(x) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct s), count(distinct x) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);


            // no where clause, distinct constant
            expected = """
                    Async Group By workers: 1
                      vectorized: false
                      values: [count_distinct(10)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(10) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct 10) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // no where clause, distinct column
            expected = """
                    Count
                        Async JIT Group By workers: 1
                          keys: [s]
                          filter: s is not null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(s) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct s) FROM test")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // with where clause, distinct column
            expected = """
                    Count
                        Async Group By workers: 1
                          keys: [s]
                          filter: (s like %abc% and s is not null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(s) FROM test where s like '%abc%'")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct s) FROM test where s like '%abc%'")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // no where clause, distinct expression 1
            expected = """
                    Count
                        Async Group By workers: 1
                          keys: [substring]
                          keyFunctions: [substring(s,1,1)]
                          filter: substring(s,1,1) is not null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(substring(s,1,1)) FROM test;")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct substring(s,1,1)) FROM test;")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // where clause, distinct expression 2
            expected = """
                    Count
                        Async Group By workers: 1
                          keys: [substring]
                          keyFunctions: [substring(s,1,1)]
                          filter: (s like %abc% and substring(s,1,1) is not null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%'")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct substring(s,1,1)) FROM test where s like '%abc%'")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // where clause, distinct expression 3, function name clash with column name
            expected = """
                    Count
                        Async Group By workers: 1
                          keys: [substring]
                          keyFunctions: [substring(s,1,1)]
                          filter: (s like %abc% and substring is not null and substring(s,1,1) is not null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%' and substring != null")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct substring(s,1,1)) FROM test where s like '%abc%' and substring != null")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // where clause, distinct expression 3
            expected = """
                    Count
                        Async JIT Group By workers: 1
                          keys: [column]
                          keyFunctions: [x+1]
                          filter: (5<x and x+1!=null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(x+1) FROM test where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct x+1) FROM test where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);

            // where clause, distinct expression, col alias
            expected = """
                    Count
                        Async JIT Group By workers: 1
                          keys: [column]
                          keyFunctions: [x+1]
                          filter: (5<x and x+1!=null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(x+1) cnt_dst FROM test where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct x+1) cnt_dst FROM test where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);

            expected = """
                    cnt_dst
                    5
                    """;
            assertQuery("SELECT count_distinct(x+1) cnt_dst FROM test where x > 5")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT count(distinct x+1) cnt_dst FROM test where x > 5")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);

            // where clause, distinct expression, table alias
            expected = """
                    Count
                        Async JIT Group By workers: 1
                          keys: [column]
                          keyFunctions: [x+1]
                          filter: (5<x and x+1!=null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                    """;
            assertQuery("SELECT count_distinct(x+1) FROM test tab where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);
            assertQuery("SELECT count(distinct x+1) FROM test tab where x > 5")
                    .noLeakCheck()
                    .assertsPlan(expected);

            expected = """
                    count_distinct
                    5
                    """;
            assertQuery("SELECT count_distinct(x+1) FROM test tab where x > 5")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("SELECT count(distinct x+1) FROM test tab where x > 5")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select first(i) from a sample by 1h align to first observation")
                    .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: none
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select first(i) from a sample by 1h align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(i)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByAliasesAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int, b int, ts timestamp) timestamp(ts);");

            assertQuery("select x1.a, sum(x1.b) from x x1 sample by 2m align to first observation order by x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a]
                                Sample By
                                  keys: [a]
                                  values: [sum(b)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery("select \"x1.a\", sum(\"x1.b\") from x \"x1\" sample by 2m order by \"x1.a\"")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select \"x1.a\", sum(\"x1.b\") from x \"x1\" sample by 2m align to calendar time zone 'Europe/Paris' order by \"x1.a\"")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select \"x1.a\", sum(\"x1.b\") from x \"x1\" sample by 2m align to calendar time zone 'Europe/Paris' order by 10*\"x1.a\"")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column]
                                    Async Group By workers: 1
                                      keys: [a,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 sample by 2m align to calendar time zone 'Europe/Paris' order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a desc]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select 10*x1.a as a10, sum(x1.b) from x x1 sample by 2m align to calendar time zone 'Europe/Paris' order by a10")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a10]
                                    Async Group By workers: 1
                                      keys: [a10,ts]
                                      keyFunctions: [10*a,timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b) from x x1 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    Async Group By workers: 1
                                      keys: [a0,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a0]
                                      values: [sum(b)]
                                      filter: null
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b), x1.ts from x x1 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    Async Group By workers: 1
                                      keys: [a0,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a0]
                                      values: [sum(b)]
                                      filter: null
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.ts, to_utc(x1.ts, 'Europe/Berlin') berlin_ts, x1.a as a0, sum(x1.b) from x x1 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 3 asc, berlin_ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0, berlin_ts desc]
                                    Async Group By workers: 1
                                      keys: [ts,berlin_ts,a0,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),to_utc(ts),10*a0]
                                      values: [sum(b)]
                                      filter: null
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testSampleByDuplicateKeys() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select b, sum(a), k k1, k from x sample by 3h")
                    .ddl("create table x ( a double, b symbol, k timestamp, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    SelectedRecord
                                        Async Group By workers: 1
                                          keys: [b,k1,ts]
                                          keyFunctions: [timestamp_floor_utc('3h',ts)]
                                          values: [sum(a)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select b, sum(a), k, k k1 from x sample by 3h")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    SelectedRecord
                                        Async Group By workers: 1
                                          keys: [b,k,ts]
                                          keyFunctions: [timestamp_floor_utc('3h',ts)]
                                          values: [sum(a)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select first(i) from a sample by 1h fill(linear) align to first observation")
                    .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select first(i) from a sample by 1h fill(linear) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select first(i) from a sample by 1h fill(null) align to first observation")
                    .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: null
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select first(i) from a sample by 1h fill(null) align to calendar with offset '10:00'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts,'1970-01-01T10:00:00.000Z')]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);

            // with rewrite
            assertQuery("select first(i) from a sample by 1h fill(null) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByFillPrevKeyed() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select s, first(i) from a sample by 1h fill(prev) align to first observation")
                    .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: prev
                              keys: [s]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select s, first(i) from a sample by 1h fill(prev) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [s,ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);

            // PREV(col_ref) cross-column: fill aggregate `a` with prev value of aggregate `s`.
            // Mirrors the FillRecordDispatchTest.testDoubleCrossColumnPrevToAggregate scenario.
            assertQuery("select k, first(i) AS s, first(j) AS a from b sample by 1h fill(prev, prev(s)) align to calendar")
                    .ddl("create table b (i int, j int, k symbol, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [k,ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i),first(j)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: b
                            """);

            // Negative: PREV references an alias that does not exist in the select list.
            assertQuery("select k, first(i) AS s, first(j) AS a from b sample by 1h fill(prev, prev(nonexistent)) align to calendar")
                    .noLeakCheck()
                    .fails(75, "PREV(col): column not found in output: nonexistent");
        });
    }

    @Test
    public void testSampleByFillPrevNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select first(i) from a sample by 1h fill(prev) align to first observation")
                    .ddl("create table a (i int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: prev
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select first(i) from a sample by 1h fill(prev) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);

            assertQuery("select first(a1.i) from a a1 asof join a a2 sample by 1h fill(prev) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        GroupBy vectorized: false
                                          keys: [ts]
                                          values: [first(i)]
                                            SelectedRecord
                                                AsOf Join Fast
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                            """);

            // PREV(col_ref) cross-column on a non-keyed query: fill aggregate `b`
            // with prev value of aggregate `s`.
            assertQuery("select first(i) AS s, first(j) AS b from c sample by 1h fill(prev, prev(s)) align to calendar")
                    .ddl("create table c (i int, j int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i),first(j)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: c
                            """);

            // Negative: PREV references an alias that does not exist in the select list.
            assertQuery("select first(i) AS s, first(j) AS b from c sample by 1h fill(prev, prev(missing)) align to calendar")
                    .noLeakCheck()
                    .fails(72, "PREV(col): column not found in output: missing");
        });
    }

    @Test
    public void testSampleByFillValueKeyed() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select s, first(i) from a sample by 1h fill(1) align to first observation")
                    .ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: value
                              keys: [s]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select s, first(i) from a sample by 1h fill(1) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: value
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [s,ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByFillValueNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select first(i) from a sample by 1h fill(1) align to first observation")
                    .ddl("create table a (i int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: value
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select first(i) from a sample by 1h fill(1) align to calendar with offset '10:00'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: value
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts,'1970-01-01T10:00:00.000Z')]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);

            // with rewrite
            assertQuery("select first(i) from a sample by 1h fill(1) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: value
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByFillWithConstantProjection() throws Exception {
        // Locks in that SELECT-list CONSTANT projections hoist into the outer
        // VirtualRecord and never reach the inner sample-by bottomUpColumns.
        // SqlCodeGenerator.generateFill walks bottomUpColumns to map factory
        // columns to user-fill value slots; if a constant ever leaked into
        // bottomUpColumns, aggNonKeyCount would over-count and per-column FILL
        // values would shift onto the wrong aggregate. The plan makes the
        // separation visible: 'tag' lives in VirtualRecord.functions, while
        // the inner Async Group By values: lists only the aggregates.
        assertMemoryLeak(() -> {
            assertQuery("select ts, 'tag' as c, first(i) as a, sum(j) as b from a sample by 1h fill(-1, 99) align to calendar")
                    .ddl("create table a (i int, j int, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,'tag',a,b]
                                Sample By Fill
                                  stride: '1h'
                                  fill: value
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i),sum(j)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);

            assertQuery("select ts, 'tag' as c, first(i) as a, sum(j) as b from a sample by 1h fill(prev) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [ts,'tag',a,b]
                                Sample By Fill
                                  stride: '1h'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i),sum(j)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByFirstLast() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select sym, first(i), last(s), first(l) " + "from a " + "where sym in ('S') " + "and   ts > 0::timestamp and ts < 100::timestamp " + "sample by 1h align to first observation")
                    .ddl("create table a ( l long, s symbol, sym symbol index, i int, ts timestamp) timestamp(ts) partition by day;")
                    .noLeakCheck()
                    .assertsPlan("""
                            SampleByFirstLast
                              keys: [sym]
                              values: [first(i), last(s), first(l)]
                                DeferredSingleSymbolFilterPageFrame
                                    Index forward scan on: sym deferred: true
                                      filter: sym='S'
                                    Interval forward scan on: a
                                      intervals: [("1970-01-01T00:00:00.000001Z","1970-01-01T00:00:00.000099Z")]
                            """);

            assertQuery("select sym, first(i), last(s), first(l) " + "from a " + "where sym in ('S') " + "and   ts > 0::timestamp and ts < 100::timestamp " + "sample by 1h align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    GroupBy vectorized: false
                                      keys: [sym,ts]
                                      values: [first(i),last(s),first(l)]
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: sym deferred: true
                                              filter: sym='S'
                                            Interval forward scan on: a
                                              intervals: [("1970-01-01T00:00:00.000001Z","1970-01-01T00:00:00.000099Z")]
                            """);
        });
    }

    @Test
    public void testSampleByJoinAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int, b int, ts timestamp) timestamp(ts);");

            assertQuery("select x1.a, sum(x1.b) from x x1 asof join x x2 sample by 2m align to first observation order by x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a]
                                Sample By
                                  keys: [a]
                                  values: [sum(b)]
                                    SelectedRecord
                                        AsOf Join Fast
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 asof join x x2 sample by 2m order by x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    GroupBy vectorized: false
                                      keys: [a,ts]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    GroupBy vectorized: false
                                      keys: [a,ts]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a")
                    .fails(116, "Ambiguous column [name=a]");

            assertQuery("select x1.a, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a desc]
                                    GroupBy vectorized: false
                                      keys: [a,ts]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select 10*x1.a as a10, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by a10")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a10]
                                    GroupBy vectorized: false
                                      keys: [a10,ts]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    GroupBy vectorized: false
                                      keys: [a0,ts,column]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b), x1.ts from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    GroupBy vectorized: false
                                      keys: [a0,ts,column]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);

            assertQuery("select x1.ts, to_utc(x1.ts, 'Europe/Berlin') berlin_ts, x1.a as a0, sum(x1.b) from x x1 asof join x x2 sample by 2m align to calendar time zone 'Europe/Paris' order by 10*x1.a desc, 3 asc, berlin_ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0, berlin_ts desc]
                                    GroupBy vectorized: false
                                      keys: [ts,berlin_ts,a0,column]
                                      values: [sum(b)]
                                        SelectedRecord
                                            AsOf Join Fast
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testSampleByKeyed0() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, i, first(i) from a sample by 1h align to first observation")
                    .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              keys: [l,i]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, i, first(i) from a sample by 1h align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [l,i,ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(i)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed1() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, i, first(i) from a sample by 1h align to first observation")
                    .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              keys: [l,i]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, i, first(i) from a sample by 1h align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [l,i,ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(i)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed2() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, first(i) from a sample by 1h fill(null) align to first observation")
                    .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: null
                              keys: [l]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, first(i) from a sample by 1h fill(null) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [l,ts]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [first(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed3() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, first(i) from a sample by 1d fill(linear) align to first observation")
                    .ddl("create table a (i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              keys: [l]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, first(i) from a sample by 1d fill(linear) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              keys: [l]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed3MultiKeyLinear() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, m, first(i) from a sample by 1d fill(linear) align to first observation")
                    .ddl("create table a (i int, l long, m long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              keys: [l,m]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, m, first(i) from a sample by 1d fill(linear) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              keys: [l,m]
                              values: [first(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed4() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, first(i), last(i) from a sample by 1d fill(1,2) align to first observation")
                    .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: value
                              keys: [l]
                              values: [first(i),last(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, first(i), last(i) from a sample by 1d fill(1,2) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1d'
                                  fill: value
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [l,ts]
                                          keyFunctions: [timestamp_floor_utc('1d',ts)]
                                          values: [first(i),last(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByKeyed5() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select l, first(i), last(i) from a sample by 1d fill(prev,prev) align to first observation")
                    .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts);")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: value
                              keys: [l]
                              values: [first(i),last(i)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);

            assertQuery("select l, first(i), last(i) from a sample by 1d fill(prev,prev) align to calendar")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Sample By Fill
                                  stride: '1d'
                                  fill: prev
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [l,ts]
                                          keyFunctions: [timestamp_floor_utc('1d',ts)]
                                          values: [first(i),last(i)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSampleByOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int, b int, ts timestamp) timestamp(ts);");

            assertQuery("select a, sum(b) from x sample by 2m align to first observation order by a")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a]
                                Sample By
                                  keys: [a]
                                  values: [sum(b)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b) from x sample by 2m order by a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by 10*a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column]
                                    Async Group By workers: 1
                                      keys: [a,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a desc]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select 10*a as a10, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by a10")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [a10]
                                    Async Group By workers: 1
                                      keys: [a10,ts]
                                      keyFunctions: [10*a,timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a as a0, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by 10*a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    Async Group By workers: 1
                                      keys: [a0,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a0]
                                      values: [sum(b)]
                                      filter: null
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select a as a0, sum(b), ts from x sample by 2m align to calendar time zone 'Europe/Paris' order by 10*a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0]
                                    Async Group By workers: 1
                                      keys: [a0,ts,column]
                                      keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris'),10*a0]
                                      values: [sum(b)]
                                      filter: null
                                        SelectedRecord
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select ts, to_utc(ts, 'Europe/Berlin') berlin_ts, a as a0, sum(b) from x sample by 2m align to calendar time zone 'Europe/Paris' order by 10*a desc, 3 asc, berlin_ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [column desc, a0, berlin_ts desc]
                                    VirtualRecord
                                      functions: [ts,to_utc(ts),a0,sum,10*a0]
                                        Async Group By workers: 1
                                          keys: [ts,a0]
                                          keyFunctions: [timestamp_floor_utc('2m',ts,null,'00:00','Europe/Paris')]
                                          values: [sum(b)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testSampleByOrderByTimestampFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int, b int, ts timestamp) timestamp(ts);");

            assertQuery("select a, sum(b), to_timezone(ts, 'Europe/Berlin') berlin_ts from x sample by 2m order by berlin_ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [berlin_ts desc]
                                VirtualRecord
                                  functions: [a,sum,to_timezone(ts)]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b), to_timezone(ts, 'Europe/Berlin') berlin_ts from x sample by 2m order by 3 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [berlin_ts]
                                VirtualRecord
                                  functions: [a,sum,to_timezone(ts)]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, sum(b), to_timezone(ts, 'Europe/Berlin') berlin_ts from x sample by 2m order by to_timezone(ts, 'Europe/Berlin')")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [berlin_ts]
                                VirtualRecord
                                  functions: [a,sum,to_timezone(ts)]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select a, timestamp_floor('M', ts) month_ts, sum(b), to_timezone(ts, 'Europe/Berlin') berlin_ts from x sample by 2m order by berlin_ts desc, a asc, month_ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [berlin_ts desc, a, month_ts]
                                VirtualRecord
                                  functions: [a,timestamp_floor('month',ts),sum,to_timezone(ts)]
                                    Async Group By workers: 1
                                      keys: [a,ts]
                                      keyFunctions: [timestamp_floor_utc('2m',ts)]
                                      values: [sum(b)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testSelect0() throws Exception {
        assertQuery("select * from a")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectConcat() throws Exception {
        assertMemoryLeak(() -> assertQuery("select concat('a', 'b', rnd_str('c', 'd', 'e'))")
                .noLeakCheck()
                .assertsPlan("""
                        VirtualRecord
                          functions: [concat(['a','b',rnd_str([c,d,e])])]
                            long_sequence count: 1
                        """));
    }

    @Test
    public void testSelectCount1() throws Exception {
        assertQuery("select count(*) from a")
                .ddl("create table a ( i int, d double)")
                .assertsPlan("""
                        Count
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount10() throws Exception {
        assertQuery("select count(*) from (select 1 from a limit 1) ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            Limit value: 1 skip-rows: 0 take-rows: 0
                                VirtualRecord
                                  functions: [1]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount11() throws Exception {
        assertQuery("select count(*) from (select * from a lt join a b) ")
                .ddl("create table a ( i int, ts timestamp ) timestamp(ts)")
                .assertsPlan("""
                        Count
                            SelectedRecord
                                Lt Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount12() throws Exception {
        assertQuery("select count(*) from (select * from a asof join a b) ")
                .ddl("create table a ( i int, ts timestamp ) timestamp(ts)")
                .assertsPlan("""
                        Count
                            SelectedRecord
                                AsOf Join Fast
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test // TODO: should return count(first table)*count(second_table) instead
    public void testSelectCount13() throws Exception {
        assertQuery("select count(*) from (select * from a cross join a b) ")
                .ddl("create table a ( i int, ts timestamp ) timestamp(ts)")
                .assertsPlan("""
                        Count
                            SelectedRecord
                                Cross Join
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount14() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts desc ")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        DeferredSingleSymbolFilterPageFrame
                            Index backward scan on: s deferred: true
                              filter: s='S1'
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectCount15() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts asc")
                .ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        DeferredSingleSymbolFilterPageFrame
                            Index forward scan on: s deferred: true
                              filter: s='S1'
                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount16() throws Exception {
        assertQuery("select count(*) from (select i, j from a group by i, j)")
                .ddl("create table a (i long, j long)")
                .assertsPlan("""
                        Count
                            Async Group By workers: 1
                              keys: [i,j]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount17() throws Exception {
        assertQuery("select count(*) from (select i, j from a where d > 42 group by i, j)")
                .ddl("create table a (i long, j long, d double)")
                .assertsPlan("""
                        Count
                            Async JIT Group By workers: 1
                              keys: [i,j]
                              filter: 42<d
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount2() throws Exception {
        assertQuery("select count() from a")
                .ddl("create table a ( i int, d double)")
                .assertsPlan("""
                        Count
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount3() throws Exception {
        assertQuery("select count(2) from a")
                .ddl("create table a ( i int, d double)")
                .assertsPlan("""
                        Count
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount4() throws Exception {
        assertQuery("select count(*) from a where s = 'S1'")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: s deferred: true
                                  filter: s='S1'
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount5() throws Exception {
        assertQuery("select count(*) from (select * from a union all select * from a) ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount6() throws Exception {
        assertQuery("select count(*) from (select * from a union select * from a) ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            Union
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount7() throws Exception {
        assertQuery("select count(*) from (select * from a intersect select * from a) ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            Intersect
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                Hash
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCount8() throws Exception {
        assertQuery("select count(*) from a where 1=0 ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            Empty table
                        """);
    }

    @Test
    public void testSelectCount9() throws Exception {
        assertQuery("select count(*) from a where 1=1 ")
                .ddl("create table a ( i int, s symbol index)")
                .assertsPlan("""
                        Count
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectCountDistinct1() throws Exception {
        String expected = """
                GroupBy vectorized: false
                  values: [count_distinct(s)]
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select count_distinct(s) from tab")
                .ddl("create table tab (s symbol, ts timestamp);")
                .assertsPlan(expected);
        assertQuery("select count(distinct s) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct2() throws Exception {
        String expected = """
                GroupBy vectorized: false
                  values: [count_distinct(s)]
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select count_distinct(s) from tab")
                .ddl("create table tab (s symbol index, ts timestamp);")
                .assertsPlan(expected);
        assertQuery("select count(distinct s) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct3() throws Exception {
        String expected = """
                Count
                    Async JIT Group By workers: 1
                      keys: [l]
                      filter: l!=null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: tab
                """;
        assertQuery("select count_distinct(l) from tab")
                .ddl("create table tab ( s string, l long )")
                .assertsPlan(expected);
        assertQuery("select count(distinct l) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct4() throws Exception {
        String expected = """
                Async Group By workers: 1
                  keys: [s]
                  values: [count_distinct(i)]
                  filter: null
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select s, count_distinct(i) from tab")
                .ddl("create table tab ( s string, i int )")
                .assertsPlan(expected);
        assertQuery("select s, count(distinct i) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct5() throws Exception {
        String expected = """
                Async Group By workers: 1
                  keys: [s]
                  values: [count_distinct(ip)]
                  filter: null
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select s, count_distinct(ip) from tab")
                .ddl("create table tab ( s string, ip ipv4 )")
                .assertsPlan(expected);
        assertQuery("select s, count(distinct ip) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct6() throws Exception {
        String expected = """
                Async Group By workers: 1
                  keys: [s]
                  values: [count_distinct(l)]
                  filter: null
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select s, count_distinct(l) from tab")
                .ddl("create table tab ( s string, l long )")
                .assertsPlan(expected);
        assertQuery("select s, count(distinct l) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct7() throws Exception {
        String expected = """
                Async JIT Group By workers: 1
                  vectorized: false
                  values: [count_distinct(s)]
                  filter: s='foobar'
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select count_distinct(s) from tab where s = 'foobar'")
                .ddl("create table tab (s symbol, ts timestamp);")
                .assertsPlan(expected);
        assertQuery("select count(distinct s) from tab where s = 'foobar'")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectCountDistinct8() throws Exception {
        String expected = """
                Async Group By workers: 1
                  vectorized: false
                  values: [count_distinct(s),first(s)]
                  filter: null
                    PageFrame
                        Row forward scan
                        Frame forward scan on: tab
                """;
        assertQuery("select count_distinct(s), first(s) from tab")
                .ddl("create table tab (s symbol, ts timestamp);")
                .assertsPlan(expected);
        assertQuery("select count(distinct s), first(s) from tab")
                .assertsPlan(expected);
    }

    @Test
    public void testSelectDesc() throws Exception {
        assertQuery("select * from a order by ts desc")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        PageFrame
                            Row backward scan
                            Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectDesc2() throws Exception {
        assertQuery("select * from a order by ts desc")
                .ddl("create table a ( i int, ts timestamp) ;")
                .assertsPlan("""
                        Encode sort light
                          keys: [ts desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectDescMaterialized() throws Exception {
        assertQuery("select * from (select i, ts from a union all select 1, null ) order by ts desc")
                .ddl("create table a ( i int, ts timestamp) ;")
                .assertsPlan("""
                        Encode sort
                          keys: [ts desc]
                            Union All
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                                VirtualRecord
                                  functions: [1,null]
                                    long_sequence count: 1
                        """);
    }

    @Test
    public void testSelectDistinct0() throws Exception {
        assertQuery("select distinct l, ts from tab")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [l,ts]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Ignore
    @Test // FIXME: somehow only ts gets included, pg returns record type
    public void testSelectDistinct0a() throws Exception {
        assertQuery("select distinct (l, ts) from tab")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        DistinctTimeSeries
                          keys: l,ts
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDistinct1() throws Exception {
        assertQuery("select distinct(l) from tab")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [l]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDistinct2() throws Exception {
        assertQuery("select distinct(s) from tab")
                .ddl("create table tab ( s symbol, ts timestamp);")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDistinct3() throws Exception {
        assertQuery("select distinct(s) from tab")
                .ddl("create table tab ( s symbol index, ts timestamp);")
                .assertsPlan("""
                        GroupBy vectorized: true workers: 1
                          keys: [s]
                          values: [count(*)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDistinct4() throws Exception {
        assertQuery("select distinct ts, l  from tab")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Group By workers: 1
                          keys: [ts,l]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDoubleInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( d double)");

            assertQuery("select * from t where d in (5, -1, 1, null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: d in [-1.0,1.0,5.0,NaN]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);

            assertQuery("select * from t where d not in (5, -1, 1, null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: not (d in [-1.0,1.0,5.0,NaN])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test // there's no interval scan because sysdate is evaluated per-row
    public void testSelectDynamicTsInterval1() throws Exception {
        assertQuery("select * from tab where ts > sysdate()")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: sysdate()<ts
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // there's no interval scan because systimestamp is evaluated per-row
    public void testSelectDynamicTsInterval2() throws Exception {
        assertQuery("select * from tab where ts > systimestamp()")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: systimestamp()<ts
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectDynamicTsInterval3() throws Exception {
        assertQuery("select * from tab where ts > now()")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("1970-01-01T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testSelectDynamicTsInterval4() throws Exception {
        assertQuery("select * from tab where ts > dateadd('d', -1, now()) and ts < now()")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("1969-12-31T00:00:00.000001Z","1969-12-31T23:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectDynamicTsInterval5() throws Exception {
        assertQuery("select * from tab where ts > '2022-01-01' and ts > now()")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2022-01-01T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testSelectDynamicTsInterval6() throws Exception {
        assertQuery("select * from tab where ts > '2022-01-01' and ts > now() order by ts desc")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row backward scan
                            Interval backward scan on: tab
                              intervals: [("2022-01-01T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testSelectFromAllTables() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from all_tables()")
                .noLeakCheck()
                .assertsPlan("all_tables()\n"));
    }

    @Test
    public void testSelectFromMemoryMetrics() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from memory_metrics()")
                .noLeakCheck()
                .assertsPlan("memory_metrics\n"));
    }

    @Test
    public void testSelectFromReaderPool() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from reader_pool()")
                .noLeakCheck()
                .assertsPlan("reader_pool\n"));
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        assertQuery("select * from table_columns('tab')")
                .ddl("create table tab ( s string, sy symbol, i int, ts timestamp)")
                .assertsPlan("show_columns of: tab\n");
    }

    @Test
    public void testSelectFromTablePartitions() throws Exception {
        assertQuery("select * from table_partitions('tab')")
                .ddl("create table tab ( s string, sy symbol, i int, ts timestamp)")
                .assertsPlan("show_partitions of: tab\n");
    }

    @Test
    public void testSelectFromTableWriterMetrics() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from table_writer_metrics()")
                .noLeakCheck()
                .assertsPlan("table_writer_metrics\n"));
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsAscNotPartitioned() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts desc limit 1 ")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s deferred: true
                                  filter: s='S1'
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsAscPartitioned() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts desc limit 1 ")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;")
                .assertsPlan("""
                        Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s deferred: true
                                  filter: s='S1'
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsDescNotPartitioned() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts desc limit 1 ")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s deferred: true
                                  filter: s='S1'
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsDescPartitioned() throws Exception {
        assertQuery("select * from a where s = 'S1' order by ts desc limit 1 ")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;")
                .assertsPlan("""
                        Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s deferred: true
                                  filter: s='S1'
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols01a() throws Exception {
        assertMemoryLeak(() -> {
            // if query is ordered by symbol and there's only one partition to scan, there's no need to sort
            testSelectIndexedSymbol("");
            testSelectIndexedSymbol("timestamp(ts)");
            testSelectIndexedSymbolWithIntervalFilter();
        });
    }

    @Test
    public void testSelectIndexedSymbols01b() throws Exception {
        // if query is ordered by symbol and there's more than partition to scan, then sort is necessary even if we use cursor order scan
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp)  timestamp(ts) partition by hour");
            execute("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

            String queryDesc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s desc limit 5";
            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            String expectedPlan = """
                    Encode sort light lo: 5
                      keys: [s#ORDER#]
                        FilterOnValues symbolOrder: desc
                            Cursor-order scan
                                Index forward scan on: s deferred: true
                                  filter: s=:s2::string
                                Index forward scan on: s deferred: true
                                  filter: s=:s1::string
                            Interval forward scan on: a
                              intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T23:59:59.999999Z")]
                    """;

            assertQuery(queryDesc)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", " desc"));
            assertQuery(queryDesc)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s	ts
                            S2	1970-01-01T00:00:00.000000Z
                            S2	1970-01-01T01:00:00.000003Z
                            S1	1970-01-01T00:00:00.000001Z
                            """);

            // order by asc
            String queryAsc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";
            assertQuery(queryAsc)
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", ""));
            assertQuery(queryAsc)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s	ts
                            S1	1970-01-01T00:00:00.000001Z
                            S2	1970-01-01T00:00:00.000000Z
                            S2	1970-01-01T01:00:00.000003Z
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols01c() throws Exception {
        assertQuery("select ts, s from a where s in ('S1', 'S2') and length(s) = 2 order by s desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("Limit value: 1 skip-rows-max: 0 take-rows-max: 1\n" + "    FilterOnValues symbolOrder: desc\n" + "        Cursor-order scan\n" + //actual order is S2, S1
                        "            Index forward scan on: s deferred: true\n" + "              symbolFilter: s='S2'\n" + "              filter: length(s)=2\n" + "            Index forward scan on: s deferred: true\n" + "              symbolFilter: s='S1'\n" + "              filter: length(s)=2\n" + "        Frame forward scan on: a\n");
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols02() throws Exception {
        assertQuery("select * from a where s = $1 or s = $2 order by ts desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: (s=$0::string or s=$1::string)
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols03() throws Exception {
        assertQuery("select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: (s='S1' or s='S2')
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols04() throws Exception {
        assertQuery("select * from a where s = 'S1' and s = 'S2' order by ts desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light lo: 1
                          keys: [ts desc]
                            Empty table
                        """);
    }

    @Test
    public void testSelectIndexedSymbols05() throws Exception {
        assertQuery("select * from a where s in (select 'S1' union all select 'S2') order by ts desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light lo: 1
                          keys: [ts desc]
                            FilterOnSubQuery
                                Union All
                                    VirtualRecord
                                      functions: ['S1']
                                        long_sequence count: 1
                                    VirtualRecord
                                      functions: ['S2']
                                        long_sequence count: 1
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols05a() throws Exception {
        assertQuery("select * from a where s in (select 'S1' union all select 'S2') and length(s) = 2 order by ts desc limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light lo: 1
                          keys: [ts desc]
                            FilterOnSubQuery
                              filter: length(s)=2
                                Union All
                                    VirtualRecord
                                      functions: ['S1']
                                        long_sequence count: 1
                                    VirtualRecord
                                      functions: ['S2']
                                        long_sequence count: 1
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols06() throws Exception {
        assertQuery("select * from a where s = 'S1' order by s asc limit 10")
                .ddl("create table a ( s symbol index) ;")
                .assertsPlan("""
                        Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: s deferred: true
                                  filter: s='S1'
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols06a() throws Exception {
        assertQuery("select * from a where s = 'S1' order by s asc limit 10")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [s]
                            DeferredSingleSymbolFilterPageFrame
                                Index forward scan on: s deferred: true
                                  filter: s='S1'
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols07NonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index)");
            String expectedPlan = """
                    FilterOnExcludedValues symbolOrder: #ORDER#
                      symbolFilter: s not in ['S1']
                      filter: length(s)=2
                        Cursor-order scan
                        Frame forward scan on: a
                    """;
            assertQuery("select * from a where s != 'S1' and length(s) = 2 order by s ")
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", "asc"));
            assertQuery("select * from a where s != 'S1' and length(s) = 2 order by s desc")
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", "desc"));
        });
    }

    @Test
    public void testSelectIndexedSymbols07Partitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day");

            String query = "select * from a where s != 'S1' and length(s) = 2 and ts in '2023-03-15' order by s #ORDER#";
            String expectedPlan = """
                    FilterOnExcludedValues symbolOrder: #ORDER#
                      symbolFilter: s not in ['S1']
                      filter: length(s)=2
                        Cursor-order scan
                        Interval forward scan on: a
                          intervals: [("2023-03-15T00:00:00.000000Z","2023-03-15T23:59:59.999999Z")]
                    """;

            assertQuery(query.replace("#ORDER#", "asc"))
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", "asc"));

            assertQuery(query.replace("#ORDER#", "desc"))
                    .noLeakCheck()
                    .assertsPlan(expectedPlan.replace("#ORDER#", "desc"));
        });
    }

    @Test
    public void testSelectIndexedSymbols08() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index)");
            execute("insert into a values ('a'), ('w'), ('b'), ('a'), (null);");

            String query = "select * from a where s != 'a' order by s";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            FilterOnExcludedValues symbolOrder: asc
                              symbolFilter: s not in ['a']
                                Cursor-order scan
                                    Index forward scan on: s
                                      filter: s=0
                                    Index forward scan on: s
                                      filter: s=3
                                    Index forward scan on: s
                                      filter: s=2
                                Frame forward scan on: a
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .returns("s\n" + "\n" +//null
                            "b\n" + "w\n");

            query = "select * from a where s != 'a' order by s desc";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            FilterOnExcludedValues symbolOrder: desc
                              symbolFilter: s not in ['a']
                                Cursor-order scan
                                    Index forward scan on: s
                                      filter: s=2
                                    Index forward scan on: s
                                      filter: s=3
                                    Index forward scan on: s
                                      filter: s=0
                                Frame forward scan on: a
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            s
                            w
                            b
                            
                            """/*null*/);

            query = "select * from a where s != null order by s desc";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            FilterOnExcludedValues symbolOrder: desc
                              symbolFilter: s not in [null]
                                Cursor-order scan
                                    Index forward scan on: s
                                      filter: s=2
                                    Index forward scan on: s
                                      filter: s=3
                                    Index forward scan on: s
                                      filter: s=1
                                Frame forward scan on: a
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            s
                            w
                            b
                            a
                            a
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols09() throws Exception {
        assertQuery("select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;")
                .assertsPlan("""
                        SortedSymbolIndex
                            Index forward scan on: s
                              symbolOrder: asc
                            Interval forward scan on: a
                              intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T00:00:00.000099Z")]
                        """);
    }

    @Test
    public void testSelectIndexedSymbols10() throws Exception {
        assertQuery("select * from a where s in ('S1', 'S2') limit 1")
                .ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                            FilterOnValues
                                Table-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s='S2'
                                    Index forward scan on: s deferred: true
                                      filter: s='S1'
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectIndexedSymbols10WithOrder() throws Exception {
        assertMemoryLeak(() -> {
            testSelectIndexedSymbols10WithOrder("");
            testSelectIndexedSymbols10WithOrder("partition by hour");
        });
    }

    @Test
    public void testSelectIndexedSymbols11() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");

            assertQuery("select * from a where s in ('S1', 'S2') and length(s) = 2 limit 1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                                FilterOnValues
                                    Table-order scan
                                        Index forward scan on: s
                                          filter: s=2 and length(s)=2
                                        Index forward scan on: s
                                          filter: s=1 and length(s)=2
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols12() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a where s1 in ('S1', 'S2') and s2 in ('S2') limit 1")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                                PageFrame
                                    Index forward scan on: s2
                                      filter: s2=2 and s1 in [S1,S2]
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols13() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a where s1 in ('S1')  order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s1
                                  filter: s1=1
                                Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols14() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a where s1 = 'S1'  order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s1
                                  filter: s1=1
                                Frame backward scan on: a
                            """);
        });
    }

    @Test // backward index scan is triggered only if query uses a single partition and orders by key column and ts desc
    public void testSelectIndexedSymbols15() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a " + "where s1 = 'S1' " + "and ts > 0::timestamp and ts < 9::timestamp  " + "order by s1,ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            DeferredSingleSymbolFilterPageFrame
                                Index backward scan on: s1
                                  filter: s1=1
                                Interval forward scan on: a
                                  intervals: [("1970-01-01T00:00:00.000001Z","1970-01-01T00:00:00.000008Z")]
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols16() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a " + "where s1 in ('S1', 'S2') " + "and ts > 0::timestamp and ts < 9::timestamp  " + "order by s1,ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            FilterOnValues symbolOrder: asc
                                Cursor-order scan
                                    Index backward scan on: s1
                                      filter: s1=1
                                    Index backward scan on: s1
                                      filter: s1=2
                                Interval forward scan on: a
                                  intervals: [("1970-01-01T00:00:00.000001Z","1970-01-01T00:00:00.000008Z")]
                            """);
        });
    }

    @Test // TODO: should use the same plan as above
    public void testSelectIndexedSymbols17() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertQuery("select * from a " + "where (s1 = 'S1' or s1 = 'S2') " + "and ts > 0::timestamp and ts < 9::timestamp  " + "order by s1,ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [s1, ts desc]
                                Async JIT Filter workers: 1
                                  filter: (s1='S1' or s1='S2')
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: a
                                          intervals: [("1970-01-01T00:00:00.000001Z","1970-01-01T00:00:00.000008Z")]
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols18() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by hour;");
            execute("insert into a select 'S' || (6-x), dateadd('m', 20*x::int, 0::timestamp) from long_sequence(5)");
            String query = "select * from " + "(" + "  select * from a " + "  where s1 not in ('S1', 'S2') " + "  order by ts asc " + "  limit 5" + ") order by ts asc";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                                FilterOnExcludedValues
                                  symbolFilter: s1 not in ['S1','S2']
                                    Table-order scan
                                        Index forward scan on: s1
                                          filter: s1=1
                                        Index forward scan on: s1
                                          filter: s1=2
                                        Index forward scan on: s1
                                          filter: s1=3
                                    Frame forward scan on: a
                            """);

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s1\tts
                            S5\t1970-01-01T00:20:00.000000Z
                            S4\t1970-01-01T00:40:00.000000Z
                            S3\t1970-01-01T01:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testSelectIndexedSymbols7b() throws Exception {
        assertQuery("select s from a where s != 'S1' and length(s) = 2 order by s ")
                .ddl("create table a ( ts timestamp, s symbol index) timestamp(ts);")
                .assertsPlan("""
                        FilterOnExcludedValues symbolOrder: asc
                          symbolFilter: s not in ['S1']
                          filter: length(s)=2
                            Cursor-order scan
                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectLongInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( l long)");

            assertQuery("select * from t where l in (5, -1, 1, null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: l in [null,-1,1,5]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);

            assertQuery("select * from t where l not in (5, -1, 1, null)")
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              filter: not (l in [null,-1,1,5])
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testSelectNoOrderByWithNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            assertQuery("select * from a limit -5")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -5 skip-rows: 5 take-rows: 5
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectNoOrderByWithNegativeLimitArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            assertQuery("select * from a limit -10+2")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -8 skip-rows: 2 take-rows: 8
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectOrderByTsAsIndexDescNegativeLimit() throws Exception {
        assertQuery("select * from a order by 2 desc limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Limit value: -10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsAsc() throws Exception {
        assertQuery("select * from a order by ts asc")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsAscAndDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            assertQuery("select * from (select * from a order by ts asc limit 5) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts desc]
                                Limit value: 5 skip-rows: 0 take-rows: 5
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectOrderByTsDescAndAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            assertQuery("select * from (select * from a order by ts desc limit 5) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Limit value: 5 skip-rows: 0 take-rows: 5
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit1() throws Exception {
        assertQuery("select * from a order by ts desc limit 9223372036854775806L+3L ")
                .ddl("create table a as (select rnd_int() i, timestamp_sequence(0, 100) ts from long_sequence(10000)) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -9223372036854775807L skip-rows: 0 take-rows: 10000
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit2() throws Exception {
        assertQuery("select * from a order by ts desc limit -1_000_000 ")
                .ddl("create table a as (select rnd_int() i, timestamp_sequence(0,100) ts from long_sequence(2_000_000)) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -1000000 skip-rows: 1000000 take-rows: 1000000
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsDescNegativeLimit() throws Exception {
        assertQuery("select * from a order by ts desc limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsWithNegativeLimit() throws Exception {
        assertQuery("select * from a order by ts  limit -5")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts)")
                .assertsPlan("""
                        Limit value: -5 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderByTsWithNegativeLimit1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            assertQuery("select ts, count(*)  from a sample by 1s ALIGN TO FIRST OBSERVATION limit -5")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -5 skip-rows: baseRows-5 take-rows-max: 5
                                Sample By
                                  fill: none
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);

            assertQuery("select i, count(*)  from a group by i limit -5")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -5 skip-rows: baseRows-5 take-rows-max: 5
                                GroupBy vectorized: true workers: 1
                                  keys: [i]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);

            assertQuery("select i, count(*)  from a limit -5")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -5 skip-rows: baseRows-5 take-rows-max: 5
                                GroupBy vectorized: true workers: 1
                                  keys: [i]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);

            assertQuery("select distinct(i) from a limit -5")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: -5 skip-rows: baseRows-5 take-rows-max: 5
                                GroupBy vectorized: true workers: 1
                                  keys: [i]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSelectOrderedAsc() throws Exception {
        assertQuery("select * from a order by i asc")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light
                          keys: [i]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderedDesc() throws Exception {
        assertQuery("select * from a order by i desc")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light
                          keys: [i desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectOrderedWithLimitLoHi() throws Exception {
        assertQuery("select * from a order by i limit 10, 100")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Encode sort light lo: 10 hi: 100
                          keys: [i]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectRandomBoolean() throws Exception {
        allowFunctionMemoization();
        assertMemoryLeak(() -> assertQuery("select rnd_boolean()")
                .noLeakCheck()
                .assertsPlan("""
                        VirtualRecord
                          functions: [memoize(rnd_boolean())]
                            long_sequence count: 1
                        """));
    }

    @Test
    public void testSelectStaticTsInterval1() throws Exception {
        assertQuery("select * from tab where ts > '2020-03-01'")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2020-03-01T00:00:00.000001Z","MAX")]
                        """);
    }

    @Test
    public void testSelectStaticTsInterval10() throws Exception {
        assertQuery("select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc ")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Encode sort light
                          keys: [l desc]
                            PageFrame
                                Row forward scan
                                Interval forward scan on: tab
                                  intervals: [("2020-01-01T03:00:00.000000Z","2020-01-01T03:59:59.999999Z"),("2020-01-02T03:00:00.000000Z","2020-01-02T03:59:59.999999Z"),("2020-01-03T03:00:00.000000Z","2020-01-03T03:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectStaticTsInterval10a() throws Exception {
        assertQuery("select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc, ts desc ")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Encode sort light
                          keys: [l desc, ts desc]
                            PageFrame
                                Row forward scan
                                Interval forward scan on: tab
                                  intervals: [("2020-01-01T03:00:00.000000Z","2020-01-01T03:59:59.999999Z"),("2020-01-02T03:00:00.000000Z","2020-01-02T03:59:59.999999Z"),("2020-01-03T03:00:00.000000Z","2020-01-03T03:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectStaticTsInterval2() throws Exception {
        assertQuery("select * from tab where ts in '2020-03-01'")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2020-03-01T00:00:00.000000Z","2020-03-01T23:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectStaticTsInterval3() throws Exception {
        assertQuery("select * from tab where ts in '2020-03-01' or ts in '2020-03-10'")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2020-03-01T00:00:00.000000Z","2020-03-01T23:59:59.999999Z"),("2020-03-10T00:00:00.000000Z","2020-03-10T23:59:59.999999Z")]
                        """);
    }

    @Test // ranges don't overlap so result is empty
    public void testSelectStaticTsInterval4() throws Exception {
        assertQuery("select * from tab where ts in '2020-03-01' and ts in '2020-03-10'")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("Empty table\n");
    }

    @Test // only 2020-03-10->2020-03-31 needs to be scanned
    public void testSelectStaticTsInterval5() throws Exception {
        assertQuery("select * from tab where ts in '2020-03' and ts > '2020-03-10'")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2020-03-10T00:00:00.000001Z","2020-03-31T23:59:59.999999Z")]
                        """);
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval6() throws Exception {
        assertQuery("select * from tab where (ts > '2020-03-01' and ts < '2020-03-10') or (ts > '2020-04-01' and ts < '2020-04-10') ")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ((2020-03-01T00:00:00.000000Z<ts and ts<2020-03-10T00:00:00.000000Z) or (2020-04-01T00:00:00.000000Z<ts and ts<2020-04-10T00:00:00.000000Z))
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval7() throws Exception {
        assertQuery("select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') ")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: (ts between 1583020800000000 and 1583798400000000 or ts between 1585699200000000 and 1586476800000000)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectStaticTsInterval8() throws Exception {
        assertQuery("select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' ")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: tab
                              intervals: [("2020-01-01T03:00:00.000000Z","2020-01-01T03:59:59.999999Z"),("2020-01-02T03:00:00.000000Z","2020-01-02T03:59:59.999999Z"),("2020-01-03T03:00:00.000000Z","2020-01-03T03:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectStaticTsInterval9() throws Exception {
        assertQuery("select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc")
                .ddl("create table tab ( l long, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        PageFrame
                            Row backward scan
                            Interval backward scan on: tab
                              intervals: [("2020-01-01T03:00:00.000000Z","2020-01-01T03:59:59.999999Z"),("2020-01-02T03:00:00.000000Z","2020-01-02T03:59:59.999999Z"),("2020-01-03T03:00:00.000000Z","2020-01-03T03:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws Exception {
        assertQuery("select * from tab where ts > '2020-03-01'")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 2020-03-01T00:00:00.000000Z<ts
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWalTransactions() throws Exception {
        assertQuery("select * from wal_transactions('tab')")
                .ddl("create table tab ( s string, sy symbol, i int, ts timestamp) timestamp(ts) partition by day WAL")
                .assertsPlan("wal_transactions of: tab\n");
    }

    @Test
    public void testSelectWhereOrderByLimit1() throws Exception {
        assertQuery("select * from xx where str = 'A' order by str,x limit 10")
                .ddl("create table xx ( x long, str string) ")
                .assertsPlan("""
                        Async Top K lo: 10 workers: 1
                          filter: str='A'
                          keys: [str, x]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: xx
                        """);
    }

    @Test
    public void testSelectWhereOrderByLimit2() throws Exception {
        assertQuery("select * from xx where str is not null order by str,x limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        Async JIT Top K lo: 10 workers: 1
                          filter: str is not null
                          keys: [str, x]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: xx
                        """);
    }

    @Test
    public void testSelectWhereOrderByLimit3() throws Exception {
        assertQuery("select * from xx order by id desc, x limit 10")
                .ddl("create table xx ( x long, id uuid ) ")
                .assertsPlan("""
                        Async Top K lo: 10 workers: 1
                          filter: null
                          keys: [id desc, x]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: xx
                        """);
    }

    @Test // projection wrapper + JIT filter still hits top-K (issue #6528)
    public void testSelectWhereOrderByLimit4() throws Exception {
        assertQuery("select x, * from xx where str is not null order by str desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            Async JIT Top K lo: 10 workers: 1
                              filter: str is not null
                              keys: [str desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: xx
                        """);
    }

    @Test // virtual-column projection + JIT filter still hits top-K
    public void testSelectWhereOrderByLimit5() throws Exception {
        assertQuery("select x + 1 as xp from xx where str is not null order by str desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            VirtualRecord
                              functions: [x+1,str]
                                Async JIT Top K lo: 10 workers: 1
                                  filter: str is not null
                                  keys: [str desc]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: xx
                        """);
    }

    @Test // bare virtual-column wrapper (no outer SelectedRecord) still hits top-K
    public void testSelectWhereOrderByLimit6() throws Exception {
        assertQuery("select x + 1 as xp, * from xx where str is not null order by str desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        VirtualRecord
                          functions: [x+1,x,str]
                            Async JIT Top K lo: 10 workers: 1
                              filter: str is not null
                              keys: [str desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: xx
                        """);
    }

    @Test // projection without filter: peel SelectedRecord, run top-K on base, rewrap projection
    public void testSelectWhereOrderByLimit7() throws Exception {
        assertQuery("select x, * from xx order by str desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            Async Top K lo: 10 workers: 1
                              filter: null
                              keys: [str desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: xx
                        """);
    }

    @Test // ORDER BY on a computed column must fall back to Sort light
    public void testSelectWhereOrderByLimit8() throws Exception {
        assertQuery("select x + 1 as xp from xx where str is not null order by xp desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        Encode sort light lo: 10
                          keys: [xp desc]
                            VirtualRecord
                              functions: [x+1]
                                Async JIT Filter workers: 1
                                  filter: str is not null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: xx
                        """);
    }

    @Test // two-bound LIMIT is not a top-K candidate; Sort light handles it
    public void testSelectWhereOrderByLimit9() throws Exception {
        assertQuery("select x, * from xx where str is not null order by str desc limit 10, 20")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        Encode sort light lo: 10 hi: 20
                          keys: [str desc]
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  filter: str is not null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: xx
                        """);
    }

    @Test // memoized passthrough column at ORDER BY position must still hit top-K
    public void testSelectWhereOrderByLimit_memoizedPassthrough() throws Exception {
        // generateSelectVirtual wraps slot 0's `x` ColumnFunction in a memoizer
        // because alias `a` is referenced more than once. translateOrderByColumnToBase
        // and getLongTopKColumnIndex must peel the wrapper via ColumnFunction.unwrap;
        // otherwise the gate falls back to Sort light.
        allowFunctionMemoization();
        assertQuery("select x + 1 as xp, x as a, x as b, str from xx where str is not null order by a desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        VirtualRecord
                          functions: [x+1,memoize(x),x,str]
                            Async JIT Top K lo: 10 workers: 1
                              filter: str is not null
                              keys: [x desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: xx
                        """);
    }

    @Test // multi-key ORDER BY translates every key through a SelectedRecord wrapper
    public void testSelectWhereOrderByLimit_multiKeyThroughSelectedRecord() throws Exception {
        assertQuery("select x, * from xx where str is not null order by str desc, x asc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            Async JIT Top K lo: 10 workers: 1
                              filter: str is not null
                              keys: [str desc, x]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: xx
                        """);
    }

    @Test // multi-key ORDER BY translates every key through a VirtualRecord wrapper
    public void testSelectWhereOrderByLimit_multiKeyThroughVirtualRecord() throws Exception {
        assertQuery("select x + 1 as xp, x from xx where str is not null order by str desc, x asc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            VirtualRecord
                              functions: [x+1,x,str]
                                Async JIT Top K lo: 10 workers: 1
                                  filter: str is not null
                                  keys: [str desc, x]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: xx
                        """);
    }

    @Test // VirtualRecord over JIT filter: projection peel reaches leaf; outer SelectedRecord is added later
    public void testSelectWhereOrderByLimit_virtualRecordPeel() throws Exception {
        // ORDER BY str references a column absent from the SELECT list, forcing the
        // optimizer to keep str inside VirtualRecord for the sort but project it away on top.
        // The top-K gate fires while recordCursorFactory is the VirtualRecord wrapper:
        // translateOrderByColumnToBase peels VirtualRecord -> JIT filter leaf in a single step.
        // The outer SelectedRecord visible in the plan is added afterwards by generateSelectChoose
        // and is not what the gate inspects.
        assertQuery("select x + 1 as xp, x from xx where str is not null order by str desc limit 10")
                .ddl("create table xx ( x long, str varchar ) ")
                .assertsPlan("""
                        SelectedRecord
                            VirtualRecord
                              functions: [x+1,x,str]
                                Async JIT Top K lo: 10 workers: 1
                                  filter: str is not null
                                  keys: [str desc]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: xx
                        """);
    }

    @Test
    public void testSelectWithJittedFilter1() throws Exception {
        assertQuery("select * from tab where l > 100 ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 100<l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter10() throws Exception {
        assertQuery("select * from tab where s in ( 'A', 'B' )")
                .ddl("create table tab ( s symbol, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: s in [A,B]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: this one should interval scan without filter
    public void testSelectWithJittedFilter11() throws Exception {
        assertQuery("select * from tab where ts in ( '2020-01-01', '2020-01-02' )")
                .ddl("create table tab ( s symbol, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ts in [1577836800000000,1577923200000000]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter12() throws Exception {
        assertQuery("select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'")
                .ddl("create table tab ( s symbol, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (ts in [1577836800000000,1578009600000000] and s='ABC')
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter13() throws Exception {
        assertQuery("select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'")
                .ddl("create table tab ( s symbol, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (ts in [1577836800000000,1577923199999999] and s='ABC')
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter14() throws Exception {
        assertQuery("select * from tab where l = 12 or l = 15 ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (l=12 or l=15)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter15() throws Exception {
        assertQuery("select * from tab where l = 12.345 ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l=12.345
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter16() throws Exception {
        assertQuery("select * from tab where b = false ")
                .ddl("create table tab ( b boolean, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: b=false
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter17() throws Exception {
        assertQuery("select * from tab where not(b = false or ts = 123) ")
                .ddl("create table tab ( b boolean, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (b!=false and 123!=ts)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter18() throws Exception {
        assertQuery("select * from tab where l1 < l2 ")
                .ddl("create table tab ( l1 long, l2 long);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l1<l2
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter19() throws Exception {
        assertQuery("select * from tab where l1 * l2 > 0  ")
                .ddl("create table tab ( l1 long, l2 long);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: 0<l1*l2
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter2() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (100<l and l<1000)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter20() throws Exception {
        assertQuery("select * from tab where l1 * l2 > l3  ")
                .ddl("create table tab ( l1 long, l2 long, l3 long);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l3<l1*l2
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter21() throws Exception {
        assertQuery("select * from tab where l = $1 ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l=$0::long
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter22() throws Exception {
        assertQuery("select * from tab where d = 1024.1 + 1 ")
                .ddl("create table tab ( d double, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: d=1025.1
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter23() throws Exception {
        assertQuery("select * from tab where d = null ")
                .ddl("create table tab ( d double, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: d is null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24a() throws Exception {
        assertQuery("select * from tab where d = 1.2 order by ts limit 1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24b() throws Exception {
        assertQuery("select * from tab where d = 1.2 order by ts limit -1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24b2() throws Exception {
        assertQuery("select * from tab where d = 1.2 limit -1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24c() throws Exception {
        assertQuery("select * from tab where d = 1.2 order by ts desc limit 1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24d() throws Exception {
        assertQuery("select * from tab where d = 1.2 limit -1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter24e() throws Exception {
        bindVariableService.setInt("maxRows", -1);

        assertQuery("select * from tab where d = 1.2 limit :maxRows ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter25() throws Exception {
        assertQuery("select * from tab where d = 1.2 order by ts desc limit 1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter26() throws Exception {
        assertQuery("select * from tab where d = 1.2 order by ts limit -1 ")
                .ddl("create table tab ( d double, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          limit: 1
                          filter: d=1.2
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter27() throws Exception {
        assertQuery("select * from tab where s = null ")
                .ddl("create table tab (s string, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: s is null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter28() throws Exception {
        assertQuery("select * from tab where v = null ")
                .ddl("create table tab (v varchar, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: v is null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter3() throws Exception {
        assertQuery("select /*+ ENABLE_PRE_TOUCH(tab) */ * from tab where l > 100 and l < 1000 and ts = '2022-01-01' ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (100<l and l<1000 and 2022-01-01T00:00:00.000000Z=ts) [pre-touch]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter4() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 and l = 20")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (100<l and l<1000 and l=20)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter5() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 or l = 20")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ((100<l and l<1000) or l=20)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter6() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 or ts = 123")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ((100<l and l<1000) or 123=ts)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter7() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'")
                .ddl("create table tab ( l long, ts timestamp) timestamp (ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ((100<l and l<1000) or 2021-01-01T00:00:00.000000Z<ts)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithJittedFilter8() throws Exception {
        assertQuery("select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'")
                .ddl("create table tab ( l long, ts timestamp) timestamp (ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: (100<l and l<1000)
                            PageFrame
                                Row forward scan
                                Interval forward scan on: tab
                                  intervals: [("2021-01-01T00:00:00.000000Z","2021-01-01T23:59:59.999999Z")]
                        """);
    }

    @Test
    public void testSelectWithJittedFilter9() throws Exception {
        assertQuery("select * from tab where l in ( 100, 200 )")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l in [100,200]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithLimitLo() throws Exception {
        assertQuery("select * from a limit 10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: 10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithLimitLoHi() throws Exception {
        assertQuery("select * from a limit 10, 100")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit left: 10 right: 100 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithLimitLoHiNegative() throws Exception {
        assertQuery("select * from a limit -10, -100")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit left: -10 right: -100 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithLimitLoNegative() throws Exception {
        assertQuery("select * from a limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter1() throws Exception {
        assertQuery("select * from tab where l = 12::short ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=12
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter10() throws Exception {
        assertQuery("select * from tab where s = 1::short ")
                .ddl("create table tab ( s short, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: s=1
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: should run with jitted filter just like b = true
    public void testSelectWithNonJittedFilter11() throws Exception {
        assertQuery("select * from tab where b = true::boolean ")
                .ddl("create table tab ( b boolean, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: b=true
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: should run with jitted filter just like l = 1024
    public void testSelectWithNonJittedFilter12() throws Exception {
        assertQuery("select * from tab where l = 1024::long ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=1024L
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: should run with jitted filter just like d = 1024.1
    public void testSelectWithNonJittedFilter13() throws Exception {
        assertQuery("select * from tab where d = 1024.1::double ")
                .ddl("create table tab ( d double, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: d=1024.1
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // TODO: should run with jitted filter just like d = null
    public void testSelectWithNonJittedFilter14() throws Exception {
        assertQuery("select * from tab where d = null::double ")
                .ddl("create table tab ( d double, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: d is null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter15() throws Exception {
        assertQuery("select * from tab where (l | l) > 0  ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: 0<l|l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter16() throws Exception {
        assertQuery("select * from tab where (l & l) > 0  ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: 0<l&l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter17() throws Exception {
        assertQuery("select * from tab where (l ^ l) > 0  ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: 0<l^l
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithNonJittedFilter18() throws Exception {
        assertQuery("select * from tab where (l ^ l) > 0 limit -1")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          limit: 1
                          filter: 0<l^l
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithNonJittedFilter19() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong("maxRows", -1);

        assertQuery("select * from tab where (l ^ l) > 0 limit :maxRows")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          limit: 1
                          filter: 0<l^l
                            PageFrame
                                Row backward scan
                                Frame backward scan on: tab
                        """);
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter2() throws Exception {
        assertQuery("select * from tab where l = 12::byte ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=12
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter3() throws Exception {
        assertQuery("select * from tab where l = '123' ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l='123'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit is not because rnd_long() value is not stable
    public void testSelectWithNonJittedFilter4() throws Exception {
        // Async filter function doesn't support memoization.
        assertQuery("select * from tab where l = rnd_long() ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=rnd_long()
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithNonJittedFilter5() throws Exception {
        assertQuery("select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=case([0<l,1,l=0,0,-1])
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // interval scan is not used because of type mismatch
    public void testSelectWithNonJittedFilter6() throws Exception {
        assertQuery("select * from tab where l = $1::string ")
                .ddl("create table tab ( l long, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l=$0::string
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit filter doesn't work for string type
    public void testSelectWithNonJittedFilter7() throws Exception {
        assertQuery("select * from tab where s = 'test' ")
                .ddl("create table tab ( s string, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: s='test'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter9() throws Exception {
        assertQuery("select * from tab where b = 1::byte ")
                .ddl("create table tab ( b byte, ts timestamp);")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: b=1
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tab
                        """);
    }

    @Test
    public void testSelectWithNotOperator() throws Exception {
        assertQuery("select * from tst where timestamp not between '2021-01-01' and '2021-01-10' ")
                .ddl("CREATE TABLE tst ( timestamp TIMESTAMP );")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: not (timestamp between 1609459200000000 and 1610236800000000)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tst
                        """);
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLo() throws Exception {
        assertQuery("select * from a order by ts desc limit 10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: 10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative1() throws Exception {
        assertQuery("select * from a order by ts desc limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row backward scan
                                Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative2() throws Exception {
        assertQuery("select i from a order by ts desc limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Limit value: -10 skip-rows: 0 take-rows: 0
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: a
                        """);
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative1() throws Exception {
        assertQuery("select * from a order by ts limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Limit value: -10 skip-rows: 0 take-rows: 0
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative2() throws Exception {
        assertQuery("select i from a order by ts limit -10")
                .ddl("create table a ( i int, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Limit value: -10 skip-rows: 0 take-rows: 0
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder1() throws Exception {
        assertQuery("select ts, l, i from a where l<i")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: l<i
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2() throws Exception {
        assertQuery("select ts, l, i from a where l::short<i")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: l::short<i
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2a() throws Exception {
        assertQuery("select i2, i1, ts1 from " + "(select ts as ts1, l as l1, i as i1, i as i2 " + "from a " + "where l::short<i " + "limit 100) " + "where l1*i2 != 0")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: l1*i2!=0
                                SelectedRecord
                                    Async Filter workers: 1
                                      limit: 100
                                      filter: l::short<i
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2b() throws Exception {
        assertQuery("select i2, i1, ts1 from " + "(select ts as ts1, l as l1, i as i1, i as i2 " + "from a " + "order by ts, l1 " + "limit 100 ) " + "where i1*i2 != 0")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Filter filter: i1*i2!=0
                            SelectedRecord
                                Encode sort light lo: 100 partiallySorted: true
                                  keys: [ts, l1]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2c() throws Exception {
        assertQuery("select i2, i1, ts, ts1 from " + "(select ts, ts as ts1, l as l1, i as i1, i as i2 " + "from a " + "order by ts, l1 " + "limit 100 ) " + "where i1*i2 != 0")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: i1*i2!=0
                                Encode sort light lo: 100 partiallySorted: true
                                  keys: [ts, l1]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2d() throws Exception {
        assertQuery("select i2, i1, ts, ts1 from " + "(select ts, ts as ts1, l as l1, i as i1, i as i2 " + "from a " + "order by 1, 3 " + "limit 100 ) " + "where i1*i2 != 0")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: i1*i2!=0
                                Encode sort light lo: 100 partiallySorted: true
                                  keys: [ts, l1]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder2e() throws Exception {
        assertQuery("select i2, i1, ts, ts1 from " + "(select ts, ts as ts1, l as l1, i as i1, i as i2 " + "from a " + "order by 2, 3 " + "limit 100 ) " + "where i1*i2 != 0")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        SelectedRecord
                            Filter filter: i1*i2!=0
                                Encode sort light lo: 100 partiallySorted: true
                                  keys: [ts1, l1]
                                    SelectedRecord
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder3() throws Exception {
        assertQuery("select k, max(ts) from ( select ts, l as k, i from a where l::short<i ) where k < 0 ")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        GroupBy vectorized: false
                          keys: [k]
                          values: [max(ts)]
                            SelectedRecord
                                Async Filter workers: 1
                                  filter: (l::short<i and l<0)
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelectWithReorder4() throws Exception {
        assertQuery("select mil, k, minl, mini from " + "( select ts as k, max(i*l) as mil, min(i) as mini, min(l) as minl  " + "from a where l::short<i ) " + "where mil + mini> 1 ")
                .ddl("create table a ( i int, l long, ts timestamp) timestamp(ts) ;")
                .assertsPlan("""
                        Filter filter: 1<mil+mini
                            Async Group By workers: 1
                              keys: [k]
                              values: [max(i*l),min(l),min(i)]
                              filter: l::short<i
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                        """);
    }

    @Test
    public void testSelfJoinFilterPushedThroughView() throws Exception {
        // Regression lock-in for a self-join wrapped in a subquery: both join instances reference the
        // same table and the same column name 'k'. The constant pinned to the master instance
        // (a.k='x') must propagate to the slave instance (b.k) without conflating the two
        // identically-named columns - addTransitiveFilters matches on both name AND join-model index,
        // so only the slave's own key is filtered. ORDER BY makes the hash-join output deterministic.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL INDEX, v STRING)");
            execute("INSERT INTO t VALUES ('x', 'x1'), ('x', 'x2'), ('y', 'y1')");
            assertQuery("""
                    SELECT * FROM (
                      SELECT a.k AS ka, a.v AS av, b.v AS bv
                      FROM t a JOIN t b ON b.k = a.k
                    ) WHERE ka = 'x' ORDER BY av, bv""")
                    .noLeakCheck()
                    .returns("""
                            ka\tav\tbv
                            x\tx1\tx1
                            x\tx1\tx2
                            x\tx2\tx1
                            x\tx2\tx2
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain1a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows: 0 take-rows: 0
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain1b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc, l desc limit 10) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light lo: 10 partiallySorted: true
                              keys: [ts desc, l desc]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc, l limit 10) lt join (select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    Encode sort light lo: 10 partiallySorted: true
                                      keys: [ts, l]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from " + "(select * from (select * from a order by ts asc, l) limit 10) " + "lt join " + "(select * from a) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        Encode sort light
                                          keys: [ts, l]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from " + "(select * from (select * from a order by ts desc, l desc) limit 10) " + "order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    Encode sort light
                                      keys: [ts desc, l desc]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from " + "(select * from " + "   (select * from a) " + "    cross join " + "   (select * from a) " + " order by ts asc, l  " + " limit 10" + ") " + "lt join (select * from a) " + "order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Lt Join Fast
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        Encode sort
                                          keys: [ts, l]
                                            SelectedRecord
                                                Cross Join
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from " + "(select * from " + "   (select * from a) " + "    cross join " + "   (select * from a) " + " order by ts desc " + " limit 10" + ") " + "order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows: 0 take-rows: 0
                                SelectedRecord
                                    Cross Join
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortAscLimitAndSortDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts asc limit 10) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts desc]
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSortDescLimitAndSortAgain() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 10 skip-rows: 0 take-rows: 0
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: a
                            """);
        });
    }

    @Test
    public void testSortDescLimitAndSortAsc1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from (select * from a order by ts desc limit 10) order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Limit value: 10 skip-rows: 0 take-rows: 0
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: a
                            """);
        });
    }

    @Test // TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long)");

            assertQuery("select * from (select * from a order by ts, l limit 10) order by ts, l")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts, l]
                                Async Top K lo: 10 workers: 1
                                  filter: null
                                  keys: [ts, l]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                            """);
        });
    }

    @Test // TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long)");

            assertQuery("select * from (select * from a order by ts, l limit 10,-10) order by ts, l")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts, l]
                                Limit left: 10 right: -10 skip-rows: 0 take-rows: 0
                                    Encode sort light
                                      keys: [ts, l]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                            """);
        });
    }

    @Test
    public void testSpliceJoin0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            execute("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from a splice join b on ts where a.i = b.ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.ts
                                    Splice Join
                                      condition: b.ts=a.ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testSpliceJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            execute("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertQuery("select * from a splice join b on ts where a.i + b.i = 1")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i+b.i=1
                                    Splice Join
                                      condition: b.ts=a.ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testSpliceJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a splice join b on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Splice Join
                                  condition: b.ts=a.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testSpliceJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a splice join (select * from b limit 10) on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Splice Join
                                  condition: _xQdbA1.ts=a.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Limit value: 10 skip-rows: 0 take-rows: 0
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testSpliceJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Splice Join
                                  condition: _xQdbA1.ts=a.ts
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: a
                                    Encode sort light
                                      keys: [ts, i]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testSpliceJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            assertQuery("select * from a splice join b where a.i = b.i")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: a.i=b.i
                                    Splice Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: a
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: b
                            """);
        });
    }

    @Test
    public void testStringToDoubleArrayPlanDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select '{}'::double[] from long_sequence(1)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: ['{}'::DOUBLE[]]
                                long_sequence count: 1
                            """);


            assertQuery("select '{}'::double[][][] from long_sequence(1)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: ['{}'::DOUBLE[][][]]
                                long_sequence count: 1
                            """);
        });
    }

    @Test
    public void testTimestampEqSubQueryFilter1() throws Exception {
        assertQuery("select * from x where ts = (select min(ts) from x)")
                .ddl("create table x (l long, ts timestamp)")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: ts=cursor\s
                            Async Group By workers: 1
                              vectorized: true
                              values: [min(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            PageFrame
                                Row forward scan
                                Frame forward scan on: x
                        """);
    }

    @Test
    public void testTimestampEqSubQueryFilter2() throws Exception {
        assertQuery("select * from x where ts = (select min(ts) from x)")
                .ddl("create table x (l long, ts timestamp) timestamp(ts) partition by day")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: x
                              intervals: []
                        """);
    }

    @Test
    public void testTimestampGtSubQueryFilter1() throws Exception {
        assertQuery("select * from x where ts > (select min(ts) from x)")
                .ddl("create table x (l long, ts timestamp)")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: ts [thread-safe] > cursor\s
                            Async Group By workers: 1
                              vectorized: true
                              values: [min(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            PageFrame
                                Row forward scan
                                Frame forward scan on: x
                        """);
    }

    @Test
    public void testTimestampGtSubQueryFilter2() throws Exception {
        assertQuery("select * from x where ts > (select min(ts) from x)")
                .ddl("create table x (l long, ts timestamp) timestamp(ts) partition by day")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: x
                              intervals: []
                        """);
    }

    @Test
    public void testTimestampLtSubQueryFilter1() throws Exception {
        assertQuery("select * from x where ts < (select max(ts) from x)")
                .ddl("create table x (l long, ts timestamp)")
                .assertsPlan("""
                        Async Filter workers: 1
                          filter: ts [thread-safe] < cursor\s
                            Async Group By workers: 1
                              vectorized: true
                              values: [max(ts)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            PageFrame
                                Row forward scan
                                Frame forward scan on: x
                        """);
    }

    @Test
    public void testTimestampLtSubQueryFilter2() throws Exception {
        assertQuery("select * from x where ts < (select max(ts) from x)")
                .ddl("create table x (l long, ts timestamp) timestamp(ts) partition by day")
                .assertsPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: x
                              intervals: []
                        """);
    }

    @Test
    public void testUnion() throws Exception {
        assertQuery("select * from a union select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Union
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testUnionAll() throws Exception {
        assertQuery("select * from a union all select * from a")
                .ddl("create table a ( i int, s string);")
                .assertsPlan("""
                        Union All
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testWhereOrderByTsLimit1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( x long, ts timestamp) timestamp(ts)");

            String query = "select * from t where x < 100 order by ts desc limit -5";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlan("""
                            Async JIT Filter workers: 1
                              limit: 5
                              filter: x<100
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """);

            execute("insert into t select x, x::timestamp from long_sequence(10000)");

            assertQuery(query)
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            x\tts
                            5\t1970-01-01T00:00:00.000005Z
                            4\t1970-01-01T00:00:00.000004Z
                            3\t1970-01-01T00:00:00.000003Z
                            2\t1970-01-01T00:00:00.000002Z
                            1\t1970-01-01T00:00:00.000001Z
                            """);
        });
    }

    @Test
    public void testWhereUuid() throws Exception {
        assertQuery("select u, ts from a where u = '11111111-1111-1111-1111-111111111111' or u = '22222222-2222-2222-2222-222222222222' or u = '33333333-3333-3333-3333-333333333333'")
                .ddl("create table a (u uuid, ts timestamp) timestamp(ts);")
                .assertsPlan("""
                        Async JIT Filter workers: 1
                          filter: ((u='11111111-1111-1111-1111-111111111111' or u='22222222-2222-2222-2222-222222222222') or u='33333333-3333-3333-3333-333333333333')
                            PageFrame
                                Row forward scan
                                Frame forward scan on: a
                        """);
    }

    @Test
    public void testWindow0() throws Exception {
        assertQuery("select ts, str,  row_number() over (order by l), row_number() over (partition by l) from t")
                .ddl("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))")
                .assertsPlan("""
                        CachedWindowLight
                          orderedFunctions: [[l] => [row_number()]]
                          unorderedFunctions: [row_number() over (partition by [l])]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: t
                        """);
    }

    @Test
    public void testWindow1() throws Exception {
        assertQuery("select str, ts, l, 10, row_number() over ( partition by l order by ts) from t")
                .ddl("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))")
                .assertsPlan("""
                        CachedWindowLight
                          orderedFunctions: [[ts] => [row_number() over (partition by [l])]]
                            VirtualRecord
                              functions: [str,ts,l,10]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                        """);
    }

    @Test
    public void testWindow2() throws Exception {
        assertQuery("select str, ts, l as l1, ts::long+l as tsum, row_number() over ( partition by l, ts order by str) from t")
                .ddl("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))")
                .assertsPlan("""
                        CachedWindowLight
                          orderedFunctions: [[str] => [row_number() over (partition by [l1,ts])]]
                            VirtualRecord
                              functions: [str,ts,l1,ts::long+l1]
                                SelectedRecord
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                        """);
    }

    @Test
    public void testWindow3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertQuery("select ts, i, j, " + "avg(j) over (order by i, j rows unbounded preceding), " + "sum(j) over (order by i, j rows unbounded preceding), " + "first_value(j) over (order by i, j rows unbounded preceding), " + "from tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            CachedWindowLight
                              orderedFunctions: [[i, j] => [avg(j) over (rows between unbounded preceding and current row),\
                            sum(j) over (rows between unbounded preceding and current row),first_value(j) over ()]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("select ts, i, j, " + "avg(j) over (partition by i order by ts rows between 1 preceding and current row), " + "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " + "first_value(j) over (partition by i order by ts rows between 1 preceding and current row) " + "from tab")
                    .noLeakCheck()
                    .assertsPlan("""
                            Window
                              functions: [avg(j) over (partition by [i] rows between 1 preceding and current row),\
                            sum(j) over (partition by [i] rows between 1 preceding and current row),first_value(j) over (partition by [i] rows between 1 preceding and current row)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """);

            assertQuery("select row_number() over (partition by i order by i desc, j asc), " + "avg(j) over (partition by i order by j, i desc rows unbounded preceding), " + "sum(j) over (partition by i order by j, i desc rows unbounded preceding), " + "first_value(j) over (partition by i order by j, i desc rows unbounded preceding) " + "from tab " + "order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  orderedFunctions: [[i desc, j] => [row_number() over (partition by [i])],\
                            [j, i desc] => [avg(j) over (partition by [i] rows between unbounded preceding and current row),\
                            sum(j) over (partition by [i] rows between unbounded preceding and current row),\
                            first_value(j) over (partition by [i] rows between unbounded preceding and current row)]]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: tab
                            """);

            assertQuery("select row_number() over (partition by i order by i desc, j asc), " + "        avg(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " + "        sum(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " + "        first_value(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " + "        rank() over (partition by j, i) " + "from tab order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                CachedWindowLight
                                  orderedFunctions: [[i desc, j] => [row_number() over (partition by [i]),avg(j) over (partition by [i,j] rows between unbounded preceding and current row),\
                            sum(j) over (partition by [i,j] rows between unbounded preceding and current row),\
                            first_value(j) over (partition by [i,j] rows between unbounded preceding and current row)]]
                                  unorderedFunctions: [rank() over (partition by [j,i])]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: tab
                            """);
        });
    }

    @Test
    public void testWindowJoinAndOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int, b int, ts timestamp) timestamp(ts);");

            assertQuery("select x1.a, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a]
                                Async Window Join workers: 1
                                  vectorized: false
                                  window lo: 1000000 preceding (include prevailing)
                                  window hi: 2000000 following
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by 10*x1.a")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort
                                  keys: [column]
                                    VirtualRecord
                                      functions: [a,sum,10*a]
                                        Async Window Join workers: 1
                                          vectorized: false
                                          window lo: 1000000 preceding (include prevailing)
                                          window hi: 2000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.a, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a desc]
                                Async Window Join workers: 1
                                  vectorized: false
                                  window lo: 1000000 preceding (include prevailing)
                                  window hi: 2000000 following
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery("select 10*x1.a as a10, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by a10")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [a10]
                                VirtualRecord
                                  functions: [10*a,sum]
                                    Async Window Join workers: 1
                                      vectorized: false
                                      window lo: 1000000 preceding (include prevailing)
                                      window hi: 2000000 following
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort
                                  keys: [column desc, a0]
                                    VirtualRecord
                                      functions: [a0,sum,10*a0]
                                        Async Window Join workers: 1
                                          vectorized: false
                                          window lo: 1000000 preceding (include prevailing)
                                          window hi: 2000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.a as a0, sum(x1.b), x1.ts from x x1 window join x x2 range between 1 second preceding and 2 second following order by 10*x1.a desc, 1 asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort
                                  keys: [column desc, a0]
                                    VirtualRecord
                                      functions: [a0,sum,ts,10*a0]
                                        Async Window Join workers: 1
                                          vectorized: false
                                          window lo: 1000000 preceding (include prevailing)
                                          window hi: 2000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);

            assertQuery("select x1.ts, to_utc(x1.ts, 'Europe/Berlin') berlin_ts, x1.a as a0, sum(x1.b) from x x1 window join x x2 range between 1 second preceding and 2 second following order by 10*x1.a desc, 3 asc, berlin_ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Encode sort
                                  keys: [column desc, a0, berlin_ts desc]
                                    VirtualRecord
                                      functions: [ts,to_utc(ts),a0,sum,10*a0]
                                        Async Window Join workers: 1
                                          vectorized: false
                                          window lo: 1000000 preceding (include prevailing)
                                          window hi: 2000000 following
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testWindowModelOrderByIsNotIgnored() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertQuery("""
                    select sum(avg), sum(sum), sum(first_value) from (
                    select ts, hostname, usage_system, \
                    avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, \
                    sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, \
                    first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value \
                    from cpu_ts \
                    order by ts desc
                    )\s""")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [sum(avg),sum(sum),sum(first_value)]
                                Window
                                  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: cpu_ts
                            """);

            assertQuery("select sum(avg), sum(sum), sum(first_value) from (\n" + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from cpu_ts " + ") ")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [sum(avg),sum(sum),sum(first_value)]
                                Window
                                  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cpu_ts
                            """);

            assertQuery("""
                    select sum(avg), sum(sum) sm, sum(first_value) fst from (
                    select ts, hostname, usage_system, \
                    avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, \
                    sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, \
                    first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value \
                    from cpu_ts \
                    order by ts asc
                    ) order by sm\s""")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [sm]
                                GroupBy vectorized: false
                                  values: [sum(avg),sum(sum),sum(first_value)]
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)\
                            ]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);
        });
    }

    @Test
    public void testWindowOrderByUnderWindowModelIsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertQuery("select sum(avg), sum(sum), first(first_value) from ( " + "select ts, hostname, usage_system, " + "avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from cpu_ts " + "order by ts desc" + ") ")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [sum(avg),sum(sum),first(first_value)]
                                Window
                                  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                    PageFrame
                                        Row backward scan
                                        Frame backward scan on: cpu_ts
                            """);

            assertQuery("select sum(avg), sum(sum), first(first_value) from ( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + ") order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [sum desc]
                                GroupBy vectorized: false
                                  values: [sum(avg),sum(sum),first(first_value)]
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: cpu_ts
                            """);

            assertQuery("select sum(avg), sum(sum), count(first_value) from ( " +
                    "select ts, hostname, usage_system, " +
                    "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                    "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                    "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                    "from (select * from cpu_ts order by ts desc) " +
                    ") order by 1 desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [sum desc]
                                GroupBy vectorized: false
                                  values: [sum(avg),sum(sum),count(first_value)]
                                    CachedWindowLight
                                      orderedFunctions: [[ts desc] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);
        });
    }

    // TODO: remove artificial limit models used to force ordering on window models (and avoid unnecessary sorts)
    @Test
    public void testWindowParentModelOrderPushdownIsBlockedWhenWindowModelSpecifiesOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value, " + "from cpu_ts " + "order by ts desc " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [ts]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from cpu_ts " + "order by ts asc " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [ts desc]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from cpu_ts " + "order by ts asc " + ") order by hostname")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [hostname]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + ") order by ts asc ")
                    .noLeakCheck()
                    .assertsPlan("""
                            CachedWindowLight
                              orderedFunctions: [[ts desc] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts asc) " + ") order by ts desc ")
                    .noLeakCheck()
                    .assertsPlan("""
                            CachedWindowLight
                              orderedFunctions: [[ts] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts asc) " + ") order by hostname ")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [hostname]
                                Window
                                  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + ") order by hostname ")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [hostname]
                                Window
                                  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts asc) " + "order by ts desc " + ") order by ts asc ")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [ts]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row backward scan
                                            Frame backward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + "order by ts asc " + ") order by ts desc ")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [ts desc]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc ) " + "order by ts asc " + ") order by hostname ")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
                              keys: [hostname]
                                Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                                    Window
                                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                            first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cpu_ts
                            """);
        });
    }

    @Test
    public void testWindowParentModelOrderPushdownIsDoneWhenNestedModelsSpecifyNoneOrMatchingOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            String expectedForwardPlan = """
                    Window
                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: cpu_ts
                    """;

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from cpu_ts " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value, " + "from (select * from cpu_ts order by ts asc) " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by hostname) " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardPlan);

            String expectedForwardLimitPlan = """
                    Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                        Window
                          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cpu_ts
                    """;

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from cpu_ts " + "order by ts asc  " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardLimitPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts asc) " + "order by ts asc  " + ") order by ts asc")
                    .noLeakCheck()
                    .assertsPlan(expectedForwardLimitPlan);

            String expectedBackwardPlan = """
                    Window
                      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                        PageFrame
                            Row backward scan
                            Frame backward scan on: cpu_ts
                    """;
            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from cpu_ts " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts asc) " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by hostname) " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardPlan);

            String expectedBackwardLimitPlan = """
                    Limit value: 9223372036854775807L skip-rows: 0 take-rows: 0
                        Window
                          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row),\
                    first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: cpu_ts
                    """;

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from cpu_ts " + "order by ts desc  " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardLimitPlan);

            assertQuery("select * from " + "( " + "select ts, hostname, usage_system, " + "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " + "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " + "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " + "from (select * from cpu_ts order by ts desc) " + "order by ts desc  " + ") order by ts desc")
                    .noLeakCheck()
                    .assertsPlan(expectedBackwardLimitPlan);
        });
    }

    @Test
    public void testWindowRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as ( " + "  select " + "    cast(x as int) i, " + "    rnd_symbol('a','b','c') sym, " + "    timestamp_sequence(0, 100000000) ts " + "   from long_sequence(100)" + ") timestamp(ts) partition by hour");

            String sql = "select i, " + "row_number() over (partition by sym), " + "avg(i) over (partition by i rows unbounded preceding), " + "sum(i) over (partition by i rows unbounded preceding), " + "first_value(i) over (partition by i rows unbounded preceding) " + "from x limit 3";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlan("""
                            Limit value: 3 skip-rows: 0 take-rows: 3
                                Window
                                  functions: [row_number() over (partition by [sym]),\
                            avg(i) over (partition by [i] rows between unbounded preceding and current row),\
                            sum(i) over (partition by [i] rows between unbounded preceding and current row),\
                            first_value(i) over (partition by [i] rows between unbounded preceding and current row)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: x
                            """);

            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            i\trow_number\tavg\tsum\tfirst_value
                            1\t1\t1.0\t1.0\t1
                            2\t2\t2.0\t2.0\t2
                            3\t1\t3.0\t3.0\t3
                            """);
        });
    }

    @Test
    public void testWithBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( x int );");

            int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                bindVariableService.clear();
                bindVariableService.setStr("v1", "a");
                assertBindVarPlan("string");

                bindVariableService.clear();
                bindVariableService.setLong("v1", 123);
                assertBindVarPlan("long");

                bindVariableService.clear();
                bindVariableService.setByte("v1", (byte) 123);
                assertBindVarPlan("byte");

                bindVariableService.clear();
                bindVariableService.setShort("v1", (short) 123);
                assertBindVarPlan("short");

                bindVariableService.clear();
                bindVariableService.setInt("v1", 12345);
                assertBindVarPlan("int");

                bindVariableService.clear();
                bindVariableService.setFloat("v1", 12f);
                assertBindVarPlan("float");

                bindVariableService.clear();
                bindVariableService.setDouble("v1", 12d);
                assertBindVarPlan("double");

                bindVariableService.clear();
                bindVariableService.setDate("v1", 123456L);
                assertBindVarPlan("date");

                bindVariableService.clear();
                bindVariableService.setTimestamp("v1", 123456L);
                assertBindVarPlan("timestamp");
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    private static boolean isEqSymTimestampFactory(FunctionFactory factory) {
        if (factory instanceof EqSymTimestampFunctionFactory) {
            return true;
        }
        if (factory instanceof ArgSwappingFunctionFactory) {
            return ((ArgSwappingFunctionFactory) factory).getDelegate() instanceof EqSymTimestampFunctionFactory;
        }

        if (factory instanceof NegatingFunctionFactory) {
            if (((NegatingFunctionFactory) factory).getDelegate() instanceof ArgSwappingFunctionFactory) {
                return ((ArgSwappingFunctionFactory) ((NegatingFunctionFactory) factory).getDelegate()).getDelegate() instanceof EqSymTimestampFunctionFactory;
            }
            return ((NegatingFunctionFactory) factory).getDelegate() instanceof EqSymTimestampFunctionFactory;
        }

        return false;
    }

    private static boolean isIPv4StrFactory(FunctionFactory factory) {
        if (factory instanceof ArgSwappingFunctionFactory) {
            return isIPv4StrFactory(((ArgSwappingFunctionFactory) factory).getDelegate());
        }
        if (factory instanceof NegatingFunctionFactory) {
            return isIPv4StrFactory(((NegatingFunctionFactory) factory).getDelegate());
        }
        return factory instanceof EqIPv4FunctionFactory || factory instanceof EqIPv4StrFunctionFactory || factory instanceof LtIPv4StrFunctionFactory || factory instanceof LtStrIPv4FunctionFactory;
    }

    private static boolean isLong256StrFactory(FunctionFactory factory) {
        if (factory instanceof ArgSwappingFunctionFactory) {
            return isLong256StrFactory(((ArgSwappingFunctionFactory) factory).getDelegate());
        }
        if (factory instanceof NegatingFunctionFactory) {
            return isLong256StrFactory(((NegatingFunctionFactory) factory).getDelegate());
        }
        return factory instanceof EqLong256StrFunctionFactory;
    }

    private void assertBindVarPlan(String type) throws Exception {
        assertQuery("select * from t where x = :v1 ")
                .noLeakCheck()
                .assertsPlan("Async Filter workers: 1\n" + "  filter: x=:v1::" + type + "\n" + "    PageFrame\n" + "        Row forward scan\n" + "        Frame forward scan on: t\n");
    }

    private void assertNotSupportedByExplain(String sql, SqlExecutionContextImpl sqlExecutionContext) {
        try (RecordCursorFactory ignored = engine.select(sql, sqlExecutionContext)) {
            fail("Expected exception missing");
        } catch (SqlException e) {
            assertEquals(8, e.getPosition());
            assertContains(e.getFlyweightMessage(), "'create', 'format', 'insert', 'update', 'select' or 'with' expected");
        }
    }

    private void assertReadOnlyOperationAllowed(String sql, SqlExecutionContextImpl sqlExecutionContext) throws SqlException {
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            assertFalse(factory.isProjection());
        }
    }

    private void assertWritePermissionDenied(String sql, SqlExecutionContextImpl sqlExecutionContext) throws SqlException {
        try (RecordCursorFactory ignored = engine.select(sql, sqlExecutionContext)) {
            fail("Expected exception missing");
        } catch (CairoException e) {
            assertContains(e.getFlyweightMessage(), "Write permission denied");
        }
    }

    private Function getConst(IntObjHashMap<ObjList<Function>> values, int type, int paramNo, int iteration) {
        //use param number to work around rnd factories validation logic
        int val = paramNo + 1;

        switch (type) {
            case ColumnType.BYTE:
                return new ByteConstant((byte) val);
            case ColumnType.SHORT:
                return new ShortConstant((short) val);
            case ColumnType.INT:
                return new IntConstant(val);
            case ColumnType.IPv4:
                return new IPv4Constant(val);
            case ColumnType.LONG:
                return new LongConstant(val);
            case ColumnType.DATE:
                return new DateConstant(val * 86_400_000L);
            case ColumnType.TIMESTAMP_MICRO:
                return new TimestampConstant(val * 86_400_000L, ColumnType.TIMESTAMP_MICRO);
            case ColumnType.TIMESTAMP_NANO:
                return new TimestampConstant(val * 86_400_000L, ColumnType.TIMESTAMP_NANO);
            default:
                ObjList<Function> availableValues = values.get(type);
                if (availableValues != null) {
                    int n = availableValues.size();
                    return availableValues.get(iteration % n);
                } else {
                    return null;
                }
        }
    }

    // you cannot win with JDK8, without "SafeVarargs" - a warning we corrupt something
    // with "SafeVarargs" - JDK8 wants private method to be "final", even more final than private.
    // this bunch of suppressions is to shut intellij code inspection up
    @SuppressWarnings("FinalPrivateMethod")
    @SafeVarargs
    private final <T> ObjList<T> list(T... values) {
        return new ObjList<>(values);
    }

    private void test2686Prepare() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table table_1 (
                              ts timestamp,
                              name string,
                              age int,
                              member boolean
                            ) timestamp(ts) PARTITION by month""");

            execute("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            execute("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            execute("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            execute("""
                    create table table_2 (
                              ts timestamp,
                              name string,
                              age int,
                              address string
                            ) timestamp(ts) PARTITION by month""");

            execute("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            execute("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
        });
    }

    // left join maintains order metadata and can be part of asof join
    private void testHashAndAsOfJoin(SqlCompiler compiler, boolean isLight, boolean isFastAsOfJoin) throws Exception {
        execute("create table taba (a1 int, ts1 timestamp) timestamp(ts1)");
        execute("create table tabb (b1 int, b2 long, ts2 timestamp) timestamp(ts2)");
        execute("create table tabc (c1 int, c2 long, ts3 timestamp) timestamp(ts3)");

        String asofJoinType = isFastAsOfJoin ? " Fast" : (isLight ? "Light" : "");
        assertQuery("select * " + "from taba " + "left join tabb on a1=b1 " + "asof join tabc on b1=c1")
                .withCompiler(compiler)
                .noLeakCheck()
                .assertsPlan("SelectedRecord\n" + "    AsOf Join" + asofJoinType + "\n" + "      condition: c1=b1\n" + "        Hash Left Outer Join" + (isLight ? " Light" : "") + "\n" + "          condition: b1=a1\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            Hash\n" + "                PageFrame\n" + "                    Row forward scan\n" + "                    Frame forward scan on: tabb\n" + "        PageFrame\n" + "            Row forward scan\n" + "            Frame forward scan on: tabc\n");
        assertQuery("select * " + "from taba " + "asof join tabb on a1=b1 " + "right join tabc on b1=c1")
                .withCompiler(compiler)
                .noLeakCheck()
                .assertsPlan("SelectedRecord\n" + "    Hash Right Outer Join" + (isLight ? " Light" : "") + "\n" + "      condition: c1=b1\n" + "        AsOf Join" + asofJoinType + "\n" + "          condition: b1=a1\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabc\n");
        assertQuery("select * " + "from taba " + "asof join tabb on a1=b1 " + "full join tabc on b1=c1")
                .withCompiler(compiler)
                .noLeakCheck()
                .assertsPlan("SelectedRecord\n" + "    Hash Full Outer Join" + (isLight ? " Light" : "") + "\n" + "      condition: c1=b1\n" + "        AsOf Join" + asofJoinType + "\n" + "          condition: b1=a1\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: taba\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabb\n" + "        Hash\n" + "            PageFrame\n" + "                Row forward scan\n" + "                Frame forward scan on: tabc\n");
    }

    private void testSelectIndexedSymbol(String timestampAndPartitionByClause) throws Exception {
        execute("drop table if exists a");
        execute("create table a ( s symbol index, ts timestamp) " + timestampAndPartitionByClause);
        execute("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

        String query = "select * from a where s in (:s1, :s2) order by s desc limit 5";
        bindVariableService.clear();
        bindVariableService.setStr("s1", "S1");
        bindVariableService.setStr("s2", "S2");

        // even though plan shows cursors in S1, S2 order, FilterOnValues sorts them before query execution
        // actual order is S2, S1
        assertQuery(query)
                .noLeakCheck()
                .assertsPlan("""
                        Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                            FilterOnValues symbolOrder: desc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                Frame forward scan on: a
                        """);

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        s\tts
                        S2\t1970-01-01T00:00:00.000000Z
                        S2\t1970-01-01T01:00:00.000003Z
                        S1\t1970-01-01T00:00:00.000001Z
                        """);

        //order by asc
        query = "select * from a where s in (:s1, :s2) order by s asc limit 5";

        assertQuery(query)
                .noLeakCheck()
                .assertsPlan("""
                        Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                            FilterOnValues symbolOrder: asc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                Frame forward scan on: a
                        """);

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        s\tts
                        S1\t1970-01-01T00:00:00.000001Z
                        S2\t1970-01-01T00:00:00.000000Z
                        S2\t1970-01-01T01:00:00.000003Z
                        """);
    }

    @SuppressWarnings("SameParameterValue")
    private void testSelectIndexedSymbolWithIntervalFilter() throws Exception {
        execute("drop table if exists a");
        execute("create table a ( s symbol index, ts timestamp) " + "timestamp(ts) partition by day");
        execute("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

        String query = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s desc limit 5";
        bindVariableService.clear();
        bindVariableService.setStr("s1", "S1");
        bindVariableService.setStr("s2", "S2");

        // even though plan shows cursors in S1, S2 order, FilterOnValues sorts them before query execution
        // actual order is S2, S1
        assertQuery(query)
                .noLeakCheck()
                .assertsPlan("""
                        Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                            FilterOnValues symbolOrder: desc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                Interval forward scan on: a
                                  intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T23:59:59.999999Z")]
                        """);

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        s\tts
                        S2\t1970-01-01T00:00:00.000000Z
                        S2\t1970-01-01T01:00:00.000003Z
                        S1\t1970-01-01T00:00:00.000001Z
                        """);

        //order by asc
        query = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";

        assertQuery(query)
                .noLeakCheck()
                .assertsPlan("""
                        Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                            FilterOnValues symbolOrder: asc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                Interval forward scan on: a
                                  intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T23:59:59.999999Z")]
                        """);

        assertQuery(query)
                .noLeakCheck()
                .returns("""
                        s\tts
                        S1\t1970-01-01T00:00:00.000001Z
                        S2\t1970-01-01T00:00:00.000000Z
                        S2\t1970-01-01T01:00:00.000003Z
                        """);
    }

    private void testSelectIndexedSymbols10WithOrder(String partitionByClause) throws Exception {
        execute("drop table if exists a");
        execute("create table a ( s symbol index, ts timestamp) timestamp(ts)" + partitionByClause);
        execute("insert into a values ('S2', 1), ('S3', 2),('S1', 3+3600000000),('S2', 4+3600000000), ('S1', 5+3600000000);");

        bindVariableService.clear();
        bindVariableService.setStr("s1", "S1");
        bindVariableService.setStr("s2", "S2");

        String queryAsc = "select * from a where s in (:s2, :s1) order by ts asc limit 5";
        assertQuery(queryAsc)
                .noLeakCheck()
                .assertsPlan("""
                        Limit value: 5 skip-rows-max: 0 take-rows-max: 5
                            FilterOnValues
                                Table-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                Frame forward scan on: a
                        """);
        assertQuery(queryAsc)
                .noLeakCheck()
                .timestamp("ts")
                .returns("""
                        s\tts
                        S2\t1970-01-01T00:00:00.000001Z
                        S1\t1970-01-01T01:00:00.000003Z
                        S2\t1970-01-01T01:00:00.000004Z
                        S1\t1970-01-01T01:00:00.000005Z
                        """);

        String queryDesc = "select * from a where s in (:s2, :s1) order by ts desc limit 5";
        assertQuery(queryDesc)
                .noLeakCheck()
                .assertsPlan("""
                        Encode sort light lo: 5
                          keys: [ts desc]
                            FilterOnValues symbolOrder: desc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s=:s2::string
                                    Index forward scan on: s deferred: true
                                      filter: s=:s1::string
                                Frame backward scan on: a
                        """);
        assertQuery(queryDesc)
                .noLeakCheck()
                .timestampDesc("ts")
                .expectSize()
                .returns("""
                        s\tts
                        S1\t1970-01-01T01:00:00.000005Z
                        S2\t1970-01-01T01:00:00.000004Z
                        S1\t1970-01-01T01:00:00.000003Z
                        S2\t1970-01-01T00:00:00.000001Z
                        """);
    }
}
