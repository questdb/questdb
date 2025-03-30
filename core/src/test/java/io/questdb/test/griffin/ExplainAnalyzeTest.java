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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.griffin.engine.functions.SwappingArgsFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampIntervalFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InUuidFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToRegClassFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToStrArrayFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.StringToStringArrayFunction;
import io.questdb.griffin.engine.functions.catalogue.WalTransactionsFunctionFactory;
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
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorOffsetFunctionFactory;
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
import io.questdb.griffin.engine.functions.finance.LevelTwoPriceFunctionFactory;
import io.questdb.griffin.engine.functions.json.JsonExtractTypedFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtStrIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.math.GreatestNumericFunctionFactory;
import io.questdb.griffin.engine.functions.math.LeastNumericFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndIPv4CCFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndSymbolListFunctionFactory;
import io.questdb.griffin.engine.functions.table.HydrateTableMetadataFunctionFactory;
import io.questdb.griffin.engine.functions.table.ReadParquetFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestSumXDoubleGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.window.LagDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.window.LeadDoubleFunctionFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class ExplainAnalyzeTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(ExplainAnalyzeTest.class);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    public void selectLeakCheck(CharSequence selectSql) throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(selectSql));
    }

    public void selectLeakCheck(CharSequence ddl, CharSequence selectSql) throws Exception {
        assertMemoryLeak(() -> {
            execute(ddl);
            selectNoLeakCheck(selectSql);
        });
    }

    public void selectLeakCheck(CharSequence ddl1, CharSequence ddl2, CharSequence selectSql) throws Exception {
        assertMemoryLeak(() -> {
            execute(ddl1);
            execute(ddl2);
            selectNoLeakCheck(selectSql);
        });
    }

    @SuppressWarnings({"StatementWithEmptyBody"})
    public void selectNoLeakCheck(CharSequence selectSql) throws Exception {
        try (RecordCursorFactory factory = select("EXPLAIN ANALYZE " + selectSql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            }
        }
    }


    @SuppressWarnings({"StatementWithEmptyBody"})
    public void selectNoLeakCheck(CharSequence ddl, CharSequence selectSql) throws Exception {
        execute(ddl);
        try (RecordCursorFactory factory = select("EXPLAIN ANALYZE " + selectSql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            }
        }
    }


    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        engine.getMatViewGraph().clear();
        inputRoot = root;
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotMoveOtherInnerJoinPredicate() throws Exception {
        test2686Prepare();

        selectLeakCheck("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                "from table_1 as a \n" +
                "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                "join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10 ");
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotMoveOtherLeftJoinPredicate() throws Exception {
        test2686Prepare();

        selectLeakCheck("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                "from table_1 as a \n" +
                "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                "left join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10 ");
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotMoveOtherTwoTableEqJoinPredicate() throws Exception {
        test2686Prepare();

        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and a.age = b.age "
        );
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotPushJoinPredicateToLeftTable() throws Exception {
        test2686Prepare();

        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and a.age = 10 "
        );
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotPushJoinPredicateToRightTable() throws Exception {
        test2686Prepare();

        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts \n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and b.age = 10 "
        );
    }

    @Test
    public void testAnalyze2686LeftJoinDoesNotPushWherePredicateToRightTable() throws Exception {
        test2686Prepare();

        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)" +
                        "where b.age = 10 "
        );
    }

    @Test
    public void testAnalyze2686LeftJoinPushesWherePredicateToLeftJoinCondition() throws Exception {
        test2686Prepare();

        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "where a.age * b.age = 10"
        );
    }

    @Test
    public void testAnalyze2686LeftJoinPushesWherePredicateToLeftTable() throws Exception {
        test2686Prepare();
        selectLeakCheck(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)" +
                        "where a.age = 10 "
        );
    }

    @Test
    public void testAnalyzeAsOfJoin0() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b on ts where a.i = b.ts::int"
        );
    }

    @Test
    public void testAnalyzeAsOfJoin0a() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select ts, ts1, i, i1 from (select * from a asof join b on ts ) where i/10 = i1"
        );
    }

    @Test
    public void testAnalyzeAsOfJoin1() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b on ts"
        );
    }

    @Test
    public void testAnalyzeAsOfJoin2() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join (select * from b limit 10) on ts"
        );
    }

    @Test
    public void testAnalyzeAsOfJoin3() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts"
        );
    }

    @Test
    public void testAnalyzeAsOfJoin4() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * " +
                        "from a " +
                        "asof join b on ts " +
                        "asof join a c on ts"
        );
    }

    @Test // where clause predicate can't be pushed to join clause because asof is and outer join
    public void testAnalyzeAsOfJoin5() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * " +
                        "from a " +
                        "asof join b " +
                        "where a.i = b.i"
        );
    }

    @Test
    public void testAnalyzeAsOfJoinNoKey() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b where a.i > 0"
        );
    }

    @Test
    public void testAnalyzeAsOfJoinNoKeyFast1() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b"
        );
    }

    @Test
    public void testAnalyzeAsOfJoinNoKeyFast2() throws Exception {
        selectLeakCheck(
                "create table a (i int, ts timestamp) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b on(ts)"
        );
    }

    @Test
    public void testAnalyzeAsOfJoinNoKeyFast3() throws Exception {
        selectLeakCheck(
                "create table a (ts timestamp, i int) timestamp(ts)",
                "create table b (i int, ts timestamp) timestamp(ts)",
                "select * from a asof join b on(ts)"
        );
    }

    @Test
    public void testAnalyzeCachedWindowRecordCursorFactoryWithLimit() throws Exception {
        selectLeakCheck(
                "create table x as ( " +
                        "  select " +
                        "    cast(x as int) i, " +
                        "    rnd_symbol('a','b','c') sym, " +
                        "    timestamp_sequence(0, 100000000) ts " +
                        "   from long_sequence(100)" +
                        ") timestamp(ts) partition by hour",
                "select i, " +
                        "row_number() over (partition by sym), " +
                        "avg(i) over (), " +
                        "sum(i) over (), " +
                        "first_value(i) over (), " +
                        "from x limit 3"
        );
    }

    @Test
    public void testAnalyzeCastFloatToDouble() throws Exception {
        selectLeakCheck(
                "select rnd_float()::double "
        );
    }

    @Test
    public void testAnalyzeCountOfColumnsVectorized() throws Exception {
        selectLeakCheck("create table x " +
                        "(" +
                        " k int, " +
                        " i int, " +
                        " l long, " +
                        " f float, " +
                        " d double, " +
                        " dat date, " +
                        " ts timestamp " +
                        ")",
                "select k, count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts " +
                        "from x");
    }

    @Test
    public void testAnalyzeCrossJoin0() throws Exception {
        selectLeakCheck("create table a ( i int, s1 string, s2 string)",
                "select * from a cross join a b where length(a.s1) = length(b.s2)");
    }

    @Test
    public void testAnalyzeCrossJoin0Output() throws Exception {
        selectLeakCheck("create table a as (select x, 's' || x as s1, 's' || (x%3) as s2 from long_sequence(3))",
                "select count(*) cnt from a cross join a b where length(a.s1) = length(b.s2)");
    }

    @Test
    public void testAnalyzeCrossJoin1() throws Exception {
        selectLeakCheck(
                "create table a ( i int)",
                "select * from a cross join a b"
        );
    }

    @Test
    public void testAnalyzeCrossJoin2() throws Exception {
        selectLeakCheck(
                "create table a ( i int)",
                "select * from a cross join a b cross join a c"
        );
    }

    @Test
    public void testAnalyzeCrossJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {
                    "select * from t t1 cross join t t2 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 cross join t t2 order by t1.ts"
            };
            for (String query : queries) {
                selectNoLeakCheck(
                        query
                );

                assertQueryNoLeakCheck(
                        "x\tts\tx1\tts1\n" +
                                "1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                                "1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000002Z\n" +
                                "2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                                "2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\n",
                        query,
                        "ts",
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testAnalyzeCrossJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "cross join t t2 " +
                    "order by t1.ts desc";

            selectNoLeakCheck(
                    query
            );
        });
    }

    @Test
    public void testAnalyzeCrossJoinWithSort3() throws Exception {
        selectLeakCheck(
                "create table t (x int, ts timestamp) timestamp(ts)",
                "select * from " +
                        "((select * from t order by ts asc) limit 10) t1 " +
                        "cross join t t2 " +
                        "order by t1.ts asc"
        );
    }

    @Test
    public void testAnalyzeCrossJoinWithSort4() throws Exception {
        selectLeakCheck(
                "create table t (x int, ts timestamp) timestamp(ts)",
                "select * from " +
                        "((select * from t order by ts asc) limit 10) t1 " +
                        "cross join t t2 " +
                        "order by t1.ts desc"
        );
    }

    @Test
    public void testAnalyzeCrossJoinWithSort5() throws Exception {
        selectLeakCheck("create table t (x int, ts timestamp) timestamp(ts)",
                "select * from " +
                        "((select * from t order by ts asc) limit 10) t1 " +
                        "cross join t t2 " +
                        "order by t1.ts asc"
        );
    }

    @Test
    public void testAnalyzeDistinctOverWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (event double )");
            execute("insert into test select x from long_sequence(3)");

            String query = "select * from ( SELECT DISTINCT avg(event) OVER (PARTITION BY 1) FROM test )";
            selectNoLeakCheck(
                    query
            );
        });
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit1() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit2() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 desc limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit3() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit4() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit5a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit5b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit6a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit6b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctTsWithLimit7() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10, 20"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit1() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di order by 1 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit2() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di order by 1 desc limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit3() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit4() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit5a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di where y = 5 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit5b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di where y = 5 limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit6a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit6b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit -10"
        );
    }

    @Test
    public void testAnalyzeDistinctWithLimit7() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10, 20"
        );
    }

    @Test
    public void testAnalyzeExcept() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a except select * from a"
        );
    }

    @Test
    public void testAnalyzeExceptAll() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a except all select * from a"
        );
    }

    @Test
    public void testAnalyzeExceptAndSort1() throws Exception {
        selectLeakCheck("create table a ( i int, ts timestamp, l long) timestamp(ts)",
                "select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts desc"
        );
    }

    @Test
    public void testAnalyzeExceptAndSort2() throws Exception {
        selectLeakCheck("create table a ( i int, ts timestamp, l long) timestamp(ts)",
                "select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts asc"
        );
    }

    @Test
    public void testAnalyzeExceptAndSort3() throws Exception {
        selectLeakCheck("create table a ( i int, ts timestamp, l long) timestamp(ts)",
                "select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts asc");
    }

    @Test
    public void testAnalyzeExceptAndSort4() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp, l long) timestamp(ts)",
                "select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts desc"
        );
    }

    @Test
    public void testAnalyzeExplainCreateMatView() throws Exception {
        selectLeakCheck(
                "create table tab (ts timestamp, k symbol, v long) timestamp(ts) partition by day wal",
                "create materialized view test as (select ts, k, avg(v) from tab sample by 30s) partition by day"
        );
    }

    @Test
    public void testAnalyzeExplainCreateTable() throws Exception {
        selectLeakCheck(
                "create table a ( l long, d double)"
        );
    }

    @Test
    public void testAnalyzeExplainCreateTableAsSelect() throws Exception {
        selectLeakCheck(
                "create table a as (select x, 1 from long_sequence(10))");

    }

    @Test
    public void testAnalyzeExplainDeferredSingleSymbolFilterPageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab \n" +
                    "(\n" +
                    "   id symbol index,\n" +
                    "   ts timestamp,\n" +
                    "   val double  \n" +
                    ") timestamp(ts);");
            execute("insert into tab values ( 'XXX', 0::timestamp, 1 );");

            selectNoLeakCheck(
                    "  select\n" +
                            "   ts,\n" +
                            "    id, \n" +
                            "    last(val)\n" +
                            "  from tab\n" +
                            "  where id = 'XXX' \n" +
                            "  sample by 15m ALIGN to CALENDAR\n"
            );

        });
    }

    @Test
    public void testAnalyzeExplainInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            selectNoLeakCheck("insert into a values (1, 2.0)"
            );
        });
    }

    @Test
    public void testAnalyzeExplainInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            selectNoLeakCheck("insert into a select x, 1 from long_sequence(10)"
            );
        });
    }

    @Test
    public void testAnalyzeExplainPlanNoTrailingQuote() throws Exception {
        selectLeakCheck(
                "(format json) select * from long_sequence(1)"
        );
    }

    @Test
    public void testAnalyzeExplainPlanWithEOLs1() throws Exception {
        selectLeakCheck(
                "create table a (s string)",
                "select * from a where s = '\b\f\n\r\t\\u0013'"
        );
    }

    @Test
    public void testAnalyzeExplainSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            selectNoLeakCheck("select * from a");
        });
    }

    @Test
    public void testAnalyzeExplainSelectWithCte1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "with b as (select * from a where i = 0)" +
                        "select * from a union all select * from b"
        );
    }

    @Test
    public void testAnalyzeExplainSelectWithCte2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "with b as (select i from a order by s)" +
                        "select * from a join b on a.i = b.i"
        );
    }

    @Test
    public void testAnalyzeExplainSelectWithCte3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Limit lo: 10 skip-over-rows: 0 limit: 0\n" +
                    "    PageFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: a\n", "explain with b as (select * from a limit 10) select * from b;"
            );
        });
    }

    @Test
    public void testAnalyzeExplainUpdate1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Update table: a\n" +
                    "    VirtualRecord\n" +
                    "      functions: [1,10.1]\n" +
                    "        PageFrame\n" +
                    "            Row forward scan\n" +
                    "            Frame forward scan on: a\n", "explain update a set l = 1, d=10.1;"
            );
        });
    }

    @Test
    public void testAnalyzeExplainUpdate2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( l1 long, d1 double)");
            execute("create table b ( l2 long, d2 double)");
            assertSql("QUERY PLAN\n" +
                    "Update table: a\n" +
                    "    VirtualRecord\n" +
                    "      functions: [1,d1]\n" +
                    "        SelectedRecord\n" +
                    "            Hash Join Light\n" +
                    "              condition: l2=l1\n" +
                    "                PageFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: a\n" +
                    "                Hash\n" +
                    "                    PageFrame\n" +
                    "                        Row forward scan\n" +
                    "                        Frame forward scan on: b\n", "explain update a set l1 = 1, d1=d2 from b where l1=l2;"
            );
        });
    }

    @Test
    public void testAnalyzeExplainUpdateWithFilter() throws Exception {
        selectLeakCheck(
                "create table a ( l long, d double, ts timestamp) timestamp(ts)",
                "update a set l = 20, d = d+rnd_double() " +
                        "where d < 100.0d and ts > dateadd('d', 1, now()  );"
        );
    }

    @Test
    public void testAnalyzeExplainWindowFunctionWithCharConstantFrameBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab ( key int, value double, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '1' MINUTES PRECEDING AND CURRENT ROW) from tab"
            );

            selectNoLeakCheck(
                    "select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '4' MINUTES PRECEDING AND '3' MINUTES PRECEDING) from tab"
            );

            selectNoLeakCheck(
                    "select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND '10' MINUTES PRECEDING) from tab"
            );
        });
    }

    @Test
    public void testAnalyzeExplainWithJsonFormat1() throws Exception {
        assertQuery(
                "QUERY PLAN\n" +
                        "[\n" +
                        "  {\n" +
                        "    \"Plan\": {\n" +
                        "        \"Node Type\": \"Count\",\n" +
                        "        \"Plans\": [\n" +
                        "        {\n" +
                        "            \"Node Type\": \"long_sequence\",\n" +
                        "            \"count\":  10\n" +
                        "        } ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "]\n", "explain (format json) select count (*) from long_sequence(10)",
                null,
                null,
                false,
                true
        );
    }

    @Test
    public void testAnalyzeExplainWithJsonFormat2() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);

                String expected = "QUERY PLAN\n" +
                        "[\n" +
                        "  {\n" +
                        "    \"Plan\": {\n" +
                        "        \"Node Type\": \"SelectedRecord\",\n" +
                        "        \"Plans\": [\n" +
                        "        {\n" +
                        "            \"Node Type\": \"Filter\",\n" +
                        "            \"filter\": \"0<a.l+b.l\",\n" +
                        "            \"Plans\": [\n" +
                        "            {\n" +
                        "                \"Node Type\": \"Hash Join\",\n" +
                        "                \"condition\": \"b.l=a.l\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"PageFrame\",\n" +
                        "                    \"Plans\": [\n" +
                        "                    {\n" +
                        "                        \"Node Type\": \"Row forward scan\"\n" +
                        "                    },\n" +
                        "                    {\n" +
                        "                        \"Node Type\": \"Frame forward scan\",\n" +
                        "                        \"on\": \"a\"\n" +
                        "                    } ]\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Hash\",\n" +
                        "                    \"Plans\": [\n" +
                        "                    {\n" +
                        "                        \"Node Type\": \"Async JIT Filter\",\n" +
                        "                        \"workers\":  1,\n" +
                        "                        \"limit\":  4,\n" +
                        "                        \"filter\": \"10<l\",\n" +
                        "                        \"Plans\": [\n" +
                        "                        {\n" +
                        "                            \"Node Type\": \"PageFrame\",\n" +
                        "                            \"Plans\": [\n" +
                        "                            {\n" +
                        "                                \"Node Type\": \"Row forward scan\"\n" +
                        "                            },\n" +
                        "                            {\n" +
                        "                                \"Node Type\": \"Frame forward scan\",\n" +
                        "                                \"on\": \"a\"\n" +
                        "                            } ]\n" +
                        "                        } ]\n" +
                        "                } ]\n" +
                        "            } ]\n" +
                        "        } ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "]\n";

                if (!JitUtil.isJitSupported()) {
                    expected = expected.replace("JIT ", "");
                }

                execute("create table a ( l long)");
                assertQueryNoLeakCheck(
                        compiler,
                        expected,
                        "explain (format json) select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                        null,
                        false,
                        sqlExecutionContext,
                        true
                );
            }
        });
    }

    @Test
    public void testAnalyzeExplainWithJsonFormat3() throws Exception {
        assertQuery(
                "QUERY PLAN\n" +
                        "[\n" +
                        "  {\n" +
                        "    \"Plan\": {\n" +
                        "        \"Node Type\": \"GroupBy\",\n" +
                        "        \"vectorized\":  false,\n" +
                        "        \"keys\": \"[d]\",\n" +
                        "        \"values\": \"[max(i)]\",\n" +
                        "        \"Plans\": [\n" +
                        "        {\n" +
                        "            \"Node Type\": \"Union\",\n" +
                        "            \"Plans\": [\n" +
                        "            {\n" +
                        "                \"Node Type\": \"PageFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"a\"\n" +
                        "                } ]\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"Node Type\": \"PageFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"a\"\n" +
                        "                } ]\n" +
                        "            } ]\n" +
                        "        } ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "]\n",
                "explain (format json) select d, max(i) from (select * from a union select * from a)",
                "create table a ( i int, d double)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAnalyzeExplainWithJsonFormat4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            assertQueryNoLeakCheck(
                    "QUERY PLAN\n" +
                            "[\n" +
                            "  {\n" +
                            "    \"Plan\": {\n" +
                            "        \"Node Type\": \"SelectedRecord\",\n" +
                            "        \"Plans\": [\n" +
                            "        {\n" +
                            "            \"Node Type\": \"Nested Loop Left Join\",\n" +
                            "            \"filter\": \"(taba.a1=tabb.b1 or taba.a2=tabb.b2)\",\n" +
                            "            \"Plans\": [\n" +
                            "            {\n" +
                            "                \"Node Type\": \"PageFrame\",\n" +
                            "                \"Plans\": [\n" +
                            "                {\n" +
                            "                    \"Node Type\": \"Row forward scan\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                    \"Node Type\": \"Frame forward scan\",\n" +
                            "                    \"on\": \"taba\"\n" +
                            "                } ]\n" +
                            "            },\n" +
                            "            {\n" +
                            "                \"Node Type\": \"PageFrame\",\n" +
                            "                \"Plans\": [\n" +
                            "                {\n" +
                            "                    \"Node Type\": \"Row forward scan\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                    \"Node Type\": \"Frame forward scan\",\n" +
                            "                    \"on\": \"tabb\"\n" +
                            "                } ]\n" +
                            "            } ]\n" +
                            "        } ]\n" +
                            "    }\n" +
                            "  }\n" +
                            "]\n",
                    " explain (format json) select * from taba left join tabb on a1=b1  or a2=b2",
                    null,
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testAnalyzeExplainWithQueryInParentheses1() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "(select 1)"
        ));
    }

    @Test
    public void testAnalyzeExplainWithQueryInParentheses2() throws Exception {
        selectLeakCheck(
                "create table x ( i int)",
                "(select * from x)"
        );
    }

    @Test
    public void testAnalyzeExplainWithQueryInParentheses3() throws Exception {
        selectLeakCheck(
                "create table x ( i int)",
                "((select * from x))"
        );
    }

    @Test
    public void testAnalyzeExplainWithQueryInParentheses4() throws Exception {
        selectLeakCheck(
                "create table x ( i int)",
                "((x))"
        );
    }

    @Test
    public void testAnalyzeExplainWithQueryInParentheses5() throws Exception {
        selectLeakCheck(
                "CREATE TABLE trades (\n" +
                        "  symbol SYMBOL,\n" +
                        "  side SYMBOL,\n" +
                        "  price DOUBLE,\n" +
                        "  amount DOUBLE,\n" +
                        "  timestamp TIMESTAMP\n" +
                        ") timestamp (timestamp) PARTITION BY DAY",
                "((select last(timestamp) as x, last(price) as btcusd " +
                        "from trades " +
                        "where symbol = 'BTC-USD' " +
                        "and timestamp > dateadd('m', -30, now())) " +
                        "timestamp(x))"
        );
    }

    @Test
    public void testAnalyzeFilterOnExcludedIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("drop table if exists trips");
            execute("CREATE TABLE trips (l long, s symbol index capacity 5, ts TIMESTAMP) " +
                    "timestamp(ts) partition by month");

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null order by count desc"
            );

            execute("insert into trips " +
                    "  select x, 'A' || ( x%3000 )," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(10000);");
            execute("insert into trips " +
                    "  select x, null," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(4000);"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and s != 'A1000'"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and s != :s1"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and l != 0"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null or l != 0"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l != 0 and s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l != 0 or s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l > 100 or l != 0 and s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', 'A2000')"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");

            selectNoLeakCheck(
                    "select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)"
            );
        });
    }

    @Test
    public void testAnalyzeFilterOnExcludedNonIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("drop table if exists trips");
            execute("CREATE TABLE trips(l long, s symbol capacity 5, ts TIMESTAMP) " +
                    "timestamp(ts) partition by month");

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null"
            );

            execute("insert into trips " +
                    "  select x, 'A' || ( x%3000 )," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(10000);");
            execute("insert into trips " +
                    "  select x, null," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(4000);"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and s != 'A1000'"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and s != :s1"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null and l != 0"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where s is not null or l != 0"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l != 0 and s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l != 0 or s is not null"
            );

            selectNoLeakCheck(
                    "select s, count() from trips where l > 100 or l != 0 and s is not null"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");

            selectNoLeakCheck(
                    "select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)"
            );
        });
    }

    @Test
    public void testAnalyzeFiltersOnIndexedSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE reference_prices (\n" +
                    "  venue SYMBOL index ,\n" +
                    "  symbol SYMBOL index,\n" +
                    "  instrumentType SYMBOL index,\n" +
                    "  referencePriceType SYMBOL index,\n" +
                    "  resolutionType SYMBOL ,\n" +
                    "  ts TIMESTAMP,\n" +
                    "  referencePrice DOUBLE\n" +
                    ") timestamp (ts)");

            execute("insert into reference_prices \n" +
                    "select rnd_symbol('VENUE1', 'VENUE2', 'VENUE3'), \n" +
                    "          'symbol', \n" +
                    "          'instrumentType', \n" +
                    "          rnd_symbol('TYPE1', 'TYPE2'), \n" +
                    "          'resolutionType', \n" +
                    "          cast(x as timestamp), \n" +
                    "          rnd_double()\n" +
                    "from long_sequence(10000)");

            String query1 = "select referencePriceType,count(*) " +
                    "from reference_prices " +
                    "where referencePriceType not in ('TYPE1') " +
                    "and venue in ('VENUE1', 'VENUE2')";
            String expectedResult = "referencePriceType\tcount\n" +
                    "TYPE2\t3344\n";

            selectNoLeakCheck(query1);
            assertSql(expectedResult, query1);

            String query2 = "select referencePriceType, count(*) \n" +
                    "from reference_prices \n" +
                    "where venue in ('VENUE1', 'VENUE2') \n" +
                    "and referencePriceType not in ('TYPE1')";

            selectNoLeakCheck(query2);
            assertSql(expectedResult, query2);
        });
    }

    @Test
    public void testAnalyzeFunctions() throws Exception {
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
            constFuncs.put(ColumnType.TIMESTAMP, list(new TimestampConstant(86400000000L)));
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
            constFuncs.put(ColumnType.INTERVAL, list(IntervalConstant.NULL));

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
            colFuncs.put(ColumnType.BOOLEAN, new BooleanColumn(1));
            colFuncs.put(ColumnType.BYTE, new ByteColumn(1));
            colFuncs.put(ColumnType.SHORT, new ShortColumn(2));
            colFuncs.put(ColumnType.CHAR, new CharColumn(1));
            colFuncs.put(ColumnType.INT, new IntColumn(1));
            colFuncs.put(ColumnType.IPv4, new IPv4Column(1));
            colFuncs.put(ColumnType.LONG, new LongColumn(1));
            colFuncs.put(ColumnType.DATE, new DateColumn(1));
            colFuncs.put(ColumnType.TIMESTAMP, new TimestampColumn(1));
            colFuncs.put(ColumnType.FLOAT, new FloatColumn(1));
            colFuncs.put(ColumnType.DOUBLE, new DoubleColumn(1));
            colFuncs.put(ColumnType.STRING, new StrColumn(1));
            colFuncs.put(ColumnType.VARCHAR, new VarcharColumn(1));
            colFuncs.put(ColumnType.SYMBOL, new SymbolColumn(1, true));
            colFuncs.put(ColumnType.LONG256, new Long256Column(1));
            colFuncs.put(ColumnType.GEOBYTE, new GeoByteColumn(1, ColumnType.getGeoHashTypeWithBits(5)));
            colFuncs.put(ColumnType.GEOSHORT, new GeoShortColumn(1, ColumnType.getGeoHashTypeWithBits(10)));
            colFuncs.put(ColumnType.GEOINT, new GeoIntColumn(1, ColumnType.getGeoHashTypeWithBits(20)));
            colFuncs.put(ColumnType.GEOLONG, new GeoLongColumn(1, ColumnType.getGeoHashTypeWithBits(35)));
            colFuncs.put(ColumnType.GEOHASH, new GeoShortColumn((short) 1, ColumnType.getGeoHashTypeWithBits(15)));
            colFuncs.put(ColumnType.BINARY, new BinColumn(1));
            colFuncs.put(ColumnType.LONG128, new Long128Column(1));
            colFuncs.put(ColumnType.UUID, new UuidColumn(1));

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
                        int sigArgTypeMask = descriptor.getArgTypeMask(p);
                        final short sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);
                        boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);

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
                        int argTypeMask = descriptor.getArgTypeMask(p);
                        boolean isConstant = FunctionFactoryDescriptor.isConstant(argTypeMask);
                        short sigArgType = FunctionFactoryDescriptor.toType(argTypeMask);
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
                                int sigArgTypeMask = descriptor.getArgTypeMask(p);
                                short sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);
                                boolean isConstant = FunctionFactoryDescriptor.isConstant(sigArgTypeMask);
                                boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);
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
                                    } else if (factory instanceof LagDoubleFunctionFactory || factory instanceof LeadDoubleFunctionFactory) {
                                        sigArgType = ColumnType.INT;
                                        useConst = true;
                                    } else {
                                        sigArgType = ColumnType.STRING;
                                    }
                                }

                                if (factory instanceof SwitchFunctionFactory) {
                                    args.add(new IntConstant(1));
                                    args.add(new IntConstant(2));
                                    args.add(new StrConstant("a"));
                                    args.add(new StrConstant("b"));
                                } else if (factory instanceof EqIntervalFunctionFactory) {
                                    args.add(IntervalConstant.NULL);
                                } else if (factory instanceof CoalesceFunctionFactory) {
                                    args.add(new FloatColumn(1));
                                    args.add(new FloatColumn(2));
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
                                } else if (factory instanceof TimestampFloorFunctionFactory && p == 0) {
                                    args.add(new StrConstant("d"));
                                } else if (factory instanceof TimestampFloorOffsetFunctionFactory && p == 0) {
                                    args.add(new StrConstant("d"));
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
                                    args.add(new TimestampConstant(123141));
                                    args.add(new IntervalConstant(1231, 123146));
                                } else if (Chars.equals(key, "approx_count_distinct") && sigArgCount == 2 && p == 1 && sigArgType == ColumnType.INT) {
                                    args.add(new IntConstant(4)); // precision has to be in the range of 4 to 18
                                } else if (!useConst) {
                                    args.add(colFuncs.get(sigArgType));
                                } else if (factory instanceof WalTransactionsFunctionFactory && sigArgType == ColumnType.STRING) {
                                    // Skip it, it requires a WAL table to exist
                                    break FUNCTIONS;
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
                                sqlExecutionContext.configureWindowContext(null, null, null, false, PageFrameRecordCursorFactory.SCAN_DIRECTION_FORWARD, -1, true, WindowColumn.FRAMING_RANGE, Long.MIN_VALUE, 10, 0, 20, WindowColumn.EXCLUDE_NO_OTHERS, 0, -1, false, 0);
                            }
                            Function function = null;
                            try {
                                try {
                                    function = factory.newInstance(0, args, argPositions, engine.getConfiguration(), sqlExecutionContext);
                                    function.toPlan(planSink);
                                } finally {
                                    sqlExecutionContext.clearWindowContext();
                                }

                                goodArgsFound = true;

                                Assert.assertFalse("function " + factory.getSignature() + " should serialize to text properly. current text: " + planSink.getSink(), Chars.contains(planSink.getSink(), "io.questdb"));
                                LOG.info().$(sink).$(planSink.getSink()).$();

                                if (function instanceof NegatableBooleanFunction && !((NegatableBooleanFunction) function).isNegated()) {
                                    ((NegatableBooleanFunction) function).setNegated();
                                    tmpPlanSink.clear();
                                    function.toPlan(tmpPlanSink);

                                    if (Chars.equals(planSink.getSink(), tmpPlanSink.getSink())) {
                                        throw new AssertionError("Same output generated regardless of negatable flag! Factory: " + factory.getSignature() + " " + function);
                                    }

                                    Assert.assertFalse(
                                            "function " + factory.getSignature() + " should serialize to text properly",
                                            Chars.contains(tmpPlanSink.getSink(), "io.questdb")
                                    );
                                }
                            } finally {
                                Misc.free(function);
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
    public void testAnalyzeGroupByBoolean() throws Exception {
        selectLeakCheck(
                "create table a (l long, b boolean)",
                "select b, min(l)  from a group by b"
        );
    }

    @Test
    public void testAnalyzeGroupByBooleanFunction() throws Exception {
        selectLeakCheck(
                "create table a (l long, b1 boolean, b2 boolean)",
                "select b1||b2, min(l) from a group by b1||b2"
        );
    }

    @Test
    public void testAnalyzeGroupByBooleanWithFilter() throws Exception {
        selectLeakCheck(
                "create table a (l long, b boolean)",
                "select b, min(l)  from a where b = true group by b"
        );
    }

    @Test
    public void testAnalyzeGroupByDouble() throws Exception {
        selectLeakCheck(
                "create table a (l long, d double)",
                "select d, min(l) from a group by d"
        );
    }

    @Test
    public void testAnalyzeGroupByFloat() throws Exception {
        selectLeakCheck(
                "create table a (l long, f float)",
                "select f, min(l) from a group by f"
        );
    }

    @Test // special case
    public void testAnalyzeGroupByHour() throws Exception {
        selectLeakCheck(
                "create table a (ts timestamp, d double)",
                "select hour(ts), min(d) from a group by hour(ts)"
        );
    }

    @Test
    public void testAnalyzeGroupByHourAndFilterIsParallel() throws Exception {
        selectLeakCheck(
                "create table a (ts timestamp, d double)",
                "select hour(ts), min(d) from a where d > 0 group by hour(ts)"
        );
    }

    @Test
    public void testAnalyzeGroupByHourUnorderedColumns() throws Exception {
        selectLeakCheck(
                "create table a (ts timestamp, d double)",
                "select min(d), hour(ts) from a group by hour(ts)"
        );
    }

    @Test
    public void testAnalyzeGroupByInt1() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select min(d), i from a group by i"
        );
    }

    @Test // repeated group by keys get merged at group by level
    public void testAnalyzeGroupByInt2() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select i, i, min(d) from a group by i, i"
        );
    }

    @Test
    public void testAnalyzeGroupByInt3() throws Exception {
        selectLeakCheck(
                "create table a (i int, l long)",
                "select i, max(l) - min(l) delta from a group by i"
        );
    }

    @Test
    public void testAnalyzeGroupByIntOperation() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select min(d), i * 42 from a group by i"
        );
    }

    @Test
    public void testAnalyzeGroupByKeyedAliased() throws Exception {
        selectLeakCheck(
                "create table a (s symbol, ts timestamp) timestamp(ts) partition by year;"
        );
    }

    @Test
    public void testAnalyzeGroupByKeyedNoAlias() throws Exception {
        selectLeakCheck(
                "create table a (s symbol, ts timestamp) timestamp(ts) partition by year;"
        );
    }

    @Test
    public void testAnalyzeGroupByKeyedOnExcept() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, d double)");

            selectNoLeakCheck(
                    "create table b ( j int, e double)",
                    "select d, max(i) from (select * from a except select * from b)"
            );
        });
    }

    @Test
    public void testAnalyzeGroupByKeyedOnIntersect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, d double)");

            selectNoLeakCheck(
                    "create table b ( j int, e double)",
                    "select d, max(i) from (select * from a intersect select * from b)"
            );
        });
    }

    @Test
    public void testAnalyzeGroupByKeyedOnUnion() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double)",
                "select d, max(i) from (select * from a union select * from a)"
        );
    }

    @Test
    public void testAnalyzeGroupByKeyedOnUnionAll() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double)",
                "select d, max(i) from (select * from a union all select * from a)"
        );
    }

    @Test
    public void testAnalyzeGroupByLong() throws Exception {
        selectLeakCheck(
                "create table a ( l long, d double)",
                "select l, min(d) from a group by l"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions1() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select i from a group by i"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions2() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select i from a where d < 42 group by i"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions3() throws Exception {
        selectLeakCheck(
                "create table a (i short, d double)",
                "select i from a group by i"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions4() throws Exception {
        selectLeakCheck(
                "create table a (i long, j long)",
                "select i, j from a group by i, j"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions5() throws Exception {
        selectLeakCheck(
                "create table a (i long, j long, d double)",
                "select i, j from a where d > 42 group by i, j"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions6() throws Exception {
        selectLeakCheck(
                "create table a (s symbol)",
                "select s from a group by s"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions7() throws Exception {
        selectLeakCheck(
                "create table a (s symbol, d double)",
                "select s from a where d = 42 group by s"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions8() throws Exception {
        selectLeakCheck(
                "create table a (s string)",
                "select s from a group by s"
        );
    }

    @Test
    public void testAnalyzeGroupByNoFunctions9() throws Exception {
        selectLeakCheck(
                "create table a (s string)",
                "select s from a where s like '%foobar%' group by s"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed1() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select min(d) from a"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed10() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(i) from (select * from a join a b on i )"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed11() throws Exception {
        selectLeakCheck(
                "create table a (gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b))",
                "select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed12() throws Exception {
        selectLeakCheck(
                "create table a (gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b), i int)",
                "select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a where i > 42"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed13() throws Exception {
        selectLeakCheck(
                "create table a (i int)",
                "select max(i) - min(i) from a"
        );
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testAnalyzeGroupByNotKeyed2() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select min(d), max(d*d) from a"
        );
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testAnalyzeGroupByNotKeyed3() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(d+1) from a"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed4() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select count(*), max(i), min(d) from a"
        );
    }

    @Test
    public void testAnalyzeGroupByNotKeyed5() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select first(10), last(d), avg(10), min(10), max(10) from a"
        );
    }

    @Test // group by on filtered data is not vectorized
    public void testAnalyzeGroupByNotKeyed6() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(i) from a where i < 10"
        );
    }

    @Test // order by is ignored and grouped by - vectorized
    public void testAnalyzeGroupByNotKeyed7() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(i) from (select * from a order by d)"
        );
    }

    @Test // order by can't be ignored; group by is not vectorized
    public void testAnalyzeGroupByNotKeyed8() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(i) from (select * from a order by d limit 10)"
        );
    }

    @Test // TODO: group by could be vectorized for union tables and result merged
    public void testAnalyzeGroupByNotKeyed9() throws Exception {
        selectLeakCheck(
                "create table a (i int, d double)",
                "select max(i) from (select * from a union all select * from a)"
        );
    }

    @Test
    public void testAnalyzeGroupByStringFunction() throws Exception {
        selectLeakCheck(
                "create table a (l long, s1 string, s2 string)"
        );
    }

    @Test
    public void testAnalyzeGroupByStringFunctionWithFilter() throws Exception {
        selectLeakCheck(
                "create table a (l long, s1 string, s2 string)",
                "select s1||s2 s, avg(l) a from a where l > 42"
        );
    }

    @Test
    public void testAnalyzeGroupBySymbol() throws Exception {
        selectLeakCheck(
                "create table a (l long, s symbol)",
                "select s, avg(l) a from a"
        );
    }

    @Test
    public void testAnalyzeGroupBySymbol2() throws Exception {
        selectLeakCheck(
                "create table a (l long, s symbol)",
                "select s, max(l) - min(l) a from a"
        );
    }

    @Test
    public void testAnalyzeGroupBySymbolFunction() throws Exception {
        selectLeakCheck(
                "create table a (l long, s string)",
                "select s::symbol, avg(l) a from a"
        );
    }

    @Test
    public void testAnalyzeGroupBySymbolWithSubQueryFilter() throws Exception {
        selectLeakCheck(
                "create table a (l long, s symbol)",
                "select s, avg(l) a from a where s in (select s from a where s = 'key')"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit1() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 limit 10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit10() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select y, count(*) from di order by y desc limit 1"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit11() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select y, count(*) c from di order by c limit 42"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit12() throws Exception {
        sqlExecutionContext.setParallelGroupByEnabled(false);
        try {
            selectLeakCheck(
                    "create table di (x int, y long)",
                    "select y, count(*) c from di order by c limit 42"
            );
        } finally {
            sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelGroupByEnabled());
        }
    }

    @Test
    public void testAnalyzeGroupByWithLimit2() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 desc limit 10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit3() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x limit 10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit4() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x limit -10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit5a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit 10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit5b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit -10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit6a() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit6b() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit -10"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit7() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10, 20"
        );
    }

    @Test
    public void testAnalyzeGroupByWithLimit8() throws Exception {
        selectLeakCheck(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select ts, count(*) from di where y = 5 group by ts order by ts desc limit 10"
        );
    }

    @Test
    public void testAnalyzeHashInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s1 string)");
            execute("create table b ( i int, s2 string)");

            selectNoLeakCheck(
                    "select s1, s2 from (select a.s1, b.s2, b.i, a.i  from a join b on i) where i < i1 and s1 = s2"
            );
        });
    }

    @Test
    public void testAnalyzeHashLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int)");
            execute("create table b ( i int)");

            selectNoLeakCheck(
                    "select * from a left join b on i"
            );
        });
    }

    @Test
    public void testAnalyzeHashLeftJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int)");
            execute("create table b ( i int)");

            selectNoLeakCheck(
                    "select * from a left join b on i where b.i is not null"
            );
        });
    }

    @Ignore
    //FIXME
    //@Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testAnalyzeImplicitJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i1 int)");
            execute("create table b ( i2 int)");

            assertSql("", "select * from a, b where a.i1 = b.i2");

            selectNoLeakCheck(
                    "select * from a , b where a.i1 = b.i2"
            );
        });
    }

    @Test
    public void testAnalyzeInUuid() throws Exception {
        selectLeakCheck(
                "create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u in ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')"
        );
    }

    @Test
    public void testAnalyzeIntersect1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a intersect select * from a"
        );
    }

    @Test
    public void testAnalyzeIntersect2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a intersect select * from a where i > 0"
        );
    }

    @Test
    public void testAnalyzeIntersectAll() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a intersect all select * from a"
        );
    }

    @Test
    public void testAnalyzeIntersectAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeIntersectAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeIntersectAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeIntersectAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeKSumNSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( k long, x double );");

            selectNoLeakCheck(
                    "SELECT k, ksum(x), nsum(x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeLatestByAllSymbolsFilteredFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table maps\n" +
                    "(\n" +
                    "  timestamp timestamp, \n" +
                    "  cluster symbol, \n" +
                    "  alias symbol, \n" +
                    "  octets int, \n" +
                    "  packets int\n" +
                    ") timestamp(timestamp);");

            execute("insert into maps values ('2023-09-01T09:41:00.000Z', 'cluster10', 'a', 1, 1), " +
                    "('2023-09-01T09:42:00.000Z', 'cluster10', 'a', 2, 2)");

            String sql = "select timestamp, cluster, alias, timestamp - timestamp1 interval, (octets-octets1)*8 bits, packets-packets1 packets from (\n" +
                    "  (select timestamp, cluster, alias, octets, packets\n" +
                    "  from maps\n" +
                    "  where cluster in ('cluster10') and timestamp BETWEEN '2023-09-01T09:40:27.286Z' AND '2023-09-01T10:40:27.286Z'\n" +
                    "  latest on timestamp partition by cluster,alias)\n" +
                    "  lt join maps on (cluster,alias)\n" +
                    "  ) order by bits desc\n" +
                    "limit 10";
            selectNoLeakCheck(
                    sql
            );

            assertSql("timestamp\tcluster\talias\tinterval\tbits\tpackets\n" +
                    "2023-09-01T09:42:00.000000Z\tcluster10\ta\t60000000\t8\t1\n", sql);
        });
    }

    @Test
    public void testAnalyzeLatestByRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as ( " +
                    "  select " +
                    "    rnd_str('a','b','c') s, " +
                    "    timestamp_sequence(0, 100000000) ts " +
                    "   from long_sequence(100)" +
                    ") timestamp(ts) partition by hour");

            String sql = "with yy as (select ts, max(s) s from tab sample by 1h ALIGN TO FIRST OBSERVATION) " +
                    "select * from yy latest on ts partition by s limit 10";
            selectNoLeakCheck(
                    sql
            );

            assertSql("ts\ts\n" +
                    "1970-01-01T02:00:00.000000Z\tc\n", sql);
        });
    }

    @Test
    public void testAnalyzeLatestOn0() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from a latest on ts partition by i"
        );
    }

    @Test
    public void testAnalyzeLatestOn0a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from (select * from a where i = 10 union select * from a where i =20) latest on ts partition by i"
        );
    }

    @Test
    public void testAnalyzeLatestOn0b() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select ts,i from a where s in ('ABC') and i > 0 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn0c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select ts,i from a where s in ('a1') and i > 0 latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn0d() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select ts,i from a where s in ('a1') latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn0e() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select ts,i, s from a where s in ('a1') and i > 0 latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by i"
        );
    }

    @Test // TODO: should use index
    public void testAnalyzeLatestOn10() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn11() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn12() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn12a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn13() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn13a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) latest on ts partition by s"
        );
    }

    @Test // TODO: should use one or two indexes
    public void testAnalyzeLatestOn14() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2"
        );
    }

    @Test // TODO: should use one or two indexes
    public void testAnalyzeLatestOn15() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' latest on ts partition by s1,s2"
        );
    }

    @Test
    public void testAnalyzeLatestOn16() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 = 'S1' and ts > 0::timestamp latest on ts partition by s1,s2"
        );
    }

    @Test
    public void testAnalyzeLatestOn1a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from (select ts, i as i1, i as i2 from a ) where 0 < i1 and i2 < 10 latest on ts partition by i1"
        );
    }

    @Test
    public void testAnalyzeLatestOn1b() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select ts, i as i1, i as i2 from a where 0 < i and i < 10 latest on ts partition by i"
        );
    }

    @Test
    public void testAnalyzeLatestOn2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double, ts timestamp) timestamp(ts);",
                "select ts, d from a latest on ts partition by i"
        );
    }

    @Test
    public void testAnalyzeLatestOn3() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn4() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  = 'S1' latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn5a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            selectNoLeakCheck(
                    "select s, i, ts from a where s  in ('def1', 'def2') latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn5b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            selectNoLeakCheck(
                    "select s, i, ts from a where s  in ('1', 'deferred') latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn5c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            selectNoLeakCheck(
                    "select s, i, ts from a where s  in ('1', '2') latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLatestOn6() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn7() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn8() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            selectNoLeakCheck(
                    "select s, i, ts from a where s  in ('s1') latest on ts partition by s"
            );
        });
    }

    @Test // key outside list of symbols
    public void testAnalyzeLatestOn8a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            selectNoLeakCheck(
                    "select s, i, ts from a where s in ('bogus_key') latest on ts partition by s"
            );
        });
    }

    @Test // columns in order different to table's
    public void testAnalyzeLatestOn9() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s"
        );
    }

    @Test // columns in table's order
    public void testAnalyzeLatestOn9a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, s, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s"
        );
    }

    @Test
    public void testAnalyzeLatestOn9b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            execute("insert into a select x::int, 'S' || x, x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEquality1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a int)");
            execute("create table tabb (b int)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a=b"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEquality2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=b2"
            );
        });
    }

    @Test // FIXME: there should be no separate filter
    public void testAnalyzeLeftJoinWithEquality3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  or a2=b2"
            );
        });
    }

    @Test // FIXME: join and where clause filters should be separated
    public void testAnalyzeLeftJoinWithEquality4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  or a2=b2 where a1 > b2"
            );
        });
    }

    @Test // FIXME: ORed predicates should be applied as filter in hash join
    public void testAnalyzeLeftJoinWithEquality5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba " +
                            "left join tabb on a1=b1 and (a2=b2+10 or a2=2*b2)"
            );
        });
    }

    // left join conditions aren't transitive because left record + null right is produced if they fail
    // that means select * from a left join b on a.i = b.i and a.i=10 doesn't mean resulting records will have a.i = 10 !
    @Test
    public void testAnalyzeLeftJoinWithEquality6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");
            execute("create table tabc (c1 int, c2 long)");

            selectNoLeakCheck(
                    "select * from taba " +
                            "left join tabb on a1=b1 and a1=5 " +
                            "join tabc on a1=c1"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEquality7() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                testHashAndAsOfJoin(compiler, true, true);
            }
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEquality8() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                testHashAndAsOfJoin(compiler, false, false);
            }
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2)"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressions3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10 and 1=0"
            );
        });
    }

    // FIXME provably false predicate like x!=x in left join means we can skip join and return left + nulls or join with empty right table
    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressions4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2!=a2"
            );
        });
    }

    @Test // FIXME: a2=a2 run as past of left join or be optimized away !
    public void testAnalyzeLeftJoinWithEqualityAndExpressions5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=a2"
            );
        });
    }

    @Test // left join filter must remain intact !
    public void testAnalyzeLeftJoinWithEqualityAndExpressions6() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 string)");
            execute("create table tabb (b1 int, b2 string)");

            selectNoLeakCheck(
                    "select * from taba " +
                            "left join tabb on a1=b1  and a2 ~ 'a.*' and b2 ~ '.*z'"
            );
        });
    }

    @Test // FIXME:  abs(a2+1) = abs(b2) should be applied as left join filter  !
    public void testAnalyzeLeftJoinWithEqualityAndExpressionsAhdWhere1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba " +
                            "left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) "
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressionsAhdWhere2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) where a1=b1"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithEqualityAndExpressionsAhdWhere3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on a1=b1 and abs(a2+1) = abs(b2) where a2=b2"
            );
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testAnalyzeLeftJoinWithExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on abs(a2+1) = abs(b2)"
            );
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testAnalyzeLeftJoinWithExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table taba (a1 int, a2 long)");
            execute("create table tabb (b1 int, b2 long)");

            selectNoLeakCheck(
                    "select * from taba left join tabb on abs(a2+1) = abs(b2) or a2/2 = b2+1"
            );
        });
    }

    @Test
    public void testAnalyzeLeftJoinWithPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( created timestamp, value int ) timestamp(created)");

            String[] joinTypes = {"LEFT", "LT", "ASOF"};

            for (int i = 0; i < joinTypes.length; i++) {
                // do not push down predicate to the 'right' table of left join but apply it after join
                String joinType = joinTypes[i];

                selectNoLeakCheck(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T2.value<>T2.value"
                );

                selectNoLeakCheck(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T2.value=1"
                );

                // push down predicate to the 'left' table of left join
                selectNoLeakCheck(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T1.value=1"
                );
            }


            // two joins
            selectNoLeakCheck(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T1.value=1"
            );

            selectNoLeakCheck(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T2.created=1"
            );

            selectNoLeakCheck(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T3.value=1"
            );

            // where clause in parent model
            selectNoLeakCheck(
                    "SELECT count(1) " +
                            "FROM ( " +
                            "SELECT * " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created ) e " +
                            "WHERE not value1<>value1"
            );

            selectNoLeakCheck(
                    "SELECT count(1) " +
                            "FROM ( " +
                            "SELECT * " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created ) e " +
                            "WHERE not value<>value"
            );
        });
    }

    @Test
    public void testAnalyzeLikeFilters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (s1 string, s2 string, s3 string, s4 string, s5 string, s6 string);");

            selectNoLeakCheck(
                    "select * from tab " +
                            "where s1 like '%a'  and s2 ilike '%a' " +
                            "  and s3 like 'a%'  and s4 ilike 'a%' " +
                            "  and s5 like '%a%' and s6 ilike '%a%';"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select ts1, ts2, i1, i2 from (select a.i as i1, a.ts as ts1, b.i as i2, b.ts as ts2 from a lt join b on ts) where ts1::long*i1<ts2::long*i2"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b on ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin1a() throws Exception {
        // lt join guarantees that a.ts > b.ts [join cond is not an equality predicate]
        // CONCLUSION: a join b on X can't always be translated to a join b on a.X = b.X
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b on ts where a.i = b.ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin1b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b on ts where a.i = b.ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin1c() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b where a.i = b.ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join (select * from b limit 10) on ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoinNoKey1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b where a.i > 0"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoinNoKey2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoinNoKey3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (i int, ts timestamp) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b on(ts)"
            );
        });
    }

    @Test
    public void testAnalyzeLtJoinNoKey4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, i int) timestamp(ts)");
            execute("create table b (i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join b on(ts)"
            );
        });
    }

    @Test
    public void testAnalyzeLtOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts"
            );
        });
    }

    @Test
    public void testAnalyzeLtOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * " +
                            "from a " +
                            "lt join b on ts " +
                            "lt join a c on ts"
            );
        });
    }

    @Test
    public void testAnalyzeMultiExcept() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a except select * from a except select * from a"
        );
    }

    @Test
    public void testAnalyzeMultiIntersect() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a intersect select * from a intersect select * from a"
        );
    }

    @Test
    public void testAnalyzeMultiUnion() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a union select * from a union select * from a"
        );
    }

    @Test
    public void testAnalyzeMultiUnionAll() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a union all select * from a union all select * from a"
        );
    }

    @Test
    public void testAnalyzeNestedLoopLeftJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 left join t t2 on t1.x*t2.x>0 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 left join t t2 on t1.x*t2.x>0 order by t1.ts"};
            for (String query : queries) {
                selectNoLeakCheck(
                        query
                );
            }
        });
    }

    @Test
    public void testAnalyzeNestedLoopLeftJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (x int, ts timestamp) timestamp(ts)");
            execute("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "left join t t2 on t1.x*t2.x > 0 " +
                    "order by t1.ts desc";

            selectNoLeakCheck(
                    query
            );
        });
    }

    @Test
    public void testAnalyzeNoArgFalseConstantExpressionUsedInJoinIsOptimizedAway() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (b boolean, ts timestamp)");
            // true
            selectNoLeakCheck(
                    "update tab t1 set b=true from tab t2 where 1>2 and t1.b = t2.b"
            );
            // false
            selectNoLeakCheck(
                    "update tab t1 set b=true from tab t2 where 1<2 and t1.b = t2.b"
            );
        });
    }

    @Test
    public void testAnalyzeNoArgNonConstantExpressionUsedInJoinClauseIsUsedAsPostJoinFilter() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);

        selectLeakCheck(
                "create table tab (b boolean, ts timestamp)",
                "update tab t1 set b=true from tab t2 where not sleep(60000) and t1.b = t2.b"
        );
    }

    @Test
    public void testAnalyzeNoArgRuntimeConstantExpressionUsedInJoinClauseIsUsedAsPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (b boolean, ts timestamp)");

            // true
            selectNoLeakCheck(
                    "update tab t1 set b=true from tab t2 where now()::long > -1 and t1.b = t2.b"
            );

            // false
            selectNoLeakCheck(
                    "update tab t1 set b=true from tab t2 where now()::long < 0 and t1.b = t2.b"
            );
        });
    }

    @Test
    public void testAnalyzeOrderByAdvicePushdown() throws Exception {
        // TODO: improve :
        // - limit propagation to async filter factory
        // - negative limit rewrite
        // if order by is via alias of designated timestamp

        assertMemoryLeak(() -> {
            execute(
                    "create table device_data " +
                            "( " +
                            "  timestamp timestamp, " +
                            "  val double, " +
                            "  id symbol " +
                            ") timestamp(timestamp)"
            );

            execute("insert into device_data select x::timestamp, x, '12345678' from long_sequence(10)");

            // use column name in order by
            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp DESC " +
                            "LIMIT 1"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp  " +
                            "LIMIT -1"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp DESC " +
                            "LIMIT -2"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp DESC " +
                            "LIMIT 1,3"
            );

            // with a virtual column
            selectNoLeakCheck(
                    "SELECT timestamp, val, now() " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp DESC " +
                            "LIMIT 1"
            );

            selectNoLeakCheck(
                    "SELECT timestamp, val, now() " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY timestamp ASC " +
                            "LIMIT -3"
            );

            // use alias in order by
            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY date DESC " +
                            "LIMIT 1"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY date  " +
                            "LIMIT -1"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY date DESC " +
                            "LIMIT -2"
            );

            selectNoLeakCheck(
                    "SELECT timestamp AS date, val, val + 1 " +
                            "FROM device_data " +
                            "WHERE device_data.id = '12345678' " +
                            "ORDER BY date DESC " +
                            "LIMIT 1,3"
            );
        });
    }

    @Test
    public void testAnalyzeOrderByIsMaintainedInLtAndAsofSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            for (String joinType : Arrays.asList("AsOf", "Lt")) {
                String query = "with gp as \n" +
                        "(\n" +
                        "selecT * from (\n" +
                        "selecT * from gas_prices order by timestamp asc, galon_price desc\n" +
                        ") timestamp(timestamp))\n" +
                        "selecT * from gp gp1 \n" +
                        joinType + " join gp gp2 \n" +
                        "order by gp1.timestamp; ";

                selectNoLeakCheck(query);
            }
        });
    }

    @Test
    public void testAnalyzeOrderByIsMaintainedInSpliceSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            String query = "with gp as (\n" +
                    "selecT * from (\n" +
                    "selecT * from gas_prices order by timestamp asc, galon_price desc\n" +
                    ") timestamp(timestamp))\n" +
                    "selecT * from gp gp1 \n" +
                    "splice join gp gp2 \n" +
                    "order by gp1.timestamp; ";

            selectNoLeakCheck(query);
        });
    }

    @Test
    public void testAnalyzeOrderByIsMaintainedInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gas_prices " +
                    "(timestamp TIMESTAMP, " +
                    "galon_price DOUBLE) " +
                    "timestamp (timestamp);");

            String query = "WITH full_range AS (  \n" +
                    "  SELECT timestamp, galon_price FROM (\n" +
                    "    SELECT * FROM (\n" +
                    "      SELECT * FROM gas_prices\n" +
                    "      UNION\n" +
                    "      SELECT to_timestamp('1999-01-01', 'yyyy-MM-dd'), NULL as galon_price -- First Date\n" +
                    "      UNION \n" +
                    "      SELECT to_timestamp('2023-02-20', 'yyyy-MM-dd'), NULL  as galon_price -- Last Date\n" +
                    "    ) AS unordered_data \n" +
                    "    order by timestamp\n" +
                    "  ) TIMESTAMP(timestamp)\n" +
                    ") \n" +
                    "select * " +
                    "from full_range";

            selectNoLeakCheck(query);
            selectNoLeakCheck(query + " order by timestamp");
        });
    }

    @Test
    public void testAnalyzeOrderByTimestampAndOtherColumns1() throws Exception {
        selectLeakCheck(
                "create table tab (i int, ts timestamp) timestamp(ts)",
                "select * from (select * from tab order by ts, i desc limit 10) order by ts"
        );
    }

    @Test
    public void testAnalyzeOrderByTimestampAndOtherColumns2() throws Exception {
        selectLeakCheck(
                "create table tab (i int, ts timestamp) timestamp(ts)",
                "select * from (select * from tab order by ts desc, i asc limit 10) order by ts desc"
        );
    }

    @Test
    public void testAnalyzePostJoinConditionColumnsAreResolved() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT count(*)\n" +
                    "FROM test as T1\n" +
                    "JOIN ( SELECT * FROM test ) as T2 ON T1.event < T2.event\n" +
                    "JOIN test as T3 ON T2.created = T3.created";

            execute("create table test (event int, created timestamp)");
            execute("insert into test values (1, 1), (2, 2)");

            selectNoLeakCheck(
                    query
            );

            assertSql("count\n1\n", query);
        });
    }

    @Test
    public void testAnalyzePredicatesArentPushedIntoWindowModel() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT * " +
                    "FROM ( " +
                    "  SELECT *, ROW_NUMBER() OVER ( PARTITION BY a ORDER BY b ) rownum " +
                    "  FROM (" +
                    "    SELECT 1 a, 2 b, 4 c " +
                    "    UNION " +
                    "    SELECT 1, 3, 5 " +
                    "  ) o " +
                    ") ra " +
                    "WHERE ra.rownum = 1 " +
                    "AND   c = 5";

            selectNoLeakCheck(
                    query
            );
            assertSql("a\tb\tc\trownum\n", query);

            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");

            selectNoLeakCheck(
                    "SELECT *, ROW_NUMBER() OVER () FROM tab WHERE x = 10"
            );

            selectNoLeakCheck(
                    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) WHERE x = 10"
            );

            selectNoLeakCheck(
                    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab UNION ALL select 11, 11  ) WHERE x = 10"
            );

            selectNoLeakCheck(
                    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab cross join (select 11, 11)  ) WHERE x = 10"
            );

            selectNoLeakCheck(
                    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) join (select 11L y, 11) on x=y WHERE x = 10"
            );
        });
    }

    @Test
    public void testAnalyzeReadParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " x as a_long," +
                            " rnd_str(4,4,4,2) as a_str," +
                            " rnd_timestamp('2015','2016',2) as a_ts" +
                            " from long_sequence(3))"
            );

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                selectNoLeakCheck(
                        "select * from read_parquet('x.parquet') where a_long = 42;"
                );

                selectNoLeakCheck(
                        "select avg(a_long) from read_parquet('x.parquet');"
                );

                selectNoLeakCheck(
                        "select a_str, max(a_long) from read_parquet('x.parquet');"
                );
            }
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithAdditionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x+10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10+x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithAdditionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            selectNoLeakCheck(
                    "SELECT sum(taba.x),sum(tabb.x), sum(taba.x+10), sum(tabb.x+10) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );

            selectNoLeakCheck(
                    "SELECT sum(tabb.x),sum(taba.x),sum(10+taba.x), sum(10+tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithIntAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x+10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10+x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithIntMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x*10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10*x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithIntSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x int );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x-10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10-x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithLongAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x+2) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(2+x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithLongMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x*10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10*x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithLongSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x long );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x-10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10-x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithMultiplicationIsDisabledForNonIntegerColumnType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x*10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10*x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithMultiplicationIsDisabledForNonIntegerConstantType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x*10.0) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10.0*x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithMultiplicationOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            selectNoLeakCheck(
                    "SELECT sum(taba.x),sum(tabb.x),sum(taba.x*10), sum(tabb.x*10) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );

            selectNoLeakCheck(
                    "SELECT sum(taba.x),sum(tabb.x),sum(10*taba.x), sum(10*tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithShortAddition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x+42) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(42+x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithShortMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x*10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10*x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithShortSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x short );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x-10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10-x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithSubtractionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab ( x double );");

            selectNoLeakCheck(
                    "SELECT sum(x), sum(x-10) FROM tab"
            );

            selectNoLeakCheck(
                    "SELECT sum(x), sum(10-x) FROM tab"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregateWithSubtractionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE taba ( x int, id int );");
            execute("CREATE TABLE tabb ( x int, id int );");

            selectNoLeakCheck(
                    "SELECT sum(taba.x),sum(tabb.x),sum(taba.x-10), sum(tabb.x-10) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );

            selectNoLeakCheck(
                    "SELECT sum(taba.x),sum(tabb.x),sum(10-taba.x), sum(10-tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hits\n" +
                    "(\n" +
                    "    EventTime timestamp,\n" +
                    "    ResolutionWidth int,\n" +
                    "    ResolutionHeight int\n" +
                    ") TIMESTAMP(EventTime) PARTITION BY DAY;");

            selectNoLeakCheck(
                    "SELECT sum(resolutIONWidth), count(resolutionwIDTH), SUM(ResolutionWidth), sum(ResolutionWidth) + count(), " +
                            "SUM(ResolutionWidth+1),SUM(ResolutionWidth*2),sUM(ResolutionWidth), count()\n" +
                            "FROM hits"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteAggregatesOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hits1" +
                    "(" +
                    "    EventTime timestamp, " +
                    "    ResolutionWidth int, " +
                    "    ResolutionHeight int, " +
                    "    id int" +
                    ")");
            execute("create table hits2 as (select * from hits1)");

            selectNoLeakCheck(
                    "SELECT sum(h1.resolutIONWidth), count(h1.resolutionwIDTH), SUM(h2.ResolutionWidth), sum(h2.ResolutionWidth) + count(), " +
                            "SUM(h1.ResolutionWidth+1),SUM(h2.ResolutionWidth*2),sUM(h1.ResolutionWidth), count()\n" +
                            "FROM hits1 h1 " +
                            "join hits2 h2 on (id)"
            );
        });
    }

    @Test
    public void testAnalyzeRewriteSelectCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test(s string, x long, ts timestamp, substring string) timestamp(ts) partition by day");
            execute("insert into test " +
                    "select 's' || (x%10), " +
                    " x, " +
                    " (x*86400000000)::timestamp, " +
                    " 'substring' " +
                    "from long_sequence(10)");

            // multiple count_distinct, no re-write
            selectNoLeakCheck("SELECT count_distinct(s), count_distinct(x) FROM test");
            selectNoLeakCheck("SELECT count(distinct s), count(distinct x) FROM test");


            // no where clause, distinct constant
            selectNoLeakCheck("SELECT count_distinct(10) FROM test");
            selectNoLeakCheck("SELECT count(distinct 10) FROM test");

            // no where clause, distinct column
            selectNoLeakCheck("SELECT count_distinct(s) FROM test");
            selectNoLeakCheck("SELECT count(distinct s) FROM test");

            // with where clause, distinct column

            selectNoLeakCheck("SELECT count_distinct(s) FROM test where s like '%abc%'");
            selectNoLeakCheck("SELECT count(distinct s) FROM test where s like '%abc%'");

            // no where clause, distinct expression 1
            selectNoLeakCheck("SELECT count_distinct(substring(s,1,1)) FROM test;");
            selectNoLeakCheck("SELECT count(distinct substring(s,1,1)) FROM test;");

            // where clause, distinct expression 2
            selectNoLeakCheck("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%'");
            selectNoLeakCheck("SELECT count(distinct substring(s,1,1)) FROM test where s like '%abc%'");

            // where clause, distinct expression 3, function name clash with column name
            selectNoLeakCheck("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%' and substring != null");
            selectNoLeakCheck("SELECT count(distinct substring(s,1,1)) FROM test where s like '%abc%' and substring != null");

            // where clause, distinct expression 3
            selectNoLeakCheck("SELECT count_distinct(x+1) FROM test where x > 5");
            selectNoLeakCheck("SELECT count(distinct x+1) FROM test where x > 5");

            // where clause, distinct expression, col alias
            selectNoLeakCheck("SELECT count_distinct(x+1) cnt_dst FROM test where x > 5");
            selectNoLeakCheck("SELECT count(distinct x+1) cnt_dst FROM test where x > 5");

            selectNoLeakCheck("SELECT count_distinct(x+1) FROM test tab where x > 5");
            selectNoLeakCheck("SELECT count(distinct x+1) FROM test tab where x > 5");
        });
    }

    @Test
    public void testAnalyzeSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, ts timestamp) timestamp(ts);",
                    "select first(i) from a sample by 1h align to first observation"
            );

            selectNoLeakCheck(
                    "select first(i) from a sample by 1h align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFillLinear() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, ts timestamp) timestamp(ts);",
                    "select first(i) from a sample by 1h fill(linear) align to first observation"
            );

            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(linear) align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, ts timestamp) timestamp(ts);",
                    "select first(i) from a sample by 1h fill(null) align to first observation"
            );

            // without rewrite
            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(null) align to calendar with offset '10:00'"
            );

            // with rewrite
            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(null) align to calendar"
            );

        });
    }

    @Test
    public void testAnalyzeSampleByFillPrevKeyed() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                    "select s, first(i) from a sample by 1h fill(prev) align to first observation"
            );

            selectNoLeakCheck(
                    "select s, first(i) from a sample by 1h fill(prev) align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFillPrevNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, ts timestamp) timestamp(ts);",
                    "select first(i) from a sample by 1h fill(prev) align to first observation"
            );

            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(prev) align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFillValueKeyed() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                    "select s, first(i) from a sample by 1h fill(1) align to first observation"
            );

            selectNoLeakCheck(
                    "select s, first(i) from a sample by 1h fill(1) align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFillValueNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( i int, ts timestamp) timestamp(ts);",
                    "select first(i) from a sample by 1h fill(1) align to first observation"
            );

            // without rewrite
            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(1) align to calendar with offset '10:00'"
            );

            // with rewrite
            selectNoLeakCheck(
                    "select first(i) from a sample by 1h fill(1) align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByFirstLast() throws Exception {
        assertMemoryLeak(() -> {
            selectNoLeakCheck(
                    "create table a ( l long, s symbol, sym symbol index, i int, ts timestamp) timestamp(ts) partition by day;",
                    "select sym, first(i), last(s), first(l) " +
                            "from a " +
                            "where sym in ('S') " +
                            "and   ts > 0::timestamp and ts < 100::timestamp " +
                            "sample by 1h align to first observation"
            );

            selectNoLeakCheck(
                    "select sym, first(i), last(s), first(l) " +
                            "from a " +
                            "where sym in ('S') " +
                            "and   ts > 0::timestamp and ts < 100::timestamp " +
                            "sample by 1h align to calendar"
            );
        });
    }

    @Test
    public void testAnalyzeSampleByKeyed0() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h align to first observation"
        );
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, i, first(i) from a sample by 1h align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSampleByKeyed1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h align to first observation"
        );
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, i, first(i) from a sample by 1h align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSampleByKeyed2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h fill(null) align to first observation"
        );

        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, first(i) from a sample by 1h fill(null) align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSampleByKeyed3() throws Exception {
        selectLeakCheck(
                "create table a (i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1d fill(linear) align to first observation"
        );
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, first(i) from a sample by 1d fill(linear) align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSampleByKeyed4() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(1,2) align to first observation"
        );

        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, first(i), last(i) from a sample by 1d fill(1,2) align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSampleByKeyed5() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev) align to first observation"
        );

        assertMemoryLeak(() -> selectNoLeakCheck(
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev) align to calendar"
        ));
    }

    @Test
    public void testAnalyzeSelect0() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a"
        );
    }

    @Test
    public void testAnalyzeSelectConcat() throws Exception {
        selectLeakCheck(
                "select concat('a', 'b', rnd_str('c', 'd', 'e'))"
        );
    }

    @Test
    public void testAnalyzeSelectCount1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double)",
                "select count(*) from a"
        );
    }

    @Test
    public void testAnalyzeSelectCount10() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select 1 from a limit 1) "
        );
    }

    @Test // TODO: should return count on first table instead
    public void testAnalyzeSelectCount11() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a lt join a b) "
        );
    }

    @Test // TODO: should return count on first table instead
    public void testAnalyzeSelectCount12() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a asof join a b) "
        );
    }

    @Test // TODO: should return count(first table)*count(second_table) instead
    public void testAnalyzeSelectCount13() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a cross join a b) "
        );
    }

    @Test
    public void testAnalyzeSelectCount14() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts desc "
        );
    }

    @Test
    public void testAnalyzeSelectCount15() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts asc"
        );
    }

    @Test
    public void testAnalyzeSelectCount16() throws Exception {
        selectLeakCheck(
                "create table a (i long, j long)",
                "select count(*) from (select i, j from a group by i, j)"
        );
    }

    @Test
    public void testAnalyzeSelectCount17() throws Exception {
        selectLeakCheck(
                "create table a (i long, j long, d double)",
                "select count(*) from (select i, j from a where d > 42 group by i, j)"
        );
    }

    @Test
    public void testAnalyzeSelectCount2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double)",
                "select count() from a"
        );
    }

    @Test
    public void testAnalyzeSelectCount3() throws Exception {
        selectLeakCheck(
                "create table a ( i int, d double)",
                "select count(2) from a"
        );
    }

    @Test
    public void testAnalyzeSelectCount4() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where s = 'S1'"
        );
    }

    @Test
    public void testAnalyzeSelectCount5() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union all select * from a) "
        );
    }

    @Test
    public void testAnalyzeSelectCount6() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union select * from a) "
        );
    }

    @Test
    public void testAnalyzeSelectCount7() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a intersect select * from a) "
        );
    }

    @Test
    public void testAnalyzeSelectCount8() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where 1=0 "
        );
    }

    @Test
    public void testAnalyzeSelectCount9() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where 1=1 "
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct1() throws Exception {
        selectLeakCheck(
                "create table tab (s symbol, ts timestamp);",
                "select count_distinct(s) from tab"
        );
        selectLeakCheck(
                "select count(distinct s) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct2() throws Exception {
        selectLeakCheck(
                "create table tab (s symbol index, ts timestamp);",
                "select count_distinct(s) from tab"
        );
        selectLeakCheck(
                "select count(distinct s) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct3() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, l long )",
                "select count_distinct(l) from tab"
        );
        selectLeakCheck(
                "select count(distinct l) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct4() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, i int )",
                "select s, count_distinct(i) from tab"
        );
        selectLeakCheck(
                "select s, count(distinct i) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct5() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, ip ipv4 )",
                "select s, count_distinct(ip) from tab"
        );
        selectLeakCheck(
                "select s, count(distinct ip) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct6() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, l long )",
                "select s, count_distinct(l) from tab"
        );
        selectLeakCheck(
                "select s, count(distinct l) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct7() throws Exception {
        selectLeakCheck(
                "create table tab (s symbol, ts timestamp);",
                "select count_distinct(s) from tab where s = 'foobar'"
        );
        selectLeakCheck(
                "select count(distinct s) from tab where s = 'foobar'"
        );
    }

    @Test
    public void testAnalyzeSelectCountDistinct8() throws Exception {
        selectLeakCheck(
                "create table tab (s symbol, ts timestamp);",
                "select count_distinct(s), first(s) from tab"
        );
        selectLeakCheck(
                "select count(distinct s), first(s) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDesc() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc"
        );
    }

    @Test
    public void testAnalyzeSelectDesc2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) ;",
                "select * from a order by ts desc"
        );
    }

    @Test
    public void testAnalyzeSelectDescMaterialized() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) ;",
                "select * from (select i, ts from a union all select 1, null ) order by ts desc"
        );
    }

    @Test
    public void testAnalyzeSelectDistinct0() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct l, ts from tab"
        );
    }

    @Ignore
    @Test // FIXME: somehow only ts gets included, pg returns record type
    public void testAnalyzeSelectDistinct0a() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct (l, ts) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDistinct1() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select distinct(l) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDistinct2() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol, ts timestamp);",
                "select distinct(s) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDistinct3() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol index, ts timestamp);",
                "select distinct(s) from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDistinct4() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select distinct ts, l  from tab"
        );
    }

    @Test
    public void testAnalyzeSelectDoubleInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( d double)");

            selectNoLeakCheck(
                    "select * from t where d in (5, -1, 1, null)"
            );

            selectNoLeakCheck(
                    "select * from t where d not in (5, -1, 1, null)"
            );
        });
    }

    @Test // there's no interval scan because sysdate is evaluated per-row
    public void testAnalyzeSelectDynamicTsInterval1() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > sysdate()"
        );
    }

    @Test // there's no interval scan because systimestamp is evaluated per-row
    public void testAnalyzeSelectDynamicTsInterval2() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > systimestamp()"
        );
    }

    @Test
    public void testAnalyzeSelectDynamicTsInterval3() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > now()"
        );
    }

    @Test
    public void testAnalyzeSelectDynamicTsInterval4() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > dateadd('d', -1, now()) and ts < now()"
        );
    }

    @Test
    public void testAnalyzeSelectDynamicTsInterval5() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now()"
        );
    }

    @Test
    public void testAnalyzeSelectDynamicTsInterval6() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now() order by ts desc"
        );
    }

    @Test
    public void testAnalyzeSelectFromAllTables() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select * from all_tables()"
        ));
    }

    @Test
    public void testAnalyzeSelectFromMemoryMetrics() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select * from memory_metrics()"
        ));
    }

    @Test
    public void testAnalyzeSelectFromReaderPool() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select * from reader_pool()"
        ));
    }

    @Test
    public void testAnalyzeSelectFromTableColumns() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_columns('tab')"
        );
    }

    @Test
    public void testAnalyzeSelectFromTablePartitions() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_partitions('tab')"
        );
    }

    @Test
    public void testAnalyzeSelectFromTableWriterMetrics() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select * from table_writer_metrics()"
        ));
    }

    @Test
    public void testAnalyzeSelectIndexedSymbolWithLimitLoOrderByTsAscNotPartitioned() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbolWithLimitLoOrderByTsAscPartitioned() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;",
                "select * from a where s = 'S1' order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbolWithLimitLoOrderByTsDescNotPartitioned() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbolWithLimitLoOrderByTsDescPartitioned() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;",
                "select * from a where s = 'S1' order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols01a() throws Exception {
        assertMemoryLeak(() -> {
            // if query is ordered by symbol and there's only one partition to scan, there's no need to sort
            testSelectIndexedSymbol("");
            testSelectIndexedSymbol("timestamp(ts)");
            testSelectIndexedSymbolWithIntervalFilter();
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols01b() throws Exception {
        // if query is ordered by symbol and there's more than partition to scan, then sort is necessary even if we use cursor order scan
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp)  timestamp(ts) partition by hour");
            execute("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

            String queryDesc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s desc limit 5";
            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            selectNoLeakCheck(queryDesc);
            // order by asc
            String queryAsc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";
            selectNoLeakCheck(queryAsc);

        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols01c() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select ts, s from a where s in ('S1', 'S2') and length(s) = 2 order by s desc limit 1"
        );
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testAnalyzeSelectIndexedSymbols02() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = $1 or s = $2 order by ts desc limit 1"
        );
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testAnalyzeSelectIndexedSymbols03() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols04() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' and s = 'S2' order by ts desc limit 1"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols05() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') order by ts desc limit 1"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols05a() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') and length(s) = 2 order by ts desc limit 1"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols06() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index) ;",
                "select * from a where s = 'S1' order by s asc limit 10"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols06a() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day",
                "select * from a where s = 'S1' order by s asc limit 10"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols07NonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index)");
            selectNoLeakCheck(
                    "select * from a where s != 'S1' and length(s) = 2 order by s "
            );
            selectNoLeakCheck(
                    "select * from a where s != 'S1' and length(s) = 2 order by s desc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols07Partitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day");

            String query = "select * from a where s != 'S1' and length(s) = 2 and ts in '2023-03-15' order by s #ORDER#";

            selectNoLeakCheck(
                    query.replace("#ORDER#", "asc")
            );

            selectNoLeakCheck(
                    query.replace("#ORDER#", "desc")
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols08() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index)");
            execute("insert into a values ('a'), ('w'), ('b'), ('a'), (null);");

            String query = "select * from a where s != 'a' order by s";
            selectNoLeakCheck(
                    query
            );

            assertQueryNoLeakCheck("s\n" +
                    "\n" +//null
                    "b\n" +
                    "w\n", query, null, true, false);

            query = "select * from a where s != 'a' order by s desc";
            selectNoLeakCheck(
                    query
            );

            assertQueryNoLeakCheck("s\n" +
                    "w\n" +
                    "b\n" +
                    "\n"/*null*/, query, null, true, false);

            query = "select * from a where s != null order by s desc";
            selectNoLeakCheck(
                    query
            );

            assertQueryNoLeakCheck("s\n" +
                    "w\n" +
                    "b\n" +
                    "a\n" +
                    "a\n", query, null, true, false);
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols09() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;",
                "select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols10() throws Exception {
        selectLeakCheck(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') limit 1"
        );
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols10WithOrder() throws Exception {
        assertMemoryLeak(() -> {
            testSelectIndexedSymbols10WithOrder("");
            testSelectIndexedSymbols10WithOrder("partition by hour");
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols11() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select * from a where s in ('S1', 'S2') and length(s) = 2 limit 1"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols12() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a where s1 in ('S1', 'S2') and s2 in ('S2') limit 1"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols13() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            execute("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a where s1 in ('S1')  order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols14() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a where s1 = 'S1'  order by ts desc"
            );
        });
    }

    @Test // backward index scan is triggered only if query uses a single partition and orders by key column and ts desc
    public void testAnalyzeSelectIndexedSymbols15() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a " +
                            "where s1 = 'S1' " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols16() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a " +
                            "where s1 in ('S1', 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc"
            );
        });
    }

    @Test // TODO: should use the same plan as above
    public void testAnalyzeSelectIndexedSymbols17() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            execute("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            selectNoLeakCheck(
                    "select * from a " +
                            "where (s1 = 'S1' or s1 = 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols18() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by hour;");
            execute("insert into a select 'S' || (6-x), dateadd('m', 20*x::int, 0::timestamp) from long_sequence(5)");
            String query = "select * from " +
                    "(" +
                    "  select * from a " +
                    "  where s1 not in ('S1', 'S2') " +
                    "  order by ts asc " +
                    "  limit 5" +
                    ") order by ts asc";
            selectNoLeakCheck(
                    query
            );

            assertQueryNoLeakCheck(
                    "s1\tts\n" +
                            "S5\t1970-01-01T00:20:00.000000Z\n" +
                            "S4\t1970-01-01T00:40:00.000000Z\n" +
                            "S3\t1970-01-01T01:00:00.000000Z\n",
                    query,
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAnalyzeSelectIndexedSymbols7b() throws Exception {
        selectLeakCheck(
                "create table a ( ts timestamp, s symbol index) timestamp(ts);",
                "select s from a where s != 'S1' and length(s) = 2 order by s "
        );
    }

    @Test
    public void testAnalyzeSelectLongInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( l long)");

            selectNoLeakCheck(
                    "select * from t where l in (5, -1, 1, null)"
            );

            selectNoLeakCheck(
                    "select * from t where l not in (5, -1, 1, null)"
            );
        });
    }

    @Test
    public void testAnalyzeSelectNoOrderByWithNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select * from a limit -5"
            );
        });
    }

    @Test
    public void testAnalyzeSelectNoOrderByWithNegativeLimitArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select * from a limit -10+2"
            );
        });
    }

    @Test
    public void testAnalyzeSelectOrderByTsAsIndexDescNegativeLimit() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a order by 2 desc limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsAsc() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts asc"
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsAscAndDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc limit 5) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectOrderByTsDescAndAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc limit 5) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSelectOrderByTsDescLargeNegativeLimit1() throws Exception {
        selectLeakCheck(
                "create table a as (select rnd_int() i, timestamp_sequence(0, 100) ts from long_sequence(10000)) timestamp(ts) ;",
                "select * from a order by ts desc limit 9223372036854775806L+3L "
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsDescLargeNegativeLimit2() throws Exception {
        selectLeakCheck(
                "create table a as (select rnd_int() i, timestamp_sequence(0,100) ts from long_sequence(2_000_000)) timestamp(ts) ;",
                "select * from a order by ts desc limit -1_000_000 "
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsDescNegativeLimit() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsWithNegativeLimit() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts)",
                "select * from a order by ts  limit -5"
        );
    }

    @Test
    public void testAnalyzeSelectOrderByTsWithNegativeLimit1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("insert into a select x,x::timestamp from long_sequence(10)");

            selectNoLeakCheck(
                    "select ts, count(*)  from a sample by 1s ALIGN TO FIRST OBSERVATION limit -5"
            );

            selectNoLeakCheck(
                    "select i, count(*)  from a group by i limit -5"
            );

            selectNoLeakCheck(
                    "select i, count(*)  from a limit -5"
            );

            selectNoLeakCheck(
                    "select distinct(i) from a limit -5"
            );
        });
    }

    @Test
    public void testAnalyzeSelectOrderedAsc() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i asc"
        );
    }

    @Test
    public void testAnalyzeSelectOrderedDesc() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i desc"
        );
    }

    @Test
    public void testAnalyzeSelectOrderedWithLimitLoHi() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i limit 10, 100"
        );
    }

    @Test
    public void testAnalyzeSelectRandomBoolean() throws Exception {
        assertMemoryLeak(() -> selectNoLeakCheck(
                "select rnd_boolean()"
        ));
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval1() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2020-03-01'"
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval10() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc "
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval10a() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc, ts desc "
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval2() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01'"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testAnalyzeSelectStaticTsInterval3() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' or ts in '2020-03-10'"
        );
    }

    @Test // ranges don't overlap so result is empty
    public void testAnalyzeSelectStaticTsInterval4() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' and ts in '2020-03-10'"
        );
    }

    @Test // only 2020-03-10->2020-03-31 needs to be scanned
    public void testAnalyzeSelectStaticTsInterval5() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03' and ts > '2020-03-10'"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testAnalyzeSelectStaticTsInterval6() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testAnalyzeSelectStaticTsInterval7() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') "
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval8() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' "
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsInterval9() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc"
        );
    }

    @Test
    public void testAnalyzeSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where ts > '2020-03-01'"
        );
    }

    @Test
    public void testAnalyzeSelectWalTransactions() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, sy symbol, i int, ts timestamp) timestamp(ts) partition by day WAL",
                "select * from wal_transactions('tab')"
        );
    }

    @Test
    public void testAnalyzeSelectWhereOrderByLimit() throws Exception {
        selectLeakCheck(
                "create table xx ( x long, str string) ",
                "select * from xx where str = 'A' order by str,x limit 10"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter1() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter10() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where s in ( 'A', 'B' )"
        );
    }

    @Test // TODO: this one should interval scan without filter
    public void testAnalyzeSelectWithJittedFilter11() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-02' )"
        );
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testAnalyzeSelectWithJittedFilter12() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'"
        );
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testAnalyzeSelectWithJittedFilter13() throws Exception {
        selectLeakCheck(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter14() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12 or l = 15 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter15() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12.345 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter16() throws Exception {
        selectLeakCheck(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = false "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter17() throws Exception {
        selectLeakCheck(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where not(b = false or ts = 123) "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter18() throws Exception {
        selectLeakCheck(
                "create table tab ( l1 long, l2 long);",
                "select * from tab where l1 < l2 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter19() throws Exception {
        selectLeakCheck(
                "create table tab ( l1 long, l2 long);",
                "select * from tab where l1 * l2 > 0  "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter2() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter20() throws Exception {
        selectLeakCheck(
                "create table tab ( l1 long, l2 long, l3 long);",
                "select * from tab where l1 * l2 > l3  "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter21() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter22() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1 + 1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter23() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = null "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24a() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24b() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24b2() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24c() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24d() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter24e() throws Exception {
        bindVariableService.setInt("maxRows", -1);

        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit :maxRows "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter25() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter26() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter27() throws Exception {
        selectLeakCheck(
                "create table tab (s string, ts timestamp);",
                "select * from tab where s = null "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter28() throws Exception {
        selectLeakCheck(
                "create table tab (v varchar, ts timestamp);",
                "select * from tab where v = null "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter3() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and ts = '2022-01-01' "
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter4() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and l = 20"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter5() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or l = 20"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter6() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or ts = 123"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter7() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter8() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'"
        );
    }

    @Test
    public void testAnalyzeSelectWithJittedFilter9() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l in ( 100, 200 )"
        );
    }

    @Test
    public void testAnalyzeSelectWithLimitLo() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10"
        );
    }

    @Test
    public void testAnalyzeSelectWithLimitLoHi() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10, 100"
        );
    }

    @Test
    public void testAnalyzeSelectWithLimitLoHiNegative() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10, -100"
        );
    }

    @Test
    public void testAnalyzeSelectWithLimitLoNegative() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10"
        );
    }

    @Test // jit is not used due to type mismatch
    public void testAnalyzeSelectWithNonJittedFilter1() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::short "
        );
    }

    @Test // jit filter doesn't work with type casts
    public void testAnalyzeSelectWithNonJittedFilter10() throws Exception {
        selectLeakCheck(
                "create table tab ( s short, ts timestamp);",
                "select * from tab where s = 1::short "
        );
    }

    @Test // TODO: should run with jitted filter just like b = true
    public void testAnalyzeSelectWithNonJittedFilter11() throws Exception {
        selectLeakCheck(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = true::boolean "
        );
    }

    @Test // TODO: should run with jitted filter just like l = 1024
    public void testAnalyzeSelectWithNonJittedFilter12() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 1024::long "
        );
    }

    @Test // TODO: should run with jitted filter just like d = 1024.1
    public void testAnalyzeSelectWithNonJittedFilter13() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1::double "
        );
    }

    @Test // TODO: should run with jitted filter just like d = null
    public void testAnalyzeSelectWithNonJittedFilter14() throws Exception {
        selectLeakCheck(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = null::double "
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testAnalyzeSelectWithNonJittedFilter15() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l | l) > 0  "
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testAnalyzeSelectWithNonJittedFilter16() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l & l) > 0  "
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testAnalyzeSelectWithNonJittedFilter17() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0  "
        );
    }

    @Test
    public void testAnalyzeSelectWithNonJittedFilter18() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit -1"
        );
    }

    @Test
    public void testAnalyzeSelectWithNonJittedFilter19() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong("maxRows", -1);

        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit :maxRows"
        );
    }

    @Test // jit is not used due to type mismatch
    public void testAnalyzeSelectWithNonJittedFilter2() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::byte "
        );
    }

    @Test // jit is not used due to type mismatch
    public void testAnalyzeSelectWithNonJittedFilter3() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = '123' "
        );
    }

    @Test // jit is not because rnd_long() value is not stable
    public void testAnalyzeSelectWithNonJittedFilter4() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = rnd_long() "
        );
    }

    @Test
    public void testAnalyzeSelectWithNonJittedFilter5() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end "
        );
    }

    @Test // interval scan is not used because of type mismatch
    public void testAnalyzeSelectWithNonJittedFilter6() throws Exception {
        selectLeakCheck(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1::string "
        );
    }

    @Test // jit filter doesn't work for string type
    public void testAnalyzeSelectWithNonJittedFilter7() throws Exception {
        selectLeakCheck(
                "create table tab ( s string, ts timestamp);",
                "select * from tab where s = 'test' "
        );
    }

    @Test // jit filter doesn't work with type casts
    public void testAnalyzeSelectWithNonJittedFilter9() throws Exception {
        selectLeakCheck(
                "create table tab ( b byte, ts timestamp);",
                "select * from tab where b = 1::byte "
        );
    }

    @Test
    public void testAnalyzeSelectWithNotOperator() throws Exception {
        selectLeakCheck(
                "CREATE TABLE tst ( timestamp TIMESTAMP );",
                "select * from tst where timestamp not between '2021-01-01' and '2021-01-10' "
        );
    }

    @Test
    public void testAnalyzeSelectWithOrderByTsDescLimitLo() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 10"
        );
    }

    @Test
    public void testAnalyzeSelectWithOrderByTsDescLimitLoNegative1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectWithOrderByTsDescLimitLoNegative2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts desc limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectWithOrderByTsLimitLoNegative1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectWithOrderByTsLimitLoNegative2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts limit -10"
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder1() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l<i"
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder2() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l::short<i"
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder2a() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "where l::short<i " +
                        "limit 100) " +
                        "where l1*i2 != 0"
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder2b() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "order by ts, l1 " +
                        "limit 100 ) " +
                        "where i1*i2 != 0"
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder3() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select k, max(ts) from ( select ts, l as k, i from a where l::short<i ) where k < 0 "
        );
    }

    @Test
    public void testAnalyzeSelectWithReorder4() throws Exception {
        selectLeakCheck(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select mil, k, minl, mini from " +
                        "( select ts as k, max(i*l) as mil, min(i) as mini, min(l) as minl  " +
                        "from a where l::short<i ) " +
                        "where mil + mini> 1 "
        );
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain1a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc limit 10) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain1b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc, l desc limit 10) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc, l limit 10) lt join (select * from a) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain3a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from " +
                            "(select * from (select * from a order by ts asc, l) limit 10) " +
                            "lt join " +
                            "(select * from a) order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain3b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from " +
                            "(select * from (select * from a order by ts desc, l desc) limit 10) " +
                            "order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain4a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts asc, l  " +
                            " limit 10" +
                            ") " +
                            "lt join (select * from a) " +
                            "order by ts asc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortAgain4b() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts desc " +
                            " limit 10" +
                            ") " +
                            "order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSortAscLimitAndSortDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts asc limit 10) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSortDescLimitAndSortAgain() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc limit 10) order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeSortDescLimitAndSortAsc1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts desc limit 10) order by ts asc"
            );
        });
    }

    @Test // TODO: sorting by ts, l again is not necessary
    public void testAnalyzeSortDescLimitAndSortAsc2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts, l limit 10) order by ts, l"
            );
        });
    }

    @Test // TODO: sorting by ts, l again is not necessary
    public void testAnalyzeSortDescLimitAndSortAsc3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long)");

            selectNoLeakCheck(
                    "select * from (select * from a order by ts, l limit 10,-10) order by ts, l"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin0() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            execute("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join b on ts where a.i = b.ts"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            execute("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join b on ts where a.i + b.i = 1"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join b on ts"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join (select * from b limit 10) on ts"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts"
            );
        });
    }

    @Test
    public void testAnalyzeSpliceJoin4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a ( i int, ts timestamp) timestamp(ts)");
            execute("create table b ( i int, ts timestamp) timestamp(ts)");

            selectNoLeakCheck(
                    "select * from a splice join b where a.i = b.i"
            );
        });
    }

    @Test
    public void testAnalyzeTimestampEqSubQueryFilter1() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp)",
                "select * from x where ts = (select min(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeTimestampEqSubQueryFilter2() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp) timestamp(ts) partition by day",
                "select * from x where ts = (select min(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeTimestampGtSubQueryFilter1() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp)",
                "select * from x where ts > (select min(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeTimestampGtSubQueryFilter2() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp) timestamp(ts) partition by day",
                "select * from x where ts > (select min(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeTimestampLtSubQueryFilter1() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp)",
                "select * from x where ts < (select max(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeTimestampLtSubQueryFilter2() throws Exception {
        selectLeakCheck(
                "create table x (l long, ts timestamp) timestamp(ts) partition by day",
                "select * from x where ts < (select max(ts) from x)"
        );
    }

    @Test
    public void testAnalyzeUnion() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a union select * from a"
        );
    }

    @Test
    public void testAnalyzeUnionAll() throws Exception {
        selectLeakCheck(
                "create table a ( i int, s string);",
                "select * from a union all select * from a"
        );
    }

    @Test
    public void testAnalyzeWhereOrderByTsLimit1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t ( x long, ts timestamp) timestamp(ts)");

            String query = "select * from t where x < 100 order by ts desc limit -5";
            selectNoLeakCheck(
                    query
            );

            execute("insert into t select x, x::timestamp from long_sequence(10000)");

            assertQueryNoLeakCheck(
                    "x\tts\n" +
                            "5\t1970-01-01T00:00:00.000005Z\n" +
                            "4\t1970-01-01T00:00:00.000004Z\n" +
                            "3\t1970-01-01T00:00:00.000003Z\n" +
                            "2\t1970-01-01T00:00:00.000002Z\n" +
                            "1\t1970-01-01T00:00:00.000001Z\n",
                    query,
                    "ts###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testAnalyzeWhereUuid() throws Exception {
        selectLeakCheck(
                "create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u = '11111111-1111-1111-1111-111111111111' or u = '22222222-2222-2222-2222-222222222222' or u = '33333333-3333-3333-3333-333333333333'"
        );
    }

    @Test
    public void testAnalyzeWindow0() throws Exception {
        selectLeakCheck(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select ts, str,  row_number() over (order by l), row_number() over (partition by l) from t"
        );
    }

    @Test
    public void testAnalyzeWindow1() throws Exception {
        selectLeakCheck(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l, 10, row_number() over ( partition by l order by ts) from t"
        );
    }

    @Test
    public void testAnalyzeWindow2() throws Exception {
        selectLeakCheck(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l as l1, ts::long+l as tsum, row_number() over ( partition by l, ts order by str) from t"
        );
    }

    @Test
    public void testAnalyzeWindow3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            selectNoLeakCheck(
                    "select ts, i, j, " +
                            "avg(j) over (order by i, j rows unbounded preceding), " +
                            "sum(j) over (order by i, j rows unbounded preceding), " +
                            "first_value(j) over (order by i, j rows unbounded preceding), " +
                            "from tab"
            );

            selectNoLeakCheck(
                    "select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row) " +
                            "from tab"
            );

            selectNoLeakCheck(
                    "select row_number() over (partition by i order by i desc, j asc), " +
                            "avg(j) over (partition by i order by j, i desc rows unbounded preceding), " +
                            "sum(j) over (partition by i order by j, i desc rows unbounded preceding), " +
                            "first_value(j) over (partition by i order by j, i desc rows unbounded preceding) " +
                            "from tab " +
                            "order by ts desc"
            );

            selectNoLeakCheck(
                    "select row_number() over (partition by i order by i desc, j asc), " +
                            "        avg(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        sum(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        first_value(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        rank() over (partition by j, i) " +
                            "from tab order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeWindowModelOrderByIsNotIgnored() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            selectNoLeakCheck(
                    "select sum(avg), sum(sum), sum(first_value) from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc\n" +
                            ") "
            );

            selectNoLeakCheck(
                    "select sum(avg), sum(sum), sum(first_value) from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") "
            );

            selectNoLeakCheck(
                    "select sum(avg), sum(sum) sm, sum(first_value) fst from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc\n" +
                            ") order by sm "
            );
        });
    }

    @Test
    public void testAnalyzeWindowOrderByUnderWindowModelIsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            selectNoLeakCheck(
                    "select sum(avg), sum(sum), first(first_value) from ( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc" +
                            ") "
            );

            selectNoLeakCheck(
                    "select sum(avg), sum(sum), first(first_value) from ( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by 1 desc"
            );
        });
    }

    // TODO: remove artificial limit models used to force ordering on window models (and avoid unnecessary sorts)
    @Test
    public void testAnalyzeWindowParentModelOrderPushdownIsBlockedWhenWindowModelSpecifiesOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value, " +
                            "from cpu_ts " +
                            "order by ts desc " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc " +
                            ") order by hostname"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts asc "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts desc "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by hostname "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by hostname "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            "order by ts desc " +
                            ") order by ts asc "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            "order by ts asc " +
                            ") order by ts desc "
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc ) " +
                            "order by ts asc " +
                            ") order by hostname "
            );
        });
    }

    @Test
    public void testAnalyzeWindowParentModelOrderPushdownIsDoneWhenNestedModelsSpecifyNoneOrMatchingOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value, " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by hostname) " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc  " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            "order by ts asc  " +
                            ") order by ts asc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by hostname) " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc  " +
                            ") order by ts desc"
            );

            selectNoLeakCheck(
                    "select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            "order by ts desc  " +
                            ") order by ts desc"
            );
        });
    }

    @Test
    public void testAnalyzeWindowRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as ( " +
                    "  select " +
                    "    cast(x as int) i, " +
                    "    rnd_symbol('a','b','c') sym, " +
                    "    timestamp_sequence(0, 100000000) ts " +
                    "   from long_sequence(100)" +
                    ") timestamp(ts) partition by hour");

            String sql = "select i, " +
                    "row_number() over (partition by sym), " +
                    "avg(i) over (partition by i rows unbounded preceding), " +
                    "sum(i) over (partition by i rows unbounded preceding), " +
                    "first_value(i) over (partition by i rows unbounded preceding) " +
                    "from x limit 3";
            selectNoLeakCheck(
                    sql
            );

            assertSql("i\trow_number\tavg\tsum\tfirst_value\n" +
                    "1\t1\t1.0\t1.0\t1.0\n" +
                    "2\t2\t2.0\t2.0\t2.0\n" +
                    "3\t1\t3.0\t3.0\t3.0\n", sql);
        });
    }

    @Test
    public void testAnalyzeWithBindVariables() throws Exception {
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
        if (factory instanceof SwappingArgsFunctionFactory) {
            return ((SwappingArgsFunctionFactory) factory).getDelegate() instanceof EqSymTimestampFunctionFactory;
        }

        if (factory instanceof NegatingFunctionFactory) {
            if (((NegatingFunctionFactory) factory).getDelegate() instanceof SwappingArgsFunctionFactory) {
                return ((SwappingArgsFunctionFactory) ((NegatingFunctionFactory) factory).getDelegate()).getDelegate() instanceof EqSymTimestampFunctionFactory;
            }
            return ((NegatingFunctionFactory) factory).getDelegate() instanceof EqSymTimestampFunctionFactory;
        }

        return false;
    }

    private static boolean isIPv4StrFactory(FunctionFactory factory) {
        if (factory instanceof SwappingArgsFunctionFactory) {
            return isIPv4StrFactory(((SwappingArgsFunctionFactory) factory).getDelegate());
        }
        if (factory instanceof NegatingFunctionFactory) {
            return isIPv4StrFactory(((NegatingFunctionFactory) factory).getDelegate());
        }
        return factory instanceof EqIPv4FunctionFactory
                || factory instanceof EqIPv4StrFunctionFactory
                || factory instanceof LtIPv4StrFunctionFactory
                || factory instanceof LtStrIPv4FunctionFactory;
    }

    private static boolean isLong256StrFactory(FunctionFactory factory) {
        if (factory instanceof SwappingArgsFunctionFactory) {
            return isLong256StrFactory(((SwappingArgsFunctionFactory) factory).getDelegate());
        }
        if (factory instanceof NegatingFunctionFactory) {
            return isLong256StrFactory(((NegatingFunctionFactory) factory).getDelegate());
        }
        return factory instanceof EqLong256StrFunctionFactory;
    }

    @SuppressWarnings("unused")
    private void assertBindVarPlan(String type) throws Exception {
        selectNoLeakCheck(
                "select * from t where x = :v1 "
        );
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
            case ColumnType.TIMESTAMP:
                return new TimestampConstant(val * 86_400_000L);
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
            execute("create table table_1 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          member boolean\n" +
                    "        ) timestamp(ts) PARTITION by month");

            execute("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            execute("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            execute("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            execute("create table table_2 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          address string\n" +
                    "        ) timestamp(ts) PARTITION by month");

            execute("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            execute("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
        });
    }

    // left join maintains order metadata and can be part of asof join
    @SuppressWarnings("unused")
    private void testHashAndAsOfJoin(SqlCompiler compiler, boolean isLight, boolean isFastAsOfJoin) throws Exception {
        execute("create table taba (a1 int, ts1 timestamp) timestamp(ts1)");
        execute("create table tabb (b1 int, b2 long)");
        execute("create table tabc (c1 int, c2 long, ts3 timestamp) timestamp(ts3)");

        selectNoLeakCheck(
                "select * " +
                        "from taba " +
                        "left join tabb on a1=b1 " +
                        "asof join tabc on b1=c1"
        );
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
        selectNoLeakCheck(
                query
        );

        assertQueryNoLeakCheck("s\tts\n" +
                "S2\t1970-01-01T00:00:00.000000Z\n" +
                "S2\t1970-01-01T01:00:00.000003Z\n" +
                "S1\t1970-01-01T00:00:00.000001Z\n", query, null, true, true);

        //order by asc
        query = "select * from a where s in (:s1, :s2) order by s asc limit 5";

        selectNoLeakCheck(
                query
        );

        assertQueryNoLeakCheck("s\tts\n" +
                "S1\t1970-01-01T00:00:00.000001Z\n" +
                "S2\t1970-01-01T00:00:00.000000Z\n" +
                "S2\t1970-01-01T01:00:00.000003Z\n", query, null, true, true);
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
        selectNoLeakCheck(
                query
        );

        assertQueryNoLeakCheck("s\tts\n" +
                "S2\t1970-01-01T00:00:00.000000Z\n" +
                "S2\t1970-01-01T01:00:00.000003Z\n" +
                "S1\t1970-01-01T00:00:00.000001Z\n", query, null, true, true);

        //order by asc
        query = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";

        selectNoLeakCheck(
                query
        );

        assertQueryNoLeakCheck("s\tts\n" +
                "S1\t1970-01-01T00:00:00.000001Z\n" +
                "S2\t1970-01-01T00:00:00.000000Z\n" +
                "S2\t1970-01-01T01:00:00.000003Z\n", query, null, true, true);
    }

    private void testSelectIndexedSymbols10WithOrder(String partitionByClause) throws Exception {
        execute("drop table if exists a");
        execute("create table a ( s symbol index, ts timestamp) timestamp(ts)" + partitionByClause);
        execute("insert into a values ('S2', 1), ('S3', 2),('S1', 3+3600000000),('S2', 4+3600000000), ('S1', 5+3600000000);");

        bindVariableService.clear();
        bindVariableService.setStr("s1", "S1");
        bindVariableService.setStr("s2", "S2");

        String queryAsc = "select * from a where s in (:s2, :s1) order by ts asc limit 5";
        selectNoLeakCheck(
                queryAsc
        );
        assertQueryNoLeakCheck("s\tts\n" +
                "S2\t1970-01-01T00:00:00.000001Z\n" +
                "S1\t1970-01-01T01:00:00.000003Z\n" +
                "S2\t1970-01-01T01:00:00.000004Z\n" +
                "S1\t1970-01-01T01:00:00.000005Z\n", queryAsc, "ts", true, true);

        String queryDesc = "select * from a where s in (:s2, :s1) order by ts desc limit 5";
        selectNoLeakCheck(
                queryDesc
        );
        assertQueryNoLeakCheck("s\tts\n" +
                "S1\t1970-01-01T01:00:00.000005Z\n" +
                "S2\t1970-01-01T01:00:00.000004Z\n" +
                "S1\t1970-01-01T01:00:00.000003Z\n" +
                "S2\t1970-01-01T00:00:00.000001Z\n", queryDesc, "ts###DESC", true, true);
    }
}