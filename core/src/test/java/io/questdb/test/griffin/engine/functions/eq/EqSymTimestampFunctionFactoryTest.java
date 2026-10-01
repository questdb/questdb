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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.functions.eq.EqSymTimestampFunctionFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class EqSymTimestampFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBasicConstant() throws Exception {
        assertQuery("select '2017-01-01'::symbol = '2017-01-01'::timestamp")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        true
                        """);
        assertQuery("select '2017-01-01'::symbol = '2017-01-01'::timestamp_ns")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        true
                        """);

    }

    @Test
    public void testDynamicCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            71892425\t1970-01-01T00:01:11.892425Z
                            32891513\t1970-01-01T00:00:32.891513Z
                            58263256\t1970-01-01T00:00:58.263256Z
                            69433038\t1970-01-01T00:01:09.433038Z
                            49660563\t1970-01-01T00:00:49.660563Z
                            28354879\t1970-01-01T00:00:28.354879Z
                            25030044\t1970-01-01T00:00:25.030044Z
                            13225820\t1970-01-01T00:00:13.225820Z
                            60453090\t1970-01-01T00:01:00.453090Z
                            """);
        });
    }

    @Test
    public void testDynamicCast1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            28258937621\t1970-01-01T00:00:28.258937621Z
                            84704108866\t1970-01-01T00:01:24.704108866Z
                            4684409603\t1970-01-01T00:00:04.684409603Z
                            29566122052\t1970-01-01T00:00:29.566122052Z
                            62471208567\t1970-01-01T00:01:02.471208567Z
                            17048275024\t1970-01-01T00:00:17.048275024Z
                            80408674000\t1970-01-01T00:01:20.408674000Z
                            23080510639\t1970-01-01T00:00:23.080510639Z
                            82509017689\t1970-01-01T00:01:22.509017689Z
                            28729051699\t1970-01-01T00:00:28.729051699Z
                            """);
        });
    }

    @Test
    public void testDynamicCastConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where '37847040'::symbol = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            """);
        });
    }

    @Test
    public void testDynamicCastConst1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where '82509017689'::symbol = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            82509017689\t1970-01-01T00:01:22.509017689Z
                            """);
        });
    }

    @Test
    public void testDynamicCastNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = null::timestamp")
                    .noLeakCheck()
                    .returns("sym\tt\n");
        });
    }

    @Test
    public void testDynamicCastNull1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = null::timestamp_ns")
                    .noLeakCheck()
                    .returns("sym\tt\n");
        });
    }

    @Test
    public void testDynamicCastNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 3) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            \t
                            47753932\t1970-01-01T00:00:47.753932Z
                            85842605\t1970-01-01T00:01:25.842605Z
                            63734605\t1970-01-01T00:01:03.734605Z
                            49228924\t1970-01-01T00:00:49.228924Z
                            8395072\t1970-01-01T00:00:08.395072Z
                            63602242\t1970-01-01T00:01:03.602242Z
                            74504721\t1970-01-01T00:01:14.504721Z
                            \t
                            """);
        });
    }

    @Test
    public void testDynamicCastNulls1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 3) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            assertQuery("select sym, t from x where sym = t")
                    .noLeakCheck()
                    .returns("""
                            sym\tt
                            28258937621\t1970-01-01T00:00:28.258937621Z
                            \t
                            17536982995\t1970-01-01T00:00:17.536982995Z
                            35652982957\t1970-01-01T00:00:35.652982957Z
                            65390153277\t1970-01-01T00:01:05.390153277Z
                            15568952078\t1970-01-01T00:00:15.568952078Z
                            74965011151\t1970-01-01T00:01:14.965011151Z
                            12591706140\t1970-01-01T00:00:12.591706140Z
                            23253230564\t1970-01-01T00:00:23.253230564Z
                            \t
                            """);
        });
    }

    @Test
    public void testDynamicSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select x from long_sequence(10) where rnd_symbol('1','3','5') = 3::timestamp")
                    .noLeakCheck()
                    .returnsOnce("""
                            x
                            3
                            8
                            10
                            """);

            assertQuery("select x from long_sequence(10) where rnd_symbol('1','3','5') = 3::timestamp_ns")
                    .noLeakCheck()
                    .returnsOnce("""
                            x
                            1
                            3
                            4
                            5
                            8
                            10
                            """);
        });
    }

    @Test
    public void testLongSymbolCacheReopen() throws Exception {
        testSymbolCacheReopen("LONG");
    }

    @Test
    public void testOptimisationWithARuntimeConstantNanoTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp_ns(0, 86400000000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            String query = "select sym, t from x where sym = $1";

            bindVariableService.setTimestampNano(0, NanosTimestampDriver.INSTANCE.parseFloorLiteral("1970-01-01T00:00:28.258937621Z"));
            assertQuery(query)
                    .timestamp("")
                    .returns("""
                            sym\tt
                            28258937621\t1970-01-01T00:00:28.258937621Z
                            """);

        });
    }

    @Test
    public void testOptimisationWithARuntimeConstantTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_timestamp(0, 86400000, 0) t from long_sequence(10))");
            execute("alter table x add column sym symbol");
            execute("update x set sym = t::symbol");

            String query = "select sym, t from x where sym = $1";

            bindVariableService.setTimestamp(0, MicrosTimestampDriver.INSTANCE.parseFloorLiteral("1970-01-01T00:00:37.847040Z"));
            assertQuery(query)
                    .timestamp("")
                    .returns("""
                            sym\tt
                            37847040\t1970-01-01T00:00:37.847040Z
                            """);

        });
    }

    @Test
    public void testRuntimeConstantRebind() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (stamp VARCHAR)");
            execute("INSERT INTO x VALUES ('1'), ('2'), (null)");
            for (boolean isNano : new boolean[]{false, true}) {
                ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("first timestamp", "matches\ntrue\nfalse\nfalse\n", b -> {
                    if (isNano) {
                        b.setTimestampNano(0, 1);
                    } else {
                        b.setTimestamp(0, 1);
                    }
                }));
                cases.add(BindVarTuple.ok("second timestamp", "matches\nfalse\ntrue\nfalse\n", b -> {
                    if (isNano) {
                        b.setTimestampNano(0, 2);
                    } else {
                        b.setTimestamp(0, 2);
                    }
                }));
                cases.add(BindVarTuple.ok("null timestamp", "matches\nfalse\nfalse\ntrue\n", b -> {
                    if (isNano) {
                        b.setTimestampNano(0, Numbers.LONG_NULL);
                    } else {
                        b.setTimestamp(0, Numbers.LONG_NULL);
                    }
                }));
                assertQuery("SELECT stamp::SYMBOL = $1 AS matches FROM x").expectSize().assertBinds(cases);
            }
        });
    }

    @Test
    public void testStaticSymbolCacheReopen() throws Exception {
        testSymbolCacheReopen("SYMBOL");
    }

    @Test
    public void testStaticSymbolCacheTruncateReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (stamp SYMBOL)");
            execute("INSERT INTO x VALUES ('1')");
            final String query = "SELECT stamp = 1::TIMESTAMP AS matches FROM x";
            try (RecordCursorFactory factory = select(query)) {
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns("matches\ntrue\n");
                execute("TRUNCATE TABLE x");
                execute("INSERT INTO x VALUES ('2')");
                assertQuery(query).expectSize().returns("matches\nfalse\n");
                new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns("matches\nfalse\n");
            }
        });
    }

    @Test
    public void testStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a from long_sequence(10))");
            assertQuery("select a from x where a = 3::timestamp")
                    .noLeakCheck()
                    .returns("""
                            a
                            3
                            3
                            3
                            """);
        });
    }

    @Test
    public void testVarcharSymbolCacheReopen() throws Exception {
        testSymbolCacheReopen("VARCHAR");
    }

    @Test
    public void testVarcharSymbolFilterCacheReopen() throws Exception {
        testVarcharSymbolFilterCacheReopen(false);
    }

    @Test
    public void testVarcharSymbolFilterCacheReopenParallel() throws Exception {
        testVarcharSymbolFilterCacheReopen(true);
    }

    @Test
    public void testStaticSymbolTableNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a from long_sequence(30))");
            assertQuery("select a from x where a = null::timestamp")
                    .noLeakCheck()
                    .returns("""
                            a
                            
                            
                            
                            
                            
                            
                            
                            
                            """);
            assertQuery("select a from x where a = null::timestamp_ns")
                    .noLeakCheck()
                    .returns("""
                            a
                            
                            
                            
                            
                            
                            
                            
                            
                            """);
        });
    }

    @Test
    public void testSymbolCacheInitLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            final int threshold = EqSymTimestampFunctionFactory.BITSET_OPTIMISATION_THRESHOLD;
            for (boolean isNano : new boolean[]{false, true}) {
                for (boolean isNegated : new boolean[]{false, true}) {
                    for (long timestamp : new long[]{1, Numbers.LONG_NULL}) {
                        IntList initOrder = new IntList();
                        MutableSymbolFunction symbolFunction = new MutableSymbolFunction(initOrder);
                        ObjList<Function> args = new ObjList<>();
                        args.add(symbolFunction);
                        args.add(new TimestampConstant(timestamp, isNano ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO) {
                            @Override
                            public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
                                Assert.assertNull(symbolTableSource);
                                Assert.assertSame(sqlExecutionContext, executionContext);
                                initOrder.add(1);
                            }
                        });
                        try (Function function = new EqSymTimestampFunctionFactory().newInstance(
                                0, args, IntList.createWithValues(0, 0), configuration, sqlExecutionContext
                        )) {
                            if (isNegated) {
                                ((NegatableBooleanFunction) function).setNegated();
                            }
                            Assert.assertFalse(function.isThreadSafe());
                            assertSymbolCacheInit(function, initOrder);
                            Assert.assertEquals(0, symbolFunction.symbolCalls);
                            for (int epoch = 0; epoch < 3; epoch++) {
                                if (epoch == 1) {
                                    symbolFunction.hasInitFailure = true;
                                    initOrder.clear();
                                    try {
                                        function.init(null, sqlExecutionContext);
                                        Assert.fail("expected child init failure");
                                    } catch (SqlException e) {
                                        TestUtils.assertEquals("test symbol init failure", e.getFlyweightMessage());
                                    }
                                    Assert.assertEquals(1, initOrder.size());
                                    Assert.assertEquals(0, initOrder.getQuick(0));
                                    symbolFunction.hasInitFailure = false;
                                }
                                assertSymbolCacheInit(function, initOrder);
                                for (int key : new int[]{SymbolTable.VALUE_IS_NULL, -1, 0, 1, threshold - 1, threshold}) {
                                    symbolFunction.key = key;
                                    symbolFunction.value = key < 0 ? null : ((key + epoch) % 2 == 0 ? "1" : "2");
                                    final boolean isEqual = key < 0 ? timestamp == Numbers.LONG_NULL
                                            : timestamp == 1 && "1".equals(symbolFunction.value);
                                    final boolean isExpected = isNegated != isEqual;
                                    final boolean isCached = key >= 0 && key < threshold;
                                    final int callsBefore = symbolFunction.symbolCalls;
                                    Assert.assertEquals(isExpected, function.getBool(null));
                                    Assert.assertEquals(isExpected, function.getBool(null));
                                    function.toTop();
                                    Assert.assertEquals(isExpected, function.getBool(null));
                                    Assert.assertEquals(callsBefore + (isCached ? 1 : 3), symbolFunction.symbolCalls);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolCacheReopenAfterInvalidTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (stamp VARCHAR)");
            for (boolean isNano : new boolean[]{false, true}) {
                execute("TRUNCATE TABLE x");
                execute("INSERT INTO x VALUES ('1')");
                final String timestamp = isNano ? "TIMESTAMP_NS" : "TIMESTAMP";
                try (RecordCursorFactory factory = select("SELECT stamp::SYMBOL = 1::" + timestamp + " AS matches FROM x")) {
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns("matches\ntrue\n");
                    execute("UPDATE x SET stamp = 'invalid'");
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        for (int i = 0; i < 2; i++) {
                            try {
                                cursor.getRecord().getBool(0);
                                Assert.fail("expected invalid timestamp");
                            } catch (ImplicitCastException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: `invalid` [SYMBOL -> " + timestamp + "]");
                            }
                        }
                    }
                    execute("UPDATE x SET stamp = '2'");
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext).expectSize().returns("matches\nfalse\n");
                }
            }
        });
    }

    @Test
    public void testUnionSymbolCacheReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (stamp VARCHAR)");
            execute("CREATE TABLE y (stamp VARCHAR)");
            execute("INSERT INTO x VALUES ('1')");
            execute("INSERT INTO y VALUES ('2')");
            for (boolean isNano : new boolean[]{false, true}) {
                execute("UPDATE x SET stamp = '1'");
                execute("UPDATE y SET stamp = '2'");
                final String timestamp = isNano ? "TIMESTAMP_NS" : "TIMESTAMP";
                final String query = "SELECT stamp, stamp = 1::" + timestamp + " AS eq, stamp != 1::" + timestamp + " AS ne FROM ("
                        + "SELECT stamp::SYMBOL AS stamp FROM x UNION ALL SELECT stamp::SYMBOL AS stamp FROM y)";
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(UnionSymbolCastRecordCursorFactory.class, factory.getBaseFactory().getBaseFactory().getClass());
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                            .withBaseFactoryClass(VirtualRecordCursorFactory.class)
                            .columnType(0, ColumnType.SYMBOL).noRandomAccess().expectSize()
                            .returns("""
                                    stamp\teq\tne
                                    1\ttrue\tfalse
                                    2\tfalse\ttrue
                                    """);
                    execute("UPDATE x SET stamp = '2'");
                    execute("UPDATE y SET stamp = '1'");
                    new QueryAssertion(engine, factory).withContext(sqlExecutionContext)
                            .withBaseFactoryClass(VirtualRecordCursorFactory.class)
                            .columnType(0, ColumnType.SYMBOL).noRandomAccess().expectSize()
                            .returns("""
                                    stamp\teq\tne
                                    2\tfalse\ttrue
                                    1\ttrue\tfalse
                                    """);
                }
            }
        });
    }

    @Test
    public void testVarcharSymbolCompoundFilterCacheReopen() throws Exception {
        testVarcharSymbolCompoundFilterCacheReopen(false);
    }

    @Test
    public void testVarcharSymbolCompoundFilterCacheReopenParallel() throws Exception {
        testVarcharSymbolCompoundFilterCacheReopen(true);
    }

    private void assertSymbolCacheInit(Function function, IntList initOrder) throws SqlException {
        initOrder.clear();
        function.init(null, sqlExecutionContext);
        Assert.assertEquals(2, initOrder.size());
        Assert.assertEquals(0, initOrder.getQuick(0));
        Assert.assertEquals(1, initOrder.getQuick(1));
    }

    private void testSymbolCacheReopen(String type) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, stamp " + type + ")");
            for (boolean isNano : new boolean[]{false, true}) {
                execute("TRUNCATE TABLE x");
                execute("INSERT INTO x VALUES (1, '1'), (2, '2'), (3, null)");
                final String timestamp = isNano ? "TIMESTAMP_NS" : "TIMESTAMP";
                assertQuery("SELECT stamp::SYMBOL = 1::" + timestamp + " AS eq, stamp::SYMBOL != 1::" + timestamp
                        + " AS ne, stamp::SYMBOL = null::" + timestamp + " AS is_null FROM x")
                        .expectSize()
                        .mutateWith("UPDATE x SET stamp = (CASE WHEN id = 1 THEN '2'::STRING WHEN id = 2 THEN '1'::STRING ELSE null END)::" + type)
                        .returns("""
                                        eq\tne\tis_null
                                        true\tfalse\tfalse
                                        false\ttrue\tfalse
                                        false\ttrue\ttrue
                                        """,
                                """
                                        eq\tne\tis_null
                                        false\ttrue\tfalse
                                        true\tfalse\tfalse
                                        false\ttrue\ttrue
                                        """);
            }
        });
    }

    private void testVarcharSymbolCompoundFilterCacheReopen(boolean isParallel) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelFilterEnabled(isParallel);
            execute("CREATE TABLE x (id INT, stamp VARCHAR)");
            for (boolean isNano : new boolean[]{false, true}) {
                execute("TRUNCATE TABLE x");
                execute("INSERT INTO x VALUES (1, '1'), (2, '2'), (3, null), (4, '1')");
                final String timestamp = isNano ? "TIMESTAMP_NS" : "TIMESTAMP";
                assertQuery("SELECT * FROM x WHERE stamp::SYMBOL = 1::" + timestamp + " AND id < 4")
                        .withBaseFactoryClass(isParallel ? AsyncFilteredRecordCursorFactory.class : FilteredRecordCursorFactory.class)
                        .mutateWith("UPDATE x SET stamp = CASE WHEN id = 1 THEN '2'::VARCHAR WHEN id = 2 THEN '1'::VARCHAR ELSE stamp END")
                        .returns("id\tstamp\n1\t1\n", "id\tstamp\n2\t1\n");
            }
        });
    }

    private void testVarcharSymbolFilterCacheReopen(boolean isParallel) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelFilterEnabled(isParallel);
            execute("CREATE TABLE x (id INT, stamp VARCHAR)");
            execute("INSERT INTO x VALUES (1, '1'), (2, '2'), (3, null)");
            assertQuery("SELECT * FROM x WHERE stamp::SYMBOL = 1::TIMESTAMP")
                    .withBaseFactoryClass(isParallel ? AsyncFilteredRecordCursorFactory.class : FilteredRecordCursorFactory.class)
                    .mutateWith("UPDATE x SET stamp = CASE WHEN id = 1 THEN '2'::VARCHAR WHEN id = 2 THEN '1'::VARCHAR ELSE null END")
                    .returns("id\tstamp\n1\t1\n", "id\tstamp\n2\t1\n");
        });
    }

    private static class MutableSymbolFunction extends SymbolFunction {
        private final IntList initOrder;
        private boolean hasInitFailure;
        private int key;
        private int symbolCalls;
        private String value;

        private MutableSymbolFunction(IntList initOrder) {
            this.initOrder = initOrder;
        }

        @Override
        public int getInt(Record rec) {
            return key;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            symbolCalls++;
            return value;
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            Assert.assertNull(symbolTableSource);
            Assert.assertSame(sqlExecutionContext, executionContext);
            initOrder.add(0);
            if (hasInitFailure) {
                throw SqlException.$(0, "test symbol init failure");
            }
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key == SymbolTable.VALUE_IS_NULL) {
                return null;
            }
            Assert.assertEquals(this.key, key);
            return value;
        }
    }
}
