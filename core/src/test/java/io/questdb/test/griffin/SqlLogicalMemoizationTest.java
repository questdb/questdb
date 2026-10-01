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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalMemoizationTest extends AbstractCairoTest {
    @Test
    public void testConstantsSingleReadsAndDisabledPolicy() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createRows();
            assertRowsAndMemoizers("SELECT d+1.0 AS value FROM lp_memo", "value\n4.0\n2.0\n3.0\nnull\n", 0);
            assertRowsAndMemoizers("SELECT 7 AS value FROM lp_memo ORDER BY value", "value\n7\n7\n7\n7\n", 0);
            SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
            try {
                assertRowsAndMemoizers("SELECT d+1.0 AS value FROM lp_memo WHERE d>0.0 ORDER BY value DESC LIMIT 2", "value\n4.0\n3.0\n", 0);
            } finally {
                SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = true;
            }
        });
    }

    @Test
    public void testInterleavedRecordsKeepNumericAndTextCachesSeparate() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createRows();
            final String sql = "SELECT v,v AS v2,s,s AS s2,u,u AS u2 FROM (SELECT d+1.0 AS v,id::STRING AS s,id::VARCHAR AS u FROM lp_memo) WHERE v>0.0";
            assertRowsAndMemoizers(sql, "v\tv2\ts\ts2\tu\tu2\n4.0\t4.0\t1\t1\t1\t1\n2.0\t2.0\t2\t2\t2\t2\n3.0\t3.0\t3\t3\t3\t3\n", 3);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record a = cursor.getRecord();
                    final Record b = cursor.getRecordB();
                    Assert.assertTrue(cursor.hasNext());
                    cursor.recordAt(b, a.getRowId());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(2.0, a.getDouble(0), 0.0);
                    final CharSequence strA = a.getStrA(2);
                    final Utf8Sequence varcharA = a.getVarcharA(4);
                    Assert.assertEquals(4.0, b.getDouble(0), 0.0);
                    final CharSequence strB = b.getStrB(2);
                    final Utf8Sequence varcharB = b.getVarcharB(4);
                    TestUtils.assertEquals("2", strA);
                    TestUtils.assertEquals("1", strB);
                    Assert.assertEquals("2", Utf8s.toString(varcharA));
                    Assert.assertEquals("1", Utf8s.toString(varcharB));
                    Assert.assertEquals(2.0, a.getDouble(0), 0.0);
                    TestUtils.assertEquals("2", a.getStrA(2));
                    Assert.assertEquals("2", Utf8s.toString(a.getVarcharA(4)));
                }
            }
        });
    }

    @Test
    public void testMaterializationBoundariesAndAggregateArguments() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createRows();
            assertRowsAndMemoizers("SELECT sum(v),min(v) FROM (SELECT d+1.0 AS v FROM lp_memo)", "sum\tmin\n9.0\t2.0\n", 1);
            assertRowsAndMemoizers("SELECT s,s AS second FROM (SELECT sum(d+1.0) AS s FROM lp_memo)", "s\tsecond\n9.0\t9.0\n", 0);
            assertRowsAndMemoizers("SELECT l.v,l.v AS second FROM (SELECT id,d+1.0 AS v FROM lp_memo) l JOIN lp_memo r ON(id)",
                    "v\tsecond\n4.0\t4.0\n2.0\t2.0\n3.0\t3.0\nnull\tnull\n", 0);
            assertRowsAndMemoizers("SELECT v FROM (SELECT d+1.0 AS v FROM lp_memo LIMIT 2) WHERE v>0.0", "v\n4.0\n2.0\n", 1);
            assertRowsAndMemoizers("SELECT v FROM (SELECT d+1.0 AS v FROM lp_memo LIMIT 2) ORDER BY v", "v\n2.0\n4.0\n", 1);
            assertRowsAndMemoizers("SELECT v,v AS second FROM ((SELECT d+1.0 AS v FROM lp_memo) UNION ALL (SELECT d+2.0 AS v FROM lp_memo))",
                    "v\tsecond\n4.0\t4.0\n2.0\t2.0\n3.0\t3.0\nnull\tnull\n5.0\t5.0\n3.0\t3.0\n4.0\t4.0\nnull\tnull\n", 2);
        });
    }

    @Test
    public void testMemoizedOwnedFunctionSurvivesCompilerResetCloseAndDataChange() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createRows();
            final String sql = "SELECT CASE WHEN id IN (1,2,3,5) THEN d+1.0 ELSE d END AS value FROM lp_memo WHERE d>0.0 ORDER BY value DESC LIMIT 2";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    assertMemoizerCount(retained, 1);
                    assertResult(retained, "value\n4.0\n3.0\n");
                    try (RecordCursorFactory other = compiler.compile("SELECT d+2.0 AS value FROM lp_memo LIMIT 1", sqlExecutionContext).getRecordCursorFactory()) {
                        assertMemoizerCount(other, 0);
                        assertResult(other, "value\n5.0\n");
                    }
                    compiler.clear();
                    assertResult(retained, "value\n4.0\n3.0\n");
                }
                assertResult(retained, "value\n4.0\n3.0\n");
                execute("INSERT INTO lp_memo VALUES (5,10.0)");
                assertResult(retained, "value\n11.0\n4.0\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testOrderAndRepeatedColumnReferences() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createRows();
            assertRowsAndMemoizers("SELECT d+1.0 AS value FROM lp_memo WHERE d>0.0 ORDER BY value DESC LIMIT 2", "value\n4.0\n3.0\n", 1);
            assertRowsAndMemoizers("SELECT v,v AS second FROM (SELECT d+1.0 AS v FROM lp_memo)",
                    "v\tsecond\n4.0\t4.0\n2.0\t2.0\n3.0\t3.0\nnull\tnull\n", 1);
            assertRowsAndMemoizers("SELECT v+v AS value FROM (SELECT d+1.0 AS v FROM lp_memo)", "value\n8.0\n4.0\n6.0\nnull\n", 1);
            assertRowsAndMemoizers("SELECT v,v AS second FROM (SELECT d+1.0 AS v FROM lp_memo WHERE d>0.0 ORDER BY v LIMIT 2)",
                    "v\tsecond\n2.0\t2.0\n3.0\t3.0\n", 1);
            assertRowsAndMemoizers("SELECT v FROM (SELECT d+1.0 AS v FROM lp_memo) WHERE v>0.0", "v\n4.0\n2.0\n3.0\n", 1);
            assertRowsAndMemoizers("SELECT d::VARCHAR AS value FROM lp_memo WHERE d>0.0 ORDER BY value", "value\n1.0\n2.0\n3.0\n", 1);
        });
    }

    @Test
    public void testSharedPolicyHonoursFunctionRequestAndTransfersOwnership() throws Exception {
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            final ObjList<TrackingDouble> instances = new ObjList<>();
            final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
            descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "lp_memo_requested()";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            CairoConfiguration configuration, SqlExecutionContext executionContext) {
                    final TrackingDouble function = new TrackingDouble();
                    instances.add(function);
                    return function;
                }
            }));
            engine.getFunctionFactoryCache().getFactories().put("lp_memo_requested", descriptors);
            try {
                final String sql = "SELECT lp_memo_requested() AS value FROM long_sequence(2)";
                try (RecordCursorFactory factory = select(sql)) {
                    assertMemoizerCount(factory, 1);
                    final TrackingDouble requested = instances.getLast();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        final Record record = cursor.getRecord();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1.0, record.getDouble(0), 0.0);
                        Assert.assertEquals(1.0, record.getDouble(0), 0.0);
                        Assert.assertEquals(1, requested.readCount);
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(2.0, record.getDouble(0), 0.0);
                    }
                    Assert.assertEquals(0, requested.closeCount);
                }
                for (int i = 0, n = instances.size(); i < n; i++) {
                    Assert.assertEquals(1, instances.getQuick(i).closeCount);
                }
                SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
                try (RecordCursorFactory factory = select(sql)) {
                    assertMemoizerCount(factory, 0);
                } finally {
                    SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = true;
                }
            } finally {
                engine.getFunctionFactoryCache().getFactories().remove("lp_memo_requested");
            }
        });
    }

    private void assertRowsAndMemoizers(String sql, String expected, int memoizerCount) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertMemoizerCount(factory, memoizerCount);
            assertResult(factory, expected);
        }
    }

    private void assertMemoizerCount(RecordCursorFactory factory, int expected) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final String plan = sink.getSink().toString();
        int count = 0;
        for (int offset = 0; (offset = plan.indexOf("memoize(", offset)) >= 0; offset += 8) {
            count++;
        }
        Assert.assertEquals(plan, expected, count);
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        execute("CREATE TABLE lp_memo(id INT,d DOUBLE)");
        execute("INSERT INTO lp_memo VALUES (1,3.0),(2,1.0),(3,2.0),(4,null)");
    }

    private static final class TrackingDouble extends DoubleFunction {
        private int closeCount;
        private int readCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public double getDouble(Record rec) {
            return ++readCount;
        }

        @Override
        public boolean shouldMemoize() {
            return true;
        }
    }
}
