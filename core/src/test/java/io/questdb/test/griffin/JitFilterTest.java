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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.jit.JitUtil;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JitFilterTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = AsyncFilterConstructionFault.CONFIGURATION_FACTORY;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAggregateStealsJitFilterAndParameters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 0);
            assertModes("SELECT l,sum(i) FROM lp_filter WHERE i>$1 GROUP BY l ORDER BY l", true);
            assertModes("SELECT sum(i),count() FROM lp_filter WHERE b", true);
            final int oldMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT l,sum(i) FROM lp_filter WHERE b GROUP BY l", sqlExecutionContext).getRecordCursorFactory()) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    Assert.assertTrue(plan.getSink().toString(), plan.getSink().toString().contains(JitUtil.isJitSupported() ? "Async JIT Group By" : "Async Group By"));
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testAsyncConstructionFailureReleasesAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int oldMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                AsyncFilterConstructionFault.arm(CairoException.nonCritical().put("logical async constructor failure"));
                try {
                    final CairoException failure = Assert.assertThrows(CairoException.class,
                            () -> compiler.compile("SELECT id FROM lp_filter WHERE i>0", sqlExecutionContext));
                    Assert.assertTrue(failure.getFlyweightMessage().toString().contains("logical async constructor failure"));
                } finally {
                    AsyncFilterConstructionFault.disarm();
                }
                try (RecordCursorFactory recovered = compiler.compile("SELECT id FROM lp_filter WHERE b", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(recovered, "id\n3\n4\n5\n");
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testBooleanNumericOverflowAndNullJitParity() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> predicates = new ObjList<>();
            predicates.add("b");
            predicates.add("not b");
            predicates.add("i>0");
            predicates.add("i+1<0");
            predicates.add("i*2>l");
            predicates.add("i=f");
            predicates.add("i>f");
            predicates.add("f+1.0>f");
            predicates.add("d>=9007199254740992.0");
            predicates.add("i=null");
            predicates.add("f=null");
            predicates.add("label=null");
            predicates.add("b and i>0");
            predicates.add("b or i<0");
            for (int i = 0, n = predicates.size(); i < n; i++) {
                assertModes("SELECT id FROM lp_filter WHERE " + predicates.getQuick(i), true);
            }
        });
    }

    @Test
    public void testIntervalInJitParityForDateAndTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k INT, dt DATE, tm TIMESTAMP, tn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-02T12:00:00.000Z', '2024-01-02T12:00:00.000000Z', '2024-01-02T12:00:00.000000000Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000Z', '2024-01-02T00:00:00.000000Z', '2024-01-02T00:00:00.000000000Z', '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000Z', '2024-01-03T00:00:00.000000Z', '2024-01-03T00:00:00.000000000Z', '2024-01-01T02:00:00.000000Z')
                    """);
            final int oldMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode = SqlJitMode.JIT_MODE_DISABLED; mode >= SqlJitMode.JIT_MODE_ENABLED; mode--) {
                    sqlExecutionContext.setJitMode(mode);
                    assertQuery("SELECT k FROM x WHERE dt IN ('2024-01-02')").noLeakCheck().returns("k\n1\n2\n");
                    assertQuery("SELECT k FROM x WHERE dt IN '2024-01-02'").noLeakCheck().returns("k\n1\n2\n");
                    assertQuery("SELECT k FROM x WHERE dt IN ('2024-01')").noLeakCheck().returns("k\n1\n2\n3\n");
                    assertQuery("SELECT k FROM x WHERE dt NOT IN ('2024-01-02')").noLeakCheck().returns("k\n3\n");
                    assertQuery("SELECT k FROM x WHERE dt IN ('2024-01-02', '2024-01-03')").noLeakCheck().returns("k\n2\n3\n");
                    assertQuery("SELECT k FROM x WHERE dt NOT IN ('2024-01-02', '2024-01-03')").noLeakCheck().returns("k\n1\n");
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
            final String[] keys = {"dt", "tm", "tn"};
            for (String key : keys) {
                final boolean isOneValueJit = !"dt".equals(key);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01-02')", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " IN '2024-01-02'", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01')", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " NOT IN ('2024-01-02')", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01-02') AND k > 0", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01-02') OR k < 0", isOneValueJit);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01-02', '2024-01-03')", true);
                assertModes("SELECT k FROM x WHERE " + key + " IN ('2024-01-02', '2024-01-03', '2024-01-05')", true);
                assertModes("SELECT k FROM x WHERE " + key + " NOT IN ('2024-01-02', '2024-01-03')", true);
                assertModes("SELECT k FROM x WHERE " + key + " NOT IN ('2024-01-02', '2024-01-03', '2024-01-05')", true);
            }
        });
    }

    @Test
    public void testJitFactorySurvivesCompilerResetAndRebindsParameters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int oldMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            RecordCursorFactory retained = null;
            try {
                bindVariableService.setInt(0, 0);
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id FROM lp_filter WHERE i>$1", sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT label FROM lp_filter WHERE b", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                }
                Assert.assertEquals(JitUtil.isJitSupported(), retained.usesCompiledFilter());
                assertResult(retained, "id\n3\n4\n5\n");
                bindVariableService.setInt(0, 16777217);
                assertResult(retained, "id\n4\n");
            } finally {
                Misc.free(retained);
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testRuntimeConstantGateAndJitFallbacks() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertModes("SELECT id FROM lp_filter WHERE nullif(i,0)>0", false);
            assertModes("SELECT id FROM lp_filter WHERE label='a'", false);
            assertModes("SELECT id FROM lp_filter WHERE ts<'2020-01-01T00:00:00.000000001Z'", true);
            assertModes("SELECT id FROM lp_filter WHERE ts<'2020-01-01T00:00:00.000001Z'", true);
            bindVariableService.setBoolean(0, true);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT id FROM lp_filter WHERE $1", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "id\n1\n2\n3\n4\n5\n6\n");
                    bindVariableService.setBoolean(0, false);
                    assertResult(factory, "id\n");
                }
            }
        });
    }

    private void assertModes(String sql, boolean isJitEligible) throws Exception {
        final int oldMode = sqlExecutionContext.getJitMode();
        final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
        sqlExecutionContext.setParallelFilterEnabled(true);
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            final String expected;
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final StringSink sink = new StringSink();
                CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
                expected = sink.toString();
            }
            for (int mode = SqlJitMode.JIT_MODE_DISABLED; mode >= SqlJitMode.JIT_MODE_ENABLED; mode--) {
                sqlExecutionContext.setJitMode(mode);
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(sql, mode != SqlJitMode.JIT_MODE_DISABLED && isJitEligible && JitUtil.isJitSupported(), factory.usesCompiledFilter());
                    assertResult(factory, expected);
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(oldMode);
            sqlExecutionContext.setParallelFilterEnabled(wasParallel);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_filter(id INT,i INT,l LONG,f FLOAT,d DOUBLE,b BOOLEAN,label STRING,ts TIMESTAMP)");
        execute("""
                INSERT INTO lp_filter VALUES
                (1,null,null,null,null,false,null,null),
                (2,0,0,0,0,false,'b','2020-01-01T00:00:00Z'),
                (3,1,1,1,1,true,'a','2020-01-01T00:00:00.000001Z'),
                (4,2147483647,2147483647,16777216,16777217,true,'a','2020-01-01T00:00:00.000002Z'),
                (5,16777217,9007199254740993,16777216,9007199254740992,true,'b','2020-01-01T00:00:00.000003Z'),
                (6,-2,-2,-2,-2,false,'b','2020-01-01T00:00:00.000004Z')
                """);
    }
}
