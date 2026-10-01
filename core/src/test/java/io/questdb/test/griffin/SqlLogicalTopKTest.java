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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.jit.JitUtil;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalTopKTest extends AbstractCairoTest {
    @Test
    public void testComputedSortKeyKeepsOrdinarySort() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            try {
                assertTopK(
                        "SELECT id+rank AS key,label FROM lp_top_filter WHERE keep ORDER BY key DESC,label LIMIT 3",
                        "Encode sort light",
                        """
                        key	label
                        8	d
                        6	c
                        4	a
                        """
                );
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testFilteredProjectionAndHiddenKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            try {
                for (int mode = SqlJitMode.JIT_MODE_DISABLED; mode >= SqlJitMode.JIT_MODE_ENABLED; mode--) {
                    sqlExecutionContext.setJitMode(mode);
                    final String algorithm = mode != SqlJitMode.JIT_MODE_DISABLED && JitUtil.isJitSupported() ? "Async JIT Top K" : "Async Top K";
                    assertTopK(
                            "SELECT label AS name,id AS key FROM lp_top_filter WHERE keep ORDER BY key DESC,name LIMIT 3",
                            algorithm,
                            """
                            name	key
                            d	4
                            c	3
                            a	2
                            """
                    );
                    assertTopK(
                            "SELECT id,rank+1 AS value,label FROM lp_top_filter WHERE keep ORDER BY id DESC,label LIMIT 3",
                            algorithm,
                            """
                            id	value	label
                            4	5	d
                            3	4	c
                            2	3	a
                            """
                    );
                    assertTopK(
                            "SELECT id,rank+1 AS value FROM lp_top_filter WHERE rank>0 ORDER BY id DESC,label LIMIT 3",
                            algorithm,
                            """
                            id	value
                            4	5
                            3	4
                            2	3
                            """
                    );
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testJitParametersSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setParallelTopKEnabled(true);
            RecordCursorFactory retained = null;
            try {
                bindVariableService.setInt(0, 0);
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id,rank+1 AS value FROM lp_top_filter WHERE rank>$1 ORDER BY id DESC,label LIMIT 3", sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT label FROM lp_top_filter WHERE keep", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                }
                assertPlan(retained, JitUtil.isJitSupported() ? "Async JIT Top K" : "Async Top K");
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tvalue\n4\t5\n3\t4\n2\t3\n");
                bindVariableService.setInt(0, 3);
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tvalue\n4\t5\n");
            } finally {
                Misc.free(retained);
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testLimitFormsKeepTheirSortBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            final String[][] cases = {
                    {"0", "id\tlabel\n"},
                    {"-2", "id\tlabel\n1\ta\nnull\t\n"},
                    {"1,-1", "id\tlabel\n3\tc\n2\ta\n2\tb\n1\ta\n"},
                    {"$1", "id\tlabel\n4\td\n3\tc\n"}
            };
            bindVariableService.setLong(0, 2);
            try {
                for (String[] c : cases) {
                    assertQuery("SELECT id,label FROM lp_top_filter ORDER BY id DESC,label LIMIT " + c[0])
                            .noLeakCheck()
                            .withPlanNotContaining("Top K")
                            .inferRandomAccess()
                            .sizeMayVary()
                            .returns(c[1]);
                }
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testNestedSelectedPageFrameLayout() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_top_nested(a INT,b INT)");
            execute("INSERT INTO lp_top_nested VALUES (3,20),(1,40),(2,30),(4,10)");
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            final String plan = """
                    SelectedRecord
                        Async Top K lo: 2 workers: 1
                          filter: null
                          keys: [b, a]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_top_nested
                    """;
            try {
                assertQuery("SELECT x,y FROM (SELECT b AS x,aa AS y FROM (SELECT a AS aa,b FROM lp_top_nested)) ORDER BY x,y LIMIT 2")
                        .noLeakCheck()
                        .withPlan(plan)
                        .expectSize()
                        .returns("x\ty\n10\t4\n20\t3\n");
                assertQuery("SELECT y,x FROM (SELECT bb AS x,aa AS y FROM (SELECT a AS aa,b AS bb FROM lp_top_nested)) ORDER BY x,y LIMIT 2")
                        .noLeakCheck()
                        .withPlan("""
                                SelectedRecord
                                    SelectedRecord
                                        Async Top K lo: 2 workers: 1
                                          filter: null
                                          keys: [b, a]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_top_nested
                                """)
                        .expectSize()
                        .returns("y\tx\n4\t10\n3\t20\n");
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testRuntimeConstantGateStaysOnPageFrameSource() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final boolean wasParallel = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            try {
                bindVariableService.setBoolean(0, true);
                try (RecordCursorFactory factory = select("SELECT id,rank+1 AS value FROM lp_top_filter WHERE $1 ORDER BY id DESC,label LIMIT 3")) {
                    assertPlan(factory, "Async Top K");
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tvalue\n4\t5\n3\t4\n2\t3\n");
                    bindVariableService.setBoolean(0, false);
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tvalue\n");
                    bindVariableService.setBoolean(0, true);
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tvalue\n4\t5\n3\t4\n2\t3\n");
                }
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testThreadLocalFiltersRetainIndependentWorkers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4)) {
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                context.setParallelTopKEnabled(true);
                final RecordCursorFactory retained;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT id,upper(label) AS name FROM lp_top_filter WHERE CASE WHEN id>1 THEN lower(label) IN ('a','c') ELSE false END ORDER BY id DESC,label LIMIT 2", context).getRecordCursorFactory();
                }
                try (RecordCursorFactory factory = retained) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, context);
                    TestUtils.assertContains(plan.getSink(), "Async Top K");
                    assertFactory(factory).withContext(context).inferRandomAccess().inferTimestamp().sizeMayVary().returns("id\tname\n3\tC\n2\tA\n");
                }
            }
        });
    }

    private void assertTopK(String sql, String algorithm, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .withPlanContaining(algorithm)
                .inferRandomAccess()
                .sizeMayVary()
                .returns(expected);
    }

    private void assertPlan(RecordCursorFactory factory, String algorithm) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), algorithm);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_top_filter(id INT,rank INT,label STRING,keep BOOLEAN)");
        execute("INSERT INTO lp_top_filter VALUES (3,3,'c',true),(1,1,'a',true),(null,null,null,false),(2,2,'b',false),(2,2,'a',true),(4,4,'d',true)");
    }
}
