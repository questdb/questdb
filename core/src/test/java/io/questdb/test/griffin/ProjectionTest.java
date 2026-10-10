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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ProjectionTest extends AbstractCairoTest {
    @Test
    public void testNativeColumnPermutationKeepsSymbolsAndHiddenFilterColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            assertPermutation(
                    "SELECT ts,sym,id FROM lp_projection",
                    """
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_projection
                            """,
                    """
                            ts	sym	id
                            2020-01-01T00:00:00.000000Z	C	3
                            2020-01-01T00:00:01.000000Z	A	1
                            2020-01-01T00:00:02.000000Z		null
                            2020-01-01T00:00:03.000000Z	B	2
                            """
            );
            assertPermutation(
                    "SELECT ts,sym,id FROM lp_projection WHERE id>1",
                    """
                            Async JIT Filter workers: 1
                              filter: 1<id
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_projection
                            """,
                    """
                            ts	sym	id
                            2020-01-01T00:00:00.000000Z	C	3
                            2020-01-01T00:00:03.000000Z	B	2
                            """
            );
            assertPermutation(
                    "SELECT ts,sym,id FROM lp_projection WHERE id>1 ORDER BY ts DESC",
                    """
                            Async JIT Filter workers: 1
                              filter: 1<id
                                PageFrame
                                    Row backward scan
                                    Frame backward scan on: lp_projection
                            """,
                    """
                            ts	sym	id
                            2020-01-01T00:00:03.000000Z	B	2
                            2020-01-01T00:00:00.000000Z	C	3
                            """
            );
            assertPermutation(
                    "SELECT ts,sym FROM lp_projection WHERE id>1",
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  filter: 1<id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_projection
                            """,
                    """
                            ts	sym
                            2020-01-01T00:00:00.000000Z	C
                            2020-01-01T00:00:03.000000Z	B
                            """
            );
            assertPermutation(
                    "SELECT ts,sym AS label,sym AS duplicate FROM lp_projection WHERE id>1",
                    """
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  filter: 1<id
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_projection
                            """,
                    """
                            ts	label	duplicate
                            2020-01-01T00:00:00.000000Z	C	C
                            2020-01-01T00:00:03.000000Z	B	B
                            """
            );
        });
    }

    @Test
    public void testComputedCtasGeneratesSelectOnce() throws Exception {
        assertMemoryLeak(() -> {
            final int[] generatedQueries = {0};
            try (
                    CairoEngine statementEngine = newGenerationCountingEngine(generatedQueries);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                createRows(statementEngine, executionContext);
                statementEngine.execute("""
                        CREATE TABLE lp_copy AS
                        (SELECT id+1 AS adjusted,label FROM lp_projection WHERE id>0)
                        """, executionContext);
                Assert.assertEquals(1, generatedQueries[0]);
                try (
                        SqlCompiler compiler = statementEngine.getSqlCompiler();
                        RecordCursorFactory factory = compiler.compile(
                                "SELECT adjusted,label FROM lp_copy ORDER BY adjusted", executionContext
                        ).getRecordCursorFactory()
                ) {
                    assertFactory(factory).withContext(executionContext).inferRandomAccess().inferTimestamp()
                            .sizeMayVary().returns("adjusted\tlabel\n2\ta\n3\tb\n4\tc\n");
                }
                Assert.assertEquals(2, generatedQueries[0]);
            }
        });
    }

    @Test
    public void testComputedFactorySurvivesCompilerResetAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(
                            "SELECT id+1 AS adjusted,sym,note FROM lp_projection ORDER BY ts", sqlExecutionContext
                    ).getRecordCursorFactory();
                    assertRowsOnly(retained, "adjusted\tsym\tnote\n4\tC\tcafé\n2\tA\talpha\nnull\t\t\n3\tB\tβeta\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "SELECT id+2 AS later FROM lp_projection WHERE id>1", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertRowsOnly(other, "later\n5\n4\n");
                    }
                    compiler.clear();
                    assertRowsOnly(retained, "adjusted\tsym\tnote\n4\tC\tcafé\n2\tA\talpha\nnull\t\t\n3\tB\tβeta\n");
                }
                assertRowsOnly(retained, "adjusted\tsym\tnote\n4\tC\tcafé\n2\tA\talpha\nnull\t\t\n3\tB\tβeta\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testComputedInsertAndNativeUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            execute("CREATE TABLE lp_target (adjusted INT,label STRING)");
            execute("ALTER TABLE lp_projection ADD COLUMN copied INT");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_target SELECT id+1,label FROM lp_projection WHERE id>0 ORDER BY ts");
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT adjusted,label FROM lp_target ORDER BY adjusted", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "adjusted\tlabel\n2\ta\n3\tb\n4\tc\n");
                }
                execute(compiler, "UPDATE lp_projection SET copied=id+1 WHERE id>1");
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id,copied FROM lp_projection ORDER BY ts", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\tcopied\n3\t4\n1\tnull\nnull\tnull\n2\t3\n");
                }
            }
        });
    }

    @Test
    public void testConstantsNestedCallsAndDiscardedInputs() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            assertProjection("SELECT 7 AS fixed,TRUE AS enabled,id+1 AS adjusted FROM lp_projection ORDER BY ts", """
                    fixed	enabled	adjusted
                    7	true	4
                    7	true	2
                    7	true	null
                    7	true	3
                    """);
            assertProjection("SELECT (id+1)+2 AS adjusted FROM lp_projection", "adjusted\n6\n4\nnull\n5\n");
            // The folded expression no longer needs id: a zero-column scan must still preserve
            // the source row count, and discarded leaves must not be relocated or closed twice.
            assertProjection("SELECT id+null AS discarded FROM lp_projection", "discarded\nnull\nnull\nnull\nnull\n");
        });
    }

    @Test
    public void testMixedProjectionPreservesTypesAndTimestampAfterPruning() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            assertProjection("SELECT id+1 AS adjusted,sym,label,note,ts FROM lp_projection ORDER BY ts", """
                    adjusted	sym	label	note	ts
                    4	C	c	café	2020-01-01T00:00:00.000000Z
                    2	A	a	alpha	2020-01-01T00:00:01.000000Z
                    null				2020-01-01T00:00:02.000000Z
                    3	B	b	βeta	2020-01-01T00:00:03.000000Z
                    """);
        });
    }

    @Test
    public void testOrderByNumericAliasesAndOrdinalPrecedence() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            assertProjection("SELECT id+1 AS \"5\" FROM lp_projection ORDER BY 5", "5\nnull\n2\n3\n4\n");
            assertProjection("SELECT id+1 AS \"0\" FROM lp_projection ORDER BY 0", "0\nnull\n2\n3\n4\n");
            assertProjection("SELECT id+1 AS v,ts AS \"1\" FROM lp_projection ORDER BY 1", """
                    v	1
                    null	2020-01-01T00:00:02.000000Z
                    2	2020-01-01T00:00:01.000000Z
                    3	2020-01-01T00:00:03.000000Z
                    4	2020-01-01T00:00:00.000000Z
                    """);
            assertProjection("SELECT id+1 AS v,ts AS \"5\" FROM lp_projection ORDER BY 5", """
                    v	5
                    4	2020-01-01T00:00:00.000000Z
                    2	2020-01-01T00:00:01.000000Z
                    null	2020-01-01T00:00:02.000000Z
                    3	2020-01-01T00:00:03.000000Z
                    """);
        });
    }

    @Test
    public void testOrderByOrdinalDiagnostics() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            assertOrdinalFailure("SELECT id FROM lp_projection ORDER BY 0", "order column position is out of range [max=1]");
            assertOrdinalFailure("SELECT id FROM lp_projection ORDER BY 2", "order column position is out of range [max=1]");
            assertOrdinalFailure("SELECT id+1 AS v FROM lp_projection ORDER BY 2", "order column position is out of range [max=1]");
        });
    }

    @Test
    public void testOrderingUsesComputedOutputsAndHiddenKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows(engine, sqlExecutionContext);
            execute("INSERT INTO lp_projection VALUES (50,1,'A2','a2','again','2020-01-01T00:00:04.000000Z')");
            assertProjection("SELECT id+1 AS v FROM lp_projection ORDER BY v", "v\nnull\n2\n2\n3\n4\n");
            assertProjection("SELECT id+1 AS v FROM lp_projection ORDER BY 1", "v\nnull\n2\n2\n3\n4\n");
            assertProjection("SELECT id+1 AS v,label FROM lp_projection ORDER BY v,ts DESC", "v\tlabel\nnull\t\n2\ta2\n2\ta\n3\tb\n4\tc\n");
            assertProjection("SELECT id+1 AS v FROM lp_projection ORDER BY id+2 DESC", "v\n4\n3\n2\n2\nnull\n");
        });
    }

    private static void createRows(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        engine.execute("""
                CREATE TABLE lp_projection (unused INT,id INT,sym SYMBOL,label STRING,note VARCHAR,ts TIMESTAMP)
                TIMESTAMP(ts)
                """, executionContext);
        engine.execute("""
                INSERT INTO lp_projection VALUES
                    (10,3,'C','c','café','2020-01-01T00:00:00.000000Z'),
                    (20,1,'A','a','alpha','2020-01-01T00:00:01.000000Z'),
                    (30,null,null,null,null,'2020-01-01T00:00:02.000000Z'),
                    (40,2,'B','b','βeta','2020-01-01T00:00:03.000000Z')
                """, executionContext);
    }

    private void assertPermutation(String sql, String expectedPlan, String expectedRows) throws Exception {
        assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().withPlan(expectedPlan).returns(expectedRows);
    }

    private void assertProjection(String sql, String expected) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, sqlExecutionContext);
            TestUtils.assertContains(plan.getSink(), "VirtualRecord");
            LogicalPlan source = compiler.getPlanForTesting();
            while (source.inputCount() > 0) {
                source = source.inputAt(0);
            }
            Assert.assertEquals(-1, source.getOutput().getColumnIndexQuiet("unused"));
            assertRowsOnly(factory, expected);
        }
    }

    private void assertOrdinalFailure(String sql, String message) throws Exception {
        assertQuery(sql).noLeakCheck().fails(sql.lastIndexOf(' ') + 1, message);
    }

    private CairoEngine newGenerationCountingEngine(int[] generatedQueries) throws IOException {
        return new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath())) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return engine -> {
                    final SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                        @Override
                        protected RecordCursorFactory generateSelectOneShot(
                                QueryModel model,
                                SqlExecutionContext executionContext,
                                boolean generateProgressLogger
                        ) throws SqlException {
                            Assert.assertNotNull(getPlanForTesting());
                            generatedQueries[0]++;
                            return super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                        }
                    };
                    return compiler;
                };
            }
        };
    }
}
