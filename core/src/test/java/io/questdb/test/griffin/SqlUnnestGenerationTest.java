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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlUnnestGenerationTest extends AbstractCairoTest {
    @Test
    public void testBoundColumnsZipArrayAndJsonAndRetainFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_unnest(ts TIMESTAMP,arr DOUBLE[],payload VARCHAR) TIMESTAMP(ts)");
            execute("INSERT INTO lp_unnest VALUES('2024-01-01T00:00:01',ARRAY[1.0,2.0],'[{\"v\":10}]'),"
                    + "('2024-01-01T00:00:02',null,'[{\"v\":20},{\"v\":30}]'),('2024-01-01T00:00:03',ARRAY[4.0],null)");
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(
                        "SELECT u.x,u.v,u.n FROM lp_unnest t,UNNEST(t.arr,t.payload COLUMNS(v INT)) WITH ORDINALITY u(x,v,n)",
                        sqlExecutionContext
                ).getRecordCursorFactory();
            }
            try (retained) {
                assertFactory(retained).withContext(sqlExecutionContext).noRandomAccess().sizeMayVary()
                        .returns("x\tv\tn\n1.0\t10\t1\n2.0\tnull\t2\nnull\t20\t1\nnull\t30\t2\n4.0\tnull\t1\n");
            }
        });
    }

    @Test
    public void testJsonSourceFailureClosesAllOwnersAndPreservesFailure() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_JSON_UNNEST_MAX_VALUE_SIZE, 256);
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("p1", ColumnType.VARCHAR, "p2", ColumnType.VARCHAR).closeFailure = new RuntimeException("master close");
                fixture.assertCompileOomSweep(compiler, sqlExecutionContext,
                        "SELECT * FROM owned_table(0) t,UNNEST(t.p1 COLUMNS(v INT),t.p2 COLUMNS(w INT)) u");
            }
        });
    }

    @Test
    public void testNeverOpenedFactoryClosesArgumentsOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine)) {
                fixture.table("p", ColumnType.VARCHAR);
                final RecordCursorFactory retained;
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT * FROM owned_table(0) t,UNNEST(t.p COLUMNS(v INT)) u", sqlExecutionContext)
                            .getRecordCursorFactory();
                }
                try (retained) {
                    Assert.assertEquals(1, fixture.tableCount());
                    fixture.assertNoneClosed();
                }
                fixture.assertAllClosedOnce();
            }
        });
    }

    @Test
    public void testSpecAndDependentOccurrenceClearTheirOwnState() {
        final UnnestSpec spec = new UnnestSpec().of(true, true);
        spec.getOutput().add(72, "value", ColumnType.DOUBLE, true);
        spec.getExpressions().add(new ColumnExpression().of(12, ColumnType.VARCHAR, 31));
        final JoinInput step = new JoinInput().ofUnnest(spec, "u", 15);
        Assert.assertNull(step.getInput());
        Assert.assertSame(spec.getOutput(), step.getSourceOutput());
        step.clear();
        Assert.assertNull(step.getUnnest());
        Assert.assertEquals(1, spec.getExpressions().size());
        spec.clear();
        Assert.assertEquals(0, spec.getExpressions().size());
        Assert.assertEquals(0, spec.getOutput().getColumnCount());
        Assert.assertFalse(spec.isStandalone());
        Assert.assertFalse(spec.hasOrdinality());
    }

    @Test
    public void testStandaloneJsonNamesSurvivePlanReuse() throws Exception {
        assertMemoryLeak(() -> {
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(
                        "SELECT * FROM UNNEST('[{\"v\":7},null,{\"v\":9}]'::VARCHAR COLUMNS(v LONG)) WITH ORDINALITY u(value,n)",
                        sqlExecutionContext
                ).getRecordCursorFactory();
                try {
                    compiler.compile(
                            "SELECT * FROM UNNEST('[{\"other_field\":1}]'::VARCHAR COLUMNS(other_field LONG)) WITH ORDINALITY u(other_alias,m)",
                            sqlExecutionContext
                    ).getRecordCursorFactory().close();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (retained) {
                final TextPlanSink sink = new TextPlanSink();
                sink.of(retained, sqlExecutionContext);
                TestUtils.assertContains(sink.getSink(), "columns: [value,n]");
                assertFactory(retained).withContext(sqlExecutionContext).noRandomAccess().sizeMayVary()
                        .returns("value\tn\n7\t1\nnull\t2\n9\t3\n");
            }
        });
    }

    @Test
    public void testWrongArgumentTypeClosesConsumedOwners() throws Exception {
        assertMemoryLeak(() -> {
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                fixture.table("p", ColumnType.VARCHAR);
                fixture.assertCompileFails(compiler, sqlExecutionContext,
                        "SELECT * FROM owned_table(0) t,UNNEST(t.p) u", "array type expected in UNNEST, got VARCHAR");
            }
        });
    }
}
