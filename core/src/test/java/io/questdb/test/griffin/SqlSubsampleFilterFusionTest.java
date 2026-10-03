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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlSubsampleFilterFusionTest extends AbstractCairoTest {
    @Test
    public void testInternalKeepFlagUsesResolvedColumn() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT id,flag FROM (SELECT id,flag,ts FROM lp_keep SUBSAMPLE uniform(3))", sqlExecutionContext
                 ).getRecordCursorFactory()) {
                final CachedWindowLightRecordCursorFactory window = findWindow(factory);
                final WindowFunction keep = window.getSingleRowSelectingFunction();
                Assert.assertNotNull(keep);
                TestUtils.assertContains(plan(factory), "CachedWindowLightSelect");
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\tflag\n1\ttrue\n3\ttrue\n5\ttrue\n");
            }
        });
    }

    @Test
    public void testProjectedUserKeepBooleanRemainsMaterialized() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int reuse = 0; reuse < 2; reuse++) {
                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT id,keep FROM (SELECT id,uniform(3) OVER(ORDER BY ts) keep FROM lp_keep) WHERE keep",
                            sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        final CachedWindowLightRecordCursorFactory window = findWindow(factory);
                        Assert.assertNull(window.getSingleRowSelectingFunction());
                        final String plan = plan(factory);
                        TestUtils.assertContains(plan, "Filter");
                        TestUtils.assertContains(plan, "CachedWindowLight");
                        Assert.assertFalse(plan.contains("CachedWindowLightSelect"));
                        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("id\tkeep\n1\ttrue\n3\ttrue\n5\ttrue\n");
                    }
                }
            }
        });
    }

    @Test
    public void testWindowSpecCopiesAndClearsOnlyExplicitMarker() {
        final WindowExpression expression = WindowExpression.FACTORY.newInstance();
        final WindowSpec spec = new WindowSpec();
        Assert.assertFalse(spec.isSubsampleKeepFlag());
        spec.of(expression);
        Assert.assertFalse(spec.isSubsampleKeepFlag());
        expression.setSubsampleKeepFlag(true);
        spec.of(expression);
        Assert.assertTrue(spec.isSubsampleKeepFlag());
        spec.clear();
        Assert.assertFalse(spec.isSubsampleKeepFlag());
        spec.of(expression);
        expression.setSubsampleKeepFlag(false);
        spec.of(expression);
        Assert.assertFalse(spec.isSubsampleKeepFlag());
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }

    private static CachedWindowLightRecordCursorFactory findWindow(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof CachedWindowLightRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (CachedWindowLightRecordCursorFactory) factory;
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_keep(id INT,flag BOOLEAN,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_keep VALUES(1,true,'2024-01-01T00:00:01'),"
                + "(2,false,'2024-01-01T00:00:02'),(3,true,'2024-01-01T00:00:03'),"
                + "(4,false,'2024-01-01T00:00:04'),(5,true,'2024-01-01T00:00:05')");
    }
}
