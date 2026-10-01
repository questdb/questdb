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

package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.test.AbstractCairoTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class SqlCodeGeneratorCleanupTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // enables the test_fault() instrumentation
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testLogicalCaptureFailureClosesPreparedFunctionsOnce() throws Exception {
        assertLogicalGenerationFailure(0);
    }

    @Test
    public void testLogicalRegeneratedBranchFailureClosesAdoptedFunctionsOnce() throws Exception {
        assertLogicalGenerationFailure(1);
    }

    @Test
    public void testLogicalUnionTailFailureClosesGroupedHeadFunctionsOnce() throws Exception {
        assertLogicalGenerationFailure(2);
    }

    private void assertLogicalGenerationFailure(int failureMode) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u AS (SELECT x::TIMESTAMP ts FROM long_sequence(50)) TIMESTAMP(ts)");
            String source = failureMode == 2
                    ? "SELECT x::TIMESTAMP ts, max(x) x FROM long_sequence(200) WHERE x IN (1, 101) AND " + TestFaultFunctionFactory.CALL + " GROUP BY ts "
                    + "UNION ALL SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x = 200 AND " + TestFaultFunctionFactory.CALL
                    : "SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x IN (101, 200) AND " + TestFaultFunctionFactory.CALL + " LIMIT 1";
            String query = "SELECT o.ts, o.x, l.c FROM (" + source + ") o "
                    + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts AND " + TestFaultFunctionFactory.CALL + ") l ON true ORDER BY o.x";
            // Mode 0 fails before any source is generated; modes 1 and 2 fail on the second table-function
            // source, after the first one and its filter were adopted by generated factories.
            final int failingSource = failureMode == 0 ? 0 : 2;
            final OutOfMemoryError failure = new OutOfMemoryError("injected logical generation");
            final int[] sourceCount = {0};
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.setLogicalGenerationTestHook(plan -> {
                    if (failingSource == 0 && sourceCount[0]++ == 0
                            || plan.getType() == LogicalPlan.Type.FUNCTION_SOURCE && ++sourceCount[0] == failingSource) {
                        throw failure;
                    }
                });
                TestFaultFunctionFactory.armCloseFailures();
                try {
                    try (RecordCursorFactory ignored = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("generation must fail");
                    } catch (OutOfMemoryError e) {
                        Assert.assertSame(failure, e);
                    }
                    Assert.assertTrue("must prepare counted functions", TestFaultFunctionFactory.created() > 0);
                    Assert.assertEquals("each prepared function closes exactly once",
                            TestFaultFunctionFactory.created(), TestFaultFunctionFactory.closeCalls());
                    for (int i = 0, n = TestFaultFunctionFactory.closeFailureCount(); i < n; i++) {
                        Assert.assertTrue("close failures must not replace the primary error",
                                Arrays.asList(failure.getSuppressed()).contains(TestFaultFunctionFactory.closeFailure(i)));
                    }
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                compiler.setLogicalGenerationTestHook(null);
                assertQuery("SELECT x FROM long_sequence(2) WHERE x = 2").withCompiler(compiler).returns("x\n2\n");
            }
        });
    }}
