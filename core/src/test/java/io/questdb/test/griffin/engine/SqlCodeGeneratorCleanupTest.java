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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.GenerationStepContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SqlCodeGeneratorCleanupTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // enables the test_fault() instrumentation
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testRegeneratedBranchFailureClosesAdoptedFunctionsOnce() throws Exception {
        assertEveryGenerationFailure("SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x IN (101, 200) AND "
                + TestFaultFunctionFactory.CALL + " LIMIT 1");
    }

    @Test
    public void testUnionTailFailureClosesGroupedHeadFunctionsOnce() throws Exception {
        assertEveryGenerationFailure("SELECT x::TIMESTAMP ts, max(x) x FROM long_sequence(200) WHERE x IN (1, 101) AND "
                + TestFaultFunctionFactory.CALL + " GROUP BY ts "
                + "UNION ALL SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x = 200 AND " + TestFaultFunctionFactory.CALL);
    }

    private static boolean isSuppressedBy(Throwable primary, Throwable failure) {
        for (Throwable suppressed : primary.getSuppressed()) {
            if (suppressed == failure || isSuppressedBy(suppressed, failure)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fails generation at each of its steps in turn, after binding prepared the counted functions,
     * while every created instance fails to close.
     */
    private void assertEveryGenerationFailure(String source) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u AS (SELECT x::TIMESTAMP ts FROM long_sequence(50)) TIMESTAMP(ts)");
            final String query = "SELECT o.ts, o.x, l.c FROM (" + source + ") o "
                    + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts AND " + TestFaultFunctionFactory.CALL + ") l ON true ORDER BY o.x";
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    GenerationStepContext context = new GenerationStepContext(engine)
            ) {
                final int[] steps = {0};
                context.setStep(() -> steps[0]++);
                compiler.compile(query, context).getRecordCursorFactory().close();
                final int stepCount = steps[0];
                Assert.assertTrue("generation must have steps", stepCount > 1);
                for (int i = 0; i < stepCount; i++) {
                    final int failingStep = i;
                    final OutOfMemoryError failure = new OutOfMemoryError("injected logical generation");
                    steps[0] = 0;
                    context.setStep(() -> {
                        if (steps[0]++ == failingStep) {
                            throw failure;
                        }
                    });
                    TestFaultFunctionFactory.armCloseFailures();
                    try {
                        try (RecordCursorFactory ignored = compiler.compile(query, context).getRecordCursorFactory()) {
                            Assert.fail("generation must fail");
                        } catch (OutOfMemoryError e) {
                            Assert.assertSame(failure, e);
                        }
                        Assert.assertTrue("must prepare counted functions", TestFaultFunctionFactory.created() > 0);
                        Assert.assertEquals("each prepared function closes exactly once",
                                TestFaultFunctionFactory.created(), TestFaultFunctionFactory.closeCalls());
                        for (int j = 0, n = TestFaultFunctionFactory.closeFailureCount(); j < n; j++) {
                            Assert.assertTrue("close failures must not replace the primary error",
                                    isSuppressedBy(failure, TestFaultFunctionFactory.closeFailure(j)));
                        }
                    } finally {
                        TestFaultFunctionFactory.disarm();
                    }
                }
                context.setStep(null);
                assertQuery("SELECT x FROM long_sequence(2) WHERE x = 2").withCompiler(compiler).returns("x\n2\n");
            }
        });
    }
}
