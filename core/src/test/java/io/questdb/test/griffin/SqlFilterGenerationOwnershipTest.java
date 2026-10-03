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
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlFilterGenerationOwnershipTest extends AbstractCairoTest {
    private static final String QUERY = "SELECT * FROM f_owner WHERE id > 0 LIMIT owned_long($1)";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = AsyncFilterConstructionFault.CONFIGURATION_FACTORY;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAsyncConstructorFailureClosesInputsOnce() throws Exception {
        assertFilter(true, (fixture, compiler) -> {
            final RuntimeException primary = new RuntimeException("filter construction");
            final RuntimeException cleanup = new RuntimeException("limit cleanup");
            fixture.failLongClose(cleanup);
            AsyncFilterConstructionFault.arm(primary);
            try {
                final RuntimeException actual = Assert.assertThrows(RuntimeException.class,
                        () -> compiler.compile(QUERY, sqlExecutionContext));
                Assert.assertSame(primary, actual);
                Assert.assertTrue(OwnershipFixture.hasSuppressed(actual, cleanup));
                fixture.assertAllClosedOnce();
            } finally {
                AsyncFilterConstructionFault.disarm();
            }
        });
    }

    @Test
    public void testSerialAndAsyncFactoriesOwnInputsAfterGeneratorCloses() throws Exception {
        for (int mode = 0; mode < 2; mode++) {
            final boolean isParallel = mode == 0;
            assertFilter(isParallel, (fixture, compiler) -> {
                try (RecordCursorFactory factory = compiler.compile(QUERY, sqlExecutionContext).getRecordCursorFactory()) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan.getSink(), isParallel ? "Async Filter" : "Filter filter");
                    Assert.assertEquals(1, fixture.longCount());
                    if (isParallel) {
                        fixture.assertNoneClosed();
                    }
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                            .returns("id\n1\n2\n");
                }
                fixture.assertAllClosedOnce();
            });
        }
    }

    @Test
    public void testSerialUnusedLimitCloseFailureStillClosesInputs() throws Exception {
        assertFilter(false, (fixture, compiler) -> {
            final RuntimeException primary = new RuntimeException("unused limit cleanup");
            fixture.failLongClose(primary);
            final RecordCursorFactory factory = compiler.compile(QUERY, sqlExecutionContext).getRecordCursorFactory();
            final RuntimeException actual = Assert.assertThrows(RuntimeException.class, factory::close);
            Assert.assertSame(primary, actual);
            fixture.assertAllClosedOnce();
        });
    }

    private static void assertFilter(boolean isParallel, FilterAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f_owner(id INT)");
            execute("INSERT INTO f_owner VALUES (1),(2),(3)");
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            sqlExecutionContext.setParallelFilterEnabled(isParallel);
            bindVariableService.clear();
            bindVariableService.setLong(0, 2);
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertion.run(fixture, compiler);
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
            }
            execute("DROP TABLE f_owner");
        });
    }

    @FunctionalInterface
    private interface FilterAssertion {
        void run(OwnershipFixture fixture, SqlCompilerImpl compiler) throws Exception;
    }
}
