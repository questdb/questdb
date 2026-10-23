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
import io.questdb.jit.JitUtil;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlJitFilterGenerationOwnershipTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = AsyncFilterConstructionFault.CONFIGURATION_FACTORY;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testBoundLimitDeclineAndSuccessfulAdoption() throws Exception {
        assertJitFilter((fixture, compiler) -> {
            try (RecordCursorFactory declined = compiler.compile(
                    "SELECT * FROM jf_owner WHERE abs(id) > $1 LIMIT owned_long($2)", sqlExecutionContext
            ).getRecordCursorFactory()) {
                Assert.assertFalse(declined.usesCompiledFilter());
                assertFactory(declined).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("id\n1\n2\n");
            }
            fixture.assertAllClosedOnce();
            fixture.reset();
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT * FROM jf_owner WHERE id > $1 LIMIT owned_long($2)", sqlExecutionContext
            ).getRecordCursorFactory()) {
                Assert.assertTrue(factory.usesCompiledFilter());
                Assert.assertTrue(factory.implementsLimit());
                fixture.assertNoneClosed();
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("id\n1\n2\n");
                bindVariableService.setLong(1, 1);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("id\n1\n");
                bindVariableService.setLong(1, -1);
                assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("id\n3\n");
                fixture.assertNoneClosed();
            }
            fixture.assertAllClosedOnce();
        });
    }

    @Test
    public void testBoundLimitFailureClosesOnceAndPreservesPrimary() throws Exception {
        assertJitFilter((fixture, compiler) -> {
            final RuntimeException primary = new RuntimeException("bound JIT construction");
            final RuntimeException cleanup = new RuntimeException("bound limit close");
            fixture.failLongClose(cleanup);
            AsyncFilterConstructionFault.arm(primary);
            try {
                final RuntimeException actual = Assert.assertThrows(RuntimeException.class, () -> compiler.compile(
                        "SELECT * FROM jf_owner WHERE id > $1 LIMIT owned_long($2)", sqlExecutionContext
                ));
                Assert.assertSame(primary, actual);
                Assert.assertTrue(OwnershipFixture.hasSuppressed(actual, cleanup));
                Assert.assertTrue(fixture.longCount() > 0);
                fixture.assertAllClosedOnce();
            } finally {
                AsyncFilterConstructionFault.disarm();
            }
        });
    }

    private static void assertJitFilter(JitAssertion assertion) throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jf_owner(id INT)");
            execute("INSERT INTO jf_owner VALUES (1),(2),(3)");
            final int oldMode = sqlExecutionContext.getJitMode();
            final boolean wasParallel = sqlExecutionContext.isParallelFilterEnabled();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setParallelFilterEnabled(true);
            bindVariableService.clear();
            bindVariableService.setInt(0, 0);
            bindVariableService.setLong(1, 2);
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertion.run(fixture, compiler);
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
                sqlExecutionContext.setParallelFilterEnabled(wasParallel);
            }
        });
    }

    @FunctionalInterface
    private interface JitAssertion {
        void run(OwnershipFixture fixture, SqlCompilerImpl compiler) throws Exception;
    }
}
