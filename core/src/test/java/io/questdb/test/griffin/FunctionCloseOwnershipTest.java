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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A function closes every argument it owns, and frees its native memory even when closing one of its arguments
 * fails.
 */
public class FunctionCloseOwnershipTest extends AbstractCairoTest {

    @Test
    public void testArrayFunctionFreesItsArrayWhenArgumentCloseFails() throws Exception {
        assertCloseFailure(
                "SELECT ARRAY[x::DOUBLE, 2.0] * owned_long($1) a FROM long_sequence(2)",
                """
                        a
                        [2.0,4.0]
                        [4.0,4.0]
                        """
        );
    }

    @Test
    public void testInLongFreesItsSetWhenArgumentCloseFails() throws Exception {
        assertCloseFailure(
                "SELECT owned_long($1) IN (1, 2, 3) i FROM long_sequence(2)",
                """
                        i
                        true
                        true
                        """
        );
        assertCloseFailure(
                "SELECT x IN (owned_long($1), 3, 4) i FROM long_sequence(2)",
                """
                        i
                        false
                        true
                        """
        );
    }

    @Test
    public void testTimestampSequenceClosesItsIncrement() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 2);
            try (OwnershipFixture fixture = new OwnershipFixture(engine); SqlCompiler compiler = engine.getSqlCompiler()) {
                assertClosesIncrement(fixture, compiler, "timestamp_sequence(0, owned_long($1))", """
                        t
                        1970-01-01T00:00:00.000000Z
                        1970-01-01T00:00:00.000002Z
                        """);
                assertClosesIncrement(fixture, compiler, "timestamp_sequence_ns(0, owned_long($1))", """
                        t
                        1970-01-01T00:00:00.000000000Z
                        1970-01-01T00:00:00.000000002Z
                        """);
                assertClosesIncrement(fixture, compiler, "timestamp_sequence(NULL, owned_long($1))", "t\n\n\n");
                assertClosesIncrement(fixture, compiler, "timestamp_sequence_ns(NULL, owned_long($1))", "t\n\n\n");
            }
        });
    }

    private static void assertCloseFailure(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 2);
            try (OwnershipFixture fixture = new OwnershipFixture(engine); SqlCompiler compiler = engine.getSqlCompiler()) {
                final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                final RuntimeException closeFailure = new RuntimeException("owned_long close");
                fixture.failLiveLongClose(closeFailure);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }
                TestUtils.assertEquals(expected, sink);
                Assert.assertSame(closeFailure, Assert.assertThrows(RuntimeException.class, factory::close));
                fixture.assertAllClosedOnce();
            }
        });
    }

    private static void assertClosesIncrement(OwnershipFixture fixture, SqlCompiler compiler, String call, String expected) throws Exception {
        fixture.reset();
        try (
                RecordCursorFactory factory = compiler.compile("SELECT " + call + " t FROM long_sequence(2)", sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            println(factory, cursor);
        }
        TestUtils.assertEquals(expected, sink);
        fixture.assertAllClosedOnce();
    }
}
