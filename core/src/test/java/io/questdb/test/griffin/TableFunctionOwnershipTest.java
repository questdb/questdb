/*******************************************************************************
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

import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TableFunctionTestUtils;
import io.questdb.test.tools.TableFunctionTestUtils.CloseCountingRecordCursorFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Ownership of the cursor factories {@code SqlOptimiser#parseFunctionAndEnumerateColumns} instantiates for
 * FROM/JOIN table functions, on the compile paths that reject a statement AFTER {@code optimise()} returned
 * and BEFORE code generation has taken those factories over.
 * <p>
 * {@code CreateMatViewTest} covers the materialized-view path; this covers the general one in
 * {@code SqlCompilerImpl#compileExecutionModel}, which rejects INSERT and UPDATE models without ever
 * generating. Both close counts are asserted exactly, in both directions: a miss leaks the factory and a
 * second close is a use-after-free.
 */
public class TableFunctionOwnershipTest extends AbstractCairoTest {

    @Test
    public void testInsertAsSelectRejectedAfterOptimiseClosesTableFunctionFactoryOnce() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dest (a varchar, b varchar)");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            final String functionName = "owned_cursor";
            TableFunctionTestUtils.register(engine, functionName, SqlExecutionRequirements.NONE, null, factories);
            try {
                // validateAndOptimiseInsertAsSelect() optimises FIRST and only then compares the column
                // count, so the rejection lands in exactly the window this test is about: the optimiser
                // has instantiated and is holding the table function, and generation - which would have
                // taken it over - never runs.
                assertExceptionNoLeakCheck(
                        "insert into dest (a, b) select * from " + functionName + "()",
                        12,
                        "column count mismatch"
                );

                assertEquals(1, factories.size());
                assertEquals(1, factories.getQuick(0).getCloseCount());

                // The next compile borrows the same pooled compiler and clears its optimiser state. A
                // reference left behind there must not close the factory a second time.
                execute("create table other (x long)");
                assertEquals(1, factories.getQuick(0).getCloseCount());
            } finally {
                TableFunctionTestUtils.unregister(engine, functionName);
            }
        });
    }

    @Test
    public void testInsertAsSelectSuccessLeavesTableFunctionFactoryToTheGeneratedTree() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dest (a varchar)");

            final ObjList<CloseCountingRecordCursorFactory> factories = new ObjList<>();
            final String functionName = "owned_cursor";
            TableFunctionTestUtils.register(engine, functionName, SqlExecutionRequirements.NONE, null, factories);
            try {
                // The counterpart of the rejection above: when the statement is accepted, generation owns
                // every instantiated factory and the compiled tree - nothing else - closes it, once. Without
                // this direction the test above would still pass if the compile path closed the factory
                // unconditionally rather than only on the throw.
                execute("insert into dest (a) select * from " + functionName + "()");

                assertTrue(factories.size() > 0);
                for (int i = 0, n = factories.size(); i < n; i++) {
                    assertEquals(1, factories.getQuick(i).getCloseCount());
                }

                execute("create table other (x long)");
                for (int i = 0, n = factories.size(); i < n; i++) {
                    assertEquals(1, factories.getQuick(i).getCloseCount());
                }
            } finally {
                TableFunctionTestUtils.unregister(engine, functionName);
            }
        });
    }
}
