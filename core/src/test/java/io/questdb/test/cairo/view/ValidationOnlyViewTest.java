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

package io.questdb.test.cairo.view;

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.Assert;
import org.junit.Test;

// Validation-only compilation must not mutate view state. CREATE OR REPLACE VIEW rewrites the
// stored view definition inline during compilation, and COMPILE VIEW enqueues a recompile event;
// both are guarded by !isValidationOnly() in SqlCompilerImpl. These tests validate each statement
// (asserting no mutation), then run it for real (asserting the effect takes place).
public class ValidationOnlyViewTest extends AbstractViewTest {

    @Test
    public void testValidateCompileViewDoesNotEnqueue() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);

            // Validation compiles COMPILE VIEW to the right query type but must not enqueue a
            // recompile event, so draining the view queue afterwards is a no-op and the view
            // definition stays put.
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
            ctx.setValidationOnly(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("COMPILE VIEW " + VIEW1, ctx);
                Assert.assertEquals(CompiledQuery.COMPILE_VIEW, cq.getType());
            } finally {
                ctx.setValidationOnly(false);
            }
            drainViewQueue();

            assertViewDefinition(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);
        });
    }

    @Test
    public void testValidateCreateOrReplaceViewDoesNotReplace() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createView(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);

            // Validation must not replace the stored view definition.
            validate("CREATE OR REPLACE VIEW " + VIEW1 + " AS (SELECT ts, k2, v FROM " + TABLE1 + ")");
            assertViewDefinition(VIEW1, "SELECT ts, k, v FROM " + TABLE1, TABLE1);

            // Real execution replaces it.
            createOrReplaceView("SELECT ts, k2, v FROM " + TABLE1, TABLE1);
            assertViewDefinition(VIEW1, "SELECT ts, k2, v FROM " + TABLE1, TABLE1);
        });
    }

    private static void validate(String sql) throws Exception {
        final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) sqlExecutionContext;
        ctx.setValidationOnly(true);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.compile(sql, ctx);
        } finally {
            ctx.setValidationOnly(false);
        }
    }
}
