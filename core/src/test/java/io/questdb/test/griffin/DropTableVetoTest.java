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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * {@code checkTableDroppable} vetoes dropping a table outright, independently of permissions.
 * Enterprise overrides it for the view audit table, so every route that drops a table has to
 * consult it - a veto that only one route honours is not a veto.
 */
public class DropTableVetoTest extends AbstractCairoTest {

    private static final String UNDROPPABLE = "undroppable";
    private static final String VETO_MESSAGE = "this table cannot be dropped";

    @Test
    public void testDropAllTablesHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ordinary (s SYMBOL)");
            execute("CREATE TABLE " + UNDROPPABLE + " (s SYMBOL)");

            try {
                dropWith("DROP ALL TABLES");
                fail("expected DROP ALL TABLES to report the vetoed table");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), VETO_MESSAGE);
            }

            assertNull("an ordinary table should still have been dropped", engine.getTableTokenIfExists("ordinary"));
            assertNotNull("the vetoed table must survive DROP ALL TABLES", engine.getTableTokenIfExists(UNDROPPABLE));
        });
    }

    @Test
    public void testDropTableHonoursTheVeto() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + UNDROPPABLE + " (s SYMBOL)");

            try {
                dropWith("DROP TABLE " + UNDROPPABLE);
                fail("expected DROP TABLE to be vetoed");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), VETO_MESSAGE);
            }

            assertNotNull("the vetoed table must survive DROP TABLE", engine.getTableTokenIfExists(UNDROPPABLE));
        });
    }

    /**
     * Runs the drop through the vetoing compiler end to end. The operation has to be executed by
     * the same compiler that compiled it: {@code Operation.execute} borrows a compiler from the
     * engine's pool, which would be a plain one.
     */
    private static void dropWith(String sql) throws SqlException {
        try (VetoingCompiler compiler = new VetoingCompiler(engine)) {
            final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
            compiler.execute(cq.getOperation(), sqlExecutionContext);
        }
    }

    private static class VetoingCompiler extends SqlCompilerImpl {
        VetoingCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        protected void checkTableDroppable(TableToken tableToken) {
            if (Chars.equalsIgnoreCase(tableToken.getTableName(), UNDROPPABLE)) {
                throw CairoException.nonCritical().put(VETO_MESSAGE);
            }
        }
    }
}
