/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.fail;

public class ViewCompileTest extends ViewInvalidationTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public void enqueueCompileView(TableToken viewToken) {
                // do not publish compile events from engine, so views do not get invalidated and
                // fixed automatically on table create/drop/rename and structural changes
            }
        };
        AbstractViewTest.setUpStatic();
    }

    @Test
    public void testCompileViewInQuotes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW " + VIEW1 + " AS (SELECT 42 AS col)");

            execute("COMPILE VIEW '" + VIEW1 + "'");
            execute("COMPILE VIEW \"" + VIEW1 + "\"");
        });
    }

    @Test
    public void testCompileViewSyntax() throws Exception {
        assertMemoryLeak(() -> {
            assertException("compile view", 12, "view name expected");

            execute("create view " + VIEW1 + " as (select 249)");
            assertException("compile view " + VIEW1 + " bla", 19, "unexpected token [bla]");
        });
    }

    @Override
    protected void detectInvalidView(String viewName, String expectedErrorMessage) {
        // automatic view invalidation is switched off in test, so have to query or
        // recompile the view to be able to detect that it does not work anymore.
        // child views are invalidated recursively.
        try (RecordCursorFactory ignored = select(viewName)) {
            fail("Expected SqlException");
        } catch (SqlException e) {
            assertContains(e.getFlyweightMessage(), expectedErrorMessage);
        }
    }

    @Override
    protected void fixInvalidView(String viewName) throws SqlException {
        // automatic view reset is switched off in test, so have to compile
        // the view manually to be able to fix it.
        // if the view successfully compiles and becomes valid, its children
        // will be compiled recursively too.
        compileView(viewName);
    }
}
