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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * cairo.sql.max.bind.variables caps the highest $n that SQL text can introduce. The compiler
 * keeps indexed bind variables in a list as long as the highest index, so without the cap a
 * single $n could make it allocate an array of up to 2^31 slots.
 */
public class BindVariableLimitTest extends AbstractCairoTest {

    @Test
    public void testDefinedVariableAboveLimitIsAccepted() throws Exception {
        // A caller that defines the variables before it compiles, as WAL apply does when it
        // replays an UPDATE, recompiles the SQL whatever the current limit is: the variables
        // exist already, so the SQL text allocates none.
        setProperty(PropertyKey.CAIRO_SQL_MAX_BIND_VARIABLES, 2);
        assertMemoryLeak(() -> {
            bindVariableService.setLong(2, 42);
            assertQuery("SELECT $3 x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n42\n");
        });
    }

    @Test
    public void testIndexAboveLimitIsRejected() throws Exception {
        assertException(
                "SELECT x FROM long_sequence(1) WHERE x = $129",
                41,
                "bind variable index exceeds cairo.sql.max.bind.variables [index=129, max=128]"
        );
    }

    @Test
    public void testIndexAtLimitIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory ignore = select("SELECT $128 x")) {
                Assert.assertEquals(128, bindVariableService.getIndexedVariableCount());
            }
        });
    }

    @Test
    public void testLimitIsReadOnEveryCompile() throws Exception {
        // the setting is reloadable, so the compiler reads it on every compile
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_MAX_BIND_VARIABLES, 2);
            assertExceptionNoLeakCheck(
                    "SELECT 1 + $3 x",
                    11,
                    "bind variable index exceeds cairo.sql.max.bind.variables [index=3, max=2]"
            );
            setProperty(PropertyKey.CAIRO_SQL_MAX_BIND_VARIABLES, 3);
            try (RecordCursorFactory ignore = select("SELECT 1 + $3 x")) {
                Assert.assertEquals(3, bindVariableService.getIndexedVariableCount());
            }
        });
    }

    @Test
    public void testMaxIntIndexIsRejected() throws Exception {
        // before the limit, this asked the JVM for an array of Integer.MAX_VALUE slots
        assertException(
                "SELECT $2147483647",
                7,
                "bind variable index exceeds cairo.sql.max.bind.variables [index=2147483647, max=128]"
        );
    }
}
