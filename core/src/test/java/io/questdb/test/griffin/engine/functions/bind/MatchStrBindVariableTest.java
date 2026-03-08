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

package io.questdb.test.griffin.engine.functions.bind;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class MatchStrBindVariableTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select x from long_sequence(1) where '1GQO2' ~ $1")) {
                bindVariableService.setStr(0, "GQO");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("x\n" +
                        "1\n", sink);

                bindVariableService.setStr(0, "QTQ");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("x\n", sink);

                bindVariableService.setStr(0, null);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("x\n", sink);
            }
        });
    }

    @Test
    public void testDynamicRegexFailure() throws Exception {
        assertException(
                "x where s ~ s",
                "create table x as (select rnd_str() s from long_sequence(100))",
                12,
                "not implemented: dynamic pattern would be very slow to execute"
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str() s from long_sequence(100))");

            try (RecordCursorFactory factory = select("x where s ~ $1")) {
                bindVariableService.setStr(0, "GQO");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("s\n" +
                        "YCTGQO\n", sink);

                bindVariableService.setStr(0, "QTQ");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("s\n" +
                        "ZWEVQTQO\n", sink);

                bindVariableService.setStr(0, null);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("s\n", sink);
            }
        });
    }
}
