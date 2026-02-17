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

public class RegexpReplaceStrBindVariableTest extends AbstractCairoTest {

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_str('foobar','barbaz') s from long_sequence(3))");

            try (RecordCursorFactory factory = select("select regexp_replace(s, $1, $2) from x")) {
                bindVariableService.setStr(0, "foo");
                bindVariableService.setStr(1, "bar");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals("regexp_replace\n" +
                        "barbar\n" +
                        "barbar\n" +
                        "barbaz\n", sink);

                bindVariableService.setStr(0, "def");
                bindVariableService.setStr(1, "abc");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals(
                        "regexp_replace\n" +
                                "foobar\n" +
                                "foobar\n" +
                                "barbaz\n",
                        sink
                );

                bindVariableService.setStr(0, null);
                bindVariableService.setStr(1, "abc");

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals(
                        "regexp_replace\n" +
                                "\n" +
                                "\n" +
                                "\n",
                        sink
                );

                bindVariableService.setStr(0, "abc");
                bindVariableService.setStr(1, null);

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }

                TestUtils.assertEquals(
                        "regexp_replace\n" +
                                "\n" +
                                "\n" +
                                "\n",
                        sink
                );
            }
        });
    }
}
