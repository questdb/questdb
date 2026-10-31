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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ExplainPlanLifecycleTest extends AbstractCairoTest {
    @Test
    public void testInsertSelectPlanSurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (x INT)");
            execute("CREATE TABLE source (x INT)");
            assertPlanSurvivesCompilerReuseAndClose(
                    "EXPLAIN INSERT INTO target SELECT x FROM source",
                    """
                            Insert into table: target
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: source
                            """
            );
        });
    }

    @Test
    public void testInsertValuesPlanSurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE target (x INT)");
            execute("CREATE TABLE source (x INT)");
            assertPlanSurvivesCompilerReuseAndClose(
                    "EXPLAIN INSERT INTO target VALUES (1)",
                    "Insert into table: target\n"
            );
        });
    }

    private static void assertPlan(RecordCursorFactory factory, String expected) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                sink.put(cursor.getRecord().getStrA(0)).put('\n');
            }
        }
        TestUtils.assertEquals(expected, sink);
    }

    private static void assertPlanSurvivesCompilerReuseAndClose(String sql, String expected) throws Exception {
        RecordCursorFactory retained = null;
        try {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                assertPlan(retained, expected);
                try (RecordCursorFactory other = compiler.compile(
                        "EXPLAIN INSERT INTO source VALUES (2)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertPlan(other, "Insert into table: source\n");
                    assertPlan(retained, expected);
                }
                compiler.clear();
                assertPlan(retained, expected);
            }
            assertPlan(retained, expected);
        } finally {
            Misc.free(retained);
        }
    }
}
