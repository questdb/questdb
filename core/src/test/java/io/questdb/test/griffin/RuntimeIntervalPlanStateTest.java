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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RuntimeIntervalPlanStateTest extends AbstractCairoTest {

    @Test
    public void testFailedRecalculationDoesNotPublishPartialIntervals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('1970-01-01T00:00:01.000000Z'), ('1970-01-01T00:00:03.000000Z')");
            execute("CREATE TABLE bounds (b TIMESTAMP)");
            execute("INSERT INTO bounds VALUES ('1970-01-01T00:00:01.000000Z')");

            try (RecordCursorFactory factory = select("SELECT * FROM x WHERE ts = (SELECT b FROM bounds)")) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    // Opening the cursor evaluates the scalar-subquery interval.
                }

                execute("INSERT INTO bounds VALUES ('1970-01-01T00:00:02.000000Z')");
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected scalar sub-query failure");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "more than one row");
                }

                execute("TRUNCATE TABLE bounds");
                execute("INSERT INTO bounds VALUES ('1970-01-01T00:00:03.000000Z')");
                assertPlanContains(factory, "1970-01-01T00:00:03.000000Z");
            }
        });
    }

    @Test
    public void testPlanRenderUsesCurrentBindAfterFactoryExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('1970-01-01T00:00:01.000000Z'), ('1970-01-01T00:00:02.000000Z')");
            bindVariableService.setTimestamp(0, 1_000_000L);

            try (RecordCursorFactory factory = select("SELECT * FROM x WHERE ts >= $1")) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    // Opening the cursor evaluates the first bind value.
                }

                bindVariableService.setTimestamp(0, 2_000_000L);
                assertPlanContains(factory, "1970-01-01T00:00:02.000000Z");
            }
        });
    }

    private static void assertPlanContains(RecordCursorFactory factory, CharSequence expectedInterval) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        TestUtils.assertContains(sink.getSink(), expectedInterval);
    }
}
