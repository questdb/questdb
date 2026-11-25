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
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FirstLastTimestampCachedPlanTest extends AbstractCairoTest {
    private static final String FIRST_TS = "2022-02-24T00:00:00.000000Z";
    private static final String LAST_TS = "2022-02-24T00:00:09.000000Z";

    @Test
    public void testCompiledPlanIsOutOfDateAfterDropColumn() throws Exception {
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                assertOutOfDateAfterDropColumn(walMode, "SELECT max(ts) FROM tab", LAST_TS);
                assertOutOfDateAfterDropColumn(walMode, "SELECT min(ts) FROM tab", FIRST_TS);
                assertOutOfDateAfterDropColumn(walMode, "SELECT first(ts) FROM tab", FIRST_TS);
                assertOutOfDateAfterDropColumn(walMode, "SELECT last(ts) FROM tab", LAST_TS);
                assertOutOfDateAfterDropColumn(walMode, "SELECT max(ts) FROM tab WHERE b > 2_000", LAST_TS);
                assertOutOfDateAfterDropColumn(walMode, "SELECT min(ts) FROM tab WHERE b > 2_000", "2022-02-24T00:00:02.000000Z");
            }
        });
    }

    @Test
    public void testCompiledPlanIsOutOfDateAfterRenameAndAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            createTable("WAL");
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile("SELECT max(ts) FROM tab", sqlExecutionContext).getRecordCursorFactory()
            ) {
                assertCursor(factory, LAST_TS);

                execute("ALTER TABLE tab RENAME COLUMN a TO c");
                drainWalQueue();
                assertOutOfDate(factory);

                execute("ALTER TABLE tab ADD COLUMN d LONG");
                drainWalQueue();
                assertOutOfDate(factory);
            }
            assertQuery("SELECT max(ts) FROM tab")
                    .noLeakCheck()
                    .timestampDesc("max")
                    .expectSize()
                    .returns("max\n" + LAST_TS + '\n');
        });
    }

    @Test
    public void testCompiledPlanIsOutOfDateAfterTableRecreated() throws Exception {
        assertMemoryLeak(() -> {
            createTable("BYPASS WAL");
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile("SELECT last(ts) FROM tab", sqlExecutionContext).getRecordCursorFactory()
            ) {
                assertCursor(factory, LAST_TS);

                execute("DROP TABLE tab");
                execute("CREATE TABLE tab (a LONG, ts TIMESTAMP, b LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO tab VALUES (1, '2023-01-01T00:00:00.000000Z', 1)");
                assertOutOfDate(factory);
            }
        });
    }

    private static void assertCursor(RecordCursorFactory factory, String expectedTs) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            sink.clear();
            Assert.assertTrue(cursor.hasNext());
            sink.putISODate(cursor.getRecord().getTimestamp(0));
            TestUtils.assertEquals(expectedTs, sink);
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void assertOutOfDate(RecordCursorFactory factory) throws Exception {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("stale plan ran");
        } catch (TableReferenceOutOfDateException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
        }
    }

    private static void createTable(String walMode) throws Exception {
        execute("CREATE TABLE tab (a LONG, ts TIMESTAMP, b LONG) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("INSERT INTO tab SELECT x, timestamp_sequence('2022-02-24', 1_000_000L), x * 1_000 FROM long_sequence(10)");
        drainWalQueue();
    }

    private void assertOutOfDateAfterDropColumn(String walMode, String query, String expectedTs) throws Exception {
        createTable(walMode);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()
        ) {
            assertCursor(factory, expectedTs);
            assertCursor(factory, expectedTs);

            execute("ALTER TABLE tab DROP COLUMN a");
            drainWalQueue();
            assertOutOfDate(factory);
        }
        final String column = query.substring(7, query.indexOf('('));
        final QueryAssertion assertion = assertQuery(query).noLeakCheck();
        if (column.equals("max") || column.equals("last")) {
            assertion.timestampDesc(column);
        } else {
            assertion.timestamp(column);
        }
        assertion
                .expectSize(!query.contains("WHERE"))
                .returns(column + '\n' + expectedTs + '\n');
        execute("DROP TABLE tab");
    }
}
