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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every sub-query level a compilation borrows returns to the compiler's free levels, which retain at most 32 levels
 * whatever the width or depth of the compiled queries.
 */
public class QueryLevelRetentionTest extends AbstractCairoTest {
    private static final int MAX_RETAINED_LEVELS = 32;
    private static final String BAD_SUBQUERY = "(SELECT sum(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym)"
            + " RANGE BETWEEN 2 minute PRECEDING AND 4 minute PRECEDING)";

    @Test
    public void testBindFailureReturnsEveryLevel() throws Exception {
        assertMemoryLeak(() -> {
            assertRetainedAfterFailure(
                    "SELECT x = (" + nestedQuery(0) + ") a, x = (" + nestedQuery(1) + ") b, x = (SELECT missing FROM long_sequence(1)) c FROM long_sequence(1)",
                    "Invalid column: missing",
                    4
            );
            assertRetainedAfterFailure(
                    "SELECT x FROM long_sequence(1) WHERE x = (SELECT x FROM long_sequence(1) WHERE x = (SELECT missing FROM long_sequence(1)))",
                    "Invalid column: missing",
                    2
            );
        });
    }

    @Test
    public void testDeepQueryRetainsAtMostCap() throws Exception {
        assertMemoryLeak(() -> {
            assertRetainedAfterSuccess(nestedQuery(5), 5);
            assertRetainedAfterSuccess(nestedQuery(MAX_RETAINED_LEVELS + 8), MAX_RETAINED_LEVELS);
        });
    }

    @Test
    public void testGenerationFailureReturnsEveryLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertRetainedAfterFailure(
                    "SELECT price FROM trades WHERE price = (SELECT price FROM prices WHERE price = (SELECT price FROM prices)) AND price = " + BAD_SUBQUERY,
                    "WINDOW join hi value cannot be less than lo value",
                    3
            );
            assertRetainedAfterFailure(
                    "SELECT price FROM trades WHERE price = (SELECT price FROM prices WHERE price = " + BAD_SUBQUERY + ")",
                    "WINDOW join hi value cannot be less than lo value",
                    2
            );
        });
    }

    @Test
    public void testWideAndDeepQueryRetainsAtMostCap() throws Exception {
        assertMemoryLeak(() -> {
            final StringSink sql = new StringSink();
            sql.put("SELECT ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    sql.put(", ");
                }
                sql.put("x = (").put(nestedQuery(5)).put(") c").put(i);
            }
            sql.put(" FROM long_sequence(1)");
            assertRetainedAfterSuccess(sql.toString(), MAX_RETAINED_LEVELS);
        });
    }

    @Test
    public void testWideQueryRetainsAtMostCap() throws Exception {
        assertMemoryLeak(() -> {
            assertRetainedAfterSuccess(wideQuery(3), 3);
            assertRetainedAfterSuccess(wideQuery(1000), MAX_RETAINED_LEVELS);
        });
    }

    private static void assertRetainedAfterFailure(String sql, String message, int expected) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int i = 0; i < 2; i++) {
                try (RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("compilation must fail");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), message);
                }
                Assert.assertEquals(expected, compiler.getRetainedQueryLevelCount());
            }
        }
    }

    private static void assertRetainedAfterSuccess(String sql, int expected) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int i = 0; i < 2; i++) {
                try (RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(ignore);
                }
                compiler.clear();
                Assert.assertEquals(expected, compiler.getRetainedQueryLevelCount());
            }
        }
    }

    private static String nestedQuery(int depth) {
        String sql = "SELECT x FROM long_sequence(1)";
        for (int i = 0; i < depth; i++) {
            sql = "SELECT x FROM long_sequence(1) WHERE x = (" + sql + ")";
        }
        return sql;
    }

    private static String wideQuery(int width) {
        final StringSink sql = new StringSink();
        sql.put("SELECT ");
        for (int i = 0; i < width; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put("x = (SELECT x FROM long_sequence(1)) c").put(i);
        }
        sql.put(" FROM long_sequence(1)");
        return sql.toString();
    }
}
