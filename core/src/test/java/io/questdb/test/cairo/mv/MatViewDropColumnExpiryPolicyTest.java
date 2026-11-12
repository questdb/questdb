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

package io.questdb.test.cairo.mv;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * ALTER MATERIALIZED VIEW ... DROP COLUMN of a column that the view's EXPIRE ROWS policy reads. Every read of a
 * view with a policy goes through the policy's keep-filter, LATEST ON or window rewrite, which references the
 * columns the policy names, so dropping one of them would leave a view that no query can read. The statement is
 * refused at the column name, like the drop of the designated timestamp, and names the policy and the way out:
 * change the policy with SET EXPIRE, or remove it with DROP EXPIRE, first. Columns the policy does not read drop
 * as usual.
 * <p>
 * Expected rows follow from the base rows each test inserts and the policy's documented semantics: WHEN hides
 * the rows its predicate holds for, KEEP LATEST keeps the newest row per key, KEEP [N] HIGHEST / LOWEST keeps the
 * rows with the highest or lowest values per key.
 */
public class MatViewDropColumnExpiryPolicyTest extends AbstractCairoTest {
    // The buckets of createSampledView() that the policies of the tests that use it leave visible.
    private static final String SAMPLED_VISIBLE = """
            ts\tsym\tprice\tamount
            2024-01-01T00:00:00.000000Z\ta\t3.5\t30.0
            2024-01-01T01:00:00.000000Z\ta\t4.5\t40.0
            """;
    private static final String SAMPLED_VIEW_SQL = "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h";

    @Test
    public void testDropColumnAllowedAfterDropExpire() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);
            assertDropColumnRefused("price_1h", "price", 2);

            execute("ALTER MATERIALIZED VIEW price_1h DROP EXPIRE");
            drainWalAndMatViewQueues();
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalAndMatViewQueues();
            // Without the policy the bucket the policy hid (b at 2.5) is visible again.
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    2024-01-01T01:00:00.000000Z\ta\t40.0
                    """);
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testDropColumnAllowedAfterSetExpireStopsReadingIt() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0 AND amount > 15.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);
            // A predicate over two columns reads both.
            assertDropColumnRefused("price_1h", "price", 2);
            assertDropColumnRefused("price_1h", "amount", 2);

            execute("ALTER MATERIALIZED VIEW price_1h SET EXPIRE ROWS WHEN amount > 35.0");
            drainWalAndMatViewQueues();
            assertDropColumnRefused("price_1h", "amount", 2);
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN price");
            drainWalAndMatViewQueues();
            // The new policy hides the bucket whose amount is above 35 (a at 01:00).
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t30.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    """);
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testDropColumnReadByKeepHighestRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS KEEP HIGHEST ON v PARTITION BY k");
            // The view has not refreshed yet: the policy comes with CREATE. The policy reads the ranked column
            // and the key.
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);
            // The highest v of each key: A at 2.0 and B at 3.0.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
            // A new highest v of B reaches the view.
            execute("INSERT INTO base VALUES ('B', 9.0, 90.0, '2024-01-05T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("mv");
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t9.0\t2024-01-05T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnReadByKeepLatestRefused() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, \"group key\" SYMBOL, \"select\" SYMBOL, v DOUBLE, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 'X', 'S', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'X', 'S', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('B', 'X', 'S', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS KEEP LATEST PARTITION BY K, \"group key\", \"select\"");
            // The view has not refreshed yet: the policy comes with CREATE.
            assertDropColumnRefused("mv", "k", 2);
            // The newest row of each key: A at 2.0 and B at 3.0.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tgroup key\tselect\tv\tts
                    A\tX\tS\t2.0\t2024-01-02T00:00:00.000000Z
                    B\tX\tS\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            // The key is matched case-insensitively, and quoted and keyword names are keys like any other.
            assertDropColumnRefused("mv", "k", 2);
            assertDropColumnRefused("mv", "\"K\"", 2);
            assertDropColumnRefused("mv", "\"group key\"", 2);
            assertDropColumnRefused("mv", "\"select\"", 2);
            assertColumns("mv", """
                    column\ttype
                    k\tSYMBOL
                    group key\tSYMBOL
                    select\tSYMBOL
                    v\tDOUBLE
                    ts\tTIMESTAMP
                    """);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN v");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns("""
                    k\tgroup key\tselect\tts
                    A\tX\tS\t2024-01-02T00:00:00.000000Z
                    B\tX\tS\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    @Test
    public void testDropColumnReadByKeepLowestTopNRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS KEEP 2 LOWEST ON v");
            drainWalAndMatViewQueues();
            // The two lowest v of all rows: A at 1.0 and B at 0.5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t1.0\t10.0\t2024-01-01T00:00:00.000000Z
                    B\t0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);

            // Without PARTITION BY the policy reads no key.
            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN k");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    v\tw\tts
                    1.0\t10.0\t2024-01-01T00:00:00.000000Z
                    0.5\t40.0\t2024-01-04T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    @Test
    public void testDropColumnReadByScalarPredicateRefused() throws Exception {
        assertMemoryLeak(() -> {
            createSampledView("EXPIRE ROWS WHEN price < 3.0");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns(SAMPLED_VISIBLE);

            // A view WAL transaction that has not applied yet does not change the answer.
            execute("ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT");
            assertDropColumnRefused("price_1h", "price", 2);
            assertDropColumnRefused("price_1h", "PRICE", 2);
            assertColumns("price_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    extra\tINT
                    """);

            // A column the policy does not read drops as usual, and the policy still applies.
            execute("ALTER MATERIALIZED VIEW price_1h DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t3.5\tnull
                    2024-01-01T01:00:00.000000Z\ta\t4.5\tnull
                    """);
            // A base row reaches the view, which the policy still filters.
            execute("INSERT INTO base VALUES ('b', 5.5, 50.0, '2024-01-01T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("price_1h");
            assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().returns("""
                    ts\tsym\tprice\textra
                    2024-01-01T00:00:00.000000Z\ta\t3.5\tnull
                    2024-01-01T01:00:00.000000Z\ta\t4.5\tnull
                    2024-01-01T01:00:00.000000Z\tb\t5.5\tnull
                    """);
        });
    }

    @Test
    public void testDropColumnReadByWindowPredicateRefused() throws Exception {
        assertMemoryLeak(() -> {
            createPassthroughView("EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k)");
            drainWalAndMatViewQueues();
            // The policy hides every row below its key's maximum v: A at 1.0 and B at 0.5.
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tw\tts
                    A\t2.0\t20.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t30.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertDropColumnRefused("mv", "v", 2);
            assertDropColumnRefused("mv", "k", 2);

            execute("ALTER MATERIALIZED VIEW mv DROP COLUMN w");
            drainWalAndMatViewQueues();
            assertQuery("mv ORDER BY ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
            assertViewValid("mv");
        });
    }

    private void assertColumns(String viewName, String expected) throws Exception {
        assertQuery("SELECT \"column\", type FROM table_columns('" + viewName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    // The DROP COLUMN fails at the column name, with a message that names the policy and how to change it, and
    // writes nothing: the view keeps the column, stays valid and stays readable.
    private void assertDropColumnRefused(String viewName, String columnToken, int visibleRowCount) throws Exception {
        final String sql = "ALTER MATERIALIZED VIEW " + viewName + " DROP COLUMN " + columnToken;
        try {
            execute(sql);
            Assert.fail("the statement must fail: " + sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, sql.lastIndexOf(columnToken), e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "EXPIRE ROWS");
            TestUtils.assertContains(e.getFlyweightMessage(), "DROP EXPIRE");
        }
        drainWalAndMatViewQueues();
        assertViewValid(viewName);
        // Every read of the view goes through the policy's rewrite.
        sink.clear();
        printSql("SELECT count() FROM " + viewName, sink);
        TestUtils.assertEquals("count\n" + visibleRowCount + "\n", sink);
    }

    private void assertViewValid(String viewName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    // Base rows (k, v, w, ts): A 1.0, A 2.0, B 3.0, B 0.5 on four days; a passthrough view with the policy.
    private static void createPassthroughView(String expireClause) throws SqlException {
        execute("CREATE TABLE base (k SYMBOL, v DOUBLE, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('A', 1.0, 10.0, '2024-01-01T00:00:00.000000Z'),
                ('A', 2.0, 20.0, '2024-01-02T00:00:00.000000Z'),
                ('B', 3.0, 30.0, '2024-01-03T00:00:00.000000Z'),
                ('B', 0.5, 40.0, '2024-01-04T00:00:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " + expireClause);
    }

    // A SAMPLE BY view: buckets a@00:00 (3.5, 30.0), b@00:00 (2.5, 20.0) and a@01:00 (4.5, 40.0).
    private static void createSampledView(String expireClause) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.5, 10.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.5, 20.0, '2024-01-01T00:20:00.000000Z'),
                ('a', 3.5, 30.0, '2024-01-01T00:40:00.000000Z'),
                ('a', 4.5, 40.0, '2024-01-01T01:10:00.000000Z')
                """);
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW price_1h AS (" + SAMPLED_VIEW_SQL + ") PARTITION BY DAY " + expireClause);
        drainWalAndMatViewQueues();
    }
}
