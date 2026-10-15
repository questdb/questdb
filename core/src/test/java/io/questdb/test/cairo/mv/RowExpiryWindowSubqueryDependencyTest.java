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


package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A window WHEN policy can read another table through a subquery. Every read of the view compiles
 * the stored predicate again, so a table or column the subquery names can disappear or change type
 * after SET EXPIRE accepted it. The read then fails closed with the raw compile error, positioned in
 * the query that {@code SqlParser.expandExpiringTable} builds around the stored predicate, not in the
 * reader's SQL. {@code materialized_views()} keeps reporting the view as valid. Restoring what the
 * policy reads, or DROP EXPIRE, makes the view readable again. A same-named table created later is
 * the one the policy reads. See "A window policy's subquery can lose its table" in
 * {@code docs/row-expiry-limitations.md}.
 * <p>
 * Shared data: base A: 1.0@d1, 3.0@d2; B: 5.0@d1, 4.0@d2. Under {@link #IN_LOOKUP_POLICY} the policy
 * expires a row below its key's maximum when {@code lookup} lists the key.
 */
public class RowExpiryWindowSubqueryDependencyTest extends AbstractCairoTest {
    private static final String ALL_ROWS = """
            k\tv\tts
            A\t1.0\t2024-01-01T00:00:00.000000Z
            A\t3.0\t2024-01-02T00:00:00.000000Z
            B\t5.0\t2024-01-01T00:00:00.000000Z
            B\t4.0\t2024-01-02T00:00:00.000000Z
            """;
    private static final String IN_LOOKUP_POLICY = "v < max(v) OVER (PARTITION BY k) AND k IN (SELECT k FROM lookup)";
    private static final String LOOKUP_A_ROWS = """
            k\tv\tts
            A\t3.0\t2024-01-02T00:00:00.000000Z
            B\t5.0\t2024-01-01T00:00:00.000000Z
            B\t4.0\t2024-01-02T00:00:00.000000Z
            """;
    private static final String MIN_X_POLICY = "v < max(v) OVER (PARTITION BY k) AND v > (SELECT min(x) FROM lookup)";
    // SqlParser.expandExpiringTable reads a window-policied view through this query, followed by the
    // stored predicate. Errors that compiling the predicate raises carry offsets into that query.
    private static final int POLICY_OFFSET = "SELECT \"k\",\"v\",\"ts\" FROM (SELECT *, CASE WHEN (".length();
    private static final String READ = "SELECT * FROM mv ORDER BY k, ts";

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testCompatibleTypeChangeKeepsPolicyWorking() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithLookupPolicy(IN_LOOKUP_POLICY, LOOKUP_A_ROWS);
            // SYMBOL -> VARCHAR keeps "k IN (SELECT k FROM lookup)" legal
            execute("ALTER TABLE lookup ALTER COLUMN k TYPE VARCHAR");
            drainWalQueue();
            assertQuery(READ).noLeakCheck().returns(LOOKUP_A_ROWS);

            // the policy reads the converted column: listing B now expires B 4.0
            execute("INSERT INTO lookup VALUES ('B', 2.0)");
            assertQuery(READ).noLeakCheck().returns("""
                    k\tv\tts
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testDropColumnReadByPolicySubquery() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithLookupPolicy(IN_LOOKUP_POLICY, LOOKUP_A_ROWS);
            execute("ALTER TABLE lookup DROP COLUMN k");
            drainWalQueue();
            // SqlOptimiser.validateColumnAndGetModelIndex reports the subquery's column reference
            final int kPosition = policyPosition(IN_LOOKUP_POLICY, "k FROM lookup");
            assertRawError(READ, kPosition, "Invalid column: k");

            // the policy reads the re-added column, which is NULL in the existing row, so nothing
            // expires until a row lists A again
            execute("ALTER TABLE lookup ADD COLUMN k SYMBOL");
            drainWalQueue();
            assertQuery(READ).noLeakCheck().returns(ALL_ROWS);
            execute("INSERT INTO lookup (k, x) VALUES ('A', 2.0)");
            assertQuery(READ).noLeakCheck().returns(LOOKUP_A_ROWS);

            execute("ALTER TABLE lookup DROP COLUMN k");
            drainWalQueue();
            assertRawError(READ, kPosition, "Invalid column: k");
            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            assertQuery(READ).noLeakCheck().expectSize().returns(ALL_ROWS);
        });
    }

    @Test
    public void testDropTableReadByPolicySubquery() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithLookupPolicy(IN_LOOKUP_POLICY, LOOKUP_A_ROWS);
            execute("DROP TABLE lookup");
            // SqlOptimiser.openReaderAndEnumerateColumns reports the subquery's table name, at the same
            // position whatever SQL the reader wrote around the view
            final int lookupPosition = policyPosition(IN_LOOKUP_POLICY, "lookup");
            final String message = "table does not exist [table=lookup]";
            assertRawError(READ, lookupPosition, message);
            assertRawError("SELECT count() FROM mv", lookupPosition, message);
            assertRawError("SELECT b.k, m.v FROM base b JOIN mv m ON b.k = m.k AND b.ts = m.ts", lookupPosition, message);

            // a refresh does not read the policy, so the view stays valid
            execute("INSERT INTO base VALUES ('C', 6.0, '2024-01-03T00:00:00Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            mv\tvalid\t
                            """);
            assertRawError(READ, lookupPosition, message);

            // the policy reads the re-created table, which lists only B
            execute("CREATE TABLE lookup (k SYMBOL)");
            execute("INSERT INTO lookup VALUES ('B')");
            assertQuery(READ).noLeakCheck().returns("""
                    k\tv\tts
                    A\t1.0\t2024-01-01T00:00:00.000000Z
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    C\t6.0\t2024-01-03T00:00:00.000000Z
                    """);

            execute("DROP TABLE lookup");
            assertRawError(READ, lookupPosition, message);
            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            assertQuery(READ).noLeakCheck().expectSize().returns(ALL_ROWS + "C\t6.0\t2024-01-03T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testIncompatibleTypeChangeReadByPolicySubquery() throws Exception {
        assertMemoryLeak(() -> {
            // with min(x) = 2.0 the policy expires B 4.0 only
            createViewWithLookupPolicy(
                    MIN_X_POLICY,
                    """
                            k\tv\tts
                            A\t1.0\t2024-01-01T00:00:00.000000Z
                            A\t3.0\t2024-01-02T00:00:00.000000Z
                            B\t5.0\t2024-01-01T00:00:00.000000Z
                            """
            );
            execute("ALTER TABLE lookup ALTER COLUMN x TYPE VARCHAR");
            drainWalQueue();
            // AbstractDoubleCursorFunctionFactory reports the position of the scalar subquery, its
            // SELECT keyword
            assertRawError(READ, policyPosition(MIN_X_POLICY, "SELECT min(x)"), "cannot compare DOUBLE and VARCHAR");

            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            assertQuery(READ).noLeakCheck().expectSize().returns(ALL_ROWS);
        });
    }

    @Test
    public void testRenameColumnReadByPolicySubquery() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithLookupPolicy(IN_LOOKUP_POLICY, LOOKUP_A_ROWS);
            execute("ALTER TABLE lookup RENAME COLUMN k TO k2");
            drainWalQueue();
            // the policy keeps naming the old column; SqlOptimiser.validateColumnAndGetModelIndex reports
            // the subquery's column reference
            final int kPosition = policyPosition(IN_LOOKUP_POLICY, "k FROM lookup");
            final String message = "Invalid column: k";
            assertRawError(READ, kPosition, message);

            // a refresh does not read the policy, so the view stays valid
            execute("INSERT INTO base VALUES ('C', 6.0, '2024-01-03T00:00:00Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            mv\tvalid\t
                            """);
            assertRawError(READ, kPosition, message);

            // renaming the column back makes the view readable under the policy again
            execute("ALTER TABLE lookup RENAME COLUMN k2 TO k");
            drainWalQueue();
            assertQuery(READ).noLeakCheck().returns(LOOKUP_A_ROWS + "C\t6.0\t2024-01-03T00:00:00.000000Z\n");
        });
    }

    @Test
    public void testRenameTableReadByPolicySubquery() throws Exception {
        assertMemoryLeak(() -> {
            createViewWithLookupPolicy(IN_LOOKUP_POLICY, LOOKUP_A_ROWS);
            execute("RENAME TABLE lookup TO lookup2");
            // the policy keeps naming the old table
            assertRawError(READ, policyPosition(IN_LOOKUP_POLICY, "lookup"), "table does not exist [table=lookup]");

            execute("RENAME TABLE lookup2 TO lookup");
            assertQuery(READ).noLeakCheck().returns(LOOKUP_A_ROWS);
        });
    }

    private static void assertRawError(String sql, int position, String message) throws Exception {
        try {
            assertExceptionNoLeakCheck(sql);
        } catch (SqlException e) {
            Assert.assertEquals(message, e.getFlyweightMessage().toString());
            Assert.assertEquals(position, e.getPosition());
        }
    }

    private static int policyPosition(String predicate, String token) {
        final int index = predicate.indexOf(token);
        Assert.assertTrue(index > -1);
        return POLICY_OFFSET + index;
    }

    private void createViewWithLookupPolicy(String predicate, String expectedRows) throws Exception {
        execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('A', 1.0, '2024-01-01T00:00:00Z'),
                ('A', 3.0, '2024-01-02T00:00:00Z'),
                ('B', 5.0, '2024-01-01T00:00:00Z'),
                ('B', 4.0, '2024-01-02T00:00:00Z')
                """);
        execute("CREATE TABLE lookup (k SYMBOL, x DOUBLE)");
        execute("INSERT INTO lookup VALUES ('A', 2.0)");
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)");
        drainWalAndMatViewQueues();
        execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + predicate);
        drainWalAndMatViewQueues();
        assertQuery(READ).noLeakCheck().returns(expectedRows);
    }
}
