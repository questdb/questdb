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

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Holds the two acceptance rules for an {@code EXPIRE ROWS} policy across every mode and every field
 * of a policy, rather than one field at a time.
 * <p>
 * The parser captures a policy's parts as raw text and the read filter drops them into a generated
 * query: the keep column into {@code max(<col>) OVER (...)}, the PARTITION BY list into that
 * {@code OVER} clause, a scalar {@code WHEN} predicate into a {@code NOT (...)} filter and a window one
 * into a projected CASE. Whatever a check does not
 * resolve therefore reaches the generated SQL as written. The compile-time probe cannot see all of
 * it, because it runs the query with {@code LIMIT 0} and so evaluates no row. Successive rounds of
 * review each found one more part that nothing resolved - a keep column with no {@code max()}, a
 * LONG256 keep column silently ranked by its low 64 bits, a PARTITION BY list that closed the
 * generated {@code OVER (} early and turned retention off. Each fix covered the part that was
 * reported.
 * <p>
 * So this test states the rules the parts have to satisfy, and applies them to a matrix:
 * <ul>
 *     <li>{@link #MUST_REJECT} - text that is not a policy must be refused at DDL time, on CREATE and
 *     on ALTER alike, with an error that names EXPIRE ROWS. Silently accepting it is how a policy
 *     ends up disabled while the catalogue still reports it.</li>
 *     <li>{@link #ANY_VERDICT} - a policy the grammar allows to be written. Either verdict is
 *     correct, and the test does not prescribe which: a mode may legitimately refuse a column type it
 *     cannot rank. What it may not do is accept the policy and leave a view that throws when read.
 *     Accepted means readable.</li>
 * </ul>
 * Adding a mode, a keep-column type or a new field to the policy encoding means adding a line here,
 * and both rules then apply to it without anyone having to decide in advance how it might break.
 * <p>
 * What these rules do not reach is a policy that is accepted, reads cleanly, and still keeps the
 * wrong rows - a keep column ranked through a narrowing cast, say. Deciding that needs an expected
 * keep-set worked out independently of {@code RowExpiryUtil}, which is what {@code RowExpiryFuzzTest}
 * maintains; the one such case known today, LONG256, is pinned in {@link #MUST_REJECT} instead.
 * <p>
 * Mutation-checked: reverting the PARTITION BY key check for either KEEP mode, the keep-column type
 * check, or the LONG256 rejection makes this test fail.
 */
public class RowExpiryPolicyAcceptanceTest extends AbstractCairoTest {

    /**
     * Policies the grammar allows. Either verdict is correct; accepting one means every read of the
     * resulting view must succeed.
     */
    private static final String[] ANY_VERDICT = {
            // keep column, one per type the bare form may or may not be able to rank
            "KEEP HIGHEST v PARTITION BY k",
            "KEEP LOWEST v PARTITION BY k",
            "KEEP HIGHEST i PARTITION BY k",
            "KEEP HIGHEST l PARTITION BY k",
            "KEEP HIGHEST ts2 PARTITION BY k",
            "KEEP HIGHEST dt PARTITION BY k",
            "KEEP HIGHEST s PARTITION BY k",
            "KEEP HIGHEST vc PARTITION BY k",
            "KEEP HIGHEST c PARTITION BY k",
            "KEEP HIGHEST sym PARTITION BY k",
            "KEEP HIGHEST b PARTITION BY k",
            "KEEP HIGHEST u PARTITION BY k",
            // the top-N form ranks instead of taking an extreme, so it accepts more types
            "KEEP 2 HIGHEST v PARTITION BY k",
            "KEEP 2 HIGHEST s PARTITION BY k",
            "KEEP 2 LOWEST sym PARTITION BY k",
            "KEEP 2 HIGHEST l256 PARTITION BY k",
            "KEEP 2 HIGHEST b PARTITION BY k",
            "KEEP 2 HIGHEST u PARTITION BY k",
            // no PARTITION BY at all, which the bare form allows and KEEP LATEST does not
            "KEEP HIGHEST v",
            "KEEP 3 HIGHEST v",
            // a quoted keep column and a quoted key
            "KEEP HIGHEST \"my val\" PARTITION BY k",
            "KEEP HIGHEST v PARTITION BY \"k\"",
            // composite key list
            "KEEP HIGHEST v PARTITION BY k, sym",
            // KEEP LATEST
            "KEEP LATEST PARTITION BY k",
            "KEEP LATEST ON ts PARTITION BY k",
            "KEEP LATEST PARTITION BY k, sym",
            // scalar and window WHEN predicates
            "WHEN v < 2.0",
            "WHEN v IS NULL",
            "WHEN s = 'x'",
            "WHEN ts < dateadd('d', -1, now())",
            "WHEN v < max(v) OVER (PARTITION BY k)",
            "WHEN row_number() OVER (PARTITION BY k ORDER BY ts DESC) > 2",
    };

    /**
     * Text that is not a policy. CREATE and ALTER must both refuse it.
     */
    private static final String[] MUST_REJECT = {
            // a PARTITION BY list that is not a column list: these close the generated OVER ( early,
            // so the keep filter becomes a constant and retention is silently off (or expires all)
            "KEEP HIGHEST v PARTITION BY k) AND (1=0",
            "KEEP HIGHEST v PARTITION BY k) OR (1=1",
            "KEEP LOWEST v PARTITION BY k) AND (1=0",
            "KEEP 2 HIGHEST v PARTITION BY k) AND (1=0",
            "KEEP LATEST PARTITION BY k) OR (1=1",
            // a window frame swallowed into the key list
            "KEEP HIGHEST v PARTITION BY k ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            // a name that is not a column, in each field of each mode
            "KEEP HIGHEST nosuch PARTITION BY k",
            "KEEP HIGHEST v PARTITION BY nosuch",
            "KEEP LOWEST v PARTITION BY nosuch",
            "KEEP 2 HIGHEST v PARTITION BY k, nosuch",
            "KEEP LATEST PARTITION BY nosuch",
            "KEEP LATEST ON nosuch PARTITION BY k",
            // KEEP LATEST ON must name the designated timestamp, not just any timestamp
            "KEEP LATEST ON ts2 PARTITION BY k",
            // predicate text that resolves to nothing, or to a non-boolean
            "WHEN nosuch > 1",
            "WHEN v",
            "WHEN nosuchfunc(v) > 1",
            // a comment would swallow the tail of the generated single-line query
            "WHEN v > 1 -- comment",
            // No window max()/min() takes LONG256, and LONG is its overload fallback, so accepting
            // this would rank rows by the low 64 bits of the value with every read still succeeding.
            // Revisit only alongside a window max()/min() that takes LONG256 as it stands.
            "KEEP HIGHEST l256 PARTITION BY k",
            "KEEP LOWEST l256 PARTITION BY k",
            // row counts the grammar rejects
            "KEEP 0 HIGHEST v PARTITION BY k",
            "KEEP -1 HIGHEST v PARTITION BY k",
    };

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testEveryAcceptedPolicyLeavesAReadableView() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            for (String clause : ANY_VERDICT) {
                final String rejection = createViewWith("mv_ok", clause);
                if (rejection != null) {
                    // refusing a policy is always allowed, but the message must say what it is about
                    Assert.assertTrue(
                            "rejected without naming EXPIRE ROWS: " + clause + "\n  " + rejection,
                            rejection.contains("EXPIRE ROWS")
                    );
                    continue;
                }
                // accepted, so every read of the view has to work
                final String readError = readError("mv_ok");
                Assert.assertNull("accepted a policy that leaves an unreadable view: " + clause
                        + "\n  " + readError, readError);
                execute("DROP MATERIALIZED VIEW mv_ok");
                drainWalAndMatViewQueues();
            }
        });
    }

    @Test
    public void testTextThatIsNotAPolicyIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW mv_alt AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            for (String clause : MUST_REJECT) {
                final String createRejection = createViewWith("mv_bad", clause);
                Assert.assertNotNull("CREATE accepted text that is not a policy: " + clause, createRejection);

                String alterRejection = null;
                try {
                    execute("ALTER MATERIALIZED VIEW mv_alt SET EXPIRE ROWS " + clause);
                } catch (Throwable e) {
                    alterRejection = message(e);
                }
                Assert.assertNotNull("ALTER accepted text that is not a policy: " + clause, alterRejection);
            }
        });
    }

    private static void createBase() throws Exception {
        execute("""
                CREATE TABLE base (
                    k SYMBOL, sym SYMBOL, v DOUBLE, i INT, l LONG, s STRING, vc VARCHAR, c CHAR,
                    l256 LONG256, b BOOLEAN, u UUID, dt DATE, "my val" DOUBLE, ts2 TIMESTAMP, ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        execute("""
                INSERT INTO base VALUES
                ('A', 'X', 1.0, 1, 1, 'a', 'a', 'a', '0x01', true, '11111111-1111-1111-1111-111111111111',
                 '2024-01-01', 1.0, '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                ('A', 'X', 3.0, 3, 3, 'c', 'c', 'c', '0x03', false, '33333333-3333-3333-3333-333333333333',
                 '2024-01-03', 3.0, '2024-01-03T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z'),
                ('B', 'Y', 2.0, 2, 2, 'b', 'b', 'b', '0x02', true, '22222222-2222-2222-2222-222222222222',
                 '2024-01-02', 2.0, '2024-01-02T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z'),
                ('C', 'Z', null, null, null, null, null, null, null, false, null, null, null, null,
                 '2024-01-04T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
    }

    /**
     * Creates a passthrough view carrying {@code clause}. Returns null when the DDL was accepted, or
     * the rejection message when it was not.
     */
    private static String createViewWith(String view, String clause) throws Exception {
        try {
            execute("CREATE MATERIALIZED VIEW " + view + " AS (SELECT * FROM base) EXPIRE ROWS " + clause);
        } catch (Throwable e) {
            return message(e);
        }
        drainWalAndMatViewQueues();
        return null;
    }

    private static String message(Throwable e) {
        final String message = e.getMessage();
        return message == null ? e.getClass().getName() : message;
    }

    /**
     * Reads every row and every column of {@code view}. Returns null when the read succeeded, or the
     * error otherwise - a policy whose generated filter does not resolve, or casts per row, fails
     * here rather than at DDL time.
     */
    private static String readError(String view) {
        try {
            printSql("SELECT * FROM " + view);
            printSql("SELECT count() FROM " + view);
        } catch (Throwable e) {
            return message(e);
        }
        return null;
    }
}
