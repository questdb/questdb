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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Semantic-validation rejects for CREATE LIVE VIEW that the grammar/shape validators
 * do not cover.
 * <p>
 * A live view is only well-defined if its result is a pure function of the base
 * table: the forward-append refresh emits survivors row by row and never retracts,
 * and an O3 or checkpoint replay recomputes ranges, so a non-deterministic function
 * anywhere in the body would let the view diverge from any recompute (permanently,
 * when it sits in a WHERE that admits or drops rows). CREATE therefore rejects
 * non-deterministic functions in the projection, the WHERE filter and window-function
 * arguments - the same guard materialized views arm around their SELECT. (The ANCHOR
 * EXPRESSION is validated separately by validateAnchorPurity and is not covered here.)
 */
public class LiveViewValidationTest extends AbstractCairoTest {

    @Test
    public void testCreateNameCollisionMessage() throws Exception {
        // A name already taken by a non-live-view (here a plain table) is rejected up
        // front, mirroring CREATE MATERIALIZED VIEW. Crucially, IF NOT EXISTS does NOT
        // silently no-op over a wrong-typed name: without the pre-check the shared create
        // helper would swallow the IF NOT EXISTS branch, leaving a user believing a live
        // view exists when the name is actually a plain table. A same-kind (live view)
        // IF NOT EXISTS collision stays a genuine no-op.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE lv (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Collision over the plain table, with and without IF NOT EXISTS - both reject.
            assertCreateLiveViewCollisionRejected(false);
            assertCreateLiveViewCollisionRejected(true);
            Assert.assertNull("no live view should be registered over the colliding name",
                    engine.getLiveViewRegistry().getViewInstance("lv"));

            // Free the name, then a real live view; IF NOT EXISTS over the SAME kind no-ops.
            execute("DROP TABLE lv");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            Assert.assertNotNull("IF NOT EXISTS over an existing live view must be a no-op",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateRejectedWhenLiveViewsDisabled() throws Exception {
        // Parity with materialized views (CreateMatViewTest#testCreateMatViewDisabled): when the
        // feature is turned off, CREATE is rejected at parse time rather than silently creating a
        // view that never refreshes - its state store is a no-op (NoOpLiveViewStateStore) and no
        // refresh workers are started, so a silently-accepted view would appear healthy while never
        // updating. The reject sits next to the identical materialized-view guard in SqlParser.
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_ENABLED, "false");
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected CREATE LIVE VIEW to be rejected when live views are disabled");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "live views are disabled")
                );
            }
            Assert.assertNull("no view should be created when the feature is disabled",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testCreateSameKindCollisionMessage() throws Exception {
        // A same-kind (live view) collision without IF NOT EXISTS reports the
        // specific "live view already exists" wording, mirroring CREATE MATERIALIZED
        // VIEW's "materialized view already exists" rather than the generic
        // "table exists" the shared create helper would otherwise surface. IF NOT
        // EXISTS over the same kind stays a no-op (covered by
        // testCreateNameCollisionMessage), so both wordings are locked.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected same-kind collision reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "live view already exists")
                );
            }
            // The original view survives the rejected re-create.
            Assert.assertNotNull("the pre-existing live view must be untouched",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDurationUnderscoreSeparators() throws Exception {
        // FLUSH EVERY / IN MEMORY durations accept '_' thousands separators, matching
        // mat-view strides, Numbers.parseLong, and the CLAUDE.md convention. The parsed
        // value is the underscore-free number; placement of the separators is validated
        // by parseLong, so leading / trailing / doubled '_' still fail closed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // A single-letter and a two-letter unit, both with an underscore, round-trip
            // to the plain numeric value (1_200s -> 1200s, 1_800s -> 1800s, both under the
            // 60-minute IN MEMORY cap; 1_500ms -> 1500ms exercises the "ms" unit path).
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1_200s IN MEMORY 1_800s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);
            Assert.assertEquals(1200L, lv.getDefinition().getFlushEveryInterval());
            Assert.assertEquals('s', lv.getDefinition().getFlushEveryIntervalUnit());
            Assert.assertEquals(1800L, lv.getDefinition().getInMemoryInterval());
            Assert.assertEquals('s', lv.getDefinition().getInMemoryIntervalUnit());
            execute("DROP LIVE VIEW lv");

            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1_500ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance lv2 = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertNotNull(lv2);
            Assert.assertEquals(1500L, lv2.getDefinition().getFlushEveryInterval());
            // 'T' is the millisecond unit char (see LiveViewDefinition.toMicros).
            Assert.assertEquals('T', lv2.getDefinition().getFlushEveryIntervalUnit());
            execute("DROP LIVE VIEW lv2");

            // Misplaced separators fail closed with the "invalid duration value" reject.
            assertInvalidDurationValueRejected("CREATE LIVE VIEW lv FLUSH EVERY _600s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertInvalidDurationValueRejected("CREATE LIVE VIEW lv FLUSH EVERY 3__600s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertInvalidDurationValueRejected("CREATE LIVE VIEW lv FLUSH EVERY 600_s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            Assert.assertNull("no view should survive a malformed-duration reject",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testRejectNonDeterministicFunctionInProjection() throws Exception {
        // The SELECT list rides the same compile-time guard as the WHERE filter and
        // the window arguments: a non-deterministic value projected into a live-view
        // column would diverge from any recompute on a re-refresh, O3 replay or
        // checkpoint restore, so CREATE must reject it up front.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertLiveViewCreateRejected("SELECT ts, x, rnd_double() AS r, row_number() OVER () AS rn FROM base", "rnd_double");
            assertLiveViewCreateRejected("SELECT ts, x, now() AS r, row_number() OVER () AS rn FROM base", "now");
            assertLiveViewCreateRejected("SELECT ts, x, systimestamp() AS r, row_number() OVER () AS rn FROM base", "systimestamp");
            assertLiveViewCreateRejected("SELECT ts, x, sysdate() AS r, row_number() OVER () AS rn FROM base", "sysdate");
        });
    }

    @Test
    public void testRejectNonDeterministicFunctionInWhere() throws Exception {
        // WHERE is the worst case: a row admitted on one random draw cannot be
        // un-emitted, so the row set diverges permanently from any recompute.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE v > rnd_double()", "rnd_double");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > now()", "now");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > systimestamp()", "systimestamp");
            assertLiveViewCreateRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE ts > sysdate()", "sysdate");
        });
    }

    @Test
    public void testRejectNonDeterministicFunctionInWindowArg() throws Exception {
        // Non-determinism nested in a window expression stays on the window fast path
        // and its argument compiles under the LV context, yielding timing-dependent
        // output; the guard must reach into the window-function argument too.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String frame = " OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base";
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + now()::long)" + frame, "now");
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + systimestamp()::long)" + frame, "systimestamp");
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + sysdate()::long)" + frame, "sysdate");
            assertLiveViewCreateRejected("SELECT ts, x, sum(v + rnd_double(0))" + frame, "rnd_double");
        });
    }

    @Test
    public void testRejectOutOfRangeDuration() throws Exception {
        // A duration whose micros overflow a long must be rejected up front
        // rather than silently narrowed. Before the fix toMicros cast the value
        // through an int (fromMinutes / fromHours / fromDays), so an out-of-range
        // value wrapped to a small one and slipped through instead of being
        // caught - 100000000000000000d overflows a long micros count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertDurationOutOfRangeRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100000000000000000d AS " +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertDurationOutOfRangeRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 100000000000000000d AS " +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base");
        });
    }

    @Test
    public void testRejectLagOverWideDecimalPartition() throws Exception {
        // lag(DECIMAL128/256) OVER a partitioned frame compiles to a function
        // without incremental-snapshot support (Decimal128/256LagOverPartitionFunction),
        // so CREATE LIVE VIEW must reject it up front: a window function that cannot
        // snapshot would make the refresh worker silently skip head checkpoints and
        // route every restart / O3 through a full head-miss replay. The narrower
        // DECIMAL64 lag over the same partitioned frame is snapshot-capable and is
        // accepted, so the reject is specific to the wide widths, not to lag itself.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, " +
                    "d64 DECIMAL(18, 6), d128 DECIMAL(38, 6), d256 DECIMAL(76, 6)) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");

            assertLagOverWideDecimalRejected("d128");
            assertLagOverWideDecimalRejected("d256");

            // DECIMAL64 (precision 18) stays on the snapshot-capable base function.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, lag(d64, 1) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            Assert.assertNotNull(
                    "DECIMAL64 lag over a partition must be accepted",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectNonWalBaseTable() throws Exception {
        // Incremental refresh drains the base table's WAL, and createLiveView assumes
        // a WAL base (CairoEngine relies on isWalTable). A BYPASS WAL base is rejected
        // at CREATE, with the position pointing at the base table name.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_nowal (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base_nowal";
            try {
                execute(createSql);
                // Should not reach here; drop defensively so a spurious success does
                // not leave a view that trips a later assertion on the same name.
                execute("DROP LIVE VIEW lv");
                Assert.fail("expected non-WAL base reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(), "base table must be a WAL table [name=base_nowal]")
                );
                final int pos = e.getPosition();
                Assert.assertTrue(
                        "position " + pos + " must point at the base table name in: " + createSql,
                        pos >= 0 && createSql.startsWith("base_nowal", pos)
                );
            }
        });
    }

    private void assertCreateLiveViewCollisionRejected(boolean ifNotExists) throws Exception {
        try {
            execute("CREATE LIVE VIEW " + (ifNotExists ? "IF NOT EXISTS " : "") +
                    "lv FLUSH EVERY 1s AS SELECT ts, x, row_number() OVER () AS rn FROM base");
            Assert.fail("expected name-collision reject [ifNotExists=" + ifNotExists + ']');
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + ", ifNotExists=" + ifNotExists + ']',
                    Chars.contains(e.getFlyweightMessage(), "table or view with the requested name already exists")
            );
        }
    }

    private void assertLagOverWideDecimalRejected(String col) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, lag(" + col + ", 1) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected wide-decimal lag reject for column " + col);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for column " + col,
                    Chars.contains(
                            e.getFlyweightMessage(),
                            "live view select cannot use window function lag(); incremental snapshot is not supported for this function yet"
                    )
            );
        }
    }

    private void assertDurationOutOfRangeRejected(String createSql) throws Exception {
        try {
            execute(createSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected out-of-range duration reject for: " + createSql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + createSql,
                    Chars.contains(e.getFlyweightMessage(), "live view duration is out of range")
            );
        }
    }

    private void assertInvalidDurationValueRejected(String createSql) throws Exception {
        try {
            execute(createSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected invalid-duration reject for: " + createSql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + createSql,
                    Chars.contains(e.getFlyweightMessage(), "invalid duration value")
            );
        }
    }

    private void assertLiveViewCreateRejected(String selectSql, String offendingToken) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " + selectSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected non-deterministic function reject for: " + selectSql);
        } catch (SqlException e) {
            // A CREATE LIVE VIEW reject names the live view - not "materialized view" -
            // and identifies the offending function token.
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + selectSql,
                    Chars.contains(
                            e.getFlyweightMessage(),
                            "non-deterministic function cannot be used in live view: " + offendingToken
                    )
            );
            // The position points at the offending token in the SELECT text (the LV
            // create compiles op.getSelectSql() directly, so positions are relative
            // to selectSql). Error Position Convention: point at the offending char.
            final int pos = e.getPosition();
            Assert.assertTrue(
                    "position " + pos + " must point at '" + offendingToken + "' in: " + selectSql,
                    pos >= 0 && selectSql.startsWith(offendingToken, pos)
            );
        }
    }
}
