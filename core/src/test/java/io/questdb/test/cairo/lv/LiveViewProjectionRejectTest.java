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

package io.questdb.test.cairo.lv;

import org.junit.Before;
import org.junit.Test;

/**
 * What a live view still turns away once projections around the window are admitted, and
 * what it says when it does.
 * <p>
 * Admitting projections widened the accepted shape considerably, so the interesting
 * question moved from "is this rejected" to "is this rejected for the reason the author
 * can act on". Every message below is asserted verbatim-ish rather than merely asserting
 * that CREATE failed: #7479 was filed because a reject fired with a message that named the
 * wrong cause, and a test that only checks for failure cannot tell the two apart.
 * <p>
 * Grouped by why the reject exists, because the groups have different futures. The shape
 * rejects are the ones a later change could lift; the purity and timestamp rejects are
 * contract, and lifting them would make a view that cannot be recomputed.
 *
 * @see LiveViewProjectionTest
 * @see io.questdb.cairo.lv.LiveViewCompiledPlan
 */
public class LiveViewProjectionRejectTest extends AbstractLiveViewTest {

    private static final String FRAME = "PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING";

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testMultiPassWindowRejectSurvivesAProjection() throws Exception {
        // The multi-pass gate looks at the node the window is expected at. With a
        // projection on top, the cached factory sits one level down, and a root-only check
        // walks straight past it into the generic shape reject - trading a message that
        // says which capability is missing for one that does not. Each of these compiles
        // to a cached window under a projection.
        assertMemoryLeak(() -> {
            createBase();
            // A window ordered by something other than the designated timestamp.
            assertRejected(
                    "SELECT ts, sym, avg(px) OVER (PARTITION BY sym ORDER BY px ROWS 10 PRECEDING) + 1 AS c FROM base",
                    "requires caching or multi-pass evaluation"
            );
            // A descending scan over the designated timestamp.
            assertRejected(
                    "SELECT ts, sym, avg(px) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1 PRECEDING "
                            + "AND CURRENT ROW) + 1 AS c FROM base ORDER BY ts DESC",
                    "requires caching or multi-pass evaluation"
            );
        });
    }

    @Test
    public void testNonDeterministicProjectionIsRejected() throws Exception {
        // A live view's contents must be a pure function of the base table: the forward
        // drain never retracts a row, and an O3 or checkpoint replay recomputes ranges, so
        // a projection that cannot reproduce its own output would let the view diverge from
        // any recompute. The guard already covered the window arguments and the WHERE; it
        // covers the projections around the window on the same terms.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") + rnd_double() AS c FROM base",
                    "non-deterministic function cannot be used in live view: rnd_double"
            );
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") + now()::long AS c FROM base",
                    "non-deterministic function cannot be used in live view: now"
            );
            // A pre-window projection is held to the same standard.
            assertRejected(
                    "SELECT ts, sym, px * rnd_double() AS p2, avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "non-deterministic function cannot be used in live view: rnd_double"
            );
            // A bind variable has no value to reproduce at all: the view recompiles from
            // stored SQL on restart, with nothing bound.
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base WHERE px > $1",
                    "non-deterministic function cannot be used in live view"
            );
        });
    }

    @Test
    public void testProjectionOverAWindowFreeScanStillNamesTheMissingWindow() throws Exception {
        // The one case where the original message is the right one. A projection admits
        // nothing on its own - the view still needs a window function - and blaming the
        // projection here would repeat the mistake in the other direction.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px * 2 AS p2 FROM base",
                    "live view select must contain at least one window function"
            );
            assertRejected(
                    "SELECT ts, sym, px FROM base",
                    "live view select must contain at least one window function"
            );
        });
    }

    @Test
    public void testResultSetShapesAroundTheWindowNameTheClause() throws Exception {
        // A projection is a per-row transform, so the refresh path can rebuild it. These
        // are not: a limit, a sort and a grouping are properties of the whole result set,
        // and no per-row rebuild reproduces one. They stay rejected, but the message now
        // names the clause instead of claiming the window function is missing - which is
        // what sent the reporter of #7479 looking in the wrong place.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base LIMIT 10",
                    "LIMIT over a window function is not supported yet"
            );
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base ORDER BY sym DESC",
                    "ORDER BY over a window function is not supported yet"
            );
            assertRejected(
                    "SELECT ts, sym, sum(px - avg(px) OVER (" + FRAME + ")) AS c FROM base",
                    "GROUP BY over a window function is not supported yet"
            );
        });
    }

    @Test
    public void testShapesRejectedBeforeTheProjectionGateIsReached() throws Exception {
        // Turned away earlier than the shape walk, and pinned here so a later change to
        // that walk cannot silently start admitting one of them. The messages are not the
        // walk's own and are asserted as they stand rather than reworded.
        assertMemoryLeak(() -> {
            createBase();
            // A join is refused by the shape walk, but at the base-scan end rather than at
            // the window: the tree reaches a join factory where the leaf scan belongs.
            assertRejected(
                    "SELECT b.ts, b.sym, b.px - avg(b.px) OVER (PARTITION BY b.sym ORDER BY b.ts ROWS 10 PRECEDING) AS c "
                            + "FROM base b JOIN base b2 ON b.sym = b2.sym",
                    "live view select must be a simple scan of a single WAL base table"
            );
            // A subquery is refused by the parser's single-base-table rule.
            assertRejected(
                    "SELECT ts, sym, c FROM (SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base)",
                    "live view requires a single base table in FROM clause"
            );
            // DISTINCT over a window function is refused by the SQL compiler itself.
            assertRejected(
                    "SELECT DISTINCT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "Window function is not allowed in context of aggregation"
            );
            // A set operation reaches the shape walk with a union factory at the root,
            // whose base chain never passes through the window, so the walk cannot see the
            // window function and reports it missing. The message understates the cause;
            // it is pinned as it stands so a change to it is a deliberate one.
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base "
                            + "UNION ALL SELECT ts, sym, px FROM base",
                    "live view select must contain at least one window function"
            );
        });
    }

    @Test
    public void testTimestampMustSurviveTheProjection() throws Exception {
        // Every drain path stamps its rows with the view's designated timestamp, so a view
        // that projects none cannot refresh at all. Until projections were admitted, the
        // only way to lose it was to leave it out of the SELECT; now it can also be
        // computed away or shadowed by a derived column wearing its name. CREATE used to
        // accept all of these and hand back a view that faulted its way to INVALID on the
        // refresh worker, which is a worse answer than a reject: the operator has a table
        // that exists and never works.
        assertMemoryLeak(() -> {
            createBase();
            // Left out of the SELECT entirely - reachable with no projection at all, and
            // the spelling that predates them.
            assertRejected(
                    "SELECT sym, avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Left out of a projected SELECT.
            assertRejected(
                    "SELECT sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Computed, and aliased back to the base's own timestamp name. The output
            // column is called ts and is not the designated timestamp, so a message that
            // reported what it found would read "expected 'ts', got 'ts'".
            assertRejected(
                    "SELECT ts + 1 AS ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select must project the base table's designated timestamp 'ts' as a plain column"
            );
            // Carried through but renamed: a different reject, and one that can name both
            // sides because a designated timestamp does reach the output.
            assertRejected(
                    "SELECT ts AS t, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base",
                    "live view select cannot override the designated timestamp; expected 'ts', got 't'"
            );
        });
    }

    @Test
    public void testWindowContractRejectsSurviveAProjection() throws Exception {
        // The per-window-function contract runs on the window factory the walk found, so
        // wrapping the call in an expression must not route around it. Both of these are
        // about what the function keeps across rows, which arithmetic around its result
        // does not change.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, lead(px) OVER (" + FRAME + ") + 1 AS c FROM base",
                    "lead() is not supported in live views; use lag() for lookback"
            );
            assertRejected(
                    "SELECT ts, sym, row_number() OVER (PARTITION BY sym ORDER BY ts) + 1 AS c FROM base",
                    "live view unbounded window must have an ANCHOR clause"
            );
        });
    }

    @Test
    public void testDesignatedTimestampFilterRejectSurvivesAProjection() throws Exception {
        // A WHERE on the designated timestamp compiles into an interval scan whose
        // predicate lives in the frame cursor rather than in a residual filter Function.
        // The refresh path applies only the residual filter, so every base row would slip
        // through. The projection above the window does not change that.
        assertMemoryLeak(() -> {
            createBase();
            assertRejected(
                    "SELECT ts, sym, px - avg(px) OVER (" + FRAME + ") AS c FROM base WHERE ts > '2026-08-07'",
                    "live view select cannot filter on the designated timestamp yet"
            );
        });
    }

    private void assertRejected(String selectSql, String expectedMessage) throws Exception {
        assertQuery("CREATE LIVE VIEW lv_rejected FLUSH EVERY 1s START FROM NOW AS " + selectSql)
                .failsWith(expectedMessage);
    }

    private void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }
}
