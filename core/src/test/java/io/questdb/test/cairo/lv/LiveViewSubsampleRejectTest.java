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

import org.junit.Test;

/**
 * Pins the live-view rejection contract for the SUBSAMPLE family: the clause
 * itself and the row-selecting keep-flag window functions it desugars to
 * (lttb / m4 / minmax).
 * <p>
 * These functions are TWO_PASS whole-set selectors: pass1 buffers every
 * (ts, value) row, then the selection runs over the complete buffer. That is
 * structurally incompatible with a live view's incremental, bounded-influence
 * refresh model, and two independent rule sets enforce it:
 * <ul>
 *     <li>Their only supported window shape - the default (unbounded
 *     preceding) frame over {@code ORDER BY ts} - is turned away by the
 *     finite-influence rule ({@code SqlParser.validateLiveViewFiniteInfluence}),
 *     naming the function.</li>
 *     <li>Both remedies that reject suggests are unavailable for these
 *     functions specifically: a bounded frame trips "does not support framing"
 *     and an anchored named WINDOW (which requires PARTITION BY) trips "does
 *     not support PARTITION BY" in the function factory. There is no
 *     admissible live-view shape at all.</li>
 *     <li>The SUBSAMPLE clause never reaches either rule: the live-view shape
 *     validator rejects the desugared query model up front.</li>
 * </ul>
 * If any of these rejects disappears, a live view would silently compile a
 * whole-history downsampler into an incremental runtime - re-selecting (and
 * re-buffering) unbounded state per refresh cycle - so acceptance here must be
 * a deliberate design change, not a validation regression.
 */
public class LiveViewSubsampleRejectTest extends AbstractLiveViewTest {

    private static final String CREATE_PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS ";

    @Test
    public void testFiniteInfluenceRemediesUnavailableForKeepFlagFunctions() throws Exception {
        // The finite-influence reject suggests bounding the frame or anchoring the
        // window. Both are dead ends for the keep-flag functions: they reject framing
        // and PARTITION BY (which an ANCHOR requires) at the factory. Pinning both
        // proves no admissible live-view shape exists for lttb/m4/minmax.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertException(
                    CREATE_PREFIX +
                            "SELECT ts, v, lttb(ts, v, 100) OVER (ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS keep FROM base",
                    14,
                    "lttb() does not support framing; remove ROWS/RANGE clause"
            );
            assertException(
                    CREATE_PREFIX +
                            "SELECT ts, v, lttb(ts, v, 100) OVER w AS keep FROM base " +
                            "WINDOW w AS (PARTITION BY v ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))",
                    14,
                    "lttb() does not support PARTITION BY"
            );
        });
    }

    @Test
    public void testSubsampleClauseRejectedByLiveViewShapeValidator() throws Exception {
        // The SUBSAMPLE clause desugars into a window subquery + keep filter, which is
        // not the plain windowed base-table scan live views support. The shape
        // validator turns it away before the finite-influence rule ever sees the
        // desugared window function - for every method, including the position-only
        // ones (uniform/cadence) and sdt, which have no window-function form at all.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String[] methods = {
                    "lttb(v, 100)",
                    "lttb(v, 100, '1h')",
                    "m4(v, 100)",
                    "minmax(v, 100)",
                    "uniform(100)",
                    "cadence(100)",
                    "sdt(v, 0.5)"
            };
            for (String method : methods) {
                assertException(
                        CREATE_PREFIX + "SELECT ts, v FROM base SUBSAMPLE " + method,
                        17,
                        "live view select must be a plain windowed scan of the base table; this query shape is not supported yet"
                );
            }
        });
    }

    @Test
    public void testWindowKeepFlagFunctionsRejectedByFiniteInfluenceRule() throws Exception {
        // The only window shape these functions accept (default unbounded frame,
        // ORDER BY ts, no PARTITION BY) is exactly the shape the finite-influence
        // rule rejects, naming the function. Covers the gap-preserving lttb overload
        // too - same factory family, same frame shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String[] functions = {"lttb(ts, v, 100)", "lttb(ts, v, 100, '1h')", "m4(ts, v, 100)", "minmax(ts, v, 100)"};
            final String[] names = {"lttb", "lttb", "m4", "minmax"};
            for (int i = 0; i < functions.length; i++) {
                assertException(
                        CREATE_PREFIX + "SELECT ts, v, " + functions[i] + " OVER (ORDER BY ts) AS keep FROM base",
                        67,
                        "live view select cannot use " + names[i] + "() over a frame starting at UNBOUNDED PRECEDING; " +
                                "it has no finite out-of-order influence boundary, so a late row would replay the whole history"
                );
            }
        });
    }
}
