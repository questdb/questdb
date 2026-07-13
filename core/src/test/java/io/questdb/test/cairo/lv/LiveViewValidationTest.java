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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

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
                // the error points at the LIVE keyword, not at char 0
                Assert.assertEquals(7, e.getPosition());
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
    public void testExplainCreateLiveViewReturnsAPlan() throws Exception {
        // The CREATE_LIVE_VIEW arm of compileExplainExecutionModel0 only authorized and broke, so
        // generateExplain() ran codegen over the RAW parser model. CREATE TABLE and CREATE MAT VIEW
        // both optimise theirs first. The unoptimised model tripped "wtf? ts" under -ea (an AIOOBE
        // without) - an Error escaping compile(), i.e. a 500 on HTTP/pgwire instead of a plan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String body = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS "
                    + "SELECT ts, x, row_number() OVER () AS rn FROM base";

            final StringSink plan = new StringSink();
            printSql("EXPLAIN " + body, plan);
            Assert.assertTrue("EXPLAIN must return a plan [plan=" + plan + ']', plan.length() > 0);
            Assert.assertTrue("the plan must mention the base table [plan=" + plan + ']',
                    Chars.contains(plan, "base"));

            // The JSON format goes through the same codegen and must not throw either.
            final StringSink jsonPlan = new StringSink();
            printSql("EXPLAIN (FORMAT JSON) " + body, jsonPlan);
            Assert.assertTrue("EXPLAIN (FORMAT JSON) must return a plan", jsonPlan.length() > 0);

            // EXPLAIN must not actually create the view.
            Assert.assertNull("EXPLAIN must not create the live view",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
        });
    }

    @Test
    public void testCreateLiveViewWritesDefinitionBeforeTxn() throws Exception {
        // _lv must land BEFORE _txn, like _view and _mv. _txn is what TableUtils.exists() keys on,
        // so writing _lv after it leaves a crash window whose directory looks like a plain WAL
        // table: the loader types it TABLE (no _lv) and it squats the view's name, so CREATE LIVE
        // VIEW then fails forever with "already exists" while DROP LIVE VIEW fails with "live view
        // name expected". Fail the _lv write and assert the directory never reaches TABLE_EXISTS.
        // Pin the ordering itself. A caught failure cannot stand in for the real hazard
        // (createLiveView rolls the orphan back on an exception; only a crash leaves it), so record
        // the order in which CREATE LIVE VIEW opens _lv and _txn.
        final AtomicBoolean armed = new AtomicBoolean(false);
        final StringBuilder order = new StringBuilder();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get()) {
                    if (Utf8s.endsWithAscii(name, '/' + LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME)) {
                        order.append("_lv ");
                    } else if (Utf8s.endsWithAscii(name, '/' + TableUtils.TXN_FILE_NAME)) {
                        order.append("_txn ");
                    }
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            armed.set(true);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            armed.set(false);

            final String seq = order.toString();
            Assert.assertTrue("CREATE LIVE VIEW must write both _lv and _txn [seq=" + seq + ']',
                    seq.contains("_lv") && seq.contains("_txn"));
            Assert.assertTrue(
                    "_lv must be written BEFORE _txn, else a crash between them leaves a directory"
                            + " that reports TABLE_EXISTS and squats the view's name [seq=" + seq + ']',
                    seq.indexOf("_lv") < seq.indexOf("_txn")
            );
            Assert.assertNotNull(engine.getLiveViewRegistry().getViewInstance("lv"));
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateMatViewOverLiveViewRejected() throws Exception {
        // CREATE LIVE VIEW rejects live-on-live composition, but a materialized view slipped
        // through: Type.LIVE_VIEW.isImplicitlyWal() is true, so an LV token answers isWal() and
        // sailed past CreateMatViewOperationImpl's only base-kind gate. That matters because a
        // mat view refreshes through the very apply pipeline that does not support an LV as a
        // base, and its refresh reads the LV through LiveViewRecordCursorFactory - which unions
        // the un-flushed in-memory tier - so it could materialise rows no LV WAL txn covers yet
        // and then record a lastRefreshBaseTxn behind them (non-deterministic w.r.t. flush timing).
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, a DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, a, sum(a) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base");

            final String sql = "CREATE MATERIALIZED VIEW mv AS (SELECT ts, avg(a) AS av FROM lv SAMPLE BY 1h) PARTITION BY DAY";
            try {
                execute(sql);
                Assert.fail("a live view must not be accepted as a materialized view base");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("live views are not allowed as base tables"));
                // The caret points at the base table name, not the statement head.
                Assert.assertEquals("the error must point at the base table name",
                        sql.indexOf("lv SAMPLE"), e.getPosition());
            }
            Assert.assertNull("no materialized view may be left behind", engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateTableOverLiveViewNameRejected() throws Exception {
        // The mirror image of testCreateNameCollisionMessage. CREATE TABLE's collision check
        // recognised regular and materialized views but not live views, so a live view fell
        // through it. Without IF NOT EXISTS that surfaced the generic "table already exists";
        // WITH IF NOT EXISTS the create silently no-opped, leaving a user believing a plain
        // table exists when the name is actually a live view - exactly the hazard
        // executeCreateLiveView already guards in the opposite direction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // IF NOT EXISTS first: it is the dangerous arm (a silent no-op, not a wrong message).
            assertCreateTableOverLiveViewRejected(true);
            assertCreateTableOverLiveViewRejected(false);

            // The live view survives both rejected creates and the name still resolves to it,
            // rather than to a freshly created plain table.
            Assert.assertNotNull("the live view must be untouched by the rejected CREATE TABLE",
                    engine.getLiveViewRegistry().getViewInstance("lv"));
            Assert.assertTrue("the name must still resolve to a live view",
                    engine.getTableTokenIfExists("lv").isLiveView());
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
    public void testRejectOrderByDesignatedTimestampDesc() throws Exception {
        // ORDER BY <designated ts> DESC produces no Sort factory - the planner elides it into a
        // backward page frame scan, so the tree keeps the shape the generic "simple scan" reject
        // looks for and CREATE used to accept it. Incremental refresh then drove rows in ascending
        // WAL arrival order, computing order-sensitive windows in the opposite order to the one
        // declared and persisting the result silently. Only the scan direction tells the two apart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // A window that declares no ORDER BY of its own takes the base scan order, so it stays
            // on the incremental fast path under a backward scan and reaches the new reject. This
            // is the whole exploitable surface: a window WITH an inner ORDER BY ts needs a cached
            // plan under a DESC scan and the incremental-refresh gate above already rejects it.
            assertOrderByDescRejected("SELECT ts, x, row_number() OVER () AS rn FROM base ORDER BY ts DESC");
            // Same through the residual-filter factory, which sits between window and page frame.
            assertOrderByDescRejected("SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 1 ORDER BY ts DESC");

            // The elision is what makes DESC dangerous. ORDER BY on a non-timestamp column, or on
            // the timestamp under an alias, keeps its Sort factory and hits a generic shape reject.
            assertLiveViewShapeRejected(
                    "SELECT ts, x, row_number() OVER () AS rn FROM base ORDER BY x DESC",
                    "live view select must contain at least one window function"
            );
            assertLiveViewShapeRejected(
                    "SELECT ts AS t, x, row_number() OVER () AS rn FROM base ORDER BY t DESC",
                    "live view select must be a simple scan of a single WAL base table"
            );

            // Ascending ORDER BY on the designated timestamp elides into the forward scan the
            // refresh path already drives, so it stays accepted - the reject must not widen to it.
            execute("CREATE LIVE VIEW lv_asc FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base ORDER BY ts ASC");
            execute("DROP LIVE VIEW lv_asc");
            execute("CREATE LIVE VIEW lv_default FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base ORDER BY ts");
            execute("DROP LIVE VIEW lv_default");
            execute("CREATE LIVE VIEW lv_anchor FLUSH EVERY 1s AS " +
                    "SELECT ts, x, avg(x) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)) ORDER BY ts ASC");
            execute("DROP LIVE VIEW lv_anchor");
        });
    }

    @Test
    public void testRejectWildcardProjection() throws Exception {
        // A live view freezes its output schema at CREATE but persists the SELECT text verbatim and
        // recompiles it whenever the base metadata drifts. Under a wildcard the recompiled
        // projection re-expands against the NEW base metadata, so a base ADD COLUMN - documented as
        // transparent, and deliberately not an invalidation trigger - silently widens the projection
        // past the frozen on-disk schema. In-process the cached row copier survives the recompile
        // (its cache key is the LV's own metadata version, which a base-side change never moves) and
        // writes the added base column into the slot of the column after it: silent corruption.
        // After a restart the copier is rebuilt against the wider source and the view instead dies
        // as "flush retry budget exhausted". Mat views never reach this because SAMPLE BY already
        // bans wildcards for exactly the same reason.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");

            assertLiveViewShapeRejected(
                    "SELECT *, row_number() OVER () AS rn FROM base",
                    "wildcard column select is not allowed in live view queries"
            );
            // A qualified wildcard expands identically, so it must be caught by the same gate.
            assertLiveViewShapeRejected(
                    "SELECT base.*, row_number() OVER () AS rn FROM base",
                    "wildcard column select is not allowed in live view queries"
            );

            // The gate reads the top-level projection, which is the one that fixes the view's
            // schema. Nothing else can smuggle a wildcard past it: a subquery in FROM is already
            // rejected outright (LiveViewSmokeTest#testRejectSubqueryInFrom), so the model reaching
            // the gate always projects straight off the single base table.
            //
            // An explicit column list naming exactly what "*" would have expanded to stays accepted
            // - the reject is about the wildcard re-expanding on a recompile, not about the columns.
            execute("CREATE LIVE VIEW lv_explicit FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv_explicit");
        });
    }

    @Test
    public void testRejectOutOfRangeDuration() throws Exception {
        // A duration whose micros overflow a long must be rejected up front
        // rather than silently narrowed. Before the fix toMicros cast the value
        // through an int (fromMinutes / fromHours / fromDays), so an out-of-range
        // value wrapped to a small one and slipped through instead of being
        // caught - 100_000_000_000_000_000d overflows a long micros count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertDurationOutOfRangeRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 100_000_000_000_000_000d AS " +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertDurationOutOfRangeRejected(
                    "CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 100_000_000_000_000_000d AS " +
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

    private void assertCreateTableOverLiveViewRejected(boolean ifNotExists) throws Exception {
        try {
            execute("CREATE TABLE " + (ifNotExists ? "IF NOT EXISTS " : "") +
                    "lv (ts TIMESTAMP, y LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            Assert.fail("expected CREATE TABLE over a live view name to be rejected [ifNotExists=" + ifNotExists + ']');
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + ", ifNotExists=" + ifNotExists + ']',
                    Chars.contains(e.getFlyweightMessage(), "live view with the requested name already exists")
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

    private void assertLiveViewShapeRejected(String selectSql, String expectedMessage) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " + selectSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected factory-shape reject for: " + selectSql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + selectSql,
                    Chars.contains(e.getFlyweightMessage(), expectedMessage)
            );
        }
    }

    private void assertOrderByDescRejected(String selectSql) throws Exception {
        try {
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " + selectSql);
            // Should not reach here; drop defensively so a spurious success does not
            // leave a view that trips the next assertion on the same name.
            execute("DROP LIVE VIEW lv");
            Assert.fail("expected ORDER BY DESC reject for: " + selectSql);
        } catch (SqlException e) {
            Assert.assertTrue(
                    "wrong message [msg=" + e.getFlyweightMessage() + "] for: " + selectSql,
                    Chars.contains(
                            e.getFlyweightMessage(),
                            "live view select cannot ORDER BY the designated timestamp in descending order"
                    )
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
