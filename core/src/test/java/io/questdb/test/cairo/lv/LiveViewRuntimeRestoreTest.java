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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairSession;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRebuildRestatementGuard;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The restore from the checkpoint timeline that a refreshing view runs in place of a whole-view
 * rebuild, when a base schema change or a mid-drain failure has cost it its accumulators.
 * <p>
 * Both recoveries leave the view's durable output correct and only its runtime wrong: a drift
 * freed the compiled factory, and a mid-drain failure fed rows the turn never committed. They used
 * to answer that by recomputing every retained row from the applied base and replacing the whole
 * output - a restatement the rebuild restatement guard refuses once the base has lost rows the
 * view retains, which stopped the view until a restart. The restore is the restart's own recovery
 * run in place: recompile, restore the newest compatible root, replay the base WAL above it up to
 * the applied watermark. It rewrites no output, so it has nothing to restate.
 * <p>
 * The view here keeps the checkpoint cadence at its default, which seals the first boundary when
 * the first row lands and nothing after it for the length of any case. So the newest root sits
 * well below the durable frontier, and every restore has base WAL to replay above it: a restore
 * that brought back the root alone would leave acct-1's day-two accumulation short, which the
 * expected rows would catch.
 * <p>
 * Every case ends on explicit rows and on a counter that tells the restore from the rebuild.
 * The rows alone could not: over a base that still holds every row, the rebuild reproduces them
 * exactly, so a restore that silently fell back would pass a row comparison.
 * <p>
 * Two of the cases - the ones that end in a parked repair - cover what a restore owes the turn it
 * runs in rather than what it brings back. A replay that meets an unresolved out-of-order commit
 * hands off to the out-of-order repair, and a localized repair there can park on the refresh
 * turn's budget - at which point it owns the runtime, and the turn has to end on it rather than
 * drain through accumulators the parked replay is standing half-way through. The refresh turn
 * checks for that twice, once after the restart restore and once after the running one, and the
 * two cases take one door each.
 * <p>
 * Two more cover the opposite question: what keeps a commit an <em>earlier</em> out-of-order
 * repair already resolved out of that replay gap. A repair advances the applied point over the
 * commit it rewrites, so a restorable generation left below that point would put the commit back
 * in the gap - and a restart would re-feed it from raw WAL and meet it out of order all over
 * again. A repair that truncates its timeline leaves exactly that generation behind until its
 * post-replay seal moves the coordinate, so the two cases take that repair with the seal failed
 * and with the seal left alone: the failed one must retire the prefix rather than leave it
 * addressable, and the sealed one must carry the repair's own coordinate.
 * <p>
 * The tie cases put an in-order commit on the newest root's own timestamp, after that root was
 * sealed. No seal can record it - a root only extends the timeline upwards - so every restore
 * has to replay it from the base WAL above the root, and a restore that skipped it would fail its
 * own row count and fall back to the rebuild, which the guard refuses over a base that lost rows,
 * unless the base has dedup keys: then the guard stands down and the rebuild drops those rows.
 * The newest root comes from the first flush in some of them and from the seed sweep in others,
 * and the last of them covers what the restore leaves for a resume anchored on that root. The
 * splice-tie cases then land a late row below the tie: the repair it triggers publishes above the
 * tie's commit, so it has to leave a newest root that holds the tie, whichever route it takes.
 * Four of them take that late row over a bounded ROWS or RANGE frame, which carries no anchor.
 * Two more carry a row above the tie in the late row's commit and restart while the repair is
 * parked: whatever that repair has published by then has to leave the tie restorable.
 */
public class LiveViewRuntimeRestoreTest extends AbstractLiveViewCheckpointCompatTest {
    // Brings the third and fourth account to a column sized for SMALL_ACCOUNT_SYMBOL_CAPACITY, past
    // the 0.8 auto-scale threshold, and collapses two of its rows on the dedup keys: the collapse
    // routes the drain through the applied base, whose reader the capacity growth moved on.
    private static final String CAPACITY_GROWING_COMMIT = "INSERT INTO tx (created_at, account_id, amount) VALUES "
            + "('2026-01-03T10:00:00.000000Z', 'acct-3', 30.0), "
            + "('2026-01-03T10:00:00.000000Z', 'acct-3', 64.0), "
            + "('2026-01-03T10:05:00.000000Z', 'acct-4', 1.0)";
    private static final String CAPACITY_GROWING_COMMIT_OUTPUT = """
            2026-01-03T10:00:00.000000Z\tacct-3\t64.0\t1
            2026-01-03T10:05:00.000000Z\tacct-4\t1.0\t1
            """;
    private static final String[] FOUR_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)"
    };
    private static final String SEVEN_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1
            2026-01-02T09:30:00.000000Z\tacct-1\t44.0\t3
            2026-01-02T09:40:00.000000Z\tacct-2\t80.0\t2
            """;
    // Day four's first row, the one the restart case leaves in the base unconsumed so the drain
    // the parked repair's check suppresses has work waiting behind it.
    private static final String DAY_FOUR_FIRST_ROW_OUTPUT =
            "2026-01-04T09:00:00.000000Z\tacct-1\t1.0\t1\n";
    private static final String DAY_FOUR_OUTPUT = DAY_FOUR_FIRST_ROW_OUTPUT
            + "2026-01-04T09:10:00.000000Z\tacct-1\t3.0\t2\n"
            + "2026-01-04T09:20:00.000000Z\tacct-1\t7.0\t3\n";
    // One commit per entry, and the last of them is the whole point: its rows are not in
    // timestamp order, every one of them sits above the frontier the commit before it left, and
    // two of them collide on the base's dedup keys.
    //
    // The collision is what routes the drain through the applied base rather than the raw WAL,
    // and the applied base's reader yields rows in timestamp order - so the view consumes the
    // commit with no out-of-order repair, and the default cadence seals no root over it. The raw
    // WAL under it still holds those rows in the order they arrived, which is what a later
    // restore's replay of the gap reads.
    private static final String[] O3_IN_THE_REPLAY_GAP = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-03T09:00:00.000000Z', 'acct-1', 8.0), ('2026-01-03T09:10:00.000000Z', 'acct-1', 16.0), "
                    + "('2026-01-03T09:20:00.000000Z', 'acct-1', 32.0)",
            "('2026-01-03T09:50:00.000000Z', 'acct-1', 64.0), ('2026-01-03T09:50:00.000000Z', 'acct-1', 65.0), "
                    + "('2026-01-03T09:40:00.000000Z', 'acct-1', 128.0)"
    };
    private static final String O3_GAP_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-03T09:00:00.000000Z\tacct-1\t8.0\t1
            2026-01-03T09:10:00.000000Z\tacct-1\t24.0\t2
            2026-01-03T09:20:00.000000Z\tacct-1\t56.0\t3
            2026-01-03T09:40:00.000000Z\tacct-1\t184.0\t4
            2026-01-03T09:50:00.000000Z\tacct-1\t249.0\t5
            """;
    private static final String[] SIX_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)",
            "('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)",
            "('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)"
    };
    // One commit per entry. The first seals the only root the default cadence writes, and the second
    // lands on that root's own timestamp after the seal.
    private static final String[] TIED_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:00:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)"
    };
    private static final String TIED_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:00:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            """;
    // The base the seed-root cases create their view over, in one commit. The seed sweep seals its
    // root on the last row.
    private static final String SEEDED_ROWS = "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), "
            + "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0), "
            + "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0), "
            + "('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)";
    // A commit on the seed root's own timestamp, and what the view holds with it.
    private static final String SEED_ROOT_TIE = "('2026-01-02T09:10:00.000000Z', 'acct-1', 16.0)";
    private static final String SEED_ROOT_TIE_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-02T09:10:00.000000Z\tacct-1\t28.0\t3
            """;
    // The base the splice-tie cases create their view over, in one commit: a row on each of three
    // days. The seed sweep seals its root on the third day's row.
    private static final String SPLICE_SEEDED_ROWS = "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), "
            + "('2026-01-02T09:00:00.000000Z', 'acct-1', 2.0), "
            + "('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)";
    // A commit on that seed root's own timestamp, and what the view holds with it.
    private static final String SPLICE_ROOT_TIE = "('2026-01-03T09:00:00.000000Z', 'acct-2', 8.0)";
    private static final String SPLICE_ROOT_TIE_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t2.0\t1
            2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-03T09:00:00.000000Z\tacct-2\t8.0\t1
            """;
    // A late row in the second day, a closed segment below the frontier, and what the view holds
    // with it.
    private static final String SPLICE_LATE_ROW = "('2026-01-02T10:00:00.000000Z', 'acct-1', 16.0)";
    private static final String SPLICE_LATE_ROW_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t2.0\t1
            2026-01-02T10:00:00.000000Z\tacct-1\t18.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-03T09:00:00.000000Z\tacct-2\t8.0\t1
            """;
    // The splice-tie cases over a bounded frame, which carries no anchor. An hour-wide RANGE frame
    // carries the late row into no other row, so a repair of it quoted the runtime frontier would
    // converge below the tie.
    private static final String SPLICE_RANGE_FRAME = "RANGE BETWEEN 1 HOUR PRECEDING AND CURRENT ROW";
    private static final String SPLICE_RANGE_ROWS_BELOW_THE_LATE_ROW = """
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0
            2026-01-02T09:00:00.000000Z\tacct-1\t2.0
            """;
    private static final String SPLICE_RANGE_ROWS_FROM_THE_LATE_ROW = """
            2026-01-02T10:00:00.000000Z\tacct-1\t18.0
            2026-01-03T09:00:00.000000Z\tacct-1\t4.0
            2026-01-03T09:00:00.000000Z\tacct-2\t8.0
            """;
    // A ROWS frame reaching one row back carries the late row into acct-1's next row, so two more
    // acct-1 rows on the second day would let a repair of it quoted the runtime frontier converge
    // on that day, below the tie.
    private static final String SPLICE_ROWS_FRAME = "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW";
    private static final String SPLICE_ROWS_SEEDED_ROWS = "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), "
            + "('2026-01-02T09:00:00.000000Z', 'acct-1', 2.0), "
            + "('2026-01-02T11:00:00.000000Z', 'acct-1', 32.0), "
            + "('2026-01-02T12:00:00.000000Z', 'acct-1', 64.0), "
            + "('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)";
    private static final String SPLICE_ROWS_ROWS_BELOW_THE_LATE_ROW = """
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0
            2026-01-02T09:00:00.000000Z\tacct-1\t3.0
            """;
    private static final String SPLICE_ROWS_ROWS_FROM_THE_LATE_ROW = """
            2026-01-02T10:00:00.000000Z\tacct-1\t18.0
            2026-01-02T11:00:00.000000Z\tacct-1\t48.0
            2026-01-02T12:00:00.000000Z\tacct-1\t96.0
            2026-01-03T09:00:00.000000Z\tacct-1\t68.0
            2026-01-03T09:00:00.000000Z\tacct-2\t8.0
            """;
    // ANCHOR DAILY resets each account's accumulators at midnight.
    private static final String SIX_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // One row below the frontier the six above leave, and above the only root the default
    // cadence sealed - so the repair it triggers has a prefix under it the truncate can keep,
    // and rows over it to re-emit.
    private static final String CORRECTION_COMMIT =
            "INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T09:05:00.000000Z', 'acct-1', 64.0)";
    private static final String CORRECTED_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:05:00.000000Z\tacct-1\t68.0\t2
            2026-01-02T09:10:00.000000Z\tacct-1\t76.0\t3
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            2026-01-03T09:10:00.000000Z\tacct-2\t32.0\t1
            """;
    // One commit per entry, over a deduplicating base. The fourth carries a duplicate the dedup
    // keys collapse into its last row: the view's drain of the applied base sees one row, and a
    // restore's replay of the raw WAL above the first root feeds both, so the restore's own check
    // refuses it and the recovery falls back to the rebuild.
    private static final String[] COLLAPSED_DUPLICATE_ROWS = {
            "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
            "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
            "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
            "('2026-01-02T09:10:00.000000Z', 'acct-1', 7.0), ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)",
            "('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)"
    };
    private static final String COLLAPSED_DUPLICATE_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            """;
    // What a rebuild from the applied base derives from COLLAPSED_DUPLICATE_ROWS once the base has
    // lost day one: the view's rows of that day go with it.
    private static final String COLLAPSED_DUPLICATE_RESTATED_OUTPUT = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
            2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
            2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
            """;
    // The errno the fault's failed WAL read reports: EIO, a read error that may clear on a retry.
    // Not a lost file, which an exhausted budget would re-derive the view from, and not a breach
    // of the view's memory limit, which invalidates at once.
    private static final int ERRNO_EIO = 5;
    // How a log line reports the WAL read the fault fails.
    private static final String EIO_READ_ERROR_RE = "error=io\\.questdb\\.cairo\\.CairoException: \\["
            + ERRNO_EIO
            + "] could not open read-only \\[file=[^\\]]*created_at\\.d";
    private static final String RESTORED = "live view restored its runtime from the checkpoint timeline";
    // Logged by a whole-view rebuild that runs without the restatement guard.
    private static final String GUARD_STAND_DOWN = "live view rebuild from the applied base runs without the restatement guard";
    // Logged by the rebuild a deduplicating base's restore falls back to when its replay of the raw
    // WAL does not reproduce the view's durable output.
    private static final String DEDUP_RESTORE_MISMATCH_STAND_DOWN = GUARD_STAND_DOWN + " [view=lv, reason=dedup restore mismatch]";
    // Logged once per tier rebuild or empty-lead drain that takes back symbol ids a discarded
    // turn interned and no flush committed. Plain text, so it reads the same as a regex.
    private static final String SYMBOL_IDS_REWOUND = "live view rewound symbol ids no flush committed";
    // Logged by a flush that finds its lead's symbol ids out of step with the ids the view's
    // table committed, and serves the flushed rows from disk. Plain text, as above.
    private static final String SYMBOL_IDS_OUT_OF_STEP = "live view symbol ids are out of step with the committed symbols, serving flushed rows from disk";
    // Logged by a disk-subset publish that finds the same, and rebuilds the slot from disk.
    private static final String SYMBOL_IDS_OUT_OF_STEP_REBUILT = "live view symbol ids are out of step with the committed symbols, rebuilding the in-mem tier from disk";
    // How many times the stuck-rebuild case re-drives the fault; far more than the budget below
    // allows, so only a turn that goes uncharged can keep the view running through all of them.
    private static final int STUCK_REBUILD_MAX_DRIVES = 16;
    // The flush-retry count the stuck cases set. It is below the charged turns the duration budget
    // below allows, so a view those turns invalidate shows the count played no part: a fault the
    // recovery answered without moving the view is charged to the duration budget alone.
    private static final int STUCK_REBUILD_RETRY_MAX = 3;
    // The flush-retry duration a fault the view cannot get past spends before the view invalidates.
    private static final long STUCK_REBUILD_RETRY_MAX_DURATION_MICROS = 4 * CLOCK_ADVANCE_MICROS;
    // The charged turns that duration allows, each at the retry deadline the one before it armed:
    // the first starts the clock, and the first one at or past a whole duration later exhausts it.
    private static final int STUCK_REBUILD_CHARGED_TURNS = refreshRetryTurnsUntilDurationExhausts(STUCK_REBUILD_RETRY_MAX_DURATION_MICROS);
    // How many turns in a row the transient mid-drain fault fails: twice the default count budget,
    // each at the retry deadline the one before it armed, and inside the default duration budget.
    private static final int TRANSIENT_FAULT_TURNS = 10;
    private static final int SMALL_ACCOUNT_SYMBOL_CAPACITY = 4;
    // A bounded ROWS frame partitioned by a single SYMBOL column: over an indexed account column,
    // an out-of-order repair seeks each account's dependency floor through the base index.
    private static final String BOUNDED_ROWS_WINDOW =
            "sum(amount) OVER (PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
    // Sized for the eight accounts the indexed-account seed brings, so the six more the index
    // drop case adds take it past the 0.8 auto-scale threshold.
    private static final int INDEXED_ACCOUNT_SYMBOL_CAPACITY = 16;
    private static final String VIEW_ROWS_QUERY = "SELECT created_at, account_id, cumulative_sum, cumulative_count FROM lv";
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpClock() {
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testABaseSchemaChangeRestoresTheRuntimeFromTheTimeline() throws Exception {
        assertMemoryLeak(() -> {
            // A deduplicating base, because its drain reads the applied base through the
            // compiled factory, and that is where a base metadata change surfaces as drift.
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(SIX_ROWS);
            Assert.assertEquals("the default cadence seals the first boundary only", 1, countSealedBoundaries("lv"));

            // A schema change the view survives, then a commit the base collapses into one row:
            // the collapse routes the drain through the applied base, whose reader the view's
            // compiled plan now predates.
            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            // The restore replayed the five rows above the first root, and nothing rebuilt.
            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            capture.assertNotLogged("live view recomputed window state from applied base");
            capture.assertNotLogged("could not restore its runtime");
            Assert.assertEquals(
                    "no whole-view rebuild may have run",
                    LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED,
                    guard.getAbstention()
            );
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());

            // The commit that met the drift is materialized by the recompiled runtime, on top of
            // the day-three accumulation the restore put back.
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");

            // The ladder the restore stood on is the one a restart reads, and it agrees.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertNoRefreshFaults("lv");
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    @Test
    public void testABaseSymbolCapacityGrowthKeepsTheRuntime() throws Exception {
        assertMemoryLeak(() -> {
            // The drain above, through the applied base, over an account column sized for four
            // keys. The commit below brings the third and fourth account, which crosses the
            // auto-scale threshold, so its apply doubles the column's capacity and moves the base
            // metadata version with nothing else in the schema changed. The compiled plan depends
            // on none of it: the capacity rebuild keeps every symbol key, and the base reader
            // reopens its symbol map in place.
            createBaseWithAccountCapacity(SMALL_ACCOUNT_SYMBOL_CAPACITY);
            createView();
            insertAndRefresh(SIX_ROWS);
            Assert.assertEquals(SMALL_ACCOUNT_SYMBOL_CAPACITY, baseAccountSymbolCapacity());

            execute(CAPACITY_GROWING_COMMIT);
            drainWalQueue();
            Assert.assertEquals(
                    "the commit must have grown the base symbol capacity",
                    2 * SMALL_ACCOUNT_SYMBOL_CAPACITY,
                    baseAccountSymbolCapacity()
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // No drift, so no fault, no recompile and no restore: the runtime that counted
            // day three keeps counting.
            assertNoRefreshFaults("lv");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals("a capacity growth must not restore the runtime", 0, instance.getCheckpointRuntimeRestores());
            capture.drain();
            capture.assertNotLogged("base table metadata change");
            assertViewRows(SIX_ROWS_OUTPUT + CAPACITY_GROWING_COMMIT_OUTPUT);
        });
    }

    @Test
    public void testABaseSymbolCapacityGrowthBesideASchemaChangeStillRestores() throws Exception {
        assertMemoryLeak(() -> {
            // The capacity growth above, in the same backlog as a schema change the view
            // survives. The growth must not hide the schema change: the plan is stale, so the
            // drift recovers it exactly as it does without the growth.
            createBaseWithAccountCapacity(SMALL_ACCOUNT_SYMBOL_CAPACITY);
            createView();
            insertAndRefresh(SIX_ROWS);

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute(CAPACITY_GROWING_COMMIT);
            drainWalQueue();
            Assert.assertEquals(
                    "the commit must have grown the base symbol capacity",
                    2 * SMALL_ACCOUNT_SYMBOL_CAPACITY,
                    baseAccountSymbolCapacity()
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(SIX_ROWS_OUTPUT + CAPACITY_GROWING_COMMIT_OUTPUT);
        });
    }

    @Test
    public void testABaseSymbolCapacityGrowthBesideAStructuralNoOpStillRestores() throws Exception {
        assertMemoryLeak(() -> {
            // The capacity growth beside a structural statement that leaves the metadata as it
            // was: the dedup keys the base already has, enabled again. Nothing but the capacity
            // differs in the metadata, yet the statement moved the column structure version,
            // which a capacity change never does, so the change is not capacity alone.
            createBaseWithAccountCapacity(SMALL_ACCOUNT_SYMBOL_CAPACITY);
            createView();
            insertAndRefresh(SIX_ROWS);

            final int columnStructureVersion;
            try (TableReader reader = getReader("tx")) {
                columnStructureVersion = reader.getTxFile().getColumnStructureVersion();
            }
            execute("ALTER TABLE tx DEDUP ENABLE UPSERT KEYS(created_at, account_id)");
            execute(CAPACITY_GROWING_COMMIT);
            drainWalQueue();
            try (TableReader reader = getReader("tx")) {
                Assert.assertNotEquals(
                        "the statement must have moved the column structure version",
                        columnStructureVersion,
                        reader.getTxFile().getColumnStructureVersion()
                );
            }
            Assert.assertEquals(
                    "the commit must have grown the base symbol capacity",
                    2 * SMALL_ACCOUNT_SYMBOL_CAPACITY,
                    baseAccountSymbolCapacity()
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(SIX_ROWS_OUTPUT + CAPACITY_GROWING_COMMIT_OUTPUT);
        });
    }

    @Test
    public void testABaseSymbolCapacityGrowthBesideASymbolCacheChangeStillRestores() throws Exception {
        assertMemoryLeak(() -> {
            // The capacity growth beside a change to the attribute that sits next to the capacity
            // on the same column. NOCACHE, like the growth, moves the metadata version and leaves
            // the column structure version alone, and the growth supplies the capacity change
            // the exemption asks for. So only the cache flag in the plan's metadata snapshot
            // tells this backlog from a capacity change alone, and it must keep it drift.
            createBaseWithAccountCapacity(SMALL_ACCOUNT_SYMBOL_CAPACITY);
            createView();
            insertAndRefresh(SIX_ROWS);

            final int columnStructureVersion = baseColumnStructureVersion();
            execute("ALTER TABLE tx ALTER COLUMN account_id NOCACHE");
            execute(CAPACITY_GROWING_COMMIT);
            drainWalQueue();
            Assert.assertEquals(
                    "NOCACHE must not have moved the column structure version",
                    columnStructureVersion,
                    baseColumnStructureVersion()
            );
            Assert.assertEquals(
                    "the commit must have grown the base symbol capacity",
                    2 * SMALL_ACCOUNT_SYMBOL_CAPACITY,
                    baseAccountSymbolCapacity()
            );
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(SIX_ROWS_OUTPUT + CAPACITY_GROWING_COMMIT_OUTPUT);
        });
    }

    @Test
    public void testABaseSymbolCapacityGrowthBesideAnIndexDropStillRestores() throws Exception {
        assertMemoryLeak(() -> {
            // A bounded ROWS view partitioned by an indexed account column. An out-of-order
            // repair finds each account's dependency floor through the base index, opening the
            // base at the metadata version the view's plan compiled against. The index drop
            // below moves that version and leaves the column structure version alone, as a
            // capacity change does, and the commit after it grows the column's capacity. The
            // in-order drain reads the raw WAL, so neither reaches the plan until a late row
            // sends the repair to its indexed seek. A reader served there has no index on the
            // column and fails the seek on every retry until the view invalidates. The index
            // type in the plan's metadata snapshot keeps the backlog drift, so the view
            // recompiles over the unindexed column and restores its runtime instead.
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL CAPACITY " + INDEXED_ACCOUNT_SYMBOL_CAPACITY
                    + " INDEX, amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + indexedAccountSeed());
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, amount, " + BOUNDED_ROWS_WINDOW + " AS s FROM tx");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertNoRefreshFaults("lv");
                final LiveViewInstance instance = instance("lv");
                Assert.assertTrue(
                        "the plan must seek the dependency floor through the account index",
                        instance.getCompiledPlan().getPageFrameFactory().isIndexedBackwardTimestampRangeSupported(1)
                );

                final int columnStructureVersion = baseColumnStructureVersion();
                execute("ALTER TABLE tx ALTER COLUMN account_id DROP INDEX");
                // Six new accounts, 14 in all, past 0.8 of the capacity, above the frontier.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                        + "('2026-01-02T00:07:09.000000Z', 'acct-9', 1.0), "
                        + "('2026-01-02T00:07:10.000000Z', 'acct-10', 1.0), "
                        + "('2026-01-02T00:07:11.000000Z', 'acct-11', 1.0), "
                        + "('2026-01-02T00:07:12.000000Z', 'acct-12', 1.0), "
                        + "('2026-01-02T00:07:13.000000Z', 'acct-13', 1.0), "
                        + "('2026-01-02T00:07:14.000000Z', 'acct-14', 1.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "DROP INDEX must not have moved the column structure version",
                        columnStructureVersion,
                        baseColumnStructureVersion()
                );
                Assert.assertEquals(
                        "the commit must have grown the base symbol capacity",
                        2 * INDEXED_ACCOUNT_SYMBOL_CAPACITY,
                        baseAccountSymbolCapacity()
                );
                assertNoRefreshFaults("lv");
                Assert.assertEquals("the in-order drain must not have met the drift", 0, instance.getCheckpointRuntimeRestores());
                Assert.assertTrue(
                        "the plan compiled over the index must still be in place",
                        instance.getCompiledPlan().getPageFrameFactory().isIndexedBackwardTimestampRangeSupported(1)
                );

                // acct-1 between its 21st and 22nd rows, with 19 rows of its own above it.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T00:03:25.000000Z', 'acct-1', 1000.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertFalse("the view must stay valid", instance.isInvalid());
                capture.drain();
                capture.assertNotLogged("Not indexed");
                capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, ");
                Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
                Assert.assertEquals("the drift must have restored the runtime", 1, instance.getCheckpointRuntimeRestores());
                Assert.assertFalse(
                        "the recompiled plan must no longer seek through the dropped index",
                        instance.getCompiledPlan().getPageFrameFactory().isIndexedBackwardTimestampRangeSupported(1)
                );
                assertBoundedRowsViewMatchesRecompute();

                // The recovered view keeps materializing.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                        + "('2026-01-02T00:08:00.000000Z', 'acct-1', 7.0), "
                        + "('2026-01-02T00:08:00.000000Z', 'acct-9', 8.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertFalse(instance.isInvalid());
                Assert.assertEquals("no fault after the recovery", 1, instance.getRefreshFaultCount());
                assertBoundedRowsViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testABaseMetadataRewriteWithNoChangeStillRestores() throws Exception {
        assertMemoryLeak(() -> {
            // A parameter set to the value it already has rewrites the base metadata and moves
            // its version with nothing in it changed. The capacity exemption asks for a capacity
            // that actually moved, so a version that moved with no visible change stays drift.
            createBaseWithAccountCapacity(SMALL_ACCOUNT_SYMBOL_CAPACITY);
            createView();
            insertAndRefresh(SIX_ROWS);

            final int maxUncommittedRows;
            final long metadataVersion;
            try (TableReader reader = getReader("tx")) {
                maxUncommittedRows = reader.getMetadata().getMaxUncommittedRows();
                metadataVersion = reader.getMetadataVersion();
            }
            execute("ALTER TABLE tx SET PARAM maxUncommittedRows = " + maxUncommittedRows);
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (TableReader reader = getReader("tx")) {
                Assert.assertEquals("the parameter must be unchanged", maxUncommittedRows, reader.getMetadata().getMaxUncommittedRows());
                Assert.assertTrue("the rewrite must have moved the metadata version", reader.getMetadataVersion() > metadataVersion);
            }
            Assert.assertEquals(SMALL_ACCOUNT_SYMBOL_CAPACITY, baseAccountSymbolCapacity());
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=base table metadata change, .*replayedRows=5]");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    @Test
    public void testADriftWhoseRecoveryFailsLeavesTheDebtForTheNextTurn() throws Exception {
        final String[] baseDir = new String[1];
        final AtomicBoolean failTimelineOpen = new AtomicBoolean();
        final AtomicBoolean failBaseColumnOpen = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // The rebuild's first read of a base partition column - its probe, which runs
                // before it wipes the runtime.
                if (failBaseColumnOpen.get()
                        && baseDir[0] != null
                        && Utf8s.containsAscii(name, baseDir[0])
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.endsWithAscii(name, ".d")) {
                    failBaseColumnOpen.set(false);
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                // The restore's first open of the timeline, which maps its superblock.
                if (failTimelineOpen.get() && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME)) {
                    failTimelineOpen.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            baseDir[0] = engine.verifyTableName("tx").getDirName();
            insertAndRefresh(SIX_ROWS);

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            // Both recoveries of the drift turn fail. The drift freed the factory before either
            // ran, and the rebuild's probe fails before its wipe, so neither recovery marks the
            // runtime it leaves behind: the drift itself has to. A later turn that drained
            // through that runtime would count acct-1's day three from nothing - 64.0 over one
            // row instead of 80.0 over two.
            failTimelineOpen.set(true);
            failBaseColumnOpen.set(true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse("the restore's timeline read must have been failed", failTimelineOpen.get());
            Assert.assertFalse("the rebuild's probe must have been failed", failBaseColumnOpen.get());
            assertViewRows(SIX_ROWS_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");

            capture.drain();
            capture.assertLogged("live view could not restore its runtime from the checkpoint timeline");
            capture.assertLogged("live view window-state recompute failed");
            // The next turn's gate took the debt and restored, now that nothing fails.
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertFalse(instance.isInvalid());
        });
    }

    @Test
    public void testAGateRestoreWhoseParkedRepairEndsTheTurn() throws Exception {
        // The same disposition reached from the running door. The gate every turn opens at once
        // the view owes its accumulators a recovery runs the same restore, over the same replay
        // gap, and parks the same repair - so it ends its turn the same way.
        //
        // Both recoveries of the failing turn have to fail for the debt to reach a turn of its
        // own: a restore that succeeded would settle it, and so would the rebuild behind it. The
        // failures are one-shot, so the gate turn that follows them runs against an intact tree.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            assertViewRows(O3_GAP_OUTPUT);
            final LiveViewInstance instance = instance("lv");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Commits the collapse above left provably clean, so the view drains them through
                // the raw WAL and the fault can strike between two of them. The first goes in on
                // its own turn; the next two coalesce behind it and drain in one pass, which is
                // what puts the failure after a row this turn has already fed.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                runOnePass(job);
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);

                fault.arm(1);
                fault.armTimelineOpen();
                fault.armAppliedScan();
                runOnePass(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertFalse("the recovery's restore must have been failed", fault.isTimelineOpenArmed());
                Assert.assertTrue("the recovery's rebuild must have been failed", fault.hasAppliedScanFired());
                Assert.assertTrue(
                        "the failed recovery must leave the window-state debt for the next turn",
                        instance.isWindowStateDirty()
                );
                Assert.assertNull("nothing may park while both recoveries fail", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the failed restore must have brought nothing back",
                        0,
                        instance.getCheckpointRuntimeRestores()
                );
                final long watermarkBeforeTheGate = instance.getLastProcessedSeqTxn();

                // The gate turn. Its restore runs now that nothing fails, meets the same
                // out-of-order commit in the replay gap, and parks the repair it hands off to.
                runOnePass(job);
                Assert.assertNotNull(
                        "the gate's restore must leave the repair it handed off to parked on the view",
                        instance.getSuspendedRepair()
                );
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLoggedRE("live view O3 repair yielded on its turn budget \\[view=lv, turns=1,");
                Assert.assertEquals(
                        "the repair must have come out of the gate's own in-process restore",
                        1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals(
                        "the parked repair owns the runtime, so the gate must not have let the drain run",
                        watermarkBeforeTheGate,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertTrue(
                        "the debt belongs to the repair until it finishes",
                        instance.isWindowStateDirty()
                );
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);

                driveRefreshToQuiescence(job);
            }

            // The repair finishes across the turns after it and the view converges on every row,
            // the three commits the fault interrupted included.
            Assert.assertNull(instance.getSuspendedRepair());
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the injected mid-drain failure is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainRestoreWhoseParkedRepairMeetsAnExhaustedBudgetIsDiscarded() throws Exception {
        // The running door once more, reached from the failing turn itself: its recovery's
        // restore runs, meets the same out-of-order commit in the replay gap and parks the repair
        // it hands off to. The recovery does not settle the fault, so the turn charges the
        // flush-retry budget for it - the duration budget, since the restore left the view in
        // front of the commits the fault stopped - and a duration budget of zero runs out on this
        // very turn. The fault reads as a lost base WAL segment, so the exhausted budget
        // re-derives the view from the applied base. That re-derive rebuilds the runtime the
        // parked repair stands in and rewrites the output its replacement stands over, so the
        // repair must be gone before it runs, and nothing may resume it afterwards.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, 0);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            fault.reportReadErrno(CairoException.ERRNO_FILE_DOES_NOT_EXIST);
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            assertViewRows(O3_GAP_OUTPUT);
            final LiveViewInstance instance = instance("lv");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in the gate case: the first commit drains on its own turn, and the next two
                // coalesce behind it so the fault strikes after this turn has fed a row.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                runOnePass(job);
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);

                fault.arm(1);
                runOnePass(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                capture.drain();
                capture.assertLoggedRE("live view O3 repair yielded on its turn budget \\[view=lv, turns=1,");
                capture.assertLogged("live view re-derived from the applied base after base WAL loss [view=lv");
                Assert.assertEquals(
                        "the repair must have come out of the failing turn's own in-process restore",
                        1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertNull("the re-derive must not leave the restore's repair parked", instance.getSuspendedRepair());
                Assert.assertFalse("the re-derive recovered the view", instance.isInvalid());
                Assert.assertEquals("the re-derive zeroes the streak it ended", 0, instance.getFlushRetryCount());

                driveRefreshToQuiescence(job);
            }

            Assert.assertNull(instance.getSuspendedRepair());
            Assert.assertFalse(instance.isWindowStateDirty());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the injected mid-drain failure is the one fault", 1, instance.getRefreshFaultCount());
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT);
        });
    }

    @Test
    public void testARestartRestoreWhoseParkedRepairEndsTheTurn() throws Exception {
        // A restore's replay walks the base WAL above the root it came back on, and that WAL is
        // raw: a commit whose own rows are not in timestamp order corrupts the accumulators if it
        // is fed in WAL order, so the replay hands off to the out-of-order repair. A localized
        // repair there parks on the refresh turn's budget like any other, and it owns the runtime
        // from that point - so the turn has to end on it. The drain below it would otherwise feed
        // rows through accumulators the parked replay is standing half-way through.
        //
        // The base deduplicates, which is what puts such a commit in the gap at all. Its drain
        // reads the applied base, whose reader yields rows in timestamp order, so a commit that is
        // out of order only within itself and entirely above the frontier is consumed with no
        // repair and no root sealed over it. The raw WAL under it still holds the rows unsorted.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            Assert.assertEquals("the default cadence seals the first boundary only", 1, countSealedBoundaries("lv"));
            assertViewRows(O3_GAP_OUTPUT);
            final long gapWatermark = instance("lv").getLastProcessedSeqTxn();

            // One commit the view does not consume, so the drain the check suppresses has work of
            // its own waiting behind it.
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
            drainWalQueue();

            shutdown();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                runOnePass(job);
                final LiveViewInstance instance = instance("lv");
                Assert.assertNotNull(
                        "the restart restore must leave the repair it handed off to parked on the view",
                        instance.getSuspendedRepair()
                );
                capture.drain();
                capture.assertLoggedRE("live view O3 replay \\[view=lv, lateRowTs=");
                capture.assertLoggedRE("live view O3 repair yielded on its turn budget \\[view=lv, turns=1,");
                // Nothing below the check ran: the watermark still names the commit the restart
                // read off disk, and the commit waiting above it is not in the view.
                Assert.assertEquals(
                        "the parked repair owns the runtime, so the turn must not have drained over it",
                        gapWatermark,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals("a park is not a fault", 0, instance.getRefreshFaultCount());
                Assert.assertEquals(
                        "the repair must have come out of the restart's own restore",
                        0,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals("the restore must not have fallen back to a rebuild", 0, instance.getCheckpointRebuildAttempts());
                assertViewRows(O3_GAP_OUTPUT);

                driveRefreshToQuiescence(job);
                Assert.assertNull(instance.getSuspendedRepair());
                Assert.assertFalse(instance.isInvalid());
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);
            }

            // The ladder the repair left behind is the one the next restart reads, and it needs no
            // repair of its own.
            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);
        });
    }

    @Test
    public void testAFailedPostRepairSealRetiresThePrefixARestartWouldReplayOver() throws Exception {
        // The other producer the hand-off's javadoc used to name - a failed post-O3 seal - and the
        // disposition that keeps it from being one.
        //
        // A repair that declines the checkpoint chain truncates instead: it keeps the roots below
        // its own output floor, writes the durable repair marker over them and re-seals a fresh
        // head once the replay is committed. The truncate alone does not move the generation's
        // base coordinate - publishTruncate carries the superblock's forward untouched - so
        // between it and that seal the preserved prefix is a generation valid against a base
        // snapshot predating the commit the repair just rewrote. A restart standing on such a
        // prefix replays raw base WAL above that coordinate, which walks the repaired commit
        // again in the arrival order the WAL still holds it in, and meets it out of order.
        //
        // The seal is what moves the coordinate, so a seal that fails has to take the prefix with
        // it. It does: the timeline is retired, the marker goes with it, and the restart rebuilds
        // from the applied base - a reader, in timestamp order, with no replay opened at all.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createBase("");
            createView();
            insertAndRefresh(SIX_ROWS);
            Assert.assertEquals("the default cadence seals the first boundary only", 1, countSealedBoundaries("lv"));
            final LiveViewInstance instance = instance("lv");
            final long resetsBefore = instance.getCheckpointTimelineResets();
            final long sealFailuresBefore = instance.getCheckpointSealFailures();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Fails the root append the repair closes on and nothing else the turn runs: the
                // truncate publishes through publishTruncate, which this stage leaves alone, and
                // the declined chain leaves no range splice to fail.
                job.setCheckpointTimelineTestFailureStage(
                        LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_DATA_PUBLISH
                );
                execute(CORRECTION_COMMIT);
                drainWalQueue();
                drainJob(job);
                job.setCheckpointTimelineTestFailureStage(0);
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE("live view O3 head miss declined the checkpoint splice, truncating instead \\[view=lv,");
            Assert.assertTrue(
                    "the repair's head seal must have been failed",
                    instance.getCheckpointSealFailures() > sealFailuresBefore
            );
            Assert.assertTrue(
                    "a repair that could not re-anchor its prefix must retire the timeline",
                    instance.getCheckpointTimelineResets() > resetsBefore
            );
            try (Path dir = checkpointsDir(instance); Path timeline = new Path()) {
                LiveViewCheckpointLayout.timelinePath(timeline, dir);
                Assert.assertFalse(
                        "the retire must take the prefix the truncate kept",
                        engine.getConfiguration().getFilesFacade().exists(timeline.$())
                );
                Assert.assertFalse(
                        "the retire must take the repair marker with it",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            assertViewRows(CORRECTED_OUTPUT);

            shutdown();
            restart();
            assertRebuiltFromAppliedBase("lv");
            assertViewRows(CORRECTED_OUTPUT);
        });
    }

    @Test
    public void testARepairThatSealedItsHeadRestartsAboveTheCommitItRepaired() throws Exception {
        // The control for the case above, over the same repair with the seal left alone. The head
        // it appends carries the repair's own base coordinate, and the whole generation is
        // published under it - so the restart's replay starts above the commit the repair
        // rewrote rather than over it, and meets nothing out of order.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createBase("");
            createView();
            insertAndRefresh(SIX_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long coordinateBefore = normalizedBaseSeqTxn(instance);
            final long resetsBefore = instance.getCheckpointTimelineResets();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute(CORRECTION_COMMIT);
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE("live view O3 head miss declined the checkpoint splice, truncating instead \\[view=lv,");
            Assert.assertEquals(
                    "a repair that re-anchored its prefix keeps the timeline",
                    resetsBefore,
                    instance.getCheckpointTimelineResets()
            );
            Assert.assertTrue(
                    "the seal must have moved the generation past the coordinate the prefix was sealed under",
                    normalizedBaseSeqTxn(instance) > coordinateBefore
            );
            Assert.assertEquals(
                    "the generation the repair leaves behind must be valid against the repair's own"
                            + " base snapshot, which is the floor a restart replays above",
                    instance.getLastProcessedSeqTxn(),
                    normalizedBaseSeqTxn(instance)
            );
            assertViewRows(CORRECTED_OUTPUT);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            final LiveViewInstance restarted = instance("lv");
            Assert.assertEquals(
                    "the restart's replay must have met no out-of-order commit",
                    0,
                    restarted.getO3BoundaryReplayRows() + restarted.getO3ResumeReplayRows()
            );
            assertViewRows(CORRECTED_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainFailureOverABaseThatLostADayKeepsTheViewRunning() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            // The incremental path walks past the DROP PARTITION and keeps the day's rows. A
            // whole-view rebuild from here would drop them, and the restatement guard would
            // refuse it on the history floor and stop the view.
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                insertThreeAndFailMidDrain(job, fault);
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, .*replayedRows=[1-9]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED, guard.getAbstention());
            final LiveViewInstance instance = instance("lv");
            Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
            assertRestoredInProcess(instance, 1);
            // The dropped day stays, and the three commits the fault interrupted land on top of
            // accumulators that neither lost nor double-counted a row.
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainFailureRestoresTheRuntimeAndDerivesTheLeadAgain() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final long durableSeqTxn = instance("lv").getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertThreeAndFailMidDrain(job, fault);
                // The clock has not moved, so nothing has flushed since the fault: the
                // recovery dropped the lead the failed turn stood on, and the view waits out the
                // backoff the fault armed before any turn drains again.
                final LiveViewInstance recovering = instance("lv");
                Assert.assertEquals(durableSeqTxn, recovering.getLastProcessedSeqTxn());
                Assert.assertEquals(durableSeqTxn, recovering.getRefreshedUpToSeqTxn());
                Assert.assertEquals(0, recovering.getLeadRowCount());
                // At its deadline the next turn derives all three commits again, over the restored
                // runtime. FLUSH EVERY has elapsed by then as well, so the same turn flushes them.
                final long retryUs = recovering.getRefreshRetryNotBeforeUs();
                Assert.assertEquals(currentMicros + REFRESH_RETRY_BACKOFF_BASE_MICROS, retryUs);
                setCurrentMicros(retryUs);
                Assert.assertTrue(job.run());
                Assert.assertEquals(durableSeqTxn + 3, recovering.getLastProcessedSeqTxn());
                Assert.assertEquals(0, recovering.getLeadRowCount());
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, .*replayedRows=3]");
            capture.assertNotLogged("live view recomputed window state from applied base");
            final LiveViewInstance instance = instance("lv");
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertEquals(
                    "the turn that drained past the fault zeroes the retry the restore's turn charged",
                    0,
                    instance.getFlushRetryCount()
            );
            Assert.assertEquals(
                    "the view must have refreshed and flushed past every commit",
                    instance.getLastProcessedSeqTxn(),
                    instance.getRefreshedUpToSeqTxn()
            );
            // Row 09:30 is the one the failed turn had already fed: a runtime left as the turn
            // left it would count it twice, 76.0 over four rows.
            assertViewRows(SEVEN_ROWS_OUTPUT);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testAMidDrainRestoreKeepsNewSymbolsOnTheIdsTheirFlushCommits() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As insertThreeAndFailMidDrain, but every commit brings an account the view's
                // table has never held. The first drains on a turn of its own and the second
                // feeds before the fault, so the turns the restore discards interned two values
                // that never reached the view's table.
                setCurrentMicros(instance("lv").getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 16.0)");
                drainWalQueue();
                execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-4', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-5', 64.0)");
                drainWalQueue();
                fault.arm(2);
                drainJob(job);
                Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, ");
            capture.assertNotLogged("live view recomputed window state from applied base");
            assertRestoredInProcess(instance("lv"), 1);
            // The lead the drain derived again over the restored runtime carries the ids it
            // interned the three accounts at, and the flush committed them on the view's table
            // in the same order. An id counter the restore left past the discarded values would
            // put every one of them a slot or two above its committed id, which reads back as the
            // next committed account or as none.
            final String fiveAccountRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-3\t16.0\t1
                    2026-01-02T09:30:00.000000Z\tacct-4\t32.0\t1
                    2026-01-02T09:40:00.000000Z\tacct-5\t64.0\t1
                    """;
            assertViewRows(fiveAccountRows);
            final String fiveAccounts = """
                    acct-1\t3
                    acct-2\t1
                    acct-3\t1
                    acct-4\t1
                    acct-5\t1
                    """;
            assertAccountsMatchTheBase(fiveAccounts);
            // No reader pinned the tier, so the restore's own tier rebuild took the two ids
            // back, and the flush that followed found its ids in step and re-stamped the slot:
            // the rows above came from it, not from a disk-only fallback.
            capture.assertOnlyOnce(SYMBOL_IDS_REWOUND);
            capture.assertNotLogged(SYMBOL_IDS_OUT_OF_STEP);
            assertSlotStampedAtTheViewTable(instance("lv"));

            // One more account after the flush: the next id past the committed ones must go to
            // it, not to an account the restored lead already holds.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO tx VALUES ('2026-01-02T09:50:00.000000Z', 'acct-6', 128.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance("lv").getRefreshFaultCount());
            assertSlotStampedAtTheViewTable(instance("lv"));
            assertViewRows(fiveAccountRows + "2026-01-02T09:50:00.000000Z\tacct-6\t128.0\t1\n");
            assertAccountsMatchTheBase(fiveAccounts + "acct-6\t1\n");
        });
    }

    @Test
    public void testAReaderPinnedAcrossAMidDrainRestoreDefersTheSymbolRewind() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final String pinnedRows = """
                    created_at\taccount_id
                    2026-01-01T09:00:00.000000Z\tacct-1
                    2026-01-01T09:10:00.000000Z\tacct-2
                    2026-01-02T09:00:00.000000Z\tacct-1
                    2026-01-02T09:10:00.000000Z\tacct-1
                    2026-01-02T09:20:00.000000Z\tacct-3
                    """;
            final String sixAccountRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-3\t16.0\t1
                    2026-01-02T09:30:00.000000Z\tacct-4\t32.0\t1
                    2026-01-02T09:40:00.000000Z\tacct-5\t64.0\t1
                    2026-01-02T09:50:00.000000Z\tacct-6\t128.0\t1
                    """;
            final String sixAccounts = """
                    acct-1\t3
                    acct-2\t1
                    acct-3\t1
                    acct-4\t1
                    acct-5\t1
                    acct-6\t1
                    """;
            final StringSink pinnedSink = new StringSink();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // acct-3 drains on a turn of its own into the un-flushed lead, at the first id
                // past the two committed accounts. The clock stays on the last flush, so nothing
                // flushes until the restore's retry.
                setCurrentMicros(instance("lv").getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 16.0)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals(1, instance("lv").getLeadRowCount());

                // The reader opens on the slot that holds that lead and stays open across the
                // restore that discards it, the turn that derives it again and the flush.
                try (
                        RecordCursorFactory factory = select("SELECT created_at, account_id FROM lv");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);

                    // As insertThreeAndFailMidDrain: acct-4 drains on a turn of its own, the next
                    // two coalesce behind it, and the fault fails the read of the last one after
                    // acct-5 fed. The restore discards three interned accounts.
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-4', 32.0)");
                    drainWalQueue();
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-5', 64.0)");
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:50:00.000000Z', 'acct-6', 128.0)");
                    drainWalQueue();
                    fault.arm(2);
                    drainJob(job);
                    Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
                    driveRefreshToQuiescence(job);

                    // Reads in the meantime agree with the base.
                    assertViewRows(sixAccountRows);
                    assertAccountsMatchTheBase(sixAccounts);

                    capture.drain();
                    capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, ");
                    // The pin held at both rewind attempts - the restore's tier rebuild and the
                    // drain that derived the four accounts again - so the ids stayed where the
                    // discarded turns left them, and that drain interned the accounts above them.
                    // The flush detected it and left the slot un-stamped instead of re-stamping
                    // ids the view's table gives to other accounts.
                    capture.assertNotLogged(SYMBOL_IDS_REWOUND);
                    capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP);
                    final LiveViewInstance pending = instance("lv");
                    Assert.assertTrue("the out-of-step flush must mark the tier stale", pending.isTierStale());
                    final LiveViewInMemoryTier pendingTier = pending.getInMemoryTier();
                    Assert.assertEquals(
                            "the out-of-step flush must un-stamp the published slot, so those reads ran disk-only",
                            Numbers.LONG_NULL,
                            pendingTier.getSlot(pendingTier.getPublishedIdx()).lvSeqTxn()
                    );

                    // The pinned reader still resolves the lead it holds: nothing re-bound the
                    // id acct-3 sits at in its slot.
                    cursor.toTop();
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);
                }

                // With the pin gone, the next rebuild of the slot takes the stranded ids back.
                // acct-7 is that drain's own value: the stale tier routes it straight to disk
                // and rebuilds the slot behind it, and the rebuild rewinds.
                execute("INSERT INTO tx VALUES ('2026-01-02T10:00:00.000000Z', 'acct-7', 256.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                capture.drain();
                capture.assertOnlyOnce(SYMBOL_IDS_REWOUND);
                Assert.assertFalse("the rebuild must clear the stale marking", instance("lv").isTierStale());

                // From here on the drain interns at the committed ids again, so a lead flush
                // finds them in step and re-stamps the slot as a subset of disk.
                execute("INSERT INTO tx VALUES ('2026-01-02T10:10:00.000000Z', 'acct-8', 512.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP);
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            assertSlotStampedAtTheViewTable(instance);
            assertViewRows(sixAccountRows
                    + "2026-01-02T10:00:00.000000Z\tacct-7\t256.0\t1\n"
                    + "2026-01-02T10:10:00.000000Z\tacct-8\t512.0\t1\n");
            assertAccountsMatchTheBase(sixAccounts + "acct-7\t1\nacct-8\t1\n");
        });
    }

    @Test
    public void testADedupViewPinnedAcrossAMidDrainRestoreRebuildsItsTierFromDisk() throws Exception {
        // A deduplicating base is never lead-eligible: every turn commits its rows, applies them
        // and publishes them into the tier as a subset of disk, under the ids the drain interned.
        // With a reader pinning the tier across a mid-drain restore, the turn after it interns
        // above the id the restore stranded, so its ids are not the ones the apply committed.
        // The publish has to notice and rebuild the slot from disk instead; once the reader is
        // gone, the next turn takes the stranded id back and publishes normally again.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final String pinnedRows = """
                    created_at\taccount_id
                    2026-01-01T09:00:00.000000Z\tacct-1
                    2026-01-01T09:10:00.000000Z\tacct-2
                    2026-01-02T09:00:00.000000Z\tacct-1
                    2026-01-02T09:10:00.000000Z\tacct-1
                    """;
            final String fiveAccountRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-3\t16.0\t1
                    2026-01-02T09:30:00.000000Z\tacct-4\t32.0\t1
                    2026-01-02T09:40:00.000000Z\tacct-5\t64.0\t1
                    """;
            final String fiveAccounts = """
                    acct-1\t3
                    acct-2\t1
                    acct-3\t1
                    acct-4\t1
                    acct-5\t1
                    """;
            final StringSink pinnedSink = new StringSink();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                try (
                        RecordCursorFactory factory = select("SELECT created_at, account_id FROM lv");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);

                    // acct-3 drains, commits and publishes on a turn of its own. The next two
                    // coalesce into one turn, and the fault fails the read of acct-5 after acct-4
                    // fed, so the restore strands the id that turn interned acct-4 at.
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 16.0)");
                    drainWalQueue();
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-4', 32.0)");
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-5', 64.0)");
                    drainWalQueue();
                    runOnePass(job);
                    fault.arm(1);
                    runOnePass(job);
                    Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
                    driveRefreshToQuiescence(job);

                    assertViewRows(fiveAccountRows);
                    assertAccountsMatchTheBase(fiveAccounts);
                    capture.drain();
                    capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, ");
                    capture.assertNotLogged(SYMBOL_IDS_REWOUND);
                    capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
                    // The rebuild left a disk-staged slot behind, not a stale one, and the pinned
                    // reader still reads the slot it opened on.
                    Assert.assertFalse(instance("lv").isTierStale());
                    cursor.toTop();
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);
                }

                // With the pin gone, the next turn takes the stranded id back before it interns,
                // and its publish finds the ids in step.
                execute("INSERT INTO tx VALUES ('2026-01-02T09:50:00.000000Z', 'acct-6', 128.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertOnlyOnce(SYMBOL_IDS_REWOUND);
            capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            assertSlotStampedAtTheViewTable(instance);
            assertViewRows(fiveAccountRows + "2026-01-02T09:50:00.000000Z\tacct-6\t128.0\t1\n");
            assertAccountsMatchTheBase(fiveAccounts + "acct-6\t1\n");
        });
    }

    @Test
    public void testAnAppliedScanRestoreKeepsAccountsOnTheirIdsWhenALaterAccountSortsAboveTheDiscardedRow() throws Exception {
        assertAppliedScanRestoreKeepsAccountsOnTheirIds(false);
    }

    @Test
    public void testAnAppliedScanRestoreKeepsAccountsOnTheirIdsWhenALaterAccountSortsBelowTheDiscardedRow() throws Exception {
        assertAppliedScanRestoreKeepsAccountsOnTheirIds(true);
    }

    @Test
    public void testAReaderPinnedAcrossAnAppliedScanRestoreDefersTheRewindToTheNextAppliedScan() throws Exception {
        // As the applied-scan restore cases, with a reader pinning the tier across the restore and
        // the retry: neither may take back the id the discarded turn interned acct-X at, so the
        // retry interns both accounts above it and its publish rebuilds the slot from disk. Once
        // the reader is gone, the next turn that drains the applied base takes the id back before
        // it interns, and its publish keeps the slot.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean hasFired = new AtomicBoolean();
        final AtomicReference<String> baseDir = new AtomicReference<>();
        assertMemoryLeak(newDayAmountOpenFault(baseDir, isArmed, hasFired), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            baseDir.set(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final long rawWalCleanCycles = instance("lv").getDedupRawWalCleanCycles();
            final String pinnedRows = """
                    created_at\taccount_id
                    2026-01-01T09:00:00.000000Z\tacct-1
                    2026-01-01T09:10:00.000000Z\tacct-2
                    2026-01-02T09:00:00.000000Z\tacct-1
                    2026-01-02T09:10:00.000000Z\tacct-1
                    """;
            final String expectedRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-Y\t8.0\t1
                    2026-01-02T09:30:00.000000Z\tacct-X\t2.0\t1
                    2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
                    """;
            final String accounts = """
                    acct-1\t4
                    acct-2\t1
                    acct-X\t1
                    acct-Y\t1
                    """;
            final StringSink pinnedSink = new StringSink();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                try (
                        RecordCursorFactory factory = select("SELECT created_at, account_id FROM lv");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);

                    execute("""
                            INSERT INTO tx VALUES
                                ('2026-01-02T09:30:00.000000Z', 'acct-X', 1.0),
                                ('2026-01-02T09:30:00.000000Z', 'acct-X', 2.0)
                            """);
                    execute("INSERT INTO tx VALUES ('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)");
                    drainWalQueue();
                    isArmed.set(true);
                    runOnePass(job);
                    Assert.assertTrue("the applied scan's open of the new day must have been failed", hasFired.get());
                    execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-Y', 8.0)");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);

                    assertViewRows(expectedRows);
                    assertAccountCountsMatchTheBase(accounts);
                    assertAccountRowsMatchTheBase("acct-X", "2026-01-02T09:30:00.000000Z\tacct-X\n");
                    assertAccountRowsMatchTheBase("acct-Y", "2026-01-02T09:20:00.000000Z\tacct-Y\n");
                    capture.drain();
                    capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, ");
                    // The pin held at the restore's rebuild and at the top of the retry's applied
                    // scan, so the retry interned acct-Y and acct-X above the stranded id.
                    capture.assertNotLogged(SYMBOL_IDS_REWOUND);
                    capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
                    Assert.assertFalse(instance("lv").isTierStale());
                    cursor.toTop();
                    println(factory.getMetadata(), cursor, pinnedSink);
                    TestUtils.assertEquals(pinnedRows, pinnedSink);
                }

                // acct-Z twice on one key, which the base collapses, so this turn drains the
                // applied base too, and with the pin gone it takes the stranded id back first.
                execute("""
                        INSERT INTO tx VALUES
                            ('2026-01-03T10:00:00.000000Z', 'acct-Z', 1.0),
                            ('2026-01-03T10:00:00.000000Z', 'acct-Z', 16.0)
                        """);
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals("every turn must have drained the applied base", rawWalCleanCycles, instance.getDedupRawWalCleanCycles());
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the applied scan fault is the one fault", 1, instance.getRefreshFaultCount());
            capture.drain();
            capture.assertOnlyOnce(SYMBOL_IDS_REWOUND);
            capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            assertSlotStampedAtTheViewTable(instance);
            assertViewRows(expectedRows + "2026-01-03T10:00:00.000000Z\tacct-Z\t16.0\t1\n");
            assertAccountCountsMatchTheBase(accounts + "acct-Z\t1\n");
            assertAccountRowsMatchTheBase("acct-Z", "2026-01-03T10:00:00.000000Z\tacct-Z\n");
        });
    }

    @Test
    public void testARestoreBehindALiveRepairMarkerFallsBackToTheRebuild() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            // What a prefix-preserving repair leaves while its truncated head is not yet
            // re-sealed: the superblock still names the discarded head, so no restore may read
            // the timeline under it.
            writeRepairMarker(instance("lv"));
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                insertThreeAndFailMidDrain(job, fault);
                driveRefreshToQuiescence(job);
            }

            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertTrue("the rebuild retires the timeline under the marker", instance.getCheckpointTimelineResets() > 0);
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            try (Path dir = checkpointsDir(instance)) {
                Assert.assertFalse(
                        "the retire takes the marker with the timeline",
                        LiveViewCheckpointRepairMarker.exists(engine.getConfiguration().getFilesFacade(), dir)
                );
            }
            // The base holds every row, so the rebuild is compared and reproduces them.
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testARestoreThatCannotReproduceTheViewFallsBackToTheRebuild() throws Exception {
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            // The fourth commit carries a duplicate the base collapses into its last row. The
            // view's drain reads the applied base and emits one row for it; a replay of the raw
            // WAL above the first root feeds both.
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
                    "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)",
                    "('2026-01-02T09:10:00.000000Z', 'acct-1', 7.0), ('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)",
                    "('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)"
            );
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:00:00.000000Z\tacct-1\t16.0\t1
                    """;
            assertViewRows(viewRows);

            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            // The restore's own check refuses a replay that feeds five rows above the first root
            // where the view holds four, and the rebuild covers for it, with the guard stood down
            // as behind every such restore over a deduplicating base. The base still holds every
            // row, so the rebuild reproduces them.
            assertGuardStoodDownBehindADedupRestoreMismatch(
                    guard,
                    "live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                            + "\\[view=lv, cause=base table metadata change, "
            );
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=base table metadata change]");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isWindowStateDirty());
            assertViewRows(viewRows + "2026-01-03T10:00:00.000000Z\tacct-1\t80.0\t2\n");
        });
    }

    @Test
    public void testARestartOverADedupBaseThatLostADayRestatesTheViewWhenItsRestoreMeetsACollapsedDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            // The fourth commit carries a duplicate the base collapses, above the only root the
            // default cadence seals. The view then walks past the day the base loses, so a rebuild
            // from the applied base drops that day.
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            dropPartitionAndRefresh("2026-01-01", COLLAPSED_DUPLICATE_OUTPUT);

            shutdown();
            final LiveViewRebuildRestatementGuard guard = restart();

            // The restore's replay of the raw WAL feeds both copies of the duplicate, so the
            // restore refuses the timeline, and the rebuild it falls back to runs without the
            // guard: the view follows the base and loses the day the base lost.
            assertRebuiltFromAppliedBase("lv");
            assertGuardStoodDownBehindADedupRestoreMismatch(guard, "could not restore live view from checkpoint timeline, rebuilding derived state \\[view=lv, ");
            assertNoRefreshFaults("lv");
            assertViewRows(COLLAPSED_DUPLICATE_RESTATED_OUTPUT);

            // Later commits keep flowing, on top of the accumulation the rebuild derived.
            insertAndRefresh("('2026-01-03T09:10:00.000000Z', 'acct-1', 32.0)");
            assertViewRows(COLLAPSED_DUPLICATE_RESTATED_OUTPUT + "2026-01-03T09:10:00.000000Z\tacct-1\t48.0\t2\n");
        });
    }

    @Test
    public void testARestartOverATtlDedupBaseRestatesTheViewWhenItsRestoreMeetsTwoCollidingCommits() throws Exception {
        assertMemoryLeak(() -> {
            // TTL measures a partition's age against the earlier of the table's newest row and the
            // wall clock, so the clock moves past every row first.
            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY TTL 1 DAY WAL DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            // The third commit moves the base's newest row to day three, which evicts day one.
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0)",
                    "('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)"
            );
            // Two commits on one (timestamp, key) that the base applies before the view drains
            // either: the base keeps the second, and the view drains that one row out of the
            // applied base, while the raw WAL above the root holds both.
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T09:10:00.000000Z', 'acct-1', 7.0)");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T09:10:00.000000Z', 'acct-1', 8.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            insertAndRefresh("('2026-01-03T09:20:00.000000Z', 'acct-2', 16.0)");
            assertQuery("SELECT min(created_at), count() FROM tx")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            min\tcount
                            2026-01-03T09:00:00.000000Z\t3
                            """);
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:20:00.000000Z\tacct-2\t16.0\t1
                    """);

            shutdown();
            final LiveViewRebuildRestatementGuard guard = restart();

            // The rebuild follows the base, which TTL emptied of day one.
            assertRebuiltFromAppliedBase("lv");
            assertGuardStoodDownBehindADedupRestoreMismatch(guard, "could not restore live view from checkpoint timeline, rebuilding derived state \\[view=lv, ");
            assertNoRefreshFaults("lv");
            final String restatedRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-03T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-03T09:20:00.000000Z\tacct-2\t16.0\t1
                    """;
            assertViewRows(restatedRows);
            insertAndRefresh("('2026-01-03T09:30:00.000000Z', 'acct-1', 32.0)");
            assertViewRows(restatedRows + "2026-01-03T09:30:00.000000Z\tacct-1\t44.0\t3\n");
        });
    }

    @Test
    public void testARestartOverADedupBaseThatLostADayRefusesTheRebuildBehindARestoreThatFailsOtherwise() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            dropPartitionAndRefresh("2026-01-01", COLLAPSED_DUPLICATE_OUTPUT);

            shutdown();
            engine.buildViewGraphs();
            // The restore fails before its replay reaches the duplicate: the failed open of the
            // timeline stands in for an IO error or a root it cannot read. Only a replay that
            // disagrees with the view's durable output stands the guard down, so the guard still
            // refuses the rebuild that would drop the day the base lost. Armed after the boot
            // pass, which reads the same file, so the open the restore makes is the one that fails.
            fault.armTimelineOpen();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }
            Assert.assertFalse("the restart's restore must have been failed", fault.isTimelineOpenArmed());

            final LiveViewInstance instance = instance("lv");
            Assert.assertTrue("the guard must refuse the rebuild", instance.isCheckpointRecoveryBlocked());
            Assert.assertEquals("rebuild_blocked", LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute()));
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            capture.drain();
            capture.assertLogged("could not restore live view from checkpoint timeline, rebuilding derived state [view=lv, ");
            capture.assertNotLogged("does not match durable materialization");
            capture.assertNotLogged(GUARD_STAND_DOWN);
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains "
                    + "[view=lv, cause=timeline restore failed, ");
            assertViewRows(COLLAPSED_DUPLICATE_OUTPUT);
        });
    }

    @Test
    public void testABaseSchemaChangeOverADedupBaseThatLostADayRestatesTheViewWhenItsRestoreMeetsACollapsedDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            dropPartitionAndRefresh("2026-01-01", COLLAPSED_DUPLICATE_OUTPUT);

            // A schema change on the running view, then a commit the base collapses into one row:
            // the collapse routes the drain through the applied base, where the drift surfaces,
            // and the restore that puts the runtime back replays the raw base WAL above the newest
            // root, as a restart does.
            execute("ALTER TABLE tx ADD COLUMN note INT");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                    + "('2026-01-03T10:00:00.000000Z', 'acct-1', 32.0)");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            assertGuardStoodDownBehindADedupRestoreMismatch(
                    guard,
                    "live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                            + "\\[view=lv, cause=base table metadata change, "
            );
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=base table metadata change]");
            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals("the drift is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertFalse(instance.isWindowStateDirty());
            final String restatedRows = COLLAPSED_DUPLICATE_RESTATED_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t48.0\t2\n";
            assertViewRows(restatedRows);

            execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T11:00:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewRows(restatedRows + "2026-01-03T11:00:00.000000Z\tacct-1\t112.0\t3\n");
            Assert.assertEquals(1, instance.getRefreshFaultCount());
        });
    }

    @Test
    public void testAMidDrainFailureOverADedupBaseThatLostADayRestatesTheViewWhenItsRestoreMeetsACollapsedDuplicate() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            dropPartitionAndRefresh("2026-01-01", COLLAPSED_DUPLICATE_OUTPUT);
            final LiveViewInstance instance = instance("lv");
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in failMidDrainIntoARebuildThenIdleThenFailOnce, on day three, above the view's
                // frontier: the fault fails the read of the third commit after the turn fed the
                // second.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-03T09:20:00.000000Z', 'acct-2', 16.0)");
                drainWalQueue();
                execute("INSERT INTO tx VALUES ('2026-01-03T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-03T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                fault.arm(2);
                drainJob(job);
                driveRefreshToQuiescence(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                guard = job.rebuildRestatementGuardForTest();
            }

            assertGuardStoodDownBehindADedupRestoreMismatch(
                    guard,
                    "live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                            + "\\[view=lv, cause=mid-drain refresh failure, "
            );
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            Assert.assertEquals(0, instance.getCheckpointRuntimeRestores());
            Assert.assertEquals("the mid-drain fault is the one fault", 1, instance.getRefreshFaultCount());
            final String restatedRows = COLLAPSED_DUPLICATE_RESTATED_OUTPUT
                    + "2026-01-03T09:20:00.000000Z\tacct-2\t16.0\t1\n"
                    + "2026-01-03T09:30:00.000000Z\tacct-1\t48.0\t2\n"
                    + "2026-01-03T09:40:00.000000Z\tacct-2\t80.0\t2\n";
            assertViewRows(restatedRows);

            execute("INSERT INTO tx VALUES ('2026-01-03T09:50:00.000000Z', 'acct-1', 64.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
            assertViewRows(restatedRows + "2026-01-03T09:50:00.000000Z\tacct-1\t112.0\t3\n");
            Assert.assertEquals(1, instance.getRefreshFaultCount());
        });
    }

    @Test
    public void testADedupRestoreMismatchAtARestartStandsTheGuardDownForItsOwnRebuildOnly() throws Exception {
        assertGuardStandsDownForTheMismatchedRestoresOwnRebuildOnly(true);
    }

    @Test
    public void testADedupRestoreMismatchInPlaceStandsTheGuardDownForItsOwnRebuildOnly() throws Exception {
        assertGuardStandsDownForTheMismatchedRestoresOwnRebuildOnly(false);
    }

    @Test
    public void testAMidDrainFailureOverABaseThatLostADayRefusesTheRebuildBehindARestoreThatFails() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final LiveViewInstance instance = instance("lv");
            final long processedBefore;
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                processedBefore = instance.getLastProcessedSeqTxn();
                // The recovery's restore fails on the timeline's open, which stands in for an IO
                // error or a root it cannot read, so the recovery falls back to the rebuild. Over
                // a base without dedup keys no restore failure stands the guard down.
                fault.armTimelineOpen();
                insertThreeAndFailMidDrain(job, fault);
                Assert.assertFalse("the recovery's restore must have been failed", fault.isTimelineOpenArmed());
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            Assert.assertTrue("the guard must refuse the rebuild", instance.isCheckpointRecoveryBlocked());
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(processedBefore, instance.getLastProcessedSeqTxn());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            capture.drain();
            capture.assertLogged("live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, ");
            capture.assertNotLogged(GUARD_STAND_DOWN);
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains "
                    + "[view=lv, cause=mid-drain refresh failure, ");
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootOverABaseThatLostADay() throws Exception {
        assertMemoryLeak(() -> {
            createBase("");
            createView();
            insertAndRefresh(TIED_ROWS);
            assertSingleRootAt("2026-01-01T09:00:00.000000Z");
            dropPartitionAndRefresh("2026-01-01", TIED_ROWS_OUTPUT);

            // The tied row and both day-two rows sit above the root's base seqTxn.
            restartAndAssertRestoredOverATie(TIED_ROWS_OUTPUT, 3);
            // The day the base lost stays, and the view keeps refreshing on top of the day-two
            // accumulation the restore put back.
            insertAndRefresh("('2026-01-02T09:20:00.000000Z', 'acct-1', 16.0)");
            assertViewRows(TIED_ROWS_OUTPUT + "2026-01-02T09:20:00.000000Z\tacct-1\t28.0\t3\n");
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootOverABaseTtlEvicted() throws Exception {
        assertMemoryLeak(() -> {
            // TTL measures a partition's age against the earlier of the table's newest row and the
            // wall clock, so the clock moves past every row first.
            setCurrentMicros(ts("2026-01-05T00:00:00.000000Z"));
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY TTL 1 DAY WAL");
            createView();
            // The third commit moves the base's newest row to day three, which evicts day one.
            insertAndRefresh(TIED_ROWS[0], TIED_ROWS[1], "('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)");
            assertSingleRootAt("2026-01-01T09:00:00.000000Z");
            assertQuery("SELECT min(created_at), count() FROM tx")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            min\tcount
                            2026-01-03T09:00:00.000000Z\t1
                            """);
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-2\t2.0\t1
                    2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1
                    """;
            assertViewRows(viewRows);

            restartAndAssertRestoredOverATie(viewRows, 2);
            insertAndRefresh("('2026-01-03T09:10:00.000000Z', 'acct-1', 8.0)");
            assertViewRows(viewRows + "2026-01-03T09:10:00.000000Z\tacct-1\t12.0\t2\n");
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootOverADeduplicatingBase() throws Exception {
        assertMemoryLeak(() -> {
            // The tied row carries a key of its own, so the base keeps both rows at the root's
            // timestamp and the raw WAL the restore replays holds exactly what the base applied.
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(TIED_ROWS);
            assertSingleRootAt("2026-01-01T09:00:00.000000Z");

            restartAndAssertRestoredOverATie(TIED_ROWS_OUTPUT, 3);
            insertAndRefresh("('2026-01-02T09:20:00.000000Z', 'acct-1', 16.0)");
            assertViewRows(TIED_ROWS_OUTPUT + "2026-01-02T09:20:00.000000Z\tacct-1\t28.0\t3\n");
        });
    }

    @Test
    public void testARestartAfterAnUpsertOnTheNewestRootsTimestampKeepsTheReplacement() throws Exception {
        assertMemoryLeak(() -> {
            // The second commit replaces the row the root folded. Reaching the frontier, it takes
            // the out-of-order repair, which publishes a root over the replacement, so no restore
            // replays it from the raw WAL - where it would read as a second row.
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 5.0)",
                    "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0)"
            );
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t5.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    """;
            assertViewRows(viewRows);

            shutdown();
            restart();
            assertRestoredFromTimeline("lv");
            Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
            assertNoRefreshFaults("lv");
            assertViewRows(viewRows);
            insertAndRefresh("('2026-01-02T09:10:00.000000Z', 'acct-1', 8.0)");
            assertViewRows(viewRows + "2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2\n");
        });
    }

    @Test
    public void testARestartRestoresATieOnTheSeedRoot() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieOnTheSeedRoot(false));
    }

    @Test
    public void testARestartRestoresATieOnTheSeedRootOverABaseThatLostADay() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieOnTheSeedRoot(true));
    }

    @Test
    public void testAMidDrainFailureRestoresATieOnTheNewestRootOverABaseThatLostADay() throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            fault.of(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(TIED_ROWS);
            assertSingleRootAt("2026-01-01T09:00:00.000000Z");
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                insertThreeAndFailMidDrain(job, fault);
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            assertRestoredInProcess(instance("lv"), 1);
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NOT_EVALUATED, guard.getAbstention());
            assertViewRows(TIED_ROWS_OUTPUT
                    + "2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1\n"
                    + "2026-01-02T09:30:00.000000Z\tacct-1\t44.0\t3\n"
                    + "2026-01-02T09:40:00.000000Z\tacct-2\t80.0\t2\n");
            // The restore replayed the tied row and both day-two rows, and nothing rebuilt.
            capture.drain();
            capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, .*replayedRows=3]");
            capture.assertNotLogged("live view rebuild from the applied base refused");
        });
    }

    @Test
    public void testALateCommitAfterARestartDoesNotResumeFromTheRootItsTieOutgrew() throws Exception {
        assertMemoryLeak(() -> {
            createBase("");
            createView();
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:00:00.000000Z', 'acct-2', 2.0)",
                    "('2026-01-01T09:20:00.000000Z', 'acct-2', 4.0)"
            );
            assertSingleRootAt("2026-01-01T09:00:00.000000Z");
            restartAndAssertRestoredOverATie("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-2\t2.0\t1
                    2026-01-01T09:20:00.000000Z\tacct-2\t6.0\t2
                    """, 2);

            // A late row between the root and the frontier. The root the restore stood on is the
            // head again and the newest root below the late row, but it does not hold the tied
            // row: a resume from it reads the base from just above it, and would answer 8.0 over
            // one row and 12.0 over two for acct-2.
            insertAndRefresh("('2026-01-01T09:10:00.000000Z', 'acct-2', 8.0)");
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:00:00.000000Z\tacct-2\t2.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t10.0\t2
                    2026-01-01T09:20:00.000000Z\tacct-2\t14.0\t3
                    """);
            capture.drain();
            capture.assertLogged("live view resume anchor no longer covers its timestamp group, re-anchoring below it "
                    + "[view=lv, anchorMaxTs=2026-01-01T09:00:00.000000Z");
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInAnAnchoredView() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowIt(false));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInAnAnchoredViewOverABaseThatLostADay() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowIt(true));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInAnAnchoredViewWithoutThePerSegmentRepair() throws Exception {
        // Without the decomposition the late row takes the union range directly rather than past
        // the grown-group gate, and the plan localizes behind the end of the table all the same.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowIt(false));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInAnAnchoredViewWithoutThePerSegmentRepairOverABaseThatLostADay() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowIt(true));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInARangeFramedView() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowItInAFramedView(
                SPLICE_RANGE_FRAME,
                SPLICE_SEEDED_ROWS,
                SPLICE_RANGE_ROWS_BELOW_THE_LATE_ROW,
                SPLICE_RANGE_ROWS_FROM_THE_LATE_ROW,
                false
        ));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInARangeFramedViewOverABaseThatLostADay() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowItInAFramedView(
                SPLICE_RANGE_FRAME,
                SPLICE_SEEDED_ROWS,
                SPLICE_RANGE_ROWS_BELOW_THE_LATE_ROW,
                SPLICE_RANGE_ROWS_FROM_THE_LATE_ROW,
                true
        ));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInARowsFramedView() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowItInAFramedView(
                SPLICE_ROWS_FRAME,
                SPLICE_ROWS_SEEDED_ROWS,
                SPLICE_ROWS_ROWS_BELOW_THE_LATE_ROW,
                SPLICE_ROWS_ROWS_FROM_THE_LATE_ROW,
                false
        ));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterALateRowInARowsFramedViewOverABaseThatLostADay() throws Exception {
        assertMemoryLeak(() -> assertRestartRestoresATieAfterALateRowBelowItInAFramedView(
                SPLICE_ROWS_FRAME,
                SPLICE_ROWS_SEEDED_ROWS,
                SPLICE_ROWS_ROWS_BELOW_THE_LATE_ROW,
                SPLICE_ROWS_ROWS_FROM_THE_LATE_ROW,
                true
        ));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterAKeyedResumeOfTheOpenSegment() throws Exception {
        // One root per flush, so a root sits below the late row inside the open day and the repair
        // resumes from it. A resume following the late row's own key - which the posting index on
        // the key column is what makes available - re-versions the newest root from the old one for
        // every other key, and the tied row's key is one of them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL INDEX, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            createView();
            insertAndRefresh(
                    "('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0)",
                    "('2026-01-01T09:20:00.000000Z', 'acct-1', 2.0)",
                    "('2026-01-01T09:20:00.000000Z', 'acct-2', 4.0)"
            );
            Assert.assertEquals("one root per flush, none on the tie", 2, countSealedBoundaries("lv"));
            final long keyedResumes;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.setForceOpenSegmentKeyedReplayForTest(true);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                        + "('2026-01-01T09:10:00.000000Z', 'acct-1', 8.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                keyedResumes = job.openSegmentKeyedResumeCountForTest();
            }
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-1\t9.0\t2
                    2026-01-01T09:20:00.000000Z\tacct-1\t11.0\t3
                    2026-01-01T09:20:00.000000Z\tacct-2\t4.0\t1
                    """;
            assertViewRows(viewRows);

            restartAndAssertRestoredOverATie(viewRows, 0);
            insertAndRefresh("('2026-01-01T09:30:00.000000Z', 'acct-2', 16.0)");
            assertViewRows(viewRows + "2026-01-01T09:30:00.000000Z\tacct-2\t20.0\t2\n");
            Assert.assertEquals(
                    "a resume by key would re-version the newest root without the tied row",
                    0,
                    keyedResumes
            );
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootAfterAColdKeyedRepairOfTheOpenSegment() throws Exception {
        // The default cadence seals one root, on the first row, so no root sits below the late row
        // and the repair replays the open day from its start. A replay following the late row's own
        // key re-versions the newest root from the old one for every other key, and the tied row's
        // key is one of them.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL INDEX, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            createView();
            insertAndRefresh(
                    "('2026-01-01T09:20:00.000000Z', 'acct-1', 2.0)",
                    "('2026-01-01T09:20:00.000000Z', 'acct-2', 4.0)"
            );
            assertSingleRootAt("2026-01-01T09:20:00.000000Z");
            final long coldKeyedReplays;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.setForceOpenSegmentKeyedReplayForTest(true);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                        + "('2026-01-01T09:10:00.000000Z', 'acct-1', 8.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                coldKeyedReplays = job.openSegmentColdKeyedReplayCountForTest();
            }
            assertNoRefreshFaults("lv");
            final String viewRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:10:00.000000Z\tacct-1\t8.0\t1
                    2026-01-01T09:20:00.000000Z\tacct-1\t10.0\t2
                    2026-01-01T09:20:00.000000Z\tacct-2\t4.0\t1
                    """;
            assertViewRows(viewRows);

            restartAndAssertRestoredOverATie(viewRows, 0);
            insertAndRefresh("('2026-01-01T09:30:00.000000Z', 'acct-2', 16.0)");
            assertViewRows(viewRows + "2026-01-01T09:30:00.000000Z\tacct-2\t20.0\t2\n");
            Assert.assertEquals(
                    "a cold replay by key would re-version the newest root without the tied row",
                    0,
                    coldKeyedReplays
            );
        });
    }

    @Test
    public void testARestartRestoresATieTheLastRestartReplayedAfterALateRowBelowIt() throws Exception {
        // The first restart replays the tied row onto the root it restores, as a live run would have
        // folded it. Only the batch minimum the restore leaves behind tells the next repair that the
        // root's timestamp group has grown.
        assertMemoryLeak(() -> {
            createBase("");
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + SPLICE_SEEDED_ROWS);
            drainWalQueue();
            createView();
            insertAndRefresh(SPLICE_ROOT_TIE);
            assertSingleRootAt("2026-01-03T09:00:00.000000Z");
            restartAndAssertRestoredOverATie(SPLICE_ROOT_TIE_OUTPUT, 1);

            final long segmentRepairs = landALateRowBelowATieOnTheNewestRoot();
            restartAndAssertRestoredOverATie(SPLICE_LATE_ROW_OUTPUT, 0);
            insertAndRefresh("('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)");
            assertViewRows(SPLICE_LATE_ROW_OUTPUT + "2026-01-03T09:10:00.000000Z\tacct-2\t40.0\t2\n");
            Assert.assertEquals("closed segments repaired over their own range", 0, segmentRepairs);
        });
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootWhileARepairAcrossTwoDaysIsParked() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> assertRestartRestoresATieWhileARepairAcrossTwoDaysIsParked(false));
    }

    @Test
    public void testARestartRestoresATieOnTheNewestRootWhileARepairAcrossTwoDaysIsParkedOverABaseThatLostADay() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> assertRestartRestoresATieWhileARepairAcrossTwoDaysIsParked(true));
    }

    @Test
    public void testARebuildThatGetsPastAMidDrainFaultEndsTheRetryStreak() throws Exception {
        // A live repair marker declines the restore, so the mid-drain recovery rebuilds the view
        // from the applied base. See failMidDrainIntoARebuildThenIdleThenFailOnce.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            insertAndRefresh(FOUR_ROWS);
            writeRepairMarker(instance("lv"));
            failMidDrainIntoARebuildThenIdleThenFailOnce(fault, "2026-01-02", "2026-01-03");
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
            assertViewRows(SEVEN_ROWS_OUTPUT + "2026-01-03T09:00:00.000000Z\tacct-1\t128.0\t1\n");
        });
    }

    @Test
    public void testADedupRebuildThatGetsPastAMidDrainFaultEndsTheRetryStreak() throws Exception {
        // No marker this time: the collapsed duplicate makes the restore's replay disagree with the
        // view's durable output, so the restore fails and the mid-drain recovery falls back to the
        // rebuild, as testARestoreThatCannotReproduceTheViewFallsBackToTheRebuild's drift recovery
        // does. See failMidDrainIntoARebuildThenIdleThenFailOnce.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            failMidDrainIntoARebuildThenIdleThenFailOnce(fault, "2026-01-03", "2026-01-04");
            capture.assertLoggedRE("live view could not restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "\\[view=lv, cause=mid-drain refresh failure, .*does not match durable materialization");
            assertViewRows(COLLAPSED_DUPLICATE_OUTPUT
                    + "2026-01-03T09:20:00.000000Z\tacct-2\t16.0\t1\n"
                    + "2026-01-03T09:30:00.000000Z\tacct-1\t48.0\t2\n"
                    + "2026-01-03T09:40:00.000000Z\tacct-2\t80.0\t2\n"
                    + "2026-01-04T09:00:00.000000Z\tacct-1\t128.0\t1\n");
        });
    }

    @Test
    public void testARestoreWhoseRepairGetsPastAMidDrainFaultEndsTheRetryStreak() throws Exception {
        // The restore puts the view back at its applied watermark, in front of the commits the
        // fault stopped, unless its replay meets an out-of-order commit in the gap above the root
        // and hands off to the out-of-order repair. That repair pins the base's applied head rather
        // than the watermark, so it consumes the commits the fault stopped from the applied base,
        // as the rebuild in failMidDrainIntoARebuildThenIdleThenFailOnce does, and no later turn
        // has anything left to get past. The recovery itself therefore has to end the retry
        // streak: one it left standing would still measure from the first fault when a second, on
        // a new commit, arrives a minute later, and that one fault would exhaust the duration
        // budget and invalidate a view that recovered long before.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            assertViewRows(O3_GAP_OUTPUT);
            final LiveViewInstance instance = instance("lv");
            final long processedBeforeFault;
            final long baseHeadAtFault;
            final int retryCountAfterRestore;
            final long retryStartAfterRestore;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in testAMidDrainRestoreWhoseParkedRepairMeetsAnExhaustedBudgetIsDiscarded, with
                // the repair's turn budget left at its default, so the repair the restore hands off
                // to finishes on the failing turn rather than parking.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                runOnePass(job);
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_FIRST_ROW_OUTPUT);
                processedBeforeFault = instance.getLastProcessedSeqTxn();
                baseHeadAtFault = engine.getTableSequencerAPI().lastTxn(baseToken);

                fault.arm(1);
                runOnePass(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals(
                        "the repair must have come out of the failing turn's own in-process restore",
                        1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertNull("the repair must have finished on the failing turn", instance.getSuspendedRepair());
                Assert.assertEquals(
                        "the restore's repair must have consumed every commit the fault stopped",
                        baseHeadAtFault,
                        instance.getLastProcessedSeqTxn()
                );
                assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT);
                retryCountAfterRestore = instance.getFlushRetryCount();
                retryStartAfterRestore = instance.getFlushRetryStartUs();

                // No base commit for longer than the duration budget, so no turn has work to do.
                setCurrentMicros(currentMicros + engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros());
                driveRefreshToQuiescence(job);

                // One unrelated fault on the first read of a new commit, as in
                // failMidDrainIntoARebuildThenIdleThenFailOnce.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-05T09:00:00.000000Z', 'acct-1', 256.0)");
                drainWalQueue();
                fault.arm(0);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                job.run();
                Assert.assertTrue("the new commit's read must have been failed", fault.hasFired());
                Assert.assertFalse("one fault long after the view recovered must not invalidate it", instance.isInvalid());
                Assert.assertEquals("the later fault starts a streak of its own", 1, instance.getFlushRetryCount());
                driveRefreshToQuiescence(job);
            }

            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the two injected faults", 2, instance.getRefreshFaultCount());
            Assert.assertEquals("the turn that drained past the later fault zeroes its streak", 0, instance.getFlushRetryCount());
            Assert.assertEquals(
                    "the view must have materialized the commit the later fault stopped",
                    engine.getTableSequencerAPI().lastTxn(baseToken),
                    instance.getLastProcessedSeqTxn()
            );
            Assert.assertEquals("the restore that got past the first fault ends its streak", 0, retryCountAfterRestore);
            Assert.assertEquals(Numbers.LONG_NULL, retryStartAfterRestore);
            capture.drain();
            capture.assertLoggedRE("live view O3 replay \\[view=lv, .*advanceTo=" + processedBeforeFault
                    + ", pinnedSeqTxn=" + baseHeadAtFault + ", ");
            // The fault the recovery got past, which nothing else reports.
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, recovery advanced the view "
                    + "\\[view=lv, fromSeqTxn=" + processedBeforeFault + ", toSeqTxn=" + baseHeadAtFault + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed \\[view=lv, retryCount=1, " + EIO_READ_ERROR_RE);
            capture.assertNotLogged("window state recovered, retrying");
            capture.assertNotLogged("live view refresh budget exhausted");
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT + "2026-01-05T09:00:00.000000Z\tacct-1\t256.0\t1\n");
        });
    }

    @Test
    public void testARebuildThatGetsPastAMidDrainFaultEndsTheStreakAnEarlierFaultStarted() throws Exception {
        // In the cases above, the recovery that gets past the fault answers the first fault of its
        // streak, so there is no earlier charge for it to end. Here the first fault meets a base
        // whose apply stands where the view does, as in
        // testARebuildThatCannotGetPastAMidDrainFaultExhaustsTheRetryBudget: its rebuild gets past
        // nothing, and the turn is charged, which starts the streak clock. The base then applies,
        // and the next drain meets a second fault, whose rebuild consumes every commit either fault
        // stopped. That rebuild has to end the streak the first fault started. One it left standing
        // would still measure from the first fault when a third, on a new commit, arrives a minute
        // later, and that one fault would exhaust the duration budget and invalidate a view that
        // recovered long before.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long appliedSeqTxn = instance.getLastProcessedSeqTxn();
            final long firstFaultUs = instance.getLastFlushTimeUs();
            final long baseHeadAtFault;
            final int retryCountAfterAdvance;
            final long retryStartAfterAdvance;
            // Both recoveries rebuild behind a marker. The first rebuild seals a fresh root that the
            // second recovery would restore from instead, so the second drain stamps one again.
            writeRepairMarker(instance);
            execute("ALTER TABLE tx SUSPEND WAL");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The stuck-rebuild case's three commits, level with one another for the same
                // reason, which the suspended apply leaves ahead of the base's applied head.
                setCurrentMicros(firstFaultUs);
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 64.0)");
                baseHeadAtFault = engine.getTableSequencerAPI().lastTxn(baseToken);
                fault.arm(2);
                drainJob(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals("the first rebuild stood where the view stood", appliedSeqTxn, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("a rebuild that got past nothing is charged to the duration alone", 0, instance.getFlushRetryCount());
                Assert.assertEquals("a rebuild that got past nothing starts the streak clock", firstFaultUs, instance.getFlushRetryStartUs());

                // The base applies the three commits, and the next commit notification re-drains
                // them into a second fault on the same read.
                execute("ALTER TABLE tx RESUME WAL");
                drainWalQueue();
                writeRepairMarker(instance);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                fault.arm(2);
                engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseHeadAtFault);
                drainJob(job);
                Assert.assertTrue("the re-drain's segment read must have been failed", fault.hasFired());
                Assert.assertEquals(
                        "the second rebuild must have consumed every commit the faults stopped",
                        baseHeadAtFault,
                        instance.getLastProcessedSeqTxn()
                );
                retryCountAfterAdvance = instance.getFlushRetryCount();
                retryStartAfterAdvance = instance.getFlushRetryStartUs();

                // No base commit for longer than the duration budget, so no turn has work to do.
                setCurrentMicros(currentMicros + engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros());
                driveRefreshToQuiescence(job);

                // One unrelated fault on the first read of a new commit, as in
                // failMidDrainIntoARebuildThenIdleThenFailOnce.
                execute("INSERT INTO tx VALUES ('2026-01-03T09:00:00.000000Z', 'acct-1', 128.0)");
                drainWalQueue();
                fault.arm(0);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                job.run();
                Assert.assertTrue("the new commit's read must have been failed", fault.hasFired());
                Assert.assertFalse("one fault long after the view recovered must not invalidate it", instance.isInvalid());
                Assert.assertEquals("the later fault starts a streak of its own", 1, instance.getFlushRetryCount());
                driveRefreshToQuiescence(job);
            }

            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the three injected faults", 3, instance.getRefreshFaultCount());
            Assert.assertEquals("the turn that drained past the later fault zeroes its streak", 0, instance.getFlushRetryCount());
            Assert.assertEquals(
                    "the view must have materialized the commit the later fault stopped",
                    engine.getTableSequencerAPI().lastTxn(baseToken),
                    instance.getLastProcessedSeqTxn()
            );
            Assert.assertEquals("the rebuild that got past the second fault ends the first fault's streak", 0, retryCountAfterAdvance);
            Assert.assertEquals(Numbers.LONG_NULL, retryStartAfterAdvance);
            Assert.assertEquals("every recovery rebuilt", 0, instance.getCheckpointRuntimeRestores());
            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, window state recovered, retrying "
                    + "\\[view=lv, retryCount=0, elapsedUs=0, " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, recovery advanced the view "
                    + "\\[view=lv, fromSeqTxn=" + appliedSeqTxn + ", toSeqTxn=" + baseHeadAtFault + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed \\[view=lv, retryCount=1, " + EIO_READ_ERROR_RE);
            capture.assertNotLogged("live view refresh budget exhausted");
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1
                    2026-01-02T09:20:00.000000Z\tacct-1\t44.0\t3
                    2026-01-02T09:20:00.000000Z\tacct-3\t64.0\t1
                    2026-01-03T09:00:00.000000Z\tacct-1\t128.0\t1
                    """);
        });
    }

    @Test
    public void testARebuildThatCannotGetPastAMidDrainFaultExhaustsTheRetryBudget() throws Exception {
        // The rebuild pins the base's applied head, not the head the drain read from raw WAL. Here
        // the base's own apply stands where the view does - an operator's SUSPEND WAL, or in the
        // wild a segment the apply cannot read either - so the rebuild recomputes the view where it
        // already stood and leaves the commit the fault stopped ahead of it. Every later drain meets
        // the fault again. A rebuild like that got past nothing and must be charged like a restore,
        // to the duration budget, or a fault that does not clear would rebuild the whole view on
        // every commit notification, forever, with neither budget able to run out.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, STUCK_REBUILD_RETRY_MAX);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, STUCK_REBUILD_RETRY_MAX_DURATION_MICROS);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long appliedSeqTxn = instance.getLastProcessedSeqTxn();
            // Every recovery rebuilds behind a marker. Each rebuild seals a fresh root that the
            // next recovery would restore from instead, so every drain below stamps one again.
            writeRepairMarker(instance);
            execute("ALTER TABLE tx SUSPEND WAL");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in insertThreeAndFailMidDrain, but the three rows share one timestamp. A
                // rebuild leaves the frontier the failed turn fed the view up to, and the next
                // drain re-feeds the same commits from below it: rows strictly under it would read
                // as out of order, and the drain would wait for the base's apply rather than meet
                // the fault again. Rows level with it are an ordinary forward append.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 64.0)");
                fault.arm(2);
                drainJob(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                // The fallback scan drives a view only as far as the base has applied, so what
                // re-drains the three commits is the next commit notification.
                for (int i = 0; i < STUCK_REBUILD_MAX_DRIVES && !instance.isInvalid(); i++) {
                    writeRepairMarker(instance);
                    advanceClockToNextRefreshPass();
                    fault.arm(2);
                    engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, appliedSeqTxn + 3);
                    drainJob(job);
                    Assert.assertTrue("every drain must meet the fault again", fault.hasFired());
                }
            }

            Assert.assertTrue("a fault the rebuild cannot get past must exhaust the retry budget", instance.isInvalid());
            Assert.assertEquals("flush retry budget exhausted", instance.getStateReader().getInvalidationReason());
            Assert.assertEquals(
                    "one fault per charged turn, until the duration budget runs out",
                    STUCK_REBUILD_CHARGED_TURNS,
                    instance.getRefreshFaultCount()
            );
            Assert.assertEquals("every rebuild stood where the view stood", appliedSeqTxn, instance.getLastProcessedSeqTxn());
            Assert.assertEquals("every recovery rebuilt", 0, instance.getCheckpointRuntimeRestores());
            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, window state recovered, retrying "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS - 1)
                    + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh budget exhausted, invalidating "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS) + ", " + EIO_READ_ERROR_RE);
            capture.assertNotLogged("recovery advanced the view");
            // Nothing past the base's applied head reached the output.
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    """);
        });
    }

    @Test
    public void testARebuildThatStopsBelowAMidDrainFaultExhaustsTheRetryBudget() throws Exception {
        // Between the case above and the ones whose rebuild gets past the fault: the base has
        // applied part of what the view has not, but not the commit the fault stops. The first
        // rebuild moves the view up to the base's applied head, below the fault, and its turn goes
        // uncharged. Every drain after it meets the fault again, and every rebuild after it stands
        // where the view stands, so each of those turns is charged and the duration budget runs
        // out. A rule that let the one move forward excuse the rebuilds after it would rebuild the
        // whole view on every commit notification, forever.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, STUCK_REBUILD_RETRY_MAX);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, STUCK_REBUILD_RETRY_MAX_DURATION_MICROS);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long appliedSeqTxn = instance.getLastProcessedSeqTxn();
            final long baseAppliedAtFault;
            int turns = 0;
            // Every recovery rebuilds behind a marker, which every drain below stamps again.
            writeRepairMarker(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Four commits level with one another, for the reason the case above gives. The base
                // applies the first two and not the last two, and the first drain feeds the first
                // three and fails the read of the fourth.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-1', 32.0)");
                drainWalQueue();
                execute("ALTER TABLE tx SUSPEND WAL");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-3', 64.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-4', 128.0)");
                baseAppliedAtFault = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
                final long baseHead = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals("the base applied the first two commits", appliedSeqTxn + 2, baseAppliedAtFault);
                Assert.assertEquals(appliedSeqTxn + 4, baseHead);
                fault.arm(3);
                drainJob(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals(
                        "the first rebuild must have moved the view up to the base's applied head",
                        baseAppliedAtFault,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals("a rebuild that moved the view forward is not charged", 0, instance.getFlushRetryCount());
                // As in the case above, what re-drains the stopped commits is the next commit
                // notification. Each drain feeds the third commit and fails the read of the fourth.
                for (; turns < STUCK_REBUILD_MAX_DRIVES && !instance.isInvalid(); turns++) {
                    writeRepairMarker(instance);
                    advanceClockToNextRefreshPass();
                    fault.arm(1);
                    engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseHead);
                    drainJob(job);
                    Assert.assertTrue("every drain must meet the fault again", fault.hasFired());
                }
            }

            Assert.assertTrue("a fault the view keeps standing below must exhaust the retry budget", instance.isInvalid());
            Assert.assertEquals("flush retry budget exhausted", instance.getStateReader().getInvalidationReason());
            Assert.assertEquals("charged turns until the duration budget runs out", STUCK_REBUILD_CHARGED_TURNS, turns);
            Assert.assertEquals("the uncharged fault and one per charged turn", 1 + STUCK_REBUILD_CHARGED_TURNS, instance.getRefreshFaultCount());
            Assert.assertEquals("every later rebuild stood where the view stood", baseAppliedAtFault, instance.getLastProcessedSeqTxn());
            Assert.assertEquals("every recovery rebuilt", 0, instance.getCheckpointRuntimeRestores());
            capture.drain();
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, recovery advanced the view "
                    + "\\[view=lv, fromSeqTxn=" + appliedSeqTxn + ", toSeqTxn=" + baseAppliedAtFault + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, window state recovered, retrying "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS - 1)
                    + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh budget exhausted, invalidating "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS) + ", " + EIO_READ_ERROR_RE);
            // Nothing past the base's applied head reached the output.
            assertViewRows("""
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    2026-01-02T09:20:00.000000Z\tacct-2\t16.0\t1
                    2026-01-02T09:20:00.000000Z\tacct-1\t44.0\t3
                    """);
        });
    }

    @Test
    public void testARestoreRepairThatGotPastAFaultDoesNotExcuseALaterFaultThatDoesNotClear() throws Exception {
        // The restore's route to the case above. The first fault's restore hands off to the
        // out-of-order repair, as in testARestoreWhoseRepairGetsPastAMidDrainFaultEndsTheRetryStreak,
        // which consumes the commits the fault stopped, and the turn goes uncharged. Once that
        // repair has resolved the out-of-order commit, a later restore's replay does not hand off
        // again: it puts the view back at its applied watermark, in front of the commit a fault
        // that does not clear stops. Each of those turns is charged until the duration budget runs
        // out.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, STUCK_REBUILD_RETRY_MAX);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, STUCK_REBUILD_RETRY_MAX_DURATION_MICROS);
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(O3_IN_THE_REPLAY_GAP);
            final LiveViewInstance instance = instance("lv");
            final long processedBeforeFault;
            final long baseHeadAtFault;
            final long stuckAt;
            int turns = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Day four as in testARestoreWhoseRepairGetsPastAMidDrainFaultEndsTheRetryStreak.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:00:00.000000Z', 'acct-1', 1.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:10:00.000000Z', 'acct-1', 2.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-04T09:20:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                runOnePass(job);
                processedBeforeFault = instance.getLastProcessedSeqTxn();
                baseHeadAtFault = engine.getTableSequencerAPI().lastTxn(baseToken);
                fault.arm(1);
                runOnePass(job);
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals(
                        "the repair must have come out of the failing turn's own in-process restore",
                        1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals(
                        "the restore's repair must have consumed every commit the fault stopped",
                        baseHeadAtFault,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertEquals("a restore whose repair moved the view forward is not charged", 0, instance.getFlushRetryCount());

                // Day five: one commit the view drains clean, then day four's shape again, one
                // commit with a task of its own and two coalesced behind it.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-05T09:00:00.000000Z', 'acct-1', 256.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-05T09:10:00.000000Z', 'acct-1', 512.0)");
                drainWalQueue();
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-05T09:20:00.000000Z', 'acct-1', 1024.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-05T09:30:00.000000Z', 'acct-1', 2048.0)");
                drainWalQueue();
                runOnePass(job);
                stuckAt = instance.getLastProcessedSeqTxn();
                final long baseHead = engine.getTableSequencerAPI().lastTxn(baseToken);
                Assert.assertEquals("the two coalesced commits wait for the next turn", baseHead - 2, stuckAt);
                // Each turn feeds the first coalesced commit and fails the read of the second.
                for (; turns < STUCK_REBUILD_MAX_DRIVES && !instance.isInvalid(); turns++) {
                    fault.arm(1);
                    engine.getLiveViewStateStore().notifyBaseTableCommit(baseToken, baseHead);
                    runOnePass(job);
                    Assert.assertTrue("every turn must meet the fault again", fault.hasFired());
                }
            }

            Assert.assertTrue("a fault that does not clear must exhaust the retry budget", instance.isInvalid());
            Assert.assertEquals("flush retry budget exhausted", instance.getStateReader().getInvalidationReason());
            Assert.assertEquals("charged turns until the duration budget runs out", STUCK_REBUILD_CHARGED_TURNS, turns);
            Assert.assertEquals("the uncharged fault and one per charged turn", 1 + STUCK_REBUILD_CHARGED_TURNS, instance.getRefreshFaultCount());
            Assert.assertEquals("every later restore put the view back where it stood", stuckAt, instance.getLastProcessedSeqTxn());
            Assert.assertEquals("every recovery restored", 1 + STUCK_REBUILD_CHARGED_TURNS, instance.getCheckpointRuntimeRestores());
            capture.drain();
            capture.assertLoggedRE("live view O3 replay \\[view=lv, .*advanceTo=" + processedBeforeFault
                    + ", pinnedSeqTxn=" + baseHeadAtFault + ", ");
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, recovery advanced the view "
                    + "\\[view=lv, fromSeqTxn=" + processedBeforeFault + ", toSeqTxn=" + baseHeadAtFault + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, window state recovered, retrying "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS - 1)
                    + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh budget exhausted, invalidating "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(STUCK_REBUILD_CHARGED_TURNS) + ", " + EIO_READ_ERROR_RE);
            // Nothing past the commit the view stood on reached the output.
            assertViewRows(O3_GAP_OUTPUT + DAY_FOUR_OUTPUT
                    + "2026-01-05T09:00:00.000000Z\tacct-1\t256.0\t1\n"
                    + "2026-01-05T09:10:00.000000Z\tacct-1\t768.0\t2\n");
        });
    }

    @Test
    public void testAMidDrainBreachARebuildGetsPastStillInvalidatesTheView() throws Exception {
        // A rebuild that moves the view forward ends the retry streak and charges nothing, but
        // not for a breach of the view's own refresh memory limit. The view's working set does not
        // fit the limit its operator set, and a rebuild answers nothing about that, so the breach
        // invalidates the view on its first turn with the tracker's own message, as it does
        // behind a restore.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        // Fits everything the view does here; the fault supplies the breach.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 67_108_864);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            final LiveViewInstance instance = instance("lv");
            writeRepairMarker(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in insertThreeAndFailMidDrain, with the read of the third commit breaching
                // the limit instead of failing its open.
                setCurrentMicros(instance.getLastFlushTimeUs());
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                drainWalQueue();
                execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                fault.armBreach(2, instance.getMemoryTracker());
                drainJob(job);
                Assert.assertTrue("the mid-drain segment read must have breached the limit", fault.hasFired());
            }

            Assert.assertEquals(
                    "the rebuild must have moved the view past the commits the breach stopped",
                    engine.getTableSequencerAPI().lastTxn(baseToken),
                    instance.getLastProcessedSeqTxn()
            );
            Assert.assertEquals("the breach is the one fault", 1, instance.getRefreshFaultCount());
            Assert.assertTrue("a breach of the view's own limit must invalidate it", instance.isInvalid());
            TestUtils.assertContains(instance.getInvalidationReason(), "query memory limit exceeded [workload=LIVE_VIEW_REFRESH");
            capture.drain();
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertLogged("live view exceeded its refresh memory limit, invalidating [view=lv");
            capture.assertNotLogged("recovery advanced the view");
        });
    }

    @Test
    public void testAMidDrainFaultThatOutlastsTheRetryCountThenClearsLetsTheViewConverge() throws Exception {
        // A mid-drain fault whose restore puts the view back in front of the commits it stopped is
        // charged to the duration budget alone, so a transient one - a disk freeing up, a burst of
        // open files, a read error on network storage - gets the whole duration budget to clear, and
        // the retry backoff paces the turns meanwhile. Here the fault outlasts the count budget twice
        // over, well inside the duration budget, and then clears: the view must ride it out.
        // A different fault in the same streak, one that strikes before the turn feeds a row and is
        // counted, must then find the count budget as the recovered faults before it left it.
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            Assert.assertTrue(
                    "the fault must outlast the count budget",
                    TRANSIENT_FAULT_TURNS > engine.getConfiguration().getLiveViewFlushRetryMax()
            );
            final long maxDurationMicros = engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros();
            Assert.assertTrue(
                    "the fault must clear inside the duration budget",
                    refreshRetryStreakMicros(TRANSIENT_FAULT_TURNS + 2) < maxDurationMicros
            );
            final LiveViewInstance instance = instance("lv");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            final long firstFaultUs = instance.getLastFlushTimeUs();
            final int retryCountWhileFaulting;
            final long retryStartWhileFaulting;
            int turns = 0;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // As in insertThreeAndFailMidDrain, with one job pass rather than a drain: the pass
                // after the failing one would derive the three commits into the lead again, and the
                // next flush would commit them with no fault left to meet.
                setCurrentMicros(firstFaultUs);
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                drainWalQueue();
                execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                drainWalQueue();
                fault.arm(2);
                job.run();
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertEquals("the recovery must have restored the runtime", 1, instance.getCheckpointRuntimeRestores());
                Assert.assertEquals("the restore puts the view back where it stood", durableSeqTxn, instance.getLastProcessedSeqTxn());
                // Every turn after it re-drains the three commits from the durable output, feeds
                // the first and fails the read of the second.
                for (; turns < TRANSIENT_FAULT_TURNS && !instance.isInvalid(); turns++) {
                    fault.arm(1);
                    runOnePass(job);
                    Assert.assertTrue("every turn must meet the fault again", fault.hasFired());
                }
                Assert.assertFalse(
                        "a mid-drain fault that clears inside the duration budget must not invalidate the view",
                        instance.isInvalid()
                );
                Assert.assertEquals(TRANSIENT_FAULT_TURNS, turns);
                Assert.assertEquals(
                        "every turn's fault struck mid-drain, and its recovery restored",
                        TRANSIENT_FAULT_TURNS + 1,
                        instance.getCheckpointRuntimeRestores()
                );
                Assert.assertEquals("no restore got past the fault", durableSeqTxn, instance.getLastProcessedSeqTxn());
                retryCountWhileFaulting = instance.getFlushRetryCount();
                retryStartWhileFaulting = instance.getFlushRetryStartUs();

                // A different fault in the same streak: the first read of the next turn, before the
                // turn feeds a row, so no recovery runs and the turn is counted.
                fault.arm(0);
                runOnePass(job);
                Assert.assertTrue("the next turn's first read must have been failed", fault.hasFired());
                Assert.assertFalse("the recovered faults must not have spent the count budget", instance.isInvalid());
                Assert.assertEquals("the counted fault is the streak's first", 1, instance.getFlushRetryCount());
                Assert.assertEquals("the streak still measures from the first fault", firstFaultUs, instance.getFlushRetryStartUs());

                // The fault has cleared.
                driveRefreshToQuiescence(job);
            }

            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the recovered faults leave the count alone", 0, retryCountWhileFaulting);
            Assert.assertEquals("the first recovered fault starts the duration clock", firstFaultUs, retryStartWhileFaulting);
            Assert.assertEquals("the turn that got past the fault zeroes the streak", 0, instance.getFlushRetryCount());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            Assert.assertEquals("the mid-drain faults and the counted one", TRANSIENT_FAULT_TURNS + 2, instance.getRefreshFaultCount());
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getLastProcessedSeqTxn());
            capture.drain();
            capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, window state recovered, retrying "
                    + "\\[view=lv, retryCount=0, elapsedUs=" + refreshRetryStreakMicros(TRANSIENT_FAULT_TURNS + 1) + ", " + EIO_READ_ERROR_RE);
            capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed \\[view=lv, retryCount=1, " + EIO_READ_ERROR_RE);
            capture.assertNotLogged("live view refresh budget exhausted");
            assertViewRows(SEVEN_ROWS_OUTPUT);
        });
    }

    @Test
    public void testAGateRecoveryThatGetsPastAFailedRecoveryEndsTheRetryStreakBeforeAFirstReadFault() throws Exception {
        failARecoveryThenGetPastItAtTheGateBeforeAFault(false);
    }

    @Test
    public void testAGateRecoveryThatGetsPastAFailedRecoveryEndsTheRetryStreakBeforeAMidDrainFault() throws Exception {
        failARecoveryThenGetPastItAtTheGateBeforeAFault(true);
    }

    /**
     * Asserts the view's accumulators came back from its own timeline while it was refreshing,
     * rather than from a rebuild: the runtime restores counted, no timeline retired, no restart
     * rebuild started, and no debt left over.
     */
    private static void assertRestoredInProcess(LiveViewInstance instance, long expectedRestores) {
        Assert.assertEquals(
                "the view must have restored its runtime from the timeline while refreshing",
                expectedRestores,
                instance.getCheckpointRuntimeRestores()
        );
        Assert.assertEquals("a restore retires no timeline", 0, instance.getCheckpointTimelineResets());
        Assert.assertEquals("a restore starts no rebuild", 0, instance.getCheckpointRebuildAttempts());
        Assert.assertFalse("a restore settles the window-state debt", instance.isWindowStateDirty());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
    }

    /**
     * Forty ten-second groups on 2026-01-02 from midnight, each with one row for each of eight
     * accounts, as VALUES tuples.
     */
    private static String indexedAccountSeed() {
        final StringBuilder seed = new StringBuilder();
        for (int group = 0; group < 40; group++) {
            final int seconds = group * 10;
            for (int account = 1; account <= 8; account++) {
                if (!seed.isEmpty()) {
                    seed.append(", ");
                }
                seed.append(String.format(
                        "('2026-01-02T00:%02d:%02d.000000Z', 'acct-%d', %d.0)",
                        seconds / 60,
                        seconds % 60,
                        account,
                        group * 10 + account
                ));
            }
        }
        return seed.toString();
    }

    /**
     * Asserts the view counts and filters its SYMBOL key exactly as the base does. Both reads
     * resolve the key by its raw id against the table's own symbol table, so a row the view stores
     * under an id its table committed to another account counts and matches as that account.
     *
     * @param accountCounts one {@code account\tcount} line per account, in account order
     */
    private void assertAccountsMatchTheBase(String accountCounts) throws Exception {
        for (int i = 0; i < 2; i++) {
            final String table = i == 0 ? "tx" : "lv";
            assertQuery("SELECT account_id, count() FROM " + table + " ORDER BY account_id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("account_id\tcount\n" + accountCounts);
            assertQuery("SELECT created_at, account_id FROM " + table + " WHERE account_id = 'acct-3'")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .returns("""
                            created_at\taccount_id
                            2026-01-02T09:20:00.000000Z\tacct-3
                            """);
        }
    }

    /**
     * Fails the first open of the base's own {@code amount} column in the 2026-01-03 partition
     * once {@code isArmed} is set. The applied scan of a deduplicating base reaches that file only
     * after it appended every row of the day before, and the raw-WAL drain never opens it.
     */
    private static FilesFacade newDayAmountOpenFault(AtomicReference<String> baseDir, AtomicBoolean isArmed, AtomicBoolean hasFired) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final String dir = baseDir.get();
                if (isArmed.get()
                        && dir != null
                        && Utf8s.containsAscii(name, dir)
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.containsAscii(name, "2026-01-03")
                        && Utf8s.endsWithAscii(name, "amount.d")
                        && isArmed.compareAndSet(true, false)) {
                    hasFired.set(true);
                    return -1;
                }
                return super.openRO(name);
            }
        };
    }

    /**
     * Asserts the view counts its SYMBOL key exactly as the base does.
     *
     * @param accountCounts one {@code account\tcount} line per account, in account order
     */
    private void assertAccountCountsMatchTheBase(String accountCounts) throws Exception {
        for (int i = 0; i < 2; i++) {
            final String table = i == 0 ? "tx" : "lv";
            assertQuery("SELECT account_id, count() FROM " + table + " ORDER BY account_id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("account_id\tcount\n" + accountCounts);
        }
    }

    /**
     * Asserts the view filters {@code account} by its SYMBOL key exactly as the base does: the
     * filter resolves the value to a raw id through the table's own symbol table, so a row the
     * view stores under an id its table committed to another account matches as that account.
     */
    private void assertAccountRowsMatchTheBase(String account, String expectedRows) throws Exception {
        for (int i = 0; i < 2; i++) {
            final String table = i == 0 ? "tx" : "lv";
            assertQuery("SELECT created_at, account_id FROM " + table + " WHERE account_id = '" + account + "'")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .returns("created_at\taccount_id\n" + expectedRows);
        }
    }

    /**
     * A deduplicating base whose range the dedup signal cannot vouch for drains the applied base,
     * appending each output row to the view's WAL writer as it goes. A fault after the drain
     * appended a row with an account new to the view's table fails the turn, and the restore
     * discards it - but the rolled-back writer goes back to the pool still holding that account
     * in its symbol map, so the next commit through it assigns that account the first new id,
     * whatever order the retry meets the new accounts in. A later commit with another new
     * account that sorts below the discarded row makes the retry meet it first, and intern it at
     * that first new id. The ids the tier serves would then swap the two accounts' labels, at
     * an unchanged symbol count, so the publish has to compare the values themselves and rebuild
     * the slot from disk.
     *
     * @param isLaterAccountBelow whether the later account's row sorts below the discarded row,
     *                            which reorders the new accounts against the writer's
     */
    private void assertAppliedScanRestoreKeepsAccountsOnTheirIds(boolean isLaterAccountBelow) throws Exception {
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean hasFired = new AtomicBoolean();
        final AtomicReference<String> baseDir = new AtomicReference<>();
        assertMemoryLeak(newDayAmountOpenFault(baseDir, isArmed, hasFired), () -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            baseDir.set(engine.verifyTableName("tx").getDirName());
            insertAndRefresh(FOUR_ROWS);
            final long rawWalCleanCycles = instance("lv").getDedupRawWalCleanCycles();
            final String laterTs = isLaterAccountBelow ? "2026-01-02T09:20:00.000000Z" : "2026-01-02T09:40:00.000000Z";
            final String laterRow = laterTs + "\tacct-Y\t8.0\t1\n";
            final String accountXRow = "2026-01-02T09:30:00.000000Z\tacct-X\t2.0\t1\n";
            final String expectedRows = """
                    created_at\taccount_id\tcumulative_sum\tcumulative_count
                    2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
                    2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
                    2026-01-02T09:00:00.000000Z\tacct-1\t4.0\t1
                    2026-01-02T09:10:00.000000Z\tacct-1\t12.0\t2
                    """
                    + (isLaterAccountBelow ? laterRow + accountXRow : accountXRow + laterRow)
                    + "2026-01-03T09:00:00.000000Z\tacct-1\t4.0\t1\n";
            final String accounts = """
                    acct-1\t4
                    acct-2\t1
                    acct-X\t1
                    acct-Y\t1
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The first commit brings acct-X twice on one key, which the base collapses, so
                // the dedup signal cannot vouch for the range and the turn drains the applied
                // base. The second opens a new day, whose first column open the fault fails.
                execute("""
                        INSERT INTO tx VALUES
                            ('2026-01-02T09:30:00.000000Z', 'acct-X', 1.0),
                            ('2026-01-02T09:30:00.000000Z', 'acct-X', 2.0)
                        """);
                execute("INSERT INTO tx VALUES ('2026-01-03T09:00:00.000000Z', 'acct-1', 4.0)");
                drainWalQueue();
                isArmed.set(true);
                runOnePass(job);
                Assert.assertTrue("the applied scan's open of the new day must have been failed", hasFired.get());
                capture.drain();
                capture.assertLoggedRE(RESTORED + " \\[view=lv, cause=mid-drain refresh failure, ");

                // acct-Y lands before the retry, above the frontier the restore went back to.
                execute("INSERT INTO tx VALUES ('" + laterTs + "', 'acct-Y', 8.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            assertViewRows(expectedRows);
            assertAccountCountsMatchTheBase(accounts);
            assertAccountRowsMatchTheBase("acct-X", "2026-01-02T09:30:00.000000Z\tacct-X\n");
            assertAccountRowsMatchTheBase("acct-Y", laterTs + "\tacct-Y\n");

            final LiveViewInstance instance = instance("lv");
            Assert.assertEquals("the turns must have drained the applied base", rawWalCleanCycles, instance.getDedupRawWalCleanCycles());
            assertRestoredInProcess(instance, 1);
            Assert.assertEquals("the applied scan fault is the one fault", 1, instance.getRefreshFaultCount());
            capture.drain();
            // No reader pinned the tier, so the restore's rebuild took back the id the discarded
            // turn interned acct-X at.
            capture.assertOnlyOnce(SYMBOL_IDS_REWOUND);
            if (isLaterAccountBelow) {
                capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            } else {
                capture.assertNotLogged(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            }
            assertSlotStampedAtTheViewTable(instance);

            // One more new account: the writer no longer holds anything the turn did not append,
            // so the drain and the apply agree on its id and the publish keeps the slot.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO tx VALUES ('2026-01-03T10:00:00.000000Z', 'acct-Z', 16.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            assertViewRows(expectedRows + "2026-01-03T10:00:00.000000Z\tacct-Z\t16.0\t1\n");
            assertAccountCountsMatchTheBase(accounts + "acct-Z\t1\n");
            assertAccountRowsMatchTheBase("acct-X", "2026-01-02T09:30:00.000000Z\tacct-X\n");
            assertAccountRowsMatchTheBase("acct-Z", "2026-01-03T10:00:00.000000Z\tacct-Z\n");
            capture.drain();
            if (isLaterAccountBelow) {
                capture.assertOnlyOnce(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            } else {
                capture.assertNotLogged(SYMBOL_IDS_OUT_OF_STEP_REBUILT);
            }
            Assert.assertEquals("the applied scan fault is the one fault", 1, instance("lv").getRefreshFaultCount());
            assertSlotStampedAtTheViewTable(instance("lv"));
        });
    }

    // The ROWS view's rows against its own query recomputed over the base.
    private void assertBoundedRowsViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(SELECT created_at, account_id, amount, " + BOUNDED_ROWS_WINDOW + " AS s FROM tx) ORDER BY 2, 1",
                "(SELECT created_at, account_id, amount, s FROM lv) ORDER BY 2, 1",
                LOG,
                true
        );
    }

    /**
     * Asserts the framed splice-tie case's view returns {@code expected}: the rows from
     * {@link #SPLICE_LATE_ROW} up when {@code isFromTheLateRow}, or else every row.
     */
    private void assertFramedViewRows(boolean isFromTheLateRow, String expected) throws Exception {
        final String query = isFromTheLateRow
                ? "SELECT * FROM lv WHERE created_at >= '2026-01-02T10:00:00.000000Z'"
                : "SELECT * FROM lv";
        assertQuery(query)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize(!isFromTheLateRow)
                .returns(expected);
    }

    /**
     * Runs a recovery whose restore meets {@link #COLLAPSED_DUPLICATE_ROWS}'s collapsed duplicate
     * over a base that lost day one - the restart's when {@code isRestart}, else the base schema
     * change's in place - and then, on the same refresh job, a second recovery whose restore is
     * declined behind a live repair marker, over a base that has also lost day two. The first
     * rebuild runs without the guard and follows the base. The second never replays anything, so
     * its rebuild has to compare again and refuse to drop day two: the stand-down belongs to the
     * rebuild behind the restore that failed, not to the job that ran it.
     */
    private void assertGuardStandsDownForTheMismatchedRestoresOwnRebuildOnly(boolean isRestart) throws Exception {
        assertMemoryLeak(() -> {
            createBase("DEDUP UPSERT KEYS(created_at, account_id)");
            createView();
            insertAndRefresh(COLLAPSED_DUPLICATE_ROWS);
            dropPartitionAndRefresh("2026-01-01", COLLAPSED_DUPLICATE_OUTPUT);
            if (isRestart) {
                shutdown();
                engine.buildViewGraphs();
            }
            final String restatedRows;
            final LiveViewRebuildRestatementGuard guard;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                if (isRestart) {
                    driveRefreshToQuiescence(job);
                    restatedRows = COLLAPSED_DUPLICATE_RESTATED_OUTPUT;
                } else {
                    execute("ALTER TABLE tx ADD COLUMN note INT");
                    execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                            + "('2026-01-03T10:00:00.000000Z', 'acct-1', 30.0), "
                            + "('2026-01-03T10:00:00.000000Z', 'acct-1', 32.0)");
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    restatedRows = COLLAPSED_DUPLICATE_RESTATED_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-1\t48.0\t2\n";
                }
                Assert.assertEquals(
                        LiveViewRebuildRestatementGuard.ABSTAIN_DEDUP_RESTORE_MISMATCH,
                        job.rebuildRestatementGuardForTest().getAbstention()
                );
                Assert.assertFalse(instance("lv").isCheckpointRecoveryBlocked());
                assertViewRows(restatedRows);

                execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-02'");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                assertViewRows(restatedRows);
                writeRepairMarker(instance("lv"));
                execute("ALTER TABLE tx ADD COLUMN note2 INT");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                        + "('2026-01-03T11:00:00.000000Z', 'acct-1', 1.0), "
                        + "('2026-01-03T11:00:00.000000Z', 'acct-1', 2.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                guard = job.rebuildRestatementGuardForTest();
            }

            Assert.assertTrue("the guard must refuse the second rebuild", instance("lv").isCheckpointRecoveryBlocked());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_NONE, guard.getAbstention());
            Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_HISTORY_FLOOR, guard.getVerdict());
            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=base table metadata change, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view rebuild from the applied base refused, it would drop rows the view retains "
                    + "[view=lv, cause=base table metadata change, ");
            assertViewRows(restatedRows);
        });
    }

    /**
     * Asserts the view's last recovery fell back from a restore whose replay of the raw base WAL did
     * not reproduce the view's durable output - what a duplicate a deduplicating base collapsed in
     * the replay gap makes it do - and that the whole-view rebuild behind it ran without the
     * restatement guard, so the view follows its base and keeps refreshing.
     *
     * @param restoreFailedRE how the recovery's caller logs the failed restore, as a regex
     */
    private void assertGuardStoodDownBehindADedupRestoreMismatch(LiveViewRebuildRestatementGuard guard, String restoreFailedRE) {
        final LiveViewInstance instance = instance("lv");
        Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals(LiveViewRebuildRestatementGuard.ABSTAIN_DEDUP_RESTORE_MISMATCH, guard.getAbstention());
        Assert.assertEquals(LiveViewRebuildRestatementGuard.VERDICT_NONE, guard.getVerdict());
        capture.drain();
        capture.assertLoggedRE(restoreFailedRE + ".*does not match durable materialization");
        capture.assertLogged(DEDUP_RESTORE_MISMATCH_STAND_DOWN);
        capture.assertNotLogged("live view rebuild from the applied base refused");
    }

    /**
     * Seeds the view over {@link #SPLICE_SEEDED_ROWS}, which seals its root on the third day's row,
     * commits {@link #SPLICE_ROOT_TIE} on that root's timestamp, drops the base's first day when
     * {@code isDayLost}, and lands {@link #SPLICE_LATE_ROW} in the second day. A restart then has to
     * restore from the timeline, and the next row has to carry the tied row's amount forward.
     * <p>
     * The second day is a closed segment below the frontier, so a repair quoted the runtime
     * frontier would converge at that day's end, below the newest root, and keep the primary
     * runtime. Such a repair published a splice stamped at the late commit and kept the newest
     * root as it was, without the tied row whose commit sits below that stamp, so no restore could
     * replay it: the row count failed and the rebuild took over, which the guard refuses over a
     * base without dedup keys that lost a day. The refresh withholds the frontier while the
     * newest root's timestamp group has grown, so the closed-segment loop does not run and the
     * union range's anchor arm localizes behind the end of the table, which re-versions the
     * newest root from the pinned snapshot, tie included.
     */
    private void assertRestartRestoresATieAfterALateRowBelowIt(boolean isDayLost) throws Exception {
        createBase("");
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + SPLICE_SEEDED_ROWS);
        drainWalQueue();
        createView();
        insertAndRefresh(SPLICE_ROOT_TIE);
        assertSingleRootAt("2026-01-03T09:00:00.000000Z");
        if (isDayLost) {
            dropPartitionAndRefresh("2026-01-01", SPLICE_ROOT_TIE_OUTPUT);
        }
        final long segmentRepairs = landALateRowBelowATieOnTheNewestRoot();

        restartAndAssertRestoredOverATie(SPLICE_LATE_ROW_OUTPUT, 0);
        // A runtime that missed the tied row would answer 32.0 over one row.
        insertAndRefresh("('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)");
        assertViewRows(SPLICE_LATE_ROW_OUTPUT + "2026-01-03T09:10:00.000000Z\tacct-2\t40.0\t2\n");
        Assert.assertEquals("closed segments repaired over their own range", 0, segmentRepairs);
    }

    /**
     * {@link #assertRestartRestoresATieAfterALateRowBelowIt} over a view whose bounded
     * {@code frame} carries no anchor, so the late row's repair has no closed segment to take on
     * its own. The seeded rows let the frame converge below the tie, where a repair quoted the
     * runtime frontier would keep the primary runtime and publish a splice that keeps the newest
     * root as it was, without the tied row. No restore could replay that row, so the restart
     * would fall back to the rebuild, which the guard refuses over a base without dedup keys
     * that lost a day. The refresh withholds the frontier while the newest root's timestamp
     * group has grown, so the RANGE arm localizes behind the end of the table and the ROWS arm
     * does not localize at all.
     * <p>
     * Over a base that lost a day the ROWS arm's boundary rebuild recomputes a retained row
     * below the late row without the lost day, so those cases assert the view from the late row
     * up: the restart route and the next row are what the tie decides.
     *
     * @param seededRows          the base the view is created over, in one commit; the seed sweep
     *                            seals its root on the third day's row
     * @param rowsBelowTheLateRow the view's rows below {@link #SPLICE_LATE_ROW}
     * @param rowsFromTheLateRow  the view's rows from {@link #SPLICE_LATE_ROW} up, the tie included
     */
    private void assertRestartRestoresATieAfterALateRowBelowItInAFramedView(
            String frame,
            String seededRows,
            String rowsBelowTheLateRow,
            String rowsFromTheLateRow,
            boolean isDayLost
    ) throws Exception {
        createBase("");
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + seededRows);
        drainWalQueue();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER (PARTITION BY account_id ORDER BY created_at "
                + frame + ") AS windowed_sum FROM tx");
        insertAndRefresh(SPLICE_ROOT_TIE);
        assertSingleRootAt("2026-01-03T09:00:00.000000Z");
        if (isDayLost) {
            execute("ALTER TABLE tx DROP PARTITION LIST '2026-01-01'");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }
        }
        insertAndRefresh(SPLICE_LATE_ROW);
        final String expectedRows = "created_at\taccount_id\twindowed_sum\n"
                + (isDayLost ? "" : rowsBelowTheLateRow)
                + rowsFromTheLateRow;
        assertFramedViewRows(isDayLost, expectedRows);

        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        Assert.assertFalse("the view must keep refreshing", instance("lv").isCheckpointRecoveryBlocked());
        assertNoRefreshFaults("lv");
        assertFramedViewRows(isDayLost, expectedRows);
        // A runtime that missed the tied row would answer 32.0 over one row.
        insertAndRefresh("('2026-01-03T09:10:00.000000Z', 'acct-2', 32.0)");
        assertFramedViewRows(isDayLost, expectedRows + "2026-01-03T09:10:00.000000Z\tacct-2\t40.0\n");
    }

    /**
     * Seeds the view over {@link #SEEDED_ROWS}, which seals its root on the last of them, and then
     * commits {@link #SEED_ROOT_TIE} on that root's timestamp. Drops the base's first day when
     * {@code isDayLost}, and restarts. The restart has to restore from the seed root with the tied
     * row replayed, and the next row has to carry the tied row's amount forward.
     */
    private void assertRestartRestoresATieOnTheSeedRoot(boolean isDayLost) throws Exception {
        createBase("");
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + SEEDED_ROWS);
        drainWalQueue();
        createView();
        insertAndRefresh(SEED_ROOT_TIE);
        assertSingleRootAt("2026-01-02T09:10:00.000000Z");
        assertViewRows(SEED_ROOT_TIE_OUTPUT);
        if (isDayLost) {
            dropPartitionAndRefresh("2026-01-01", SEED_ROOT_TIE_OUTPUT);
        }

        restartAndAssertRestoredOverATie(SEED_ROOT_TIE_OUTPUT, 1);
        // A runtime that missed the tied row would answer 44.0 over three rows.
        insertAndRefresh("('2026-01-02T09:20:00.000000Z', 'acct-1', 32.0)");
        assertViewRows(SEED_ROOT_TIE_OUTPUT + "2026-01-02T09:20:00.000000Z\tacct-1\t60.0\t4\n");
    }

    /**
     * Seeds the view as {@link #assertRestartRestoresATieAfterALateRowBelowIt} does, then lands
     * {@link #SPLICE_LATE_ROW} in the second day and a row above the tie in the third in one
     * commit, and restarts while the repair that commit triggers is parked on its turn budget.
     * The restart has to restore from the timeline, and the next row has to carry the tied row's
     * amount forward.
     * <p>
     * The repair spans a closed segment and the open one. Taken per segment, it would publish
     * the second day's splice first, stamped at the watermark the tie's commit already sits
     * under, and keep the newest root as it was, without the tied row. Until the open segment's
     * own repair published, a restore would replay nothing above that stamp, fail its row count
     * and fall back to the rebuild, which the guard refuses over a base without dedup keys that
     * lost a day. The refresh takes the union range instead while the newest root's timestamp
     * group has grown. The helper drives one pass at a time until a repair outside any segment
     * loop parks - the union range, or a decomposition's residual - and restarts there.
     */
    private void assertRestartRestoresATieWhileARepairAcrossTwoDaysIsParked(boolean isDayLost) throws Exception {
        createBase("");
        execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + SPLICE_SEEDED_ROWS);
        drainWalQueue();
        createView();
        insertAndRefresh(SPLICE_ROOT_TIE);
        assertSingleRootAt("2026-01-03T09:00:00.000000Z");
        if (isDayLost) {
            dropPartitionAndRefresh("2026-01-01", SPLICE_ROOT_TIE_OUTPUT);
        }
        final LiveViewInstance instance = instance("lv");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES "
                    + SPLICE_LATE_ROW + ", ('2026-01-03T10:00:00.000000Z', 'acct-2', 32.0)");
            drainWalQueue();
            boolean isParkedOutsideASegmentLoop = false;
            for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !isParkedOutsideASegmentLoop; pass++) {
                runOnePass(job);
                final LiveViewCheckpointRepairSession parked = instance.getSuspendedRepair();
                isParkedOutsideASegmentLoop = parked != null && !parked.getSegmentLoop().isOpen();
            }
            Assert.assertTrue(
                    "the union range, or a decomposition's residual, must park on its turn budget",
                    isParkedOutsideASegmentLoop
            );
        }

        final String viewRows = SPLICE_LATE_ROW_OUTPUT + "2026-01-03T10:00:00.000000Z\tacct-2\t40.0\t2\n";
        restartAndAssertRestoredOverATie(viewRows, 1);
        // A runtime that missed the tied row would answer 96.0 over two rows.
        insertAndRefresh("('2026-01-03T11:00:00.000000Z', 'acct-2', 64.0)");
        assertViewRows(viewRows + "2026-01-03T11:00:00.000000Z\tacct-2\t104.0\t3\n");
    }

    /**
     * Asserts the view's timeline holds a single root, on {@code timestamp}, and that the head
     * mirrors it: the root a tie case's restore stands on.
     */
    private void assertSingleRootAt(String timestamp) {
        Assert.assertEquals("the default cadence seals one boundary", 1, countSealedBoundaries("lv"));
        Assert.assertEquals(ts(timestamp), instance("lv").getHeadCheckpointMaxTs());
    }

    /**
     * Asserts that the last flush re-stamped the published slot as a subset of the view's table:
     * no stale marking, no lead, and the table's applied seqTxn as its stamp - the fence a read
     * passes to be served from the slot. The refresh job is closed, so nothing writes the slot.
     */
    private void assertSlotStampedAtTheViewTable(LiveViewInstance instance) {
        Assert.assertFalse("the tier must not be stale", instance.isTierStale());
        final LiveViewInMemoryTier tier = instance.getInMemoryTier();
        final LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
        Assert.assertEquals("the published slot must carry no lead", 0, slot.leadRowCount());
        try (TableReader reader = getReader("lv")) {
            Assert.assertEquals("the published slot must carry the table's applied seqTxn", reader.getSeqTxn(), slot.lvSeqTxn());
        }
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery(VIEW_ROWS_QUERY)
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    private int baseAccountSymbolCapacity() {
        try (TableReader reader = getReader("tx")) {
            return reader.getSymbolMapReader(reader.getMetadata().getColumnIndex("account_id")).getSymbolCapacity();
        }
    }

    private int baseColumnStructureVersion() {
        try (TableReader reader = getReader("tx")) {
            return reader.getTxFile().getColumnStructureVersion();
        }
    }

    private void createBase(String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
    }

    // A deduplicating base, so the drain reads the applied base through the compiled factory.
    private void createBaseWithAccountCapacity(int capacity) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL CAPACITY " + capacity + ", amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL DEDUP UPSERT KEYS(created_at, account_id)");
    }

    private void createView() throws Exception {
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum, "
                + "count(account_id) OVER w AS cumulative_count "
                + "FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
    }

    /**
     * Drops one base partition the view has already derived rows from and lets the view walk past
     * the DROP PARTITION, which keeps those rows: a rebuild from the applied base would drop them,
     * and the restatement guard would refuse it. Over a base with dedup keys the guard stands
     * down instead when the rebuild follows a restore whose replay of the raw base WAL does not
     * reproduce the view, and that rebuild drops those rows.
     */
    private void dropPartitionAndRefresh(String day, String expectedViewRows) throws Exception {
        execute("ALTER TABLE tx DROP PARTITION LIST '" + day + "'");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        assertViewRows(expectedViewRows);
        assertNoRefreshFaults("lv");
    }

    /**
     * Fails a mid-drain turn's recovery outright, so the view carries the window-state debt and a
     * charged streak into a later turn, and has that turn's gate - the recovery a turn runs first
     * when an earlier one left the debt - get past the commits the fault stopped. A fault later in
     * the same turn then arrives a whole duration budget after the first one. The gate's recovery
     * has to end the streak, as the recovery a fault asks for does when it moves the view forward:
     * one it left standing would still measure from the first fault, and that one later fault
     * would exhaust the duration budget and invalidate a view that had just caught up.
     * <p>
     * The base's apply is suspended while the first turn fails, so nothing drives the view while
     * it idles. It then applies the three commits the fault stopped, and a new commit it has not
     * applied yet drives the gate turn. A live repair marker declines the gate's restore, so its
     * rebuild from the applied base consumes the three commits. {@code isMidDrain} decides where
     * the gate turn's own fault strikes: on the new commit's first read, before the turn feeds a
     * row, or on the read of a second new commit after the turn fed the first, where the fault's
     * own recovery puts the view back at the gate's watermark.
     */
    private void failARecoveryThenGetPastItAtTheGateBeforeAFault(boolean isMidDrain) throws Exception {
        final LiveViewMidDrainFault fault = new LiveViewMidDrainFault();
        fault.reportReadErrno(ERRNO_EIO);
        assertMemoryLeak(fault.facade(), () -> {
            createBase("");
            createView();
            final TableToken baseToken = engine.verifyTableName("tx");
            fault.of(baseToken.getDirName());
            insertAndRefresh(FOUR_ROWS);
            final LiveViewInstance instance = instance("lv");
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            final long firstFaultUs = instance.getLastFlushTimeUs();
            final long appliedAtGate;
            final long gateTurnUs;
            execute("ALTER TABLE tx SUSPEND WAL");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The three commits of insertThreeAndFailMidDrain, left unapplied and failed in one
                // job pass. The fault fails the read of the third after the turn fed the second,
                // and both recoveries behind it fail too.
                setCurrentMicros(firstFaultUs);
                execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
                execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
                fault.arm(2);
                fault.armTimelineOpen();
                fault.armAppliedScan();
                job.run();
                Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
                Assert.assertFalse("the recovery's restore must have been failed", fault.isTimelineOpenArmed());
                Assert.assertTrue("the recovery's rebuild must have been failed", fault.hasAppliedScanFired());
                Assert.assertTrue("the failed recovery must leave the window-state debt", instance.isWindowStateDirty());
                Assert.assertEquals(durableSeqTxn, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("the failed recovery is charged", 1, instance.getFlushRetryCount());
                Assert.assertEquals(firstFaultUs, instance.getFlushRetryStartUs());

                // Nothing applies for the whole duration budget, so the fallback scan has nothing to
                // drive, and the debt and the streak stand.
                setCurrentMicros(currentMicros + engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros());
                driveRefreshToQuiescence(job);
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(1, instance.getFlushRetryCount());

                execute("ALTER TABLE tx RESUME WAL");
                drainWalQueue();
                execute("ALTER TABLE tx SUSPEND WAL");
                appliedAtGate = engine.getTableSequencerAPI().getTxnTracker(baseToken).getWriterTxn();
                Assert.assertEquals("the base applied the three commits", durableSeqTxn + 3, appliedAtGate);
                writeRepairMarker(instance);
                execute("INSERT INTO tx VALUES ('2026-01-02T09:50:00.000000Z', 'acct-1', 128.0)");
                if (isMidDrain) {
                    // A checkpoint freeze skips the turn this commit's notification drives, so the
                    // next commit's notification drains both.
                    Assert.assertTrue(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                    try {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        job.run();
                    } finally {
                        instance.endCheckpoint();
                    }
                    Assert.assertTrue("the frozen turn must leave the debt to the gate", instance.isWindowStateDirty());
                    execute("INSERT INTO tx VALUES ('2026-01-02T10:00:00.000000Z', 'acct-2', 256.0)");
                }
                fault.arm(isMidDrain ? 1 : 0);
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                gateTurnUs = currentMicros;
                job.run();
                Assert.assertTrue("the gate turn's drain must have met the fault", fault.hasFired());
                Assert.assertEquals(
                        "the gate's rebuild must have consumed the commits the first fault stopped",
                        appliedAtGate,
                        instance.getLastProcessedSeqTxn()
                );
                Assert.assertFalse(
                        "one fault a whole duration budget after the first must not invalidate a view the gate moved past it",
                        instance.isInvalid()
                );
                Assert.assertEquals(
                        "the gate turn's fault starts a streak of its own",
                        isMidDrain ? 0 : 1,
                        instance.getFlushRetryCount()
                );
                Assert.assertEquals(gateTurnUs, instance.getFlushRetryStartUs());

                execute("ALTER TABLE tx RESUME WAL");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals("the turn that got past the later fault zeroes its streak", 0, instance.getFlushRetryCount());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getFlushRetryStartUs());
            Assert.assertEquals("the two injected faults", 2, instance.getRefreshFaultCount());
            Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(baseToken), instance.getLastProcessedSeqTxn());
            capture.drain();
            capture.assertLogged("live view cannot restore its runtime from the checkpoint timeline, rebuilding from the applied base "
                    + "[view=lv, cause=mid-drain refresh failure, reason=prefix preservation repair marker present]");
            capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
            capture.assertNotLogged("live view refresh budget exhausted");
            final String gateOutput = SEVEN_ROWS_OUTPUT + "2026-01-02T09:50:00.000000Z\tacct-1\t172.0\t4\n";
            assertViewRows(isMidDrain ? gateOutput + "2026-01-02T10:00:00.000000Z\tacct-2\t336.0\t3\n" : gateOutput);
        });
    }

    /**
     * Fails a turn mid-drain on a view whose restore from the timeline cannot run, leaves the view
     * idle for longer than the flush-retry duration budget, then fails one turn more, and asserts
     * the view rides out the second fault. The caller creates the view, sets up why its restore
     * cannot run, and asserts the rows.
     * <p>
     * The base applies the three commits on {@code day} before the view drains them, and the fault
     * fails the read of the third after the turn has fed the second. So the mid-drain recovery's
     * rebuild from the applied base consumes all three, the one the fault stopped included, and no
     * later turn has anything left to get past. The recovery therefore has to end the retry
     * streak. One it left standing would still measure from the first fault when the second, on a
     * new commit on {@code nextDay}, arrives a minute later: that one fault would exhaust the
     * duration budget and invalidate a view that recovered long before.
     */
    private void failMidDrainIntoARebuildThenIdleThenFailOnce(LiveViewMidDrainFault fault, String day, String nextDay) throws Exception {
        final TableToken baseToken = engine.verifyTableName("tx");
        fault.of(baseToken.getDirName());
        final LiveViewInstance instance = instance("lv");
        final int retryCountAfterRebuild;
        final long retryStartAfterRebuild;
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            // As in insertThreeAndFailMidDrain: the first commit drains on a turn of its own, and
            // the next two coalesce behind it.
            setCurrentMicros(instance.getLastFlushTimeUs());
            execute("INSERT INTO tx VALUES ('" + day + "T09:20:00.000000Z', 'acct-2', 16.0)");
            drainWalQueue();
            execute("INSERT INTO tx VALUES ('" + day + "T09:30:00.000000Z', 'acct-1', 32.0)");
            execute("INSERT INTO tx VALUES ('" + day + "T09:40:00.000000Z', 'acct-2', 64.0)");
            drainWalQueue();
            fault.arm(2);
            drainJob(job);
            driveRefreshToQuiescence(job);
            Assert.assertTrue("the mid-drain segment read must have been failed", fault.hasFired());
            Assert.assertEquals(
                    "the rebuild must have consumed every commit the fault stopped",
                    engine.getTableSequencerAPI().lastTxn(baseToken),
                    instance.getLastProcessedSeqTxn()
            );
            retryCountAfterRebuild = instance.getFlushRetryCount();
            retryStartAfterRebuild = instance.getFlushRetryStartUs();

            // No base commit for longer than the duration budget, so no turn has work to do.
            setCurrentMicros(currentMicros + engine.getConfiguration().getLiveViewFlushRetryMaxDurationMicros());
            driveRefreshToQuiescence(job);

            // One unrelated fault: the first read of a new commit, before the turn feeds a row,
            // so no recovery runs and the turn is charged as it stands. The base applies the
            // commit before the fault is armed, so the view's read is the one it fails.
            execute("INSERT INTO tx VALUES ('" + nextDay + "T09:00:00.000000Z', 'acct-1', 128.0)");
            drainWalQueue();
            fault.arm(0);
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            job.run();
            Assert.assertTrue("the new commit's read must have been failed", fault.hasFired());
            Assert.assertFalse("one fault long after the view recovered must not invalidate it", instance.isInvalid());
            Assert.assertEquals("the later fault starts a streak of its own", 1, instance.getFlushRetryCount());
            driveRefreshToQuiescence(job);
        }

        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals("the two injected faults", 2, instance.getRefreshFaultCount());
        Assert.assertEquals("the turn that drained past the later fault zeroes its streak", 0, instance.getFlushRetryCount());
        Assert.assertEquals(
                "the view must have materialized the commit the later fault stopped",
                engine.getTableSequencerAPI().lastTxn(baseToken),
                instance.getLastProcessedSeqTxn()
        );
        Assert.assertEquals("the rebuild that got past the first fault ends its streak", 0, retryCountAfterRebuild);
        Assert.assertEquals(Numbers.LONG_NULL, retryStartAfterRebuild);
        Assert.assertEquals("the recovery rebuilt rather than restored", 0, instance.getCheckpointRuntimeRestores());
        capture.drain();
        capture.assertLogged("live view recomputed window state from applied base [view=lv, cause=mid-drain refresh failure]");
        // The fault the rebuild got past, which nothing else reports.
        capture.assertLoggedRE("E i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed, recovery advanced the view "
                + "\\[view=lv, fromSeqTxn=\\d+, toSeqTxn=\\d+, " + EIO_READ_ERROR_RE);
        capture.assertLoggedRE("C i\\.q\\.c\\.l\\.LiveViewRefreshJob live view refresh failed \\[view=lv, retryCount=1, " + EIO_READ_ERROR_RE);
        capture.assertNotLogged("window state recovered, retrying");
        capture.assertNotLogged("live view refresh budget exhausted");
    }

    /**
     * One commit per argument, each refreshed before the next, so the view's watermark sits on
     * the last commit and its first root on the first.
     */
    private void insertAndRefresh(String... commits) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            for (String values : commits) {
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + values);
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
        }
        assertNoRefreshFaults("lv");
    }

    /**
     * Commits three rows and has the refresh fail between feeding the second and reading the
     * third: the first gets a refresh task of its own, the next two coalesce behind it and drain
     * in one pass, and the fault fails that pass's read of the third commit's timestamp column.
     * <p>
     * The clock stays on the view's last flush throughout, so the first commit is an un-flushed
     * lead when the fault lands - the lead the recovery has to drop - and whatever the recovery
     * leaves is still unflushed when this returns. The caller drives the flush.
     */
    private void insertThreeAndFailMidDrain(LiveViewRefreshJob job, LiveViewMidDrainFault fault) throws Exception {
        setCurrentMicros(instance("lv").getLastFlushTimeUs());
        execute("INSERT INTO tx VALUES ('2026-01-02T09:20:00.000000Z', 'acct-2', 16.0)");
        drainWalQueue();
        execute("INSERT INTO tx VALUES ('2026-01-02T09:30:00.000000Z', 'acct-1', 32.0)");
        execute("INSERT INTO tx VALUES ('2026-01-02T09:40:00.000000Z', 'acct-2', 64.0)");
        drainWalQueue();
        fault.arm(2);
        drainJob(job);
        Assert.assertTrue("the mid-drain segment read must have been failed exactly once", fault.hasFired());
    }

    /**
     * Commits {@link #SPLICE_LATE_ROW} into the second day, below the tie on the newest root, and
     * drives the repair it triggers.
     *
     * @return the closed segments the repair took over their own range
     */
    private long landALateRowBelowATieOnTheNewestRoot() throws Exception {
        final long segmentRepairs;
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO tx (created_at, account_id, amount) VALUES " + SPLICE_LATE_ROW);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            segmentRepairs = job.segmentRepairCountForTest();
        }
        assertNoRefreshFaults("lv");
        assertViewRows(SPLICE_LATE_ROW_OUTPUT);
        return segmentRepairs;
    }

    private long newestGeneration(LiveViewInstance instance) {
        try (
                LiveViewCheckpointMetaStore store = openStore(instance);
                LiveViewCheckpointGenerationPin pin = store.pin()
        ) {
            return pin.getGeneration();
        }
    }

    // The base-table coordinate the whole published generation is valid against, and the floor a
    // restart's replay of the raw base WAL starts above.
    private long normalizedBaseSeqTxn(LiveViewInstance instance) {
        try (LiveViewCheckpointMetaStore store = openStore(instance)) {
            return store.getSuperblock().normalizedBaseSeqTxn;
        }
    }

    /**
     * Registers the views again and drives the restart's recovery to quiescence.
     *
     * @return the restatement guard of the job that ran the recovery, holding the last whole-view
     * rebuild's evidence
     */
    private LiveViewRebuildRestatementGuard restart() {
        engine.buildViewGraphs();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
            return job.rebuildRestatementGuardForTest();
        }
    }

    /**
     * Restarts the view and asserts the restart restored it from the timeline, replaying
     * {@code replayedRows} base rows above the root, with nothing rebuilt or blocked, and that the
     * view still holds {@code expectedRows}. The tied row comes back one of two ways: among the
     * replayed rows over a root sealed before it, or inside a root a repair re-versioned from a
     * snapshot that holds it, which leaves the restore no tie to replay. The callers that pass
     * zero replay nothing at all. A replay floored above the root would also have dropped a
     * replayed tied row as one below the view's START FROM.
     */
    private void restartAndAssertRestoredOverATie(String expectedRows, int replayedRows) throws Exception {
        shutdown();
        restart();
        assertRestoredFromTimeline("lv");
        final LiveViewInstance instance = instance("lv");
        Assert.assertFalse("the view must keep refreshing", instance.isCheckpointRecoveryBlocked());
        Assert.assertFalse(instance.isInvalid());
        assertNoRefreshFaults("lv");
        assertViewRows(expectedRows);
        capture.drain();
        capture.assertLoggedRE(
                "restored live view from checkpoint timeline \\[view=lv, .*replayedRows=" + replayedRows + "]"
        );
        capture.assertNotLogged("live view is dropping in-order rows below its START FROM boundary");
    }

    /**
     * Advances the clock and runs exactly one refresh turn, so a caller can read the state that
     * turn left rather than the state the turns after it converged on. A view backing off after a
     * faulting turn gets the pass at its retry deadline ({@link #advanceClockToNextRefreshPass}).
     */
    private void runOnePass(LiveViewRefreshJob job) {
        advanceClockToNextRefreshPass();
        drainWalQueue();
        job.run();
        drainWalQueue();
    }

    private void shutdown() {
        engine.getLiveViewRegistry().clear();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseInactive();
    }

    /**
     * Stamps a live repair marker over the generation on disk. The seqTxn it records sits one
     * below the view's newest commit, as a repair whose replacement committed leaves it.
     */
    private void writeRepairMarker(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance)) {
            LiveViewCheckpointRepairMarker.write(
                    engine.getConfiguration(),
                    dir,
                    instance.getLiveViewToken().getTableId(),
                    0,
                    newestGeneration(instance),
                    ts("2026-01-02T00:00:00.000000Z"),
                    engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()) - 1
            );
        }
    }
}
