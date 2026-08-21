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

import io.questdb.cairo.lv.LiveViewRefreshJob;
import org.junit.Test;

/**
 * What {@code live_views().checkpoint_segment_repair_gate} and
 * {@code checkpoint_keyed_scan_gate} report for each shape of view.
 * <p>
 * Both columns describe the view's SQL rather than any repair: what a scoped, keyed repair
 * of its closed anchor segments would admit. The cases below are one per gate, because the
 * point of the columns is that an operator can tell which one stands in the way - "not
 * available" without a reason names nothing to act on.
 * <p>
 * Every case asserts NULL before the view compiles its SELECT as well. "Not known yet" is a
 * different statement about a view than "denied", and the two must not be reported alike:
 * a view that has never refreshed would otherwise read as one whose SQL was rejected.
 */
public class LiveViewSegmentRepairEnvelopeTest extends AbstractLiveViewTest {

    @Test
    public void testABoundedFrameBesideTheAnchorDeniesTheSegmentScope() throws Exception {
        // The frame of a bounded ROWS window keeps sliding across the anchor boundary, so a
        // row in a closed segment still changes a later segment's output. Repairing that
        // segment on its own would leave the runtime wrong, which is why the gate is the
        // same one that denies a keyed replay.
        assertMemoryLeak(() -> {
            createBase("symbol index capacity 4");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over w as s, "
                    + "sum(amt_txn) over (partition by cod_acct_no order by created_at "
                    + "rows between 3 preceding and current row) as b "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            assertGates("bounded frame", "available");
        });
    }

    @Test
    public void testACompoundKeyDemotesTheKeyedScan() throws Exception {
        // The posting index names one column's values. A second partition column leaves the
        // segment on a whole-segment replay, which costs the same write and only a larger
        // read - so the segment scope itself is untouched.
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, cod_acct_no symbol index capacity 4, "
                    + "branch symbol, amt_txn double) timestamp(created_at) partition by hour wal");
            execute("insert into tx values ('2026-01-01T09:00:00.000000Z', 'acct-1', 'br-1', 1.5)");
            drainWalQueue();
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, branch, sum(amt_txn) over w as s "
                    + "from tx window w as (partition by cod_acct_no, branch order by created_at "
                    + "anchor daily '00:00')");
            assertGates("available", "compound key");
        });
    }

    @Test
    public void testAnAnchoredViewCannotCarryAnExpressionKeyAtAll() throws Exception {
        // Why the expression-key gate is a backstop rather than a case an operator meets:
        // CREATE rejects a PARTITION BY that is not a direct base column reference on an
        // anchored view, so today every anchored view's key already traces to a base
        // column. The gate stays because the trace can also come back empty for a key a
        // later projection shape carries, and answering that with a wrong gate name would
        // be worse than answering it with the right one nothing reaches yet.
        assertMemoryLeak(() -> {
            createBase("symbol index capacity 4");
            assertException(
                    "create live view lv flush every 100ms start from beginning as "
                            + "select created_at, cod_acct_no, sum(amt_txn) over w as s "
                            + "from tx window w as (partition by concat(cod_acct_no, '-x') order by created_at "
                            + "anchor daily '00:00')",
                    153,
                    "live view ANCHOR currently requires PARTITION BY to reference base columns directly"
            );
        });
    }

    @Test
    public void testAnIndexedSymbolKeyAdmitsBothHalves() throws Exception {
        // The shape the reported workload has, and the only one both halves accept: one
        // anchored window, one direct indexed base SYMBOL key, carried into the output.
        assertMemoryLeak(() -> {
            createBase("symbol index capacity 4");
            createAnchoredView();
            assertGates("available", "available");
        });
    }

    @Test
    public void testAnUnanchoredViewHasNoSegmentToScopeTo() throws Exception {
        // Without an anchored window there is no closed segment, so neither half describes
        // anything. The dependency gate fires first: a bounded ROWS view carries a ROWS plan
        // and no anchor plan, so the anchored arm covers nothing.
        assertMemoryLeak(() -> {
            createBase("symbol index capacity 4");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, cod_acct_no, sum(amt_txn) over "
                    + "(partition by cod_acct_no order by created_at rows between 3 preceding and current row) as s "
                    + "from tx");
            assertGates("no anchor plan", "no anchor plan");
        });
    }

    @Test
    public void testAnUnindexedSymbolKeyDemotesTheKeyedScan() throws Exception {
        // The operational case: the same view, the same anchor, and the only difference is
        // that the base column carries no index. Nothing about the repair changes - the
        // segment simply reads whole - and the column is what says so before a late row
        // arrives.
        assertMemoryLeak(() -> {
            createBase("symbol");
            createAnchoredView();
            assertGates("available", "key not indexed");
        });
    }

    @Test
    public void testALongKeyDemotesTheKeyedScan() throws Exception {
        // Only SYMBOL carries the posting index that names one value's rows, so a numeric
        // key leaves the segment on a whole-segment replay however selective it is.
        assertMemoryLeak(() -> {
            createBase("long");
            createAnchoredView();
            assertGates("available", "key not symbol");
        });
    }

    @Test
    public void testAnUnprojectedKeyDemotesTheKeyedScan() throws Exception {
        // A keyed replay re-emits only its keys' rows, so the output has to name which key
        // each row belongs to. This view partitions on the account and never selects it.
        assertMemoryLeak(() -> {
            createBase("symbol index capacity 4");
            execute("create live view lv flush every 100ms start from beginning as "
                    + "select created_at, sum(amt_txn) over w as s "
                    + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
            assertGates("available", "key not projected");
        });
    }

    /**
     * Compiles the view by driving one refresh, then asserts both gates. The pre-compile
     * assertion runs first, on the same view, because NULL and a denial are different
     * answers and only the ordering proves the column reports both.
     */
    private void assertGates(String segmentScopeGate, String keyedScanGate) throws Exception {
        assertQuery("SELECT checkpoint_segment_repair_gate, checkpoint_keyed_scan_gate FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("checkpoint_segment_repair_gate\tcheckpoint_keyed_scan_gate\n" +
                        "\t\n");

        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        }

        assertQuery("SELECT checkpoint_segment_repair_gate, checkpoint_keyed_scan_gate FROM live_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("checkpoint_segment_repair_gate\tcheckpoint_keyed_scan_gate\n" +
                        segmentScopeGate + "\t" + keyedScanGate + "\n");
    }

    private void createAnchoredView() throws Exception {
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as s "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    /**
     * Creates the base table with the given partition-key column, and puts one row in it.
     * The row is what makes the refresh compile the view's SELECT at all - a view with
     * nothing to seed never reaches the compile, and every gate would read NULL.
     */
    private void createBase(String keyColumnDdl) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no " + keyColumnDdl + ", amt_txn double) "
                + "timestamp(created_at) partition by hour wal");
        execute("insert into tx values ('2026-01-01T09:00:00.000000Z', "
                + (keyColumnDdl.startsWith("symbol") ? "'acct-1'" : "1") + ", 1.5)");
        drainWalQueue();
    }
}
