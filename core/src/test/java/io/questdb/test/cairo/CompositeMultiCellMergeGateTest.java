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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite partitioning WRITE-side safety: a single O3 commit whose out-of-order rows genuinely
 * INTERLEAVE across 2+ ALREADY-populated cells of one day, where 2+ of those new rows land in the SAME
 * cell, must be LOUD-GATED ({@code CairoException}, table suspended) -- never silently corrupt that
 * cell's non-timestamp column data.
 * <p>
 * <b>The bug this gate guards against is REAL and was isolated empirically</b> (found while developing
 * {@code io.questdb.test.griffin.CompositeReadEndToEndTest}, the read-side capstone). In
 * {@code TableWriter#processO3BlockComposite}'s multi-cellKey regrouping path, WITHOUT this gate, the
 * 2nd (and later) row of such a group silently LOST its own non-timestamp column values, instead gaining
 * a duplicate of a LATER row's values -- the row's own designated timestamp stayed correct, so no rows
 * vanished and the row count stayed right, but the payload was wrong. Concrete negative-control repro
 * (gate absent): bulk 4 rows across cells X/Y, then ONE commit adding out-of-order rows
 * {@code (00:30 X A 900), (02:30 X C 901), (01:30 Y A 902), (03:30 Y C 903)} produced, for the Y cell,
 * {@code 01:30 -> Y C 903} (should have been {@code Y A 902}) and {@code 03:30 -> Y C 903} -- i.e. the
 * 01:30 row's own {@code (sym=A, px=902)} was overwritten with the 03:30 row's {@code (sym=C, px=903)}.
 * <p>
 * The root cause is deeper in the {@link io.questdb.cairo.O3PartitionJob} async merge-into-existing-data
 * internals than the composite dispatch layer itself (the per-cell scratch buffers
 * {@code buildCompositeCellGroupScratch} hands off were verified correct by manual trace) -- too deep and
 * too performance-critical to fix safely within the read-side task that found it. It is therefore
 * LOUD-GATED rather than fixed, mirroring the "explicit, loud (not silent) scope boundaries" precedent
 * {@code processO3BlockComposite} already sets for REPLACE mode / FORMAT PARQUET / var-size columns.
 * <p>
 * <b>The trigger is narrow</b> -- empirically isolated to exactly "a cellKey GROUP with 2+ rows AND that
 * cell already has committed data (a genuine O3_BLOCK_MERGE)". Three neighbouring shapes are PROVEN SAFE
 * and are NOT gated (each confirmed correct via its own minimal repro): a single-cellKey commit extending
 * one existing cell with 2+ out-of-order rows (never uses the regrouping path); a multi-cell commit where
 * every group has exactly 1 new row; and a multi-cell commit where the 2+-row group targets a genuinely
 * BRAND-NEW cell (no existing data). The documented workaround for the gated case is to issue each
 * already-populated cell's out-of-order rows in its OWN separate commit -- exactly what
 * {@code CompositeReadEndToEndTest}'s lifecycle builders do.
 */
public class CompositeMultiCellMergeGateTest extends AbstractCairoTest {

    @Test
    public void testMultiCellMultiRowMergeIntoExistingCellsIsLoudGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("insert into g values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T01:00:00.000000Z','Y','C',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-01T03:00:00.000000Z','Y','B',4.0)");
            drainWalQueue();
            Assert.assertFalse("g must not be suspended after the bulk commit",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));

            // ONE commit, genuinely interleaved across BOTH already-populated cells (X and Y), 2 new
            // out-of-order rows landing in EACH cell -- the exact gated shape. WITHOUT the gate this
            // silently corrupts the Y cell's 2nd row (see class javadoc's negative-control detail).
            execute("insert into g values " +
                    "('2020-04-01T00:30:00.000000Z','X','A',900.0), ('2020-04-01T02:30:00.000000Z','X','C',901.0), " +
                    "('2020-04-01T01:30:00.000000Z','Y','A',902.0), ('2020-04-01T03:30:00.000000Z','Y','C',903.0)");
            drainWalQueue();

            Assert.assertTrue("g must be suspended by the multi-cell multi-row merge gate, not silently corrupted",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            printSql("select errorMessage from wal_tables() where name = 'g'");
            TestUtils.assertContains(sink,
                    "composite partitioning does not yet support 2 or more out-of-order rows landing in the same already-populated cell within one interleaved multi-cell commit");
        });
    }

    /**
     * Proven-safe neighbour 1 (NOT gated): a multi-cell commit where the 2+-row group targets a genuinely
     * BRAND-NEW cell (Z, never committed before) while the other group extends an existing cell (X) with
     * just ONE row -- must NOT be gated, and must land all rows correctly. Guards against the gate being
     * widened to fire for a brand-new cell (which is safe).
     */
    @Test
    public void testMultiCellMultiRowIntoBrandNewCellIsNotGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("insert into g values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T02:00:00.000000Z','X','A',3.0)");
            drainWalQueue();

            // X gets 1 new row (extend existing), Z is brand-new with 2 rows -- one commit, multi-cell.
            execute("insert into g values " +
                    "('2020-04-01T01:00:00.000000Z','X','Z',900.0), " +
                    "('2020-04-01T00:30:00.000000Z','Z','P',902.0), ('2020-04-01T01:30:00.000000Z','Z','Q',903.0)");
            drainWalQueue();

            Assert.assertFalse("g must NOT be suspended -- the 2+-row group targets a brand-new cell (safe)",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            assertQuery("select ts, exch, sym, px from g order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-04-01T00:00:00.000000Z\tX\tB\t1.0\n" +
                            "2020-04-01T00:30:00.000000Z\tZ\tP\t902.0\n" +
                            "2020-04-01T01:00:00.000000Z\tX\tZ\t900.0\n" +
                            "2020-04-01T01:30:00.000000Z\tZ\tQ\t903.0\n" +
                            "2020-04-01T02:00:00.000000Z\tX\tA\t3.0\n");
        });
    }

    /**
     * Proven-safe neighbour 2 (NOT gated): a multi-cell commit where EVERY group has exactly ONE new row
     * (into already-populated cells) -- must NOT be gated, and must land all rows correctly. Guards
     * against the gate being widened to fire whenever a commit merely touches 2+ existing cells.
     */
    @Test
    public void testMultiCellSingleRowPerGroupIntoExistingCellsIsNotGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("insert into g values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T01:00:00.000000Z','Y','C',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-01T03:00:00.000000Z','Y','B',4.0)");
            drainWalQueue();

            // One new out-of-order row into EACH of X and Y (1 per group) -- one commit, multi-cell.
            execute("insert into g values " +
                    "('2020-04-01T00:30:00.000000Z','X','A',900.0), ('2020-04-01T01:30:00.000000Z','Y','A',902.0)");
            drainWalQueue();

            Assert.assertFalse("g must NOT be suspended -- every group has exactly one new row (safe)",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            assertQuery("select ts, exch, sym, px from g order by ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\texch\tsym\tpx\n" +
                            "2020-04-01T00:00:00.000000Z\tX\tB\t1.0\n" +
                            "2020-04-01T00:30:00.000000Z\tX\tA\t900.0\n" +
                            "2020-04-01T01:00:00.000000Z\tY\tC\t2.0\n" +
                            "2020-04-01T01:30:00.000000Z\tY\tA\t902.0\n" +
                            "2020-04-01T02:00:00.000000Z\tX\tA\t3.0\n" +
                            "2020-04-01T03:00:00.000000Z\tY\tB\t4.0\n");
        });
    }
}
