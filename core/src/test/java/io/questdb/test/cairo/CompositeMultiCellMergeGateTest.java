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
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite partitioning WRITE-side safety: a single O3 commit whose out-of-order rows genuinely
 * INTERLEAVE across 2+ ALREADY-populated cells of one day must write CORRECT non-timestamp column data,
 * even when 2+ of those new rows land in the SAME already-populated cell (a genuine O3_BLOCK_MERGE) --
 * byte-identical to a plain (non-composite) twin table fed the exact same commits.
 * <p>
 * <b>History (why this class exists):</b> {@code TableWriter#processO3BlockComposite}'s multi-cellKey
 * regrouping path used to silently corrupt non-timestamp column data for this shape. Root cause
 * (follow-up task #25, confirmed to the native merge-shuffle level): {@code
 * buildCompositeCellGroupScratch} gathers each group's non-timestamp columns into a LOCAL, 0-based
 * scratch buffer ({@code scratchColumn[j]}, {@code j} in {@code [0, groupLen)}) but used to write the
 * row's ORIGINAL, batch-ABSOLUTE position into the scratch timestamp-index's {@code rowid} ({@code .i})
 * field instead of that same local {@code j}. On a genuine merge, {@code O3PartitionJob}'s native merge
 * path ({@code createMergeIndex} / {@code binary_merge_ts_long_index}) copies each out-of-order
 * {@code index_t} entry -- {@code .ts} AND {@code .i} -- verbatim into the merge index, and
 * {@code merge_shuffle_vanilla} then does {@code dest[k] = scratchColumn[index[k].i]}: with the absolute
 * row in {@code .i}, that reads the WRONG (later, or out-of-bounds) slot of the tiny
 * {@code groupLen}-sized scratch buffer instead of the row's own local slot {@code j}. Timestamps
 * themselves always stayed correct (the merged {@code ts} comes from the index's inline {@code ts}
 * field, never {@code .i}). Concrete negative-control repro (bug present, gate/fix both absent): bulk 4
 * rows across cells X/Y, then ONE commit adding out-of-order rows {@code (00:30 X A 900), (02:30 X C
 * 901), (01:30 Y A 902), (03:30 Y C 903)} produced, for the Y cell, {@code 01:30 -> Y C 903} (should have
 * been {@code Y A 902}) and {@code 03:30 -> Y C 903} -- i.e. the 01:30 row's own {@code (sym=A, px=902)}
 * was overwritten with the 03:30 row's {@code (sym=C, px=903)}.
 * <p>
 * This shape was initially LOUD-GATED ({@code CairoException}, table suspended) rather than fixed, as an
 * "explicit, loud (not silent) scope boundary" (mirroring the REPLACE mode / FORMAT PARQUET / var-size
 * column precedents {@code processO3BlockComposite} already sets) -- the root cause was believed to be
 * deeper in the async merge internals than it actually was. Follow-up task #25 traced it to the single
 * {@code buildCompositeCellGroupScratch} line above and fixed it (store the LOCAL position {@code j} in
 * the {@code .i} field, unconditionally for every group regardless of size) -- this {@code groupLen > 1}
 * shape is now proven correct and the gate is REMOVED. (Task #25 also investigated whether the narrower
 * {@code groupLen == 1} case -- a singleton group whose one new row's ts is at or below its cell's
 * existing max -- was an ungated live corruption too, since the old gate only ever fired for {@code
 * groupLen > 1}; it is NOT, for a reason specific to that group size -- see {@code
 * CompositeMultiCellMergeTest}'s class javadoc for the full account. The {@code .i = j} fix still applies
 * to {@code groupLen == 1} groups unconditionally, pre-emptively closing that path too.)
 * <p>
 * <b>Proven-safe neighbours</b> (were never gated, remain correct, regression-pinned below): a
 * single-cellKey commit extending one existing cell with 2+ out-of-order rows (never uses the regrouping
 * path); a multi-cell commit where every group has exactly 1 new row; a multi-cell commit where the
 * 2+-row group targets a genuinely BRAND-NEW cell (no existing data); and a multi-cell commit where each
 * 2+-row group is a pure IN-ORDER APPEND into its existing cell (every new row strictly AFTER that cell's
 * existing max), the ROUTINE continuous batch-ingest shape, which takes {@code O3PartitionJob}'s
 * suffix-append branch rather than the merge path and so was never exposed to the {@code .i} bug either.
 */
public class CompositeMultiCellMergeGateTest extends AbstractCairoTest {

    /**
     * The FORMERLY-gated shape (follow-up task #25): a cellKey group with 2+ rows genuinely merges into
     * an already-populated cell inside an interleaved multi-cell commit. Must now SUCCEED (no suspension)
     * and match a plain (non-composite) twin table fed the identical two commits, exactly -- not merely
     * "does not throw".
     */
    @Test
    public void testMultiCellMultiRowMergeIntoExistingCellsMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table gp (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day wal");

            final String bulk = " values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T01:00:00.000000Z','Y','C',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-01T03:00:00.000000Z','Y','B',4.0)";
            execute("insert into g" + bulk);
            execute("insert into gp" + bulk);
            drainWalQueue();
            Assert.assertFalse("g must not be suspended after the bulk commit",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));

            // ONE commit, genuinely interleaved across BOTH already-populated cells (X and Y), 2 new
            // out-of-order rows landing in EACH cell -- the shape task #25's fix makes correct (formerly
            // loud-gated; see class javadoc for the exact corruption this used to produce without a fix
            // or a gate).
            final String merge = " values " +
                    "('2020-04-01T00:30:00.000000Z','X','A',900.0), ('2020-04-01T02:30:00.000000Z','X','C',901.0), " +
                    "('2020-04-01T01:30:00.000000Z','Y','A',902.0), ('2020-04-01T03:30:00.000000Z','Y','C',903.0)";
            execute("insert into g" + merge);
            execute("insert into gp" + merge);
            drainWalQueue();

            Assert.assertFalse("g must not be suspended -- task #25's fix makes this genuine merge correct, not gated",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            assertSqlCursors("select * from gp order by ts", "select * from g order by ts");
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

    /**
     * Proven-safe neighbour 3 (NOT gated): a multi-cell commit where EACH group carries 2+ new rows that
     * are a pure IN-ORDER APPEND into its already-populated cell (every new row's ts is strictly AFTER
     * that cell's existing max -- an O3_BLOCK_APPEND, not a genuine O3_BLOCK_MERGE). This is the ROUTINE
     * continuous batch-ingest shape (a single INSERT spanning several cells, each already populated, 2+
     * rows/cell) and MUST NOT be gated -- it lands every row correctly, byte-identical to a plain twin.
     * Guards against the gate firing purely on "2+ rows into an existing cell" without an append-vs-merge
     * test (which would cripple normal composite ingestion).
     */
    @Test
    public void testMultiCellInOrderAppendIntoExistingCellsIsNotGated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day wal");

            // Commit 1: populate BOTH cells X and Y on day1 (2 in-order rows each). X max = 00:30,
            // Y max = 01:30. Brand-new cells here, so this commit is itself un-gated.
            final String bulk = " values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T00:30:00.000000Z','X','A',2.0), " +
                    "('2020-04-01T01:00:00.000000Z','Y','C',3.0), ('2020-04-01T01:30:00.000000Z','Y','D',4.0)";
            execute("insert into c" + bulk);
            execute("insert into p" + bulk);
            drainWalQueue();

            // Commit 2: ONE multi-cell commit, 2 rows into EACH already-populated cell, every new row
            // strictly AFTER its cell's existing max (02:00/03:00 > X's 00:30; 05:00/06:00 > Y's 01:30) --
            // a pure per-cell in-order append. Distinct sym/px per row so any value swap diverges.
            final String append = " values " +
                    "('2020-04-01T02:00:00.000000Z','X','P',200.0), ('2020-04-01T03:00:00.000000Z','X','Q',300.0), " +
                    "('2020-04-01T05:00:00.000000Z','Y','R',500.0), ('2020-04-01T06:00:00.000000Z','Y','S',600.0)";
            execute("insert into c" + append);
            execute("insert into p" + append);
            drainWalQueue();

            Assert.assertFalse("c must NOT be suspended -- a per-cell in-order append is not a merge (safe)",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            assertSqlCursors("select * from p order by ts", "select * from c order by ts");
        });
    }
}
