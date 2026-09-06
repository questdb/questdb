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
 * Composite partitioning WRITE-side safety (follow-up task #25): coverage for {@code groupLen == 1}
 * cellKey groups -- the shape the pre-fix {@code groupLen > 1} gate in {@link
 * CompositeMultiCellMergeGateTest} never covered -- plus a combined-shapes stress test.
 * <p>
 * <b>Investigated, and IMPORTANT correction to this task's own starting assumption:</b> the task brief
 * that opened follow-up #25 assumed a {@code groupLen == 1} group whose one new row's ts is at or below
 * its cell's existing max (the {@code groupLen > 1} gate's own "might be a genuine merge" heuristic) was
 * ALSO a live, ungated silent-corruption case, by the identical {@code .i} mechanism {@code
 * CompositeMultiCellMergeGateTest} documents for {@code groupLen > 1}. Empirical testing (TDD RED phase)
 * plus a full case analysis of every branch in {@code O3PartitionJob}'s merge-vs-append classification
 * disproved this for the composite implementation as it stands today: a single-row ({@code lo == hi})
 * out-of-order dispatch can NEVER be classified {@code O3_BLOCK_MERGE} (the only block type whose copy
 * ({@code O3CopyJob#copy}'s {@code mergeCopy}) reads the scratch index's {@code .i} field at all) unless
 * {@code mergeEquals} is {@code 1}, which requires dedup or REPLACE commit mode -- and composite
 * partitioning explicitly rejects BOTH (dedup: DDL-time {@code CairoException} "composite partitioning
 * does not yet support DEDUP UPSERT KEYS"; REPLACE: {@code processO3BlockComposite}'s own top-of-method
 * throw). Without a tie plus {@code mergeEquals=1}, every single-row placement -- squeezed between two
 * existing rows, prepended before the cell's min, or appended after its max -- resolves to {@code
 * O3_BLOCK_O3} or an append/prepend block, both of which copy the OOO/scratch source by plain contiguous
 * position ({@code O3CopyJob#copyO3}), never dereferencing {@code .i}. This was confirmed both
 * structurally (case analysis of every branch in the merge-classification method) and empirically (a
 * debug build logging the actual {@code mergeType} chosen for the shape below returned {@code O3_BLOCK_O3},
 * not {@code O3_BLOCK_MERGE}).
 * <p>
 * So: the {@code groupLen == 1} scratch is written with the SAME wrong {@code .i} value ({@code
 * absoluteRow} instead of local {@code j}) the {@code groupLen > 1} case had, but NOTHING currently reads
 * it for a singleton group -- it is a latent defect, not a reachable one, today. It WOULD become live the
 * moment dedup or REPLACE support is added to composite partitioning (a single row tying an existing
 * timestamp under either mode reaches {@code O3_BLOCK_MERGE} even for {@code groupLen == 1} -- verified
 * for the dedup case up to the point where table creation itself is rejected). Task #25's {@code .i = j}
 * fix is applied unconditionally in {@code buildCompositeCellGroupScratch} (not conditioned on {@code
 * groupLen}), so this latent path is ALREADY closed pre-emptively; nothing further is needed unless/until
 * dedup or REPLACE gains composite support, at which point this class's tests should be revisited.
 * <p>
 * The test below is kept as a genuine regression pin for the shape (single OOO row merging by timestamp
 * into an already-populated cell, interleaved with other cells in one commit) even though it does not
 * exercise the buggy path -- it still exercises {@code buildCompositeCellGroupScratch} for a singleton
 * group and confirms the fix does not regress it.
 */
public class CompositeMultiCellMergeTest extends AbstractCairoTest {

    /**
     * A {@code groupLen == 1} cellKey group (cell Y) whose one new row's ts is at or below Y's existing
     * max, interleaved in one commit with a second cell's (X) pure in-order append and a third, brand-new
     * cell (Z). See this class's own javadoc: {@code O3PartitionJob} classifies Y's dispatch here as
     * {@code O3_BLOCK_O3} (squeezed between two existing rows, no tie), not {@code O3_BLOCK_MERGE} -- so
     * this shape was, and remains, safe even before task #25's fix; kept as a regression pin. Must match
     * a plain (non-composite) twin table fed the identical two commits, exactly.
     */
    @Test
    public void testMultiCellSingleRowMergeIntoExistingCellMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table gp (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day wal");

            // Seed X (max ts 02:00) and Y (max ts 03:00) as two already-populated, brand-new-at-the-time
            // cells -- itself un-gated (no pre-existing data yet).
            final String bulk = " values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T01:00:00.000000Z','Y','C',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-01T03:00:00.000000Z','Y','D',4.0)";
            execute("insert into g" + bulk);
            execute("insert into gp" + bulk);
            drainWalQueue();
            Assert.assertFalse("g must not be suspended after the bulk commit",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));

            // ONE commit, 3 cells, 1 row each (every group is groupLen==1):
            //  - Z (00:05): brand-new cell, globally ts-earliest row in this commit -- exercises another
            //    singleton-group scratch build in the same dispatch loop as Y and X.
            //  - Y (01:30): ts is at or below Y's existing max (01:30 <= 03:00) -- resolves to
            //    O3_BLOCK_O3 (squeezed between Y's existing 01:00 and 03:00 rows, no tie), not
            //    O3_BLOCK_MERGE, so this was already safe (see class javadoc); regression-pinned here.
            //  - X (04:00): pure in-order APPEND into X's existing data (04:00 > X's max 02:00) -- takes
            //    O3PartitionJob's suffix-append branch, never touches the scratch index's .i field.
            final String merge = " values " +
                    "('2020-04-01T00:05:00.000000Z','Z','N',999.0), " +
                    "('2020-04-01T01:30:00.000000Z','Y','M',902.5), " +
                    "('2020-04-01T04:00:00.000000Z','X','P',400.0)";
            execute("insert into g" + merge);
            execute("insert into gp" + merge);
            drainWalQueue();

            Assert.assertFalse("g must not be suspended",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            assertSqlCursors("select * from gp order by ts", "select * from g order by ts");
        });
    }

    /**
     * Combined stress shape: ONE interleaved commit that simultaneously exercises a {@code groupLen > 1}
     * genuine {@code O3_BLOCK_MERGE} (cell X, formerly gated by {@code CompositeMultiCellMergeGateTest}'s
     * old gate) AND a {@code groupLen == 1} group whose ts is at or below its cell's existing max but
     * resolves to {@code O3_BLOCK_O3} (cell Y, see class javadoc), alongside a brand-new cell (Z) and a
     * plain in-order append (cell W) -- four distinct group shapes in one dispatch loop. Every group must
     * independently end up with its own correct {@code .i}, not just the single-shape cases above.
     */
    @Test
    public void testMixedGroupShapesInOneCommitMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table gp (ts timestamp, exch symbol, sym symbol, px double) timestamp(ts) partition by day wal");

            final String bulk = " values " +
                    "('2020-04-01T00:00:00.000000Z','X','B',1.0), ('2020-04-01T01:00:00.000000Z','Y','C',2.0), " +
                    "('2020-04-01T02:00:00.000000Z','X','A',3.0), ('2020-04-01T03:00:00.000000Z','Y','D',4.0), " +
                    "('2020-04-01T00:15:00.000000Z','W','E',5.0), ('2020-04-01T02:15:00.000000Z','W','F',6.0)";
            execute("insert into g" + bulk);
            execute("insert into gp" + bulk);
            drainWalQueue();
            Assert.assertFalse("g must not be suspended after the bulk commit",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));

            // ONE commit, 4 cells:
            //  - X (00:20, 02:30): groupLen==2, straddles past X's existing max (02:00) -- a genuine
            //    O3_BLOCK_MERGE, the formerly-gated shape (CompositeMultiCellMergeGateTest's old gate).
            //  - Y (01:15): groupLen==1, ts <= Y's max (03:00) but resolves to O3_BLOCK_O3 (squeezed
            //    between Y's existing rows, no tie) -- see class javadoc.
            //  - W (03:00): groupLen==1, pure in-order APPEND (> W's max 02:15) -- safe neighbour.
            //  - Z (00:01): brand-new cell, globally ts-earliest -- safe neighbour, also exercises another
            //    singleton-group scratch build in the same dispatch loop.
            final String merge = " values " +
                    "('2020-04-01T00:01:00.000000Z','Z','N',999.0), " +
                    "('2020-04-01T00:20:00.000000Z','X','G',700.0), ('2020-04-01T02:30:00.000000Z','X','H',800.0), " +
                    "('2020-04-01T01:15:00.000000Z','Y','I',900.0), " +
                    "('2020-04-01T03:00:00.000000Z','W','J',1000.0)";
            execute("insert into g" + merge);
            execute("insert into gp" + merge);
            drainWalQueue();

            Assert.assertFalse("g must not be suspended -- task #25's fix makes the genuine merge (cell X) correct",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("g")));
            assertSqlCursors("select * from gp order by ts", "select * from g order by ts");
        });
    }
}
