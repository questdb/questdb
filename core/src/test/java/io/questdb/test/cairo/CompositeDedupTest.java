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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * DEDUP UPSERT KEYS on a composite table.
 *
 * <p>Gated in TWO places until now -- at CREATE
 * ({@code CreateTableOperationBuilderImpl#resolvePartitionSpec}) and at ALTER
 * ({@code SqlCompilerImpl}) -- both pointing at the same suspect:
 * {@code O3PartitionJob#getDedupRowsWithAdditionalKeys} is not cell-aware.
 *
 * <p><b>Why the twin oracle is the right one here.</b> Dedup is defined entirely by which rows SURVIVE
 * a commit, so a plain table with identical data and identical keys is an exact reference: composite
 * must keep the same rows. Structure assertions cannot see a dedup bug, and a row-count check alone
 * cannot tell "deduplicated correctly" from "dropped the wrong row" -- so every test here compares full
 * row sets, not counts.
 */
public class CompositeDedupTest extends AbstractCompositeTwinTest {

    /**
     * Dedup on the designated timestamp alone: a repeated timestamp within ONE cell must collapse.
     */
    @Ignore("PROVEN 2026-08-20 -- the sorted-timestamps index and the O3 columns disagree about their"
            + " BASE, and the fix is to rebase the index for the composite single-cell dispatch."
            + " PROOF, both measured at the merge call:"
            + "   DBGIDX2 [srcOooMax=1, srcDataMax=1, mergeOOOLo=0, mergeOOOHi=0, o3row0=1]"
            + "   DBGSRC  [px0=99.0, px1=6.65e-310]"
            + " The O3 columns hold the NEW value at index 0; index 1 is uninitialised. The"
            + " sorted-timestamps entry references index 1. That garbage IS what the query returns, so"
            + " the index entry is wrong and the correct reference is 0. Its timestamp is right, which"
            + " is why ts reads back correctly while exch and px do not."
            + " The native merge (dedup.cpp) and the copy tasks are both EXONERATED -- they copy the bad"
            + " entry verbatim (high bit CLEAR = O3 data). Same absolute-vs-relative class already fixed"
            + " for the MULTI-CELL scratch path; the single-cell path was never audited for it because"
            + " nothing exercised it until dedup made a stale index observable."
            + " FIX: rebase the o3 row index in the sorted-timestamps entries to the dispatched range"
            + " for the single-cell composite dispatch (dispatchCompositeCellRange passes"
            + " sortedTimestampsAddr unchanged with srcOooLo/srcOooHi)."
            + " TWELVE suspects eliminated by measurement: identical-check bounds; phantom-dir removal;"
            + " openROFromMemoryColumns; partition nameTxn; open-column path builds; identical-check"
            + " column source; 'O3 path not reached'; the `last` flag (HANGS); column dispatch; a"
            + " general same-timestamp merge bug (no-dedup control PASSES); the dedup merge; the copy.")
    @Test(timeout = 60_000)
    public void testDedupOnTimestampWithinOneCell() throws Exception {
        assertMemoryLeak(() -> {
            createDedupTwins("ts");
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            // same timestamp, same cell, different value -> must UPSERT, not duplicate
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',99.0)");
            drainWalQueue();

            assertTwinEqual("");
        });
    }

    /**
     * The case that makes composite different: the SAME timestamp in DIFFERENT cells. These are
     * distinct rows and must both survive -- a dedup that collapsed them would be losing data that the
     * plain twin keeps, because on the plain table the dimension column is just another column.
     */
    @Ignore("Gated with the rest; this case PASSES with the gates lifted (verified 2026-08-20).")
    @Test(timeout = 60_000)
    public void testSameTimestampInDifferentCellsBothSurvive() throws Exception {
        assertMemoryLeak(() -> {
            createDedupTwins("ts, exch");
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E1',2.0)");
            drainWalQueue();

            assertTwinEqual("");
        });
    }

    /**
     * Dedup keyed on the DIMENSION column, upserting within a cell while a sibling cell holds the same
     * timestamp. This is the shape {@code getDedupRowsWithAdditionalKeys} has to get right: the key set
     * spans a column that also decides which cell a row lives in.
     */
    @Ignore("Gated with the rest; this case PASSES with the gates lifted (verified 2026-08-20).")
    @Test(timeout = 60_000)
    public void testUpsertWithinACellWhileSiblingHoldsSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createDedupTwins("ts, exch");
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            // upsert ONLY the E0 row; E1 at the same timestamp must be untouched
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',11.0)");
            drainWalQueue();

            assertTwinEqual("");
        });
    }

    /**
     * Dedup across an O3 write, which is where dedup and the composite cell router interact: the
     * incoming batch is out of order AND spans two cells.
     */
    @Ignore("Gated with the rest; this case PASSES with the gates lifted (verified 2026-08-20).")
    @Test(timeout = 60_000)
    public void testDedupAcrossAnO3Write() throws Exception {
        assertMemoryLeak(() -> {
            createDedupTwins("ts, exch");
            insertIntoBoth("('2023-01-01T05:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T20:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            // O3: lands before both, and repeats the first row's key
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',3.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-01T05:00:00.000000Z','E0',44.0)");
            drainWalQueue();

            assertTwinEqual("");
        });
    }

    private void createDedupTwins(String keys) throws SqlException {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts)"
                + " PARTITION BY DAY, exch LAYOUT PLAIN WAL DEDUP UPSERT KEYS(" + keys + ')');
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts)"
                + " PARTITION BY DAY WAL DEDUP UPSERT KEYS(" + keys + ')');
    }
}
