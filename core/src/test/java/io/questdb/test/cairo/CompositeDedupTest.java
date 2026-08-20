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
    @Ignore("THE ONE REMAINING DEDUP FAILURE. Keys = TIMESTAMP ONLY. TEN suspects eliminated by"
            + " measurement -- do not re-check: identical-check bounds; phantom-dir removal;"
            + " openROFromMemoryColumns; partition nameTxn; the open-column job's path builds; the"
            + " identical-check's column source; 'the O3 path is not reached' (it IS: composite=true,"
            + " multiCell=false); passing the true `last` (HANGS WAL apply -- O3PartitionJob reads"
            + " tableWriter.columns when last=true and composite does not maintain them); and now"
            + " COLUMN DISPATCH -- instrumented O3OpenColumnJob and ALL THREE columns receive tasks in"
            + " every phase (ts/exch/px, mode=3 then mode=4, mergeType=3, mergeRowCount=1). So no column"
            + " is skipped: the copy RUNS for exch and px and writes the wrong bytes."
            + " NEXT: the copy's SOURCE addresses for non-key columns under dedup. ts is copied from"
            + " sortedTimestampsAddr and is correct; exch/px come from the o3 column addresses via the"
            + " dedup merge index -- instrument those addresses in O3CopyJob for mergeType=3."
            + " SYMPTOM: cell E0.1 files sized 8/4/8 with uninitialised content."
            + " AND: the defect IS dedup-specific. CompositeSquashTest#testSameTimestampSecondCommitWithoutDedup"
            + " drives the IDENTICAL physical shape -- second row at the same timestamp, last partition,"
            + " one cell -- with NO dedup, and PASSES. So the composite same-timestamp merge itself is"
            + " sound; only the dedup variant corrupts. With ts as the only key the additional-keys path"
            + " is unused (columnCount=0), so the suspect narrows to the merge index that"
            + " mergeDedupTimestampWithLongIndexIntKeys produces and how the copy applies it to the"
            + " non-key columns.")
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
