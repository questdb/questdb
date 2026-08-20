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
    @Ignore("ROOT CAUSE FOUND 2026-08-20 -- the dedup MERGE INDEX references a row that does not exist."
            + " Instrumented the index right after mergeDedupTimestampWithLongIndexIntKeys returns:"
            + "   DBGIDX [dedupRows=1, mergeRowCount=2, mergeDataLo=0, mergeDataHi=0, mergeOOOLo=0,"
            + "           mergeOOOHi=0, e0ts=1672534800000000, e0row=1]"
            + " e0ts is CORRECT (2023-01-01T01:00). e0row=1 points at DATA row 1, but the partition"
            + " holds ONE row, index 0 (mergeDataLo=mergeDataHi=0). The copy therefore reads row 1 --"
            + " off the end -- which is why the non-key columns come back uninitialised while ts is"
            + " right: ts is taken from the index's own ts field, not from the referenced row."
            + " The expected entry is the O3 row (the newer value), which in this codebase is encoded"
            + " with the high bit set, not as a bare 1. So the index is being built with the wrong"
            + " row-reference convention for this shape -- fix that, not the copy."
            + " TEN other suspects already eliminated by measurement: identical-check bounds;"
            + " phantom-dir removal; openROFromMemoryColumns; partition nameTxn; open-column path"
            + " builds; identical-check column source; 'O3 path not reached'; the `last` flag (HANGS if"
            + " changed); column dispatch (all columns DO get tasks); and a general composite"
            + " same-timestamp merge bug (the no-dedup control passes).")
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
