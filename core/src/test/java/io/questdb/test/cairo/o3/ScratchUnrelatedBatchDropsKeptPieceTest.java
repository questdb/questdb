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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code O3PartitionJob}'s private {@code processCompositePartition} downgrades a KEEP piece to DROP (or a
 * MERGE piece to NEW_PIECE, discarding its own rows) whenever the piece's bounds sit fully inside
 * {@code [o3TimestampLo, o3TimestampHi]} - the loop right after the {@code processCompositePartition} call
 * that builds the plan, guarded only by the piece's own bounds, never by
 * {@code tableWriter.isCommitReplaceMode()}. That downgrade is correct for a REPLACE commit, whose declared
 * range means "delete everything in here" - but outside replace mode {@code o3TimestampLo}/{@code Hi} are
 * just the incoming O3 batch's own min/max timestamp (see the {@code else} branch that sets them in
 * {@code TableWriter}, next to the {@code isCommitReplaceMode()} branch that computes the declared range
 * instead). So an ORDINARY out-of-order batch that never touches an existing piece, but whose own span
 * happens to fully contain it, silently deletes that piece's rows.
 * <p>
 * No REPLACE, no TRUNCATE, no fuzzing: one pre-existing composite partition, one plain multi-row INSERT
 * whose two rows straddle - without touching - a small piece landed by an earlier commit. That piece's row
 * vanishes.
 */
public class ScratchUnrelatedBatchDropsKeptPieceTest extends AbstractCairoTest {

    @Test
    public void testOrdinaryBatchSpanningAnUntouchedPieceDropsIt() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "16");

            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES" +
                    " ('2020-02-03T00:00:00.000000Z', 1)," +
                    " ('2020-02-03T01:00:00.000000Z', 2)," +
                    " ('2020-02-03T02:00:00.000000Z', 3)");
            // A later day, so 2020-02-03 is never the active partition and every further write to it goes
            // through the O3 path.
            execute("INSERT INTO x VALUES ('2020-02-06T00:00:00.000000Z', 999)");
            drainWalQueue();

            final TableToken xt = engine.verifyTableName("x");

            // Tail append, above everything the day holds: tiles cleanly, still one implicit piece.
            execute("INSERT INTO x VALUES ('2020-02-03T10:00:00.000000Z', 4)");
            drainWalQueue();

            // One row, alone in the cold gap between 02:00 and 10:00: batch-edge cuts isolate it into its
            // own small piece, tightly bounded to [05:00, 05:00].
            execute("INSERT INTO x VALUES ('2020-02-03T05:00:00.000000Z', 5)");
            drainWalQueue();
            Assert.assertFalse("isolating commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT v FROM x WHERE ts = '2020-02-03T05:00:00.000000Z'")
                    .returns("v\n5\n");

            // An ORDINARY (non-replace) out-of-order batch whose own two rows sit at 03:00 and 07:00 - its
            // span [03:00, 07:00] fully CONTAINS the 05:00 piece, but neither row lands ON it, so that
            // piece gets no O3 row of this commit's own and should stay KEEP.
            execute("INSERT INTO x VALUES" +
                    " ('2020-02-03T03:00:00.000000Z', 6)," +
                    " ('2020-02-03T07:00:00.000000Z', 7)");
            drainWalQueue();
            Assert.assertFalse("spanning commit suspended the table", engine.getTableSequencerAPI().isSuspended(xt));

            assertQuery("SELECT v FROM x WHERE ts = '2020-02-03T03:00:00.000000Z'")
                    .returns("v\n6\n");
            assertQuery("SELECT v FROM x WHERE ts = '2020-02-03T07:00:00.000000Z'")
                    .returns("v\n7\n");
            // The row this test is actually about: untouched by the spanning batch, but inside its range.
            assertQuery("SELECT v FROM x WHERE ts = '2020-02-03T05:00:00.000000Z'")
                    .returns("v\n5\n");

            assertQuery("SELECT count(*) c FROM x WHERE ts IN '2020-02-03'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n7\n");
        });
    }
}
