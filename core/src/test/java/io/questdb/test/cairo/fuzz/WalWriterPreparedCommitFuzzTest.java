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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

/**
 * The WAL fuzz with a share of its commits and rollbacks going through {@code WalWriter.prepareCommit()}: a commit
 * sequences the prepared record, sometimes after a refused segment roll, and a rollback abandons it and replaces
 * the writer. The prepared commits interleave with column DDL, from the same writer and, in the parallel apply,
 * from other writers, and with segment rolls. The WAL tables must end up equal to the non-WAL one.
 */
public class WalWriterPreparedCommitFuzzTest extends AbstractFuzzTest {

    @Test
    public void testPreparedCommitFuzz() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        runPreparedCommitFuzz(rnd, Integer.getInteger("questdb.fuzz.prepared.commit.transactions", 300));
    }

    /**
     * From fixed seeds, the fuzz sequences prepared commits and abandons prepared ones, the shapes it exists for.
     */
    @Test
    public void testPreparedCommitFuzzGeneratesPreparedShapes() throws Exception {
        final Rnd rnd = generateRandom(LOG, 1_234_567L, 7_654_321L);
        runPreparedCommitFuzz(rnd, 100);
        Assert.assertTrue("a prepared commit sequenced", fuzzer.getSequencedPreparedCommitCount() > 0);
        Assert.assertTrue("a prepared commit abandoned", fuzzer.getAbandonedPreparedCommitCount() > 0);
    }

    private void runPreparedCommitFuzz(Rnd rnd, int transactionCount) throws Exception {
        // Small segments, so that prepared commits land at segment rolls.
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10 + rnd.nextInt(200));
        setFuzzProbabilities(
                0.01,
                0.0,
                0.01,
                0.2,
                0.05,
                0.05,
                0.05,
                0.05,
                1.0,
                0.01,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0
        );
        setFuzzCounts(rnd.nextBoolean(), 5_000, transactionCount, 20, 10, 200, 100, 3);
        fuzzer.setPreparedCommitProbability(0.5);
        runFuzz(rnd);
    }
}
