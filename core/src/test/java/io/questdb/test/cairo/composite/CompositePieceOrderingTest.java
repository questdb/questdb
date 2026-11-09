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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A composite partition's pieces must ascend strictly by {@code tsLo} - {@link
 * io.questdb.cairo.PartitionGeometry#addPiece} rejects anything else, and a rejected piece list fails the
 * commit inside the O3 worker, which suspends a WAL table for good.
 * <p>
 * The shape that gets there: a single-point piece ({@code tsLo == tsHi}) that is last by {@code tsLo} but
 * no longer owns the shared files' tail. {@code O3CompositeMergeStrategy}'s dedup-free tie rule exempts
 * the LAST piece from the {@code tsLo < tsHi} guard, on the grounds that a tail-extending APPEND can
 * never found a second piece beside it. That holds only while the piece still owns the tail; once an
 * earlier piece has been merged out to it, APPEND declines and the spared tie falls through to the
 * trailing NEW_PIECE - founded at the very timestamp the kept piece already starts at.
 */
public class CompositePieceOrderingTest extends AbstractCairoTest {

    /**
     * The unit-level shape lives in {@code O3CompositeMergeStrategyTest}; this is the same defect driven
     * end to end through ordinary INSERTs, so it also covers the executor and the geometry publish.
     */
    @Test
    public void testTieOnASinglePointPieceThatLostTheTailDoesNotSuspendTheTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        // A production-sized partition pre-splits on its own at the 50MB default; shrink the threshold so
        // a fixture small enough to read does the same.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);

        assertMemoryLeak(() -> {
            // 2024-01-01T00:00 .. 03:59, one row a minute. No DEDUP UPSERT KEYS, so a commit's ties need
            // no key comparison and the dedup-free tie rule is the one that applies.
            execute("""
                    CREATE TABLE x AS (
                      SELECT x::INT v, timestamp_sequence('2024-01-01', 60_000_000L) ts
                      FROM long_sequence(240)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
            // A later day, so 2024-01-01 is never the active partition and every write below is O3.
            execute("INSERT INTO x VALUES (90_000, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();

            // Above the day's last row: founds a piece of its own at the files' tail.
            execute("INSERT INTO x VALUES (700_001, '2024-01-01T04:30:00.000000Z')");
            drainWalQueue();
            // Backdated: merges a piece out to the tail, so the piece above stops owning it.
            execute("INSERT INTO x SELECT x::INT + 800_000," +
                    " timestamp_sequence('2024-01-01T02:00:00', 60_000_000L) FROM long_sequence(10)");
            drainWalQueue();
            // The same timestamp again: leaves the day's highest piece a single point in time.
            execute("INSERT INTO x VALUES (700_002, '2024-01-01T04:30:00.000000Z')");
            drainWalQueue();
            // Backdated again: takes the tail away from that single-point piece.
            execute("INSERT INTO x SELECT x::INT + 810_000," +
                    " timestamp_sequence('2024-01-01T01:00:00', 60_000_000L) FROM long_sequence(10)");
            drainWalQueue();

            // The tie that must merge into the single-point piece rather than found a second one beside it.
            execute("INSERT INTO x VALUES (700_003, '2024-01-01T04:30:00.000000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    "the commit published a piece list that does not ascend by tsLo, so the table was" +
                            " suspended - and the transaction is deterministic, so RESUME WAL cannot clear it",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );
            assertQuery("SELECT count() FROM x WHERE ts = '2024-01-01T04:30:00.000000Z'")
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n3\n");
            assertQuery("SELECT count() FROM x").expectSize().noRandomAccess().returns("count\n264\n");
        });
    }
}
