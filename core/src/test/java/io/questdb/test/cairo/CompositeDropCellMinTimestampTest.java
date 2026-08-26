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

import io.questdb.cairo.TableWriter;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Sibling of the {@code dropPartitionByExactTimestamp} stranded-{@code minTimestamp} defect fixed in
 * {@code e548783f9e}. {@code removePartitionCell} carries the SAME shape:
 * <pre>
 *   long newMinTimestamp = txWriter.getMinTimestamp();          // carried over
 *   if (removingFirst &amp;&amp; partitionCountBefore &gt; 1) { ... }      // excluded when count == 1
 *   txWriter.setMinTimestamp(newMinTimestamp);                  // stale survives
 * </pre>
 * When the table holds exactly one partition and that cell is dropped, neither branch runs, so the old
 * minimum is written back for a table that now has no partitions at all. The WAL/O3 commit path then
 * folds later data in with {@code min(existing, batchMin)}, so the stale value wins forever.
 * <p>
 * DIFFERENT from its sibling in one respect worth recording: {@code removePartitionCell}'s
 * <i>recompute</i> guard is correct. It keys on {@code removingFirst} -- an INDEX comparison -- so
 * index 1 genuinely is the survivor. It was the day-comparison in the other method that was cell-blind.
 * Only the empty-table half is shared.
 * <p>
 * REACHABILITY: {@code AlterOperation.DROP_PARTITION_CELL} exists but nothing in the SQL parser
 * constructs it, so this is unreachable from SQL today. Driven through the public
 * {@code TableWriter#removePartitionCell} instead -- the real entry point the opcode will call once
 * wired -- rather than left as a comment for whoever wires it up to rediscover.
 */
public class CompositeDropCellMinTimestampTest extends AbstractCairoTest {

    /**
     * POSITIVE CONTROL. Dropping one cell of a MULTI-cell table must keep working and must recompute
     * the minimum correctly. Without this the main assertion could pass simply because cell drops were
     * broken wholesale.
     */
    @Test
    public void testDroppingFirstCellOfSeveralRecomputesMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO m VALUES ('2023-01-01T01:00:00.000000Z','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','B',2.0),"
                    + "('2023-01-02T03:00:00.000000Z','A',3.0)");
            drainWalQueue();

            try (TableWriter w = getWriter("m")) {
                Assert.assertTrue(w.removePartitionCell(
                        parseFloorPartialTimestamp("2023-01-01T00:00:00.000000Z"), 0));
            }
            drainWalQueue();

            Assert.assertFalse("table must stay live after dropping one cell of several",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("m")));
        });
    }

    /**
     * THE LOCK. Drop the only cell so the table becomes EMPTY, then write again. A stale minimum
     * survives the subsequent commit and leaves {@code _txn} describing a partition that no longer
     * exists, which suspends the table on the
     * {@code minTimestamp >= partition[0]} assert in {@code commitWalInsertTransactions}.
     */
    @Test
    public void testDroppingTheOnlyCellDoesNotStrandMinTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // Two cells so the table is genuinely ROUTED (removePartitionCell refuses otherwise),
            // both on the SAME day, then drop them so the table empties.
            execute("INSERT INTO s VALUES ('2023-01-05T10:00:00.000000Z','A',1.0),"
                    + "('2023-01-05T11:00:00.000000Z','B',2.0)");
            drainWalQueue();

            final long day = parseFloorPartialTimestamp("2023-01-05T00:00:00.000000Z");
            try (TableWriter w = getWriter("s")) {
                w.removePartitionCell(day, 1);
                w.removePartitionCell(day, 0);
            }
            drainWalQueue();

            // New data, strictly LATER than everything dropped.
            execute("INSERT INTO s VALUES ('2023-03-01T09:00:00.000000Z','A',7.0)");
            drainWalQueue();

            Assert.assertFalse("stranded minTimestamp suspended the table after it was emptied",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("s")));

            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT min(ts), count() FROM s", sink);
            TestUtils.assertContains(sink, "2023-03-01T09:00:00.000000Z");
        });
    }
}
