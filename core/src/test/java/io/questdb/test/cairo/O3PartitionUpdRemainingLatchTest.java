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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * An exception thrown between {@code o3PartitionUpdRemaining.incrementAndGet()} and the
 * {@code o3CommitPartitionAsync()} that takes ownership of the matching decrement must fail the
 * commit LOUDLY -- not hang it.
 * <p>
 * That counter is only ever lowered by a dispatched unit draining through
 * {@code TableWriter#o3ConsumePartitionUpdates()}, whose loop is
 * {@code do { ... } while (o3PartitionUpdRemaining.get() > 0);} -- no timeout, no error escape. So a
 * throw in that window used to strand the counter above zero with nobody obliged to lower it, and
 * the commit spun forever: an unkillable WAL-apply hang, not a crash and not a suspended table.
 * <p>
 * This was not theoretical. A NULL symbol value in a composite IDENTITY dimension threw in exactly
 * this window and hung WAL apply on a 4-row INSERT (fixed at source by commit 1654f92f17, which also
 * moved the render out of the window; {@code CompositeNullDimensionTest} covers that specific
 * trigger). These tests close the CLASS instead of that one instance: they inject a failure into the
 * window directly, via {@link TableWriter#O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH}, and assert
 * the commit ends as a suspended table within a bounded time.
 * <p>
 * Every test is {@code @Test(timeout = 30_000)} on purpose: the pre-fix failure mode is an infinite
 * spin, so a plain assertion-based test would never reach its assertions -- it would wedge the whole
 * suite instead of failing it. Each test also runs its own NEGATIVE CONTROL first (the identical
 * commit with the injection OFF, which must succeed and leave the table unsuspended), so a test that
 * passed merely because the commit never reached the dispatch path at all would be caught.
 */
public class O3PartitionUpdRemainingLatchTest extends AbstractCairoTest {

    @After
    public void clearInjection() {
        TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = false;
    }

    /**
     * The composite dispatch site, {@code TableWriter#dispatchCompositeCellRange}. This is the site
     * that produced the original real-world hang.
     */
    @Test(timeout = 30_000)
    public void testCompositeThrowInWindowSuspendsInsteadOfHanging() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            // NEGATIVE CONTROL: same shape, injection off. Must reach dispatch and succeed.
            execute("insert into c values ('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                    "('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            drainWalQueue();
            assertNotSuspended("c");
            assertCount("c", 2);

            // Now inject a failure into the increment->dispatch window.
            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = true;
            execute("insert into c values ('2023-01-01T00:00:01.000000Z','BTC',3.0)");
            drainWalQueue();

            // Pre-fix: drainWalQueue() above never returns. Post-fix: a loud, diagnosable failure.
            Assert.assertTrue(
                    "the injected failure must suspend the table, not hang the apply",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));

            // The failed commit must have been rolled back whole -- no half-applied row.
            assertCount("c", 2);

            // And the writer must be left in a state that recovers: with the injection off, the very
            // same transaction replays and lands. A stranded counter would have made this hang too.
            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = false;
            execute("alter table c resume wal");
            drainWalQueue();
            assertNotSuspended("c");
            assertCount("c", 3);
            assertRows("select ts, exch, px from c order by ts",
                    "ts\texch\tpx\n" +
                            "2023-01-01T00:00:00.000000Z\tBTC\t1.0\n" +
                            "2023-01-01T00:00:01.000000Z\tBTC\t3.0\n" +
                            "2023-01-01T00:00:02.000000Z\tETH\t2.0\n");
        });
    }

    /**
     * The general dispatch site, {@code TableWriter#processO3BlockPlain}. This one is SHIPPED
     * PRODUCT -- it has nothing to do with composite partitioning; every ordinary partitioned table
     * goes through it.
     */
    @Test(timeout = 30_000)
    public void testPlainTableThrowInWindowSuspendsInsteadOfHanging() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Seed an in-order partition, then commit an OUT-OF-ORDER row: that is what forces the
            // commit down processO3BlockPlain's per-partition dispatch loop rather than a fast append.
            execute("insert into p values ('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                    "('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            drainWalQueue();
            assertNotSuspended("p");

            // NEGATIVE CONTROL: an out-of-order commit with the injection off must succeed.
            execute("insert into p values ('2023-01-01T00:00:01.000000Z','BTC',3.0)");
            drainWalQueue();
            assertNotSuspended("p");
            assertCount("p", 3);

            // Same out-of-order shape, injection on.
            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = true;
            execute("insert into p values ('2023-01-01T00:00:00.500000Z','SOL',4.0)");
            drainWalQueue();

            Assert.assertTrue(
                    "the injected failure must suspend the table, not hang the apply",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("p")));
            assertCount("p", 3);

            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = false;
            execute("alter table p resume wal");
            drainWalQueue();
            assertNotSuspended("p");
            assertCount("p", 4);
            assertRows("select ts, exch, px from p order by ts",
                    "ts\texch\tpx\n" +
                            "2023-01-01T00:00:00.000000Z\tBTC\t1.0\n" +
                            "2023-01-01T00:00:00.500000Z\tSOL\t4.0\n" +
                            "2023-01-01T00:00:01.000000Z\tBTC\t3.0\n" +
                            "2023-01-01T00:00:02.000000Z\tETH\t2.0\n");
        });
    }

    /**
     * Multi-partition variant of the plain case: the failure lands on ONE partition of a commit that
     * dispatches several. The sibling partitions' units are genuinely in flight when the throw
     * happens, so this is the shape where an over-eager compensation (lowering an already-lowered
     * counter, driving it negative) would deadlock instead -- see the ownership comments at the
     * dispatch site.
     */
    @Test(timeout = 30_000)
    public void testPlainMultiPartitionThrowInWindowSuspendsInsteadOfHanging() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table m (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            execute("insert into m values ('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                    "('2023-01-02T00:00:00.000000Z','ETH',2.0)," +
                    "('2023-01-03T00:00:00.000000Z','SOL',3.0)");
            drainWalQueue();
            assertNotSuspended("m");

            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = true;
            execute("insert into m values ('2023-01-01T00:00:00.500000Z','BTC',4.0)," +
                    "('2023-01-02T00:00:00.500000Z','ETH',5.0)," +
                    "('2023-01-03T00:00:00.500000Z','SOL',6.0)");
            drainWalQueue();

            Assert.assertTrue(
                    "the injected failure must suspend the table, not hang the apply",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("m")));
            assertCount("m", 3);

            TableWriter.O3_FAIL_BETWEEN_PARTITION_COUNT_AND_DISPATCH = false;
            execute("alter table m resume wal");
            drainWalQueue();
            assertNotSuspended("m");
            assertCount("m", 6);
        });
    }

    private void assertCount(String tableName, int expected) throws Exception {
        assertQuery("select count() from " + tableName)
                .noLeakCheck().noRandomAccess().expectSize().returns("count\n" + expected + "\n");
    }

    /**
     * Deliberately a single, plain execution path rather than {@code assertQuery(...)}'s multi-cursor
     * battery. This test's subject is the counter-ownership window, not cursor-variant equivalence —
     * the battery's metadata expectations (declared timestamp, random access, size) are orthogonal
     * here and only obscure a hang regression. Cursor-variant coverage lives in the composite read
     * suites and in the differential fuzz harness.
     */
    private void assertRows(String sql, String expected) throws Exception {
        printSql(sql);
        TestUtils.assertEquals(expected, sink);
    }

    private void assertNotSuspended(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        Assert.assertFalse(tableName + " must not be suspended", engine.getTableSequencerAPI().isSuspended(token));
    }
}
