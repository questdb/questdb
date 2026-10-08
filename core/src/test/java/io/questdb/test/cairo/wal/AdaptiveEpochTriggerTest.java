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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AdaptiveEpochTriggerTest extends AbstractCairoTest {

    @Test
    public void testAdaptiveEpochConfigDefaults() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(60000L, engine.getConfiguration().getAdaptiveEpochIntervalMs());
            Assert.assertEquals(5_000_000L, engine.getConfiguration().getAdaptiveEpochMaxRows());
        });
    }

    // Long interval so, after the mandatory first-batch epoch, ONLY the row cap can fire within a
    // sub-second test — independent of wall-clock advancement.
    private static final int LONG_INTERVAL_MS = 3_600_000;

    @Test
    public void testBacklogCapForcesEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");
            Assert.assertTrue("first-batch epoch should advance the floor", floorAfterFirst > 0);

            // 600 rows (> cap 500) with an effectively infinite interval -> only the cap can fire.
            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterCap = durableEpoch("x");
            Assert.assertTrue(
                    "backlog cap should force an epoch: floorAfterCap=" + floorAfterCap
                            + " must exceed floorAfterFirst=" + floorAfterFirst,
                    floorAfterCap > floorAfterFirst
            );
        });
    }

    @Test
    public void testCapDisabledNoEpochOnBacklog() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0); // cap disabled
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterBacklog = durableEpoch("x");
            Assert.assertEquals(
                    "cap disabled + long interval must not fire a second epoch",
                    floorAfterFirst, floorAfterBacklog
            );
        });
    }

    @Test
    public void testBacklogCounterResetsAfterEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("cap should fire epoch #2", floor2 > floor1);

            // 300 < cap, counter reset after epoch #2 -> no epoch (else cumulative 600+300 would fire).
            execute("insert into x select timestamp_sequence(200000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor3 = durableEpoch("x");
            Assert.assertEquals("counter reset: 300 < cap must not fire", floor2, floor3);

            // 300 more -> 600 since epoch #2 >= cap -> epoch #3.
            execute("insert into x select timestamp_sequence(300000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor4 = durableEpoch("x");
            Assert.assertTrue("crossing the cap again should fire epoch #3", floor4 > floor2);
        });
    }

    @Test
    public void testTimePathStillFiresWhenCapHuge() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // every batch (time path)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1_000_000_000L); // cap never hit
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("interval=0 must still epoch every batch (time path intact)", floor2 > floor1);
        });
    }

    // A graceful writer close must fold any committed-but-un-epoched tail into a final durable epoch,
    // so a clean restart lands on the committed frontier with nothing to roll forward. Long interval +
    // disabled cap means only the mandatory first-batch epoch fires; a second applied batch is left as
    // an un-epoched tail, and closing the writer must catch the durable epoch up to seqTxn.
    @Test
    public void testGracefulCloseFlushesUnEpochedTail() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS); // no time-based re-fire
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);                // no cap-based re-fire
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            Assert.assertTrue("first-batch epoch should advance the floor", durableEpoch("x") > 0);

            // A second applied batch with no time/cap trigger leaves a committed-but-un-epoched tail.
            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long frontier = committedSeqTxn("x");
            Assert.assertTrue(
                    "precondition: an un-epoched tail must exist (durableEpoch " + durableEpoch("x")
                            + " < seqTxn " + frontier + ")",
                    durableEpoch("x") < frontier
            );

            // Clean writer close -> doClose flushes a final durable epoch over the tail.
            engine.releaseInactive();

            Assert.assertEquals(
                    "graceful close must advance the durable epoch to the committed frontier",
                    frontier, durableEpoch("x")
            );
        });
    }

    // cairo.adaptive.epoch.flush.on.close=false is the operator kill-switch: a clean close must NOT
    // flush, leaving the tail for the next boot's WAL replay (durableEpoch stays behind the frontier).
    @Test
    public void testEpochFlushOnCloseDisabledLeavesTail() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long floorAfterFirst = durableEpoch("x");
            Assert.assertTrue("first-batch epoch should advance the floor", floorAfterFirst > 0);

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long frontier = committedSeqTxn("x");
            Assert.assertTrue("precondition: an un-epoched tail must exist", durableEpoch("x") < frontier);

            engine.releaseInactive();

            Assert.assertEquals(
                    "flush-on-close disabled: a clean close must NOT advance the durable epoch",
                    floorAfterFirst, durableEpoch("x")
            );
        });
    }

    private static long committedSeqTxn(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        return engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn();
    }

    private static long durableEpoch(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        return engine.getTableSequencerAPI().getTxnTracker(token).getDurableEpochSeqTxn();
    }
}
