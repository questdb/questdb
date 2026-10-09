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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * The commit-pointer sync gate: {@code _txn} ({@code TxWriter.commit}), {@code _cv}
 * ({@code ColumnVersionWriter.doCommit}) and the index files ({@code BitmapIndexWriter.commit} /
 * {@code PostingIndexWriter.commit}) decide per-commit durability from {@link CommitMode#appliesColumnSync},
 * like the column data they describe. Under ADAPTIVE apply does not flush them; the durable epoch does.
 * {@link SyncAttributingFilesFacade} scopes every count to one table's directory.
 */
public class CommitPointerSyncGateTest extends AbstractCairoTest {

    /**
     * ADAPTIVE does not flush {@code _txn}/{@code _cv} on every apply, but the durable epoch forces both,
     * so the laziness never costs crash safety. Both halves are asserted on the same table in one run.
     */
    @Test
    public void testAdaptiveIsLazyPerApplyButForcedByEpoch() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Big enough that no cadence/backlog epoch can fire inside the measurement window; the epoch under
        // test is triggered explicitly below.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 100_000_000);
        assertMemoryLeak(facade, () -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            final String dir = engine.verifyTableName("t").getDirName();

            // Warm-up: the first apply batch always epochs (lastEpochTs == 0). The two rows also establish a
            // max timestamp, and the ADD COLUMN sets up the apply-path shape that dirties _cv (see below).
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t values ('2024-01-01T00:00:10.000000Z', 2)");
            execute("alter table t add column extra long");
            drainWalQueue();

            // Steady-state apply: no epoch is due, so nothing may be flushed. The row lands out of order
            // into a partition that carries a column top: ColumnVersionWriter.commit() returns early unless
            // it has changes, and only an O3 rewrite of a partition with a column top changes _cv. With any
            // other shape the _cv assertion reads 0 under every commit mode and proves nothing.
            facade.clearCounters();
            execute("insert into t values ('2024-01-01T00:00:05.000000Z', 3, 4)");
            drainWalQueue();

            Assert.assertEquals(
                    "ADAPTIVE must not flush _txn on the apply path (lazy apply; the epoch is the anchor)",
                    0, facade.barrierCount(dir + "/_txn"));
            Assert.assertEquals(
                    "ADAPTIVE must not flush _cv on the apply path",
                    0, facade.barrierCount(dir + "/_cv"));

            // The durable epoch forces both, regardless of mode.
            facade.clearCounters();
            try (io.questdb.cairo.TableWriter w = getWriter(engine.verifyTableName("t"))) {
                w.advanceDurableEpoch(System.currentTimeMillis());
            }
            Assert.assertTrue(
                    "the durable epoch MUST force _txn durable (msync + fsync), independent of commit mode",
                    facade.barrierCount(dir + "/_txn") > 0);
            Assert.assertTrue(
                    "the durable epoch MUST force _cv durable, independent of commit mode",
                    facade.barrierCount(dir + "/_cv") > 0);
            Assert.assertTrue(
                    "the durable epoch MUST fsync the save point file that holds the _txn copy",
                    facade.fsyncCount(dir + "/" + DurableEpochManifest.FILE_NAME) > 0);
        });
    }

    /**
     * The index files follow the same gate as the column they index: lazy under ADAPTIVE on the apply
     * path, force-flushed by the epoch. The epoch half matters most: skipping {@code commit()}'s flush
     * under ADAPTIVE is safe only because {@code fsyncMaterializedState()} calls {@code sync(false)} on
     * every indexer.
     */
    @Test
    public void testAdaptiveIndexIsLazyPerApplyButForcedByEpoch() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "1h");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 100_000_000);
        assertMemoryLeak(facade, () -> {
            execute("create table ti (ts timestamp, s symbol index, v long) " +
                    "timestamp(ts) partition by day wal");
            final String dir = engine.verifyTableName("ti").getDirName();

            execute("insert into ti values ('2024-01-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();

            facade.clearCounters();
            execute("insert into ti values ('2024-01-01T00:00:01.000000Z', 'b', 2)");
            drainWalQueue();
            Assert.assertEquals(
                    "ADAPTIVE must not flush the index .k file on the apply path",
                    0, facade.barrierCount(dir + "/2024-01-01/s.k"));
            Assert.assertEquals(
                    "ADAPTIVE must not flush the index .v file on the apply path",
                    0, facade.barrierCount(dir + "/2024-01-01/s.v"));

            facade.clearCounters();
            try (io.questdb.cairo.TableWriter w = getWriter(engine.verifyTableName("ti"))) {
                w.advanceDurableEpoch(System.currentTimeMillis());
            }
            Assert.assertTrue(
                    "the durable epoch MUST force the index .k file durable",
                    facade.barrierCount(dir + "/2024-01-01/s.k") > 0);
            Assert.assertTrue(
                    "the durable epoch MUST force the index .v file durable",
                    facade.barrierCount(dir + "/2024-01-01/s.v") > 0);
        });
    }

    /**
     * Pure-function contract for {@code CommitMode.fromString}: an unrecognised token maps to
     * {@link CommitMode#UNKNOWN}, never to a mode. {@code PropServerConfiguration} turns that into a startup
     * failure, so a typo'd {@code cairo.commit.mode=syncc} cannot silently select a weaker durability
     * grade than the operator asked for.
     */
    @Test
    public void testFromStringRejectsUnrecognisedTokens() {
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString("syncc"));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString(""));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString("adaptive2"));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString("unset"));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString(null));

        // Recognised modes, case-insensitively.
        Assert.assertEquals(CommitMode.NOSYNC, CommitMode.fromString("NoSync"));
        Assert.assertEquals(CommitMode.SYNC, CommitMode.fromString("SYNC"));
        Assert.assertEquals(CommitMode.ASYNC, CommitMode.fromString("async"));
        Assert.assertEquals(CommitMode.ADAPTIVE, CommitMode.fromString("Adaptive"));
    }

    /**
     * A {@code nosync} instance must not flush the commit pointers at all. The paired arm is
     * {@link #testSyncInstanceFlushesCommitPointers}; together they fix the polarity of the gate, so a
     * regression that inverts it fails one arm.
     */
    @Test
    public void testNosyncInstanceDoesNotFlushCommitPointers() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(facade, () -> {
            execute("create table lazy (ts timestamp, v long) timestamp(ts) partition by day wal");
            final String lazyDir = engine.verifyTableName("lazy").getDirName();

            execute("insert into lazy values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            facade.clearCounters();
            execute("insert into lazy values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertEquals(
                    "a NOSYNC instance must not flush _txn",
                    0, facade.barrierCount(lazyDir + "/_txn"));
            Assert.assertEquals(
                    "a NOSYNC instance must not flush _cv",
                    0, facade.barrierCount(lazyDir + "/_cv"));
        });
    }

    /**
     * A {@code sync} instance flushes its {@code _txn} on every commit, an eager grade that ADAPTIVE does
     * not share (see {@link #testAdaptiveIsLazyPerApplyButForcedByEpoch}).
     */
    @Test
    public void testSyncInstanceFlushesCommitPointers() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertMemoryLeak(facade, () -> {
            execute("create table eager (ts timestamp, v long) timestamp(ts) partition by day wal");
            final String eagerDir = engine.verifyTableName("eager").getDirName();

            execute("insert into eager values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            facade.clearCounters();
            execute("insert into eager values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertTrue(
                    "a SYNC instance MUST flush _txn on every commit",
                    facade.barrierCount(eagerDir + "/_txn") > 0);
        });
    }

    /**
     * {@code structuralCommitMode} maps ADAPTIVE onto SYNC and leaves every other mode alone.
     * One-shot structural writers (table conversion, WAL staging creation, checkpoint restore) run
     * outside any table writer and outside the epoch's coverage, so they must not inherit the apply-path
     * laziness; this helper keeps them at the {@code != NOSYNC} grade.
     */
    @Test
    public void testStructuralCommitModeKeepsAdaptiveSitesEager() {
        Assert.assertEquals(CommitMode.SYNC, CommitMode.structuralCommitMode(CommitMode.ADAPTIVE));
        Assert.assertEquals(CommitMode.NOSYNC, CommitMode.structuralCommitMode(CommitMode.NOSYNC));
        Assert.assertEquals(CommitMode.SYNC, CommitMode.structuralCommitMode(CommitMode.SYNC));
        Assert.assertEquals(CommitMode.ASYNC, CommitMode.structuralCommitMode(CommitMode.ASYNC));

        // ADAPTIVE mapped as structural flushes; NOSYNC does not.
        Assert.assertTrue(CommitMode.appliesColumnSync(CommitMode.structuralCommitMode(CommitMode.ADAPTIVE)));
        Assert.assertFalse(CommitMode.appliesColumnSync(CommitMode.structuralCommitMode(CommitMode.NOSYNC)));
    }
}
