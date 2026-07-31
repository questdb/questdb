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
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * The COMMIT-POINTER sync gate: {@code _txn} ({@code TxWriter.commit}), {@code _cv}
 * ({@code ColumnVersionWriter.doCommit}) and the index files ({@code BitmapIndexWriter.commit} /
 * {@code PostingIndexWriter.commit}) must make their per-commit durability decision from
 * {@link CommitMode#appliesColumnSync} applied to the table's EFFECTIVE mode -- exactly like the column
 * data they describe.
 *
 * <p>Historically all four sites read the INSTANCE-GLOBAL {@code cairo.commit.mode} and branched on
 * {@code != NOSYNC}, which produced two distinct defects:
 * <ol>
 *   <li><b>Inverted per-table polarity (a correctness bug).</b> A {@code WITH commit_mode='sync'} table on
 *       a {@code nosync} instance silently skipped its {@code _txn}/{@code _cv} flush -- a real crash-loss
 *       window for a table that had explicitly asked for durability -- while a {@code nosync} table on a
 *       {@code sync} instance paid for a flush it had opted out of.</li>
 *   <li><b>ADAPTIVE treated as SYNC-grade (a cost bug on the DEFAULT path).</b> Under ADAPTIVE the
 *       materialized table is a rebuildable cache of the durable WAL, so the apply path is deliberately
 *       lazy; flushing {@code _txn}/{@code _cv}/indexes on every apply is precisely the per-commit cost the
 *       lazy-apply gate exists to remove, and is not what makes ADAPTIVE crash-safe.</li>
 * </ol>
 *
 * <p>Every assertion below is scoped to ONE table's directory via
 * {@link SyncAttributingFilesFacade}, so sibling tables (telemetry, etc.) cannot pollute the counts.
 */
public class CommitPointerSyncGateTest extends AbstractCairoTest {

    /**
     * ADAPTIVE must NOT flush {@code _txn}/{@code _cv} on every apply -- but the durable EPOCH must force
     * both, so the laziness never costs crash-safety. Both halves are asserted on the SAME table in one
     * run, which is what makes this a durability test and not merely a "we removed a flush" test.
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

            // Warm-up: the FIRST apply batch always epochs (lastEpochTs == 0), so get that out of the way
            // before measuring the steady state. The two rows also establish a max timestamp, and the ADD
            // COLUMN sets up the one apply-path shape that actually dirties _cv (see below) -- all of it
            // before the measurement window opens.
            execute("insert into t values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into t values ('2024-01-01T00:00:10.000000Z', 2)");
            execute("alter table t add column extra long");
            drainWalQueue();

            // ---- steady-state apply: no epoch is due, so nothing may be flushed ----
            // The measured row lands OUT OF ORDER into a partition that carries a COLUMN TOP, and both
            // halves are load-bearing for the _cv assertion below. ColumnVersionWriter.commit() returns
            // early unless hasChanges, and under a global SYNC -- a mode that unambiguously demands the
            // flush -- the measured barrier counts are: plain in-order append _cv=0; pure O3 with no added
            // column _cv=0; in-order append after ADD COLUMN _cv=0; and only this shape, an O3 rewrite of a
            // partition carrying a column top, _cv=1. Any of the others would make the assertion VACUOUS --
            // reading 0 under EVERY commit mode, and so unable to detect the gate regressing at all.
            facade.clearCounters();
            execute("insert into t values ('2024-01-01T00:00:05.000000Z', 3, 4)");
            drainWalQueue();

            Assert.assertEquals(
                    "ADAPTIVE must not flush _txn on the apply path (lazy apply; the epoch is the anchor)",
                    0, facade.barrierCount(dir + "/_txn"));
            Assert.assertEquals(
                    "ADAPTIVE must not flush _cv on the apply path",
                    0, facade.barrierCount(dir + "/_cv"));

            // ---- the durable epoch must force BOTH, regardless of mode ----
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
                    "the durable epoch MUST fsync the immutable _txn.epoch recovery anchor",
                    facade.fsyncCount(dir + "/_txn.epoch") > 0);
        });
    }

    /**
     * The index files follow the same gate as the column they index: lazy under ADAPTIVE on the apply
     * path, force-flushed by the epoch. The epoch half is the load-bearing one -- {@code commit()}'s
     * durability flush becoming a no-op under ADAPTIVE is only safe because
     * {@code fsyncMaterializedState()} now calls {@code sync(false)} on every indexer explicitly.
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
     * Pure-function contract for {@code CommitMode.fromString}: an UNRECOGNISED token must be
     * {@link CommitMode#UNKNOWN}, distinctly from the two inputs that genuinely mean "defer to the global
     * default". The javadoc used to claim UNSET, which -- had a caller believed it -- would have turned a
     * typo'd {@code commit_mode='syncc'} into a silent "inherit the instance default" instead of the
     * precise SQL error both DDL call sites raise.
     */
    @Test
    public void testFromStringDistinguishesUnknownFromUnset() {
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString("syncc"));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString(""));
        Assert.assertEquals(CommitMode.UNKNOWN, CommitMode.fromString("adaptive2"));
        Assert.assertNotEquals(CommitMode.UNSET, CommitMode.UNKNOWN);

        // The only two inputs that mean "defer to the global default".
        Assert.assertEquals(CommitMode.UNSET, CommitMode.fromString(null));
        Assert.assertEquals(CommitMode.UNSET, CommitMode.fromString("unset"));

        // Recognised modes, case-insensitively.
        Assert.assertEquals(CommitMode.NOSYNC, CommitMode.fromString("NoSync"));
        Assert.assertEquals(CommitMode.SYNC, CommitMode.fromString("SYNC"));
        Assert.assertEquals(CommitMode.ASYNC, CommitMode.fromString("async"));
        Assert.assertEquals(CommitMode.ADAPTIVE, CommitMode.fromString("Adaptive"));
    }

    /**
     * A {@code nosync} table on an {@code adaptive} instance must not be flushed either: the per-table
     * mode wins in BOTH directions. This is the mirror of
     * {@link #testPerTableSyncIsHonouredOnNosyncInstance} and pins that the fix reads the table's mode
     * rather than merely swapping which global mode is privileged.
     */
    @Test
    public void testPerTableNosyncIsHonouredOnSyncInstance() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertMemoryLeak(facade, () -> {
            execute("create table lazy (ts timestamp, v long) timestamp(ts) partition by day wal " +
                    "with commit_mode='nosync'");
            execute("create table eager (ts timestamp, v long) timestamp(ts) partition by day wal");
            final String lazyDir = engine.verifyTableName("lazy").getDirName();
            final String eagerDir = engine.verifyTableName("eager").getDirName();

            execute("insert into lazy values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into eager values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            facade.clearCounters();
            execute("insert into lazy values ('2024-01-01T00:00:01.000000Z', 2)");
            execute("insert into eager values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertEquals(
                    "a per-table NOSYNC table must not flush _txn even on a SYNC instance",
                    0, facade.barrierCount(lazyDir + "/_txn"));
            // Control on the SAME run: the sibling that inherits the global SYNC mode still flushes, so a
            // zero above cannot be an artefact of the harness missing barriers entirely.
            Assert.assertTrue(
                    "control: a table inheriting the global SYNC mode must still flush _txn",
                    facade.barrierCount(eagerDir + "/_txn") > 0);
        });
    }

    /**
     * THE CORRECTNESS CASE. A table created {@code WITH commit_mode='sync'} on a {@code nosync} instance
     * must flush its {@code _txn}/{@code _cv} on commit. Reading the global mode meant it did not -- the
     * table asked for per-commit durability and silently got none.
     */
    @Test
    public void testPerTableSyncIsHonouredOnNosyncInstance() throws Exception {
        final SyncAttributingFilesFacade facade = new SyncAttributingFilesFacade();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(facade, () -> {
            execute("create table eager (ts timestamp, v long) timestamp(ts) partition by day wal " +
                    "with commit_mode='sync'");
            execute("create table lazy (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken eager = engine.verifyTableName("eager");
            final String eagerDir = eager.getDirName();
            final String lazyDir = engine.verifyTableName("lazy").getDirName();

            execute("insert into eager values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into lazy values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            facade.clearCounters();
            execute("insert into eager values ('2024-01-01T00:00:01.000000Z', 2)");
            execute("insert into lazy values ('2024-01-01T00:00:01.000000Z', 2)");
            drainWalQueue();

            Assert.assertTrue(
                    "a per-table SYNC table MUST flush _txn even on a NOSYNC instance",
                    facade.barrierCount(eagerDir + "/_txn") > 0);
            // Control on the SAME run: the sibling that inherits the global NOSYNC mode must stay lazy, so
            // the assertion above cannot pass by the harness simply counting every table's barriers.
            Assert.assertEquals(
                    "control: a table inheriting the global NOSYNC mode must not flush _txn",
                    0, facade.barrierCount(lazyDir + "/_txn"));
        });
    }

    /**
     * {@code structuralCommitMode} maps ADAPTIVE onto SYNC and leaves every other mode alone.
     * One-shot structural writers (table conversion, WAL staging creation, checkpoint restore) run
     * outside any table writer and outside the epoch's coverage, so they must NOT inherit the apply-path
     * laziness -- this helper is what keeps them at their historical {@code != NOSYNC} grade.
     */
    @Test
    public void testStructuralCommitModeKeepsAdaptiveSitesEager() {
        Assert.assertEquals(CommitMode.SYNC, CommitMode.structuralCommitMode(CommitMode.ADAPTIVE));
        Assert.assertEquals(CommitMode.NOSYNC, CommitMode.structuralCommitMode(CommitMode.NOSYNC));
        Assert.assertEquals(CommitMode.SYNC, CommitMode.structuralCommitMode(CommitMode.SYNC));
        Assert.assertEquals(CommitMode.ASYNC, CommitMode.structuralCommitMode(CommitMode.ASYNC));

        // The mapped grades must land on the same side of the apply-path gate as before the change:
        // ADAPTIVE-as-structural flushes, NOSYNC still does not.
        Assert.assertTrue(CommitMode.appliesColumnSync(CommitMode.structuralCommitMode(CommitMode.ADAPTIVE)));
        Assert.assertFalse(CommitMode.appliesColumnSync(CommitMode.structuralCommitMode(CommitMode.NOSYNC)));
    }
}
