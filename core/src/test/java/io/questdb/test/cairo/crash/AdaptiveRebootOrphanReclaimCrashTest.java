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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Proves that recovery seeds the adaptive WAL purge floor from the restored epoch cut, so a fully applied
 * {@code walN} directory a crash leaves behind (a reboot orphan) goes on the first purge after recovery and
 * cannot accumulate across restarts. Two restart models mint the same orphan, with a power loss in an
 * uncommitted WAL tail followed by in-process recovery:
 * <ul>
 *   <li>Model A, the in-process {@code resetForReboot}: a purge right after recovery reclaims the orphan (A1),
 *       and it stays reclaimed after more ingest and a new durable epoch (A2).</li>
 *   <li>Model B, a new {@link CairoEngine} on the same db root, whose start runs recovery as a restarted
 *       process does: the seeded floor equals Model A's, and the same two purges reclaim the orphan (B1, B2).</li>
 * </ul>
 */
public class AdaptiveRebootOrphanReclaimCrashTest extends AbstractAdaptiveCrashSweepTest {

    private static final long BASE_TS = 1_704_067_200_000_000L;
    private static final int PREFIX_ROWS = 4;
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta"};
    private static final int TAIL_ROWS = 6;
    private static final long TS_STRIDE = 8L * 3_600L * 1_000_000L;
    private static final long V_MULT = 2_654_435_761L;
    private static final int W0_CRASH_OFFSET = 3;

    @Test
    public void testFreshRebootMatchesInProcessResetAndSelfHeals() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        // An epoch on every apply batch makes the durable epoch deterministic: the first resumed-ingest
        // apply takes a new one.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Model A: an in-process resetForReboot.
                final int recoveredA = buildOrphanInProcess("ta");
                final TableToken ta = engine.verifyTableName("ta");
                final List<String> aRecover = listWalDirs(ta);
                final long aFloorAfterRecover = durableEpoch(engine, ta);
                logState("A after in-process recover", ta, aRecover);
                Assert.assertFalse("A: a reboot-orphan walN must exist after the crash+recover", aRecover.isEmpty());
                // RecoveryCoordinator.pinRecoveredEpoch() seeds the purge floor from the restored durable
                // epoch cut, so the floor is correct as soon as recovery finishes.
                Assert.assertTrue("A: recovery must seed the purge floor (durableEpochSeqTxn) from the "
                                + "restored epoch cut, not leave it at 0; got " + aFloorAfterRecover,
                        aFloorAfterRecover > 0L);

                // A1: a purge right after recovery, with no ingest, reclaims the orphan.
                forceWalPurge(engine);
                final List<String> aQuiescentPurge = listWalDirs(ta);
                logState("A1 in-process quiescent purge", ta, aQuiescentPurge);
                Assert.assertTrue("A1: with the floor seeded by recovery, the FIRST quiescent purge already "
                                + "RECLAIMS the reboot orphan — there is no pre-epoch retention window left "
                                + "for the soak to observe; got " + aQuiescentPurge,
                        aQuiescentPurge.isEmpty());

                // A2: resume ingest, which takes a new durable epoch, then purge: the orphan stays reclaimed.
                resumeIngest(engine, ta, recoveredA);
                Assert.assertTrue("A2: resumed ingest must advance the purge floor past the orphan",
                        durableEpoch(engine, ta) > 0L);
                forceWalPurge(engine);
                final List<String> aAfterIngest = listWalDirs(ta);
                logState("A2 in-process +ingest+epoch+purge", ta, aAfterIngest);
                Assert.assertTrue("A2: resumed ingest keeps the orphan reclaimed — the floor only ever moves "
                                + "forward, so a post-reboot epoch cannot resurrect WAL the purge already freed",
                        aAfterIngest.isEmpty());

                // Model B: a new engine on the same db root, as a restarted process.
                final int recoveredB = buildOrphanInProcess("tb");
                final TableToken tb = engine.verifyTableName("tb");
                final List<String> bRecover = listWalDirs(tb);
                logState("B after in-process recover (pre-reboot)", tb, bRecover);
                Assert.assertFalse("B: a reboot-orphan walN must exist after the crash+recover", bRecover.isEmpty());
                Assert.assertEquals("B and A must mint an IDENTICAL orphan (same crash + recover path)",
                        aRecover.size(), bRecover.size());

                // Release every handle so nothing stays locked or mapped, then open a new engine on the
                // same db root: its completeInit() runs RecoveryCoordinator.recover() as a restarted process does.
                releaseEngineHandles();

                final List<String> bFreshQuiescentPurge;
                final List<String> bFreshAfterIngest;
                final long bFreshFloorAfterBoot;
                try (CairoEngine restarted = new CairoEngine(configuration)) {
                    TestUtils.drainWalQueue(restarted); // finish the recovery roll-forward
                    final TableToken rtb = restarted.verifyTableName("tb");
                    bFreshFloorAfterBoot = durableEpoch(restarted, rtb);
                    LOG.info().$("[EXP] B fresh engine post-boot: durableEpochSeqTxn=").$(bFreshFloorAfterBoot)
                            .$(" seqTxn=").$(restarted.getTableSequencerAPI().getTxnTracker(rtb).getSeqTxn()).I$();
                    // A new process seeds the floor from the same restored epoch cut as the in-process reset.
                    Assert.assertEquals("B: a REAL fresh-process restart must seed the purge floor from the "
                                    + "restored epoch cut exactly like the in-process reset",
                            aFloorAfterRecover, bFreshFloorAfterBoot);

                    // B1: a purge right after the start, with no ingest, reclaims the orphan, as in A1.
                    forceWalPurge(restarted);
                    bFreshQuiescentPurge = listWalDirs(tb);
                    LOG.info().$("[EXP] B1 fresh-engine quiescent purge: wals=").$(bFreshQuiescentPurge.toString()).I$();
                    Assert.assertTrue("B1 (DECISIVE): a fresh-process restart RECLAIMS the orphan on the first "
                                    + "quiescent purge, identical to the in-process reset — reboot orphans are "
                                    + "not retained and cannot accumulate across restarts; got "
                                    + bFreshQuiescentPurge,
                            bFreshQuiescentPurge.isEmpty());

                    // B2: resume ingest on the restarted engine, then purge: the orphan stays reclaimed.
                    resumeIngest(restarted, rtb, recoveredB);
                    Assert.assertTrue("B2: resumed ingest on the rebooted engine must advance the floor",
                            durableEpoch(restarted, rtb) > 0L);
                    forceWalPurge(restarted);
                    bFreshAfterIngest = listWalDirs(tb);
                    LOG.info().$("[EXP] B2 fresh-engine +ingest+epoch+purge: wals=").$(bFreshAfterIngest.toString()).I$();
                    Assert.assertTrue("B2 (DECISIVE): after a REAL reboot + resumed ingest the orphan stays "
                                    + "reclaimed — orphans do NOT accumulate across reboots",
                            bFreshAfterIngest.isEmpty());

                    restarted.releaseInactive();
                    restarted.releaseAllWalWriters();
                }

                LOG.info().$("[EXP] SUMMARY  A: recover=").$(aRecover.size()).$(" quiescentPurge=").$(aQuiescentPurge.size())
                        .$(" afterIngest=").$(aAfterIngest.size())
                        .$("  |  B(fresh): recover=").$(bRecover.size()).$(" quiescentPurge=").$(bFreshQuiescentPurge.size())
                        .$(" afterIngest=").$(bFreshAfterIngest.size()).I$();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Builds a reboot orphan on {@code name}: an adaptive table with a durable prefix (applied and covered by
     * an epoch), then a W=0 tail torn by a simulated power loss and rolled back by in-process recovery. Returns
     * the number of rows that survive recovery.
     */
    private int buildOrphanInProcess(String name) throws Exception {
        execute("drop table if exists " + name);
        drainWalQueue();
        execute("create table " + name + " (id long, v long, s symbol index, ts timestamp) timestamp(ts) "
                + "partition by day wal");
        final TableToken tt = engine.verifyTableName(name);
        drainWalQueue();
        final TableToken[] tokens = {tt};

        int nextId = 0;
        try (WalWriter w = engine.getWalWriter(tt)) {
            for (int j = 0; j < PREFIX_ROWS; j++) {
                appendRow(w, nextId++);
            }
        }
        drainWalQueue();
        markDurableBaseline();

        // crashable W=0 tail on a held writer, crash armed mid-tail
        final int base = crashFf.durabilityOpCount();
        crashFf.armCrashAt(base + W0_CRASH_OFFSET);
        boolean fired = false;
        final WalWriter w = engine.getWalWriter(tt);
        try {
            int id = nextId;
            for (int j = 0; j < TAIL_ROWS; j++) {
                appendRow(w, id++);
            }
        } catch (CrashSimulationError propagated) {
            fired = true;
        } finally {
            try {
                w.close();
            } catch (CrashSimulationError closeTimeCrash) {
                fired = true;
            }
        }
        if (!fired) {
            crashFf.armCrashAt(-1);
        }
        Assert.assertTrue(name + ": W=0 crash never fired (W0_CRASH_OFFSET mis-tuned)", fired);
        recoverAfterCrash(tokens);
        return (int) rowCount(name);
    }

    /**
     * Resumes ingest on {@code eng}: PREFIX_ROWS more contiguous rows, applied, which takes a new durable epoch.
     */
    private void resumeIngest(CairoEngine eng, TableToken tt, int startId) {
        int id = startId;
        try (WalWriter w = eng.getWalWriter(tt)) {
            for (int j = 0; j < PREFIX_ROWS; j++) {
                appendRow(w, id++);
            }
        }
        TestUtils.drainWalQueue(eng);
    }

    private void appendRow(WalWriter w, int id) {
        final TableWriter.Row row = w.newRow(BASE_TS + (long) id * TS_STRIDE);
        row.putLong(0, id);
        row.putLong(1, (long) id * V_MULT);
        row.putSym(2, SYMBOLS[id % SYMBOLS.length]);
        row.append();
        w.commit();
    }

    private long durableEpoch(CairoEngine eng, TableToken tt) {
        final SeqTxnTracker tr = eng.getTableSequencerAPI().getTxnTracker(tt);
        return tr.getDurableEpochSeqTxn();
    }

    /**
     * Sorted list of {@code walN} directory names on disk for {@code tt} (the reboot-orphan enumeration).
     */
    private List<String> listWalDirs(TableToken tt) {
        final List<String> out = new ArrayList<>();
        final File td = new File(engine.getConfiguration().getDbRoot().toString(), tt.getDirName());
        final File[] top = td.listFiles();
        if (top != null) {
            for (File e : top) {
                if (e.isDirectory() && e.getName().startsWith("wal") && isAllDigits(e.getName().substring(3))) {
                    out.add(e.getName());
                }
            }
        }
        Collections.sort(out);
        return out;
    }

    private void logState(String label, TableToken tt, List<String> wals) {
        final SeqTxnTracker tr = engine.getTableSequencerAPI().getTxnTracker(tt);
        LOG.info().$("[EXP] ").$(label).$(": wals=").$(wals.toString())
                .$(" durableEpochSeqTxn=").$(tr.getDurableEpochSeqTxn())
                .$(" seqTxn=").$(tr.getSeqTxn())
                .$(" writerTxn=").$(tr.getWriterTxn()).I$();
    }

    /**
     * Runs the WAL purge broad sweep to completion on {@code eng} with a strictly increasing clock.
     */
    private void forceWalPurge(CairoEngine eng) {
        eng.releaseAllWalWriters();
        final long step = eng.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        final MicrosecondClock incClock = () -> (tick[0] += step);
        try (WalPurgeJob job = new WalPurgeJob(eng, eng.getConfiguration().getFilesFacade(), incClock)) {
            job.run();
            job.run();
        }
    }

    private long rowCount(String name) {
        try (RecordCursorFactory f = select("select count() from " + name)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isAllDigits(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
