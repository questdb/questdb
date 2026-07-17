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
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * S5.1 Task 5: the apply-path materialized-view / view-definition {@code BlockFileWriter} writes
 * ({@code _mv.s} refresh state, {@code _mv} definition) are role-aware — a replica
 * ({@link LocalDurabilityPolicy#REPLICA_SKIP}) skips their fsync (they are a rebuildable cache of
 * object-store truth), while a primary / single-node (default {@link LocalDurabilityPolicy#ALWAYS_ON})
 * keeps it. {@code commit_mode=adaptive} is kept on throughout so the writes are otherwise
 * sync-eligible — exactly like {@code AdaptiveReplicaEpochSkipTest} — so the ONLY thing that can
 * suppress the sync is the new {@code LocalDurabilityPolicy.resolveCommitMode} gate at each site, not
 * the commit-mode cadence.
 *
 * <p>(a) MUST-HAVE: {@code _mv.s} mat-view refresh-state skip-vs-sync (the frequent path, written by
 * {@code ApplyWal2TableJob.updateMatViewRefreshState} on every base-table-triggered refresh).
 * <p>(b) {@code _mv} mat-view definition skip-vs-sync (written by {@code TableWriter.updateMatViewDefinition},
 * driven here via {@code ALTER MATERIALIZED VIEW ... SET REFRESH LIMIT ...}).
 * <p>(c) {@code _view} plain-view definition — DEFERRED, not dedicated-tested here. Investigated and
 * confirmed genuinely intractable as a single-node OSS unit test: {@code CREATE VIEW}/{@code ALTER VIEW}
 * apply SYNCHRONOUSLY on the primary via {@code SqlCompilerImpl.executeCreateView}/{@code alterViewExecution}
 * -> {@code CairoEngine.replaceViewDefinition}, which writes {@code _view} immediately using
 * {@code SqlCompilerImpl}'s OWN separate {@code BlockFileWriter} field (constructed once per compiler
 * instance from the raw config commit mode) — never touching {@code ApplyWal2TableJob}'s
 * {@code VIEW_DEFINITION} case. That case only fires when a REPLICA replays the WAL event the primary
 * already wrote; there is no single-node OSS SQL path that drives it. The edit is still made there
 * (see the comment on that call site) since the mechanism is identical to (a)/(b); end-to-end
 * {@code _view} coverage requires real replication and is left to the view-replication suite.
 */
public class AdaptiveReplicaBlockFileSkipTest extends AbstractCairoTest {

    // ---------- (a) _mv.s mat-view refresh state ----------

    @Test
    public void testReplicaSkipMatViewStateNoSync() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final BlockFileSyncTrackingFacade trackFf = new BlockFileSyncTrackingFacade(MatViewState.MAT_VIEW_STATE_FILE_NAME);
        assertMemoryLeak(trackFf, () -> {
            execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                trackFf.reset(); // isolate the insert-triggered refresh apply below

                execute("insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01:00.000000Z')");
                drainWalAndMatViewQueues(); // base insert -> refresh job -> MAT_VIEW_DATA apply -> updateMatViewRefreshState

                Assert.assertEquals(
                        "REPLICA_SKIP must not sync " + MatViewState.MAT_VIEW_STATE_FILE_NAME + ", but got: " + trackFf.getSyncedPaths(),
                        0, trackFf.getSyncCount()
                );

                // Visibility is unaffected — skipping the fsync doesn't skip the write itself.
                assertQuery("select sym, price from price_1h")
                        .noLeakCheck()
                        .returnsOnce("""
                                sym\tprice
                                gbpusd\t1.32
                                """);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testAlwaysOnMatViewStateSyncs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final BlockFileSyncTrackingFacade trackFf = new BlockFileSyncTrackingFacade(MatViewState.MAT_VIEW_STATE_FILE_NAME);
        assertMemoryLeak(trackFf, () -> {
            execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

            // default policy = ALWAYS_ON (no setLocalDurabilityPolicy call)
            trackFf.reset();
            execute("insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2024-09-10T12:01:00.000000Z')");
            drainWalAndMatViewQueues();

            Assert.assertTrue(
                    "ALWAYS_ON (default) must sync " + MatViewState.MAT_VIEW_STATE_FILE_NAME + ", but got 0",
                    trackFf.getSyncCount() > 0
            );

            assertQuery("select sym, price from price_1h")
                    .noLeakCheck()
                    .returnsOnce("""
                            sym\tprice
                            gbpusd\t1.32
                            """);
        });
    }

    // ---------- (b) _mv mat-view definition ----------

    @Test
    public void testReplicaSkipMatViewDefinitionNoSync() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final BlockFileSyncTrackingFacade trackFf = new BlockFileSyncTrackingFacade(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
        assertMemoryLeak(trackFf, () -> {
            execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                trackFf.reset();

                // Routes through TableWriter.updateMatViewDefinition on the view's own WAL apply.
                execute("alter materialized view price_1h set refresh limit 2 hours;");
                drainWalAndMatViewQueues();

                Assert.assertEquals(
                        "REPLICA_SKIP must not sync " + MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME + ", but got: " + trackFf.getSyncedPaths(),
                        0, trackFf.getSyncCount()
                );

                // Visibility is unaffected — the new definition is readable regardless of the sync skip.
                assertQuery("select refresh_limit, refresh_limit_unit from materialized_views")
                        .noLeakCheck()
                        .returnsOnce("""
                                refresh_limit\trefresh_limit_unit
                                2\tHOUR
                                """);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testAlwaysOnMatViewDefinitionSyncs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final BlockFileSyncTrackingFacade trackFf = new BlockFileSyncTrackingFacade(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
        assertMemoryLeak(trackFf, () -> {
            execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

            trackFf.reset();
            execute("alter materialized view price_1h set refresh limit 2 hours;");
            drainWalAndMatViewQueues();

            Assert.assertTrue(
                    "ALWAYS_ON (default) must sync " + MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME + ", but got 0",
                    trackFf.getSyncCount() > 0
            );
        });
    }

    // ---------- (d) SequencerMetadata.create definition waiver (review Finding 5.1) ----------

    /**
     * Finding 5.1 (waiver — the DELIBERATE opposite of (a)/(b)): the CREATE-time view/mat-view DEFINITION
     * writes in {@code SequencerMetadata.create} ({@code txn_seq/_mv}, {@code txn_seq/_view}) use the RAW
     * configured commit mode DELIBERATELY — they are intentionally NOT routed through
     * {@code LocalDurabilityPolicy.resolveCommitMode} the way the apply-side sites in (a)/(b) are. A view
     * definition is structural DDL (not the lazily-applied column data the durable epoch protects), so it
     * must stay durable under {@code commitMode != NOSYNC} REGARDLESS of role. This test pins that: even with
     * {@link LocalDurabilityPolicy#REPLICA_SKIP} installed and {@code commit_mode=adaptive} (the ONLY mode
     * {@code resolveCommitMode} would downgrade to NOSYNC on a replica), {@code CREATE MATERIALIZED VIEW}
     * still fsyncs the sequencer's {@code _mv} definition. If a future change wrongly routed this site through
     * the policy, the sync would vanish under REPLICA_SKIP and this test would fail.
     */
    @Test
    public void testSequencerMetaCreateMatViewDefinitionStaysDurableRegardlessOfRole() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final BlockFileSyncTrackingFacade trackFf = new BlockFileSyncTrackingFacade(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME);
        assertMemoryLeak(trackFf, () -> {
            execute("create table base_price (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");

            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                trackFf.reset(); // isolate the CREATE MATERIALIZED VIEW below
                execute("create materialized view price_1h as select sym, last(price) as price, ts from base_price sample by 1h");

                // Under REPLICA_SKIP + adaptive, every POLICY-AWARE (resolveCommitMode) site downgrades to
                // NOSYNC and does NOT sync — so ANY _mv sync here comes from a RAW-commitMode (waiver) site.
                // Assert specifically that SequencerMetadata.create's sequencer-dir (txn_seq) _mv was synced:
                // the definition stays durable regardless of role.
                boolean seqMvSynced = false;
                for (String p : trackFf.getSyncedPaths()) {
                    if (p.contains(WalUtils.SEQ_DIR) && p.endsWith(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME)) {
                        seqMvSynced = true;
                        break;
                    }
                }
                Assert.assertTrue(
                        "SequencerMetadata.create must keep the mat-view definition durable regardless of role "
                                + "(REPLICA_SKIP); synced=" + trackFf.getSyncedPaths(),
                        seqMvSynced
                );
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    // ---------- (c) _view plain-view definition: DEFERRED, see class javadoc ----------

    /**
     * A FilesFacade that counts every {@code msync}/{@code fdatasync} call whose resolved file path
     * ends with the given {@code targetFileSuffix} — e.g. {@link MatViewState#MAT_VIEW_STATE_FILE_NAME}
     * ({@code _mv.s}) or {@link MatViewDefinition#MAT_VIEW_DEFINITION_FILE_NAME} ({@code _mv}).
     *
     * <p>Same fd/mmap-address tracking technique as {@code AdaptiveWalDurabilityTest.TableSyncTrackingFacade}:
     * tracks fd -> path (on open), addr -> fd (on mmap), then resolves msync(addr)/fdatasync(fd) to a
     * path and counts it iff the path ends with the target suffix. {@code BlockFileWriter.commit()}
     * syncs via {@code MemoryCMARW.sync()} -> {@code FilesFacade.msync}, so {@code msync} is the one
     * that actually fires in practice; {@code fdatasync} is tracked too for robustness / parity with
     * the existing facade.
     */
    static class BlockFileSyncTrackingFacade extends TestFilesFacadeImpl {
        private final Map<Long, Long> addrToFd = new HashMap<>();
        private final Map<Long, String> fdToPath = new HashMap<>();
        private final List<String> syncedPaths = new ArrayList<>();
        private final String targetFileSuffix;
        private long syncCount = 0;

        BlockFileSyncTrackingFacade(String targetFileSuffix) {
            this.targetFileSuffix = targetFileSuffix;
        }

        public long getSyncCount() {
            return syncCount;
        }

        public List<String> getSyncedPaths() {
            return new ArrayList<>(syncedPaths);
        }

        public void reset() {
            syncCount = 0;
            syncedPaths.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            record(fdToPath.get(fd), "fdatasync");
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmapNoCache(fd, len, offset, flags, memoryTag);
            if (addr != -1L && addr != 0L) {
                addrToFd.put(addr, fd);
            }
            return addr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            Long fd = addrToFd.get(addr);
            if (fd != null) {
                record(fdToPath.get(fd), "msync(" + (async ? "async" : "sync") + ")");
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            addrToFd.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openAppend(LPSZ name) {
            long fd = super.openAppend(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            long fd = super.openCleanRW(name, size);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            long fd = super.openRO(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            trackFd(fd, name);
            return fd;
        }

        private void record(String path, String kind) {
            if (path != null && path.endsWith(targetFileSuffix)) {
                syncCount++;
                syncedPaths.add(kind + ":" + path);
            }
        }

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }
}
