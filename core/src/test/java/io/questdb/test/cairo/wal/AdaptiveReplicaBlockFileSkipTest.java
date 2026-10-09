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
import io.questdb.std.Os;
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
 * The apply-path {@code BlockFileWriter} writes of a materialized view's refresh state ({@code _mv.s}) and
 * definition ({@code _mv}) follow the node's role: a replica ({@link LocalDurabilityPolicy#REPLICA_SKIP})
 * skips their fsync, since they are a rebuildable cache, while a primary or single node
 * ({@link LocalDurabilityPolicy#ALWAYS_ON}) keeps it. The commit mode stays adaptive throughout, so only the
 * {@code LocalDurabilityPolicy.resolveCommitMode} gate at each site can suppress the sync. The CREATE-time
 * definition writes in {@code SequencerMetadata.create} stay durable on every role.
 *
 * <p>The plain-view definition ({@code _view}) has no test here: on a single node {@code CREATE VIEW} and
 * {@code ALTER VIEW} write it through {@code SqlCompilerImpl}'s own {@code BlockFileWriter}, and the apply-path
 * site runs only when a replica replays the event, so covering it needs replication.
 */
public class AdaptiveReplicaBlockFileSkipTest extends AbstractCairoTest {

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

                // Visibility is unaffected: skipping the fsync does not skip the write itself.
                assertQuery("select sym, price from price_1h")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
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
                    .expectSize()
                    .returns("""
                            sym\tprice
                            gbpusd\t1.32
                            """);
        });
    }

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

                // Visibility is unaffected: the new definition is readable without the sync.
                assertQuery("select refresh_limit, refresh_limit_unit from materialized_views")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
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

    /**
     * The CREATE-time view and mat-view definition writes in {@code SequencerMetadata.create}
     * ({@code txn_seq/_mv}, {@code txn_seq/_view}) use the configured commit mode, not
     * {@code LocalDurabilityPolicy.resolveCommitMode}: a view definition is structural DDL, not lazily applied
     * column data, so it stays durable under any mode but NOSYNC on every role. With
     * {@link LocalDurabilityPolicy#REPLICA_SKIP} and adaptive, the one combination {@code resolveCommitMode}
     * downgrades, {@code CREATE MATERIALIZED VIEW} still syncs the sequencer's {@code _mv}.
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

                // Under REPLICA_SKIP and adaptive every resolveCommitMode site downgrades to NOSYNC, so a sync
                // of the sequencer-dir (txn_seq) _mv comes from SequencerMetadata.create's configured mode.
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

    /**
     * A FilesFacade that counts every {@code msync}/{@code fdatasync} call whose resolved file path
     * ends with the given {@code targetFileSuffix}, e.g. {@link MatViewState#MAT_VIEW_STATE_FILE_NAME}
     * ({@code _mv.s}) or {@link MatViewDefinition#MAT_VIEW_DEFINITION_FILE_NAME} ({@code _mv}). It maps
     * fd -> path on open and addr -> fd on mmap to resolve each sync to a path.
     * {@code BlockFileWriter.commit()} syncs through {@code MemoryCMARW.sync()} -> {@code FilesFacade.msync}.
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

        /**
         * Off Darwin {@code FilesFacadeImpl.barrierFsync} routes through the overridable
         * {@code fdatasync}, which has already been recorded; on Darwin it calls the static directly, so
         * a facade that does not override this sees no WAL ordering barrier at all and its assertions
         * pass having observed nothing.
         */
        @Override
        public void barrierFsync(long fd) {
            super.barrierFsync(fd);
            if (Os.isOSX()) {
                record(fdToPath.get(fd), "fdatasync");
            }
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
