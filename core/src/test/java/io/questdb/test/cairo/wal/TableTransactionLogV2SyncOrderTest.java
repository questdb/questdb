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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Asserts that the V2 sequencer transaction log syncs the part file (txnPartMem) before the header
 * file (txnMem) in both the addEntry path (sync0) and the fullSync/metadata-change path.
 *
 * <p>The invariant "part-file durable before the header that points to it" prevents a crash window
 * where maxTxn=N is persisted in the header but the record for txn N in the part file has not yet
 * been written back by the OS, leaving a zeroed/partial record on reopen.
 *
 * <p>This test would FAIL against the old code because the old sync0() synced only txnMem and
 * never called txnPartMem.sync() at all, so txnPartMem would never appear first in the msync order.
 * Similarly fullSync() only synced txnMem, missing txnPartMem entirely.
 */
public class TableTransactionLogV2SyncOrderTest extends AbstractCairoTest {

    private static final MemorySerializer VOID_SERIALIZER = new MemorySerializer() {
        @Override
        public void fromSink(Object instance, MemoryCR memory, long offsetLo, long offsetHi) {
        }

        @Override
        public short getCommandType(Object instance) {
            return 0;
        }

        @Override
        public void toSink(Object obj, MemoryA sink) {
        }
    };

    /**
     * Verifies that for addEntry (sync0 path), the part file is msync'd before the header.
     * RED against the old code (sync0 only synced txnMem, never txnPartMem).
     * GREEN after the fix (sync0 syncs txnPartMem first, then txnMem).
     */
    @Test
    public void testAddEntrySyncsPartFileBeforeHeader() throws Exception {
        assertMemoryLeak(() -> {
            final SyncOrderFilesFacade syncFf = new SyncOrderFilesFacade();
            final CairoConfiguration cfg = syncConfig(syncFf);

            try (Path path = new Path()) {
                path.of(root).concat("v2seq");
                syncFf.mkdir(path.$(), configuration.getMkDirMode());

                TableTransactionLogV2 v2 = new TableTransactionLogV2(cfg, 16, DefaultWalDirectoryPolicy.INSTANCE);
                try {
                    v2.create(path, System.currentTimeMillis());
                    v2.open(path);
                    syncFf.resetSyncOrder(); // ignore syncs during create/open

                    // Append several transactions; each addEntry calls sync0().
                    for (int i = 0; i < 5; i++) {
                        v2.addEntry(i, i + 1, i + 2, i + 3, System.currentTimeMillis(), 0L, 0L, 0L);
                    }

                    List<String> order = syncFf.getSyncOrder();
                    assertPartBeforeHeader(order, "addEntry (sync0)");
                } finally {
                    v2.close();
                }
            }
        });
    }

    /**
     * Verifies that for fullSync (metadata-change path), the part file is msync'd before the header.
     * RED against the old code (fullSync only synced txnMem, never txnPartMem).
     * GREEN after the fix (fullSync syncs txnPartMem first, then txnMem).
     */
    @Test
    public void testFullSyncSyncsPartFileBeforeHeader() throws Exception {
        assertMemoryLeak(() -> {
            final SyncOrderFilesFacade syncFf = new SyncOrderFilesFacade();
            final CairoConfiguration cfg = syncConfig(syncFf);

            try (Path path = new Path()) {
                path.of(root).concat("v2seqmeta");
                syncFf.mkdir(path.$(), configuration.getMkDirMode());

                TableTransactionLogV2 v2 = new TableTransactionLogV2(cfg, 16, DefaultWalDirectoryPolicy.INSTANCE);
                try {
                    v2.create(path, System.currentTimeMillis());
                    v2.open(path);
                    syncFf.resetSyncOrder(); // ignore syncs during create/open

                    // beginMetadataChangeEntry writes to txnPartMem; fullSync must sync it before txnMem.
                    v2.beginMetadataChangeEntry(1L, VOID_SERIALIZER, null, System.currentTimeMillis());
                    v2.fullSync();

                    List<String> order = syncFf.getSyncOrder();
                    assertPartBeforeHeader(order, "fullSync (metadata-change path)");
                } finally {
                    v2.close();
                }
            }
        });
    }

    private static void assertPartBeforeHeader(List<String> order, String context) {
        int firstPartIdx = -1;
        int firstHeaderIdx = -1;
        for (int i = 0; i < order.size(); i++) {
            String p = order.get(i);
            // Part file lives under _txn_parts/; header is _txnlog at the txn_seq root.
            boolean isPart = p.contains(WalUtils.TXNLOG_PARTS_DIR);
            boolean isHeader = (p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                    || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."));
            if (isPart && firstPartIdx < 0) {
                firstPartIdx = i;
            }
            if (isHeader && firstHeaderIdx < 0) {
                firstHeaderIdx = i;
            }
        }

        if (firstPartIdx < 0 || firstHeaderIdx < 0) {
            StringBuilder sb = new StringBuilder(
                    "Expected both part file and header file to be msync'd in [" + context + "]. Recorded msync order:\n"
            );
            for (int i = 0; i < order.size(); i++) {
                sb.append("  [").append(i).append("] ").append(order.get(i)).append('\n');
            }
            sb.append("firstPartIdx=").append(firstPartIdx)
                    .append(" firstHeaderIdx=").append(firstHeaderIdx);
            Assert.fail(sb.toString());
        }

        Assert.assertTrue(
                "[" + context + "] part file (_txn_parts/) must be msync'd before header (_txnlog) "
                        + "(firstPartIdx=" + firstPartIdx + " firstHeaderIdx=" + firstHeaderIdx + ")",
                firstPartIdx < firstHeaderIdx
        );
    }

    private CairoConfiguration syncConfig(SyncOrderFilesFacade syncFf) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCommitMode() {
                return CommitMode.SYNC;
            }

            @Override
            public FilesFacade getFilesFacade() {
                return syncFf;
            }
        };
    }

    /**
     * A FilesFacade that records the order of msync calls, keyed by the file path that was
     * mmap'd to produce the address being sync'd. This lets us verify part-file-before-header
     * ordering without relying on fsync (which is conditional on file size growth).
     */
    static class SyncOrderFilesFacade extends FilesFacadeImpl {
        // addr → path (resolved at mmap time via fd → path).
        private final Map<Long, String> addrToPath = new HashMap<>();
        // fd → path (populated at open time).
        private final Map<Long, String> fdToPath = new HashMap<>();
        // ordered list of file paths as they were msync'd.
        private final List<String> syncOrder = new ArrayList<>();

        public List<String> getSyncOrder() {
            return syncOrder;
        }

        /** Drop all tracked state (fd, addr, and sync order). For full reset between test phases. */
        public void reset() {
            addrToPath.clear();
            fdToPath.clear();
            syncOrder.clear();
        }

        /**
         * Clear only the sync-order log; keep fd→addr mappings intact so that subsequent msync
         * calls (e.g. after addEntry) can still resolve addresses to file paths. Use this when the
         * files are already open and you only want to discard syncs that happened during create/open.
         */
        public void resetSyncOrder() {
            syncOrder.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (addr != MAP_FAILED) {
                String p = fdToPath.get(fd);
                if (p != null) {
                    addrToPath.put(addr, p);
                }
            }
            return addr;
        }

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int flags, int memoryTag) {
            long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, flags, memoryTag);
            if (newAddr != MAP_FAILED) {
                // Update the addr mapping (old addr may have moved).
                String p = addrToPath.remove(addr);
                if (p != null) {
                    addrToPath.put(newAddr, p);
                }
            }
            return newAddr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            String p = addrToPath.get(addr);
            if (p != null) {
                syncOrder.add(p);
            }
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            addrToPath.remove(address);
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

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }
}
