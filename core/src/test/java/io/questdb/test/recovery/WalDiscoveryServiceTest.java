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

package io.questdb.test.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.recovery.BoundedSeqTxnLogReader;
import io.questdb.recovery.SeqTxnLogState;
import io.questdb.recovery.WalDirEntry;
import io.questdb.recovery.WalDiscoveryService;
import io.questdb.recovery.WalScanState;
import io.questdb.recovery.WalScanStatus;
import io.questdb.recovery.WalSegmentEntry;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class WalDiscoveryServiceTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final long V1_RECORD_SIZE = TableTransactionLogV1.RECORD_SIZE;


    @Test
    public void testDdlRecordsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("ddl_ignore");
            createWalDir(tableDir, 1);

            // txnlog with walId=1 plus a DDL record (walId=-1)
            SeqTxnLogState seqState = buildSeqState(new int[]{1, WalUtils.METADATA_WALID});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(0).getStatus());
        });
    }

    @Test
    public void testDropRecordsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("drop_ignore");
            createWalDir(tableDir, 1);

            // txnlog with walId=1 plus a DROP record (walId=-2)
            SeqTxnLogState seqState = buildSeqState(new int[]{1, WalUtils.DROP_TABLE_WAL_ID});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(0).getStatus());
        });
    }

    @Test
    public void testEmptySeqState() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("empty_seq");
            createWalDir(tableDir, 1);
            createWalDir(tableDir, 2);

            // empty seqState (no records)
            SeqTxnLogState seqState = buildSeqState(new int[]{});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(2, entries.size());
            for (int i = 0; i < entries.size(); i++) {
                Assert.assertEquals(WalScanStatus.ORPHAN, entries.getQuick(i).getStatus());
            }
        });
    }

    @Test
    public void testEmptyTableDir() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("empty_table");

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            Assert.assertEquals(0, result.getEntries().size());
        });
    }

    @Test
    public void testEntriesSortedByWalId() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("sorted");
            // create in reverse order
            createWalDir(tableDir, 5);
            createWalDir(tableDir, 3);
            createWalDir(tableDir, 1);

            SeqTxnLogState seqState = buildSeqState(new int[]{5, 3, 1});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(3, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(3, entries.getQuick(1).getWalId());
            Assert.assertEquals(5, entries.getQuick(2).getWalId());
        });
    }

    @Test
    public void testMissingWalDir() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("missing_wal");
            // no wal2 dir on disk, but txnlog references walId=2

            SeqTxnLogState seqState = buildSeqState(new int[]{2});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(2, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.MISSING, entries.getQuick(0).getStatus());
            Assert.assertEquals(0, entries.getQuick(0).getSegments().size());
        });
    }

    @Test
    public void testMissingWalHasRefCount() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("missing_refcount");
            // walId=3 referenced 4 times but not on disk

            SeqTxnLogState seqState = buildSeqState(new int[]{3, 3, 3, 3});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(3, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.MISSING, entries.getQuick(0).getStatus());
            Assert.assertEquals(4, entries.getQuick(0).getTxnlogReferenceCount());
        });
    }

    @Test
    public void testMixedStatuses() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("mixed");
            createWalDir(tableDir, 1); // referenced
            createWalDir(tableDir, 3); // orphan (not in txnlog)
            // walId=5 is missing (in txnlog, not on disk)

            SeqTxnLogState seqState = buildSeqState(new int[]{1, 5});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(3, entries.size());

            // sorted by walId: 1, 3, 5
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(0).getStatus());

            Assert.assertEquals(3, entries.getQuick(1).getWalId());
            Assert.assertEquals(WalScanStatus.ORPHAN, entries.getQuick(1).getStatus());

            Assert.assertEquals(5, entries.getQuick(2).getWalId());
            Assert.assertEquals(WalScanStatus.MISSING, entries.getQuick(2).getStatus());
        });
    }

    @Test
    public void testMultipleRefsToSameWal() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("multi_refs");
            createWalDir(tableDir, 1);

            // walId=1 appears 5 times
            SeqTxnLogState seqState = buildSeqState(new int[]{1, 1, 1, 1, 1});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(0).getStatus());
            Assert.assertEquals(5, entries.getQuick(0).getTxnlogReferenceCount());
        });
    }

    @Test
    public void testMultipleWalsReferenced() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("multi_wal");
            createWalDir(tableDir, 1);
            createWalDir(tableDir, 2);
            createWalDir(tableDir, 3);

            SeqTxnLogState seqState = buildSeqState(new int[]{1, 2, 3});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(3, entries.size());
            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(i + 1, entries.getQuick(i).getWalId());
                Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(i).getStatus());
            }
        });
    }

    @Test
    public void testNonNumericDirsIgnored() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("non_numeric");
            createWalDir(tableDir, 1);

            // create non-numeric dirs that should be ignored
            try (Path path = new Path()) {
                path.of(tableDir).concat("walX").$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                path.of(tableDir).concat("wal").$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                path.of(tableDir).concat("walABC").$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
            }

            SeqTxnLogState seqState = buildSeqState(new int[]{1});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
        });
    }

    @Test
    public void testNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("non_wal");
            // no wal dirs, null seq state

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            Assert.assertEquals(0, result.getEntries().size());
        });
    }

    @Test
    public void testNullSeqState() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("null_seq");
            createWalDir(tableDir, 1);
            createWalDir(tableDir, 2);

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(2, entries.size());
            for (int i = 0; i < entries.size(); i++) {
                Assert.assertEquals(WalScanStatus.ORPHAN, entries.getQuick(i).getStatus());
                Assert.assertEquals(0, entries.getQuick(i).getTxnlogReferenceCount());
            }
        });
    }

    @Test
    public void testOrphanRefCountIsZero() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("orphan_refcount");
            createWalDir(tableDir, 1); // orphan

            // txnlog references only walId=2, wal1 is orphan
            SeqTxnLogState seqState = buildSeqState(new int[]{2});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();

            WalDirEntry orphan = findEntryByWalId(entries, 1);
            Assert.assertNotNull(orphan);
            Assert.assertEquals(WalScanStatus.ORPHAN, orphan.getStatus());
            Assert.assertEquals(0, orphan.getTxnlogReferenceCount());
        });
    }

    @Test
    public void testOrphanWalDir() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("orphan");
            createWalDir(tableDir, 5);

            // empty txnlog - wal5 is not referenced
            SeqTxnLogState seqState = buildSeqState(new int[]{});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(5, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.ORPHAN, entries.getQuick(0).getStatus());
        });
    }

    @Test
    public void testSingleWalReferencedInTxnlog() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("single_ref");
            createWalDir(tableDir, 1);

            SeqTxnLogState seqState = buildSeqState(new int[]{1});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(1, entries.getQuick(0).getWalId());
            Assert.assertEquals(WalScanStatus.REFERENCED, entries.getQuick(0).getStatus());
        });
    }

    @Test
    public void testTxnlogReferenceCount() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("refcount");
            createWalDir(tableDir, 1);

            // walId=1 appears 3 times in the txnlog
            SeqTxnLogState seqState = buildSeqState(new int[]{1, 1, 1});

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, seqState);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(3, entries.getQuick(0).getTxnlogReferenceCount());
        });
    }


    @Test
    public void testSegmentCompletelyEmpty() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_empty");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            // no files in segment

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            ObjList<WalSegmentEntry> segments = entries.getQuick(0).getSegments();
            Assert.assertEquals(1, segments.size());

            WalSegmentEntry seg = segments.getQuick(0);
            Assert.assertEquals(0, seg.getSegmentId());
            Assert.assertFalse(seg.hasEventFile());
            Assert.assertFalse(seg.hasEventIndexFile());
            Assert.assertFalse(seg.hasMetaFile());
            Assert.assertFalse(seg.isComplete());
        });
    }

    @Test
    public void testSegmentDiscovery() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_disc");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            createSegmentDir(tableDir, 1, 1);
            createSegmentDir(tableDir, 1, 2);

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            ObjList<WalSegmentEntry> segments = entries.getQuick(0).getSegments();
            Assert.assertEquals(3, segments.size());
            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(i, segments.getQuick(i).getSegmentId());
            }
        });
    }

    @Test
    public void testSegmentFilePresence() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_complete");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_FILE_NAME);
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_INDEX_FILE_NAME);
            createSegmentFile(tableDir, 1, 0, "_meta");

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalDirEntry> entries = result.getEntries();
            WalSegmentEntry seg = entries.getQuick(0).getSegments().getQuick(0);
            Assert.assertTrue(seg.hasEventFile());
            Assert.assertTrue(seg.hasEventIndexFile());
            Assert.assertTrue(seg.hasMetaFile());
            Assert.assertTrue(seg.isComplete());
        });
    }

    @Test
    public void testSegmentMissingEvent() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_no_event");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            // missing _event
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_INDEX_FILE_NAME);
            createSegmentFile(tableDir, 1, 0, "_meta");

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            WalSegmentEntry seg = result.getEntries().getQuick(0).getSegments().getQuick(0);
            Assert.assertFalse(seg.hasEventFile());
            Assert.assertTrue(seg.hasEventIndexFile());
            Assert.assertTrue(seg.hasMetaFile());
            Assert.assertFalse(seg.isComplete());
        });
    }

    @Test
    public void testSegmentMissingEventIndex() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_no_idx");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_FILE_NAME);
            // missing _event.i
            createSegmentFile(tableDir, 1, 0, "_meta");

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            WalSegmentEntry seg = result.getEntries().getQuick(0).getSegments().getQuick(0);
            Assert.assertTrue(seg.hasEventFile());
            Assert.assertFalse(seg.hasEventIndexFile());
            Assert.assertTrue(seg.hasMetaFile());
            Assert.assertFalse(seg.isComplete());
        });
    }

    @Test
    public void testSegmentMissingMeta() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("seg_no_meta");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_FILE_NAME);
            createSegmentFile(tableDir, 1, 0, WalUtils.EVENT_INDEX_FILE_NAME);
            // missing _meta

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            WalSegmentEntry seg = result.getEntries().getQuick(0).getSegments().getQuick(0);
            Assert.assertTrue(seg.hasEventFile());
            Assert.assertTrue(seg.hasEventIndexFile());
            Assert.assertFalse(seg.hasMetaFile());
            Assert.assertFalse(seg.isComplete());
        });
    }

    @Test
    public void testWalDirWithMixedSegmentDirs() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("mixed_seg");
            createWalDir(tableDir, 1);
            createSegmentDir(tableDir, 1, 0);
            createSegmentDir(tableDir, 1, 1);

            // create non-numeric dirs inside wal1
            try (Path path = new Path()) {
                path.of(tableDir).concat("wal1").concat("backup").$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));

                path.of(tableDir).concat("wal1").concat("temp").$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
            }

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalSegmentEntry> segments = result.getEntries().getQuick(0).getSegments();
            Assert.assertEquals(2, segments.size());
            Assert.assertEquals(0, segments.getQuick(0).getSegmentId());
            Assert.assertEquals(1, segments.getQuick(1).getSegmentId());
        });
    }

    @Test
    public void testWalDirWithNoSegments() throws Exception {
        assertMemoryLeak(() -> {
            String tableDir = createTableDir("no_seg");
            createWalDir(tableDir, 1);

            WalScanState result = new WalDiscoveryService(FF).scan(tableDir, null);
            ObjList<WalDirEntry> entries = result.getEntries();
            Assert.assertEquals(1, entries.size());
            Assert.assertEquals(0, entries.getQuick(0).getSegments().size());
        });
    }


    /**
     * Builds a SeqTxnLogState by writing a V1 binary _txnlog file and reading it
     * with BoundedSeqTxnLogReader, since SeqTxnLogState setters and SeqTxnRecord
     * constructor are package-private.
     */
    private static SeqTxnLogState buildSeqState(int[] walIds) {
        if (walIds.length == 0) {
            // write a header-only txnlog with maxTxn=0
            try (Path path = new Path()) {
                String dir = root + "/seq_state_empty_" + System.nanoTime();
                path.of(dir).$();
                Assert.assertEquals(0, FF.mkdir(path.$(), 509));
                path.of(dir).concat("_txnlog").$();

                long headerSize = TableTransactionLogFile.HEADER_SIZE;
                long buf = Unsafe.malloc(headerSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(buf, headerSize, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                    Unsafe.getUnsafe().putLong(buf + 4, 0L); // maxTxn = 0
                    writeBuf(path.$(), buf, headerSize);
                } finally {
                    Unsafe.free(buf, headerSize, MemoryTag.NATIVE_DEFAULT);
                }

                return new BoundedSeqTxnLogReader(FF).read(path.$());
            }
        }

        try (Path path = new Path()) {
            String dir = root + "/seq_state_" + System.nanoTime();
            path.of(dir).$();
            Assert.assertEquals(0, FF.mkdir(path.$(), 509));
            path.of(dir).concat("_txnlog").$();

            long headerSize = TableTransactionLogFile.HEADER_SIZE;
            long fileSize = headerSize + (long) walIds.length * V1_RECORD_SIZE;
            long buf = Unsafe.malloc(fileSize, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, fileSize, (byte) 0);
                Unsafe.getUnsafe().putInt(buf, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
                Unsafe.getUnsafe().putLong(buf + 4, walIds.length); // maxTxn

                for (int r = 0; r < walIds.length; r++) {
                    long recOff = buf + headerSize + r * V1_RECORD_SIZE;
                    Unsafe.getUnsafe().putLong(recOff, 0L);             // structureVersion
                    Unsafe.getUnsafe().putInt(recOff + 8, walIds[r]);   // walId
                    Unsafe.getUnsafe().putInt(recOff + 12, 0);          // segmentId
                    Unsafe.getUnsafe().putInt(recOff + 16, 0);          // segmentTxn
                    Unsafe.getUnsafe().putLong(recOff + 20, 1000L * (r + 1)); // commitTimestamp
                }

                writeBuf(path.$(), buf, fileSize);
            } finally {
                Unsafe.free(buf, fileSize, MemoryTag.NATIVE_DEFAULT);
            }

            return new BoundedSeqTxnLogReader(FF).read(path.$());
        }
    }

    private static void createSegmentDir(String tableDir, int walId, int segmentId) {
        try (Path path = new Path()) {
            path.of(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).slash$();
            Assert.assertEquals(0, FF.mkdirs(path, 509));
        }
    }

    private static void createSegmentFile(String tableDir, int walId, int segmentId, String fileName) {
        try (Path path = new Path()) {
            path.of(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId).slash().concat(fileName).$();
            Assert.assertTrue("failed to create file: " + path, FF.touch(path.$()));
        }
    }

    private static String createTableDir(String name) {
        String tableDir = root + "/" + name + "~123";
        try (Path path = new Path()) {
            path.of(tableDir).$();
            Assert.assertEquals(0, FF.mkdir(path.$(), 509));
        }
        return tableDir;
    }

    private static void createWalDir(String tableDir, int walId) {
        try (Path path = new Path()) {
            path.of(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).$();
            Assert.assertEquals(0, FF.mkdir(path.$(), 509));
        }
    }

    private static WalDirEntry findEntryByWalId(ObjList<WalDirEntry> entries, int walId) {
        for (int i = 0, n = entries.size(); i < n; i++) {
            if (entries.getQuick(i).getWalId() == walId) {
                return entries.getQuick(i);
            }
        }
        return null;
    }

    private static void writeBuf(LPSZ filePath, long buf, long size) {
        long fd = FF.openRW(filePath, CairoConfiguration.O_NONE);
        Assert.assertTrue("failed to open file for writing", fd > -1);
        try {
            Assert.assertEquals(size, FF.write(fd, buf, size, 0));
            Assert.assertTrue(FF.truncate(fd, size));
        } finally {
            FF.close(fd);
        }
    }
}
