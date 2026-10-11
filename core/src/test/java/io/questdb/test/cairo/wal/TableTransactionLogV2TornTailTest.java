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
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import static io.questdb.cairo.wal.seq.TableTransactionLogFile.HEADER_SIZE;
import static io.questdb.cairo.wal.seq.TableTransactionLogFile.MAX_TXN_OFFSET_64;
import static io.questdb.cairo.wal.seq.TableTransactionLogV2.RECORD_SIZE;
import static io.questdb.cairo.wal.seq.TableTransactionLogV2.RESERVED_OFFSET;

/**
 * The ADAPTIVE open of a V2 sequencer log. It must cut every record a power loss can leave torn among the records no
 * completed flush covered, wherever the tear falls, and never read a torn record's structure version. It must keep
 * every record a completed flush covered, and every record written without a checksum by an older binary or by a
 * replica's downloader.
 */
public class TableTransactionLogV2TornTailTest extends AbstractCairoTest {
    private static final int PART = 200;
    private static final int RS = (int) RECORD_SIZE;
    // Slots 0-127 give a 60-byte record every offset from a 512-byte sector boundary it can have.
    private static final int SECTOR_POSITIONS = 128;
    private static final long UNSYNCED_FROM_OFFSET = 40;

    @Test
    public void testChecksumlessRecordsOfAnOldLogAreKept() throws Exception {
        // A log an older binary wrote: no field, records without a checksum after one with a checksum.
        assertMemoryLeak(() -> {
            final String dir = "old";
            writeLog(dir, 120, CommitMode.NOSYNC, 0);
            for (int slot = 1; slot < 120; slot++) {
                patch(partFile(dir, 0), (long) slot * RS + RESERVED_OFFSET, new byte[Long.BYTES]);
            }
            Assert.assertEquals(120, reopen(dir, adaptive(10_000))[0]);
            Assert.assertEquals(120, reopen(dir, adaptive(10_000))[0]);
        });
    }

    @Test
    public void testEveryTearOfTheLastUnflushedRecordIsCut() throws Exception {
        // A writeback can catch a record half-stored (any prefix of it on disk, the rest zeros), and a power loss can
        // keep one side of a sector boundary the record crosses and lose the other.
        assertMemoryLeak(() -> {
            int cases = 0;
            int kept = 0;
            final StringBuilder firstKept = new StringBuilder();
            for (int slot = 0; slot < SECTOR_POSITIONS; slot++) {
                final String dir = "tear" + slot;
                writeLog(dir, slot + 1, CommitMode.ADAPTIVE, 10_000);
                final String part = partFile(dir, 0);
                final byte[] record = read(part, (long) slot * RS, RS);
                final byte[] header = read(headerFile(dir), 0, (int) HEADER_SIZE);
                final int split = (int) (512 - ((long) slot * RS) % 512);
                for (int shape = 0; shape <= RS / 4; shape++) {
                    final int lo;
                    final int hi;
                    if (shape < RS / 4) {
                        // half-stored: the first 4 * shape bytes reached the disk
                        lo = 4 * shape;
                        hi = RS;
                    } else if (split < RS) {
                        // the sector before the boundary was lost, the one after it reached the disk
                        lo = 0;
                        hi = split;
                    } else {
                        continue;
                    }
                    if (isZero(record, lo, hi)) {
                        continue;
                    }
                    cases++;
                    patch(part, (long) slot * RS + lo, new byte[hi - lo]);
                    final long[] r = reopen(dir, adaptive(10_000));
                    if (r[0] != slot || r[1] != slot) {
                        if (kept++ == 0) {
                            firstKept.append("slot=").append(slot).append(" zeroed=[").append(lo).append(',').append(hi)
                                    .append(") lastTxn=").append(r[0]).append(" structureVersion=").append(r[1]);
                        }
                    }
                    patch(part, (long) slot * RS, record);
                    patch(headerFile(dir), 0, header);
                }
            }
            Assert.assertEquals("torn records kept: " + kept + " of " + cases + ", first: " + firstKept, 0, kept);
        });
    }

    @Test
    public void testFieldMovesOnlyAfterAFlushInAdaptive() throws Exception {
        assertMemoryLeak(() -> {
            // With a window the first append starts the field; the batch flush moves it past what it covered.
            try (Path path = new Path()) {
                path.of(root).concat("moves");
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                final TableTransactionLogV2 v2 = new TableTransactionLogV2(adaptive(10_000), PART, DefaultWalDirectoryPolicy.INSTANCE);
                try {
                    v2.create(path, 1L);
                    v2.open(path);
                    Assert.assertEquals(0, readLong(headerFile("moves"), UNSYNCED_FROM_OFFSET));
                    for (int txn = 1; txn <= 5; txn++) {
                        addEntry(v2, txn);
                        Assert.assertEquals(1, readLong(headerFile("moves"), UNSYNCED_FROM_OFFSET));
                    }
                    v2.fdatasyncTxnLog();
                    Assert.assertEquals(6, readLong(headerFile("moves"), UNSYNCED_FROM_OFFSET));
                } finally {
                    v2.close();
                }
            }
            // Without a window every append flushes, so the field follows the tail.
            writeLog("w0", 3, CommitMode.ADAPTIVE, 0);
            Assert.assertEquals(4, readLong(headerFile("w0"), UNSYNCED_FROM_OFFSET));
        });
    }

    @Test
    public void testFlushedRecordIsNeverCut() throws Exception {
        // A record a completed flush covered and that is damaged since is not a tear: the cursor reports it.
        assertMemoryLeak(() -> {
            final String dir = "flushed";
            try (Path path = new Path()) {
                path.of(root).concat(dir);
                configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
                final TableTransactionLogV2 v2 = new TableTransactionLogV2(adaptive(10_000), PART, DefaultWalDirectoryPolicy.INSTANCE);
                try {
                    v2.create(path, 1L);
                    v2.open(path);
                    for (int txn = 1; txn <= 10; txn++) {
                        addEntry(v2, txn);
                    }
                    v2.fdatasyncTxnLog();
                    for (int txn = 11; txn <= 13; txn++) {
                        addEntry(v2, txn);
                    }
                } finally {
                    v2.close();
                }
            }
            // txn 10 was flushed and is damaged since; txn 12 was not flushed and is torn
            patch(partFile(dir, 0), 9L * RS, new byte[RS]);
            patch(partFile(dir, 0), 11L * RS + 12, new byte[RS - 12]);
            final long[] r = reopen(dir, adaptive(10_000));
            Assert.assertEquals(11, r[0]);
            Assert.assertEquals(11, r[1]);
        });
    }

    @Test
    public void testModesOtherThanAdaptiveWriteNoField() throws Exception {
        // NOSYNC, ASYNC and SYNC keep base behaviour: the header's reserved bytes stay as a base binary leaves them.
        assertMemoryLeak(() -> {
            for (int mode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC, CommitMode.SYNC}) {
                final String dir = "mode" + mode;
                writeLog(dir, 10, mode, 0);
                reopen(dir, withMode(mode, 0));
                final byte[] header = read(headerFile(dir), 0, (int) HEADER_SIZE);
                Assert.assertTrue("mode " + mode + ": " + Arrays.toString(header), isZero(header, 24, (int) HEADER_SIZE));
            }
        });
    }

    @Test
    public void testOnlyTheLastPartIsScanned() throws Exception {
        // Within an ADAPTIVE run a part is flushed before the next one opens; an earlier part is never scanned.
        assertMemoryLeak(() -> {
            final String dir = "parts";
            writeLog(dir, PART + 10, CommitMode.ADAPTIVE, 10_000);
            final String part0 = partFile(dir, 0);
            patch(part0, 150L * RS, new byte[RS]);
            Assert.assertEquals(PART + 10, reopen(dir, adaptive(10_000))[0]);
            patch(partFile(dir, 1), 4L * RS + 12, new byte[RS - 12]);
            final long[] r = reopen(dir, adaptive(10_000));
            Assert.assertEquals(PART + 4, r[0]);
            Assert.assertEquals(PART + 4, r[1]);
        });
    }

    @Test
    public void testRebaseSeedOnAReplicaThenPromotionKeepsDownloadedRecords() throws Exception {
        // REBASE WAL INTO on a replica seeds txns 1-2 through the replica's own sequencer, with checksums; the
        // downloader then appends an older primary's records without one. The node promoted from that replica must
        // keep them all: the replica never starts the field, so the promoted node trusts every record, and its own
        // first append starts the field above them.
        assertMemoryLeak(() -> {
            final String dir = "rebased";
            writeLog(dir, 2, CommitMode.ADAPTIVE, 0, LocalDurabilityPolicy.REPLICA_SKIP);
            Assert.assertEquals("the replica's seed must not start the field", 0, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
            for (int slot = 0; slot < 2; slot++) {
                Assert.assertNotEquals("a seed record carries a checksum", 0, readLong(partFile(dir, 0), (long) slot * RS + RESERVED_OFFSET));
            }
            appendAsDownloader(dir, 100);
            Assert.assertEquals(102, reopen(dir, adaptive(10_000))[0]);
            Assert.assertEquals(102, reopen(dir, adaptive(0))[0]);
            appendLog(dir, 3, adaptive(0));
            Assert.assertEquals(106, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
            Assert.assertEquals(105, reopen(dir, adaptive(10_000))[0]);
        });
    }

    @Test
    public void testRecordsPastTheTailAreZeroed() throws Exception {
        // An append that crashed before the header counted its record, or a checkpoint restore that lowered the max
        // txn, leaves whole records past the tail. A record written over one and torn could show it whole.
        assertMemoryLeak(() -> {
            final String dir = "past";
            writeLog(dir, 10, CommitMode.ADAPTIVE, 0);
            Assert.assertEquals(11, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
            final byte[] maxTxn = new byte[Long.BYTES];
            maxTxn[0] = 9;
            patch(headerFile(dir), MAX_TXN_OFFSET_64, maxTxn);
            Assert.assertEquals(9, reopen(dir, adaptive(10_000))[0]);
            Assert.assertTrue(isZero(read(partFile(dir, 0), 9L * RS, RS), 0, RS));
            Assert.assertEquals(10, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
            maxTxn[0] = 4;
            patch(headerFile(dir), MAX_TXN_OFFSET_64, maxTxn);
            Assert.assertEquals(4, reopen(dir, adaptive(10_000))[0]);
            Assert.assertTrue(isZero(read(partFile(dir, 0), 4L * RS, (PART - 4) * RS), 0, (PART - 4) * RS));
            Assert.assertEquals(5, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
        });
    }

    @Test
    public void testReplicaKeepsDownloadedRecords() throws Exception {
        // A primary's log gets records from the downloader once the node is a replica; an older primary sends them
        // without a checksum. The replica, and the node promoted from it, must keep them.
        assertMemoryLeak(() -> {
            final String dir = "replica";
            writeLog(dir, 5, CommitMode.ADAPTIVE, 0);
            appendAsDownloader(dir, 100);
            Assert.assertEquals(105, reopen(dir, adaptive(10_000), LocalDurabilityPolicy.REPLICA_SKIP)[0]);
            appendAsDownloader(dir, 10);
            Assert.assertEquals(115, reopen(dir, adaptive(10_000), LocalDurabilityPolicy.ALWAYS_ON)[0]);
            // The replica's first open dropped the field the primary had kept, so the promoted node trusts the
            // downloaded records; its own first append starts the field above them.
            Assert.assertEquals(0, readLong(headerFile(dir), UNSYNCED_FROM_OFFSET));
        });
    }

    private static void addEntry(TableTransactionLogV2 v2, long txn) {
        v2.addEntry(txn, 1, 0, (int) (txn - 1), 1000L + txn, 1000L, 2000L, 1L);
    }

    private static boolean isZero(byte[] bytes, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return true;
    }

    private static void patch(String file, long offset, byte[] bytes) throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            f.seek(offset);
            f.write(bytes);
        }
    }

    private static byte[] read(String file, long offset, int len) throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            final byte[] bytes = new byte[len];
            f.seek(offset);
            f.readFully(bytes);
            return bytes;
        }
    }

    private static long readLong(String file, long offset) throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            f.seek(offset);
            return Long.reverseBytes(f.readLong());
        }
    }

    private CairoConfiguration adaptive(long windowUs) {
        return withMode(CommitMode.ADAPTIVE, windowUs);
    }

    // Records as a replica's downloader writes them from an older primary: no checksum, the header's max txn only.
    private void appendAsDownloader(String dir, int count) throws IOException {
        final long lastTxn = readLong(headerFile(dir), MAX_TXN_OFFSET_64);
        final byte[] header = read(headerFile(dir), 0, (int) HEADER_SIZE);
        try (Path path = new Path()) {
            path.of(root).concat(dir);
            final TableTransactionLogV2 v2 = new TableTransactionLogV2(withMode(CommitMode.NOSYNC, 0), PART, DefaultWalDirectoryPolicy.INSTANCE);
            try {
                v2.open(path);
                for (long txn = lastTxn + 1; txn <= lastTxn + count; txn++) {
                    addEntry(v2, txn);
                }
            } finally {
                v2.close();
            }
        }
        for (long txn = lastTxn + 1; txn <= lastTxn + count; txn++) {
            patch(partFile(dir, (txn - 1) / PART), ((txn - 1) % PART) * RS + RESERVED_OFFSET, new byte[Long.BYTES]);
        }
        // The downloader writes the max txn and nothing else of the header.
        final byte[] maxTxn = read(headerFile(dir), MAX_TXN_OFFSET_64, Long.BYTES);
        System.arraycopy(maxTxn, 0, header, (int) MAX_TXN_OFFSET_64, Long.BYTES);
        patch(headerFile(dir), 0, header);
    }

    // Appends count records as a node that keeps the log durable itself.
    private void appendLog(String dir, int count, CairoConfiguration config) {
        try (Path path = new Path()) {
            path.of(root).concat(dir);
            final TableTransactionLogV2 v2 = new TableTransactionLogV2(config, PART, DefaultWalDirectoryPolicy.INSTANCE);
            try {
                v2.open(path);
                final long lastTxn = v2.lastTxn();
                for (long txn = lastTxn + 1; txn <= lastTxn + count; txn++) {
                    addEntry(v2, txn);
                }
            } finally {
                v2.close();
            }
        }
    }

    private String headerFile(String dir) {
        return root + "/" + dir + "/" + WalUtils.TXNLOG_FILE_NAME;
    }

    private String partFile(String dir, long part) {
        return root + "/" + dir + "/" + WalUtils.TXNLOG_PARTS_DIR + "/" + part;
    }

    private long[] reopen(String dir, CairoConfiguration config) {
        return reopen(dir, config, LocalDurabilityPolicy.ALWAYS_ON);
    }

    private long[] reopen(String dir, CairoConfiguration config, LocalDurabilityPolicy policy) {
        try (Path path = new Path()) {
            path.of(root).concat(dir);
            final TableTransactionLogV2 v2 = new TableTransactionLogV2(config, PART, DefaultWalDirectoryPolicy.INSTANCE, policy);
            try {
                final long structureVersion = v2.open(path);
                return new long[]{v2.lastTxn(), structureVersion};
            } finally {
                v2.close();
            }
        }
    }

    private CairoConfiguration withMode(int mode, long windowUs) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return windowUs;
            }

            @Override
            public int getCommitMode() {
                return mode;
            }
        };
    }

    private void writeLog(String dir, int count, int mode, long windowUs) {
        writeLog(dir, count, mode, windowUs, LocalDurabilityPolicy.ALWAYS_ON);
    }

    private void writeLog(String dir, int count, int mode, long windowUs, LocalDurabilityPolicy policy) {
        try (Path path = new Path()) {
            path.of(root).concat(dir);
            configuration.getFilesFacade().mkdir(path.$(), configuration.getMkDirMode());
            final TableTransactionLogV2 v2 = new TableTransactionLogV2(withMode(mode, windowUs), PART, DefaultWalDirectoryPolicy.INSTANCE, policy);
            try {
                v2.create(path, 1L);
                v2.open(path);
                for (int txn = 1; txn <= count; txn++) {
                    addEntry(v2, txn);
                }
            } finally {
                v2.close();
            }
        }
    }
}
