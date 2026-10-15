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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;

/**
 * This class is used to read/write transactions to the disk.
 * This is V1 implementation of the sequencer transaction log storage, and it will be used
 * in parallel with the new V2 for backward compatibility.
 * <p>
 * All transactions are stored in the single file table_dir\\txn_seq\\_txnlog, the file structure is
 * <p>
 * Header: 76 bytes
 * Transaction record: 28 bytes
 * <p>
 * See the format of the header and transaction record in {@link TableTransactionLogFile}
 * <p>
 * A power loss can leave the tail of the log torn: the header can reach the disk ahead of the records it
 * publishes, and a record can be split by a sector boundary. Such records were never acknowledged, so
 * {@link #open(Path)} drops them, except those the table has already applied: its {@code _txn} can reach the disk
 * when the log page does not, and a dropped txn would be handed out again. It relies on two invariants: the bytes
 * past the published tail are zero on disk before a record is written over them, and
 * {@link #UNSYNCED_FROM_TXN_OFFSET_64} names the first record that a completed flush may not have covered.
 */
public class TableTransactionLogV1 implements TableTransactionLogFile {
    /**
     * Header slot, in the reserved area, holding the first txn whose
     * record may not be on disk: every record below it was covered by a completed flush. Negative after an
     * open that scanned records without flushing them (NOSYNC, ASYNC): a later open in such a mode trusts
     * the records below the absolute value, an open in a durable mode scans the whole log. Zero in a file
     * written by a binary without the hint, whose records are then trusted. It shares the header's first
     * sector with {@code MAX_TXN_OFFSET_64}, so a power loss cannot persist one without the other.
     */
    public static final long UNSYNCED_FROM_TXN_OFFSET_64 = HEADER_SEQ_PART_SIZE_32 + Integer.BYTES + 2 * Long.BYTES;
    private static final Log LOG = LogFactory.getLog(TableTransactionLogV1.class);
    // The most records isTailIntact reads at once.
    private static final long TAIL_CHECK_READ_RECORDS = 4096;
    private static final CarrierLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new CarrierLocal<>();
    public static long RECORD_SIZE = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;
    private final Supplier<long[]> committedTableTxn;
    private final CairoConfiguration configuration;
    private final LocalDurabilityPolicy durabilityPolicy;
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    // Set by open(): the first append checks the bytes past the tail. On a replica they are the downloader's
    // records, written ahead of the tail it publishes, and only a Java append may wipe them.
    private boolean isPastTailUnchecked;

    /**
     * @param committedTableTxn one read of the table's {@code _txn}, asked only when an open finds a torn record: its
     *                          committed txn, which a torn-tail cut keeps, and its structure version, which an open
     *                          that keeps a torn last record takes when it is above the last whole record's. Null
     *                          when it is not known: nothing is cut then
     */
    public TableTransactionLogV1(
            CairoConfiguration configuration,
            LocalDurabilityPolicy durabilityPolicy,
            Supplier<long[]> committedTableTxn
    ) {
        this.configuration = configuration;
        this.durabilityPolicy = durabilityPolicy;
        this.ff = configuration.getFilesFacade();
        this.committedTableTxn = committedTableTxn;
    }

    /**
     * Whether an open now would keep every record the header counts, for a reader that does not open the log. Reads
     * the records {@link #open(Path)} checks for a tear, the same bytes, with the record before them, and applies the
     * open's rule to them. Reads nothing when the open checks none.
     *
     * @param fd            the log, open for reading
     * @param maxTxn        the header's max txn
     * @param unsyncedFrom  the header's {@link #UNSYNCED_FROM_TXN_OFFSET_64}
     * @param isDurableMode whether the open would run in ADAPTIVE or SYNC
     * @return false when the open would cut a record, or when a record cannot be read
     */
    public static boolean isTailIntact(FilesFacade ff, long fd, long maxTxn, long unsyncedFrom, boolean isDurableMode) {
        long txn = scanFrom(unsyncedFrom, maxTxn, isDurableMode);
        if (txn > maxTxn) {
            return true;
        }
        // The first record read precedes the first one checked: the rule compares a record with the one before it.
        long lo = Math.max(1, txn - 1);
        final long bufSize = Math.min(maxTxn - lo + 1, TAIL_CHECK_READ_RECORDS) * RECORD_SIZE;
        final long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            while (true) {
                final long hi = Math.min(maxTxn, lo + bufSize / RECORD_SIZE - 1);
                final long offset = HEADER_SIZE + (lo - 1) * RECORD_SIZE;
                final long bytesRead = ff.read(fd, buf, (hi - lo + 1) * RECORD_SIZE, offset);
                // The open counts a record the file does not hold whole as torn.
                final long readHi = lo - 1 + Math.max(bytesRead, 0) / RECORD_SIZE;
                for (; txn <= hi; txn++) {
                    final long record = buf + (txn - lo) * RECORD_SIZE;
                    if (txn > readHi || isTornRecord(txn, record, record - RECORD_SIZE)) {
                        return false;
                    }
                }
                if (hi == maxTxn) {
                    return true;
                }
                lo = hi;
            }
        } finally {
            Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public static long readMaxStructureVersion(long logFileFd, FilesFacade ff) {
        long maxTxn = ff.readNonNegativeLong(logFileFd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
        if (maxTxn < 0) {
            return -1;
        }
        long offset = TableTransactionLogFile.HEADER_SIZE + (maxTxn - 1) * RECORD_SIZE;
        return ff.readNonNegativeLong(logFileFd, offset);
    }

    @Override
    public long addEntry(
            long structureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long timestamp,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        putRecord(structureVersion, walId, segmentId, segmentTxn, timestamp);
        Unsafe.storeFence();
        long maxTxn = this.maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, maxTxn);
        sync0();
        // Transactions are 1 based here
        return maxTxn;
    }

    @Override
    public void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        putRecord(newStructureVersion, STRUCTURAL_CHANGE_WAL_ID, -1, -1, timestamp);
    }

    @Override
    public void close() {
        if (txnMem.isOpen()) {
            long maxTxnInFile = txnMem.getLong(MAX_TXN_OFFSET_64);
            if (maxTxnInFile != maxTxn.get()) {
                LOG.info().$("Max txn in the file ").$(maxTxnInFile).$(" but in memory is ").$(maxTxn.get()).$();
            }
        }
        txnMem.close(false);
    }

    @Override
    public void create(Path path, long tableCreateTimestamp) {
        final int pathLength = path.size();
        openSmallFile(ff, path, pathLength, txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        // A previous lineage's records may still be in the file and on disk. The new lineage writes over
        // them, so zero them and flush in every mode: a torn record must tear over zeros.
        final boolean isReplacingLineage = (txnMem.size() >= MAX_TXN_OFFSET_64 + Long.BYTES
                && txnMem.getLong(MAX_TXN_OFFSET_64) != 0)
                || hasNonZeroBytes(HEADER_SIZE, txnMem.size());

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V1);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.putInt(0);
        txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, 1L);
        if (isReplacingLineage) {
            zeroFrom(HEADER_SIZE);
            txnMem.sync(false);
            ff.fdatasync(txnMem.getFd());
        } else {
            sync0();
        }
        txnMem.jumpTo(HEADER_SIZE);
    }

    @Override
    public long endMetadataChangeEntry() {
        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, nextTxn);
        return nextTxn;
    }

    @Override
    public void fdatasyncTxnLog() {
        if (txnMem.isOpen()) {
            // The caller holds the sequencer write lock, so no record is half-written past this tail.
            final long flushedTxn = maxTxn.get();
            // flushFile(): the records are written through the mapping and, under a group window, never msynced.
            txnMem.flushFile(false);
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, flushedTxn + 1);
        }
    }

    @Override
    public void fullSync() {
        txnMem.sync(false);
        markFlushed();
    }

    @Override
    public TransactionLogCursor getCursor(long txnLo, @Transient Path path) {
        return getCursor(txnLo, path, null);
    }

    @Override
    public TransactionLogCursor getCursor(
            long txnLo,
            @Transient Path path,
            @Nullable TableSequencerCursorHolder cursorHolder
    ) {
        TransactionLogCursorImpl cursor = cursorHolder != null
                ? (TransactionLogCursorImpl) cursorHolder.getTransactionLogCursor(WAL_SEQUENCER_FORMAT_VERSION_V1)
                : tlTransactionLogCursor.get();
        if (cursor == null) {
            cursor = new TransactionLogCursorImpl(configuration, txnLo, path);
            if (cursorHolder != null) {
                cursorHolder.registerTransactionLogCursor(WAL_SEQUENCER_FORMAT_VERSION_V1, cursor);
            } else {
                tlTransactionLogCursor.set(cursor);
            }
            return cursor;
        }
        try {
            return cursor.of(ff, configuration.getBypassWalFdCache(), txnLo, path);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            return WalUtils.DROP_TABLE_WAL_ID == txnMem.getInt(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
        }
        return false;
    }

    @Override
    public long lastTxn() {
        return maxTxn.get();
    }

    @Override
    public long open(Path path) {
        if (!txnMem.isOpen()) {
            txnMem.close(false);
            openSmallFile(ff, path, path.size(), txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        }

        final long publishedTxn = txnMem.getLong(MAX_TXN_OFFSET_64);
        final long unsyncedFrom = txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64);
        final boolean isDurableMode = isDurableMode();
        final long scanFrom = scanFrom(unsyncedFrom, publishedTxn, isDurableMode);
        final long wholeTxn = findWholeTail(scanFrom, publishedTxn);
        // One read of the table's _txn, only when a record is torn.
        final long[] tableTxn = wholeTxn < publishedTxn ? committedTableTxn.get() : null;
        final long lastTxn = recoverTail(path, publishedTxn, wholeTxn, tableTxn, unsyncedFrom, scanFrom, isDurableMode);
        isPastTailUnchecked = true;
        maxTxn.set(lastTxn);
        txnMem.jumpTo(HEADER_SIZE);
        long maxStructureVersion = txnMem.getLong(HEADER_SIZE + (wholeTxn - 1) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        if (lastTxn > wholeTxn && tableTxn != null) {
            // The kept torn records' versions are not on the disk; the table committed them, so it has their schema.
            maxStructureVersion = Math.max(maxStructureVersion, tableTxn[1]);
        }
        txnMem.jumpTo(HEADER_SIZE + lastTxn * RECORD_SIZE);
        return maxStructureVersion;
    }

    /**
     * Whether the record at {@code txn}, at {@code record}, is not wholly on disk. A sector holds a record's bytes
     * whole or not at all, the bytes past the tail were zero, and the commit timestamp is stored last, so a torn
     * record shows zeros where the written record has none: a zero wal id, a timestamp whose upper half is zero, or a
     * structure version whose lower half is zero and does not follow the previous record, at {@code prevRecord},
     * which is not read for txn 1. The previous record is intact: the scan starts after a flushed one and stops at
     * the first torn one.
     */
    private static boolean isTornRecord(long txn, long record, long prevRecord) {
        final int walId = Unsafe.getInt(record + TX_LOG_WAL_ID_OFFSET);
        if (walId == 0) {
            return true;
        }
        if (txn == 1) {
            // Record 1 shares the header's sector, so it reaches the disk with the header that publishes it.
            return false;
        }
        final long timestamp = Unsafe.getLong(record + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        final long prevTimestamp = Unsafe.getLong(prevRecord + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        if ((timestamp >>> 32) == 0 && ((prevTimestamp >>> 32) != 0 || (timestamp == 0 && prevTimestamp != 0))) {
            return true;
        }
        final long structureVersion = Unsafe.getLong(record + TX_LOG_STRUCTURE_VERSION_OFFSET);
        if ((structureVersion & 0xFFFFFFFFL) != 0) {
            return false;
        }
        final long prevStructureVersion = Unsafe.getLong(prevRecord + TX_LOG_STRUCTURE_VERSION_OFFSET);
        if (walId == STRUCTURAL_CHANGE_WAL_ID) {
            return structureVersion != prevStructureVersion + 1;
        }
        return walId > 0 && structureVersion != prevStructureVersion;
    }

    // The first txn an open checks for a tear: the records below it were covered by a completed flush, or are
    // trusted. Above publishedTxn when it checks none.
    private static long scanFrom(long unsyncedFrom, long publishedTxn, boolean isDurableMode) {
        // A file written by a binary without the hint has none: its records are trusted.
        if (unsyncedFrom > 0 || (unsyncedFrom < 0 && !isDurableMode)) {
            return Math.max(1, Math.min(Math.abs(unsyncedFrom), publishedTxn + 1));
        }
        return unsyncedFrom < 0 ? 1 : publishedTxn + 1;
    }

    // The last txn before the first torn record from scanFrom on, or publishedTxn when none is torn.
    private long findWholeTail(long scanFrom, long publishedTxn) {
        for (long txn = scanFrom; txn <= publishedTxn; txn++) {
            if (isTornRecord(txn)) {
                return txn - 1;
            }
        }
        return publishedTxn;
    }

    private boolean hasNonZeroBytes(long lo, long hi) {
        long offset = lo;
        for (; offset + Long.BYTES <= hi; offset += Long.BYTES) {
            if (txnMem.getLong(offset) != 0) {
                return true;
            }
        }
        for (; offset < hi; offset++) {
            if (txnMem.getByte(offset) != 0) {
                return true;
            }
        }
        return false;
    }

    private boolean isDurableMode() {
        final int commitMode = LocalDurabilityPolicy.resolveCommitMode(configuration.getCommitMode(), durabilityPolicy);
        return commitMode == CommitMode.ADAPTIVE || commitMode == CommitMode.SYNC;
    }

    private boolean isTornRecord(long txn) {
        final long offset = HEADER_SIZE + (txn - 1) * RECORD_SIZE;
        if (offset + RECORD_SIZE > txnMem.size()) {
            return true;
        }
        final long record = txnMem.addressOf(offset);
        return isTornRecord(txn, record, record - RECORD_SIZE);
    }

    // Every record up to the tail is on disk.
    private void markFlushed() {
        txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, maxTxn.get() + 1);
    }

    // Store order matters: a writeback that catches the record half-stored must show a zero timestamp.
    private void putRecord(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp) {
        if (isPastTailUnchecked) {
            // A crashed writer may have left whole records past the tail. Zero them on disk first, in every
            // mode, so that a torn record shows zeros and not an older record the scan cannot tell apart.
            isPastTailUnchecked = false;
            final long tailOffset = HEADER_SIZE + maxTxn.get() * RECORD_SIZE;
            if (hasNonZeroBytes(tailOffset, txnMem.size())) {
                zeroFrom(tailOffset);
                txnMem.flushFile(false);
            }
        }
        txnMem.putLong(structureVersion);
        txnMem.putInt(walId);
        txnMem.putInt(segmentId);
        txnMem.putInt(segmentTxn);
        Unsafe.storeFence();
        txnMem.putLong(timestamp);
    }

    /**
     * Drops a torn tail left by a power loss and returns the txn the log ends at. The records from the
     * first one a flush may not have covered were scanned, and {@code wholeTxn} is the last one before the
     * first torn one. The log is cut there, but never below the table's committed txn, {@code tableTxn[0]},
     * and not at all when {@code tableTxn} is null, not known: those records stay as they are, as the apply
     * never reads them again, and a cut would hand their txns out twice. Its saved lag txns are not kept, as
     * every writer open applies them again from the log. A
     * cut zeroes everything past the new tail and makes that durable before the hint moves past the records
     * it kept. An open that cuts nothing leaves the bytes past the tail to the first append.
     */
    private long recoverTail(Path path, long publishedTxn, long wholeTxn, long[] tableTxn, long unsyncedFrom, long scanFrom, boolean isDurableMode) {
        long tail = publishedTxn;
        if (wholeTxn < publishedTxn) {
            final long committedTxn = tableTxn != null ? tableTxn[0] : Long.MAX_VALUE;
            tail = Math.max(wholeTxn, Math.min(publishedTxn, committedTxn));
            if (tail > wholeTxn) {
                LOG.error().$("keeping torn sequencer txnlog records the table already used [path=").$(path)
                        .$(", publishedTxn=").$(publishedTxn)
                        .$(", firstTornTxn=").$(wholeTxn + 1)
                        .$(", keptTxn=").$(tail)
                        .$(", committedTxn=").$(committedTxn)
                        .I$();
                final long keptEnd = HEADER_SIZE + tail * RECORD_SIZE;
                if (keptEnd > txnMem.size()) {
                    // A lost file extension: the kept records read as zeros, as before the cut existed.
                    txnMem.extend(keptEnd);
                }
            }
        }
        final boolean isCut = tail != publishedTxn;
        // A reader that does not open the log trusts a header the predicate certifies. The predicate reads the
        // scanned records with its own reads, so this checks that the two agree.
        assert !isCut || !isTailIntact(ff, txnMem.getFd(), publishedTxn, unsyncedFrom, isDurableMode);
        if (isCut) {
            LOG.info().$("dropping torn sequencer txnlog tail [path=").$(path)
                    .$(", publishedTxn=").$(publishedTxn)
                    .$(", keptTxn=").$(tail)
                    .I$();
            txnMem.putLong(MAX_TXN_OFFSET_64, tail);
            zeroFrom(HEADER_SIZE + tail * RECORD_SIZE);
        }
        // A cut is flushed in every mode: a record written over it must tear over zeros. The kept records are
        // flushed only where the hint is a durability promise.
        if (isCut || (scanFrom <= publishedTxn && isDurableMode)) {
            txnMem.flushFile(false);
        }
        final boolean isFlushed = isDurableMode || (unsyncedFrom >= 0 && (isCut || scanFrom > publishedTxn));
        txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, isFlushed ? tail + 1 : -(tail + 1));
        return tail;
    }

    private void sync0() {
        // Local Java commits and create keep the configured grade: the object store does not hold them on a replica.
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            // With a group window, ADAPTIVE takes an async sync, which writes nothing back, and leaves
            // the device flush to the batched flush (fdatasyncTxnLog). Other modes keep their own grade.
            final boolean deferDeviceFlush = commitMode == CommitMode.ADAPTIVE
                    && configuration.getAdaptiveCommitGroupWindowUs() > 0;
            txnMem.sync(commitMode == CommitMode.ASYNC || deferDeviceFlush);
            // ADAPTIVE with no group window flushes the header to the device here.
            if (commitMode == CommitMode.ADAPTIVE && !deferDeviceFlush) {
                ff.fdatasync(txnMem.getFd());
            }
            // The hint moves only past records a synchronous flush covered: here under SYNC and ADAPTIVE W=0,
            // in fdatasyncTxnLog() under ADAPTIVE W>0. Under NOSYNC and ASYNC the next open scans them.
            if (!deferDeviceFlush && commitMode != CommitMode.ASYNC) {
                markFlushed();
            }
        }
    }

    private void zeroFrom(long offset) {
        final long size = txnMem.size();
        if (offset < size) {
            Vect.memset(txnMem.addressOf(offset), size - offset, 0);
        }
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private long address;
        private long fd;
        private FilesFacade ff;
        private long txn;
        private long txnCount = -1;
        private long txnLo;
        private long txnOffset;

        public TransactionLogCursorImpl(CairoConfiguration configuration, long txnLo, final Path path) {
            try {
                of(configuration.getFilesFacade(), configuration.getBypassWalFdCache(), txnLo, path);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            if (fd > 0) {
                ff.close(fd);
                fd = 0;
            }
            if (txnCount > -1 && address > 0) {
                ff.munmap(address, getMappedLen(), MemoryTag.MMAP_TX_LOG_CURSOR);
                txnCount = 0;
                address = 0;
            }
        }

        @Override
        public boolean extend() {
            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);

                this.txnLo = txn - 1;
                this.txnOffset -= RECORD_SIZE;
                return true;
            }
            return false;
        }

        @Override
        public long getCommitTimestamp() {
            return Unsafe.getLong(address + txnOffset + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        }

        @Override
        public long getMaxTxn() {
            return txnCount;
        }

        @Override
        public int getPartitionSize() {
            return 0;
        }

        @Override
        public int getSegmentId() {
            return Unsafe.getInt(address + txnOffset + TX_LOG_SEGMENT_OFFSET);
        }

        @Override
        public int getSegmentTxn() {
            return Unsafe.getInt(address + txnOffset + TX_LOG_SEGMENT_TXN_OFFSET);
        }

        @Override
        public long getStructureVersion() {
            return Unsafe.getLong(address + txnOffset + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }

        @Override
        public long getTxn() {
            return txn;
        }

        @Override
        public long getTxnMaxTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMinTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnRowCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVersion() {
            return WAL_SEQUENCER_FORMAT_VERSION_V1;
        }

        @Override
        public int getWalId() {
            return Unsafe.getInt(address + txnOffset + TX_LOG_WAL_ID_OFFSET);
        }

        @Override
        public boolean hasNext() {
            if (hasNext(getMappedLen())) {
                return true;
            }

            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);
                return hasNext(getMappedLen());
            }
            return false;
        }

        @Override
        public void setPosition(long txn) {
            this.txnOffset = HEADER_SIZE + (txn - 1) * RECORD_SIZE;
            this.txn = txn;
        }

        @Override
        public void toMinTxn() {
            toTop();
        }

        @Override
        public void toTop() {
            if (txnCount > -1L) {
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
                this.txn = txnLo;
            }
        }

        private static long openFileRO(final FilesFacade ff, final Path path, boolean bypassFdCache) {
            return bypassFdCache
                    ? TableUtils.openRONoCache(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG)
                    : TableUtils.openRO(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG);
        }

        private long getMappedLen() {
            return txnCount * RECORD_SIZE + HEADER_SIZE;
        }

        private boolean hasNext(long mappedLen) {
            if (txnOffset + 2 * RECORD_SIZE <= mappedLen) {
                txnOffset += RECORD_SIZE;
                txn++;
                return true;
            }
            return false;
        }

        @NotNull
        private TransactionLogCursorImpl of(FilesFacade ff, boolean bypassFdCache, long txnLo, Path path) {
            this.ff = ff;
            close();
            this.fd = openFileRO(ff, path, bypassFdCache);
            long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > -1L) {
                this.txnCount = newTxnCount;
                long newAddr = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                if (newAddr == FilesFacade.MAP_FAILED) {
                    throw CairoException.critical(Os.errno()).put("cannot mmap transaction log [path=").put(path).put(']');
                }
                this.address = newAddr;
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
            } else {
                throw CairoException.critical(ff.errno()).put("cannot read sequencer transactions [path=").put(path).put(']');
            }
            this.txnLo = txnLo;
            txn = txnLo;
            return this;
        }

        private void remap(long newTxnCount) {
            final long oldSize = getMappedLen();
            txnCount = newTxnCount;
            final long newSize = getMappedLen();
            long newAddr = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
            if (newAddr == FilesFacade.MAP_FAILED) {
                throw CairoException.critical(Os.errno()).put("cannot remap transaction log [fd=").put(fd).put(']');
            }
            address = newAddr;
        }
    }
}
