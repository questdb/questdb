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
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.wal.WalUtils.*;

/**
 * This class is used to read/write transactions to the disk.
 * This is V2 implementation of the sequencer transaction log storage.
 * <p>
 * File table_dir\\txn_seq\\_txnlog stores the header to be compatible with V1
 * Header: 76 bytes
 * <p>
 * Transactions are stored in table_dir\\txn_seq\\_txn_parts\\{partId} files. {PartId} starts from 0.
 * Each part file contains a fixed number of transactions. The number of transactions per part
 * is defined by the header Partition Size value.
 * <p>
 * To read a transaction one has to open the file partition where it's located.
 * To calculate the partition one can use the following formula:
 * {@code (txn - 1) / partTransactionCount}
 * <p>
 * Header and record is described in {@link TableTransactionLogFile}
 * <p>
 * Transaction record: 60 bytes
 * <p>
 * Under ADAPTIVE a power loss can leave the tail torn: the header can reach the disk ahead of the part page that holds
 * the records it counts, and a record can be caught half-stored. {@link #UNSYNCED_FROM_TXN_OFFSET_64} names the first
 * record a completed flush may not have covered, and {@link #open(Path)} drops a torn record at or above it.
 */
public class TableTransactionLogV2 implements TableTransactionLogFile {
    public static final long MIN_TIMESTAMP_OFFSET = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long MAX_TIMESTAMP_OFFSET = MIN_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long ROW_COUNT_OFFSET = MAX_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long RESERVED_OFFSET = ROW_COUNT_OFFSET + Long.BYTES;
    public static final long RECORD_SIZE = RESERVED_OFFSET + Long.BYTES;
    /**
     * Header slot, in the reserved area, holding the first txn whose record a completed flush may not have covered.
     * Zero means every record is trusted: a log no ADAPTIVE append on a node that keeps it durable itself has
     * touched, one written by a binary without the slot, or one a replica's downloader appends to. The first such
     * append starts it, and it moves only after a flush of the part and the header. Every record at or above it was
     * appended by this binary with a checksum. It is V1's slot and encoding, and shares the header's first sector
     * with {@code MAX_TXN_OFFSET_64}, so a power loss cannot persist one without the other.
     */
    public static final long UNSYNCED_FROM_TXN_OFFSET_64 = HEADER_SEQ_PART_SIZE_32 + Integer.BYTES + 2 * Long.BYTES;
    private static final Log LOG = LogFactory.getLog(TableTransactionLogV2.class);
    private static final CarrierLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new CarrierLocal<>();
    private final CairoConfiguration configuration;
    // A replica's object store holds the durable copy: the log is not scanned there, and its field stays 0.
    private final LocalDurabilityPolicy durabilityPolicy;
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final Path rootPath;
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnPartMem = Vm.getCMARWInstance();
    private final WalDirectoryPolicy walDirectoryPolicy;
    private long partId = -1;
    private int partTransactionCount;
    // The part an ADAPTIVE append of this instance has made zero past its append position; -1 until the first one.
    private long pastTailZeroedPartId = -1;

    @TestOnly
    public TableTransactionLogV2(CairoConfiguration configuration, int seqPartTransactionCount, WalDirectoryPolicy walDirectoryPolicy) {
        this(configuration, seqPartTransactionCount, walDirectoryPolicy, LocalDurabilityPolicy.ALWAYS_ON);
    }

    public TableTransactionLogV2(
            CairoConfiguration configuration,
            int seqPartTransactionCount,
            WalDirectoryPolicy walDirectoryPolicy,
            LocalDurabilityPolicy durabilityPolicy
    ) {
        this.configuration = configuration;
        this.durabilityPolicy = durabilityPolicy;
        this.ff = configuration.getFilesFacade();
        this.partTransactionCount = seqPartTransactionCount;
        this.walDirectoryPolicy = walDirectoryPolicy;
        rootPath = new Path();
    }

    public static long readMaxStructureVersion(Path path, long logFileFd, FilesFacade ff, boolean bypassWalFdCache) {
        long lastTxn = ff.readNonNegativeLong(logFileFd, MAX_TXN_OFFSET_64);
        if (lastTxn < 0) {
            return -1;
        }
        int partTransactionCount = ff.readNonNegativeInt(logFileFd, HEADER_SEQ_PART_SIZE_32);
        if (partTransactionCount < 1) {
            return -1;
        }

        // Open last part and read last structure version
        if (lastTxn > 0L) {
            long prevTxn = lastTxn - 1;
            long part = prevTxn / partTransactionCount;
            int size = path.size();
            path.concat(TXNLOG_PARTS_DIR).slash().put(part);
            long partFd = -1;
            try {
                partFd = openPartFileRO(ff, path.$(), bypassWalFdCache);
                long fileReadOffset = (prevTxn % partTransactionCount) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET;
                return ff.readNonNegativeLong(partFd, fileReadOffset);
            } finally {
                if (partFd > -1) {
                    ff.close(partFd);
                }
                path.trimTo(size);
            }
        }
        return 0;
    }

    @Override
    public long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
        openTxnPart();
        zeroPastTail();
        startUnsyncedFrom();

        final long recordStart = txnPartMem.getAppendOffset();
        txnPartMem.putLong(structureVersion);
        txnPartMem.putInt(walId);
        txnPartMem.putInt(segmentId);
        txnPartMem.putInt(segmentTxn);
        txnPartMem.putLong(timestamp);
        txnPartMem.putLong(txnMinTimestamp);
        txnPartMem.putLong(txnMaxTimestamp);
        txnPartMem.putLong(txnRowCount);
        // Write CRC over the body [recordStart, recordStart+RESERVED_OFFSET) in the reserved trailing slot.
        txnPartMem.putLong(TableUtils.calculateCvAreaChecksum(txnPartMem.addressOf(recordStart), RESERVED_OFFSET));

        Unsafe.storeFence();
        long maxTxn = this.maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, maxTxn);
        sync0();
        // Transactions are 1 based here
        return maxTxn;
    }

    @Override
    public void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        openTxnPart();
        zeroPastTail();
        startUnsyncedFrom();

        final long recordStart = txnPartMem.getAppendOffset();
        txnPartMem.putLong(newStructureVersion);
        txnPartMem.putInt(STRUCTURAL_CHANGE_WAL_ID);
        txnPartMem.putInt(-1);
        txnPartMem.putInt(-1);
        txnPartMem.putLong(timestamp);
        txnPartMem.putLong(serializer.getCommandType(instance));
        txnPartMem.putLong(0L);
        txnPartMem.putLong(0L);
        // Write CRC over the body [recordStart, recordStart+RESERVED_OFFSET) in the reserved trailing slot.
        txnPartMem.putLong(TableUtils.calculateCvAreaChecksum(txnPartMem.addressOf(recordStart), RESERVED_OFFSET));
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
        txnPartMem.close(false);
        rootPath.close();
    }

    @Override
    public void create(Path path, long tableCreateTimestamp) {
        createTxnFile(path, tableCreateTimestamp);
        createPartsDir(configuration.getMkDirMode());
    }

    @Override
    public long endMetadataChangeEntry() {
        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, nextTxn);
        return nextTxn;
    }

    @Override
    public void fullSync() {
        // The part file must be durable before the header, as in sync0(). NOSYNC and ASYNC promise nothing
        // across a power loss and sync the header alone.
        if (txnPartMem.isOpen() && CommitMode.isPowerLossDurable(configuration.getCommitMode())) {
            txnPartMem.sync(false);
        }
        txnMem.sync(false);
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
                ? (TransactionLogCursorImpl) cursorHolder.getTransactionLogCursor(WAL_SEQUENCER_FORMAT_VERSION_V2)
                : tlTransactionLogCursor.get();
        if (cursor == null) {
            cursor = new TransactionLogCursorImpl(ff, configuration.getBypassWalFdCache(), txnLo, path, partTransactionCount);
            if (cursorHolder != null) {
                cursorHolder.registerTransactionLogCursor(WAL_SEQUENCER_FORMAT_VERSION_V2, cursor);
            } else {
                tlTransactionLogCursor.set(cursor);
            }
            return cursor;
        }
        try {
            return cursor.of(ff, configuration.getBypassWalFdCache(), txnLo, path, partTransactionCount);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            long prevTxn = lastTxn - 1;
            openTxnPart(prevTxn);
            long lastPartTxn = (prevTxn) % partTransactionCount;
            return WalUtils.DROP_TABLE_WAL_ID == txnPartMem.getInt(lastPartTxn * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
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
            openTxnMem(path);
        }

        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET_64);
        partTransactionCount = txnMem.getInt(HEADER_SEQ_PART_SIZE_32);
        if (partTransactionCount < 1) {
            throw new CairoException().put("invalid sequencer file part size [size=").put(partTransactionCount).put(", path=").put(path).put(']');
        }
        final boolean isAdaptive = LocalDurabilityPolicy.resolveCommitMode(configuration.getCommitMode(), durabilityPolicy) == CommitMode.ADAPTIVE;
        if (isAdaptive) {
            lastTxn = recoverTail(path, lastTxn);
        } else if (!durabilityPolicy.isLocalDurabilityEnabled() && txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64) != 0) {
            // A replica's downloader appends records the field does not cover, without a checksum when its primary
            // is older. A promoted node must trust them, so the field a demoted primary left goes, durably.
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, 0);
            ff.fdatasync(txnMem.getFd());
        }
        maxTxn.set(lastTxn);

        long maxStructureVersion = 0L;
        if (lastTxn > 0L) {
            long prevTxn = lastTxn - 1;
            openTxnPart(prevTxn);
            long lastPartTxn = (prevTxn) % partTransactionCount;
            maxStructureVersion = txnPartMem.getLong(lastPartTxn * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }
        openTxnPart();
        // Open part can leave prev txn append position when part is the same
        setAppendPosition();
        pastTailZeroedPartId = -1;
        return maxStructureVersion;
    }

    private static boolean isZero(long lo, long hi) {
        long p = lo;
        for (; p + Long.BYTES <= hi; p += Long.BYTES) {
            if (Unsafe.getLong(p) != 0) {
                return false;
            }
        }
        for (; p < hi; p++) {
            if (Unsafe.getByte(p) != 0) {
                return false;
            }
        }
        return true;
    }

    private static long openPartFileRO(final FilesFacade ff, final LPSZ path, boolean bypassFdCache) {
        return bypassFdCache
                ? TableUtils.openRONoCache(ff, path, LOG)
                : TableUtils.openRO(ff, path, LOG);
    }

    private void createPartsDir(int mkDirMode) {
        int rootLen = rootPath.size();
        rootPath.concat(TXNLOG_PARTS_DIR);
        try {
            if (!ff.exists(rootPath.$()) && ff.mkdir(rootPath.$(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create directory [path='").put(rootPath).put("']");
            }
        } finally {
            rootPath.trimTo(rootLen);
        }
    }

    private void createTxnFile(Path path, long tableCreateTimestamp) {
        openTxnMem(path);

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V2);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.putInt(partTransactionCount);
        sync0();
    }

    /**
     * Whether the record in part slot {@code slot} fails its check: its stored checksum is 0 or does not match its
     * body. Only records at or above the field are checked, and this binary wrote each of them with a checksum
     * (calculateCvAreaChecksum never returns 0), so a failure is a tear, wherever the lost bytes fall.
     */
    private boolean failsCheck(long slot) {
        final long address = txnPartMem.addressOf(slot * RECORD_SIZE);
        final long storedCrc = Unsafe.getLong(address + RESERVED_OFFSET);
        return storedCrc == 0L || storedCrc != TableUtils.calculateCvAreaChecksum(address, RESERVED_OFFSET);
    }

    /**
     * True under ADAPTIVE with a group window above zero, where the batched flush takes over the per-commit
     * sequencer fdatasync. It depends on configuration only, so concurrent WAL writers of a table agree.
     */
    private boolean isDeviceFlushDeferred() {
        return configuration.getCommitMode() == CommitMode.ADAPTIVE && configuration.getAdaptiveCommitGroupWindowUs() > 0;
    }

    /**
     * Moves the field past the records a completed flush of the part and the header covered. A field that has not
     * started stays 0, and a replica never moves it.
     */
    private void markFlushed(long flushedTxn) {
        if (txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64) > 0 && durabilityPolicy.isLocalDurabilityEnabled()) {
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, flushedTxn + 1);
        }
    }

    private void openTxnMem(Path path) {
        rootPath.of(path);
        int rootLen = rootPath.size();
        rootPath.concat(TXNLOG_FILE_NAME);
        try {
            txnMem.of(ff, rootPath.$(), HEADER_SIZE, HEADER_SIZE, MemoryTag.MMAP_TX_LOG);
        } finally {
            rootPath.trimTo(rootLen);
        }
    }

    private void openTxnPart(long txn) {
        long part = txn / partTransactionCount;
        if (partId != part) {
            int size = rootPath.size();
            try {
                rootPath.concat(TXNLOG_PARTS_DIR).slash().put(part);
                long partSize = partTransactionCount * RECORD_SIZE;
                if (txnPartMem.isOpen()
                        && CommitMode.isPowerLossDurable(configuration.getCommitMode())
                        && partId * partTransactionCount < maxTxn.get()) {
                    // Every later flush reaches only the open part, so the part being left would never be flushed.
                    // Its records may be pending, written under a group window or by a since-closed instance that
                    // ran in another mode, and the records after them become durable. A part past the tail holds
                    // none.
                    ff.fdatasync(txnPartMem.getFd());
                }
                txnPartMem.close(false);
                txnPartMem.of(ff, rootPath.$(), partSize, partSize, MemoryTag.MMAP_TX_LOG);
                txnPartMem.jumpTo((txn % partTransactionCount) * RECORD_SIZE);
                rootPath.trimTo(size);
                walDirectoryPolicy.initSequencerPart(rootPath, part);
                partId = part;
            } finally {
                rootPath.trimTo(size);
            }
        }
    }

    private void openTxnPart() {
        openTxnPart(this.maxTxn.get());
    }

    /**
     * Drops a torn tail left by a power loss and returns the txn the log ends at. Under ADAPTIVE with a group window
     * the part and the header reach the disk together only at the batched flush, and a power loss before it can leave
     * the header counting records the part does not hold, or holds only in part. Scans from the field, never below
     * the last part, and cuts the log before the first record that fails its check. Records below the field were
     * covered by a completed flush and are never cut: a damaged one is reported by the cursor. Earlier parts are
     * whole: ADAPTIVE flushes a part before it opens the next (openTxnPart), and at every append without a window. A
     * cut zeroes everything past the new tail and makes that durable before the header, so a record later written
     * over it tears over zeros and the header never counts a dropped record again. A log whose field is 0 is trusted.
     * Reads the records since the last completed flush, at most one part.
     */
    private long recoverTail(Path path, long publishedTxn) {
        final long unsyncedFrom = txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64);
        if (unsyncedFrom <= 0) {
            return publishedTxn;
        }
        final long lastPartFirstTxn = Math.max(publishedTxn - 1, 0) / partTransactionCount * partTransactionCount + 1;
        final long scanFrom = Math.max(unsyncedFrom, lastPartFirstTxn);
        long tail = publishedTxn;
        if (scanFrom <= publishedTxn) {
            openTxnPart(publishedTxn - 1);
            for (long txn = scanFrom; txn <= publishedTxn; txn++) {
                if (failsCheck((txn - 1) % partTransactionCount)) {
                    tail = txn - 1;
                    break;
                }
            }
        }
        if (tail != publishedTxn) {
            LOG.info().$("dropping torn sequencer txnlog tail [path=").$(path)
                    .$(", publishedTxn=").$(publishedTxn)
                    .$(", keptTxn=").$(tail)
                    .I$();
            final long cutOffset = (tail - lastPartFirstTxn + 1) * RECORD_SIZE;
            Vect.memset(txnPartMem.addressOf(cutOffset), (long) partTransactionCount * RECORD_SIZE - cutOffset, 0);
            ff.fdatasync(txnPartMem.getFd());
            txnMem.putLong(MAX_TXN_OFFSET_64, tail);
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, tail + 1);
            ff.fdatasync(txnMem.getFd());
        } else if (unsyncedFrom > publishedTxn + 1) {
            // The max txn was lowered under the field (a checkpoint restore): the records past it are written again.
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, publishedTxn + 1);
        }
        return tail;
    }

    private void setAppendPosition() {
        txnPartMem.jumpTo((this.maxTxn.get() % partTransactionCount) * RECORD_SIZE);
    }

    /**
     * Starts the field at the record about to be appended, before the header counts it, on a log that has none and a
     * node that keeps the log durable itself in ADAPTIVE. The records below it were written by something that keeps
     * no field and stay trusted.
     */
    private void startUnsyncedFrom() {
        if (configuration.getCommitMode() == CommitMode.ADAPTIVE
                && txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64) == 0
                && durabilityPolicy.isLocalDurabilityEnabled()) {
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, maxTxn.get() + 1);
        }
    }

    private void sync0() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            // The part file must be durable before the header that points to it, or a crash after the header
            // sync can leave maxTxn=N pointing at a zeroed or partial record. ASYNC promises nothing across a
            // power loss, and its MS_ASYNC orders nothing, so it syncs the header alone.
            //
            // With a group window, ADAPTIVE msyncs the files with MS_ASYNC, which flushes nothing, and leaves the
            // device flush to the batched flush (fdatasyncTxnLog). localDurableSeqTxn advances only after that
            // flush, so a durably acknowledged txn is on the device. Other modes keep their own grade.
            final boolean deferDeviceFlush = isDeviceFlushDeferred();
            final boolean async = commitMode == CommitMode.ASYNC || deferDeviceFlush;
            if (txnPartMem.isOpen() && commitMode != CommitMode.ASYNC) {
                txnPartMem.sync(async);
            }
            txnMem.sync(async);
            // ADAPTIVE flushes the part file to the device before the header: the header's maxTxn=N must not
            // reach the device before the record at txn N.
            if (commitMode == CommitMode.ADAPTIVE && !deferDeviceFlush) {
                if (txnPartMem.isOpen()) {
                    ff.fdatasync(txnPartMem.getFd());
                }
                ff.fdatasync(txnMem.getFd());
                markFlushed(maxTxn.get());
            }
        }
    }

    /**
     * Before the first ADAPTIVE append of this instance into a part, on a node that keeps the log durable itself,
     * zeroes the part from the append position on and makes that durable. An append that crashed before the header
     * counted its record, a checkpoint restore that lowered the max txn, or a power loss that kept a later sector and
     * lost an earlier one can leave a whole record anywhere past the tail, in the tail's part or in a later one. A
     * record written over it and torn by a power loss could show it whole, which passes its check. The open leaves
     * these bytes alone: a replica's downloader writes a batch's records past the max txn before it advances the max
     * txn. Reads the rest of a part once per part.
     */
    private void zeroPastTail() {
        if (pastTailZeroedPartId != partId
                && configuration.getCommitMode() == CommitMode.ADAPTIVE
                && durabilityPolicy.isLocalDurabilityEnabled()) {
            final long offset = txnPartMem.getAppendOffset();
            final long partSize = (long) partTransactionCount * RECORD_SIZE;
            final long address = txnPartMem.addressOf(offset);
            if (!isZero(address, address + partSize - offset)) {
                Vect.memset(address, partSize - offset, 0);
                ff.fdatasync(txnPartMem.getFd());
            }
            pastTailZeroedPartId = partId;
        }
    }

    @Override
    public void fdatasyncTxnLog() {
        // The batched device flush of adaptive group commit, part file before the header as in sync0(). The caller
        // holds the sequencer write lock, so no record is half-written up to this tail.
        final long flushedTxn = maxTxn.get();
        if (txnPartMem.isOpen()) {
            ff.fdatasync(txnPartMem.getFd());
        }
        if (txnMem.isOpen()) {
            ff.fdatasync(txnMem.getFd());
            markFlushed(flushedTxn);
        }
    }

    @Override
    public void fdatasyncTxnLog(long fromTxn) {
        // Walks down from the part below the open one. The WAL purge deletes parts from the oldest, and only parts
        // whose records are applied and need nothing else, so the walk stops at the first part that is gone: the
        // flush reaches the parts the log still keeps, not the table's whole history.
        final long fromPart = Math.max(fromTxn - 1, 0) / partTransactionCount;
        final int size = rootPath.size();
        try {
            for (long part = partId - 1; part >= fromPart; part--) {
                rootPath.trimTo(size).concat(TXNLOG_PARTS_DIR).slash().put(part);
                if (!ff.exists(rootPath.$())) {
                    break;
                }
                final long fd = TableUtils.openRW(ff, rootPath.$(), LOG, CairoConfiguration.O_NONE);
                try {
                    ff.fdatasync(fd);
                } finally {
                    ff.close(fd);
                }
            }
        } finally {
            rootPath.trimTo(size);
        }
        fdatasyncTxnLog();
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private final Path rootPath;
        private long address;
        private boolean bypassFdCache;
        private FilesFacade ff;
        private long headerFd;
        private long partFd = -1;
        private long partId = -1;
        private long partMapSize;
        private int partTransactionCount;
        private long txn = -2;
        private long txnCount = -1;
        private long txnLo;
        private long txnOffset;

        public TransactionLogCursorImpl(FilesFacade ff, boolean bypassWalFdCache, long txnLo, final @Transient Path path, int partTransactionCount) {
            rootPath = new Path();
            try {
                of(ff, bypassWalFdCache, txnLo, path, partTransactionCount);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            if (headerFd > -1) {
                ff.close(headerFd);
                headerFd = -1;
            }
            if (txnCount > -1 && address > 0) {
                ff.munmap(address, partMapSize, MemoryTag.MMAP_TX_LOG_CURSOR);
                txnCount = 0;
                address = 0;
            }
            closePart();
            rootPath.close();
        }

        @Override
        public boolean extend() {
            final long newTxnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
            boolean extended = newTxnCount > txnCount;
            if (extended) {
                txnCount = newTxnCount;
            }
            return extended;
        }

        @Override
        public long getCommitTimestamp() {
            assert address != 0;
            return Unsafe.getLong(address + txnOffset + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        }

        @Override
        public long getMaxTxn() {
            return txnCount;
        }

        @Override
        public int getPartitionSize() {
            return partTransactionCount;
        }

        @Override
        public int getSegmentId() {
            assert address != 0;
            return Unsafe.getInt(address + txnOffset + TX_LOG_SEGMENT_OFFSET);
        }

        @Override
        public int getSegmentTxn() {
            assert address != 0;
            return Unsafe.getInt(address + txnOffset + TX_LOG_SEGMENT_TXN_OFFSET);
        }

        @Override
        public long getStructureVersion() {
            assert address != 0;
            return Unsafe.getLong(address + txnOffset + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }

        @Override
        public long getTxn() {
            return txn;
        }

        @Override
        public long getTxnMaxTimestamp() {
            assert address != 0;
            return Unsafe.getLong(address + txnOffset + MAX_TIMESTAMP_OFFSET);
        }

        @Override
        public long getTxnMinTimestamp() {
            assert address != 0;
            return Unsafe.getLong(address + txnOffset + MIN_TIMESTAMP_OFFSET);
        }

        @Override
        public long getTxnRowCount() {
            assert address != 0;
            return Unsafe.getLong(address + txnOffset + ROW_COUNT_OFFSET);
        }

        @Override
        public int getVersion() {
            return WAL_SEQUENCER_FORMAT_VERSION_V2;
        }

        @Override
        public int getWalId() {
            assert address != 0;
            return Unsafe.getInt(address + txnOffset + TX_LOG_WAL_ID_OFFSET);
        }

        @Override
        public boolean hasNext() {
            if (txn >= txnCount) {
                txnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
                if (txn >= txnCount) {
                    return false;
                }
            }

            openPart(txn);
            verifyRecordChecksum();
            txn++;
            return true;
        }

        // Verify the per-record CRC in the reserved trailing slot, if present. calculateCvAreaChecksum never
        // returns 0, so a stored 0 means "no CRC written". That has three causes the slot cannot tell apart, and
        // all three read the record unverified, since rejecting them would suspend a table on records an older
        // QuestDB wrote correctly:
        //   (1) a record written by a binary that predates the per-record CRC. Its body is valid.
        //   (2) an absent record: an all-zero slot the cursor reached because the header's MAX_TXN became
        //       durable ahead of the record body. No real record carries walId 0 (writers use walId >= 1,
        //       STRUCTURAL_CHANGE_WAL_ID = -1, DROP_TABLE_WAL_ID = -2), and ApplyWal2TableJob rejects walId 0,
        //       so nothing is applied from it.
        //   (3) a torn write that zeroed only this slot. The checksum cannot catch it.
        // A non-zero stored CRC that does not match the body means a torn or partially written record.
        private void verifyRecordChecksum() {
            final long recordBase = address + txnOffset;
            final long storedCrc = Unsafe.getLong(recordBase + RESERVED_OFFSET);
            if (storedCrc == 0L) {
                // No checksum for this record: read it unverified.
                return;
            }
            TxnLogRecordVerifier.verify(txn, recordBase, RESERVED_OFFSET, storedCrc, txnOffset);
        }

        @Override
        public void setPosition(long txn) {
            this.txn = txn;
        }

        @Override
        public void toMinTxn() {
            int rootLen = rootPath.size();
            rootPath.concat(TXNLOG_PARTS_DIR).slash();
            long partId = txnLo / partTransactionCount;
            long minTxn = partId * partTransactionCount;

            int rootPathLen = rootPath.size();
            try {
                for (long part = partId - 1; part > -1L; part--) {
                    rootPath.trimTo(rootPathLen).put(part);
                    if (ff.exists(rootPath.$())) {
                        minTxn = part * partTransactionCount;
                    } else {
                        break;
                    }
                }
            } finally {
                rootPath.trimTo(rootLen);
            }
            openPart(minTxn);
            txn = txnLo = minTxn;
        }

        @Override
        public void toTop() {
            if (txnCount > -1L) {
                this.txn = txnLo;
            }
        }

        private static long openFileRO(final FilesFacade ff, final Path path, boolean bypassFdCache) {
            return bypassFdCache
                    ? TableUtils.openRONoCache(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG)
                    : TableUtils.openRO(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG);
        }

        private void closePart() {
            if (partFd > -1) {
                if (address != 0) {
                    ff.munmap(address, partMapSize, MemoryTag.MMAP_TX_LOG_CURSOR);
                }
                ff.close(partFd);
                partFd = -1;
                partId = -2;
                address = 0;
            }
        }

        @NotNull
        private TransactionLogCursorImpl of(FilesFacade ff, boolean bypassFdCache, long txnLo, @Transient Path path, int partTransactionCount) {
            this.ff = ff;
            close();
            this.partTransactionCount = partTransactionCount;
            partMapSize = partTransactionCount * RECORD_SIZE;
            this.headerFd = openFileRO(ff, path, bypassFdCache);
            this.bypassFdCache = bypassFdCache;
            long newTxnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
            rootPath.of(path);

            if (newTxnCount > -1L) {
                this.txnCount = newTxnCount;
                assert txnLo > -1;
                this.txn = txnLo;
            } else {
                throw CairoException.critical(ff.errno()).put("cannot read sequencer transactions [path=").put(path).put(']');
            }
            this.txnLo = txnLo;
            return this;
        }

        private void openPart(long zeroBasedTxn) {
            long part = zeroBasedTxn / partTransactionCount;
            if (part != partId) {
                closePart();
                int size = rootPath.size();
                try {
                    rootPath.concat(TXNLOG_PARTS_DIR).slash().put(part);
                    partFd = openPartFileRO(ff, rootPath.$(), bypassFdCache);
                    long newAddr = ff.mmap(partFd, partMapSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                    if (newAddr == FilesFacade.MAP_FAILED) {
                        throw CairoException.critical(ff.errno())
                                .put("cannot mmap transaction log part [path=").put(rootPath)
                                .put(", mapSize=").put(partMapSize)
                                .put(']');
                    }
                    address = newAddr;
                    partId = part;
                } finally {
                    rootPath.trimTo(size);
                }
            }
            this.txnOffset = (zeroBasedTxn % partTransactionCount) * RECORD_SIZE;
        }
    }
}
