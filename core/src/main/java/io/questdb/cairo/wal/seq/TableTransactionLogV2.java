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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
 * record a completed flush may not have covered, and {@link #open(Path)} drops a torn record at or above it, except one
 * the table has already applied. An open in another mode on a node that keeps the log durable itself stores the field
 * negated, as a V1 log does, and the next ADAPTIVE open flushes every part kept since the field's magnitude before it
 * claims the log again.
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
     * append starts it, and it moves only after a flush of the part and the header. Every record at or above it, up to
     * {@link #KNOWN_MAX_TXN_OFFSET_64}, was appended by this binary with a checksum. A negative value names the same
     * txn as its magnitude and means an open in NOSYNC, ASYNC or SYNC on a node that keeps the log durable itself
     * happened since: that run appends without a part flush before the header counts its records, so the next
     * ADAPTIVE open flushes every kept part from the magnitude's part to the tail's before it stores a positive value
     * again. With either sign, the slot next to it tracks the max txn. It is V1's slot and encoding, and shares the
     * header's first sector with {@code MAX_TXN_OFFSET_64}, so a power loss cannot persist one without the other.
     */
    public static final long UNSYNCED_FROM_TXN_OFFSET_64 = HEADER_SEQ_PART_SIZE_32 + Integer.BYTES + 2 * Long.BYTES;
    /**
     * Header slot next to {@link #UNSYNCED_FROM_TXN_OFFSET_64} holding the max txn as this binary last stored it while
     * the field is set. A writer that does not know the field, an older binary or a replica's downloader, appends
     * without a checksum and moves only the max txn, so a max txn above this slot means the records above it are that
     * writer's: they are trusted, as that writer trusts them. It is raised before the max txn and lowered after it, in
     * the same cache line and sector, so the disk never shows this binary's records above it.
     */
    public static final long KNOWN_MAX_TXN_OFFSET_64 = UNSYNCED_FROM_TXN_OFFSET_64 + Long.BYTES;
    private static final Log LOG = LogFactory.getLog(TableTransactionLogV2.class);
    private static final CarrierLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new CarrierLocal<>();
    private final Supplier<long[]> committedTableTxn;
    private final CairoConfiguration configuration;
    // A replica's object store holds the durable copy: the log is not scanned there, and its field stays 0.
    private final LocalDurabilityPolicy durabilityPolicy;
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final DirectUtf8StringZ partName = new DirectUtf8StringZ();
    private final LongList partsToFlush = new LongList();
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
        this(configuration, seqPartTransactionCount, walDirectoryPolicy, LocalDurabilityPolicy.ALWAYS_ON, () -> new long[]{0, -1});
    }

    /**
     * @param committedTableTxn one read of the table's {@code _txn}, asked only when an open finds a torn record: its
     *                          committed txn, which a torn-tail cut keeps, and its structure version, which an open
     *                          that keeps a torn last record takes when it is above the last whole record's. Null
     *                          when it is not known: nothing is cut then
     */
    public TableTransactionLogV2(
            CairoConfiguration configuration,
            int seqPartTransactionCount,
            WalDirectoryPolicy walDirectoryPolicy,
            LocalDurabilityPolicy durabilityPolicy,
            Supplier<long[]> committedTableTxn
    ) {
        this.configuration = configuration;
        this.durabilityPolicy = durabilityPolicy;
        this.ff = configuration.getFilesFacade();
        this.partTransactionCount = seqPartTransactionCount;
        this.walDirectoryPolicy = walDirectoryPolicy;
        this.committedTableTxn = committedTableTxn;
        rootPath = new Path();
    }

    /**
     * Whether an open now would keep every record the header counts, judged from the header alone, for a reader that
     * does not open the log: the open checks no record for a tear. Only an ADAPTIVE open on a node that keeps the log
     * durable itself checks any, and only those at or above the field's magnitude. For a log whose newest records a
     * writer that does not know the field appended, above {@link #KNOWN_MAX_TXN_OFFSET_64}, it answers false while
     * the open trusts them: the reader then opens the log, which trusts them too.
     *
     * @param maxTxn       the header's max txn
     * @param unsyncedFrom the header's {@link #UNSYNCED_FROM_TXN_OFFSET_64}
     * @param isLocal      whether the node keeps the log durable itself
     * @param isAdaptive   whether the open would run in ADAPTIVE
     */
    public static boolean isTailCertain(long maxTxn, long unsyncedFrom, boolean isLocal, boolean isAdaptive) {
        return !(isLocal && isAdaptive) || unsyncedFrom == 0 || Math.abs(unsyncedFrom) > maxTxn;
    }

    /**
     * Lowers a V2 header's max txn to {@code newMaxTxn} and keeps the field and the slot consistent with it, as a
     * checkpoint restore and the torn-tail cut do: the field's magnitude goes no higher than {@code newMaxTxn + 1}
     * and keeps its sign, the slot no higher than {@code newMaxTxn}, both only while the field is set. The three
     * stores land in the header's first sector, the max txn first and the slot last, behind a fence: a header caught
     * in between shows the slot above the max txn, never below, so the records an older binary appends afterwards
     * sit above the slot and are trusted at the next open instead of being cut as torn. The caller checks that the
     * header is a V2 one and that {@code newMaxTxn} is below the stored max txn.
     */
    public static void lowerMaxTxn(MemoryCMARW header, long newMaxTxn) {
        header.putLong(MAX_TXN_OFFSET_64, newMaxTxn);
        final long unsyncedFrom = header.getLong(UNSYNCED_FROM_TXN_OFFSET_64);
        if (unsyncedFrom != 0) {
            final long magnitude = Math.min(Math.abs(unsyncedFrom), newMaxTxn + 1);
            header.putLong(UNSYNCED_FROM_TXN_OFFSET_64, unsyncedFrom < 0 ? -magnitude : magnitude);
            Unsafe.storeFence();
            header.putLong(KNOWN_MAX_TXN_OFFSET_64, Math.min(header.getLong(KNOWN_MAX_TXN_OFFSET_64), newMaxTxn));
        }
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
        storeKnownMaxTxn(this.maxTxn.get() + 1);

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
        storeKnownMaxTxn(maxTxn.get() + 1);
        Unsafe.storeFence();
        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, nextTxn);
        return nextTxn;
    }

    @Override
    public void flushOnCleanClose() {
        // An open checks records only in ADAPTIVE, where the batched flush moves the hint.
    }

    @Override
    public void fullSync() {
        // Local commits keep the configured grade; the downloader does not call this method.
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
        final long unsyncedFrom = txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64);
        final long knownMaxTxn = txnMem.getLong(KNOWN_MAX_TXN_OFFSET_64);
        final boolean isLocal = durabilityPolicy.isLocalDurabilityEnabled();
        final boolean isAdaptive = LocalDurabilityPolicy.resolveCommitMode(configuration.getCommitMode(), durabilityPolicy) == CommitMode.ADAPTIVE;
        // The first txn the ADAPTIVE open checks for a tear, 0 for none, and the first txn whose part it flushes
        // before it claims the log again, 0 while the claim holds. The field's sign is read in every branch.
        long scanFrom = 0;
        long flushFrom = 0;
        if (!isLocal) {
            if (unsyncedFrom != 0) {
                // A replica's downloader appends records the field does not cover, without a checksum when its
                // primary is older. A promoted node must trust them, so the field a demoted primary left, with either
                // sign, goes durably.
                txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, 0);
                txnMem.flushFile(false);
            }
        } else if (unsyncedFrom != 0 && lastTxn > knownMaxTxn) {
            // A writer that does not know the field, an older binary, appended the records above the slot without a
            // checksum. They are trusted, as it trusts them, and so is every record under them, since a cut would drop
            // them. Cleared in every mode, so that no append stores the slot over them while the field is still set.
            // An ADAPTIVE open flushes the kept parts from the field's magnitude on and claims the log again above
            // them; in another mode the next ADAPTIVE append starts the field above them.
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, 0);
            Unsafe.storeFence();
            LOG.info().$("trusting sequencer txnlog records appended without a flushed-through txn [path=").$(path)
                    .$(", unsyncedFromTxn=").$(unsyncedFrom)
                    .$(", knownMaxTxn=").$(knownMaxTxn)
                    .$(", maxTxn=").$(lastTxn)
                    .I$();
            if (isAdaptive) {
                flushFrom = Math.min(Math.abs(unsyncedFrom), lastTxn + 1);
            }
        } else if (!isAdaptive) {
            if (unsyncedFrom > 0) {
                // This run appends without a part flush before the header counts its records, so the field's promise
                // ends here: stored negated, as a V1 log does, and left so by every later open in these modes. The
                // slot keeps tracking the max txn, so an older binary's later appends are still seen.
                txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, -unsyncedFrom);
            }
        } else if (unsyncedFrom < 0) {
            // A run in another mode wrote since the field's magnitude. Its tail is checked like this binary's own,
            // never below the last part, and every kept part from the magnitude's part on is flushed before the log
            // is claimed again.
            scanFrom = -unsyncedFrom;
            flushFrom = Math.min(-unsyncedFrom, lastTxn + 1);
        } else if (unsyncedFrom > lastTxn + 1) {
            // The max txn was lowered under the field by a restore that left the field and the slot where they were:
            // nothing to check, the claim is stored again at the tail.
            flushFrom = lastTxn + 1;
        } else if (unsyncedFrom > 0) {
            scanFrom = unsyncedFrom;
        }
        long wholeTxn = lastTxn;
        long[] tableTxn = null;
        if (isAdaptive) {
            final long publishedTxn = lastTxn;
            wholeTxn = findWholeTail(publishedTxn, scanFrom);
            // One read of the table's _txn, only when a record is torn.
            tableTxn = wholeTxn < publishedTxn ? committedTableTxn.get() : null;
            lastTxn = recoverTail(path, publishedTxn, wholeTxn, tableTxn, flushFrom);
            // A reader that does not open the log trusts a header the predicate certifies.
            assert lastTxn == publishedTxn || !isTailCertain(publishedTxn, unsyncedFrom, isLocal, true);
        }
        maxTxn.set(lastTxn);

        long maxStructureVersion = 0L;
        if (wholeTxn > 0L) {
            long prevTxn = wholeTxn - 1;
            openTxnPart(prevTxn);
            long lastPartTxn = (prevTxn) % partTransactionCount;
            maxStructureVersion = txnPartMem.getLong(lastPartTxn * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }
        if (lastTxn > wholeTxn && tableTxn != null) {
            // The kept torn records' versions are not on the disk; the table committed them, so it has their schema.
            maxStructureVersion = Math.max(maxStructureVersion, tableTxn[1]);
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

    private static boolean isPartFileName(Utf8Sequence name) {
        final int n = name.size();
        if (n == 0) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            final byte b = name.byteAt(i);
            if (b < '0' || b > '9') {
                return false;
            }
        }
        return true;
    }

    private static long lastPartFirstTxn(long publishedTxn, int partTransactionCount) {
        return Math.max(publishedTxn - 1, 0) / partTransactionCount * partTransactionCount + 1;
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
        // A new log has no field and no slot, whatever an existing file held in those bytes.
        txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, 0L);
        txnMem.putLong(KNOWN_MAX_TXN_OFFSET_64, 0L);
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
     * Flushes with fdatasync every part file from {@code fromPart} to {@code toPart} the log still keeps, highest
     * first. The parts are listed, not counted down: the WAL purge keeps a backup-locked or in-use part while it
     * deletes later ones, so a gap does not end the kept parts. Only part files are flushed; another entry in the
     * directory is ignored. The open never creates a part: one the purge removed since the listing is skipped.
     */
    private void fdatasyncParts(long fromPart, long toPart) {
        if (fromPart > toPart) {
            return;
        }
        final int size = rootPath.size();
        try {
            rootPath.concat(TXNLOG_PARTS_DIR);
            final long findPtr = ff.findFirst(rootPath.$());
            if (findPtr <= 0) {
                throw CairoException.critical(ff.errno()).put("could not list sequencer txnlog parts [path=").put(rootPath).put(']');
            }
            partsToFlush.clear();
            try {
                do {
                    // A file system that reports no entry type (DT_UNKNOWN) lists the parts as unknown. Only part
                    // files bear a part number, so the name alone tells.
                    final int type = ff.findType(findPtr);
                    if ((type == Files.DT_FILE || type == Files.DT_UNKNOWN) && isPartFileName(partName.of(ff.findName(findPtr)))) {
                        try {
                            final long part = Numbers.parseLong(partName);
                            if (part >= fromPart && part <= toPart) {
                                partsToFlush.add(part);
                            }
                        } catch (NumericException e) {
                            // not a part file
                        }
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
            partsToFlush.sort();
            final int partsDirLen = rootPath.size();
            for (int i = partsToFlush.size() - 1; i >= 0; i--) {
                rootPath.trimTo(partsDirLen).slash().put(partsToFlush.getQuick(i));
                final long fd = ff.openRWExisting(rootPath.$());
                if (fd < 0) {
                    final int errno = ff.errno();
                    // The WAL purge removed the part since the listing: there is nothing left to flush. The errno
                    // alone does not prove it, one left over from an earlier call would skip the flush.
                    if (Files.isErrnoFileDoesNotExist(errno) && !ff.exists(rootPath.$())) {
                        continue;
                    }
                    throw CairoException.critical(errno).put("could not open read-write [file=").put(rootPath).put(']');
                }
                try {
                    ff.fdatasync(fd);
                } finally {
                    ff.close(fd);
                }
            }
        } finally {
            rootPath.trimTo(size);
        }
    }

    // The last txn before the first record from scanFrom on (0 scans none), never below the last part, that fails its
    // check, or publishedTxn when none fails.
    private long findWholeTail(long publishedTxn, long scanFrom) {
        if (scanFrom > 0) {
            final long from = Math.max(scanFrom, lastPartFirstTxn(publishedTxn, partTransactionCount));
            if (from <= publishedTxn) {
                openTxnPart(publishedTxn - 1);
                for (long txn = from; txn <= publishedTxn; txn++) {
                    if (failsCheck((txn - 1) % partTransactionCount)) {
                        return txn - 1;
                    }
                }
            }
        }
        return publishedTxn;
    }

    /**
     * True under ADAPTIVE with a group window above zero, where the batched flush takes over the per-commit
     * sequencer fdatasync. Local commits keep the configured grade, including on a replica, so concurrent
     * WAL writers of a table agree.
     */
    private boolean isDeviceFlushDeferred() {
        return configuration.getCommitMode() == CommitMode.ADAPTIVE && configuration.getAdaptiveCommitGroupWindowUs() > 0;
    }

    /**
     * Moves the field past the records a completed flush of the part and the header covered. A field that has not
     * started stays 0, and a replica never moves it.
     */
    private void markFlushed(long flushedTxn) {
        // A negative field is not moved: only the open that flushes the kept parts makes it positive again.
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
                // A local append switching parts keeps the configured grade, including on a replica.
                if (txnPartMem.isOpen()
                        && CommitMode.isPowerLossDurable(configuration.getCommitMode())
                        && partId * partTransactionCount < maxTxn.get()) {
                    // Every later flush reaches only the open part, so the part being left would never be flushed.
                    // Its records may be pending, written under a group window or by a since-closed instance that
                    // ran in another mode, and the records after them become durable. A part past the tail holds
                    // none. Under ADAPTIVE this is the group window's deferred flush of the part, whose records
                    // were never msynced (see sync0()); SYNC msynced them with MS_SYNC on every commit.
                    if (configuration.getCommitMode() == CommitMode.ADAPTIVE) {
                        txnPartMem.flushFile(false);
                    } else {
                        ff.fdatasync(txnPartMem.getFd());
                    }
                }
                txnPartMem.close(false);
                txnPartMem.of(ff, rootPath.$(), partSize, partSize, MemoryTag.MMAP_TX_LOG);
                txnPartMem.jumpTo((txn % partTransactionCount) * RECORD_SIZE);
                rootPath.trimTo(size);
                walDirectoryPolicy.initSequencerPart(rootPath, part);
                partId = part;
                // A part past the published records may have just been created, and the flushes of its records reach
                // its content, never its entry in the parts directory. The header, not maxTxn, counts the records: an
                // open reads parts before it loads maxTxn.
                if (part * partTransactionCount >= txnMem.getLong(MAX_TXN_OFFSET_64)
                        && CommitMode.isPowerLossDurable(configuration.getCommitMode())) {
                    TableUtils.fsyncDirDurable(ff, rootPath.concat(TXNLOG_PARTS_DIR).$());
                }
            } finally {
                rootPath.trimTo(size);
            }
        }
    }

    private void openTxnPart() {
        openTxnPart(this.maxTxn.get());
    }

    /**
     * Drops a torn tail left by a power loss, flushes the parts a run in another mode left, and returns the txn the
     * log ends at. Under ADAPTIVE with a group window the part and the header reach the disk together only at the
     * batched flush, and a power loss before it can leave the header counting records the part does not hold, or
     * holds only in part. The records from the field (or its magnitude after a run in another mode) were scanned,
     * never below the last part, and {@code wholeTxn} is the last one before the first that fails its check. The log
     * is cut there, but never below the table's committed txn, {@code tableTxn[0]}, and not at all when
     * {@code tableTxn} is null, not known: those records stay as they are, as the apply never reads them again, and
     * a cut would hand their txns out twice. Its saved lag txns are not kept, as every writer open applies them again
     * from the log. Records below the field were covered by a
     * completed flush and are never cut: a damaged one is reported by the
     * cursor. Earlier parts are whole within ADAPTIVE runs: a part is flushed before the next one opens
     * (openTxnPart), and at every append without a window. The parts another mode's run wrote are flushed here,
     * from {@code flushFrom}'s part to the tail's (0 flushes nothing), before the field is stored positive again. A
     * cut zeroes everything past the new tail and makes that durable before the header, so a record later written
     * over it tears over zeros and the header never counts a dropped record again. The header claims the log only
     * after every part flush returned, so a power loss before that leaves the field as it was and the next open does
     * the same work again. Reads the records since the last completed flush, at most one part.
     */
    private long recoverTail(Path path, long publishedTxn, long wholeTxn, long[] tableTxn, long flushFrom) {
        final long lastPartFirstTxn = lastPartFirstTxn(publishedTxn, partTransactionCount);
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
            }
        }
        final boolean isCut = tail != publishedTxn;
        if (isCut) {
            LOG.info().$("dropping torn sequencer txnlog tail [path=").$(path)
                    .$(", publishedTxn=").$(publishedTxn)
                    .$(", keptTxn=").$(tail)
                    .I$();
            final long cutOffset = (tail - lastPartFirstTxn + 1) * RECORD_SIZE;
            Vect.memset(txnPartMem.addressOf(cutOffset), (long) partTransactionCount * RECORD_SIZE - cutOffset, 0);
            txnPartMem.flushFile(false);
            lowerMaxTxn(txnMem, tail);
        }
        if (isCut || flushFrom > 0) {
            if (flushFrom > 0) {
                fdatasyncParts(Math.max(flushFrom - 1, 0) / partTransactionCount, Math.max(tail - 1, 0) / partTransactionCount);
            }
            // Every record up to the tail is on the device: the field claims the log from above it, and the slot
            // follows it, stored last behind a fence so that a header caught in between shows the slot above the max
            // txn, never below.
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, tail + 1);
            Unsafe.storeFence();
            txnMem.putLong(KNOWN_MAX_TXN_OFFSET_64, tail);
            txnMem.flushFile(false);
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
        // Only a field of 0 starts: a negative one is made positive by the open that flushes the kept parts, never
        // by an append.
        if (configuration.getCommitMode() == CommitMode.ADAPTIVE
                && txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64) == 0
                && durabilityPolicy.isLocalDurabilityEnabled()) {
            txnMem.putLong(UNSYNCED_FROM_TXN_OFFSET_64, maxTxn.get() + 1);
        }
    }

    /**
     * Stores the max txn about to be published in the slot, while the field is set. The caller fences before it
     * stores the max txn, so the slot is never below a max txn this binary published.
     */
    private void storeKnownMaxTxn(long txn) {
        // With either sign: through a run in another mode the slot keeps following the max txn, so an older
        // binary's later appends are still seen above it.
        if (txnMem.getLong(UNSYNCED_FROM_TXN_OFFSET_64) != 0) {
            txnMem.putLong(KNOWN_MAX_TXN_OFFSET_64, txn);
        }
    }

    private void sync0() {
        // Local Java commits keep the configured grade; a replica does not upload them.
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            // The part file must be durable before the header that points to it, or a crash after the header
            // sync can leave maxTxn=N pointing at a zeroed or partial record. ASYNC promises nothing across a
            // power loss, and its MS_ASYNC orders nothing, so it syncs the header alone.
            //
            // With a group window, ADAPTIVE takes an async sync, which writes nothing back, and leaves the
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
                txnPartMem.flushFile(false);
            }
            pastTailZeroedPartId = partId;
        }
    }

    @Override
    public void fdatasyncTxnLog() {
        // The batched device flush of adaptive group commit, part file before the header as in sync0(). The caller
        // holds the sequencer write lock, so no record is half-written up to this tail.
        // flushFile(): the records are written through the mapping and, under a group window, never msynced.
        final long flushedTxn = maxTxn.get();
        if (txnPartMem.isOpen()) {
            txnPartMem.flushFile(false);
        }
        if (txnMem.isOpen()) {
            txnMem.flushFile(false);
            markFlushed(flushedTxn);
        }
    }

    @Override
    public void fdatasyncTxnLog(long fromTxn) {
        // The parts below the open one that hold txns from fromTxn on, as far as the log still keeps them, then the
        // open part and the header.
        fdatasyncParts(Math.max(fromTxn - 1, 0) / partTransactionCount, partId - 1);
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
