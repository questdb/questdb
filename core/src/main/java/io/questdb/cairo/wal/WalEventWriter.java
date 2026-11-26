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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.AtomicIntList;
import io.questdb.std.BoolList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.wal.WalUtils.*;

class WalEventWriter implements Closeable {
    private final CairoConfiguration configuration;
    private final MemoryMARW eventIndexMem = Vm.getCMARWInstance();
    private final MemoryMARW eventMem = Vm.getCMARWInstance();
    private final FilesFacade ff;
    private final WalSqlEncoder sqlEncoder = new WalSqlEncoder();
    // A record was appended since the files were last flushed at the grade a power-loss-durable mode needs.
    private boolean hasUnflushedRecords;
    private AtomicIntList initialSymbolCounts;
    // used to test older mat view format
    private boolean legacyMatViewFormat;
    private long startOffset = 0;
    private BoolList symbolMapNullFlags;
    private BoolList symbolMapNullFlagsChanged;
    private int txn = 0;
    private ObjList<DirectCharSequenceIntHashMap> txnSymbolMaps;

    WalEventWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @Override
    public void close() {
        close(true, Vm.TRUNCATE_TO_POINTER);
    }

    public void close(boolean truncate, byte truncateMode) {
        // Close both memories even if the first throws, so a throw from eventMem cannot leak eventIndexMem's mapping.
        try {
            eventMem.close(truncate, truncateMode);
        } finally {
            eventIndexMem.close(truncate, truncateMode);
        }
    }

    @TestOnly
    public void setLegacyMatViewFormat(boolean legacyMatViewFormat) {
        this.legacyMatViewFormat = legacyMatViewFormat;
    }

    /**
     * Size in bytes consumed by the events file, including any symbols.
     */
    public long size() {
        return eventMem.getAppendOffset();
    }

    private void appendIndex(long value) {
        eventIndexMem.putLong(value);
        // Every record ends with its index entry, so this marks every record type, present and future.
        hasUnflushedRecords = true;
    }

    private void init() {
        eventMem.putInt(0);
        eventMem.putInt(WALE_FORMAT_VERSION);
        eventMem.putInt(-1);

        appendIndex(WALE_HEADER_SIZE);
        txn = 0;
    }

    private void writeSymbolMapDiffs() {
        final int columns = txnSymbolMaps.size();
        for (int columnIndex = 0; columnIndex < columns; columnIndex++) {
            final var symbolMap = txnSymbolMaps.getQuick(columnIndex);
            if (symbolMap != null) {
                final int initialCount = initialSymbolCounts.get(columnIndex);
                if (initialCount > 0 || (initialCount == 0 && symbolMap.size() > 0) || symbolMapNullFlagsChanged.get(columnIndex)) {
                    eventMem.putInt(columnIndex);
                    eventMem.putBool(symbolMapNullFlags.get(columnIndex));
                    eventMem.putInt(initialCount);

                    final int size = symbolMap.size();
                    long appendAddress = eventMem.getAppendOffset();
                    eventMem.putInt(size);

                    int symbolCount = symbolMap.copyTo(eventMem, initialCount);
                    // Update the size with the exact symbolCount
                    // An empty SymbolMapDiff can be created because symbolCount can be 0
                    // in case all cached symbols come from symbolMapReader.
                    // Alternatively, two-pass approach can be used.
                    eventMem.putInt(appendAddress, symbolCount);
                    eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES);
                }
            }
        }
        eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS);
    }

    int appendCustomEvent(byte txnType, WalEventPayloadWriter payload) {
        assert WalTxnType.isDownstreamType(txnType) : "custom event types must be in reserved range 64..127, got: " + txnType;
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(txnType);
        payload.write(eventMem);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }

    /**
     * Append data to the WAL. This method is used for both regular and materialized view data.
     * The method takes various parameters to specify the data range, timestamps, and other options.
     *
     * @param startRowID           the starting row ID of the data in the segment.
     * @param endRowID             the ending row ID of the data  in the segment.
     * @param minTimestamp         the minimum timestamp of the data, inclusive
     * @param maxTimestamp         the maximum timestamp of the data, inclusive
     * @param outOfOrder           indicates if the data is out of order
     * @param lastRefreshBaseTxn   seqTxn of base transaction ID when refresh is performed
     * @param lastRefreshTimestamp wall clock mat view refresh timestamp
     * @param lastPeriodHi         the high timestamp for the last mat view period refresh
     * @param replaceRangeLowTs    the low timestamp for the range to be replaced, inclusive
     * @param replaceRangeHiTs     the high timestamp for the range to be replaced, exclusive
     * @param dedupMode            deduplication mode, can be DEFAULT, NO_DEDUP, UPSERT_NEW or REPLACE_RANGE.
     */
    int appendData(
            byte txnType,
            long startRowID,
            long endRowID,
            long minTimestamp,
            long maxTimestamp,
            boolean outOfOrder,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            long lastPeriodHi,
            long replaceRangeLowTs,
            long replaceRangeHiTs,
            byte dedupMode
    ) {
        assert txnType == WalTxnType.MAT_VIEW_DATA || txnType == WalTxnType.DATA || txnType == WalTxnType.LIVE_VIEW_DATA
                : "unexpected txn type: " + txnType;
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(txnType);
        eventMem.putLong(startRowID);
        eventMem.putLong(endRowID);
        eventMem.putLong(minTimestamp);
        eventMem.putLong(maxTimestamp);
        eventMem.putBool(outOfOrder);
        if (txnType == WalTxnType.MAT_VIEW_DATA) {
            assert lastRefreshBaseTxn != Numbers.LONG_NULL;
            eventMem.putLong(lastRefreshBaseTxn);
            eventMem.putLong(lastRefreshTimestamp);
        } else if (txnType == WalTxnType.LIVE_VIEW_DATA) {
            // Reuses the lastRefreshBaseTxn slot to carry the live view's
            // maxBaseSeqTxnInBlock (highest base sequencer txn this commit reflects).
            assert lastRefreshBaseTxn != Numbers.LONG_NULL;
            eventMem.putLong(lastRefreshBaseTxn);
        }

        if (dedupMode == WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE) {
            if (replaceRangeLowTs >= replaceRangeHiTs) {
                throw CairoException.nonCritical().put("Replace range low timestamp must be less than replace range high timestamp.");
            }
            if (replaceRangeLowTs > minTimestamp) {
                throw CairoException.nonCritical().put("Replace range low timestamp must be less than or equal to the minimum timestamp.");
            }
            if (replaceRangeHiTs <= maxTimestamp) {
                throw CairoException.nonCritical().put("Replace range high timestamp must be greater than the maximum timestamp.");
            }
        }

        writeSymbolMapDiffs();

        // To test backwards compatibility and ensure that we still can read WALs
        // written by the old QuestDB version, make the dedup mode
        // and replace range value optional. It will then not be written for the most transactions,
        // and it will test the reading WAL-E code to read the old format.
        if (dedupMode != WAL_DEDUP_MODE_DEFAULT) {
            // Always write extra 8 bytes, even for normal tables, to simplify reading the extra footer.
            if (txnType == WalTxnType.MAT_VIEW_DATA) {
                eventMem.putLong(lastPeriodHi);
            } else {
                eventMem.putLong(Numbers.LONG_NULL);
            }
            eventMem.putLong(replaceRangeLowTs);
            eventMem.putLong(replaceRangeHiTs);
            eventMem.putByte(dedupMode);
        }
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        if (txnType == WalTxnType.MAT_VIEW_DATA) {
            eventMem.putInt(WAL_FORMAT_OFFSET_32, WALE_MAT_VIEW_FORMAT_VERSION);
        } else if (txnType == WalTxnType.LIVE_VIEW_DATA) {
            eventMem.putInt(WAL_FORMAT_OFFSET_32, WALE_LIVE_VIEW_FORMAT_VERSION);
        }
        return txn++;
    }

    int appendDeltaRun(long minTimestamp, long maxTimestamp, long rowCount, WalEventPayloadWriter symbols) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.DELTA_RUN_WRITE);
        eventMem.putLong(0);
        eventMem.putLong(rowCount);
        eventMem.putLong(minTimestamp);
        eventMem.putLong(maxTimestamp);
        eventMem.putBool(false);
        symbols.write(eventMem);
        eventMem.putInt(SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);
        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }


    int appendMatViewInvalidate(
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.MAT_VIEW_INVALIDATE);
        eventMem.putLong(lastRefreshBaseTxn);
        eventMem.putLong(lastRefreshTimestamp);
        eventMem.putBool(invalid);
        eventMem.putStr(invalidationReason);
        if (!legacyMatViewFormat) {
            eventMem.putLong(lastPeriodHi);
            eventMem.putLong(refreshIntervalsBaseTxn);
            if (refreshIntervals != null) {
                eventMem.putInt(refreshIntervals.size());
                for (int i = 0, n = refreshIntervals.size(); i < n; i++) {
                    eventMem.putLong(refreshIntervals.getQuick(i));
                }
            } else {
                eventMem.putInt(-1);
            }
        }
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        eventMem.putInt(WAL_FORMAT_OFFSET_32, WALE_MAT_VIEW_FORMAT_VERSION);
        return txn++;
    }

    int appendSql(int cmdType, CharSequence sqlText, SqlExecutionContext sqlExecutionContext) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.SQL);
        sqlEncoder.encode(eventMem, cmdType, sqlText, sqlExecutionContext);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }

    int appendViewDefinition(
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies
    ) {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.VIEW_DEFINITION);

        // add view sql
        eventMem.putStr(viewSql);

        //add view dependencies
        final ObjList<CharSequence> tableNames = dependencies.keys();
        final int numOfDependencies = tableNames.size();
        eventMem.putInt(numOfDependencies);
        for (int i = 0; i < numOfDependencies; i++) {
            final CharSequence tableName = tableNames.getQuick(i);
            eventMem.putStr(tableName);
            final LowerCaseCharSequenceHashSet columns = dependencies.get(tableName);
            eventMem.putInt(columns.size());
            for (int j = 0; j < columns.getKeyCount(); j++) {
                final CharSequence key = columns.getKey(j);
                if (key != null) {
                    eventMem.putStr(key);
                }
            }
        }

        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }

    void of(ObjList<DirectCharSequenceIntHashMap> txnSymbolMaps, AtomicIntList initialSymbolCounts, BoolList symbolMapNullFlags, BoolList symbolMapNullFlagsChanged) {
        this.txnSymbolMaps = txnSymbolMaps;
        this.initialSymbolCounts = initialSymbolCounts;
        this.symbolMapNullFlags = symbolMapNullFlags;
        this.symbolMapNullFlagsChanged = symbolMapNullFlagsChanged;
    }

    void openEventFile(Path path, int pathLen, boolean truncate, boolean systemTable) {
        if (eventMem.getFd() > -1) {
            close(truncate, Vm.TRUNCATE_TO_POINTER);
        }
        final long appendPageSize = systemTable
                ? configuration.getSystemWalEventAppendPageSize()
                : configuration.getWalEventAppendPageSize();
        eventMem.of(
                ff,
                path.trimTo(pathLen).concat(EVENT_FILE_NAME).$(),
                appendPageSize,
                -1,
                MemoryTag.NATIVE_TABLE_WAL_WRITER,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_RANDOM
        );
        eventIndexMem.of(
                ff,
                path.trimTo(pathLen).concat(EVENT_INDEX_FILE_NAME).$(),
                Math.max(ff.getPageSize(), appendPageSize / 4),
                -1,
                MemoryTag.NATIVE_TABLE_WAL_WRITER,
                CairoConfiguration.O_NONE,
                Files.POSIX_MADV_SEQUENTIAL
        );
        init();
    }

    void rollback() {
        eventMem.putInt(startOffset, -1);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, --txn - 1);
        // Do not truncate files, these files may be read by WAL Apply job at the moment.
        // This is very rare case, WALE will not be written anymore after this call.
        // Not truncating the files saves from reading complexity.
    }

    void sync() {
        // Local WAL events keep the configured grade; a replica does not upload them.
        sync(configuration.getCommitMode());
    }

    /**
     * Flush the WAL events files at the given commit mode's grade, with an fdatasync barrier for ADAPTIVE
     * at W=0. The data commit path (WalWriter.syncIfRequired0) does not call it under ADAPTIVE: it flushes
     * the files itself through {@link #barrierFsync()}. The other callers pass the mode they act on.
     */
    void sync(int commitMode) {
        if (commitMode != CommitMode.NOSYNC) {
            // With a group window only the shared sequencer flush is batched (an async sync here, which writes
            // nothing back, and barrierFsync() in syncBeforeSequencing); ADAPTIVE with no window takes MS_SYNC
            // plus the barrier below.
            final boolean deferDeviceFlush = commitMode == CommitMode.ADAPTIVE
                    && configuration.getAdaptiveCommitGroupWindowUs() > 0;
            final boolean async = commitMode == CommitMode.ASYNC || deferDeviceFlush;
            eventMem.sync(async);
            eventIndexMem.sync(async);
            // The barrier puts the events data and the inode size on the medium before the sequencer record,
            // in the order data, events, sequencer. Only ordering is needed here: the sequencer flush at the
            // commit point is the full device flush. MS_SYNC above has written the mapped pages into the files,
            // so the barrier alone follows it, not barrierFsync()'s flushFile().
            if (commitMode == CommitMode.ADAPTIVE && !deferDeviceFlush) {
                if (eventMem.isOpen()) {
                    ff.barrierFsync(eventMem.getFd());
                }
                if (eventIndexMem.isOpen()) {
                    ff.barrierFsync(eventIndexMem.getFd());
                }
                hasUnflushedRecords = false;
            } else if (commitMode == CommitMode.SYNC) {
                // MS_SYNC is a range fsync: the records are on the device.
                hasUnflushedRecords = false;
            }
        }
    }

    /**
     * Orders the WAL event and index files ahead of the sequencer record (see
     * {@link io.questdb.std.Files#barrierFsync(long)}). It is an ordering barrier, not a durability barrier:
     * the sequencer flush at the commit point is the full device-cache flush that makes everything ordered
     * before it durable. For records not msynced with MS_SYNC: off Linux it first writes the mapped pages into
     * the files (see {@link io.questdb.cairo.vm.api.MemoryMA#flushFile(boolean)}).
     */
    void barrierFsync() {
        if (eventMem.isOpen()) {
            eventMem.flushFile(true);
        }
        if (eventIndexMem.isOpen()) {
            eventIndexMem.flushFile(true);
        }
        hasUnflushedRecords = false;
    }

    /**
     * Makes every record appended since the last flush durable before the sequencer names it, at the grade the
     * mode promises across a power loss, and does nothing when the caller's path has flushed already. Each WAL
     * writer calls it right before it sequences, so a path that appends a record without its own flush cannot
     * get a sequencer record ahead of its _event entry.
     */
    void syncBeforeSequencing(int commitMode) {
        if (hasUnflushedRecords && CommitMode.isPowerLossDurable(commitMode)) {
            sync(commitMode);
            if (hasUnflushedRecords) {
                // A group window leaves sync() at an async sync, which writes nothing back.
                barrierFsync();
            }
        }
    }

    /**
     * Advisory writeback drain, the wait: waits for the writeback {@link #syncFlushKick()} started. No device
     * flush, no metadata journalled: the caller's per-file {@code fdatasync} is still the barrier.
     */
    void syncFlushDrain() {
        eventMem.syncFlushDrain();
        eventIndexMem.syncFlushDrain();
    }

    /**
     * Advisory writeback drain, the start: starts writeback of both files and returns without waiting.
     */
    void syncFlushKick() {
        eventMem.syncFlushKick();
        eventIndexMem.syncFlushKick();
    }

    /**
     * Rewrites the last data record in the event file. This is used when a symbol
     * column is added after the event was already written (e.g., during segment roll
     * in addColumn). The method jumps back to the start of the last event, resets
     * the index entry, and rewrites the event so that the updated symbolMapNullFlags
     * are included in the symbol map diffs.
     * <p>
     * The rewrite is durable when this returns, in SYNC and ADAPTIVE. The record must not be sequenced yet:
     * the caller sequences it afterwards.
     */
    int rewriteLastDataRecord(
            byte txnType,
            long startRowID,
            long endRowID,
            long minTimestamp,
            long maxTimestamp,
            boolean outOfOrder,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            long lastPeriodHi,
            long replaceRangeLowTs,
            long replaceRangeHiTs,
            byte dedupMode
    ) {
        // Jump back to the start of the last event and write the -1 sentinel
        // so that appendData finds it at the expected position.
        // NB: if appendData() throws, the event file is left in a partially
        // rewritten state. This is acceptable because the exception will
        // close the WalWriter and the segment will not be used for writing anymore
        eventMem.jumpTo(startOffset);
        eventMem.putInt(-1);

        // Remove the last index entry (one long) so appendData can re-add it.
        eventIndexMem.jumpTo(eventIndexMem.getAppendOffset() - Long.BYTES);

        // Decrement txn because appendData will increment it.
        txn--;

        final int segmentTxn = appendData(
                txnType, startRowID, endRowID,
                minTimestamp, maxTimestamp, outOfOrder,
                lastRefreshBaseTxn, lastRefreshTimestamp, lastPeriodHi,
                replaceRangeLowTs, replaceRangeHiTs, dedupMode
        );
        // Flush the replaced record before the caller sequences it: the record and its index entry are
        // replaced in place in two files the kernel writes back independently, and nothing names the record
        // until it is sequenced, so flushing both here in any order is enough.
        // Local WAL events keep the configured grade; a replica does not upload them.
        if (CommitMode.isPowerLossDurable(configuration.getCommitMode())) {
            barrierFsync();
        }
        return segmentTxn;
    }

    int truncate() {
        startOffset = eventMem.getAppendOffset() - Integer.BYTES;
        eventMem.putLong(txn);
        eventMem.putByte(WalTxnType.TRUNCATE);
        eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
        eventMem.putInt(-1);

        appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
        eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
        return txn++;
    }
}
