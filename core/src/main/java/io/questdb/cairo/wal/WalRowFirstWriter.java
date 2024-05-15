/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

/**
 * WAL writer with row-first storage format. All columns are written into a single file.
 */
public class WalRowFirstWriter implements WalWriter {
    public static final int NEW_ROW_SEPARATOR = -1;
    private static final Log LOG = LogFactory.getLog(WalRowFirstWriter.class);
    private static final int MEM_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;
    private final AlterOperation alterOp = new AlterOperation();
    private final CairoConfiguration configuration;
    private final DdlListener ddlListener;
    private final WalEventWriter eventWriter;
    private final FilesFacade ff;
    private final AtomicIntList initialSymbolCounts;
    private final IntList localSymbolIds;
    private final MetadataValidatorService metaValidatorSvc = new MetadataValidatorService();
    private final MetadataService metaWriterSvc = new MetadataWriterService();
    private final WalWriterMetadata metadata;
    private final Metrics metrics;
    private final int mkDirMode;
    private final Path path;
    private final int rootLen;
    private final RowImpl row = new RowImpl();
    private final MemoryMA rowMem;
    private final TableSequencerAPI sequencer;
    private final MemoryMAR symbolMapMem;
    private final BoolList symbolMapNullFlags = new BoolList();
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final ObjList<CharSequenceIntHashMap> symbolMaps = new ObjList<>();
    private final int timestampIndex;
    private final ObjList<Utf8StringIntHashMap> utf8SymbolMaps = new ObjList<>();
    private final Uuid uuid = new Uuid();
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final int walId;
    private final String walName;
    private int columnCount;
    private ColumnVersionReader columnVersionReader;
    private long currentTxnOffset = -1;
    private long currentTxnStartRowNum = -1;
    private boolean distressed;
    private boolean isCommittingData;
    private int lastSegmentTxn = -1;
    private long lastSeqTxn = NO_TXN;
    private boolean open;
    private boolean rollSegmentOnNextRow = false;
    private int segmentId = -1;
    private int segmentLockFd = -1;
    private long segmentRowCount = -1;
    private TableToken tableToken;
    private TxReader txReader;
    private long txnMaxTimestamp = -1;
    private long txnMinTimestamp = Long.MAX_VALUE;
    private boolean txnOutOfOrder = false;
    private int walLockFd = -1;

    public WalRowFirstWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            DdlListener ddlListener,
            WalDirectoryPolicy walDirectoryPolicy,
            Metrics metrics
    ) {
        LOG.info().$("open '").utf8(tableToken.getDirName()).$('\'').$();
        this.sequencer = tableSequencerAPI;
        this.configuration = configuration;
        this.ddlListener = ddlListener;
        this.mkDirMode = configuration.getMkDirMode();
        this.ff = configuration.getFilesFacade();
        this.walDirectoryPolicy = walDirectoryPolicy;
        this.tableToken = tableToken;
        final int walId = tableSequencerAPI.getNextWalId(tableToken);
        this.walName = WAL_NAME_BASE + walId;
        this.walId = walId;
        this.path = new Path().of(configuration.getRoot()).concat(tableToken).concat(walName);
        this.rootLen = path.size();
        this.metrics = metrics;
        this.open = true;
        this.symbolMapMem = Vm.getMARInstance(configuration.getCommitMode());
        this.rowMem = Vm.getMAInstance(configuration.getCommitMode());

        try {
            lockWal();
            mkWalDir();

            metadata = new WalWriterMetadata(ff);

            tableSequencerAPI.getTableMetadata(tableToken, metadata);
            this.tableToken = metadata.getTableToken();

            columnCount = metadata.getColumnCount();
            timestampIndex = metadata.getTimestampIndex();
            initialSymbolCounts = new AtomicIntList(columnCount);
            localSymbolIds = new IntList(columnCount);

            eventWriter = new WalEventWriter(configuration);
            eventWriter.of(symbolMaps, initialSymbolCounts, symbolMapNullFlags);

            openNewSegment();
            configureSymbolTable();
        } catch (Throwable e) {
            doClose(false);
            throw e;
        }
    }

    @Override
    public void addColumn(@NotNull CharSequence columnName, int columnType, SecurityContext securityContext) {
        addColumn(
                columnName,
                columnType,
                configuration.getDefaultSymbolCapacity(),
                configuration.getDefaultSymbolCacheFlag(),
                false,
                configuration.getIndexValueBlockSize(),
                false,
                securityContext
        );
    }

    @Override
    public void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isDedupKey
    ) {
        addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                isDedupKey,
                null
        );
    }

    @Override
    public long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
        try {
            if (alterOp.isStructural()) {
                return applyStructural(alterOp);
            } else {
                return applyNonStructural(alterOp, false);
            }
        } finally {
            alterOp.clearSecurityContext();
        }
    }

    // Returns table transaction number
    @Override
    public long apply(UpdateOperation operation) {
        if (inTransaction()) {
            throw CairoException.critical(0).put("cannot update table with uncommitted inserts [table=")
                    .put(tableToken.getTableName()).put(']');
        }

        // it is guaranteed that there is no join in UPDATE statement
        // because SqlCompiler rejects the UPDATE if it contains join
        return applyNonStructural(operation, true);

        // when join is allowed in UPDATE we have 2 options
        // 1. we could write the updated partitions into WAL.
        //   since we cannot really rely on row ids we should probably create
        //   a PARTITION_REWRITE event and use it here to replace the updated
        //   partitions entirely with new ones.
        // 2. we could still pass the SQL statement if we made sure that all
        //   tables involved in the join are guaranteed to be on the same
        //   version (exact same txn number) on each node when the update
        //   statement is run.
        //   so we would need to read current txn number for each table and
        //   put it into the SQL event as a requirement for running the SQL.
        //   when the WAL event is processed we would need to query the exact
        //   versions of each table involved in the join when running the SQL.
    }

    @Override
    public void close() {
        if (isOpen()) {
            try {
                // If distressed, no need to rollback, WalWriter will not be used anymore
                if (!distressed) {
                    rollback();
                }
            } finally {
                doClose(walDirectoryPolicy.truncateFilesOnClose());
            }
        }
    }

    // Returns sequencer transaction number
    @Override
    public long commit() {
        checkDistressed();
        try {
            if (inTransaction()) {
                isCommittingData = true;
                final long rowsToCommit = getUncommittedRowCount();
                final long segmentOffset = rowMem.getAppendOffset();
                lastSegmentTxn = eventWriter.appendRowFirstData(
                        currentTxnStartRowNum,
                        segmentRowCount,
                        currentTxnOffset,
                        segmentOffset,
                        txnMinTimestamp,
                        txnMaxTimestamp,
                        txnOutOfOrder
                );
                // flush disk before getting next txn
                final int commitMode = configuration.getCommitMode();
                if (commitMode != CommitMode.NOSYNC) {
                    sync(commitMode);
                }
                final long seqTxn = getSequencerTxn();
                LOG.info().$("committed data block [wal=").$(path).$(Files.SEPARATOR).$(segmentId)
                        .$(", segmentTxn=").$(lastSegmentTxn)
                        .$(", seqTxn=").$(seqTxn)
                        .$(", rowLo=").$(currentTxnStartRowNum).$(", roHi=").$(segmentRowCount)
                        .$(", startOffset=").$(currentTxnOffset).$(", endOffset=").$(segmentOffset)
                        .$(", minTimestamp=").$ts(txnMinTimestamp).$(", maxTimestamp=").$ts(txnMaxTimestamp).I$();
                resetDataTxnProperties();
                mayRollSegmentOnNextRow();
                metrics.walMetrics().addRowsWritten(rowsToCommit);
                return seqTxn;
            }
        } catch (CairoException ex) {
            distressed = true;
            throw ex;
        } catch (Throwable th) {
            // If distressed, no need to rollback, WalWriter will not be used anymore
            if (!isDistressed()) {
                rollback();
            }
            throw th;
        } finally {
            isCommittingData = false;
        }
        return NO_TXN;
    }

    public void doClose(boolean truncate) {
        if (open) {
            open = false;
            if (metadata != null) {
                metadata.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }
            if (eventWriter != null) {
                eventWriter.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }
            freeSymbolMapReaders();
            if (symbolMapMem != null) {
                symbolMapMem.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }

            rowMem.close(truncate, Vm.TRUNCATE_TO_POINTER);

            releaseSegmentLock(segmentId, segmentLockFd, segmentRowCount);

            try {
                releaseWalLock();
            } finally {
                Misc.free(path);
                LOG.info().$("closed '").utf8(tableToken.getTableName()).$('\'').$();
            }
        }
    }

    @Override
    public TableRecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public long getMetadataVersion() {
        return metadata.getMetadataVersion();
    }

    @TestOnly
    public long getSegmentRowCount() {
        return segmentRowCount;
    }

    @Override
    public int getSymbolCountWatermark(int columnIndex) {
        // It could be the case that ILP I/O thread has newer metadata version than
        // the writer, so it may be requesting a watermark for a recently added column.
        if (columnIndex > initialSymbolCounts.size() - 1) {
            return 0;
        }
        return initialSymbolCounts.get(columnIndex);
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public long getUncommittedRowCount() {
        return segmentRowCount - currentTxnStartRowNum;
    }

    public long getUncommittedSize() {
        return rowMem.getAppendOffset() - currentTxnOffset;
    }

    @Override
    public int getWalId() {
        return walId;
    }

    @Override
    public String getWalName() {
        return walName;
    }

    @Override
    public void goActive() {
        goActive(Long.MAX_VALUE);
    }

    @Override
    public boolean goActive(long maxStructureVersion) {
        try {
            applyMetadataChangeLog(maxStructureVersion);
            return true;
        } catch (CairoException e) {
            LOG.critical().$("could not apply structure changes, WAL will be closed [table=").$(tableToken.getTableName())
                    .$(", walId=").$(walId)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$((Throwable) e).I$();
            distressed = true;
            return false;
        }
    }

    @Override
    public void ic() {
        commit();
    }

    @Override
    public void ic(long o3MaxLag) {
        commit();
    }

    public boolean inTransaction() {
        return segmentRowCount > currentTxnStartRowNum;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public TableWriter.Row newRow() {
        return newRow(0L);
    }

    @Override
    public TableWriter.Row newRow(long timestamp) {
        checkDistressed();
        if (timestamp < Timestamps.O3_MIN_TS) {
            throw CairoException.nonCritical().put("timestamp before 1970-01-01 is not allowed");
        }
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }

            row.init();
            if (timestampIndex != -1) {
                row.setTimestamp(timestamp);
            }
            return row;
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    @Override
    public TableWriter.Row newRowDeferTimestamp() {
        checkDistressed();
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }
            return row;
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    @Override
    public long renameTable(@NotNull CharSequence oldName, String newTableName) {
        if (!Chars.equalsIgnoreCaseNc(oldName, tableToken.getTableName())) {
            throw CairoException.tableDoesNotExist(oldName);
        }
        alterOp.clear();
        alterOp.ofRenameTable(tableToken, newTableName);
        long txn = apply(alterOp, true);
        assert Chars.equals(newTableName, tableToken.getTableName());
        return txn;
    }

    public void rollSegment() {
        try {
            openNewSegment();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    public void rollUncommittedToNewSegment() {
        final long uncommittedRows = getUncommittedRowCount();
        final long oldSegmentRowCount = segmentRowCount;

        if (uncommittedRows > 0) {
            final long uncommittedSize = getUncommittedSize();
            final int oldSegmentId = segmentId;
            final int newSegmentId = segmentId + 1;
            if (newSegmentId > WalUtils.SEG_MAX_ID) {
                throw CairoException.critical(0)
                        .put("cannot roll over to new segment due to SEG_MAX_ID overflow ")
                        .put("[table=").put(tableToken.getTableName())
                        .put(", walId=").put(walId)
                        .put(", segmentId=").put(newSegmentId).put(']');
            }
            final int oldSegmentLockFd = segmentLockFd;
            segmentLockFd = -1;
            try {
                createSegmentDir(newSegmentId);
                path.trimTo(rootLen);

                LOG.info().$("rolling uncommitted rows to new segment [wal=")
                        .$(path).$(Files.SEPARATOR).$(oldSegmentId)
                        .$(", lastSegmentTxn=").$(lastSegmentTxn)
                        .$(", newSegmentId=").$(newSegmentId)
                        .$(", uncommittedRows=").$(uncommittedRows)
                        .$(", uncommittedOffset=").$(currentTxnOffset)
                        .$(", uncommittedSize=").$(uncommittedSize)
                        .I$();

                final int commitMode = configuration.getCommitMode();
                int fd = CopyWalSegmentUtils.rollRowFirstToSegment(
                        ff,
                        configuration.getWriterFileOpenOpts(),
                        metadata,
                        rowMem,
                        path,
                        newSegmentId,
                        currentTxnOffset,
                        uncommittedSize,
                        commitMode
                );

                switchToNewSegment(fd, currentTxnOffset, uncommittedSize);
                rollLastWalEventRecord(newSegmentId, uncommittedRows, uncommittedSize);
                segmentId = newSegmentId;
                segmentRowCount = uncommittedRows;
                currentTxnStartRowNum = 0;
                currentTxnOffset = 0;
            } finally {
                releaseSegmentLock(oldSegmentId, oldSegmentLockFd, oldSegmentRowCount);
            }
        } else if (segmentRowCount > 0 && uncommittedRows == 0) {
            rollSegmentOnNextRow = true;
        }
    }

    @Override
    public void rollback() {
        try {
            if (!isDistressed() && inTransaction()) {
                setAppendPosition(currentTxnOffset);
                segmentRowCount = currentTxnStartRowNum;
                txnMinTimestamp = Long.MAX_VALUE;
                txnMaxTimestamp = -1;
                txnOutOfOrder = false;
            }
        } catch (Throwable th) {
            // Set to dissatisfied state, otherwise the pool will keep trying to rollback until the stack overflow
            distressed = true;
            throw th;
        }
    }

    @Override
    public boolean supportsMultipleWriters() {
        return true;
    }

    @Override
    public String toString() {
        return "WalRowFirstWriter{" +
                "name=" + walName +
                ", table=" + tableToken.getTableName() +
                '}';
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException("cannot truncate symbol tables on WAL table");
    }

    @Override
    public void truncateSoft() {
        try {
            lastSegmentTxn = eventWriter.truncate();
            getSequencerTxn();
        } catch (Throwable th) {
            rollback();
            throw th;
        }
    }

    private int acquireSegmentLock() {
        final int segmentPathLen = path.size();
        try {
            lockName(path);
            final int segmentLockFd = TableUtils.lock(ff, path);
            if (segmentLockFd == -1) {
                path.trimTo(segmentPathLen);
                throw CairoException.critical(ff.errno()).put("Cannot lock wal segment: ").put(path.$());
            }
            return segmentLockFd;
        } finally {
            path.trimTo(segmentPathLen);
        }
    }

    private void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isDedupKey,
            SecurityContext securityContext
    ) {
        alterOp.clear();
        alterOp.ofAddColumn(
                getMetadata().getTableId(),
                tableToken,
                0,
                columnName,
                0,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                isDedupKey
        );
        alterOp.withSecurityContext(securityContext);
        apply(alterOp, true);
    }

    private void applyMetadataChangeLog(long structureVersionHi) {
        try (TableMetadataChangeLog log = sequencer.getMetadataChangeLog(tableToken, getColumnStructureVersion())) {
            long structVersion = getColumnStructureVersion();
            while (log.hasNext() && structVersion < structureVersionHi) {
                TableMetadataChange chg = log.next();
                try {
                    chg.apply(metaWriterSvc, true);
                } catch (CairoException e) {
                    distressed = true;
                    throw e;
                }

                if (++structVersion != getColumnStructureVersion()) {
                    distressed = true;
                    throw CairoException.critical(0)
                            .put("could not apply table definition changes to the current transaction, version unchanged");
                }
            }
        }
    }

    private long applyNonStructural(AbstractOperation op, boolean verifyStructureVersion) {
        if (op.getSqlExecutionContext() == null) {
            throw CairoException.critical(0).put("failed to commit ALTER SQL to WAL, sql context is empty [table=").put(tableToken.getTableName()).put(']');
        }
        if ((verifyStructureVersion && op.getTableVersion() != getColumnStructureVersion())
                || op.getTableId() != metadata.getTableId()) {
            throw TableReferenceOutOfDateException.of(tableToken, metadata.getTableId(), op.getTableId(), getColumnStructureVersion(), op.getTableVersion());
        }

        try {
            lastSegmentTxn = eventWriter.appendSql(op.getCmdType(), op.getSqlText(), op.getSqlExecutionContext());
            return getSequencerTxn();
        } catch (Throwable th) {
            // perhaps half record was written to WAL-e, better to not use this WAL writer instance
            distressed = true;
            throw th;
        }
    }

    private long applyStructural(AlterOperation alterOp) {
        long txn;
        do {
            boolean retry = true;
            try {
                metaValidatorSvc.startAlterValidation();
                alterOp.apply(metaValidatorSvc, true);
                if (metaValidatorSvc.structureVersion != getColumnStructureVersion() + 1) {
                    retry = false;
                    throw CairoException.nonCritical()
                            .put("statements containing multiple transactions, such as 'alter table add column col1, col2'" +
                                    " are currently not supported for WAL tables [table=").put(tableToken.getTableName())
                            .put(", oldStructureVersion=").put(getColumnStructureVersion())
                            .put(", newStructureVersion=").put(metaValidatorSvc.structureVersion).put(']');
                }
            } catch (CairoException e) {
                if (retry) {
                    // Table schema (metadata) changed and this Alter is not valid anymore.
                    // Try to update WAL metadata to latest and repeat one more time.
                    goActive();
                    alterOp.apply(metaValidatorSvc, true);
                } else {
                    throw e;
                }
            }

            try {
                txn = sequencer.nextStructureTxn(tableToken, getColumnStructureVersion(), alterOp);
                if (txn == NO_TXN) {
                    applyMetadataChangeLog(Long.MAX_VALUE);
                }
            } catch (CairoException e) {
                distressed = true;
                throw e;
            }
        } while (txn == NO_TXN);

        // Apply to itself.
        try {
            alterOp.apply(metaWriterSvc, true);
        } catch (Throwable th) {
            LOG.critical().$("Exception during alter [ex=").$(th).I$();
            distressed = true;
            throw th;
        }
        return lastSeqTxn = txn;
    }

    private boolean breachedRolloverSizeThreshold() {
        final long threshold = configuration.getWalSegmentRolloverSize();
        if (threshold == 0) {
            return false;
        }
        long tally = rowMem.getAppendOffset();
        // The events file will also contain the symbols.
        tally += eventWriter.size();
        return tally > threshold;
    }

    private void checkDistressed() {
        if (!distressed) {
            return;
        }
        throw CairoException.critical(0)
                .put("WAL writer is distressed and cannot be used any more [table=").put(tableToken.getTableName())
                .put(", wal=").put(walId).put(']');
    }

    private void configureEmptySymbol(int columnWriterIndex) {
        symbolMapReaders.extendAndSet(columnWriterIndex, EmptySymbolMapReader.INSTANCE);
        initialSymbolCounts.extendAndSet(columnWriterIndex, 0);
        localSymbolIds.extendAndSet(columnWriterIndex, 0);
        symbolMapNullFlags.extendAndSet(columnWriterIndex, false);
        symbolMaps.extendAndSet(columnWriterIndex, new CharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        utf8SymbolMaps.extendAndSet(columnWriterIndex, new Utf8StringIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
    }

    private void configureSymbolMapWriter(
            int columnWriterIndex,
            CharSequence columnName,
            int symbolCount,
            long columnNameTxn
    ) {
        if (symbolCount == 0) {
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        // Copy or hard link symbol map files.
        FilesFacade ff = configuration.getFilesFacade();
        Path tempPath = Path.PATH.get();
        tempPath.of(configuration.getRoot()).concat(tableToken);
        int tempPathTripLen = tempPath.size();

        path.trimTo(rootLen);
        TableUtils.offsetFileName(tempPath, columnName, columnNameTxn);
        TableUtils.offsetFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link offset file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        TableUtils.charFileName(tempPath, columnName, columnNameTxn);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link char file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, rootLen, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        BitmapIndexUtils.keyFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.keyFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link key file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, rootLen, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(rootLen);
        BitmapIndexUtils.valueFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.valueFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link value file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, rootLen, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        path.trimTo(rootLen);
        SymbolMapReader symbolMapReader = new SymbolMapReaderImpl(
                configuration,
                path,
                columnName,
                COLUMN_NAME_TXN_NONE,
                symbolCount
        );

        symbolMapReaders.extendAndSet(columnWriterIndex, symbolMapReader);
        symbolMaps.extendAndSet(columnWriterIndex, new CharSequenceIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        utf8SymbolMaps.extendAndSet(columnWriterIndex, new Utf8StringIntHashMap(8, 0.5, SymbolTable.VALUE_NOT_FOUND));
        initialSymbolCounts.extendAndSet(columnWriterIndex, symbolCount);
        localSymbolIds.extendAndSet(columnWriterIndex, 0);
        symbolMapNullFlags.extendAndSet(columnWriterIndex, symbolMapReader.containsNullValue());
    }

    private void configureSymbolTable() {
        boolean initialized = false;
        try {
            int denseSymbolIndex = 0;

            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (!ColumnType.isSymbol(columnType)) {
                    // Maintain sparse list of symbol writers
                    // Note: we don't need to set initialSymbolCounts and symbolMapNullFlags values
                    // here since we already filled it with -1 and false initially
                    symbolMapReaders.extendAndSet(i, null);
                    symbolMaps.extendAndSet(i, null);
                    utf8SymbolMaps.extendAndSet(i, null);
                } else {
                    if (txReader == null) {
                        txReader = new TxReader(ff);
                        columnVersionReader = new ColumnVersionReader();
                    }

                    if (!initialized) {
                        MillisecondClock milliClock = configuration.getMillisecondClock();
                        long spinLockTimeout = configuration.getSpinLockTimeout();

                        // todo: use own path
                        Path path = Path.PATH2.get();
                        path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                        // Does not matter which PartitionBy, as long as it is partitioned
                        // WAL tables must be partitioned
                        txReader.ofRO(path, PartitionBy.DAY);
                        path.of(configuration.getRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$();
                        columnVersionReader.ofRO(ff, path);

                        initialized = true;
                        long structureVersion = getMetadataVersion();

                        do {
                            TableUtils.safeReadTxn(txReader, milliClock, spinLockTimeout);
                            if (txReader.getColumnStructureVersion() != structureVersion) {
                                initialized = false;
                                break;
                            }
                            columnVersionReader.readSafe(milliClock, spinLockTimeout);
                        } while (txReader.getColumnVersion() != columnVersionReader.getVersion());
                    }

                    if (initialized) {
                        int symbolValueCount = txReader.getSymbolValueCount(denseSymbolIndex);
                        long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(i);
                        configureSymbolMapWriter(i, metadata.getColumnName(i), symbolValueCount, columnNameTxn);
                    } else {
                        // table on disk structure version does not match the structure version of the WalWriter
                        // it is not possible to re-use table symbol table because the column name may not match.
                        // The symbol counts stored as dense in _txn file and removal of symbols
                        // shifts the counts that's why it's not possible to find out the symbol count if metadata versions
                        // don't match.
                        configureSymbolMapWriter(i, metadata.getColumnName(i), 0, COLUMN_NAME_TXN_NONE);
                    }
                }

                if (columnType == ColumnType.SYMBOL) {
                    denseSymbolIndex++;
                }
            }
        } finally {
            Misc.free(columnVersionReader);
            Misc.free(txReader);
        }
    }

    private int createSegmentDir(int segmentId) {
        path.trimTo(rootLen);
        path.slash().put(segmentId);
        final int segmentPathLen = path.size();
        segmentLockFd = acquireSegmentLock();
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    private void freeSymbolMapReaders() {
        Misc.freeObjListIfCloseable(symbolMapReaders);
    }

    private long getAppendPageSize() {
        return tableToken.isSystem() ? configuration.getSystemWalDataAppendPageSize() : configuration.getWalDataAppendPageSize();
    }

    private long getColumnStructureVersion() {
        // Sequencer metadata version is the same as column structure version of the table.
        return metadata.getMetadataVersion();
    }

    private long getSequencerTxn() {
        long seqTxn;
        do {
            seqTxn = sequencer.nextTxn(
                    tableToken,
                    walId,
                    getColumnStructureVersion(),
                    segmentId,
                    lastSegmentTxn,
                    txnMinTimestamp,
                    txnMaxTimestamp,
                    segmentRowCount - currentTxnStartRowNum
            );
            if (seqTxn == NO_TXN) {
                applyMetadataChangeLog(Long.MAX_VALUE);
            }
        } while (seqTxn == NO_TXN);
        return lastSeqTxn = seqTxn;
    }

    private boolean isTruncateFilesOnClose() {
        return this.walDirectoryPolicy.truncateFilesOnClose();
    }

    private void lockWal() {
        try {
            lockName(path);
            walLockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (walLockFd == -1) {
            throw CairoException.critical(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void mayRollSegmentOnNextRow() {
        if (rollSegmentOnNextRow) {
            return;
        }
        rollSegmentOnNextRow = (segmentRowCount >= configuration.getWalSegmentRolloverRowCount())
                || breachedRolloverSizeThreshold()
                || (lastSegmentTxn > Integer.MAX_VALUE - 2);
    }

    private void mkWalDir() {
        final int walDirLength = path.size();
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL directory: ").put(path);
        }
        path.trimTo(walDirLength);
    }

    private void openNewSegment() {
        final int oldSegmentId = segmentId;
        final int newSegmentId = segmentId + 1;
        final int oldSegmentLockFd = segmentLockFd;
        segmentLockFd = -1;
        final long oldSegmentRows = segmentRowCount;
        try {
            currentTxnStartRowNum = 0;
            currentTxnOffset = 0;
            final int segmentPathLen = createSegmentDir(newSegmentId);
            segmentId = newSegmentId;
            final int dirFd;
            final int commitMode = configuration.getCommitMode();
            if (Os.isWindows() || commitMode == CommitMode.NOSYNC) {
                dirFd = -1;
            } else {
                dirFd = TableUtils.openRO(ff, path, LOG);
            }

            rowMem.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
            rowMem.of(
                    ff,
                    path.trimTo(segmentPathLen).concat(WAL_SEGMENT_FILE_NAME).$(),
                    getAppendPageSize(),
                    -1,
                    MEM_TAG,
                    configuration.getWriterFileOpenOpts(),
                    Files.POSIX_MADV_RANDOM
            );
            path.trimTo(segmentPathLen);

            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0) {
                    if (columnType == ColumnType.SYMBOL && symbolMapReaders.size() > 0) {
                        final SymbolMapReader reader = symbolMapReaders.getQuick(i);
                        initialSymbolCounts.set(i, reader.getSymbolCount());
                        localSymbolIds.set(i, 0);
                        symbolMapNullFlags.set(i, reader.containsNullValue());
                        symbolMaps.getQuick(i).clear();
                        utf8SymbolMaps.getQuick(i).clear();
                    }
                }
            }

            segmentRowCount = 0;
            metadata.switchTo(path, segmentPathLen, isTruncateFilesOnClose());
            eventWriter.openEventFile(path, segmentPathLen, isTruncateFilesOnClose(), tableToken.isSystem());
            if (commitMode != CommitMode.NOSYNC) {
                eventWriter.sync();
            }

            if (dirFd != -1) {
                ff.fsyncAndClose(dirFd);
            }
            lastSegmentTxn = 0;
            LOG.info().$("opened WAL segment [path='").$(path).$('\'').I$();
        } finally {
            if (oldSegmentLockFd > -1) {
                releaseSegmentLock(oldSegmentId, oldSegmentLockFd, oldSegmentRows);
            }
            path.trimTo(rootLen);
        }
    }

    private void releaseSegmentLock(int segmentId, int segmentLockFd, long segmentRowCount) {
        if (ff.close(segmentLockFd)) {
            if (segmentRowCount > 0) {
                sequencer.notifySegmentClosed(tableToken, lastSeqTxn, walId, segmentId);
                LOG.debug().$("released segment lock [walId=").$(walId)
                        .$(", segmentId=").$(segmentId)
                        .$(", fd=").$(segmentLockFd)
                        .$(']').$();
            } else {
                path.trimTo(rootLen).slash().put(segmentId);
                walDirectoryPolicy.rollbackDirectory(path);
                path.trimTo(rootLen);
            }
        } else {
            LOG.error()
                    .$("cannot close segment lock fd [walId=").$(walId)
                    .$(", segmentId=").$(segmentId)
                    .$(", fd=").$(segmentLockFd)
                    .$(", errno=").$(ff.errno()).I$();
        }
    }

    private void releaseWalLock() {
        if (ff.close(walLockFd)) {
            walLockFd = -1;
            LOG.debug().$("released WAL lock [walId=").$(walId)
                    .$(", fd=").$(walLockFd)
                    .$(']').$();
        } else {
            LOG.error()
                    .$("cannot close WAL lock fd [walId=").$(walId)
                    .$(", fd=").$(walLockFd)
                    .$(", errno=").$(ff.errno()).I$();
        }
    }

    private void removeSymbolFiles(Path path, int rootLen, CharSequence columnName) {
        // Symbol files in WAL directory are hard links to symbol files in the table.
        // Removing them does not affect the allocated disk space, and it is just
        // making directory tidy. On Windows OS, removing hard link can trigger
        // ACCESS_DENIED error, caused by the fact hard link destination file is open.
        // For those reasons we do not put maximum effort into removing the files here.

        path.trimTo(rootLen);
        BitmapIndexUtils.valueFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        BitmapIndexUtils.keyFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());

        path.trimTo(rootLen);
        TableUtils.offsetFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        ff.removeQuiet(path.$());
    }

    private void removeSymbolMapReader(int index) {
        Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(index, null));
        symbolMaps.setQuick(index, null);
        utf8SymbolMaps.setQuick(index, null);
        initialSymbolCounts.set(index, -1);
        localSymbolIds.set(index, 0);
        symbolMapNullFlags.set(index, false);
        removeSymbolFiles(path, rootLen, metadata.getColumnName(index));
    }

    private void resetDataTxnProperties() {
        currentTxnStartRowNum = segmentRowCount;
        currentTxnOffset = rowMem.getAppendOffset();
        txnMinTimestamp = Long.MAX_VALUE;
        txnMaxTimestamp = -1;
        txnOutOfOrder = false;
        resetSymbolMaps();
    }

    private void resetSymbolMaps() {
        final int numOfColumns = symbolMaps.size();
        for (int i = 0; i < numOfColumns; i++) {
            final CharSequenceIntHashMap symbolMap = symbolMaps.getQuick(i);
            if (symbolMap != null) {
                symbolMap.clear();
            }

            final Utf8StringIntHashMap dbcsSymbolMap = utf8SymbolMaps.getQuick(i);
            if (dbcsSymbolMap != null) {
                dbcsSymbolMap.clear();
            }

            final SymbolMapReader reader = symbolMapReaders.getQuick(i);
            if (reader != null) {
                initialSymbolCounts.set(i, reader.getSymbolCount());
                localSymbolIds.set(i, 0);
                symbolMapNullFlags.set(i, reader.containsNullValue());
            }
        }
    }

    private void rollLastWalEventRecord(int newSegmentId, long uncommittedRows, long uncommittedSize) {
        if (isCommittingData) {
            // Sometimes we only want to add a column without committing the data in the current wal segments in ILP.
            // When this happens the data stays in the WAL column files but is not committed
            // and the events file don't have a record about the column add transaction.
            // In this case we DO NOT roll back the last record in the events file.
            eventWriter.rollback();
        }
        path.trimTo(rootLen).slash().put(newSegmentId);
        eventWriter.openEventFile(path, path.size(), isTruncateFilesOnClose(), tableToken.isSystem());
        if (isCommittingData) {
            // When current transaction is not a data transaction but a column add transaction
            // there is no need to add a record about it to the new segment event file.
            lastSegmentTxn = eventWriter.appendRowFirstData(
                    0,
                    uncommittedRows,
                    0,
                    uncommittedSize,
                    txnMinTimestamp,
                    txnMaxTimestamp,
                    txnOutOfOrder
            );
        }
        eventWriter.sync();
    }

    private void setAppendPosition(long segmentOffset) {
        rowMem.jumpTo(segmentOffset);
    }

    private void switchToNewSegment(int newFd, long currentOffset, long newOffset) {
        rowMem.jumpTo(currentOffset);
        rowMem.switchTo(newFd, newOffset, isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
    }

    private void sync(int commitMode) {
        final boolean async = commitMode == CommitMode.ASYNC;
        rowMem.sync(async);
        eventWriter.sync();
    }

    private class MetadataValidatorService implements MetadataServiceStub {
        public long structureVersion;

        @Override
        public void addColumn(
                CharSequence columnName,
                int columnType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                boolean isIndexed,
                int indexValueBlockCapacity,
                boolean isSequential,
                SecurityContext securityContext
        ) {
            if (!TableUtils.isValidColumnName(columnName, columnName.length())) {
                throw CairoException.nonCritical().put("invalid column name: ").put(columnName);
            }
            if (metadata.getColumnIndexQuiet(columnName) > -1) {
                throw CairoException.nonCritical().put("duplicate column name: ").put(columnName);
            }
            if (columnType <= 0) {
                throw CairoException.nonCritical().put("invalid column type: ").put(columnType);
            }
            structureVersion++;
        }

        @Override
        public void disableDeduplication() {
            structureVersion++;
        }

        @Override
        public void enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            for (int i = 0, n = columnsIndexes.size(); i < n; i++) {
                int columnIndex = (int) columnsIndexes.get(i);
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType < 0) {
                    throw CairoException.nonCritical().put("cannot use dropped column for deduplication [column=").put(metadata.getColumnName(columnIndex)).put(']');
                }
                if (ColumnType.isVarSize(columnType)) {
                    throw CairoException.nonCritical().put("cannot use variable length column for deduplication [column=").put(metadata.getColumnName(columnIndex))
                            .put(", type=").put(ColumnType.nameOf(columnType)).put(']');
                }
            }
            structureVersion++;
        }

        @Override
        public long getMetaO3MaxLag() {
            return 0;
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                throw CairoException.nonCritical().put("cannot remove column, column does not exist [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }

            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.nonCritical().put("cannot remove designated timestamp column [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName);
            }
            structureVersion++;
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0) {
                throw CairoException.nonCritical().put("cannot rename column, column does not exist [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }
            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.nonCritical().put("cannot rename designated timestamp column [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }

            int columnIndexNew = metadata.getColumnIndexQuiet(newName);
            if (columnIndexNew > -1) {
                throw CairoException.nonCritical().put("cannot rename column, column with the name already exists [table=").put(tableToken.getTableName())
                        .put(", newName=").put(newName).put(']');
            }
            if (!TableUtils.isValidColumnName(newName, newName.length())) {
                throw CairoException.nonCritical().put("invalid column name: ").put(newName);
            }
            structureVersion++;
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            // this check deal with concurrency
            if (!Chars.equalsIgnoreCaseNc(fromNameTable, metadata.getTableToken().getTableName())) {
                throw CairoException.tableDoesNotExist(fromNameTable);
            }
            structureVersion++;
        }

        public void startAlterValidation() {
            structureVersion = getColumnStructureVersion();
        }
    }

    private class MetadataWriterService implements MetadataServiceStub {

        @Override
        public void addColumn(
                CharSequence columnName,
                int columnType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                boolean isIndexed,
                int indexValueBlockCapacity,
                boolean isSequential,
                SecurityContext securityContext
        ) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);

            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                if (currentTxnStartRowNum > 0) {
                    // Roll last transaction to new segment
                    rollUncommittedToNewSegment();
                }

                if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                    metadata.addColumn(columnName, columnType);
                    columnCount = metadata.getColumnCount();
                    columnIndex = columnCount - 1;
                    if (ColumnType.isSymbol(columnType)) {
                        configureSymbolMapWriter(columnIndex, columnName, 0, -1);
                    }

                    if (!rollSegmentOnNextRow) {
                        // this means we have rolled uncommitted rows to a new segment already
                        // we should switch metadata to this new segment
                        path.trimTo(rootLen).slash().put(segmentId);
                        // this will close old _meta file and create the new one
                        metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        path.trimTo(rootLen);
                    }
                    // if we did not have to roll uncommitted rows to a new segment
                    // it will switch metadata file on next row write
                    // as part of rolling to a new segment

                    if (securityContext != null) {
                        ddlListener.onColumnAdded(securityContext, metadata.getTableToken(), columnName);
                    }
                    LOG.info().$("added column to WAL [path=").$(path).$(", columnName=").utf8(columnName).$(", type=").$(ColumnType.nameOf(columnType)).I$();
                } else {
                    throw CairoException.critical(0).put("column '").put(columnName)
                            .put("' was added, cannot apply commit because of concurrent table definition change");
                }
            } else {
                if (metadata.getColumnType(columnIndex) == columnType) {
                    // TODO: this should be some kind of warning probably that different WALs adding the same column concurrently
                    LOG.info().$("column has already been added by another WAL [path=").$(path).$(", columnName=").utf8(columnName).I$();
                } else {
                    throw CairoException.nonCritical().put("column '").put(columnName).put("' already exists");
                }
            }
        }

        @Override
        public void disableDeduplication() {
            metadata.disableDeduplicate();
        }

        @Override
        public void enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            metadata.enableDeduplicationWithUpsertKeys(columnsIndexes);
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return metadata;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex > -1) {
                int type = metadata.getColumnType(columnIndex);
                if (type > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment();
                    }

                    if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                        int index = metadata.getColumnIndex(columnName);
                        metadata.removeColumn(columnName);
                        columnCount = metadata.getColumnCount();

                        if (!rollSegmentOnNextRow) {
                            // this means we have rolled uncommitted rows to a new segment already
                            // we should switch metadata to this new segment
                            path.trimTo(rootLen).slash().put(segmentId);
                            // this will close old _meta file and create the new one
                            metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        if (ColumnType.isSymbol(type)) {
                            removeSymbolMapReader(index);
                        }
                        LOG.info().$("removed column from WAL [path=").$(path).$(", columnName=").utf8(columnName).I$();
                    } else {
                        throw CairoException.critical(0).put("column '").put(columnName)
                                .put("' was removed, cannot apply commit because of concurrent table definition change");
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column '").put(columnName).put("' does not exists");
            }
        }

        @Override
        public void renameColumn(
                @NotNull CharSequence columnName,
                @NotNull CharSequence newColumnName,
                SecurityContext securityContext
        ) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex > -1) {
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment();
                    }

                    if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                        metadata.renameColumn(columnName, newColumnName);
                        // We are not going to do any special for symbol readers which point
                        // to the files in the root of the table.
                        // We keep the symbol readers open against files with old name.
                        // Inconsistency between column name and symbol file names in the root
                        // does not matter, these files are for re-lookup only for the WAL writer
                        // and should not be serialised to the WAL segment.

                        if (!rollSegmentOnNextRow) {
                            // this means we have rolled uncommitted rows to a new segment already
                            // we should switch metadata to this new segment
                            path.trimTo(rootLen).slash().put(segmentId);
                            // this will close old _meta file and create the new one
                            metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        if (securityContext != null) {
                            ddlListener.onColumnRenamed(securityContext, metadata.getTableToken(), columnName, newColumnName);
                        }

                        LOG.info().$("renamed column in WAL [path=").$(path).$(", columnName=").utf8(columnName).$(", newColumnName=").utf8(newColumnName).I$();
                    } else {
                        throw CairoException.critical(0).put("column '").put(columnName)
                                .put("' was removed, cannot apply commit because of concurrent table definition change");
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column '").put(columnName).put("' does not exists");
            }
        }

        @Override
        public void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName) {
            tableToken = metadata.getTableToken().renamed(Chars.toString(toTableName));
            metadata.renameTable(tableToken);
        }
    }

    private class RowImpl implements TableWriter.Row {
        private final StringSink tempSink = new StringSink();
        private final Utf8StringSink tempUtf8Sink = new Utf8StringSink();
        private long startOffset;
        private long timestamp;

        @Override
        public void append() {
            if (timestamp > txnMaxTimestamp) {
                txnMaxTimestamp = timestamp;
            } else {
                txnOutOfOrder |= (txnMaxTimestamp != timestamp);
            }
            if (timestamp < txnMinTimestamp) {
                txnMinTimestamp = timestamp;
            }

            segmentRowCount++;
            rowMem.putInt(NEW_ROW_SEPARATOR);
        }

        @Override
        public void cancel() {
            setAppendPosition(startOffset);
        }

        public void init() {
            this.startOffset = rowMem.getAppendOffset();
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            rowMem.putInt(columnIndex);
            rowMem.putBin(address, len);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            rowMem.putInt(columnIndex);
            rowMem.putBin(sequence);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            rowMem.putInt(columnIndex);
            rowMem.putBool(value);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            rowMem.putInt(columnIndex);
            rowMem.putByte(value);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            rowMem.putInt(columnIndex);
            rowMem.putChar(value);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            rowMem.putInt(columnIndex);
            rowMem.putDouble(value);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            rowMem.putInt(columnIndex);
            rowMem.putFloat(value);
        }

        @Override
        public void putGeoHash(int columnIndex, long value) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, value, type, this);
        }

        @Override
        public void putGeoHashDeg(int columnIndex, double lat, double lon) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoHash(columnIndex, GeoHashes.fromCoordinatesDegUnsafe(lat, lon, ColumnType.getGeoHashBits(type)), type, this);
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoStr(columnIndex, hash, type, this);
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence hash) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putGeoVarchar(columnIndex, hash, type, this);
        }

        @Override
        public void putIPv4(int columnIndex, int value) {
            putInt(columnIndex, value);
        }

        @Override
        public void putInt(int columnIndex, int value) {
            rowMem.putInt(columnIndex);
            rowMem.putInt(value);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            rowMem.putInt(columnIndex);
            rowMem.putLong(value);
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            rowMem.putInt(columnIndex);
            rowMem.putLong(lo);
            rowMem.putLong(hi);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            rowMem.putInt(columnIndex);
            rowMem.putLong256(l0, l1, l2, l3);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            rowMem.putInt(columnIndex);
            rowMem.putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            rowMem.putInt(columnIndex);
            rowMem.putLong256(hexString);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            rowMem.putInt(columnIndex);
            rowMem.putLong256(hexString, start, end);
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            rowMem.putInt(columnIndex);
            rowMem.putLong256Utf8(hexString);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            rowMem.putInt(columnIndex);
            rowMem.putShort(value);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            rowMem.putInt(columnIndex);
            rowMem.putStr(value);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            rowMem.putInt(columnIndex);
            rowMem.putStr(value);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            rowMem.putInt(columnIndex);
            rowMem.putStr(value, pos, len);
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            rowMem.putInt(columnIndex);
            rowMem.putStrUtf8(value);
        }

        @Override
        public void putSym(int columnIndex, CharSequence value) {
            final SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                putSym0(columnIndex, value, symbolMapReader);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void putSym(int columnIndex, char value) {
            CharSequence str = SingleCharCharSequence.get(value);
            putSym(columnIndex, str);
        }

        @Override
        public void putSymIndex(int columnIndex, int key) {
            putInt(columnIndex, key);
        }

        @Override
        public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
            // this method will write column name to the buffer if it has to be UTF-8 decoded
            // otherwise it will write nothing
            final SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                Utf8StringIntHashMap utf8Map = utf8SymbolMaps.getQuick(columnIndex);
                int index = utf8Map.keyIndex(value);
                if (index < 0) {
                    rowMem.putInt(columnIndex);
                    rowMem.putInt(utf8Map.valueAt(index));
                } else {
                    // slow path, symbol is not in UTF-8 cache
                    utf8Map.putAt(
                            index,
                            Utf8String.newInstance(value),
                            putSymUtf8Slow(columnIndex, value, symbolMapReader)
                    );
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void putTimestamp(int columnIndex, long value) {
            if (columnIndex == timestampIndex) {
                setTimestamp(value);
            } else {
                putLong(columnIndex, value);
            }
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuidStr) {
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            putLong128(columnIndex, uuid.getLo(), uuid.getHi());
        }

        @Override
        public void putVarchar(int columnIndex, char value) {
            tempUtf8Sink.clear();
            tempUtf8Sink.put(value);
            rowMem.putInt(columnIndex);
            VarcharTypeDriver.appendPlainValue(rowMem, tempUtf8Sink);
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            rowMem.putInt(columnIndex);
            VarcharTypeDriver.appendPlainValue(rowMem, value);
        }

        private int putSym0(int columnIndex, CharSequence utf16Value, SymbolMapReader symbolMapReader) {
            int key;
            if (utf16Value != null) {
                final CharSequenceIntHashMap utf16Map = symbolMaps.getQuick(columnIndex);
                final int index = utf16Map.keyIndex(utf16Value);
                if (index > -1) {
                    key = symbolMapReader.keyOf(utf16Value);
                    if (key == SymbolTable.VALUE_NOT_FOUND) {
                        // Add it to in-memory symbol map
                        // Locally added symbols must have a continuous range of keys
                        final int initialSymCount = initialSymbolCounts.get(columnIndex);
                        key = initialSymCount + localSymbolIds.postIncrement(columnIndex);
                    }
                    // Chars.toString used as value is a parser buffer memory slice or mapped memory of symbolMapReader
                    utf16Map.putAt(index, Chars.toString(utf16Value), key);
                } else {
                    key = utf16Map.valueAt(index);
                }
            } else {
                key = SymbolTable.VALUE_IS_NULL;
                symbolMapNullFlags.set(columnIndex, true);
            }

            rowMem.putInt(columnIndex);
            rowMem.putInt(key);
            return key;
        }

        private int putSymUtf8Slow(
                int columnIndex,
                DirectUtf8Sequence utf8Value,
                SymbolMapReader symbolMapReader
        ) {
            return putSym0(
                    columnIndex,
                    Utf8s.directUtf8ToUtf16(utf8Value, tempSink),
                    symbolMapReader
            );
        }

        private void setTimestamp(long value) {
            // avoid lookups by having a designated field with primaryColumn
            rowMem.putInt(timestampIndex);
            rowMem.putLong128(value, segmentRowCount);
            this.timestamp = value;
        }
    }
}
