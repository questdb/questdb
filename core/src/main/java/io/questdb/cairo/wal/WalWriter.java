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
import io.questdb.cairo.AlterTableContextException;
import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DdlListener;
import io.questdb.cairo.EmptySymbolMapReader;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.NullMemory;
import io.questdb.cairo.wal.seq.MetadataServiceStub;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.SymbolMapWriterLite;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.AtomicIntList;
import io.questdb.std.BinarySequence;
import io.questdb.std.BoolList;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableSequencer.NO_TXN;

public class WalWriter implements TableWriterAPI {
    private static final long COLUMN_DELETED_NULL_FLAG = Long.MAX_VALUE;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final int MEM_TAG = MemoryTag.MMAP_TABLE_WAL_WRITER;
    private static final Runnable NOOP = () -> {
    };
    private final AlterOperation alterOp = new AlterOperation();
    private final ObjList<MemoryMA> columns;
    private final CairoConfiguration configuration;
    private final DdlListener ddlListener;
    private final WalEventWriter events;
    private final FilesFacade ff;
    private final AtomicIntList initialSymbolCounts;
    private final IntList localSymbolIds;
    private final MetadataValidatorService metaValidatorSvc = new MetadataValidatorService();
    private final MetadataService metaWriterSvc = new MetadataWriterService();
    private final WalWriterMetadata metadata;
    private final Metrics metrics;
    private final int mkDirMode;
    private final ObjList<Runnable> nullSetters;
    private final Path path;
    private final int pathRootSize;
    private final int pathSize;
    private final RowImpl row = new RowImpl();
    private final LongList rowValueIsNotNull = new LongList();
    private final TableSequencerAPI sequencer;
    private final BoolList symbolMapNullFlags = new BoolList();
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final ObjList<CharSequenceIntHashMap> symbolMaps = new ObjList<>();
    private final TimestampDriver timestampDriver;
    private final int timestampIndex;
    private final ObjList<Utf8StringIntHashMap> utf8SymbolMaps = new ObjList<>();
    private final Uuid uuid = new Uuid();
    private final WalDirectoryPolicy walDirectoryPolicy;
    private final int walId;
    private final String walName;
    private long avgRecordSize;
    private SegmentColumnRollSink columnConversionSink;
    private int columnCount;
    private ColumnVersionReader columnVersionReader;
    private ConversionSymbolMapWriter conversionSymbolMap;
    private ConversionSymbolTable conversionSymbolTable;
    private long currentTxnStartRowNum = -1;
    private boolean distressed;
    private boolean isCommittingData;
    private byte lastDedupMode = WAL_DEDUP_MODE_DEFAULT;
    private long lastMatViewPeriodHi = WAL_DEFAULT_LAST_PERIOD_HI;
    private long lastMatViewRefreshBaseTxn = WAL_DEFAULT_BASE_TABLE_TXN;
    private long lastMatViewRefreshTimestamp = WAL_DEFAULT_LAST_REFRESH_TIMESTAMP;
    private long lastReplaceRangeHiTs = 0;
    private long lastReplaceRangeLowTs = 0;
    private int lastSegmentTxn = -1;
    private long lastSeqTxn = NO_TXN;
    private byte lastTxnType = WalTxnType.DATA;
    private boolean open;
    private boolean rollSegmentOnNextRow = false;
    private int segmentId = -1;
    private long segmentLockFd = -1;
    private long segmentRowCount = -1;
    private TableToken tableToken;
    private long totalSegmentsRowCount;
    private long totalSegmentsSize;
    private TxReader txReader;
    private long txnMaxTimestamp = -1;
    private long txnMinTimestamp = Long.MAX_VALUE;
    private boolean txnOutOfOrder = false;
    private long walLockFd = -1;

    public WalWriter(
            CairoConfiguration configuration,
            TableToken tableToken,
            TableSequencerAPI tableSequencerAPI,
            DdlListener ddlListener,
            WalDirectoryPolicy walDirectoryPolicy
    ) {
        LOG.info().$("open [table=").$(tableToken).I$();
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
        this.path = new Path();
        path.of(configuration.getDbRoot());
        this.pathRootSize = configuration.getDbLogName() == null ? path.size() : 0;
        this.path.concat(tableToken).concat(walName);
        this.pathSize = path.size();
        this.metrics = configuration.getMetrics();
        this.open = true;

        try {
            lockWal();
            mkWalDir();

            metadata = new WalWriterMetadata(ff);
            tableSequencerAPI.getTableMetadata(tableToken, metadata);
            this.timestampDriver = ColumnType.getTimestampDriver(metadata.getTimestampType());
            this.tableToken = metadata.getTableToken();

            columnCount = metadata.getColumnCount();
            timestampIndex = metadata.getTimestampIndex();
            columns = new ObjList<>(columnCount * 2);
            nullSetters = new ObjList<>(columnCount);
            initialSymbolCounts = new AtomicIntList(columnCount);
            localSymbolIds = new IntList(columnCount);

            events = new WalEventWriter(configuration);
            events.of(symbolMaps, initialSymbolCounts, symbolMapNullFlags);

            configureColumns();
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

    @Override
    public void commit() {
        // plain old commit
        commit0(
                WalTxnType.DATA,
                WAL_DEFAULT_BASE_TABLE_TXN,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                0,
                0,
                WAL_DEDUP_MODE_DEFAULT
        );
    }

    /**
     * Commit the materialized view to update last refresh timestamp.
     * Called as the last transaction of a materialized view refresh.
     *
     * @param lastRefreshBaseTxn    the base table seqTxn the mat view is refreshed at
     * @param lastRefreshTimestamp  the wall clock timestamp when the refresh is done
     * @param lastPeriodHi          the period high boundary timestamp the mat view is refreshed at
     * @param lastReplaceRangeLowTs the low timestamp of the range to be replaced, inclusive
     * @param lastReplaceRangeHiTs  the high timestamp of the range to be replaced, exclusive
     */
    public void commitMatView(long lastRefreshBaseTxn, long lastRefreshTimestamp, long lastPeriodHi, long lastReplaceRangeLowTs, long lastReplaceRangeHiTs) {
        assert lastReplaceRangeLowTs < lastReplaceRangeHiTs;
        assert txnMinTimestamp >= lastReplaceRangeLowTs;
        assert txnMaxTimestamp <= lastReplaceRangeHiTs;
        commit0(
                WalTxnType.MAT_VIEW_DATA,
                lastRefreshBaseTxn,
                lastRefreshTimestamp,
                lastPeriodHi,
                lastReplaceRangeLowTs,
                lastReplaceRangeHiTs,
                WAL_DEDUP_MODE_REPLACE_RANGE
        );
    }

    public void commitWithParams(long replaceRangeLowTs, long replaceRangeHiTs, byte dedupMode) {
        commit0(
                WalTxnType.DATA,
                WAL_DEFAULT_BASE_TABLE_TXN,
                WAL_DEFAULT_LAST_REFRESH_TIMESTAMP,
                WAL_DEFAULT_LAST_PERIOD_HI,
                replaceRangeLowTs,
                replaceRangeHiTs,
                dedupMode
        );
    }

    public void doClose(boolean truncate) {
        if (open) {
            open = false;
            if (metadata != null) {
                metadata.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }
            if (events != null) {
                events.close(truncate, Vm.TRUNCATE_TO_POINTER);
            }
            freeSymbolMapReaders();
            freeColumns(truncate);

            releaseSegmentLock(segmentId, segmentLockFd, lastSegmentTxn);

            try {
                releaseWalLock();
            } finally {
                Misc.free(path);
                LOG.info().$("closed [table=").$(tableToken).I$();
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

    public @NotNull TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public long getUncommittedRowCount() {
        return segmentRowCount - currentTxnStartRowNum;
    }

    public int getWalId() {
        return walId;
    }

    public String getWalName() {
        return walName;
    }

    public void goActive() {
        goActive(Long.MAX_VALUE);
    }

    public boolean goActive(long maxStructureVersion) {
        try {
            applyMetadataChangeLog(maxStructureVersion);
            return true;
        } catch (CairoException e) {
            LOG.critical().$("could not apply structure changes, WAL will be closed [table=").$(tableToken)
                    .$(", walId=").$(walId)
                    .$(", ex=").$((Throwable) e)
                    .$(", errno=").$(e.getErrno())
                    .I$();
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
        timestampDriver.validateBounds(timestamp);
        try {
            if (rollSegmentOnNextRow) {
                rollSegment();
                rollSegmentOnNextRow = false;
            }
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

    // Marks the materialized view as invalid or resets its invalidation status,
    // depending on the input values.
    public void resetMatViewState(
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            boolean invalid,
            @Nullable CharSequence invalidationReason,
            long lastPeriodHi,
            @Nullable LongList refreshIntervals,
            long refreshIntervalsBaseTxn
    ) {
        try {
            lastSegmentTxn = events.appendMatViewInvalidate(
                    lastRefreshBaseTxn,
                    lastRefreshTimestamp,
                    invalid,
                    invalidationReason,
                    lastPeriodHi,
                    refreshIntervals,
                    refreshIntervalsBaseTxn
            );
            getSequencerTxn();
        } catch (Throwable th) {
            rollback();
            throw th;
        }
    }

    public void rollSegment() {
        try {
            openNewSegment();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        }
    }

    public void rollUncommittedToNewSegment(int convertColumnIndex, int convertToColumnType) {
        final long uncommittedRows = getUncommittedRowCount();
        final long oldLastSegmentTxn = lastSegmentTxn;
        long rowsRemainInCurrentSegment = currentTxnStartRowNum;

        if (uncommittedRows > 0) {
            final int oldSegmentId = segmentId;
            final int newSegmentId = segmentId + 1;
            if (newSegmentId > WalUtils.SEG_MAX_ID) {
                throw CairoException.critical(0)
                        .put("cannot roll over to new segment due to SEG_MAX_ID overflow [table=").put(tableToken)
                        .put(", walId=").put(walId)
                        .put(", segmentId=").put(newSegmentId).put(']');
            }
            final long oldSegmentLockFd = segmentLockFd;
            segmentLockFd = -1;
            try {
                createSegmentDir(newSegmentId);
                path.trimTo(pathSize);
                SegmentColumnRollSink columnRollSink = createSegmentColumnRollSink();
                rowValueIsNotNull.fill(0, columnCount, -1);

                int columnsToRoll = convertColumnIndex == -1 ? columnCount : columnCount - 1;
                try {
                    final int timestampIndex = metadata.getTimestampIndex();

                    if (convertColumnIndex < 0) {
                        LOG.info().$("rolling uncommitted rows to new segment [wal=")
                                .$(path).$(Files.SEPARATOR).$(oldSegmentId)
                                .$(", lastSegmentTxn=").$(lastSegmentTxn)
                                .$(", newSegmentId=").$(newSegmentId)
                                .$(", skipRows=").$(rowsRemainInCurrentSegment)
                                .$(", rowCount=").$(uncommittedRows)
                                .I$();
                    } else {
                        int existingType = metadata.getColumnType(convertColumnIndex);
                        LOG.info().$("rolling uncommitted rows to new segment with type conversion [wal=")
                                .$(path).$(Files.SEPARATOR).$(oldSegmentId)
                                .$(", lastSegmentTxn=").$(lastSegmentTxn)
                                .$(", newSegmentId=").$(newSegmentId)
                                .$(", skipRows=").$(rowsRemainInCurrentSegment)
                                .$(", rowCount=").$(uncommittedRows)
                                .$(", existingType=").$(ColumnType.nameOf(existingType))
                                .$(", newType=").$(ColumnType.nameOf(convertToColumnType))
                                .I$();
                    }

                    final int commitMode = configuration.getCommitMode();
                    for (int columnIndex = 0; columnIndex < columnsToRoll; columnIndex++) {
                        // Allocate space for new column in columnRollSink and move to next record
                        // Do it for deleted columns too, it will be skipped in exactly same way in switchColumnsToNewSegment
                        columnRollSink.nextColumn();
                        final int columnType = metadata.getColumnType(columnIndex);
                        if (columnType > 0) {
                            final MemoryMA primaryColumn = getDataColumn(columnIndex);
                            final MemoryMA secondaryColumn = getAuxColumn(columnIndex);
                            final String columnName = metadata.getColumnName(columnIndex);

                            SymbolMapWriterLite symbolMapWriter = null;
                            SymbolTable symbolTable = null;
                            if (columnIndex == convertColumnIndex) {
                                if (ColumnType.isSymbol(convertToColumnType)) {
                                    // New column destination is the column with last index
                                    symbolMapWriter = getConversionSymbolMapWriter(columnCount - 1);
                                }
                                if (ColumnType.isSymbol(columnType)) {
                                    symbolTable = getConversionSymbolMapReader(columnIndex);
                                }
                            }

                            int colType = columnIndex == timestampIndex ? -columnType : columnType;
                            int newColumnType = columnIndex == convertColumnIndex ? convertToColumnType : colType;
                            // Saves existing segment file offsets and new file sizes in columnRollSink.
                            CopyWalSegmentUtils.rollColumnToSegment(
                                    ff,
                                    configuration.getWriterFileOpenOpts(),
                                    primaryColumn,
                                    secondaryColumn,
                                    path,
                                    newSegmentId,
                                    columnName,
                                    colType,
                                    currentTxnStartRowNum,
                                    uncommittedRows,
                                    columnRollSink,
                                    commitMode,
                                    newColumnType,
                                    symbolTable,
                                    symbolMapWriter
                            );
                        } else {
                            // Deleted column
                            rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
                        }
                    }
                } catch (Throwable e) {
                    closeSegmentSwitchFiles(columnRollSink);
                    throw e;
                }
                switchColumnsToNewSegment(columnRollSink, columnsToRoll, convertColumnIndex);
                rollLastWalEventRecord(newSegmentId, uncommittedRows);
                segmentId = newSegmentId;
                segmentRowCount = uncommittedRows;
                currentTxnStartRowNum = 0;
            } finally {
                releaseSegmentLock(oldSegmentId, oldSegmentLockFd, oldLastSegmentTxn);
            }
        } else if (segmentRowCount > 0 && uncommittedRows == 0) {
            rollSegmentOnNextRow = true;
        }
    }

    @Override
    public void rollback() {
        try {
            if (!isDistressed() && (inTransaction() || hasDirtyColumns(currentTxnStartRowNum))) {
                setAppendPosition(currentTxnStartRowNum);
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

    @TestOnly
    public void setLegacyMatViewFormat(boolean legacyMatViewFormat) {
        events.setLegacyMatViewFormat(legacyMatViewFormat);
    }

    @Override
    public boolean supportsMultipleWriters() {
        return true;
    }

    @Override
    public String toString() {
        return "WalWriter{" +
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
            lastSegmentTxn = events.truncate();
            getSequencerTxn();
        } catch (Throwable th) {
            rollback();
            throw th;
        }
    }

    public void updateTableToken(TableToken ignoredTableToken) {
        // goActive will update table token
    }

    private static void configureNullSetters(ObjList<Runnable> nullers, int type, MemoryMA dataMem, MemoryMA auxMem) {
        int columnTag = ColumnType.tagOf(type);
        if (ColumnType.isVarSize(columnTag)) {
            final ColumnTypeDriver typeDriver = ColumnType.getDriver(columnTag);
            nullers.add(() -> typeDriver.appendNull(auxMem, dataMem));
        } else {
            switch (columnTag) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    nullers.add(() -> dataMem.putByte((byte) 0));
                    break;
                case ColumnType.DOUBLE:
                    nullers.add(() -> dataMem.putDouble(Double.NaN));
                    break;
                case ColumnType.FLOAT:
                    nullers.add(() -> dataMem.putFloat(Float.NaN));
                    break;
                case ColumnType.INT:
                    nullers.add(() -> dataMem.putInt(Numbers.INT_NULL));
                    break;
                case ColumnType.IPv4:
                    nullers.add(() -> dataMem.putInt(Numbers.IPv4_NULL));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    nullers.add(() -> dataMem.putLong(Numbers.LONG_NULL));
                    break;
                case ColumnType.LONG256:
                    nullers.add(() -> dataMem.putLong256(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                case ColumnType.SHORT:
                    nullers.add(() -> dataMem.putShort((short) 0));
                    break;
                case ColumnType.CHAR:
                    nullers.add(() -> dataMem.putChar((char) 0));
                    break;
                case ColumnType.SYMBOL:
                    nullers.add(() -> dataMem.putInt(SymbolTable.VALUE_IS_NULL));
                    break;
                case ColumnType.GEOBYTE:
                    nullers.add(() -> dataMem.putByte(GeoHashes.BYTE_NULL));
                    break;
                case ColumnType.GEOSHORT:
                    nullers.add(() -> dataMem.putShort(GeoHashes.SHORT_NULL));
                    break;
                case ColumnType.GEOINT:
                    nullers.add(() -> dataMem.putInt(GeoHashes.INT_NULL));
                    break;
                case ColumnType.GEOLONG:
                    nullers.add(() -> dataMem.putLong(GeoHashes.NULL));
                    break;
                case ColumnType.LONG128:
                    // fall through
                case ColumnType.UUID:
                    nullers.add(() -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
            }
        }
    }

    private static void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
    }

    private static int getAuxColumnOffset(int index) {
        return getDataColumnOffset(index) + 1;
    }

    private static int getDataColumnOffset(int columnIndex) {
        return columnIndex * 2;
    }

    private long acquireSegmentLock() {
        final int segmentPathLen = path.size();
        try {
            lockName(path);
            final long segmentLockFd = TableUtils.lock(ff, path.$());
            if (segmentLockFd == -1) {
                path.trimTo(segmentPathLen);
                throw CairoException.critical(ff.errno()).put("Cannot lock wal segment: ").put(path);
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
            long structVer = getColumnStructureVersion();
            while (log.hasNext() && structVer < structureVersionHi) {
                TableMetadataChange chg = log.next();
                try {
                    chg.apply(metaWriterSvc, true);
                } catch (CairoException e) {
                    distressed = true;
                    throw e;
                }

                if (++structVer != getColumnStructureVersion()) {
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
        if (
                (verifyStructureVersion && op.getTableVersion() != getColumnStructureVersion())
                        || op.getTableId() != metadata.getTableId()) {
            throw TableReferenceOutOfDateException.of(tableToken, metadata.getTableId(), op.getTableId(), getColumnStructureVersion(), op.getTableVersion());
        }

        try {
            lastSegmentTxn = events.appendSql(op.getCmdType(), op.getSqlText(), op.getSqlExecutionContext());
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
                            .put("statement is either no-op,")
                            .put(" or contains multiple transactions, such as 'alter table add column col1, col2',")
                            .put(" and currently not supported for WAL tables [table=").put(tableToken.getTableName())
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
            LOG.info().$("committed structural metadata change [wal=").$(path).$(Files.SEPARATOR).$(segmentId)
                    .$(", segmentTxn=").$(lastSegmentTxn)
                    .$(", seqTxn=").$(txn)
                    .I$();
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

        if (avgRecordSize != 0) {
            return (segmentRowCount * avgRecordSize) > threshold;
        }

        long tally = 0;
        for (int colIndex = 0, colCount = columns.size(); colIndex < colCount; ++colIndex) {
            final MemoryMA column = columns.getQuick(colIndex);
            if ((column != null) && !(column instanceof NullMemory)) {
                final long columnSize = column.getAppendOffset();
                tally += columnSize;
            }
        }

        // The events file will also contain the symbols.
        tally += events.size();

        // If we have many columns it can be a bit expensive, we can optimise the check
        // by calculating the average record size.
        if ((totalSegmentsRowCount + segmentRowCount) > 1000) {
            avgRecordSize = (totalSegmentsSize + tally) / (totalSegmentsRowCount + segmentRowCount);
        }

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

    private void closeSegmentSwitchFiles(SegmentColumnRollSink newColumnFiles) {
        int commitMode = configuration.getCommitMode();
        for (int columnIndex = 0, n = newColumnFiles.count(); columnIndex < n; columnIndex++) {
            final long primaryFd = newColumnFiles.getDestPrimaryFd(columnIndex);
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsyncAndClose(primaryFd);
            } else {
                ff.close(primaryFd);
            }

            final long secondaryFd = newColumnFiles.getDestAuxFd(columnIndex);
            if (commitMode != CommitMode.NOSYNC) {
                ff.fsyncAndClose(secondaryFd);
            } else {
                ff.close(secondaryFd);
            }
        }
    }

    private void commit0(
            byte txnType,
            long lastRefreshBaseTxn,
            long lastRefreshTimestamp,
            long lastPeriodHi,
            long replaceRangeLowTs,
            long replaceRangeHiTs,
            byte dedupMode
    ) {
        checkDistressed();
        try {
            if (inTransaction() || dedupMode == WAL_DEDUP_MODE_REPLACE_RANGE) {
                final long rowsToCommit = getUncommittedRowCount();

                this.isCommittingData = true;
                this.lastTxnType = txnType;
                this.lastReplaceRangeLowTs = replaceRangeLowTs;
                this.lastReplaceRangeHiTs = replaceRangeHiTs;
                this.lastDedupMode = dedupMode;
                this.lastMatViewRefreshBaseTxn = lastRefreshBaseTxn;
                this.lastMatViewRefreshTimestamp = lastRefreshTimestamp;
                this.lastMatViewPeriodHi = lastPeriodHi;

                lastSegmentTxn = events.appendData(
                        txnType,
                        currentTxnStartRowNum,
                        segmentRowCount,
                        txnMinTimestamp,
                        txnMaxTimestamp,
                        txnOutOfOrder,
                        lastRefreshBaseTxn,
                        lastRefreshTimestamp,
                        lastPeriodHi,
                        replaceRangeLowTs,
                        replaceRangeHiTs,
                        dedupMode
                );
                // flush disk before getting next txn
                syncIfRequired();
                final long seqTxn = getSequencerTxn();
                LogRecord logLine = LOG.info();
                try {
                    logLine.$("commit [wal=").$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                            .$(", segTxn=").$(lastSegmentTxn)
                            .$(", seqTxn=").$(seqTxn)
                            .$(", rowLo=").$(currentTxnStartRowNum).$(", rowHi=").$(segmentRowCount)
                            .$(", minTs=").$ts(timestampDriver, txnMinTimestamp).$(", maxTs=").$ts(timestampDriver, txnMaxTimestamp);
                    if (replaceRangeHiTs > replaceRangeLowTs) {
                        logLine.$(", replaceRangeLo=").$ts(timestampDriver, replaceRangeLowTs).$(", replaceRangeHi=").$ts(timestampDriver, replaceRangeHiTs);
                    }
                } finally {
                    logLine.I$();
                }
                resetDataTxnProperties();
                mayRollSegmentOnNextRow();
                metrics.walMetrics().addRowsWritten(rowsToCommit);
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
    }

    private void configureColumn(int columnIndex, int columnType) {
        final int dataColumnOffset = getDataColumnOffset(columnIndex);
        if (columnType > 0) {
            final MemoryMA dataMem = Vm.getPMARInstance(configuration);
            final MemoryMA auxMem = createAuxColumnMem(columnType);
            columns.extendAndSet(dataColumnOffset, dataMem);
            columns.extendAndSet(dataColumnOffset + 1, auxMem);
            configureNullSetters(nullSetters, columnType, dataMem, auxMem);
            rowValueIsNotNull.add(-1);
        } else {
            columns.extendAndSet(dataColumnOffset, NullMemory.INSTANCE);
            columns.extendAndSet(dataColumnOffset + 1, NullMemory.INSTANCE);
            nullSetters.add(NOOP);
            rowValueIsNotNull.add(COLUMN_DELETED_NULL_FLAG);
        }
    }

    private void configureColumns() {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(i, metadata.getColumnType(i));
        }
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
        tempPath.of(configuration.getDbRoot()).concat(tableToken);
        int tempPathTripLen = tempPath.size();

        path.trimTo(pathSize);
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
        path.trimTo(pathSize);
        TableUtils.charFileName(tempPath, columnName, columnNameTxn);
        TableUtils.charFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link char file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(pathSize);
        BitmapIndexUtils.keyFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.keyFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link key file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        tempPath.trimTo(tempPathTripLen);
        path.trimTo(pathSize);
        BitmapIndexUtils.valueFileName(tempPath, columnName, columnNameTxn);
        BitmapIndexUtils.valueFileName(path, columnName, COLUMN_NAME_TXN_NONE);
        if (-1 == ff.hardLink(tempPath.$(), path.$())) {
            // This is fine, Table Writer can rename or drop the column.
            LOG.info().$("failed to link value file [from=").$(tempPath)
                    .$(", to=").$(path)
                    .$(", errno=").$(ff.errno())
                    .I$();
            removeSymbolFiles(path, pathSize, columnName);
            configureEmptySymbol(columnWriterIndex);
            return;
        }

        path.trimTo(pathSize);
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
                        path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME);

                        // Does not matter which PartitionBy, as long as it is partitioned
                        // WAL tables must be partitioned
                        txReader.ofRO(path.$(), metadata.getTimestampType(), PartitionBy.DAY);
                        path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME);
                        columnVersionReader.ofRO(ff, path.$());

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

    private MemoryMA createAuxColumnMem(int columnType) {
        return ColumnType.isVarSize(columnType) ? Vm.getPMARInstance(configuration) : null;
    }

    private SegmentColumnRollSink createSegmentColumnRollSink() {
        if (columnConversionSink == null) {
            columnConversionSink = new SegmentColumnRollSink();
        } else {
            columnConversionSink.clear();
        }
        return columnConversionSink;
    }

    private int createSegmentDir(int segmentId) {
        path.trimTo(pathSize);
        path.slash().put(segmentId);
        final int segmentPathLen = path.size();
        segmentLockFd = acquireSegmentLock();
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL segment directory: ").put(path);
        }
        walDirectoryPolicy.initDirectory(path);
        path.trimTo(segmentPathLen);
        return segmentPathLen;
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        final MemoryMA primaryColumn = columns.getAndSetQuick(pi, null);
        final MemoryMA secondaryColumn = columns.getAndSetQuick(si, null);
        primaryColumn.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
        if (secondaryColumn != null) {
            secondaryColumn.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
        }
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final MemoryMA m = columns.getQuick(i);
                if (m != null) {
                    m.close(truncate, Vm.TRUNCATE_TO_POINTER);
                }
            }
        }
    }

    private void freeSymbolMapReaders() {
        Misc.freeObjListIfCloseable(symbolMapReaders);
    }

    private MemoryMA getAuxColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getAuxColumnOffset(column));
    }

    private long getColumnStructureVersion() {
        // Sequencer metadata version is the same as column structure version of the table.
        return metadata.getMetadataVersion();
    }

    private SymbolTable getConversionSymbolMapReader(int columnIndex) {
        if (conversionSymbolTable == null) {
            conversionSymbolTable = new ConversionSymbolTable();
        }
        conversionSymbolTable.of(this, columnIndex);
        return conversionSymbolTable;
    }

    private SymbolMapWriterLite getConversionSymbolMapWriter(int columnIndex) {
        if (conversionSymbolMap == null) {
            conversionSymbolMap = new ConversionSymbolMapWriter();
        }
        conversionSymbolMap.of(this, columnIndex);
        return conversionSymbolMap;
    }

    private long getDataAppendPageSize() {
        return tableToken.isSystem() ? configuration.getSystemWalDataAppendPageSize() : configuration.getWalDataAppendPageSize();
    }

    private MemoryMA getDataColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getDataColumnOffset(column));
    }

    private long getSequencerTxn() {
        long seqTxn;
        do {
            seqTxn = sequencer.nextTxn(tableToken, walId, getColumnStructureVersion(), segmentId, lastSegmentTxn, txnMinTimestamp, txnMaxTimestamp, segmentRowCount - currentTxnStartRowNum);
            if (seqTxn == NO_TXN) {
                applyMetadataChangeLog(Long.MAX_VALUE);
            }
        } while (seqTxn == NO_TXN);
        return lastSeqTxn = seqTxn;
    }

    private boolean hasDirtyColumns(long currentTxnStartRowNum) {
        for (int i = 0; i < columnCount; i++) {
            long writtenCount = rowValueIsNotNull.getQuick(i);
            if (writtenCount >= currentTxnStartRowNum && writtenCount != COLUMN_DELETED_NULL_FLAG) {
                return true;
            }
        }
        return false;
    }

    private boolean isTruncateFilesOnClose() {
        return this.walDirectoryPolicy.truncateFilesOnClose();
    }

    private void lockWal() {
        try {
            lockName(path);
            walLockFd = TableUtils.lock(ff, path.$());
        } finally {
            path.trimTo(pathSize);
        }

        if (walLockFd == -1) {
            throw CairoException.critical(ff.errno()).put("cannot lock table: ").put(path.$());
        }
    }

    private void markColumnRemoved(int columnIndex, int columnType) {
        if (ColumnType.isSymbol(columnType)) {
            removeSymbolMapReader(columnIndex);
        }
        final int pi = getDataColumnOffset(columnIndex);
        final int si = getAuxColumnOffset(columnIndex);
        freeNullSetter(nullSetters, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
        rowValueIsNotNull.setQuick(columnIndex, COLUMN_DELETED_NULL_FLAG);
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
        if (ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create WAL directory: ").put(path);
        }
        path.trimTo(walDirLength);
    }

    private void openColumnFiles(CharSequence columnName, int columnType, int columnIndex, int pathTrimToLen) {
        try {
            final MemoryMA dataMem = getDataColumn(columnIndex);
            totalSegmentsSize += dataMem.getAppendOffset();
            dataMem.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
            dataMem.of(
                    ff,
                    dFile(path.trimTo(pathTrimToLen), columnName),
                    getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WAL_WRITER,
                    configuration.getWriterFileOpenOpts(),
                    Files.POSIX_MADV_RANDOM
            );

            final MemoryMA auxMem = getAuxColumn(columnIndex);
            if (auxMem != null) {
                totalSegmentsSize += auxMem.getAppendOffset();
                auxMem.close(isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
                ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                columnTypeDriver.configureAuxMemMA(
                        ff,
                        auxMem,
                        iFile(path.trimTo(pathTrimToLen), columnName),
                        getDataAppendPageSize(),
                        MemoryTag.MMAP_TABLE_WAL_WRITER,
                        configuration.getWriterFileOpenOpts(),
                        Files.POSIX_MADV_RANDOM
                );
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    private void openNewSegment() {
        final int oldSegmentId = segmentId;
        final int newSegmentId = segmentId + 1;
        final long oldSegmentLockFd = segmentLockFd;
        segmentLockFd = -1;
        final long oldLastSegmentTxn = lastSegmentTxn;
        try {
            totalSegmentsRowCount += Math.max(0, segmentRowCount);
            currentTxnStartRowNum = 0;
            rowValueIsNotNull.fill(0, columnCount, -1);
            final int segmentPathLen = createSegmentDir(newSegmentId);
            segmentId = newSegmentId;
            final long dirFd;
            final int commitMode = configuration.getCommitMode();
            if (Os.isWindows() || commitMode == CommitMode.NOSYNC) {
                dirFd = -1;
            } else {
                dirFd = TableUtils.openRONoCache(ff, path.$(), LOG);
            }

            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0) {
                    final CharSequence columnName = metadata.getColumnName(i);
                    openColumnFiles(columnName, columnType, i, segmentPathLen);

                    if (columnType == ColumnType.SYMBOL && symbolMapReaders.size() > 0) {
                        final SymbolMapReader reader = symbolMapReaders.getQuick(i);
                        initialSymbolCounts.set(i, reader.getSymbolCount());
                        localSymbolIds.set(i, 0);
                        symbolMapNullFlags.set(i, reader.containsNullValue());
                        symbolMaps.getQuick(i).clear();
                        utf8SymbolMaps.getQuick(i).clear();
                    }
                } else {
                    rowValueIsNotNull.setQuick(i, COLUMN_DELETED_NULL_FLAG);
                }
            }

            segmentRowCount = 0;
            metadata.switchTo(path, segmentPathLen, isTruncateFilesOnClose());
            totalSegmentsSize += events.size();
            events.openEventFile(path, segmentPathLen, isTruncateFilesOnClose(), tableToken.isSystem());
            if (commitMode != CommitMode.NOSYNC) {
                events.sync();
            }

            if (dirFd != -1) {
                ff.fsyncAndClose(dirFd);
            }
            lastSegmentTxn = -1;
            LOG.info().$("opened WAL segment [path=").$substr(pathRootSize, path.parent()).I$();
        } finally {
            if (oldSegmentLockFd > -1) {
                releaseSegmentLock(oldSegmentId, oldSegmentLockFd, oldLastSegmentTxn);
            }
            path.trimTo(pathSize);
        }
    }

    private void releaseSegmentLock(int segmentId, long segmentLockFd, long segmentTxn) {
        if (ff.close(segmentLockFd)) {
            // if events file has some transactions
            if (segmentTxn >= 0) {
                sequencer.notifySegmentClosed(tableToken, lastSeqTxn, walId, segmentId);
                LOG.debug().$("released segment lock [walId=").$(walId)
                        .$(", segmentId=").$(segmentId)
                        .$(", fd=").$(segmentLockFd)
                        .$(']').$();
            } else {
                path.trimTo(pathSize).slash().put(segmentId);
                walDirectoryPolicy.rollbackDirectory(path);
                path.trimTo(pathSize);
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
        removeSymbolFiles(path, pathSize, metadata.getColumnName(index));
    }

    private void renameColumnFiles(int columnType, CharSequence columnName, CharSequence newName) {
        path.trimTo(pathSize).slash().put(segmentId);
        final Path tempPath = Path.PATH.get().of(path);

        if (ColumnType.isVarSize(columnType)) {
            final int trimTo = path.size();
            iFile(path, columnName);
            iFile(tempPath, newName);
            if (ff.rename(path.$(), tempPath.$()) != Files.FILES_RENAME_OK) {
                throw CairoException.critical(ff.errno())
                        .put("could not rename WAL column file [from=").put(path)
                        .put(", to=").put(tempPath)
                        .put(']');
            }
            path.trimTo(trimTo);
            tempPath.trimTo(trimTo);
        }

        dFile(path, columnName);
        dFile(tempPath, newName);
        if (ff.rename(path.$(), tempPath.$()) != Files.FILES_RENAME_OK) {
            throw CairoException.critical(ff.errno()).put("could not rename WAL column file [from=").put(path).put(", to=").put(tempPath).put(']');
        }
    }

    private void resetDataTxnProperties() {
        currentTxnStartRowNum = segmentRowCount;
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

    private void rollLastWalEventRecord(int newSegmentId, long uncommittedRows) {
        if (isCommittingData) {
            // Sometimes we only want to add a column without committing the data in the current wal segments in ILP.
            // When this happens the data stays in the WAL column files but is not committed
            // and the events file don't have a record about the column add transaction.
            // In this case we DO NOT roll back the last record in the events file.
            events.rollback();
        }
        path.trimTo(pathSize).slash().put(newSegmentId);
        events.openEventFile(path, path.size(), isTruncateFilesOnClose(), tableToken.isSystem());
        lastSegmentTxn = -1;
        if (isCommittingData) {
            // When current transaction is not a data transaction but a column add transaction
            // there is no need to add a record about it to the new segment event file.
            lastSegmentTxn = events.appendData(
                    lastTxnType,
                    0,
                    uncommittedRows,
                    txnMinTimestamp,
                    txnMaxTimestamp,
                    txnOutOfOrder,
                    lastMatViewRefreshBaseTxn,
                    lastMatViewRefreshTimestamp,
                    lastMatViewPeriodHi,
                    lastReplaceRangeLowTs,
                    lastReplaceRangeHiTs,
                    lastDedupMode
            );
        }
        events.sync();
    }

    private void rowAppend(ObjList<Runnable> activeNullSetters, long rowTimestamp) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < segmentRowCount) {
                activeNullSetters.getQuick(i).run();
            }
        }

        if (rowTimestamp > txnMaxTimestamp) {
            txnMaxTimestamp = rowTimestamp;
        } else {
            txnOutOfOrder |= (txnMaxTimestamp != rowTimestamp);
        }
        if (rowTimestamp < txnMinTimestamp) {
            txnMinTimestamp = rowTimestamp;
        }

        segmentRowCount++;
    }

    private void setAppendPosition(final long segmentRowCount) {
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (type > 0) {
                setAppendPosition0(i, segmentRowCount);
                rowValueIsNotNull.setQuick(i, segmentRowCount - 1);
            }
        }
    }

    private void setAppendPosition0(int columnIndex, long segmentRowCount) {
        MemoryMA dataMem = getDataColumn(columnIndex);
        MemoryMA auxMem = getAuxColumn(columnIndex);
        int columnType = metadata.getColumnType(columnIndex);
        if (columnType > 0) { // Not deleted
            final long rowCount = Math.max(0, segmentRowCount);
            final long dataMemOffset;
            if (ColumnType.isVarSize(columnType)) {
                assert auxMem != null;
                dataMemOffset = ColumnType.getDriver(columnType).setAppendAuxMemAppendPosition(auxMem, dataMem, columnType, rowCount);
            } else {
                dataMemOffset = rowCount << ColumnType.getWalDataColumnShl(columnType, columnIndex == metadata.getTimestampIndex());
            }
            dataMem.jumpTo(dataMemOffset);
        }
    }

    private void setColumnNull(int columnType, int columnIndex, long rowCount, int commitMode) {
        if (ColumnType.isVarSize(columnType)) {
            final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
            setVarColumnDataFileNull(columnTypeDriver, columnIndex, rowCount, commitMode);
            setVarColumnAuxFileNull(columnTypeDriver, columnIndex, rowCount, commitMode);
        } else {
            setFixColumnNulls(columnType, columnIndex, rowCount);
        }
    }

    private void setFixColumnNulls(int type, int columnIndex, long rowCount) {
        MemoryMA fixedSizeColumn = getDataColumn(columnIndex);
        long columnFileSize = rowCount * ColumnType.sizeOf(type);
        fixedSizeColumn.jumpTo(columnFileSize);
        if (columnFileSize > 0) {
            long address = TableUtils.mapRW(ff, fixedSizeColumn.getFd(), columnFileSize, MEM_TAG);
            try {
                TableUtils.setNull(type, address, rowCount);
            } finally {
                ff.munmap(address, columnFileSize, MEM_TAG);
            }
            ff.fsync(fixedSizeColumn.getFd());
        }
    }

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != segmentRowCount;
        rowValueIsNotNull.setQuick(columnIndex, segmentRowCount);
    }

    private void setVarColumnAuxFileNull(
            ColumnTypeDriver columnTypeDriver,
            int columnIndex,
            long rowCount,
            int commitMode
    ) {
        MemoryMA auxMem = getAuxColumn(columnIndex);
        final long auxMemSize = columnTypeDriver.getAuxVectorSize(rowCount);
        auxMem.jumpTo(auxMemSize);
        if (rowCount > 0) {
            final long auxMemAddr = TableUtils.mapRW(ff, auxMem.getFd(), auxMemSize, MEM_TAG);
            try {
                columnTypeDriver.setFullAuxVectorNull(auxMemAddr, rowCount);
                if (commitMode != CommitMode.NOSYNC) {
                    ff.msync(auxMemAddr, auxMemSize, commitMode == CommitMode.ASYNC);
                }
            } finally {
                ff.munmap(auxMemAddr, auxMemSize, MEM_TAG);
            }
        }
    }

    private void setVarColumnDataFileNull(ColumnTypeDriver columnTypeDriver, int columnIndex, long rowCount, int commitMode) {
        MemoryMA dataMem = getDataColumn(columnIndex);
        final long varColSize = rowCount * columnTypeDriver.getDataVectorMinEntrySize();
        dataMem.jumpTo(varColSize);
        if (rowCount > 0 && varColSize > 0) {
            final long dataMemAddr = TableUtils.mapRW(ff, dataMem.getFd(), varColSize, MEM_TAG);
            try {
                columnTypeDriver.setDataVectorEntriesToNull(dataMemAddr, rowCount);
                if (commitMode != CommitMode.NOSYNC) {
                    ff.msync(dataMemAddr, varColSize, commitMode == CommitMode.ASYNC);
                }
            } finally {
                ff.munmap(dataMemAddr, varColSize, MEM_TAG);
            }
        }
    }

    private void switchColumnsToNewSegment(SegmentColumnRollSink rollSink, int columnsToRoll, int convertColumnIndex) {
        for (int i = 0; i < columnsToRoll; i++) {
            final int columnType = metadata.getColumnType(i);
            if (columnType > 0) {
                if (i != convertColumnIndex) {
                    switchColumnsToNewSegmentRollColumn(rollSink, i, i);
                } else {
                    // Column is converted, the destination column objects are for the last added column
                    switchColumnsToNewSegmentRollColumn(rollSink, i, columnCount - 1);
                }
            }
        }
    }

    private void switchColumnsToNewSegmentRollColumn(SegmentColumnRollSink rollSink, int srcColumnIndex, int destColumnIndex) {
        long currentOffset = rollSink.getSrcPrimaryOffset(srcColumnIndex);
        MemoryMA primaryColumnFile = getDataColumn(srcColumnIndex);
        primaryColumnFile.jumpTo(currentOffset);
        primaryColumnFile.close(isTruncateFilesOnClose());

        MemoryMA auxColumn = getAuxColumn(srcColumnIndex);
        if (auxColumn != null) {
            long auxOffset = rollSink.getSrcAuxOffset(srcColumnIndex);
            auxColumn.jumpTo(auxOffset);
            auxColumn.close(isTruncateFilesOnClose());
        }

        long newSize = rollSink.getDestPrimarySize(srcColumnIndex);
        long newPrimaryFd = rollSink.getDestPrimaryFd(srcColumnIndex);
        MemoryMA destPrimeCol = getDataColumn(destColumnIndex);
        destPrimeCol.switchTo(ff, newPrimaryFd, getDataAppendPageSize(), newSize, isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);

        long newSecondaryFd = rollSink.getDestAuxFd(srcColumnIndex);
        if (newSecondaryFd > -1) {
            long secondarySize = rollSink.getDestAuxSize(srcColumnIndex);
            MemoryMA destAuxColumn = getAuxColumn(destColumnIndex);
            destAuxColumn.switchTo(ff, newSecondaryFd, getDataAppendPageSize(), secondarySize, isTruncateFilesOnClose(), Vm.TRUNCATE_TO_POINTER);
        }
    }

    private void syncIfRequired() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            final boolean async = commitMode == CommitMode.ASYNC;
            for (int i = 0, n = columns.size(); i < n; i++) {
                MemoryMA column = columns.getQuick(i);
                if (column != null) {
                    column.sync(async);
                }
            }
            events.sync();
        }
    }

    private static class ConversionSymbolMapWriter implements SymbolMapWriterLite {
        private int columnIndex;
        private IntList localSymbolIds;
        private CharSequenceIntHashMap symbolHashMap;
        private SymbolMapReader symbolMapReader;

        @Override
        public int resolveSymbol(CharSequence value) {
            return putSym0(columnIndex, value, symbolMapReader);
        }

        private int putSym0(int columnIndex, CharSequence utf16Value, SymbolMapReader symbolMapReader) {
            int key;
            if (utf16Value != null) {
                final CharSequenceIntHashMap utf16Map = symbolHashMap;
                final int index = utf16Map.keyIndex(utf16Value);
                if (index > -1) {
                    key = symbolMapReader.keyOf(utf16Value);
                    if (key == SymbolTable.VALUE_NOT_FOUND) {
                        // Add it to in-memory symbol map
                        // Locally added symbols must have a continuous range of keys
                        key = localSymbolIds.postIncrement(columnIndex);
                    }
                    // Chars.toString used as value is a parser buffer memory slice or mapped memory of symbolMapReader
                    utf16Map.putAt(index, Chars.toString(utf16Value), key);
                } else {
                    key = utf16Map.valueAt(index);
                }
            } else {
                key = SymbolTable.VALUE_IS_NULL;
            }
            return key;
        }

        void of(WalWriter writer, int columnIndex) {
            this.columnIndex = columnIndex;
            this.symbolMapReader = writer.getSymbolMapReader(columnIndex);
            this.symbolHashMap = writer.symbolMaps.getQuick(columnIndex);
            this.localSymbolIds = writer.localSymbolIds;
        }
    }


    private static class ConversionSymbolTable implements SymbolTable {
        private final IntList symbols = new IntList();
        private int symbolCountWatermark;
        private CharSequenceIntHashMap symbolHashMap;
        private SymbolMapReader symbolMapReader;

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key == SymbolTable.VALUE_IS_NULL) {
                return null;
            }
            if (key < symbolCountWatermark) {
                return symbolMapReader.valueOf(key);
            } else {
                int keyIndex = symbols.get(key - symbolCountWatermark);
                return symbolHashMap.keys().get(keyIndex);
            }
        }

        void of(WalWriter writer, int columnIndex) {
            this.symbolMapReader = writer.getSymbolMapReader(columnIndex);
            this.symbolCountWatermark = writer.getSymbolCountWatermark(columnIndex);

            symbols.clear();
            symbolHashMap = writer.symbolMaps.getQuick(columnIndex);

            int remapSize = writer.localSymbolIds.get(columnIndex);
            if (remapSize > 0) {
                symbols.setPos(remapSize);
                for (int i = 0, n = symbolHashMap.size(); i < n; i++) {
                    CharSequence symbolValue = symbolHashMap.keys().get(i);
                    int index = symbolHashMap.get(symbolValue);
                    if (index >= symbolCountWatermark) {
                        symbols.extendAndSet(index - symbolCountWatermark, i);
                    }
                }
            }
        }
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
                boolean isDedupKey,
                SecurityContext securityContext
        ) {
            validateNewColumnName(columnName);
            validateNewColumnType(columnType);
            structureVersion++;
        }

        @Override
        public void changeColumnType(CharSequence columnName, int newType, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential, SecurityContext securityContext) {
            int columnIndex = validateExistingColumnName(columnName, "cannot change type");
            validateNewColumnType(newType);
            int existingType = metadata.getColumnType(columnIndex);
            if (existingType == newType) {
                throw CairoException.nonCritical().put("column '").put(columnName)
                        .put("' type is already '").put(ColumnType.nameOf(newType)).put('\'');
            }
            structureVersion++;
        }

        @Override
        public void disableDeduplication() {
            structureVersion++;
        }

        @Override
        public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            boolean isSubsetOfOldKeys = true;
            for (int i = 0, n = columnsIndexes.size(); i < n; i++) {
                int columnIndex = (int) columnsIndexes.get(i);
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType < 0) {
                    throw CairoException.nonCritical().put("cannot use dropped column for deduplication [column=").put(metadata.getColumnName(columnIndex)).put(']');
                }
                isSubsetOfOldKeys &= metadata.isDedupKey(columnIndex);
            }
            structureVersion++;
            return isSubsetOfOldKeys;
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
        public int getTimestampType() {
            return metadata.getTimestampType();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnName) {
            validateExistingColumnName(columnName, "cannot remove");
            structureVersion++;
        }

        @Override
        public void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext) {
            validateExistingColumnName(columnName, "cannot rename");
            int columnIndexNew = metadata.getColumnIndexQuiet(newName);
            if (columnIndexNew > -1) {
                throw CairoException.nonCritical().put("cannot rename, column with the name already exists [table=").put(tableToken.getTableName())
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

        private int validateExistingColumnName(CharSequence columnName, String errorPrefix) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);
            if (columnIndex < 0) {
                throw CairoException.nonCritical().put(errorPrefix).put(", column does not exist [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }
            if (columnIndex == metadata.getTimestampIndex()) {
                throw CairoException.nonCritical().put(errorPrefix).put(" designated timestamp column [table=").put(tableToken.getTableName())
                        .put(", column=").put(columnName).put(']');
            }
            return columnIndex;
        }

        private void validateNewColumnName(CharSequence columnName) {
            if (!TableUtils.isValidColumnName(columnName, columnName.length())) {
                throw CairoException.nonCritical().put("invalid column name: ").put(columnName);
            }
            if (metadata.getColumnIndexQuiet(columnName) > -1) {
                throw CairoException.duplicateColumn(columnName);
            }
        }

        private void validateNewColumnType(int columnType) {
            if (columnType <= 0) {
                throw CairoException.nonCritical().put("invalid column type: ").put(columnType);
            }
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
                boolean isDedupKey,
                SecurityContext securityContext
        ) {
            int columnIndex = metadata.getColumnIndexQuiet(columnName);

            if (columnIndex < 0 || metadata.getColumnType(columnIndex) < 0) {
                long uncommittedRows = getUncommittedRowCount();
                if (currentTxnStartRowNum > 0) {
                    // Roll last transaction to new segment
                    rollUncommittedToNewSegment(-1, -1);
                }

                if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                    long segmentRowCount = getUncommittedRowCount();
                    metadata.addColumn(
                            columnName,
                            columnType,
                            isDedupKey,
                            symbolCacheFlag,
                            symbolCapacity
                    );
                    columnCount = metadata.getColumnCount();
                    columnIndex = columnCount - 1;
                    // create column file
                    configureColumn(columnIndex, columnType);
                    if (ColumnType.isSymbol(columnType)) {
                        configureSymbolMapWriter(columnIndex, columnName, 0, -1);
                    }

                    if (!rollSegmentOnNextRow) {
                        // this means we have rolled uncommitted rows to a new segment already
                        // we should switch metadata to this new segment
                        path.trimTo(pathSize).slash().put(segmentId);
                        // this will close old _meta file and create the new one
                        metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        openColumnFiles(columnName, columnType, columnIndex, path.size());
                        path.trimTo(pathSize);
                    }

                    // if we did not have to roll uncommitted rows to a new segment
                    // it will add the column file and switch metadata file on next row write
                    // as part of rolling to a new segment
                    if (uncommittedRows > 0) {
                        setColumnNull(columnType, columnIndex, segmentRowCount, configuration.getCommitMode());
                    }

                    if (securityContext != null) {
                        ddlListener.onColumnAdded(securityContext, metadata.getTableToken(), columnName);
                    }
                    LOG.info().$("added column to WAL [path=").$substr(pathRootSize, path)
                            .$(", columnName=").$safe(columnName)
                            .$(", type=").$(ColumnType.nameOf(columnType))
                            .I$();
                } else {
                    throw CairoException.critical(0).put("column '").put(columnName)
                            .put("' was added, cannot apply commit because of concurrent table definition change");
                }
            } else {
                if (metadata.getColumnType(columnIndex) == columnType) {
                    LOG.info().$("column has already been added by another WAL [path=").$substr(pathRootSize, path)
                            .$(", columnName=").$safe(columnName)
                            .I$();
                } else {
                    throw CairoException.nonCritical().put("column '").put(columnName).put("' already exists");
                }
            }
        }

        @Override
        public void changeColumnType(
                CharSequence columnNameSeq,
                int newType,
                int symbolCapacity,
                boolean symbolCacheFlag,
                boolean isIndexed,
                int indexValueBlockCapacity,
                boolean isSequential,
                SecurityContext securityContext
        ) {
            final int existingColumnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (existingColumnIndex > -1) {
                String columnName = metadata.getColumnName(existingColumnIndex);
                int existingColumnType = metadata.getColumnType(existingColumnIndex);
                if (existingColumnType > 0) {
                    if (existingColumnType != newType) {
                        // Configure new column, it will be used if the uncommitted data is rolled to a new segment
                        int newColumnIndex = columnCount;
                        configureColumn(newColumnIndex, newType);
                        if (ColumnType.isSymbol(newType)) {
                            configureSymbolMapWriter(newColumnIndex, columnName, 0, -1);
                        }
                        columnCount++;

                        long rowsRemainInCurrentSegment = currentTxnStartRowNum;
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(existingColumnIndex, newType);

                        if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                            metadata.changeColumnType(
                                    columnName,
                                    newType,
                                    symbolCapacity,
                                    symbolCacheFlag,
                                    isIndexed,
                                    indexValueBlockCapacity
                            );
                            path.trimTo(pathSize).slash().put(segmentId);

                            markColumnRemoved(existingColumnIndex, existingColumnType);
                            if (!rollSegmentOnNextRow) {
                                // this means we have rolled uncommitted rows to a new segment already
                                // we should switch metadata to this new segment
                                path.trimTo(pathSize).slash().put(segmentId);
                                // this will close old _meta file and create the new one
                                metadata.switchTo(path, path.size(), isTruncateFilesOnClose());

                                if (segmentRowCount == 0) {
                                    openColumnFiles(columnName, newType, newColumnIndex, path.size());
                                }
                            }

                            if (rowsRemainInCurrentSegment == 0) {
                                // if we did not have to roll uncommitted rows to a new segment
                                // remove .i files when converting var type to fixed
                                if (ColumnType.isVarSize(existingColumnType) && !ColumnType.isVarSize(newType)) {
                                    path.trimTo(pathSize).slash().put(segmentId);
                                    LPSZ lpsz = iFile(path, columnName);
                                    if (ff.exists(lpsz)) {
                                        ff.remove(lpsz);
                                    }
                                }
                            }
                            path.trimTo(pathSize);
                        } else {
                            throw CairoException.critical(0).put("column '").put(columnName)
                                    .put("' was removed, cannot apply commit because of concurrent table definition change");
                        }
                    } else {
                        throw CairoException.nonCritical().put("column '").put(columnName)
                                .put("' type is already '").put(ColumnType.nameOf(newType)).put('\'');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column '").put(columnNameSeq).put("' does not exist");
            }
        }

        @Override
        public void disableDeduplication() {
            metadata.disableDeduplicate();
        }

        @Override
        public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
            return metadata.enableDeduplicationWithUpsertKeys();
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
        public int getTimestampType() {
            return metadata.getTimestampType();
        }

        @Override
        public void removeColumn(@NotNull CharSequence columnNameSeq) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (columnIndex > -1) {
                String columnName = metadata.getColumnName(columnIndex);
                int type = metadata.getColumnType(columnIndex);
                if (type > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(-1, -1);
                    }

                    if (currentTxnStartRowNum == 0 || segmentRowCount == currentTxnStartRowNum) {
                        int index = metadata.getColumnIndex(columnName);
                        metadata.removeColumn(columnName);
                        columnCount = metadata.getColumnCount();

                        if (!rollSegmentOnNextRow) {
                            // this means we have rolled uncommitted rows to a new segment already
                            // we should switch metadata to this new segment
                            path.trimTo(pathSize).slash().put(segmentId);
                            // this will close old _meta file and create the new one
                            metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        markColumnRemoved(index, type);
                        path.trimTo(pathSize);
                        LOG.info().$("removed column from WAL [path=").$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                                .$(", columnName=").$safe(columnName).I$();
                    } else {
                        throw CairoException.critical(0)
                                .put("column was removed, cannot apply commit because of concurrent table definition change")
                                .put(" [column=").put(columnName).put(']');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column does not exist [column=").put(columnNameSeq).put(']');
            }
        }

        @Override
        public void renameColumn(
                @NotNull CharSequence columnNameSeq,
                @NotNull CharSequence newColumnName,
                SecurityContext securityContext
        ) {
            final int columnIndex = metadata.getColumnIndexQuiet(columnNameSeq);
            if (columnIndex > -1) {
                String columnName = metadata.getColumnName(columnIndex);
                int columnType = metadata.getColumnType(columnIndex);
                if (columnType > 0) {
                    if (currentTxnStartRowNum > 0) {
                        // Roll last transaction to new segment
                        rollUncommittedToNewSegment(-1, -1);
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
                            path.trimTo(pathSize).slash().put(segmentId);
                            // this will close old _meta file and create the new one
                            metadata.switchTo(path, path.size(), isTruncateFilesOnClose());
                            renameColumnFiles(columnType, columnName, newColumnName);
                        }
                        // if we did not have to roll uncommitted rows to a new segment
                        // it will switch metadata file on next row write
                        // as part of rolling to a new segment

                        if (securityContext != null) {
                            ddlListener.onColumnRenamed(securityContext, metadata.getTableToken(), columnName, newColumnName);
                        }

                        path.trimTo(pathSize);
                        LOG.info().$("renamed column in WAL [path=")
                                .$substr(pathRootSize, path).$(Files.SEPARATOR).$(segmentId)
                                .$(", columnName=").$safe(columnName)
                                .$(", newColumnName=").$safe(newColumnName)
                                .I$();
                    } else {
                        throw CairoException.critical(0)
                                .put("column was removed, cannot apply commit because of concurrent table definition change")
                                .put(" [column=").put(columnName).put(']');
                    }
                }
            } else {
                throw CairoException.nonCritical().put("column does not exist [column=")
                        .put(columnNameSeq).put(']');
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
        private long timestamp;

        @Override
        public void append() {
            rowAppend(nullSetters, timestamp);
        }

        @Override
        public void cancel() {
            setAppendPosition(segmentRowCount);
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView arrayView) {
            ArrayTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex),
                    getPrimaryColumn(columnIndex),
                    arrayView
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            getPrimaryColumn(columnIndex).putBool(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            getPrimaryColumn(columnIndex).putByte(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putChar(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            getPrimaryColumn(columnIndex).putDouble(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            getPrimaryColumn(columnIndex).putFloat(value);
            setRowValueNotNull(columnIndex);
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
            getPrimaryColumn(columnIndex).putInt(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            getPrimaryColumn(columnIndex).putLong(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong128(int columnIndex, long lo, long hi) {
            MemoryMA primaryColumn = getPrimaryColumn(columnIndex);
            primaryColumn.putLong(lo);
            primaryColumn.putLong(hi);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            getPrimaryColumn(columnIndex).putLong256(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
            getPrimaryColumn(columnIndex).putLong256Utf8(hexString);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            getPrimaryColumn(columnIndex).putShort(value);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStrUtf8(value));
            setRowValueNotNull(columnIndex);
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
            // otherwise it will write nothing.
            final SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
            if (symbolMapReader != null) {
                Utf8StringIntHashMap utf8Map = utf8SymbolMaps.getQuick(columnIndex);
                int index = utf8Map.keyIndex(value);
                if (index < 0) {
                    getPrimaryColumn(columnIndex).putInt(utf8Map.valueAt(index));
                    setRowValueNotNull(columnIndex);
                } else {
                    // slow path, symbol is not in utf8 cache
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
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    tempUtf8Sink
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex), getPrimaryColumn(columnIndex),
                    value
            );
            setRowValueNotNull(columnIndex);
        }

        private MemoryMA getPrimaryColumn(int columnIndex) {
            return columns.getQuick(getDataColumnOffset(columnIndex));
        }

        private MemoryMA getSecondaryColumn(int columnIndex) {
            return columns.getQuick(getAuxColumnOffset(columnIndex));
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

            getPrimaryColumn(columnIndex).putInt(key);
            setRowValueNotNull(columnIndex);
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
            getPrimaryColumn(timestampIndex).putLong128(value, segmentRowCount);
            setRowValueNotNull(timestampIndex);
            this.timestamp = value;
        }
    }
}
