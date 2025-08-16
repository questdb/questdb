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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BitSet;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Pool;
import io.questdb.std.Utf8StringIntHashMap;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.ANY_TABLE_VERSION;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TableUpdateDetails implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableUpdateDetails.class);
    private static final DirectUtf8SymbolLookup NOT_FOUND_LOOKUP = value -> SymbolTable.VALUE_NOT_FOUND;
    private final long commitInterval;
    private final boolean commitOnClose;
    private final DefaultColumnTypes defaultColumnTypes;
    private final long defaultMaxUncommittedRows;
    private final CairoEngine engine;
    private final ThreadLocalDetails[] localDetailsArray;
    private final MillisecondClock millisecondClock;
    // Set only for WAL tables, i.e. when writerThreadId == -1.
    private final SecurityContext ownSecurityContext;
    private final Utf8String tableNameUtf8;
    private final TableToken tableToken;
    private final TimestampDriver timestampDriver;
    private final int timestampIndex;
    private final long writerTickRowsCountMod;
    protected TableWriterAPI writerAPI;
    private boolean assignedToJob = false;
    // Number of rows processed since the last reshuffle, this is an estimate because it is incremented by
    // multiple threads without synchronisation
    private long eventsProcessedSinceReshuffle = 0;
    private boolean isDropped;
    private long lastMeasurementMillis = Long.MAX_VALUE;
    private long latestKnownMetadataVersion;
    private MetadataService metadataService;
    private int networkIOOwnerCount = 0;
    private long nextCommitTime;
    private volatile boolean writerInError;
    private int writerThreadId;

    public TableUpdateDetails(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            @Nullable SecurityContext ownSecurityContext,
            TableWriterAPI writer,
            int writerThreadId,
            NetworkIOJob[] netIoJobs,
            DefaultColumnTypes defaultColumnTypes,
            Utf8String tableNameUtf8
    ) {
        this.writerThreadId = writerThreadId;
        this.engine = engine;
        this.ownSecurityContext = ownSecurityContext;
        this.defaultColumnTypes = defaultColumnTypes;
        final CairoConfiguration cairoConfiguration = engine.getConfiguration();
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.writerTickRowsCountMod = cairoConfiguration.getWriterTickRowsCountMod();
        this.defaultMaxUncommittedRows = cairoConfiguration.getMaxUncommittedRows();
        this.writerAPI = writer;
        this.timestampIndex = writer.getMetadata().getTimestampIndex();
        this.timestampDriver = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType());
        this.tableToken = writer.getTableToken();
        this.metadataService = writer.supportsMultipleWriters() ? null : (MetadataService) writer;
        this.commitInterval = configuration.getCommitInterval();
        this.nextCommitTime = millisecondClock.getTicks() + commitInterval;

        final int n = netIoJobs.length;
        this.localDetailsArray = new ThreadLocalDetails[n];
        for (int i = 0; i < n; i++) {
            //noinspection resource
            this.localDetailsArray[i] = new ThreadLocalDetails(netIoJobs[i].getSymbolCachePool());
        }
        this.tableNameUtf8 = tableNameUtf8;
        this.commitOnClose = true;
    }

    protected TableUpdateDetails(
            CairoEngine engine,
            @Nullable SecurityContext ownSecurityContext,
            TableWriterAPI writer,
            int writerThreadId,
            DefaultColumnTypes defaultColumnTypes,
            Utf8String tableNameUtf8,
            Pool<SymbolCache> symbolCachePool,
            long commitInterval,
            boolean commitOnClose,
            long maxUncommittedRows
    ) {
        this.writerThreadId = writerThreadId;
        this.engine = engine;
        this.ownSecurityContext = ownSecurityContext;
        this.defaultColumnTypes = defaultColumnTypes;
        this.commitOnClose = commitOnClose;
        final CairoConfiguration cairoConfiguration = engine.getConfiguration();
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.writerTickRowsCountMod = cairoConfiguration.getWriterTickRowsCountMod();
        this.defaultMaxUncommittedRows = maxUncommittedRows;
        this.writerAPI = writer;
        this.timestampIndex = writer.getMetadata().getTimestampIndex();
        this.timestampDriver = ColumnType.getTimestampDriver(writer.getMetadata().getTimestampType());
        this.tableToken = writer.getTableToken();
        this.metadataService = writer.supportsMultipleWriters() ? null : (MetadataService) writer;
        this.commitInterval = commitInterval;
        this.nextCommitTime = millisecondClock.getTicks() + this.commitInterval;
        this.localDetailsArray = new ThreadLocalDetails[]{new ThreadLocalDetails(symbolCachePool)};
        this.tableNameUtf8 = tableNameUtf8;
    }

    public void addReference(int workerId) {
        if (!isWal()) {
            networkIOOwnerCount++;
            LOG.info()
                    .$("network IO thread using table [workerId=").$(workerId)
                    .$(", tableName=").$(tableToken)
                    .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                    .I$();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            closeNoLock();
        }
    }

    public void closeLocals() {
        for (int n = 0; n < localDetailsArray.length; n++) {
            LOG.info().$("closing table parsers [tableName=").$(tableToken).$(']').$();
            localDetailsArray[n] = Misc.free(localDetailsArray[n]);
        }
    }

    public void closeNoLock() {
        if (writerThreadId != Integer.MIN_VALUE) {
            LOG.info().$("closing table writer [tableName=").$(tableToken).$(']').$();
            closeLocals();
            if (writerAPI != null) {
                try {
                    if (commitOnClose) {
                        authorizeCommit();
                        writerAPI.commit();
                    }
                } catch (CairoException ex) {
                    if (!ex.isTableDropped()) {
                        LOG.error().$("cannot commit writer transaction, rolling back before releasing it [table=").$(tableToken).$(",ex=").$((Throwable) ex).I$();
                    }
                } catch (Throwable ex) {
                    LOG.error().$("cannot commit writer transaction, rolling back before releasing it [table=").$(tableToken).$(",ex=").$(ex).I$();
                } finally {
                    // returning to pool rolls back the transaction
                    writerAPI = Misc.free(writerAPI);
                    metadataService = null;
                }
            }
            writerThreadId = Integer.MIN_VALUE;
        }
    }

    public void commit(boolean withLag) throws CommitFailedException {
        if (writerAPI.getUncommittedRowCount() > 0) {
            try {
                authorizeCommit();
                if (withLag) {
                    writerAPI.ic();
                } else {
                    writerAPI.commit();
                }
            } catch (CairoException ex) {
                if (!ex.isTableDropped()) {
                    handleCommitException(ex);
                }
                throw CommitFailedException.instance(ex, ex.isTableDropped());
            } catch (Throwable ex) {
                handleCommitException(ex);
                throw CommitFailedException.instance(ex, false);
            }
        }
        if (isWal() && tableToken != engine.getTableTokenIfExists(tableToken.getTableName())) {
            setWriterInError();
        }
    }

    public long getEventsProcessedSinceReshuffle() {
        return eventsProcessedSinceReshuffle;
    }

    public long getLastMeasurementMillis() {
        return lastMeasurementMillis;
    }

    public MillisecondClock getMillisecondClock() {
        return millisecondClock;
    }

    public int getNetworkIOOwnerCount() {
        return networkIOOwnerCount;
    }

    public String getTableNameUtf16() {
        return tableToken.getTableName();
    }

    public Utf8String getTableNameUtf8() {
        return tableNameUtf8;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public int getWriterThreadId() {
        return writerThreadId;
    }

    public void incrementEventsProcessedSinceReshuffle() {
        ++eventsProcessedSinceReshuffle;
    }

    public boolean isAssignedToJob() {
        return assignedToJob;
    }

    public boolean isDropped() {
        return this.isDropped;
    }

    public boolean isWal() {
        return tableToken.isWal();
    }

    public boolean isWriterInError() {
        return writerInError;
    }

    public void removeReference(int workerId) {
        if (!isWal()) {
            networkIOOwnerCount--;
            localDetailsArray[workerId].clear();
            LOG.info()
                    .$("network IO thread released table [workerId=").$(workerId)
                    .$(", tableName=").$(tableToken)
                    .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                    .I$();
        }
    }

    public void rollback() {
        writerAPI.rollback();
    }

    public void setAssignedToJob(boolean assignedToJob) {
        this.assignedToJob = assignedToJob;
    }

    public void setIsDropped() {
        this.isDropped = true;
    }

    public void setWriterInError() {
        writerInError = true;
    }

    public void tick() {
        if (metadataService != null) {
            metadataService.tick();
        }
    }

    private void authorizeCommit() {
        if (ownSecurityContext != null) {
            ownSecurityContext.authorizeInsert(tableToken);
        }
    }

    private long getMetaMaxUncommittedRows() {
        if (metadataService != null) {
            return metadataService.getMetaMaxUncommittedRows();
        }
        return defaultMaxUncommittedRows;
    }

    private void handleCommitException(Throwable ex) {
        setWriterInError();
        LOG.error().$("could not commit [table=").$(tableToken).$(", e=").$(ex).I$();
        try {
            writerAPI.rollback();
        } catch (Throwable th) {
            LOG.error().$("could not perform emergency rollback [table=").$(tableToken).$(", e=").$(th).I$();
        }
    }

    long commitIfIntervalElapsed(long wallClockMillis) throws CommitFailedException {
        if (wallClockMillis < nextCommitTime) {
            return nextCommitTime;
        }
        if (writerAPI != null) {
            final long start = millisecondClock.getTicks();
            final boolean withLag = wallClockMillis - lastMeasurementMillis < commitInterval;
            LOG.debug().$("time-based commit ").$(withLag ? "with lag " : "")
                    .$("[rows=").$(writerAPI.getUncommittedRowCount())
                    .$(", table=").$(tableToken).I$();
            commit(withLag);
            // Do not commit row by row if the commit takes longer than commitInterval.
            // Exclude time to commit from the commit interval.
            nextCommitTime += commitInterval + millisecondClock.getTicks() - start;
        }
        return nextCommitTime;
    }

    void commitIfMaxUncommittedRowsCountReached() throws CommitFailedException {
        final long rowsSinceCommit = writerAPI.getUncommittedRowCount();
        if (rowsSinceCommit < getMetaMaxUncommittedRows()) {
            if ((rowsSinceCommit & writerTickRowsCountMod) == 0) {
                // Tick without commit. Some tick commands may force writer to commit though.
                tick();
            }
            return;
        }
        nextCommitTime = millisecondClock.getTicks() + commitInterval;

        LOG.debug().$("max-uncommitted-rows commit with lag [rows=").$(writerAPI.getUncommittedRowCount())
                .$(", table=").$(tableToken).I$();
        try {
            commit(true);
        } catch (CommitFailedException ex) {
            throw ex;
        } catch (Throwable th) {
            LOG.error()
                    .$("could not commit line protocol measurement [tableName=").$(writerAPI.getTableToken())
                    .$(", message=").$safe(th.getMessage())
                    .$(th)
                    .I$();
            writerAPI.rollback();
            throw CommitFailedException.instance(th, false);
        }

        // Tick after commit.
        tick();
    }

    ThreadLocalDetails getThreadLocalDetails(int workerId) {
        lastMeasurementMillis = millisecondClock.getTicks();
        return localDetailsArray[workerId];
    }

    TimestampDriver getTimestampDriver() {
        return timestampDriver;
    }

    int getTimestampIndex() {
        return timestampIndex;
    }

    TableWriterAPI getWriter() {
        return writerAPI;
    }

    void releaseWriter(boolean commit) {
        if (writerAPI != null) {
            try {
                if (commit) {
                    LOG.debug().$("release commit [table=").$(tableToken).I$();
                    authorizeCommit();
                    writerAPI.commit();
                }
            } catch (Throwable ex) {
                LOG.error().$("writer commit failed, force closing it [table=").$(tableToken).$(",ex=").$(ex).I$();
            } finally {
                // writer or FS can be in a bad state
                // do not leave writer locked
                writerAPI = Misc.free(writerAPI);
                metadataService = null;
            }
        }
    }

    public class ThreadLocalDetails implements Closeable {
        static final int COLUMN_NOT_FOUND = -1;
        static final int DUPLICATED_COLUMN = -2;
        // tracking of processed columns by their name, duplicates will be ignored
        // columns end up in this set only if their index cannot be resolved, i.e. new columns
        private final LowerCaseCharSequenceHashSet addedColsUtf16 = new LowerCaseCharSequenceHashSet();
        // maps column names to their indexes
        // keys are mangled strings created from the utf-8 encoded byte representations of the column names
        private final Utf8StringIntHashMap columnIndexByNameUtf8 = new Utf8StringIntHashMap();
        private final IntList columnIndices = new IntList();
        // maps column names to their types
        // will be populated for dynamically added columns only
        private final Utf8StringIntHashMap columnTypeByNameUtf8 = new Utf8StringIntHashMap();
        // indexed by colIdx + 1, first value accounts for spurious, new cols (index -1)
        private final IntList columnTypeMeta = new IntList();
        private final IntList columnTypes = new IntList();
        private final Path path = new Path();
        // tracking of processed columns by their index, duplicates will be ignored
        private final BitSet processedCols = new BitSet();
        private final ObjList<SymbolCache> symbolCacheByColumnIndex = new ObjList<>();
        private final Pool<SymbolCache> symbolCachePool;
        private final StringSink tempSink = new StringSink();
        private boolean clean = true;
        private String colNameUtf16;
        private Utf8String colNameUtf8;
        private GenericRecordMetadata latestKnownMetadata;
        private String symbolNameTemp;
        private TxReader txReader;

        ThreadLocalDetails(
                Pool<SymbolCache> symbolCachePool
        ) {
            // symbol caches are passed from the outside
            // to provide global lifecycle management for when ThreadLocalDetails cease to exist
            // the cache continue to live
            this.symbolCachePool = symbolCachePool;
            columnTypeMeta.add(0);
        }

        public void addColumn(String columnNameUtf16, int columnWriterIndex, int columnType) {
            if (latestKnownMetadata != null) {
                latestKnownMetadata.add(
                        new TableColumnMetadata(
                                columnNameUtf16,
                                columnType,
                                false,
                                0,
                                false,
                                null,
                                columnWriterIndex,
                                false
                        )
                );
                latestKnownMetadataVersion++;
            }
        }

        @Override
        public void close() {
            Misc.freeObjList(symbolCacheByColumnIndex);
            Misc.free(path);
            txReader = Misc.free(txReader);
        }

        private DirectUtf8SymbolLookup addSymbolCache(int colWriterIndex) {
            try (TableReader reader = engine.getReader(tableToken)) {
                final int symIndex = resolveSymbolIndexAndName(reader.getMetadata(), colWriterIndex);
                if (symbolNameTemp == null || symIndex < 0) {
                    // looks like the column has just been added to the table, and
                    // the reader is a bit behind and cannot see the column, yet
                    // we will pass the symbol as string
                    return NOT_FOUND_LOOKUP;
                }
                final CairoConfiguration cairoConfiguration = engine.getConfiguration();
                path.of(cairoConfiguration.getDbRoot()).concat(tableToken);
                SymbolCache symCache = symbolCachePool.pop();

                if (this.clean) {
                    if (this.txReader == null) {
                        this.txReader = new TxReader(cairoConfiguration.getFilesFacade());
                    }
                    int pathLen = path.size();
                    this.txReader.ofRO(path.concat(TXN_FILE_NAME).$(), reader.getMetadata().getTimestampType(), reader.getPartitionedBy());
                    path.trimTo(pathLen);
                    this.clean = false;
                }

                long columnNameTxn = reader.getColumnVersionReader().getDefaultColumnNameTxn(colWriterIndex);
                assert symIndex <= colWriterIndex;
                symCache.of(
                        cairoConfiguration,
                        writerAPI,
                        colWriterIndex,
                        path,
                        symbolNameTemp,
                        symIndex,
                        txReader,
                        columnNameTxn
                );
                symbolCacheByColumnIndex.extendAndSet(colWriterIndex, symCache);
                return symCache;
            }
        }

        private GenericRecordMetadata deepCopyOfDense(TableRecordMetadata that) {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            for (int i = 0, n = that.getColumnCount(); i < n; i++) {
                int columnType = that.getColumnType(i);
                if (columnType > 0) {
                    metadata.add(
                            new TableColumnMetadata(
                                    that.getColumnName(i),
                                    that.getColumnType(i),
                                    that.isColumnIndexed(i),
                                    that.getIndexValueBlockCapacity(i),
                                    that.isSymbolTableStatic(i),
                                    that.getMetadata(i),
                                    that.getWriterIndex(i),
                                    that.isDedupKey(i)
                            )
                    );
                }
            }
            metadata.setTimestampIndex(that.getTimestampIndex());
            return metadata;
        }

        private int getColumnIndex0(DirectUtf8Sequence colNameUtf8, @NotNull TableRecordMetadata metadata) {
            // lookup was unsuccessful we have to check whether the column can be passed by name to the writer
            final CharSequence colNameUtf16 = utf8ToUtf16(colNameUtf8);
            final int index = addedColsUtf16.keyIndex(colNameUtf16);
            if (index > -1) {
                // column has not been sent to the writer by name on this line before
                // we can try to resolve column index using table reader
                int colIndex = metadata.getColumnIndexQuiet(colNameUtf16);
                int colWriterIndex = colIndex < 0 ? colIndex : metadata.getWriterIndex(colIndex);
                Utf8String onHeapColNameUtf8 = Utf8String.newInstance(colNameUtf8);
                if (colWriterIndex > -1) {
                    // keys of this map will be checked against DirectByteCharSequence when get() is called
                    // DirectByteCharSequence.equals() compares chars created from each byte, basically it
                    // assumes that each char is encoded on a single byte (ASCII)
                    // utf8BytesToString() is used here instead of a simple toString() call to make sure
                    // column names with non-ASCII chars are handled properly
                    columnIndexByNameUtf8.put(onHeapColNameUtf8, colWriterIndex);

                    if (processedCols.getAndSet(colWriterIndex)) {
                        // column has been passed by index earlier on this event, duplicate should be skipped
                        return DUPLICATED_COLUMN;
                    }
                    return colWriterIndex;
                }
                // cannot not resolve column index even from the reader
                // column will be passed to the writer by name
                this.colNameUtf16 = Chars.toString(colNameUtf16);
                this.colNameUtf8 = onHeapColNameUtf8;
                addedColsUtf16.addAt(index, this.colNameUtf16);
                return COLUMN_NOT_FOUND;
            }
            // column has been passed by name earlier on this event, duplicate should be skipped
            return DUPLICATED_COLUMN;
        }

        private int getColumnWriterIndex(CharSequence colNameUtf16) {
            assert latestKnownMetadata != null;
            int colIndex = latestKnownMetadata.getColumnIndexQuiet(colNameUtf16);
            if (colIndex < 0) {
                return colIndex;
            }
            int writerColIndex = latestKnownMetadata.getWriterIndex(colIndex);
            updateColumnTypeCache(colIndex, writerColIndex, latestKnownMetadata);
            return writerColIndex;
        }

        private int resolveSymbolIndexAndName(TableRecordMetadata metadata, int colWriterIndex) {
            symbolNameTemp = null;
            int symIndex = -1;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getWriterIndex(i) == colWriterIndex) {
                    if (!ColumnType.isSymbol(metadata.getColumnType(i))) {
                        return -1;
                    }
                    symIndex++;
                    symbolNameTemp = metadata.getColumnName(i);
                    break;
                }
                if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                    symIndex++;
                }
            }
            return symIndex;
        }

        private void updateColumnTypeCache(int colIndex, int writerColIndex, GenericRecordMetadata metadata) {
            final int colType = metadata.getColumnType(colIndex);
            final int geoHashBits = ColumnType.getGeoHashBits(colType);
            columnTypes.extendAndSet(writerColIndex, colType);
            columnTypeMeta.extendAndSet(
                    writerColIndex + 1,
                    geoHashBits == 0 ? 0 : Numbers.encodeLowHighShorts((short) geoHashBits, ColumnType.tagOf(colType))
            );
        }

        void addColumnType(int columnWriterIndex, int colType) {
            columnIndices.add(columnWriterIndex);
            columnTypes.add(colType);
        }

        void clear() {
            columnIndexByNameUtf8.clear();
            columnTypeByNameUtf8.clear();
            for (int n = 0, sz = symbolCacheByColumnIndex.size(); n < sz; n++) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuick(n);
                if (symCache != null) {
                    symCache.close();
                    symbolCachePool.push(symCache);
                }
            }
            symbolCacheByColumnIndex.clear();
            columnTypes.clear();
            columnIndices.clear();
            columnTypeMeta.clear();
            columnTypeMeta.add(0);
            if (txReader != null) {
                txReader.clear();
            }
            clean = true;
        }

        void clearColumnTypes() {
            columnTypes.clear();
            columnIndices.clear();
        }

        void clearProcessedColumns() {
            processedCols.clear();
            if (addedColsUtf16.size() > 0) {
                addedColsUtf16.clear();
            }
        }

        String getColNameUtf16() {
            assert colNameUtf16 != null;
            return colNameUtf16;
        }

        Utf8String getColNameUtf8() {
            assert colNameUtf8 != null;
            return colNameUtf8;
        }

        int getColumnIndex(int colIndex) {
            return columnIndices.getQuick(colIndex);
        }

        int getColumnType(int colIndex) {
            return columnTypes.getQuick(colIndex);
        }

        int getColumnType(Utf8String colName, LineTcpParser.ProtoEntity entity) {
            int colType = columnTypeByNameUtf8.get(colName);
            if (colType < 0) {
                colType = defaultColumnTypes.DEFAULT_COLUMN_TYPES[entity.getType()];
                if (colType == ColumnType.ARRAY) {
                    colType = entity.getArray().getType();
                }
                columnTypeByNameUtf8.put(colName, colType);
            }
            return colType;
        }

        int getColumnTypeMeta(int colIndex) {
            return columnTypeMeta.getQuick(colIndex + 1); // first val accounts for new cols, index -1
        }

        // returns the column index for column name passed in colNameUtf8,
        // or COLUMN_NOT_FOUND if column index cannot be resolved (i.e. new column),
        // or DUPLICATED_COLUMN if the column has already been processed on the current event
        int getColumnWriterIndex(DirectUtf8Sequence colNameUtf8) {
            int colWriterIndex = columnIndexByNameUtf8.get(colNameUtf8);
            if (colWriterIndex < 0) {
                // lookup was unsuccessful we have to check whether the column can be passed by name to the writer
                final CharSequence colNameUtf16 = utf8ToUtf16(colNameUtf8);
                final int index = addedColsUtf16.keyIndex(colNameUtf16);
                if (index > -1) {
                    // column has not been sent to the writer by name on this line before
                    // we can try to resolve column index using table reader
                    colWriterIndex = getColumnWriterIndex(colNameUtf16);
                    Utf8String onHeapColNameUtf8 = Utf8String.newInstance(colNameUtf8);
                    if (colWriterIndex > -1) {
                        // keys of this map will be checked against DirectByteCharSequence when get() is called
                        // DirectByteCharSequence.equals() compares chars created from each byte, basically it
                        // assumes that each char is encoded on a single byte (ASCII)
                        // utf8BytesToString() is used here instead of a simple toString() call to make sure
                        // column names with non-ASCII chars are handled properly
                        columnIndexByNameUtf8.put(onHeapColNameUtf8, colWriterIndex);
                    } else {
                        // cannot not resolve column index even from the reader
                        // column will be passed to the writer by name
                        this.colNameUtf16 = Chars.toString(colNameUtf16);
                        this.colNameUtf8 = onHeapColNameUtf8;
                        addedColsUtf16.addAt(index, this.colNameUtf16);
                        return COLUMN_NOT_FOUND;
                    }
                } else {
                    // column has been passed by name earlier on this event, duplicate should be skipped
                    return DUPLICATED_COLUMN;
                }
            }

            if (processedCols.getAndSet(colWriterIndex)) {
                // column has been passed by index earlier on this event, duplicate should be skipped
                return DUPLICATED_COLUMN;
            }
            return colWriterIndex;
        }

        int getColumnWriterIndex(DirectUtf8Sequence colNameUtf8, @NotNull TableRecordMetadata metadata) {
            final int colWriterIndex = columnIndexByNameUtf8.get(colNameUtf8);
            if (colWriterIndex < 0) {
                // Hot path optimisation to allow the body of the current method to be small
                // enough for inlining. Rarely used code is extracted into a method call.
                return getColumnIndex0(colNameUtf8, metadata);
            }

            if (processedCols.getAndSet(colWriterIndex)) {
                // column has been passed by index earlier on this event, duplicate should be skipped
                return DUPLICATED_COLUMN;
            }
            return colWriterIndex;
        }

        long getMetadataVersion() {
            if (latestKnownMetadata != null) {
                return latestKnownMetadataVersion;
            }
            return ANY_TABLE_VERSION;
        }

        DirectUtf8SymbolLookup getSymbolLookup(int columnIndex) {
            if (columnIndex > -1) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuiet(columnIndex);
                if (symCache != null) {
                    return symCache;
                }
                return addSymbolCache(columnIndex);
            }
            return NOT_FOUND_LOOKUP;
        }

        void removeFromCaches(DirectUtf8Sequence colNameUtf8) {
            columnIndexByNameUtf8.remove(colNameUtf8);
            addedColsUtf16.remove(utf8ToUtf16(colNameUtf8));
        }

        void resetStateIfNecessary() {
            // First, reset processed column tracking.
            clearProcessedColumns();
            // Second, check if writer's structure version has changed
            // compared with the known metadata.
            if (latestKnownMetadata != null) {
                if (latestKnownMetadataVersion != writerAPI.getMetadataVersion()) {
                    // clear() frees latestKnownMetadata and sets it to null
                    clear();
                    latestKnownMetadata = null;
                }
            }
            if (latestKnownMetadata == null) {
                // Get the latest metadata.
                try {
                    if (isWal()) {
                        latestKnownMetadata = deepCopyOfDense(writerAPI.getMetadata());
                        latestKnownMetadataVersion = writerAPI.getMetadataVersion();
                    } else {
                        try (TableRecordMetadata meta = engine.getLegacyMetadata(tableToken)) {
                            latestKnownMetadata = deepCopyOfDense(meta);
                            latestKnownMetadataVersion = meta.getMetadataVersion();
                        }
                    }
                } catch (CairoException | TableReferenceOutOfDateException ex) {
                    if (isWal()) {
                        LOG.critical().$("could not write to WAL [ex=").$(ex).I$();
                        setWriterInError();
                    } else {
                        throw ex;
                    }
                }
            }
        }

        CharSequence utf8ToUtf16(DirectUtf8Sequence colNameUtf8) {
            return Utf8s.directUtf8ToUtf16(colNameUtf8, tempSink);
        }
    }
}
