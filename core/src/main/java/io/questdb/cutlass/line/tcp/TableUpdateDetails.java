/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16;
import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8BytesToString;

public class TableUpdateDetails implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableUpdateDetails.class);
    private final String tableNameUtf16;
    private final ThreadLocalDetails[] localDetailsArray;
    private final int timestampIndex;
    private final CairoEngine engine;
    private final MillisecondClock millisecondClock;
    private final long writerTickRowsCountMod;
    private int writerThreadId;
    // Number of rows processed since the last reshuffle, this is an estimate because it is incremented by
    // multiple threads without synchronisation
    private long eventsProcessedSinceReshuffle = 0;
    private TableWriter writer;
    private boolean assignedToJob = false;
    private long lastMeasurementMillis = Long.MAX_VALUE;
    private long nextCommitTime;
    private int networkIOOwnerCount = 0;

    TableUpdateDetails(
            LineTcpReceiverConfiguration configuration,
            CairoEngine engine,
            TableWriter writer,
            int writerThreadId,
            NetworkIOJob[] netIoJobs
    ) {
        this.writerThreadId = writerThreadId;
        this.engine = engine;
        final int n = netIoJobs.length;
        this.localDetailsArray = new ThreadLocalDetails[n];
        for (int i = 0; i < n; i++) {
            this.localDetailsArray[i] = new ThreadLocalDetails(
                    configuration, netIoJobs[i].getUnusedSymbolCaches(), writer.getMetadata().getColumnCount());
        }
        CairoConfiguration cairoConfiguration = engine.getConfiguration();
        TableWriterMetadata metadata = writer.getMetadata();
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.writerTickRowsCountMod = cairoConfiguration.getWriterTickRowsCountMod();
        this.writer = writer;
        this.timestampIndex = metadata.getTimestampIndex();
        this.tableNameUtf16 = writer.getTableName();
        writer.updateCommitInterval(configuration.getCommitIntervalFraction(), configuration.getCommitIntervalDefault());
        this.nextCommitTime = millisecondClock.getTicks() + writer.getCommitInterval();
    }

    public void addReference(int workerId) {
        networkIOOwnerCount++;
        LOG.info()
                .$("network IO thread using table [workerId=").$(workerId)
                .$(", tableName=").$(tableNameUtf16)
                .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                .$(']').$();

    }

    public void tick() {
        if (writer != null) {
            writer.tick(false);
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
            LOG.info().$("closing table parsers [tableName=").$(tableNameUtf16).$(']').$();
            localDetailsArray[n] = Misc.free(localDetailsArray[n]);
        }
    }

    public void closeNoLock() {
        if (writerThreadId != Integer.MIN_VALUE) {
            LOG.info().$("closing table writer [tableName=").$(tableNameUtf16).$(']').$();
            closeLocals();
            if (null != writer) {
                try {
                    writer.commit();
                } catch (Throwable ex) {
                    LOG.error().$("cannot commit writer transaction, rolling back before releasing it [table=").$(tableNameUtf16).$(",ex=").$(ex).I$();
                } finally {
                    // returning to pool rolls back the transaction
                    writer = Misc.free(writer);
                }
            }
            writerThreadId = Integer.MIN_VALUE;
        }
    }

    public long getEventsProcessedSinceReshuffle() {
        return eventsProcessedSinceReshuffle;
    }

    public long getLastMeasurementMillis() {
        return lastMeasurementMillis;
    }

    public int getNetworkIOOwnerCount() {
        return networkIOOwnerCount;
    }

    public String getTableNameUtf16() {
        return tableNameUtf16;
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

    public void setAssignedToJob(boolean assignedToJob) {
        this.assignedToJob = assignedToJob;
    }

    public void removeReference(int workerId) {
        networkIOOwnerCount--;
        localDetailsArray[workerId].clear();
        LOG.info()
                .$("network IO thread released table [workerId=").$(workerId)
                .$(", tableName=").$(tableNameUtf16)
                .$(", nNetworkIoWorkers=").$(networkIOOwnerCount)
                .I$();
    }

    int getSymbolIndex(ThreadLocalDetails localDetails, int colIndex, CharSequence symValue) {
        if (colIndex >= 0) {
            return localDetails.getSymbolIndex(colIndex, symValue);
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    ThreadLocalDetails getThreadLocalDetails(int workerId) {
        lastMeasurementMillis = millisecondClock.getTicks();
        return localDetailsArray[workerId];
    }

    int getTimestampIndex() {
        return timestampIndex;
    }

    TableWriter getWriter() {
        return writer;
    }

    void commitIfMaxUncommittedRowsCountReached() {
        final long rowsSinceCommit = writer.getUncommittedRowCount();
        if (rowsSinceCommit < writer.getMetadata().getMaxUncommittedRows()) {
            if ((rowsSinceCommit & writerTickRowsCountMod) == 0) {
                // Tick without commit. Some tick commands may force writer to commit though.
                writer.tick(false);
            }
            return;
        }
        LOG.debug().$("max-uncommitted-rows commit with lag [").$(tableNameUtf16).I$();
        nextCommitTime = millisecondClock.getTicks() + writer.getCommitInterval();
        writer.commitWithLag();
        // Tick after commit.
        writer.tick(false);
    }

    long commitIfIntervalElapsed(long wallClockMillis) {
        if (wallClockMillis < nextCommitTime) {
            return nextCommitTime;
        }
        if (writer != null) {
            final long commitInterval = writer.getCommitInterval();
            commit(wallClockMillis - lastMeasurementMillis < commitInterval);
            nextCommitTime += commitInterval;
        }
        return nextCommitTime;
    }

    private void commit(boolean withLag) {
        if (writer.getUncommittedRowCount() > 0) {
            try {
                LOG.debug().$("time-based commit " + (withLag ? "with lag " : "") + "[rows=").$(writer.getUncommittedRowCount()).$(", table=").$(tableNameUtf16).I$();
                if (withLag) {
                    writer.commitWithLag();
                } else {
                    writer.commit();
                }
            } catch (Throwable ex) {
                LOG.error().$("could not commit [table=").$(tableNameUtf16).$(", e=").$(ex).I$();
                try {
                    writer.rollback();
                } catch (Throwable th) {
                    LOG.error().$("could not perform emergency rollback [table=").$(tableNameUtf16).$(", e=").$(th).I$();
                }
            }
        }
    }

    void releaseWriter(boolean commit) {
        if (writer != null) {
            try {
                if (commit) {
                    LOG.debug().$("release commit [table=").$(tableNameUtf16).I$();
                    writer.commit();
                }
            } catch (Throwable ex) {
                LOG.error().$("writer commit fails, force closing it [table=").$(tableNameUtf16).$(",ex=").$(ex).I$();
            } finally {
                // writer or FS can be in a bad state
                // do not leave writer locked
                writer = Misc.free(writer);
            }
        }
    }

    public class ThreadLocalDetails implements Closeable {
        static final int COLUMN_NOT_FOUND = -1;
        static final int DUPLICATED_COLUMN = -2;

        private final Path path = new Path();
        // maps column names to their indexes
        // keys are mangled strings created from the utf-8 encoded byte representations of the column names
        private final CharSequenceIntHashMap columnIndexByNameUtf8 = new CharSequenceIntHashMap();
        private final ObjList<SymbolCache> symbolCacheByColumnIndex = new ObjList<>();
        private final ObjList<SymbolCache> unusedSymbolCaches;
        // indexed by colIdx + 1, first value accounts for spurious, new cols (index -1)
        private final IntList columnTypeMeta = new IntList();
        private final IntList columnTypes = new IntList();
        private final StringSink tempSink = new StringSink();
        // tracking of processed columns by their index, duplicates will be ignored
        private final BoolList processedCols = new BoolList();
        // tracking of processed columns by their name, duplicates will be ignored
        // columns end up in this set only if their index cannot be resolved, i.e. new columns
        private final LowerCaseCharSequenceHashSet addedColsUtf16 = new LowerCaseCharSequenceHashSet();
        private final LineTcpReceiverConfiguration configuration;
        private int columnCount;
        private String colName;

        ThreadLocalDetails(LineTcpReceiverConfiguration configuration, ObjList<SymbolCache> unusedSymbolCaches, int columnCount) {
            this.configuration = configuration;
            // symbol caches are passed from the outside
            // to provide global lifecycle management for when ThreadLocalDetails cease to exist
            // the cache continue to live
            this.unusedSymbolCaches = unusedSymbolCaches;
            this.columnCount = columnCount;
            columnTypeMeta.add(0);
        }

        @Override
        public void close() {
            Misc.freeObjList(symbolCacheByColumnIndex);
            Misc.free(path);
        }

        private SymbolCache addSymbolCache(int colIndex) {
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                if (!ColumnType.isSymbol(reader.getMetadata().getColumnType(colIndex))) {
                    throw CairoException.instance(0).put(reader.getMetadata().getColumnName(colIndex)).put(" expected to be Symbol type in table ").put(tableNameUtf16);
                }
                path.of(engine.getConfiguration().getRoot()).concat(tableNameUtf16);
                SymbolCache symCache;
                final int lastUnusedSymbolCacheIndex = unusedSymbolCaches.size() - 1;
                if (lastUnusedSymbolCacheIndex > -1) {
                    symCache = unusedSymbolCaches.get(lastUnusedSymbolCacheIndex);
                    unusedSymbolCaches.remove(lastUnusedSymbolCacheIndex);
                } else {
                    symCache = new SymbolCache(configuration);
                }
                int symIndex = resolveSymbolIndex(reader.getMetadata(), colIndex);
                symCache.of(engine.getConfiguration(), path, reader.getMetadata().getColumnName(colIndex), symIndex);
                symbolCacheByColumnIndex.extendAndSet(colIndex, symCache);
                return symCache;
            }
        }

        void clear() {
            columnIndexByNameUtf8.clear();
            for (int n = 0, sz = symbolCacheByColumnIndex.size(); n < sz; n++) {
                SymbolCache symCache = symbolCacheByColumnIndex.getQuick(n);
                if (null != symCache) {
                    symCache.close();
                    unusedSymbolCaches.add(symCache);
                }
            }
            symbolCacheByColumnIndex.clear();
            columnTypes.clear();
            columnTypeMeta.clear();
            columnTypeMeta.add(0);
        }

        // returns the column index for column name passed in colNameUtf8,
        // or COLUMN_NOT_FOUND if column index cannot be resolved (i.e. new column),
        // or DUPLICATED_COLUMN if the column has already been processed on the current event
        int getColumnIndex(DirectByteCharSequence colNameUtf8, boolean hasNonAsciiChars) {
            int colIndex = columnIndexByNameUtf8.get(colNameUtf8);
            if (colIndex < 0) {
                // lookup was unsuccessful we have to check whether the column can be passed by name to the writer
                final CharSequence colNameUtf16 = utf8ToUtf16(colNameUtf8, tempSink, hasNonAsciiChars);
                final int index = addedColsUtf16.keyIndex(colNameUtf16);
                if (index > -1) {
                    // column has not been sent to the writer by name on this line before
                    // we can try to resolve column index using table reader
                    colIndex = getColumnIndexFromReader(colNameUtf16);
                    if (colIndex > -1) {
                        // keys of this map will be checked against DirectByteCharSequence when get() is called
                        // DirectByteCharSequence.equals() compares chars created from each byte, basically it
                        // assumes that each char is encoded on a single byte (ASCII)
                        // utf8BytesToString() is used here instead of a simple toString() call to make sure
                        // column names with non-ASCII chars are handled properly
                        columnIndexByNameUtf8.put(utf8BytesToString(colNameUtf8, tempSink), colIndex);
                    } else {
                        // cannot not resolve column index even from the reader
                        // column will be passed to the writer by name
                        colName = colNameUtf16.toString();
                        addedColsUtf16.addAt(index, colName);
                        return COLUMN_NOT_FOUND;
                    }
                } else {
                    // column has been passed by name earlier on this event, duplicate should be skipped
                    return DUPLICATED_COLUMN;
                }
            }

            if (processedCols.extendAndReplace(colIndex, true)) {
                // column has been passed by index earlier on this event, duplicate should be skipped
                return DUPLICATED_COLUMN;
            }
            return colIndex;
        }

        private int getColumnIndexFromReader(CharSequence colNameUtf16) {
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                TableReaderMetadata metadata = reader.getMetadata();
                int colIndex = metadata.getColumnIndexQuiet(colNameUtf16);
                updateColumnTypeCache(colIndex, metadata);
                return colIndex;
            }
        }

        private void updateColumnTypeCache(int colIndex, TableReaderMetadata metadata) {
            if (colIndex < 0) {
                return;
            }
            columnCount = metadata.getColumnCount();
            final int colType = metadata.getColumnType(colIndex);
            columnTypes.extendAndSet(colIndex, colType);
            final int geoHashBits = ColumnType.getGeoHashBits(colType);
            columnTypeMeta.extendAndSet(colIndex + 1,
                    geoHashBits == 0 ? 0 : Numbers.encodeLowHighShorts((short) geoHashBits, ColumnType.tagOf(colType)));
        }

        int getColumnType(int colIndex) {
            return columnTypes.getQuick(colIndex);
        }

        int getColumnTypeMeta(int colIndex) {
            return columnTypeMeta.getQuick(colIndex + 1); // first val accounts for new cols, index -1
        }

        void resetProcessedColumnsTracking() {
            processedCols.setAll(columnCount, false);
            addedColsUtf16.clear();
        }

        String getColName() {
            assert colName != null;
            return colName;
        }

        int getSymbolIndex(int colIndex, CharSequence symValue) {
            SymbolCache symCache = symbolCacheByColumnIndex.getQuiet(colIndex);
            if (null == symCache) {
                symCache = addSymbolCache(colIndex);
            }
            return symCache.getSymbolKey(symValue);
        }

        private int resolveSymbolIndex(TableReaderMetadata metadata, int colIndex) {
            int symIndex = 0;
            for (int n = 0; n < colIndex; n++) {
                if (ColumnType.isSymbol(metadata.getColumnType(n))) {
                    symIndex++;
                }
            }
            return symIndex;
        }
    }
}
