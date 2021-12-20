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
import java.util.Iterator;

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
    private int eventsProcessedSinceReshuffle = 0;
    private TableWriter writer;
    private boolean assignedToJob = false;
    private long lastMeasurementMillis = Long.MAX_VALUE;
    private long lastCommitMillis;
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
        this.millisecondClock = cairoConfiguration.getMillisecondClock();
        this.writerTickRowsCountMod = cairoConfiguration.getWriterTickRowsCountMod();
        this.lastCommitMillis = millisecondClock.getTicks();
        this.writer = writer;
        this.timestampIndex = writer.getMetadata().getTimestampIndex();
        this.tableNameUtf16 = writer.getTableName();
    }

    public void addReference(int workerId) {
        networkIOOwnerCount++;
        LOG.info()
                .$("network IO thread using table [workerId=").$(workerId)
                .$(", tableName=").$(getTableNameUtf16())
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

    public int getEventsProcessedSinceReshuffle() {
        return eventsProcessedSinceReshuffle;
    }

    public void setEventsProcessedSinceReshuffle(int eventsProcessedSinceReshuffle) {
        this.eventsProcessedSinceReshuffle = eventsProcessedSinceReshuffle;
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

    public void setWriterThreadId(int writerThreadId) {
        this.writerThreadId = writerThreadId;
    }

    public int incrementEventsProcessedSinceReshuffle() {
        return ++eventsProcessedSinceReshuffle;
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

    void handleRowAppended() {
        if (checkMaxAndCommitLag(writer)) {
            lastCommitMillis = millisecondClock.getTicks();
        }
    }

    private boolean checkMaxAndCommitLag(TableWriter writer) {
        final long rowsSinceCommit = writer.getUncommittedRowCount();
        if (rowsSinceCommit < writer.getMetadata().getMaxUncommittedRows()) {
            if ((rowsSinceCommit & writerTickRowsCountMod) == 0) {
                // Tick without commit. Some tick commands may force writer to commit though.
                writer.tick(false);
            }
            return false;
        }
        writer.commitWithLag(engine.getConfiguration().getCommitMode());
        // Tick after commit.
        writer.tick(false);
        return true;
    }

    void handleWriterThreadMaintenance(long ticks, long maintenanceInterval) {
        if (ticks - lastCommitMillis < maintenanceInterval) {
            return;
        }
        if (null != writer) {
            LOG.debug().$("maintenance commit [table=").$(writer.getTableName()).I$();
            try {
                writer.commit();
            } catch (Throwable e) {
                LOG.error().$("could not commit [table=").$(writer.getTableName()).I$();
                writer = Misc.free(writer);
            }
        }
    }

    void releaseWriter(boolean commit) {
        if (null != writer) {
            LOG.debug().$("release commit [table=").$(writer.getTableName()).I$();
            try {
                if (commit) {
                    writer.commit();
                }
            } catch (Throwable ex) {
                LOG.error().$("writer commit fails, force closing it [table=").$(writer.getTableName()).$(",ex=").$(ex).I$();
            } finally {
                // writer or FS can be in a bad state
                // do not leave writer locked
                writer = Misc.free(writer);
            }
        }
    }

    public class ThreadLocalDetails implements Closeable {
        private final Path path = new Path();
        private final CharSequenceIntHashMap columnIndexByNameUtf8 = new CharSequenceIntHashMap();
        private final ObjList<SymbolCache> symbolCacheByColumnIndex = new ObjList<>();
        private final ObjList<SymbolCache> unusedSymbolCaches;
        // indexed by colIdx + 1, first value accounts for spurious, new cols (index -1)
        private final IntList columnTypeMeta = new IntList();
        private final StringSink tempSink = new StringSink();
        private final BoolList processedCols = new BoolList();
        private final LowerCaseCharSequenceHashSet addedCols = new LowerCaseCharSequenceHashSet();
        private final LineTcpReceiverConfiguration configuration;
        private int columnCount;
        private boolean unresolvedColumnFlag = false;

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
            columnTypeMeta.clear();
            columnTypeMeta.add(0);
        }

        LowerCaseCharSequenceHashSet resetAddedCols() {
            addedCols.clear();
            return addedCols;
        }

        StringSink getTempSink() {
            return tempSink;
        }

        int getColumnIndex(DirectByteCharSequence colName) {
            int colIndex = columnIndexByNameUtf8.get(colName);
            if (colIndex < 0) {
                if (!unresolvedColumnFlag) {
                    colIndex = getColumnIndexFromReader(colName);
                    if (colIndex < 0) {
                        unresolvedColumnFlag = true;
                    }
                }
                columnIndexByNameUtf8.put(colName.toString(), colIndex);
            }
            return colIndex;
        }

        private int getColumnIndexFromReader(DirectByteCharSequence colName) {
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                TableReaderMetadata metadata = reader.getMetadata();
                tempSink.clear();
                if (!Chars.utf8Decode(colName.getLo(), colName.getHi(), tempSink)) {
                    throw CairoException.instance(0).put("invalid UTF8 in value for ").put(colName);
                }
                int colIndex = metadata.getColumnIndexQuiet(tempSink);
                updateColumnTypeCache(colIndex, metadata);
                return colIndex;
            }
        }

        void resolveNewColumns() {
            if (!unresolvedColumnFlag) {
                return;
            }

            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableNameUtf16)) {
                TableReaderMetadata metadata = reader.getMetadata();
                ObjList<CharSequence> keys = columnIndexByNameUtf8.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    final CharSequence colName = keys.get(i);
                    if (columnIndexByNameUtf8.get(colName) < 0) {
                        int colIndex = metadata.getColumnIndexQuiet(colName);
                        if (colIndex > -1) {
                            columnIndexByNameUtf8.put(colName, colIndex);
                            updateColumnTypeCache(colIndex, metadata);
                        }
                    }
                }
            }
        }

        private void updateColumnTypeCache(int colIndex, TableReaderMetadata metadata) {
            if (colIndex < 0) {
                return;
            }
            columnCount = metadata.getColumnCount();
            final int colType = metadata.getColumnType(colIndex);
            final int geoHashBits = ColumnType.getGeoHashBits(colType);
            columnTypeMeta.extendAndSet(colIndex + 1,
                    geoHashBits == 0 ? 0 : Numbers.encodeLowHighShorts((short) geoHashBits, ColumnType.tagOf(colType)));
        }

        int getColumnTypeMeta(int colIndex) {
            return columnTypeMeta.getQuick(colIndex + 1); // first val accounts for new cols, index -1
        }

        BoolList resetProcessedCols() {
            processedCols.setAll(columnCount, false);
            return processedCols;
        }

        void resetUnresolvedColumnFlag() {
            unresolvedColumnFlag = false;
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
