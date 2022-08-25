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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.lockName;

public abstract class RebuildColumnBase implements Closeable, Mutable {
    static final int REBUILD_ALL_COLUMNS = -1;
    private final StringSink tempStringSink = new StringSink();
    protected Path path = new Path();
    protected CairoConfiguration configuration;
    protected int rootLen;
    protected String unsupportedColumnMessage = "Wrong column type";
    protected FilesFacade ff;
    private long lockFd;
    private MillisecondClock clock;

    @Override
    public void clear() {
        path.trimTo(0);
        tempStringSink.clear();
    }

    @Override
    public void close() {
        this.path = Misc.free(path);
    }

    public RebuildColumnBase of(CharSequence tablePath, CairoConfiguration configuration) {
        this.path.of(tablePath);
        this.rootLen = tablePath.length();
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.clock = configuration.getMillisecondClock();
        return this;
    }

    public void rebuildAll() {
        reindex(null, null);
    }

    public void reindex(
            @Nullable CharSequence partitionName,
            @Nullable CharSequence columnName
    ) {
        try {
            lock(ff);
            path.concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            try (ColumnVersionReader columnVersionReader = new ColumnVersionReader().ofRO(ff, path)) {
                final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
                columnVersionReader.readSafe(clock, deadline);
                path.trimTo(rootLen);
                reindex0(columnVersionReader, partitionName, columnName);
            }
        } finally {
            lockName(path);
            releaseLock(ff);
        }
    }

    public void reindexAfterUpdate(long partitionTimestamp, CharSequence columnName, TableWriter tableWriter) {
        TxReader txReader = tableWriter.getTxReader();
        int partitionIndex = txReader.getPartitionIndex(partitionTimestamp);
        assert partitionIndex > -1L;

        final RecordMetadata metadata = tableWriter.getMetadata();
        final int columnIndex = tableWriter.getColumnIndex(columnName);
        final int indexValueBlockCapacity = metadata.getIndexValueBlockCapacity(columnIndex);
        assert indexValueBlockCapacity > 0;

        final long partitionSize = partitionIndex == txReader.getPartitionCount() - 1
                ? txReader.getTransientRowCount()
                : txReader.getPartitionSize(partitionIndex);

        long partitionNameTxn = txReader.getPartitionNameTxn(partitionIndex);

        tempStringSink.clear();
        DateFormat partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(tableWriter.getPartitionBy());
        partitionDirFormatMethod.format(partitionTimestamp, null, null, tempStringSink);

        doReindex(
                tableWriter.getColumnVersionReader(),
                // this may not be needed, because table writer's column index is the same
                // as metadata writers' index.
                metadata.getWriterIndex(columnIndex),
                columnName,
                tempStringSink, // partition name
                partitionNameTxn,
                partitionSize,
                partitionTimestamp,
                indexValueBlockCapacity
        );
    }

    public void reindexAllInPartition(CharSequence partitionName) {
        reindex(partitionName, null);
    }

    public void reindexColumn(CharSequence columnName) {
        reindex(null, columnName);
    }

    abstract protected void doReindex(
            ColumnVersionReader columnVersionReader,
            int columnWriterIndex,
            CharSequence columnName,
            CharSequence partitionName,
            long partitionNameTxn,
            long partitionSize,
            long partitionTimestamp,
            int indexValueBlockCapacity
    );

    protected abstract boolean isSupportedColumn(RecordMetadata metadata, int columnIndex);

    private void lock(FilesFacade ff) {
        try {
            path.trimTo(rootLen);
            lockName(path);
            this.lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1L) {
            throw CairoException.critical(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    // this method is not used by UPDATE SQL
    private void reindex0(
            ColumnVersionReader columnVersionReader,
            @Nullable CharSequence partitionName, // will reindex all partitions if partition name is not provided
            @Nullable CharSequence columnName // will reindex all columns if name is not provided
    ) {
        path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME);
        try (TableReaderMetadata metadata = new TableReaderMetadata(ff, "<noname>", path.$())) {
            // Resolve column id if the column name specified
            final int columnIndex;
            if (columnName != null) {
                columnIndex = metadata.getColumnIndex(columnName);
            } else {
                columnIndex = REBUILD_ALL_COLUMNS;
            }

            path.trimTo(rootLen);
            final int partitionBy = metadata.getPartitionBy();
            final DateFormat partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);

            try (TxReader txReader = new TxReader(ff).ofRO(path.concat(TXN_FILE_NAME).$(), partitionBy)) {
                txReader.unsafeLoadAll();
                path.trimTo(rootLen);

                if (PartitionBy.isPartitioned(partitionBy)) {
                    // Resolve partition timestamp if partition name specified
                    if (partitionName != null) {
                        final long partitionTimestamp = PartitionBy.parsePartitionDirName(partitionName, partitionBy);
                        int partitionIndex = txReader.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
                        if (partitionIndex > -1L) {
                            reindexPartition(
                                    metadata,
                                    columnVersionReader,
                                    txReader,
                                    columnIndex,
                                    partitionIndex,
                                    partitionDirFormatMethod,
                                    partitionTimestamp
                            );
                        }
                    } else {
                        for (int partitionIndex = txReader.getPartitionCount() - 1; partitionIndex > -1; partitionIndex--) {
                            reindexPartition(
                                    metadata,
                                    columnVersionReader,
                                    txReader,
                                    columnIndex,
                                    partitionIndex,
                                    partitionDirFormatMethod,
                                    txReader.getPartitionTimestamp(partitionIndex)
                            );
                        }
                    }
                } else {
                    // reindexing columns in non-partitioned table
                    reindexOneOrAllColumns(
                            metadata,
                            columnVersionReader,
                            columnIndex,
                            partitionDirFormatMethod,
                            -1L,
                            0L,
                            txReader.getTransientRowCount()
                    );
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    public void reindexColumn(
            ColumnVersionReader columnVersionReader,
            RecordMetadata metadata,
            int columnIndex,
            CharSequence partitionName,
            long partitionNameTxn,
            long partitionTimestamp,
            long partitionSize
    ) {
        doReindex(
                columnVersionReader,
                metadata.getWriterIndex(columnIndex),
                metadata.getColumnName(columnIndex),
                partitionName,
                partitionNameTxn,
                partitionSize,
                partitionTimestamp,
                metadata.getIndexValueBlockCapacity(columnIndex)
        );
    }

    private void reindexOneOrAllColumns(
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            int columnIndex,
            DateFormat partitionDirFormatMethod,
            long partitionNameTxn,
            long partitionTimestamp,
            long partitionSize
    ) {
        tempStringSink.clear();
        partitionDirFormatMethod.format(partitionTimestamp, null, null, tempStringSink);

        if (columnIndex == REBUILD_ALL_COLUMNS) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (isSupportedColumn(metadata, i)) {
                    reindexColumn(
                            columnVersionReader,
                            metadata,
                            i,
                            tempStringSink,
                            partitionNameTxn,
                            partitionTimestamp,
                            partitionSize
                    );
                }
            }
        } else {
            if (isSupportedColumn(metadata, columnIndex)) {
                reindexColumn(
                        columnVersionReader,
                        metadata,
                        columnIndex,
                        tempStringSink,
                        partitionNameTxn,
                        partitionTimestamp,
                        partitionSize
                );
            } else {
                throw CairoException.nonCritical().put(unsupportedColumnMessage);
            }
        }
    }

    private void reindexPartition(
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            TxReader txReader,
            int columnIndex,
            int partitionIndex,
            DateFormat partitionDirFormatMethod,
            long partitionTimestamp
    ) {
        final long partitionSize = partitionIndex == txReader.getPartitionCount() - 1
                ? txReader.getTransientRowCount()
                : txReader.getPartitionSize(partitionIndex);

        reindexOneOrAllColumns(
                metadata,
                columnVersionReader,
                columnIndex,
                partitionDirFormatMethod,
                txReader.getPartitionNameTxn(partitionIndex),
                partitionTimestamp,
                partitionSize
        );
    }

    private void releaseLock(FilesFacade ff) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            lockFd = -1L;
            try {
                path.trimTo(rootLen);
                lockName(path);
                if (ff.exists(path) && !ff.remove(path)) {
                    throw CairoException.critical(ff.errno()).put("Cannot remove ").put(path);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }
}
