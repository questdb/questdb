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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.lockName;

public abstract class RebuildColumnBase implements Closeable, Mutable {
    static final int REBUILD_ALL_COLUMNS = -1;
    protected final CairoConfiguration configuration;
    protected final String unsupportedTableMessage = "Table does not have any indexes";
    private final MillisecondClock clock;
    private final StringSink tempStringSink = new StringSink();
    protected Path path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
    protected int rootLen;
    protected String unsupportedColumnMessage = "Wrong column type";
    private long lockFd;

    public RebuildColumnBase(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
    }

    @Override
    public void clear() {
        path.trimTo(0);
        tempStringSink.clear();
    }

    @Override
    public void close() {
        this.path = Misc.free(path);
    }

    public RebuildColumnBase of(Utf8Sequence tablePath) {
        this.path.of(tablePath);
        this.rootLen = tablePath.size();
        return this;
    }

    public void rebuildAll() {
        reindex(configuration.getFilesFacade(), null, null);
    }

    public void reindex(
            @Nullable CharSequence partitionName,
            @Nullable CharSequence columnName
    ) {
        reindex(configuration.getFilesFacade(), partitionName, columnName);
    }

    public void reindex(FilesFacade ff, @Nullable CharSequence partitionName, @Nullable CharSequence columnName) {
        try {
            lock(ff);
            path.concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            try (ColumnVersionReader columnVersionReader = new ColumnVersionReader().ofRO(ff, path.$())) {
                final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
                columnVersionReader.readSafe(clock, deadline);
                path.trimTo(rootLen);
                reindex0(ff, columnVersionReader, partitionName, columnName);
            }
        } finally {
            lockName(path);
            releaseLock(ff);
        }
    }

    public void reindexAfterUpdate(
            FilesFacade ff,
            long partitionTimestamp,
            CharSequence columnName,
            TableWriter tableWriter
    ) {
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

        doReindex(
                ff,
                tableWriter.getColumnVersionReader(),
                // this may not be needed, because table writer's column index is the same
                // as metadata writers' index.
                columnIndex,
                columnName,
                partitionNameTxn,
                partitionSize,
                partitionTimestamp,
                tableWriter.getMetadata().getTimestampType(),
                tableWriter.getPartitionBy(),
                indexValueBlockCapacity
        );
    }

    public void reindexAllInPartition(CharSequence partitionName) {
        reindex(partitionName, null);
    }

    public void reindexColumn(CharSequence columnName) {
        reindex(null, columnName);
    }

    public void reindexColumn(FilesFacade ff, CharSequence columnName) {
        reindex(ff, null, columnName);
    }

    public void reindexColumn(
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            RecordMetadata metadata,
            int columnIndex,
            long partitionNameTxn,
            long partitionTimestamp,
            int partitionBy,
            long partitionSize
    ) {
        doReindex(
                ff,
                columnVersionReader,
                metadata.getWriterIndex(columnIndex),
                metadata.getColumnName(columnIndex),
                partitionNameTxn,
                partitionSize,
                partitionTimestamp,
                metadata.getTimestampType(),
                partitionBy,
                metadata.getIndexValueBlockCapacity(columnIndex)
        );
    }

    private void lock(FilesFacade ff) {
        try {
            path.trimTo(rootLen);
            this.lockFd = TableUtils.lock(ff, lockName(path));
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1) {
            throw CairoException.nonCritical().put("cannot lock table: ").put(path.$());
        }
    }

    // this method is not used by UPDATE SQL
    private void reindex0(
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            @Nullable CharSequence partitionName, // will reindex all partitions if partition name is not provided
            @Nullable CharSequence columnName // will reindex all columns if name is not provided
    ) {
        path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME);
        try (TableReaderMetadata metadata = new TableReaderMetadata(configuration)) {
            metadata.loadMetadata(path.$());
            // Resolve column id if the column name specified
            final int columnIndex;
            if (columnName != null) {
                columnIndex = metadata.getColumnIndex(columnName);
            } else {
                columnIndex = REBUILD_ALL_COLUMNS;
            }

            path.trimTo(rootLen);
            final int partitionBy = metadata.getPartitionBy();

            try (TxReader txReader = new TxReader(ff).ofRO(path.concat(TXN_FILE_NAME).$(), metadata.getTimestampType(), partitionBy)) {
                txReader.unsafeLoadAll();
                path.trimTo(rootLen);

                if (PartitionBy.isPartitioned(partitionBy)) {
                    // Resolve partition timestamp if partition name specified
                    if (partitionName != null) {
                        final long partitionTimestamp = PartitionBy.parsePartitionDirName(partitionName, metadata.getTimestampType(), partitionBy);
                        int partitionIndex = txReader.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
                        if (partitionIndex > -1L) {
                            reindexPartition(
                                    ff,
                                    metadata,
                                    columnVersionReader,
                                    txReader,
                                    columnIndex,
                                    partitionIndex,
                                    partitionBy,
                                    partitionTimestamp
                            );
                        }
                    } else {
                        for (int partitionIndex = txReader.getPartitionCount() - 1; partitionIndex > -1; partitionIndex--) {
                            reindexPartition(
                                    ff,
                                    metadata,
                                    columnVersionReader,
                                    txReader,
                                    columnIndex,
                                    partitionIndex,
                                    partitionBy,
                                    txReader.getPartitionTimestampByIndex(partitionIndex)
                            );
                        }
                    }
                } else {
                    // reindexing columns in non-partitioned table
                    reindexOneOrAllColumns(
                            ff,
                            metadata,
                            columnVersionReader,
                            columnIndex,
                            partitionBy,
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

    private void reindexOneOrAllColumns(
            FilesFacade ff,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            int columnIndex,
            int partitionBy,
            long partitionNameTxn,
            long partitionTimestamp,
            long partitionSize
    ) {
        boolean isIndexed = false;

        if (columnIndex == REBUILD_ALL_COLUMNS) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (isSupportedColumn(metadata, i)) {
                    isIndexed = true;
                    reindexColumn(
                            ff,
                            columnVersionReader,
                            metadata,
                            i,
                            partitionNameTxn,
                            partitionTimestamp,
                            partitionBy,
                            partitionSize
                    );
                }
            }
            if (!isIndexed) {
                throw CairoException.nonCritical().put(unsupportedTableMessage);
            }
        } else {
            if (isSupportedColumn(metadata, columnIndex)) {
                reindexColumn(
                        ff,
                        columnVersionReader,
                        metadata,
                        columnIndex,
                        partitionNameTxn,
                        partitionTimestamp,
                        partitionBy,
                        partitionSize
                );
            } else {
                throw CairoException.nonCritical().put(unsupportedColumnMessage);
            }
        }
    }

    private void reindexPartition(
            FilesFacade ff,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            TxReader txReader,
            int columnIndex,
            int partitionIndex,
            int partitionBy,
            long partitionTimestamp
    ) {
        final long partitionSize = partitionIndex == txReader.getPartitionCount() - 1
                ? txReader.getTransientRowCount()
                : txReader.getPartitionSize(partitionIndex);

        reindexOneOrAllColumns(
                ff,
                metadata,
                columnVersionReader,
                columnIndex,
                partitionBy,
                txReader.getPartitionNameTxn(partitionIndex),
                partitionTimestamp,
                partitionSize
        );
    }

    private void releaseLock(FilesFacade ff) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            lockFd = -1;
            try {
                path.trimTo(rootLen);
                ff.remove(lockName(path));
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    abstract protected void doReindex(
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            int columnWriterIndex,
            CharSequence columnName,
            long partitionNameTxn,
            long partitionSize,
            long partitionTimestamp,
            int timestampType,
            int partitionBy,
            int indexValueBlockCapacity
    );

    protected abstract boolean isSupportedColumn(RecordMetadata metadata, int columnIndex);
}
