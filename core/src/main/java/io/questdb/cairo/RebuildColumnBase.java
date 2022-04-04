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

import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.lockName;

public abstract class RebuildColumnBase implements Closeable, Mutable {
    static final int ALL = -1;
    final Path path = new Path();
    private final StringSink tempStringSink = new StringSink();
    CairoConfiguration configuration;
    int rootLen;
    String columnTypeErrorMsg = "Wrong column type";
    private long lockFd;
    private TableReaderMetadata metadata;

    @Override
    public void clear() {
        path.trimTo(0);
        tempStringSink.clear();
    }

    @Override
    public void close() {
        this.path.close();
        Misc.free(metadata);
    }

    public RebuildColumnBase of(CharSequence tablePath, CairoConfiguration configuration) {
        this.path.concat(tablePath);
        this.rootLen = tablePath.length();
        this.configuration = configuration;
        return this;
    }

    public void rebuildAll() {
        rebuildPartitionColumn(null, null);
    }

    public void rebuildColumn(CharSequence columnName) {
        rebuildPartitionColumn(null, columnName);
    }

    public void rebuildPartition(CharSequence partitionName) {
        rebuildPartitionColumn(partitionName, null);
    }

    public void rebuildPartitionColumn(CharSequence partitionName, CharSequence columnName) {
        FilesFacade ff = configuration.getFilesFacade();
        try {
            lock(ff);
            path.concat(TableUtils.COLUMN_VERSION_FILE_NAME).$();
            try (ColumnVersionReader columnVersionReader = new ColumnVersionReader().ofRO(ff, path)) {
                final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
                columnVersionReader.readSafe(configuration.getMicrosecondClock(), deadline);
                path.trimTo(rootLen);

                rebuildPartitionColumn(partitionName, columnName, columnVersionReader, ff);
            }
        } finally {
                lockName(path);
                releaseLock(ff);
        }
    }

    public void rebuildColumn(CharSequence columnName, TableWriter tableWriter) {
        rebuildPartitionColumn(null, columnName, tableWriter);
    }

    // if TableWriter is passed in the lock has to be held already
    public void rebuildPartitionColumn(CharSequence partitionName, CharSequence columnName, TableWriter tableWriter) {
        rebuildPartitionColumn(partitionName, columnName, tableWriter.getColumnVersionWriter(), configuration.getFilesFacade());
    }

    private void rebuildPartitionColumn(CharSequence rebuildPartitionName, CharSequence rebuildColumn, ColumnVersionReader columnVersionReader, FilesFacade ff) {
        path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME);
        if (metadata == null) {
            metadata = new TableReaderMetadata(ff);
        }
        metadata.of(path.$(), ColumnType.VERSION);

        try {
            // Resolve column id if the column name specified
            int rebuildColumnIndex = ALL;
            if (rebuildColumn != null) {
                rebuildColumnIndex = metadata.getColumnIndexQuiet(rebuildColumn, 0, rebuildColumn.length());
                if (rebuildColumnIndex < 0) {
                    throw CairoException.instance(0).put("Column does not exist");
                }
            }

            path.trimTo(rootLen);
            int partitionBy = metadata.getPartitionBy();
            DateFormat partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);

            try (TxReader txReader = new TxReader(ff).ofRO(path, partitionBy)) {
                txReader.unsafeLoadAll();
                path.trimTo(rootLen);

                if (PartitionBy.isPartitioned(partitionBy)) {
                    // Resolve partition timestamp if partition name specified
                    long rebuildPartitionTs = ALL;
                    if (rebuildPartitionName != null) {
                        rebuildPartitionTs = PartitionBy.parsePartitionDirName(rebuildPartitionName, partitionBy);
                    }

                    for (int partitionIndex = txReader.getPartitionCount() - 1; partitionIndex > -1; partitionIndex--) {
                        long partitionTimestamp = txReader.getPartitionTimestamp(partitionIndex);
                        if (rebuildPartitionTs == ALL || partitionTimestamp == rebuildPartitionTs) {
                            long partitionSize = txReader.getPartitionSize(partitionIndex);
                            if (partitionIndex == txReader.getPartitionCount() - 1) {
                                partitionSize = txReader.getTransientRowCount();
                            }
                            long partitionNameTxn = txReader.getPartitionNameTxn(partitionIndex);
                            rebuildColumn(
                                    rebuildColumnIndex,
                                    ff,
                                    metadata,
                                    partitionDirFormatMethod,
                                    tempStringSink,
                                    partitionTimestamp,
                                    partitionSize,
                                    partitionNameTxn,
                                    columnVersionReader);
                        }
                    }
                } else {
                    long partitionSize = txReader.getTransientRowCount();
                    rebuildColumn(
                            rebuildColumnIndex,
                            ff,
                            metadata,
                            partitionDirFormatMethod,
                            tempStringSink,
                            0L,
                            partitionSize,
                            -1L,
                            columnVersionReader
                    );
                }
            }
        } finally {
            metadata.close();
            path.trimTo(rootLen);
        }
    }

    protected boolean checkColumnType(TableReaderMetadata metadata, int rebuildColumnIndex) {
        return true;
    }

    private void lock(FilesFacade ff) {
        try {
            path.trimTo(rootLen);
            lockName(path);
            this.lockFd = TableUtils.lock(ff, path);
        } finally {
            path.trimTo(rootLen);
        }

        if (this.lockFd == -1L) {
            throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
        }
    }

    private void rebuildColumn(
            int rebuildColumnIndex,
            FilesFacade ff,
            TableReaderMetadata metadata,
            DateFormat partitionDirFormatMethod,
            StringSink sink,
            long partitionTimestamp,
            long partitionSize,
            long partitionNameTxn,
            ColumnVersionReader columnVersionReader
    ) {
        sink.clear();
        partitionDirFormatMethod.format(partitionTimestamp, null, null, sink);

        if (rebuildColumnIndex == ALL) {
            for (int columnIndex = metadata.getColumnCount() - 1; columnIndex > -1; columnIndex--) {
                if (checkColumnType(metadata, columnIndex)) {
                    rebuildColumn(metadata, columnIndex, sink, partitionSize, ff, columnVersionReader, partitionTimestamp, partitionNameTxn);
                }
            }
        } else {
            if (checkColumnType(metadata, rebuildColumnIndex)) {
                rebuildColumn(metadata, rebuildColumnIndex, sink, partitionSize, ff, columnVersionReader, partitionTimestamp, partitionNameTxn);
            } else {
                throw CairoException.instance(0).put(columnTypeErrorMsg);
            }
        }
    }

    private void rebuildColumn(
            TableReaderMetadata metadata,
            int columnIndex,
            StringSink sink,
            long partitionSize,
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        CharSequence columnName = metadata.getColumnName(columnIndex);
        int indexValueBlockCapacity = metadata.getIndexValueBlockCapacity(columnIndex);
        int writerIndex = metadata.getWriterIndex(columnIndex);
        rebuildColumn(columnName, sink, indexValueBlockCapacity, partitionSize, ff, columnVersionReader, writerIndex, partitionTimestamp, partitionNameTxn);
    }

    abstract protected void rebuildColumn(
            CharSequence columnName,
            CharSequence partitionName,
            int indexValueBlockCapacity,
            long partitionSize,
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            int columnIndex,
            long partitionTimestamp,
            long partitionNameTxn
    );

    private void releaseLock(FilesFacade ff) {
        if (lockFd != -1L) {
            ff.close(lockFd);
            try {
                path.trimTo(rootLen);
                lockName(path);
                if (ff.exists(path) && !ff.remove(path)) {
                    throw CairoException.instance(ff.errno()).put("Cannot remove ").put(path);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }
}
