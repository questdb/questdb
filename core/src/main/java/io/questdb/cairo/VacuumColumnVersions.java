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

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.ColumnPurgeTask;

import java.io.Closeable;

import static io.questdb.cairo.PartitionBy.getPartitionDirFormatMethod;
import static io.questdb.std.Files.DT_DIR;

public class VacuumColumnVersions implements Closeable {
    private static final int COLUMN_VERSION_LIST_CAPACITY = 8;
    private final static Log LOG = LogFactory.getLog(VacuumColumnVersions.class);
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final ColumnPurgeTask purgeTask = new ColumnPurgeTask();
    private StringSink columnNameSink;
    private Utf8StringSink fileNameSink;
    private int partitionBy;
    private long partitionTimestamp;
    private Path path2;
    private ColumnPurgeOperator purgeExecution;
    private DirectLongList tableFiles;
    private int tablePathLen;
    private TableReader tableReader;
    private final FindVisitor visitTableFiles = this::visitTableFiles;
    private final FindVisitor visitTablePartition = this::visitTablePartition;

    public VacuumColumnVersions(CairoEngine engine) {
        try {
            this.engine = engine;
            this.purgeExecution = new ColumnPurgeOperator(engine.getConfiguration());
            this.tableFiles = new DirectLongList(COLUMN_VERSION_LIST_CAPACITY, MemoryTag.NATIVE_SQL_COMPILER);
            this.ff = engine.getConfiguration().getFilesFacade();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        this.purgeExecution = Misc.free(purgeExecution);
        this.tableFiles = Misc.free(tableFiles);
    }

    public void run(TableReader reader) {
        LOG.info().$("processing [dirName=").utf8(reader.getTableToken().getDirName()).I$();
        fileNameSink = new Utf8StringSink();
        columnNameSink = new StringSink();

        CairoConfiguration configuration = engine.getConfiguration();

        TableToken tableToken = reader.getTableToken();
        Path path = Path.getThreadLocal(configuration.getRoot());
        path.concat(tableToken);
        tablePathLen = path.size();
        path2 = Path.getThreadLocal2(configuration.getRoot()).concat(tableToken);

        this.tableReader = reader;
        partitionBy = reader.getPartitionedBy();

        tableFiles.clear();
        try {
            ff.iterateDir(path.$(), visitTablePartition);
            Vect.sort3LongAscInPlace(tableFiles.getAddress(), tableFiles.size() / 3);
            purgeColumnVersions(tableFiles, reader, engine);
        } finally {
            tableFiles.shrink(COLUMN_VERSION_LIST_CAPACITY);
        }
    }

    private static int resolveName2Index(CharSequence name, TableReader tableReader) {
        return tableReader.getMetadata().getColumnIndexQuiet(name);
    }

    private void purgeColumnVersions(DirectLongList tableFiles, TableReader reader, CairoEngine engine) {
        int columnIndex = -1;
        int writerIndex = -1;
        int tableId = reader.getMetadata().getTableId();
        long truncateVersion = reader.getTxFile().getTruncateVersion();
        TableReaderMetadata metadata = reader.getMetadata();
        long updateTxn = reader.getTxn();
        ColumnVersionReader columnVersionReader = reader.getColumnVersionReader();
        purgeTask.clear();

        for (long i = 0, n = tableFiles.size(); i < n; i += 3) {
            if (tableFiles.get(i) != columnIndex) {
                int newReaderIndex = (int) tableFiles.get(i);
                if (columnIndex != newReaderIndex) {
                    if (columnIndex != -1 && purgeTask.getUpdatedColumnInfo().size() > 0) {
                        if (!purgeExecution.purge(purgeTask, tableReader)) {
                            queueColumnVersionPurge(purgeTask, engine);
                        }
                        purgeTask.clear();
                    }

                    writerIndex = metadata.getWriterIndex(newReaderIndex);
                    String columnName = metadata.getColumnName(newReaderIndex);
                    int columnType = metadata.getColumnType(newReaderIndex);
                    purgeTask.of(reader.getTableToken(), columnName, tableId, truncateVersion, columnType, partitionBy, updateTxn);
                }
            }

            columnIndex = (int) tableFiles.get(i);
            long partitionTs = tableFiles.get(i + 1);
            long columnVersion = tableFiles.get(i + 2);
            long latestColumnNameTxn = columnVersionReader.getColumnNameTxn(partitionTs, writerIndex);
            // Do not delete if columnVersion >= reader.getTxn(), this may be the transaction not committed yet
            if (columnVersion != latestColumnNameTxn && columnVersion < reader.getTxn()) {
                // Has to be deleted. Columns can have multiple files e.g. .i, .d, .k, .v
                if (!versionSetToDelete(purgeTask, partitionTs, columnVersion)) {
                    long partitionNameTxn = reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTs);
                    purgeTask.appendColumnInfo(columnVersion, partitionTs, partitionNameTxn);
                }
            }
        }

        if (purgeTask.getUpdatedColumnInfo().size() > 0) {
            if (!purgeExecution.purge(purgeTask, tableReader)) {
                queueColumnVersionPurge(purgeTask, engine);
            }
            purgeTask.clear();
        }
    }

    private void queueColumnVersionPurge(ColumnPurgeTask purgeTask, CairoEngine engine) {
        MessageBus messageBus = engine.getMessageBus();
        LOG.info().$("scheduling column version purge [table=").$(purgeTask.getTableName())
                .$(", column=").$(purgeTask.getColumnName())
                .I$();

        Sequence pubSeq = messageBus.getColumnPurgePubSeq();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1) {
                ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                task.copyFrom(purgeTask);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1) {
                // Queue overflow
                throw CairoException.nonCritical().put("failed to schedule column version purge, queue is full. " +
                                "Please retry and consider increasing ").put(PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY.getPropertyPath())
                        .put(" configuration parameter");
            }
            Os.pause();
        }
    }

    private boolean versionSetToDelete(ColumnPurgeTask purgeTask, long partitionTs, long columnVersion) {
        // Brute force for now
        LongList columnVersionToDelete = purgeTask.getUpdatedColumnInfo();
        for (int i = 0, n = columnVersionToDelete.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
            long cv = columnVersionToDelete.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION);
            long ts = columnVersionToDelete.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP);

            if (cv == columnVersion && ts == partitionTs) {
                return true;
            }
        }
        return false;
    }

    private void visitTableFiles(long pUtf8NameZ, int type) {
        if (type != DT_DIR) {
            fileNameSink.clear();
            Utf8s.utf8ZCopy(pUtf8NameZ, fileNameSink);
            if (Files.notDots(fileNameSink)) {
                int dotIndex = Utf8s.indexOfAscii(fileNameSink, '.');
                if (dotIndex > 0) {
                    long columnVersion = -1;

                    columnNameSink.clear();
                    Utf8s.utf8ToUtf16(fileNameSink, 0, dotIndex, columnNameSink);
                    int name2Index = resolveName2Index(columnNameSink, tableReader);
                    if (name2Index < 0) {
                        // Unknown file. Log the problem
                        LOG.error().$("file does not belong to the table [name=").$(fileNameSink).$(", path=").$(path2).I$();
                        return;
                    }

                    int secondDot = Utf8s.indexOfAscii(fileNameSink, dotIndex + 1, '.');
                    int lo = secondDot + 1;
                    if (lo < fileNameSink.size()) {
                        try {
                            columnVersion = Numbers.parseLong(fileNameSink, lo, fileNameSink.size());
                        } catch (NumericException e) {
                            // leave -1, default version
                        }
                    }

                    tableFiles.add(name2Index);
                    tableFiles.add(partitionTimestamp);
                    tableFiles.add(columnVersion);
                }
            }
        }
    }

    private void visitTablePartition(long pUtf8NameZ, int type) {
        if (ff.isDirOrSoftLinkDirNoDots(path2, tablePathLen, pUtf8NameZ, type, fileNameSink)) {
            path2.trimTo(tablePathLen).$();

            int dotIndex = Utf8s.indexOfAscii(fileNameSink, '.');
            if (dotIndex < 0) {
                dotIndex = fileNameSink.size();
            }

            try {
                partitionTimestamp = getPartitionDirFormatMethod(partitionBy).parse(fileNameSink.asAsciiCharSequence(), 0, dotIndex, DateFormatUtils.EN_LOCALE);
            } catch (NumericException ex) {
                // Directory is invalid partition name, continue
                LOG.error().$("skipping column version purge VACUUM, invalid partition directory name [name=").$(fileNameSink)
                        .$(", path=").$(path2).I$();
                return;
            }

            long partitionNameTxn = -1L;
            if (dotIndex + 1 < fileNameSink.size()) {
                try {
                    partitionNameTxn = Numbers.parseLong(fileNameSink, dotIndex + 1, fileNameSink.size());
                } catch (NumericException ex) {
                    // Invalid partition name txn
                    LOG.error().$("skipping column version purge VACUUM, invalid partition directory name [name=").$(fileNameSink)
                            .$(", path=").$(path2).I$();
                    return;
                }
            }

            if (partitionNameTxn != tableReader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp)) {
                // This is partition version to be deleted by O3 partition purge
                return;
            }

            path2.concat(pUtf8NameZ);
            LOG.info().$("enumerating files at ").$(path2).$();
            ff.iterateDir(path2.$(), visitTableFiles);
        }
    }
}
