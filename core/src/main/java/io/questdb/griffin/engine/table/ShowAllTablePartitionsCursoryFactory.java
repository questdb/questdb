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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

public class ShowAllTablePartitionsCursoryFactory extends AbstractRecordCursorFactory {
    private static final int TABLE_NAME;
    private static final int WAL_ENABLED;
    private static final int PARTITION_BY;
    private static final int PARTITION_COUNT;
    private static final int ROW_COUNT;
    private static final int SIZE_B;
    private static final RecordMetadata METADATA;
    private static final Comparator<String> CHAR_COMPARATOR = Chars::compare;
    private final AllTablePartitionListRecordCursor cursor =
            new AllTablePartitionListRecordCursor();
    private static final Log LOG = LogFactory.getLog(AllTablePartitionListRecordCursor.class);
    private CairoConfiguration cairoConfig;
    private FilesFacade ff;


    public ShowAllTablePartitionsCursoryFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(executionContext);
        this.cairoConfig = executionContext.getCairoEngine().getConfiguration();
        this.ff = cairoConfig.getFilesFacade();
        return cursor.initialize();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class AllTablePartitionListRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
        ObjList<String> attachablePartitions = new ObjList<>(4);
        ObjList<String> detachedPartitions = new ObjList<>(8);
        Path path = new Path();
        private int tableIndex = -1;
        StringSink partitionName = new StringSink();
        private TxReader detachedTxReader;
        private TableReaderMetadata detachedMetaReader;
        private AllPartitionsRecord record = new AllPartitionsRecord();
        private SqlExecutionContext executionContext;
        private TableReader tableReader;
        private int rootLen;

        private AllTablePartitionListRecordCursor initialize() {
            return this;
        }

        @Override
        public void close() {
        }

        private void of(SqlExecutionContext executionContext) {
            this.executionContext = executionContext;
            toTop();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            if (tableIndex < 0) {
                executionContext.getCairoEngine().getTableTokens(tableBucket, false);
                tableIndex = -1;
            }
            tableIndex++;
            int n = tableBucket.size();
            for (; tableIndex < n; tableIndex++) {
                TableToken tableToken = tableBucket.get(tableIndex);
                if (tableToken.isSystem())
                    continue;
                record.loadPartitionDetailsForCurrentTable(tableToken);
                return true;
            }
            return tableIndex < n;

        }

        @Override
        public long size() throws DataUnavailableException {
            return 0;
        }

        @Override
        public void toTop() {
            tableIndex = -1;
        }

        private class AllPartitionsRecord implements Record, Closeable {
            private CharSequence tableName;
            private int partitionBy;
            private long partitionCount;
            private long rowCount;
            private long sizeB;
            private boolean walEnabled;

            @Override
            public void close() throws IOException {

            }

            @Override
            public long getLong(int col) {
                switch (col) {
                    case 3:
                        return partitionCount;
                    case 4:
                        return rowCount;
                    case 5:
                        return sizeB;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public boolean getBool(int col) {
                switch (col) {
                    case 1:
                        return walEnabled;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public @Nullable CharSequence getStrA(int col) {
                switch (col) {
                    case 0:
                        return tableName;
                    case 2:
                        return PartitionBy.toString(partitionBy);
                    default:
                        throw new UnsupportedOperationException();
                }
            }


            private void loadPartitionDetailsForCurrentTable(TableToken tableToken) {
                rowCount = 0;
                partitionCount = 0;
                sizeB = 0;
                tableReader = executionContext.getReader(tableToken);
                TxReader tableTxReader = tableReader.getTxFile();
                int partitionIndex = 0;
                tableName = tableToken.getTableName();
                walEnabled = tableToken.isWal();
                partitionBy = tableReader.getPartitionedBy();
                path.of(cairoConfig.getRoot()).concat(tableToken).$();
                scanDetachedAndAttachablePartitions();
                rootLen = path.size();
                int attachedPartitions = tableTxReader.getPartitionCount();
                partitionCount = attachedPartitions +
                        attachablePartitions.size() +
                        detachedPartitions.size();

                while (partitionIndex < partitionCount) {
                    path.trimTo(rootLen).$();
                    if (partitionIndex < attachedPartitions) {
                        long timestamp = tableTxReader.getPartitionTimestampByIndex(partitionIndex);
                        TableUtils.setPathForPartition
                                (path, partitionBy, timestamp, tableTxReader.getPartitionNameTxn(partitionIndex));
                        rowCount += tableTxReader.getPartitionSize(partitionIndex);
                    } else {
                        int idx = partitionIndex - attachedPartitions;
                        int n = detachedPartitions.size();
                        if (idx < n) {
                            partitionName.put(detachedPartitions.get(idx));
                        } else {
                            idx -= n;
                            if (idx < attachablePartitions.size()) {
                                partitionName.put(attachablePartitions.get(idx));
                            }
                        }
                        if (ff.exists(path.concat(partitionName).concat(TableUtils.META_FILE_NAME).$())) {
                            try {
                                if (detachedMetaReader == null) {
                                    detachedMetaReader = new TableReaderMetadata(cairoConfig);
                                }
                                detachedMetaReader.load(path);
                                if (tableToken.getTableId() == detachedMetaReader.getTableId() && partitionBy == detachedMetaReader.getPartitionBy()) {
                                    if (ff.exists(path.parent().concat(TableUtils.TXN_FILE_NAME).$())) {
                                        try {
                                            if (detachedTxReader == null) {
                                                detachedTxReader = new TxReader(FilesFacadeImpl.INSTANCE);
                                            }
                                            detachedTxReader.ofRO(path, partitionBy);
                                            detachedTxReader.unsafeLoadAll();
                                            int length = partitionName.indexOf(".");
                                            if (length < 0) {
                                                length = partitionName.length();
                                            }
                                            long timestamp = PartitionBy.parsePartitionDirName(partitionName, partitionBy, 0, length);
                                            int pIndex = detachedTxReader.getPartitionIndex(timestamp);
                                            rowCount += detachedTxReader.getPartitionSize(pIndex);

                                        } finally {
                                            if (detachedTxReader != null) {
                                                detachedTxReader.clear();
                                            }
                                        }
                                    } else {
                                        LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                                    }
                                } else {
                                    LOG.error().$("detached partition meta does not match [path=").$(path).I$();
                                }
                            } finally {
                                if (detachedMetaReader != null) {
                                    detachedMetaReader.clear();
                                }
                            }
                        } else {
                            LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                        }
                        path.parent();
                    }
                    sizeB += ff.getDirSize(path.$());
                    partitionName.clear();
                    partitionIndex++;
                }
            }

            private void scanDetachedAndAttachablePartitions() {
                long pFind = ff.findFirst(path);
                if (pFind > 0L) {
                    try {
                        attachablePartitions.clear();
                        detachedPartitions.clear();
                        do {
                            partitionName.clear();
                            long name = ff.findName(pFind);
                            Utf8s.utf8ToUtf16Z(name, partitionName);
                            int type = ff.findType(pFind);
                            if ((type == Files.DT_LNK || type == Files.DT_DIR) && Chars.endsWith(partitionName, TableUtils.ATTACHABLE_DIR_MARKER)) {
                                attachablePartitions.add(Chars.toString(partitionName));
                            } else if (type == Files.DT_DIR && CairoKeywords.isDetachedDirMarker(name)) {
                                detachedPartitions.add(Chars.toString(partitionName));
                            }
                        } while (ff.findNext(pFind) > 0);
                        attachablePartitions.sort(CHAR_COMPARATOR);
                        detachedPartitions.sort(CHAR_COMPARATOR);
                    } finally {
                        ff.findClose(pFind);
                    }
                }
            }
        }
    }

    static {
        TABLE_NAME = 0;
        WAL_ENABLED = 1;
        PARTITION_BY = 2;
        PARTITION_COUNT = 3;
        ROW_COUNT = 4;
        SIZE_B = 5;

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("wal_enabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("partition_by", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partition_count", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("row_count", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("size_b", ColumnType.LONG));
        METADATA = metadata;
    }

}

