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
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;

public class RecoverVarIndex extends RebuildColumnBase {
    private static final Log LOG = LogFactory.getLog(RecoverVarIndex.class);

    public RecoverVarIndex(CairoConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected void doReindex(
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
    ) {
        long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, columnWriterIndex);
        long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, columnWriterIndex);

        if (columnTop == -1L) {
            LOG.info().$("not rebuilding column ").$safe(columnName)
                    .$(" in partition ").$ts(ColumnType.getTimestampDriver(timestampType), partitionTimestamp)
                    .$(", column not added to partition")
                    .$();
            return;
        }

        int trimTo = path.size();
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);

        try {
            path.concat(columnName);
            int colNameLen = path.size();
            path.put(".d");
            if (columnNameTxn != -1L) {
                path.put('.').put(columnNameTxn);
            }
            LOG.info().$("reading: ").$(path).$();

            long maxOffset = ff.length(path.$());

            try (MemoryCMR roMem = new MemoryCMRImpl(ff, path.$(), maxOffset, MemoryTag.MMAP_DEFAULT)) {
                path.trimTo(colNameLen).put(".i");
                if (columnNameTxn != -1L) {
                    path.put('.').put(columnNameTxn);
                }
                LOG.info().$("writing: ").$(path).$();

                try (MemoryCMARW rwMem = new MemoryCMARWImpl(ff, path.$(), 8 * 1024 * 1024, 0, MemoryTag.MMAP_DEFAULT, 0)) {
                    long expectedRowCount = partitionSize - columnTop;
                    LOG.info().$("data file length: ").$(maxOffset).$(", expected record count: ").$(expectedRowCount).$();

                    // index
                    long offset = 0;
                    int rows = 0;
                    while (rows < expectedRowCount && offset + 3 < maxOffset) {
                        int len = roMem.getInt(offset);
                        rwMem.putLong(offset);

                        if (len > -1) {
                            offset += 4 + len * 2L;
                        } else {
                            offset += 4;
                        }
                        rows++;
                    }
                    if (rows != expectedRowCount) {
                        throw CairoException.critical(0)
                                .put(" rebuild var index file failed [path=").put(path)
                                .put(", expectedRows=").put(expectedRowCount)
                                .put(", actualRows=").put(rows).put(']');
                    }
                    rwMem.putLong(offset);
                    LOG.info().$("write complete. Index file length: ").$(rwMem.getAppendOffset()).$();
                }
            }
        } finally {
            path.trimTo(trimTo);
        }
    }

    @Override
    protected boolean isSupportedColumn(RecordMetadata metadata, int columnIndex) {
        return metadata.getColumnType(columnIndex) == ColumnType.STRING;
    }
}
