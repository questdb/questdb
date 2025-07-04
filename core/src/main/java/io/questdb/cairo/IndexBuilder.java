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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;

/**
 * Rebuild index independently of TableWriter
 * Main purpose is for support cases when table data is corrupt and TableWriter cannot be opened
 */
public class IndexBuilder extends RebuildColumnBase {
    private static final Log LOG = LogFactory.getLog(IndexBuilder.class);
    private final MemoryMAR ddlMem;
    private final SymbolColumnIndexer indexer;

    public IndexBuilder(CairoConfiguration configuration) {
        super(configuration);
        ddlMem = Vm.getPMARInstance(configuration);
        indexer = new SymbolColumnIndexer(configuration);
        unsupportedColumnMessage = "Column is not indexed";
    }

    @Override
    public void clear() {
        super.clear();
        // ddlMem is idempotent, we can call close() as many times as we need,
        // but we reuse Java object after memory is closed (method of() will reopen memory)
        ddlMem.close();
        indexer.clear();
    }

    @Override
    public void close() {
        super.close();
        Misc.free(indexer);
    }

    private void createIndexFiles(FilesFacade ff, CharSequence columnName, int indexValueBlockCapacity, int plen, long columnNameTxn) {
        try {
            LPSZ lpsz = BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
            try {
                LOG.info().$("writing ").$(path).$();
                ddlMem.smallFile(ff, lpsz, MemoryTag.MMAP_TABLE_WRITER);
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half-baked file sitting around
                LOG.error()
                        .$("could not create index [name=").$(path)
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .$(", errno=").$(e.getErrno())
                        .I$();
                if (!ff.removeQuiet(lpsz)) {
                    LOG.error()
                            .$("could not remove '").$(path).$("'. Please remove MANUALLY.")
                            .$("[errno=").$(ff.errno())
                            .I$();
                }
                throw e;
            } finally {
                // this close() closes the underlying file, but ddlMem object remains reusable
                ddlMem.close();
            }
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$("could not create index [name=").$(path).I$();
                throw CairoException.critical(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
            LOG.info().$("writing ").$(path).$();
        } finally {
            path.trimTo(plen);
        }
    }

    private void removeFile(FilesFacade ff, LPSZ path) {
        LOG.info().$("deleting ").$(path).$();
        if (!ff.removeQuiet(path)) {
            int errno = ff.errno();
            if (!ff.exists(path)) {
                // This is fine, index can be corrupt, rewriting is what we try to do here
                LOG.info().$("index file did not exist, file will be re-written [path=").$(path).I$();
            } else {
                throw CairoException.critical(errno).put("could not remove index file [file=").put(this.path).put(']');
            }
        }
    }

    private void removeIndexFiles(FilesFacade ff, CharSequence columnName, long columnNameTxn) {
        final int plen = path.size();
        removeFile(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFile(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn));
    }

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
        final int trimTo = path.size();
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        try {
            final int plen = path.size();

            if (ff.exists(path.$())) {
                long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, columnWriterIndex);
                removeIndexFiles(ff, columnName, columnNameTxn);
                TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);

                final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, columnWriterIndex);
                if (columnTop > -1L) {
                    if (partitionSize > columnTop) {
                        LOG.info().$("indexing [path=").$(path).I$();
                        createIndexFiles(ff, columnName, indexValueBlockCapacity, plen, columnNameTxn);

                        long columnDataFd = TableUtils.openRO(ff, TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn), LOG);
                        try {
                            indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop);
                            indexer.index(ff, columnDataFd, columnTop, partitionSize);
                        } finally {
                            ff.close(columnDataFd);
                            indexer.clear();
                        }
                    }
                } else {
                    LOG.info().$("column is empty in partition [path=").$(path).I$();
                }
            } else {
                LOG.info().$("partition does not exist [path=").$(path).I$();
            }
        } finally {
            path.trimTo(trimTo);
        }
    }

    @Override
    protected boolean isSupportedColumn(RecordMetadata metadata, int columnIndex) {
        return metadata.isColumnIndexed(columnIndex);
    }
}
