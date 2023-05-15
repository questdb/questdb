/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

/**
 * Rebuild index independently of TableWriter
 * Main purpose is for support cases when table data is corrupt and TableWriter cannot be opened
 */
public class IndexBuilder extends RebuildColumnBase {
    private static final Log LOG = LogFactory.getLog(IndexBuilder.class);
    private final MemoryMAR ddlMem;
    private final MemoryMR indexMem = Vm.getMRInstance();
    private final SymbolColumnIndexer indexer;

    public IndexBuilder(CairoConfiguration configuration) {
        super(configuration);
        ddlMem = Vm.getMARInstance(configuration.getCommitMode());
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
            BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
            try {
                LOG.info().$("writing ").utf8(path).$();
                ddlMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WRITER);
                BitmapIndexWriter.initKeyMemory(ddlMem, indexValueBlockCapacity);
            } catch (CairoException e) {
                // looks like we could not create key file properly
                // lets not leave half-baked file sitting around
                LOG.error()
                        .$("could not create index [name=").utf8(path)
                        .$(", errno=").$(e.getErrno())
                        .$(']').$();
                if (!ff.remove(path)) {
                    LOG.error()
                            .$("could not remove '").utf8(path).$("'. Please remove MANUALLY.")
                            .$("[errno=").$(ff.errno())
                            .$(']').$();
                }
                throw e;
            } finally {
                // this close() closes the underlying file, but ddlMem object remains reusable
                ddlMem.close();
            }
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$("could not create index [name=").utf8(path).$(']').$();
                throw CairoException.critical(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
            LOG.info().$("writing ").utf8(path).$();
        } finally {
            path.trimTo(plen);
        }
    }

    private void removeFile(FilesFacade ff, Path path) {
        LOG.info().$("deleting ").utf8(path).$();
        if (!ff.remove(this.path)) {
            if (!ff.exists(this.path)) {
                // This is fine, index can be corrupt, rewriting is what we try to do here
                LOG.info().$("index file did not exist, file will be re-written [path=").utf8(path).I$();
            } else {
                throw CairoException.critical(ff.errno()).put("cannot remove index file");
            }
        }
    }

    private void removeIndexFiles(FilesFacade ff, CharSequence columnName, long columnNameTxn) {
        final int plen = path.length();
        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
        removeFile(ff, path);

        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn);
        removeFile(ff, path);
    }

    protected void doReindex(
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            int columnWriterIndex,
            CharSequence columnName,
            long partitionNameTxn,
            long partitionSize,
            long partitionTimestamp,
            int partitionBy,
            int indexValueBlockCapacity
    ) {
        final int trimTo = path.length();
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, partitionNameTxn);
        try {
            final int plen = path.length();

            if (ff.exists(path.$())) {
                try (final MemoryMR roMem = indexMem) {
                    long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, columnWriterIndex);
                    removeIndexFiles(ff, columnName, columnNameTxn);
                    TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);

                    final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, columnWriterIndex);
                    if (columnTop > -1L) {

                        if (partitionSize > columnTop) {
                            LOG.info().$("indexing [path=").utf8(path).I$();
                            createIndexFiles(ff, columnName, indexValueBlockCapacity, plen, columnNameTxn);
                            TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);
                            roMem.of(
                                    ff,
                                    path,
                                    0,
                                    (partitionSize - columnTop) * Integer.BYTES,
                                    MemoryTag.MMAP_TABLE_WRITER
                            );
                            try {
                                indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop);
                                indexer.index(roMem, columnTop, partitionSize);
                            } finally {
                                indexer.clear();
                            }
                        }
                    } else {
                        LOG.info().$("column is empty in partition [path=").$(path).I$();
                    }
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
