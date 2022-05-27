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
    private final MemoryMR indexMem = Vm.getMRInstance();
    private final SymbolColumnIndexer indexer = new SymbolColumnIndexer();
    private final MemoryMAR ddlMem = Vm.getMARInstance();

    public IndexBuilder() {
        super();
        columnTypeErrorMsg = "Column is not indexed";
    }

    @Override
    public void clear() {
        super.clear();
        ddlMem.close();
        indexer.clear();
    }

    @Override
    public void close() {
        super.close();
        Misc.free(indexer);
    }

    @Override
    protected boolean checkColumnType(TableReaderMetadata metadata, int rebuildColumnIndex) {
        return metadata.isColumnIndexed(rebuildColumnIndex);
    }

    protected void rebuildColumn(
            CharSequence columnName,
            CharSequence partitionName,
            int indexValueBlockCapacity,
            long partitionSize,
            FilesFacade ff,
            ColumnVersionReader columnVersionReader,
            int columnIndex,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        path.trimTo(rootLen).concat(partitionName);
        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
        LOG.info().$("testing partition path").$(path).$();
        final int plen = path.length();

        if (ff.exists(path.$())) {
            try (final MemoryMR roMem = indexMem) {
                long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, columnIndex);
                removeIndexFiles(columnName, ff, columnNameTxn);
                TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);

                if (columnVersionReader.getColumnTopPartitionTimestamp(columnIndex) <= partitionTimestamp) {
                    LOG.info().$("indexing [path=").utf8(path).I$();
                    final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, columnIndex);
                    createIndexFiles(columnName, indexValueBlockCapacity, plen, ff, columnNameTxn);

                    if (partitionSize > columnTop) {
                        TableUtils.dFile(path.trimTo(plen), columnName, columnNameTxn);
                        final long columnSize = (partitionSize - columnTop) << ColumnType.pow2SizeOf(ColumnType.INT);
                        roMem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_WRITER);
                        indexer.configureWriter(configuration, path.trimTo(plen), columnName, columnNameTxn, columnTop);
                        indexer.index(roMem, columnTop, partitionSize);
                        indexer.clear();
                    }
                }
            }
        } else {
            LOG.info().$("partition does not exit ").$(path).$();
        }
    }

    private void createIndexFiles(CharSequence columnName, int indexValueBlockCapacity, int plen, FilesFacade ff, long columnNameTxn) {
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
                ddlMem.close();
            }
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$("could not create index [name=").utf8(path).$(']').$();
                throw CairoException.instance(ff.errno()).put("could not create index [name=").put(path).put(']');
            }
            LOG.info().$("writing ").utf8(path).$();
        } finally {
            path.trimTo(plen);
        }
    }

    private void removeFile(Path path, FilesFacade ff) {
        LOG.info().$("deleting ").utf8(path).$();
        if (!ff.remove(this.path)) {
            if (!ff.exists(this.path)) {
                // This is fine, index can be corrupt, rewriting is what we try to do here
                LOG.info().$("index file did not exist, file will be re-written [path=").utf8(path).I$();
            } else {
                throw CairoException.instance(ff.errno()).put("cannot remove index file");
            }
        }
    }

    private void removeIndexFiles(CharSequence columnName, FilesFacade ff, long columnNameTxn) {
        final int plen = path.length();
        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);
        removeFile(path, ff);

        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn);
        removeFile(path, ff);
    }
}
