/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;

/**
 * Rebuild index independently of TableWriter
 * Main purpose is for support cases when table data is corrupt and TableWriter cannot be opened
 */
public class IndexBuilder extends RebuildColumnBase {
    private static final Log LOG = LogFactory.getLog(IndexBuilder.class);
    private final CairoConfiguration configuration;
    // Scratch lists used by configureCoveringIfNeeded. Kept as instance
    // fields so a single-column reindex avoids allocations across partitions.
    private final IntList coveringIndices = new IntList();
    private final ObjList<CharSequence> coveringNames = new ObjList<>();
    private final LongList coveringNameTxns = new LongList();
    private final IntList coveringShifts = new IntList();
    private final LongList coveringTops = new LongList();
    private final IntList coveringTypes = new IntList();
    private final MemoryMAR ddlMem;
    private byte indexType = IndexType.BITMAP;
    private SymbolColumnIndexer indexer;

    public IndexBuilder(CairoConfiguration configuration) {
        super(configuration);
        this.configuration = configuration;
        ddlMem = Vm.getPMARInstance(configuration);
        indexer = new SymbolColumnIndexer(configuration, indexType);
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
            LPSZ lpsz = IndexFactory.keyFileName(indexType, path.trimTo(plen), columnName, columnNameTxn);
            try {
                LOG.info().$("writing ").$(path).$();
                ddlMem.smallFile(ff, lpsz, MemoryTag.MMAP_TABLE_WRITER);
                IndexFactory.initKeyMemory(indexType, ddlMem, indexValueBlockCapacity);
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
            if (!ff.touch(IndexFactory.valueFileName(indexType, path.trimTo(plen), columnName, columnNameTxn, 0L))) {
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
        if (IndexType.isPosting(indexType)) {
            // Enumerate every sealed .pv / .pc<N> for this column instance
            // across all on-disk sealTxn generations.
            PostingIndexUtils.removeAllSealedFiles(ff, path, plen, columnName, columnNameTxn);
            removeFile(ff, IndexFactory.keyFileName(indexType, path.trimTo(plen), columnName, columnNameTxn));
        } else {
            removeFile(ff, IndexFactory.keyFileName(indexType, path.trimTo(plen), columnName, columnNameTxn));
            // BITMAP keeps a single .v at columnNameTxn (no sealTxn axis).
            removeFile(ff, IndexFactory.valueFileName(indexType, path.trimTo(plen), columnName, columnNameTxn, columnNameTxn));
        }
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
            int indexValueBlockCapacity,
            byte indexType,
            RecordMetadata metadata,
            int columnIndex
    ) {
        // Update index type and recreate indexer if needed
        if (this.indexType != indexType) {
            this.indexType = indexType;
            Misc.free(indexer);
            indexer = new SymbolColumnIndexer(configuration, indexType);
        }
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
                            indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop, partitionTimestamp, partitionNameTxn);
                            // Configure covering BEFORE seal: if the index has
                            // INCLUDE columns, the seal path emits the .pci and
                            // .pc<N>.*.* sidecar files. Without this configure
                            // call, removeIndexFiles wipes the sidecars and the
                            // seal recreates only .pk + .pv, leaving covering
                            // queries to silently fall back to the slower
                            // column-file read path.
                            configureCoveringIfNeeded(metadata, columnIndex, columnVersionReader, partitionTimestamp);
                            indexer.index(ff, columnDataFd, columnTop, partitionSize);
                            indexer.seal();
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

    private void configureCoveringIfNeeded(
            RecordMetadata metadata,
            int columnIndex,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        if (metadata == null || columnIndex < 0 || columnIndex >= metadata.getColumnCount()) {
            return;
        }
        if (!IndexType.isPosting(this.indexType)) {
            return;
        }
        TableColumnMetadata colMeta = metadata.getColumnMetadata(columnIndex);
        IntList coveringWriterIndices = colMeta.getCoveringColumnIndices();
        if (coveringWriterIndices == null || coveringWriterIndices.size() == 0) {
            return;
        }
        coveringNames.clear();
        coveringNameTxns.clear();
        coveringTops.clear();
        coveringShifts.clear();
        coveringIndices.clear();
        coveringTypes.clear();
        for (int i = 0, n = coveringWriterIndices.size(); i < n; i++) {
            int covWriterIdx = coveringWriterIndices.getQuick(i);
            // Resolve writer→dense index. For freshly-loaded TableReaderMetadata
            // (no DROPs yet) dense and writer indices match; for general
            // metadata we walk the column list. coverCount is small (a handful
            // of INCLUDE columns), so the linear scan is negligible.
            int denseCovIdx = -1;
            for (int k = 0, m = metadata.getColumnCount(); k < m; k++) {
                if (metadata.getColumnMetadata(k).getWriterIndex() == covWriterIdx) {
                    denseCovIdx = k;
                    break;
                }
            }
            if (denseCovIdx < 0) {
                // Covering column was dropped — record an "absent" slot. The
                // seal records this as type=-1 and skips the read.
                coveringNames.add(null);
                coveringNameTxns.add(TableUtils.COLUMN_NAME_TXN_NONE);
                coveringTops.add(0);
                coveringShifts.add(0);
                coveringIndices.add(-1);
                coveringTypes.add(-1);
                continue;
            }
            int covType = metadata.getColumnType(denseCovIdx);
            coveringNames.add(metadata.getColumnName(denseCovIdx));
            coveringNameTxns.add(columnVersionReader.getColumnNameTxn(partitionTimestamp, covWriterIdx));
            coveringTops.add(columnVersionReader.getColumnTop(partitionTimestamp, covWriterIdx));
            coveringShifts.add(ColumnType.pow2SizeOf(covType));
            coveringIndices.add(covWriterIdx);
            coveringTypes.add(covType);
        }
        indexer.configureCovering(coveringNames, coveringNameTxns, coveringTops, coveringShifts,
                coveringIndices, coveringTypes, metadata.getTimestampIndex());
    }

    @Override
    protected boolean isSupportedColumn(RecordMetadata metadata, int columnIndex) {
        return metadata.isColumnIndexed(columnIndex);
    }
}
