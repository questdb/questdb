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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.validationException;

public class TableReaderMetadata extends AbstractRecordMetadata implements TableMetadata, Mutable {
    protected final CairoConfiguration configuration;
    private final IntList columnOrderList = new IntList();
    private final FilesFacade ff;
    private final LowerCaseCharSequenceIntHashMap tmpValidationMap = new LowerCaseCharSequenceIntHashMap();
    private boolean isCopy;
    private boolean isSoftLink;
    private int maxUncommittedRows;
    private MemoryCARW metaCopyMem; // used when loadFrom() called
    private MemoryMR metaMem;
    private long metadataVersion;
    private long o3MaxLag;
    private int partitionBy;
    private Path path;
    private int plen;
    private int tableId;
    private TableToken tableToken;
    private TableReaderMetadataTransitionIndex transitionIndex;
    private MemoryMR transitionMeta;
    private int ttlHoursOrMonths;
    private boolean walEnabled;

    public TableReaderMetadata(CairoConfiguration configuration, TableToken tableToken) {
        try {
            this.configuration = configuration;
            this.ff = configuration.getFilesFacade();
            this.tableToken = tableToken;
            this.path = new Path();
            this.path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            this.plen = path.size();
            this.isSoftLink = Files.isSoftLink(path.$());
            this.metaMem = Vm.getCMRInstance();
            this.metaCopyMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_METADATA_READER);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    // This constructor used to read random metadata files.
    public TableReaderMetadata(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableToken = null;
        this.metaMem = Vm.getCMRInstance();
    }

    public TableReaderMetadataTransitionIndex applyTransition() {
        // Swap meta and transitionMeta. It's fine if we're dealing with
        // a metadata copy and metaMem wasn't initialized.
        MemoryMR temp = this.metaMem;
        this.metaMem = this.transitionMeta;
        transitionMeta = temp;
        isCopy = false;
        Misc.free(transitionMeta); // memory is safe to double close, do not assign null to transitionMeta
        Misc.free(metaCopyMem); // close copy memory in case if it was in-use

        return applyTransition0(metaMem, columnCount);
    }

    public TableReaderMetadataTransitionIndex applyTransitionFrom(TableReaderMetadata srcMeta) {
        copyMemFrom(srcMeta);
        isCopy = true;
        Misc.free(metaMem);
        Misc.free(transitionMeta);

        return applyTransition0(metaCopyMem, columnCount);
    }

    @Override
    public void clear() {
        super.clear();
        Misc.free(metaMem);
        Misc.free(metaCopyMem);
        Misc.free(transitionMeta);
        isCopy = false;
    }

    @Override
    public void close() {
        metaMem = Misc.free(metaMem);
        metaCopyMem = Misc.free(metaCopyMem);
        transitionMeta = Misc.free(transitionMeta);
        path = Misc.free(path);
        isCopy = false;
    }

    public void dumpTo(MemoryMA mem) {
        // This may be mmapped _meta file or its copy.
        final MemoryR metaMem = getMetaMem();
        // Since _meta files are immutable and get updated with a single atomic rename
        // operation replacing the old file with the new one, it's ok to clone the metadata
        // by copying metaMem's contents. Even if _meta file was already replaced, the file
        // should be still kept on disk until inode's ref counter is above zero.
        long len = metaMem.size();
        for (long p = 0; p < len; p++) {
            mem.putByte(metaMem.getByte(p));
        }
    }

    public int getDenseSymbolIndex(int columnIndex) {
        return ((TableReaderMetadataColumn) columnMetadata.getQuick(columnIndex)).getDenseSymbolIndex();
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getIndexValueBlockCapacity();
    }

    @Override
    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    @Override
    public long getMetadataVersion() {
        return metadataVersion;
    }

    @Override
    public long getO3MaxLag() {
        return o3MaxLag;
    }

    @Override
    public int getPartitionBy() {
        return partitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolIndexFlag();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return getColumnMetadata(columnIndex).getSymbolCapacity();
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public CharSequence getTableName() {
        return tableToken.getTableName();
    }

    @Override
    public TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public int getTtlHoursOrMonths() {
        return ttlHoursOrMonths;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return getColumnMetadata(columnIndex).isSymbolIndexFlag();
    }

    public boolean isSoftLink() {
        return isSoftLink;
    }

    @Override
    public boolean isWalEnabled() {
        return walEnabled;
    }

    public void loadMetadata(LPSZ path) {
        try {
            isCopy = false;
            Misc.free(metaCopyMem);
            metaMem.smallFile(ff, path, MemoryTag.NATIVE_TABLE_READER);
            TableUtils.validateMeta(metaMem, null, ColumnType.VERSION);
            readFromMem(metaMem);
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public void loadMetadata() {
        final long spinLockTimeout = configuration.getSpinLockTimeout();
        final MillisecondClock millisecondClock = configuration.getMillisecondClock();
        long deadline = configuration.getMillisecondClock().getTicks() + spinLockTimeout;
        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        boolean existenceChecked = false;
        while (true) {
            try {
                loadMetadata(path.$());
                return;
            } catch (CairoException ex) {
                if (!existenceChecked) {
                    path.trimTo(plen).slash();
                    if (!ff.exists(path.$())) {
                        throw CairoException.tableDoesNotExist(tableToken.getTableName());
                    }
                    path.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
                }
                existenceChecked = true;
                TableUtils.handleMetadataLoadException(tableToken, deadline, ex, millisecondClock, spinLockTimeout);
            }
        }
    }

    public void loadFrom(TableReaderMetadata srcMeta) {
        assert tableToken.equals(srcMeta.tableToken);
        // Copy src meta memory.
        copyMemFrom(srcMeta);
        // Now, read it.
        try {
            isCopy = true;
            Misc.free(metaMem);
            readFromMem(metaCopyMem);
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public boolean prepareTransition(long txnMetadataVersion) {
        if (transitionMeta == null) {
            transitionMeta = Vm.getCMRInstance();
        }

        path.trimTo(plen).concat(TableUtils.META_FILE_NAME);
        transitionMeta.smallFile(ff, path.$(), MemoryTag.NATIVE_TABLE_READER);
        if (transitionMeta.size() >= TableUtils.META_OFFSET_METADATA_VERSION + 8
                && txnMetadataVersion != transitionMeta.getLong(TableUtils.META_OFFSET_METADATA_VERSION)) {
            // No match
            return false;
        }

        tmpValidationMap.clear();
        TableUtils.validateMeta(transitionMeta, tmpValidationMap, ColumnType.VERSION);
        return true;
    }

    public void readFromMem(MemoryR mem) {
        int columnCount = mem.getInt(TableUtils.META_OFFSET_COUNT);
        int timestampIndex = mem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.partitionBy = mem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
        this.tableId = mem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.maxUncommittedRows = mem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.o3MaxLag = mem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
        this.metadataVersion = mem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);
        this.walEnabled = mem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);
        this.ttlHoursOrMonths = TableUtils.getTtlHoursOrMonths(mem);
        this.columnMetadata.clear();
        this.timestampIndex = -1;

        TableUtils.buildColumnListFromMetadataFile(mem, columnCount, columnOrderList);
        this.columnNameIndexMap.clear();

        for (int i = 0, n = columnOrderList.size(); i < n; i += 3) {
            int writerIndex = columnOrderList.get(i);
            if (writerIndex < 0) {
                continue;
            }
            int stableIndex = i / 3;
            CharSequence name = mem.getStrA(columnOrderList.get(i + 1));
            int denseSymbolIndex = columnOrderList.get(i + 2);

            assert name != null;
            int columnType = TableUtils.getColumnType(mem, writerIndex);

            if (columnType > -1) {
                String colName = Chars.toString(name);
                columnMetadata.add(
                        new TableReaderMetadataColumn(
                                colName,
                                columnType,
                                TableUtils.isColumnIndexed(mem, writerIndex),
                                TableUtils.getIndexBlockCapacity(mem, writerIndex),
                                true,
                                null,
                                writerIndex,
                                TableUtils.isColumnDedupKey(mem, writerIndex),
                                denseSymbolIndex,
                                stableIndex,
                                TableUtils.isSymbolCached(mem, writerIndex),
                                TableUtils.getSymbolCapacity(mem, writerIndex)
                        )
                );
                int denseIndex = columnMetadata.size() - 1;
                if (!columnNameIndexMap.put(colName, denseIndex)) {
                    throw validationException(mem).put("Duplicate column [name=").put(name).put("] at ").put(i);
                }
                if (writerIndex == timestampIndex) {
                    this.timestampIndex = denseIndex;
                }
            }
        }
        this.columnCount = columnMetadata.size();
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }

    private TableReaderMetadataTransitionIndex applyTransition0(MemoryR newMetaMem, int existingColumnCount) {
        columnNameIndexMap.clear();

        int columnCount = newMetaMem.getInt(TableUtils.META_OFFSET_COUNT);
        assert columnCount >= existingColumnCount;
        columnMetadata.setPos(columnCount);
        int timestampIndex = newMetaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.tableId = newMetaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.metadataVersion = newMetaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION);
        this.maxUncommittedRows = newMetaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.o3MaxLag = newMetaMem.getLong(TableUtils.META_OFFSET_O3_MAX_LAG);
        this.walEnabled = newMetaMem.getBool(TableUtils.META_OFFSET_WAL_ENABLED);
        this.ttlHoursOrMonths = TableUtils.getTtlHoursOrMonths(newMetaMem);

        int shiftLeft = 0, existingIndex = 0;
        TableUtils.buildColumnListFromMetadataFile(newMetaMem, columnCount, columnOrderList);
        int newColumnCount = newMetaMem.getInt(TableUtils.META_OFFSET_COUNT);

        if (transitionIndex == null) {
            transitionIndex = new TableReaderMetadataTransitionIndex();
        } else {
            transitionIndex.clear();
        }

        TableUtils.buildColumnListFromMetadataFile(newMetaMem, newColumnCount, columnOrderList);
        for (int i = 0, n = columnOrderList.size(); i < n; i += 3) {
            int stableIndex = i / 3;
            int writerIndex = columnOrderList.get(i);
            if (writerIndex < 0) {
                continue;
            }
            CharSequence name = newMetaMem.getStrA(columnOrderList.get(i + 1));
            assert name != null;
            int denseSymbolIndex = columnOrderList.get(i + 2);
            int newColumnType = TableUtils.getColumnType(newMetaMem, writerIndex);
            int columnType = TableUtils.getColumnType(newMetaMem, writerIndex);
            boolean isIndexed = TableUtils.isColumnIndexed(newMetaMem, writerIndex);
            boolean isDedupKey = TableUtils.isColumnDedupKey(newMetaMem, writerIndex);
            int indexBlockCapacity = TableUtils.getIndexBlockCapacity(newMetaMem, writerIndex);
            boolean symbolIsCached = TableUtils.isSymbolCached(newMetaMem, writerIndex);
            int symbolCapacity = TableUtils.getSymbolCapacity(newMetaMem, writerIndex);
            TableReaderMetadataColumn existing = null;
            String newName;

            if (existingIndex < existingColumnCount) {
                existing = (TableReaderMetadataColumn) columnMetadata.getQuick(existingIndex);
                int existingStableIndex = existing.getStableIndex();
                if (existingStableIndex > stableIndex && columnType < 0) {
                    // This column must be deleted so existing dense columns do not contain it
                    continue;
                }
            }

            // index structure is
            // [action: deleted | reused, copy from:int index]
            // "copy from" >= 0 indicates that column is to be copied from old position
            // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
            // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source

            int outIndex = existingIndex - shiftLeft;
            if (newColumnType < 0) {
                shiftLeft++; // Deleted in new
                if (existing != null) {
                    transitionIndex.markDeleted(existingIndex);
                }
            } else {
                // existing column
                boolean rename = existing != null && !Chars.equals(existing.getColumnName(), name);
                newName = rename || existing == null ? Chars.toString(name) : existing.getColumnName();

                if (rename
                        || existing == null
                        || existing.getWriterIndex() != writerIndex
                        || existing.isSymbolIndexFlag() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                        || existing.isDedupKeyFlag() != isDedupKey
                        || existing.getDenseSymbolIndex() != denseSymbolIndex
                        || existing.getStableIndex() != stableIndex
                ) {
                    // new
                    columnMetadata.setQuick(
                            outIndex,
                            new TableReaderMetadataColumn(
                                    newName,
                                    columnType,
                                    isIndexed,
                                    indexBlockCapacity,
                                    true,
                                    null,
                                    writerIndex,
                                    isDedupKey,
                                    denseSymbolIndex,
                                    stableIndex,
                                    symbolIsCached,
                                    symbolCapacity
                            )
                    );
                    if (existing != null) {
                        // column deleted at existingIndex
                        transitionIndex.markDeleted(existingIndex);
                    }
                    transitionIndex.markCopyFrom(outIndex, writerIndex);
                } else {
                    // reuse
                    columnMetadata.setQuick(outIndex, existing);
                    transitionIndex.markReusedAction(outIndex, existingIndex);
                    if (existingIndex > outIndex) {
                        // mark to do nothing with existing column, this may be overwritten later
                        transitionIndex.markReplaced(existingIndex);
                    }
                }
                columnNameIndexMap.put(newName, outIndex);
                if (timestampIndex == writerIndex) {
                    this.timestampIndex = outIndex;
                }
            }
            existingIndex++;
        }

        columnMetadata.setPos(existingIndex - shiftLeft);
        this.columnCount = columnMetadata.size();
        if (timestampIndex < 0) {
            this.timestampIndex = timestampIndex;
        }

        return transitionIndex;
    }

    private void copyMemFrom(TableReaderMetadata srcMeta) {
        final MemoryR srcMetaMem = srcMeta.getMetaMem();
        long len = srcMetaMem.size();
        metaCopyMem.jumpTo(0);
        for (long p = 0; p < len; p++) {
            metaCopyMem.putByte(srcMetaMem.getByte(p));
        }
    }

    private MemoryR getMetaMem() {
        return !isCopy ? metaMem : metaCopyMem;
    }
}
