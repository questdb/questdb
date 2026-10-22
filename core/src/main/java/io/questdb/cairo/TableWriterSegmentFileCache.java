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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;
import static io.questdb.std.Files.SEPARATOR;

// Opens and closes WAL segment column files.
// A segment that is still being written to (not marked as last segment usage) keeps its
// column files open AND mapped between apply blocks: the mappings are created with headroom
// past the current end of file, so the next block's row range usually falls inside them and
// costs neither an mmap() nor a munmap(). Each munmap() is a process-wide mmap_lock critical
// section with a TLB shootdown, which is what limits WAL apply on many-core machines.
public class TableWriterSegmentFileCache {
    private static final Log LOG = LogFactory.getLog(TableWriterSegmentFileCache.class);
    private final CairoConfiguration configuration;
    private final TableToken tableToken;
    // bytes mapped past the start of each column range so that the next block's range fits the live mapping
    private static final long COLUMN_MAP_HEADROOM = 16L << 20;
    private final WeakClosableObjectPool<MemoryCMOR> walColumnMemoryPool;
    private final LongObjHashMap<ObjList<MemoryCMOR>> walFdCache = new LongObjHashMap<>();
    private final WeakClosableObjectPool<ObjList<MemoryCMOR>> walFdCacheListPool = new WeakClosableObjectPool<>(ObjList::new, 5, true);
    private final LongObjHashMap.LongObjConsumer<ObjList<MemoryCMOR>> walFdCloseCachedFdAction;
    private final ObjList<MemoryCMOR> walMappedColumns = new ObjList<>();
    private int walFdCacheSize;


    public TableWriterSegmentFileCache(TableToken tableToken, CairoConfiguration configuration) {
        this.tableToken = tableToken;
        this.configuration = configuration;
        ObjectFactory<MemoryCMOR> memoryFactory = configuration.getBypassWalFdCache()
                ? TableWriterSegmentFileCache::openMemoryCMORBypassFdCache
                : TableWriterSegmentFileCache::openMemoryCMORNormal;

        walColumnMemoryPool = new WeakClosableObjectPool<>(memoryFactory, configuration.getWalMaxSegmentFileDescriptorsCache(), true);
        walFdCloseCachedFdAction = (_, memList) -> {
            for (int i = 0, n = memList.size(); i < n; i++) {
                MemoryCMOR mem = memList.getQuick(i);
                LOG.debug().$("closing cached wal column [fd=").$(mem.getFd()).I$();
                Misc.free(mem);   // unmaps and closes the fd
                walColumnMemoryPool.push(mem);
            }
            memList.clear();
            walFdCacheListPool.push(memList);
        };
    }

    public void closeWalFiles(boolean isLastSegmentUsage, long walSegmentId, int lo) {
        LOG.debug().$("closing wal columns [table=").$(tableToken)
                .$(", walSegmentId=").$(walSegmentId)
                .$(", isLastSegmentUsage=").$(isLastSegmentUsage)
                .I$();

        int key = walFdCache.keyIndex(walSegmentId);
        boolean cacheIsDisabled = configuration.getBypassWalFdCache();
        boolean cacheIsFull = walFdCacheSize >= configuration.getWalMaxSegmentFileDescriptorsCache();
        if (isLastSegmentUsage || cacheIsFull || cacheIsDisabled) {
            if (key < 0) {
                // the mapped columns were handed out by mmapSegments(), the list is empty
                ObjList<MemoryCMOR> mems = walFdCache.valueAt(key);
                walFdCache.removeAt(key);
                walFdCacheSize--;
                mems.clear();
                walFdCacheListPool.push(mems);
            }

            for (int col = lo, n = walMappedColumns.size(); col < n; col++) {
                MemoryCMOR mappedColumnMem = walMappedColumns.get(col);
                if (mappedColumnMem != null) {
                    Misc.free(mappedColumnMem);
                    walColumnMemoryPool.push(mappedColumnMem);
                }
            }
            walMappedColumns.setPos(lo);
        } else {
            ObjList<MemoryCMOR> mems = null;
            if (key > -1) {
                // Keep the column memories open and mapped for the next block of this segment
                mems = walFdCacheListPool.pop();
                walFdCache.putAt(key, walSegmentId, mems);
                walFdCacheSize++;
            }

            for (int col = lo, n = walMappedColumns.size(); col < n; col++) {
                MemoryCMOR mappedColumnMem = walMappedColumns.getQuick(col);
                if (mappedColumnMem != null) {
                    if (mems != null) {
                        mems.add(mappedColumnMem);
                    } else {
                        Misc.free(mappedColumnMem);
                        walColumnMemoryPool.push(mappedColumnMem);
                    }
                }
            }
            walMappedColumns.setPos(lo);
        }

        if (cacheIsFull && lo == 0) {
            // Close all cached FDs.
            // It is possible to use more complicated algo and evict only those which
            // will not be used in the near future, but it's non-trivial
            // and can ruin the benefit of caching any FDs.
            // This supposed to happen rarely.
            closeWalFiles();
        }
    }

    public void closeWalFiles(TableWriterSegmentCopyInfo segmentCopyInfo, int writerColumnCount) {
        if (getWalMappedColumns().size() == 0) {
            // Everything is already closed.
            return;
        }
        for (int seg = segmentCopyInfo.getSegmentCount() - 1; seg > -1; seg--) {
            int segmentId = segmentCopyInfo.getSegmentId(seg);
            int walId = segmentCopyInfo.getWalId(seg);
            long walIdSegmentId = Numbers.encodeLowHighInts(segmentId, walId);
            boolean isLastSegmentUse = segmentCopyInfo.isLastSegmentUse(seg);

            int lo = getWalMappedColumns().size() - writerColumnCount * 2;
            closeWalFiles(isLastSegmentUse, walIdSegmentId, lo);

            // When cache is full, all wal columns are closed, no need to check the rest of the segments.
            if (getWalMappedColumns().size() == 0) {
                break;
            }
        }
    }

    public void closeWalFiles() {
        walFdCache.forEach(walFdCloseCachedFdAction);
        walFdCache.clear();
        walFdCacheSize = 0;
    }

    // Copies the address of the column in all the open segments into a dense pre-allocated buffer.
    public void createAddressBuffersPrimary(int columnIndex, int columnCount, int segmentCount, long mappedAddrBuffPrimary) {
        int walColumnCountPerSegment = columnCount * 2;

        for (int i = 0; i < segmentCount; i++) {
            var segmentColumnPrimary = walMappedColumns.get(walColumnCountPerSegment * i + 2 * columnIndex);
            Unsafe.putLong(mappedAddrBuffPrimary + (long) i * Long.BYTES, segmentColumnPrimary.addressOf(0));
        }
    }

    public long createAddressBuffersSecondary(int columnIndex, int columnCount, TableWriterSegmentCopyInfo segmentCopyInfo, long mappedAddrBuffSecondary, ColumnTypeDriver driver) {
        int walColumnCountPerSegment = columnCount * 2;

        long totalVarSize = 0;
        for (int i = 0, n = segmentCopyInfo.getSegmentCount(); i < n; i++) {
            var segmentColumnAux = walMappedColumns.get(walColumnCountPerSegment * i + 2 * columnIndex + 1);
            long segmentColumnAuxAddr = segmentColumnAux.addressOf(0);
            Unsafe.putLong(mappedAddrBuffSecondary + (long) i * Long.BYTES, segmentColumnAuxAddr);
            totalVarSize += driver.getDataVectorSize(segmentColumnAuxAddr, segmentCopyInfo.getRowLo(i), segmentCopyInfo.getRowHi(i) - 1);
        }
        return totalVarSize;
    }

    public ObjList<MemoryCMOR> getWalMappedColumns() {
        return walMappedColumns;
    }

    public void mmapSegments(TableMetadata metadata, @Transient Path walPath, long walSegmentId, long rowLo, long rowHi) {
        int timestampIndex = metadata.getTimestampIndex();
        LOG.debug().$("open columns [table=").$(tableToken)
                .$(", walSegmentId=").$(walSegmentId)
                .I$();
        int walPathLen = walPath.size();
        final int columnCount = metadata.getColumnCount();
        int fdCacheKey = walFdCache.keyIndex(walSegmentId);
        // column memories kept open and mapped by closeWalFiles() for a live segment, in file order
        ObjList<MemoryCMOR> mems = null;
        if (fdCacheKey < 0) {
            mems = walFdCache.valueAt(fdCacheKey);
        }
        int initialSize = walMappedColumns.size();
        int file = 0;

        try {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final int columnType = metadata.getColumnType(columnIndex);
                if (columnType > 0) {
                    int sizeBitsPow2 = ColumnType.getWalDataColumnShl(columnType, columnIndex == timestampIndex);

                    if (ColumnType.isVarSize(columnType)) {
                        MemoryCMOR dataMem = mems != null ? mems.getQuick(file) : walColumnMemoryPool.pop();
                        MemoryCMOR auxMem = mems != null ? mems.getQuick(file + 1) : walColumnMemoryPool.pop();

                        walMappedColumns.add(dataMem);
                        walMappedColumns.add(auxMem);

                        // same fd as the memory already holds: ofOffset() keeps the live mapping when the range fits
                        final long dataFd = mems != null ? dataMem.getFd() : -1;
                        final long auxFd = mems != null ? auxMem.getFd() : -1;
                        file += 2;

                        final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);

                        LPSZ ifile = auxFd == -1 ? iFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        if (auxFd != -1) {
                            LOG.debug().$("reusing file descriptor for WAL files [fd=").$(auxFd)
                                    .$(", path=").$(walPath)
                                    .$(", walSegment=").$(walSegmentId)
                                    .I$();
                        }
                        columnTypeDriver.configureAuxMemOM(
                                configuration.getFilesFacade(),
                                auxMem,
                                auxFd,
                                ifile,
                                rowLo,
                                rowHi,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                        walPath.trimTo(walPathLen);

                        LPSZ dfile = dataFd == -1 ? dFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        if (dataFd != -1) {
                            LOG.debug().$("reusing file descriptor for WAL files [fd=").$(dataFd)
                                    .$(", wal=").$(walPath)
                                    .$(", walSegment=").$(walSegmentId)
                                    .I$();
                        }
                        columnTypeDriver.configureDataMemOM(
                                configuration.getFilesFacade(),
                                auxMem,
                                dataMem,
                                dataFd,
                                dfile,
                                rowLo,
                                rowHi,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                    } else {
                        MemoryCMOR primary = mems != null ? mems.getQuick(file) : walColumnMemoryPool.pop();
                        walMappedColumns.add(primary);
                        walMappedColumns.add(null);

                        long fd = mems != null ? primary.getFd() : -1;
                        file++;
                        LPSZ dfile = fd == -1 ? dFile(walPath, metadata.getColumnName(columnIndex), -1L) : null;
                        if (fd != -1) {
                            LOG.debug().$("reusing file descriptor for WAL files [fd=").$(fd)
                                    .$(", path=").$(walPath)
                                    .$(", walSegment=").$(walSegmentId)
                                    .I$();
                        }
                        primary.ofOffset(
                                configuration.getFilesFacade(),
                                fd,
                                false,
                                dfile,
                                rowLo << sizeBitsPow2,
                                rowHi << sizeBitsPow2,
                                MemoryTag.MMAP_TABLE_WRITER,
                                CairoConfiguration.O_NONE
                        );
                    }
                    walPath.trimTo(walPathLen);
                } else {
                    walMappedColumns.add(null);
                    walMappedColumns.add(null);
                }
            }
        } catch (Throwable th) {
            if (mems != null) {
                // cached memories not handed out yet are not in walMappedColumns: free them here
                for (int i = file, n = mems.size(); i < n; i++) {
                    Misc.free(mems.getQuick(i));
                    walColumnMemoryPool.push(mems.getQuick(i));
                }
            }
            closeWalFiles(true, walSegmentId, initialSize);
            walMappedColumns.setPos(initialSize); // already done by closeWalFiles(); kept for clarity
            throw th;
        } finally {
            // The memories now live in walMappedColumns; drop the cache entry so nothing is freed twice.
            if (fdCacheKey < 0) {
                walFdCache.removeAt(fdCacheKey);
                walFdCacheSize--;
            }

            if (mems != null) {
                mems.clear();
                walFdCacheListPool.push(mems);
            }
        }
    }


    public void mmapWalColsEager() {
        for (int i = 0, n = walMappedColumns.size(); i < n; i++) {
            MemoryCR columnMem = walMappedColumns.get(i);
            if (columnMem != null) {
                columnMem.map();
            }
        }
    }

    public void mmapWalColumns(TableWriterSegmentCopyInfo segmentCopyInfo, TableMetadata metadata, @Transient Path path) {
        int pathSize1 = path.size();
        try {
            path.concat(WalUtils.WAL_NAME_BASE);
            int walBaseLen = path.size();
            try {
                for (int i = 0, n = segmentCopyInfo.getSegmentCount(); i < n; i++) {
                    int walId = segmentCopyInfo.getWalId(i);
                    int segmentId = segmentCopyInfo.getSegmentId(i);
                    path.trimTo(walBaseLen).put(walId).put(SEPARATOR).put(segmentId);
                    long rowLo = segmentCopyInfo.getRowLo(i);
                    long rowHi = segmentCopyInfo.getRowHi(i);
                    long walIdSegmentId = Numbers.encodeLowHighInts(segmentId, walId);
                    mmapSegments(metadata, path, walIdSegmentId, rowLo, rowHi);
                }
            } catch (Throwable th) {
                // Close all the columns without placing into the cache.
                Misc.freeObjListAndClear(walMappedColumns);
                closeWalFiles();
                throw th;
            }
        } finally {
            path.trimTo(pathSize1);
        }
    }

    private static MemoryCMOR openMemoryCMORBypassFdCache() {
        return Vm.getMemoryCMOR(true, COLUMN_MAP_HEADROOM);
    }

    private static MemoryCMOR openMemoryCMORNormal() {
        return Vm.getMemoryCMOR(false, COLUMN_MAP_HEADROOM);
    }
}
