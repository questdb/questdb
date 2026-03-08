/*******************************************************************************
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
import io.questdb.std.LongList;
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
// This class also reduces file open/close operations by caching file descriptors.
// when the segment is marked as not last segment usage.
public class TableWriterSegmentFileCache {
    private static final Log LOG = LogFactory.getLog(TableWriterSegmentFileCache.class);
    private final CairoConfiguration configuration;
    private final TableToken tableToken;
    private final WeakClosableObjectPool<MemoryCMOR> walColumnMemoryPool;
    private final LongObjHashMap<LongList> walFdCache = new LongObjHashMap<>();
    private final WeakClosableObjectPool<LongList> walFdCacheListPool = new WeakClosableObjectPool<>(LongList::new, 5, true);
    private final LongObjHashMap.LongObjConsumer<LongList> walFdCloseCachedFdAction;
    private final ObjList<MemoryCMOR> walMappedColumns = new ObjList<>();
    private int walFdCacheSize;


    public TableWriterSegmentFileCache(TableToken tableToken, CairoConfiguration configuration) {
        this.tableToken = tableToken;
        this.configuration = configuration;
        ObjectFactory<MemoryCMOR> memoryFactory = configuration.getBypassWalFdCache()
                ? TableWriterSegmentFileCache::openMemoryCMORBypassFdCache
                : TableWriterSegmentFileCache::openMemoryCMORNormal;

        FilesFacade ff = configuration.getFilesFacade();
        walColumnMemoryPool = new WeakClosableObjectPool<>(memoryFactory, configuration.getWalMaxSegmentFileDescriptorsCache(), true);
        walFdCloseCachedFdAction = (key, fdList) -> {
            for (int i = 0, n = fdList.size(); i < n; i++) {
                long fd = fdList.get(i);
                LOG.debug().$("closing wal fd cache [fd=").$(fd).I$();
                ff.close(fd);
            }
            fdList.clear();
            walFdCacheListPool.push(fdList);
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
                LongList fds = walFdCache.valueAt(key);
                walFdCache.removeAt(key);
                walFdCacheSize--;
                fds.clear();
                walFdCacheListPool.push(fds);
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
            LongList fds = null;
            if (key > -1) {
                // Add FDs to a new FD cache list
                fds = walFdCacheListPool.pop();
                walFdCache.putAt(key, walSegmentId, fds);
                walFdCacheSize++;
            }

            for (int col = lo, n = walMappedColumns.size(); col < n; col++) {
                MemoryCMOR mappedColumnMem = walMappedColumns.getQuick(col);
                if (mappedColumnMem != null) {
                    long fd = mappedColumnMem.detachFdClose();
                    if (fds != null) {
                        fds.add(fd);
                    }
                    walColumnMemoryPool.push(mappedColumnMem);
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
            Unsafe.getUnsafe().putLong(mappedAddrBuffPrimary + (long) i * Long.BYTES, segmentColumnPrimary.addressOf(0));
        }
    }

    public long createAddressBuffersSecondary(int columnIndex, int columnCount, TableWriterSegmentCopyInfo segmentCopyInfo, long mappedAddrBuffSecondary, ColumnTypeDriver driver) {
        int walColumnCountPerSegment = columnCount * 2;

        long totalVarSize = 0;
        for (int i = 0, n = segmentCopyInfo.getSegmentCount(); i < n; i++) {
            var segmentColumnAux = walMappedColumns.get(walColumnCountPerSegment * i + 2 * columnIndex + 1);
            long segmentColumnAuxAddr = segmentColumnAux.addressOf(0);
            Unsafe.getUnsafe().putLong(mappedAddrBuffSecondary + (long) i * Long.BYTES, segmentColumnAuxAddr);
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
        LongList fds = null;
        if (fdCacheKey < 0) {
            fds = walFdCache.valueAt(fdCacheKey);
        }
        int initialSize = walMappedColumns.size();

        try {
            int file = 0;
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final int columnType = metadata.getColumnType(columnIndex);
                if (columnType > 0) {
                    int sizeBitsPow2 = ColumnType.getWalDataColumnShl(columnType, columnIndex == timestampIndex);

                    if (ColumnType.isVarSize(columnType)) {
                        MemoryCMOR auxMem = walColumnMemoryPool.pop();
                        MemoryCMOR dataMem = walColumnMemoryPool.pop();

                        walMappedColumns.add(dataMem);
                        walMappedColumns.add(auxMem);

                        final long dataFd = fds != null ? fds.get(file++) : -1;
                        final long auxFd = fds != null ? fds.get(file++) : -1;

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
                        MemoryCMOR primary = walColumnMemoryPool.pop();
                        walMappedColumns.add(primary);
                        walMappedColumns.add(null);

                        long fd = fds != null ? fds.get(file++) : -1;
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
            closeWalFiles(true, walSegmentId, initialSize);
            walMappedColumns.setPos(initialSize); // already done by closeWalFiles(); kept for clarity
            // Avoid double removal/pool push by the finally block when cached FDs were consumed.
            fds = null;
            throw th;
        } finally {
            // Now that the FDs are used in the column objects, remove them from the cache.
            // to avoid double close in case of exceptions.
            if (fdCacheKey < 0) {
                walFdCache.removeAt(fdCacheKey);
                walFdCacheSize--;
            }

            if (fds != null) {
                fds.clear();
                walFdCacheListPool.push(fds);
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
        return Vm.getMemoryCMOR(true);
    }

    private static MemoryCMOR openMemoryCMORNormal() {
        return Vm.getMemoryCMOR(false);
    }
}
