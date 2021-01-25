/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderOpenColumnJob extends AbstractQueueConsumerJob<OutOfOrderOpenColumnTask> {

    private final CairoConfiguration configuration;

    public OutOfOrderOpenColumnJob(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> queue,
            Sequence subSeq
    ) {
        super(queue, subSeq);
        this.configuration = configuration;
    }

    private static long mapReadWriteOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        long addr = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (addr != -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap [file=").put(path).put(", fd=").put(fd).put(", size=").put(size).put(']');
    }

    private static long openReadWriteOrFail(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd != -1) {
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private static void truncateToSizeOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        if (ff.isRestrictedFileSystem()) {
            return;
        }
        if (!ff.truncate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("could resize [file=").put(path).put(", size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    private void appendTxnToPath(Path path, long txn) {
        path.put("-n-").put(txn);
    }

    private void createDirsOrFail(FilesFacade ff, Path path) {
        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor) {
        OutOfOrderOpenColumnTask task = queue.get(cursor);
        // copy task on stack so that publisher has fighting chance of
        // publishing all it has to the queue

        final boolean locked = task.tryLock();
        if (locked) {
            openColumn(task, cursor, subSeq);
        } else {
            subSeq.done(cursor);
        }

        return true;
    }

    private long getOutOfOrderVarColumnSize(
            ContiguousVirtualMemory oooFixColumn,
            ContiguousVirtualMemory oooVarColumn,
            long indexLo,
            long indexHi,
            long indexMax
    ) {
        // todo: duplicate logic
        // todo: fix & var columns are reversed, we need to normalize this
        final long lo = oooVarColumn.getLong(indexLo * Long.BYTES);
        final long hi;
        if (indexHi == indexMax - 1) {
            hi = oooFixColumn.getAppendOffset();
        } else {
            hi = oooVarColumn.getLong((indexHi + 1) * Long.BYTES);
        }
        return (hi - lo);
    }

    private long getVarColumnLength(
            long indexLo,
            long indexHi,
            long srcFixed,
            long srcFixedSize,
            long srcVarSize
    ) {
        final long lo = Unsafe.getUnsafe().getLong(srcFixed + indexLo * Long.BYTES);
        final long hi;
        if (indexHi + 1 == srcFixedSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = Unsafe.getUnsafe().getLong(srcFixed + (indexHi + 1) * Long.BYTES);
        }
        return hi - lo;
    }

    private long getVarColumnSize(FilesFacade ff, int columnType, long dataFd, long lastValueOffset) {
        final long addr;
        final long offset;
        if (columnType == ColumnType.STRING) {
            addr = ff.mmap(dataFd, lastValueOffset + Integer.BYTES, 0, Files.MAP_RO);
            final int len = Unsafe.getUnsafe().getInt(addr + lastValueOffset);
            ff.munmap(addr, lastValueOffset + Integer.BYTES);
            if (len < 1) {
                offset = lastValueOffset + Integer.BYTES;
            } else {
                offset = lastValueOffset + Integer.BYTES + len * 2L; // character bytes
            }
        } else {
            // BINARY
            addr = ff.mmap(dataFd, lastValueOffset + Long.BYTES, 0, Files.MAP_RO);
            final long len = Unsafe.getUnsafe().getLong(addr + lastValueOffset);
            ff.munmap(addr, lastValueOffset + Long.BYTES);
            if (len < 1) {
                offset = lastValueOffset + Long.BYTES;
            } else {
                offset = lastValueOffset + Long.BYTES + len;
            }
        }
        return offset;
    }


    private void oooOpenLastPartitionForAppend(OutOfOrderOpenColumnTask task) {
        final int columnType = task.getColumnType();
        final FilesFacade ff = task.getFf();
        final long indexLo = task.getOooIndexLo();
        final long indexHi = task.getOooIndexHi();
        final long indexMax = task.getOooIndexMax();
        final Path path = task.getPath();
        final int plen = path.length();
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                //
                final AppendMemory mem2 = task.getVarColumn();
                final long offset2 = mem2.getAppendOffset();
                long fd2 = -mem2.getFd();
                long size2 = (indexHi - indexLo + 1) * Long.BYTES + offset2;
                truncateToSizeOrFail(ff, null, -fd2, size2);
                long addr2 = mapReadWriteOrFail(ff, null, Math.abs(fd2), size2);

                final AppendMemory mem1 = task.getFixColumn();
                long oooSize1 = getOutOfOrderVarColumnSize(task.getOooFixColumn(), task.getOooVarColumn(), indexLo, indexHi, indexMax);
                final long offset1 = mem1.getAppendOffset();
                long fd1 = -mem1.getFd();
                final long size1 = oooSize1 + offset1;
                truncateToSizeOrFail(ff, null, -fd1, size1);
                final long addr1 = mapReadWriteOrFail(ff, null, -fd1, size1);
                break;
            default:
                long oooSize = (indexHi - indexLo + 1) << ColumnType.pow2SizeOf(columnType);
                final AppendMemory mem = task.getFixColumn();
                final long offset = mem.getAppendOffset();
                long fd = -mem.getFd();
                final long size = oooSize + offset;
                truncateToSizeOrFail(ff, null, -fd, size);
                long addr = mapReadWriteOrFail(ff, null, -fd, size);

                if (task.isColumnIndexed()) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), task.getColumnName());
                    long kFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), task.getColumnName());
                    long vFd = openReadWriteOrFail(ff, path);
                    // Transfer value of destination offset to the index start offset
                    // This is where we need to begin indexing from. The index will contain all the values before the offset
                    // index from "offset"
                }
                break;
        }
    }

    private void oooOpenLastPartitionForMerge(OutOfOrderOpenColumnTask task) {
        final Path path = task.getPath();
        final long indexLo = task.getOooIndexLo();
        final long indexHi = task.getOooIndexHi();
        final long indexMax = task.getOooIndexMax();
        final long mergeOOOHi = task.getMergeOOOHi();
        final long mergeOOOLo = task.getMergeOOOLo();
        final long mergeDataHi = task.getMergeDataHi();
        final long mergeDataLo = task.getMergeDataLo();
        final long dataIndexMax = task.getDataIndexMax();
        final int prefixType = task.getPrefixType();
        final long prefixLo = task.getPrefixLo();
        final long prefixHi = task.getPrefixHi();

        final int columnType = task.getColumnType();
        final boolean isColumnIndexed = task.isColumnIndexed();
        final CharSequence columnName = task.getColumnName();
        final FilesFacade ff = task.getFf();

        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final int plen = path.length();

        final AppendMemory mem2 = task.getFixColumn();
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                // index files are opened as normal
                final AppendMemory mem1 = task.getVarColumn();
                iFile(path.trimTo(plen), columnName);
                long srcFixFd = -mem1.getFd();
                long srcFixAddr = mem1.getAppendOffset();
                long srcFixSize = mapReadWriteOrFail(ff, path, Math.abs(srcFixFd), srcFixAddr);

                // open data file now
                dFile(path.trimTo(plen), columnName);
                long srcVarFd = -mem2.getFd();
                long srcVarSize = mem2.getAppendOffset();
                long srcVarAddr = mapReadWriteOrFail(ff, path, Math.abs(srcVarFd), srcVarSize);

                appendTxnToPath(path.trimTo(plen), task.getTxn());
                path.concat(columnName);
                final int pColNameLen = path.length();

                path.put(FILE_SUFFIX_I).$();
                createDirsOrFail(ff, path);

                long dstFixFd = openReadWriteOrFail(ff, path);
                long dstFixSize = (indexHi - indexLo + 1 + dataIndexMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                long dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                path.trimTo(pColNameLen);
                path.put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);
                long dstVarFd = openReadWriteOrFail(ff, path);
                long dstVarSize = srcVarSize + getOutOfOrderVarColumnSize(
                        task.getOooFixColumn(),
                        task.getOooVarColumn(),
                        indexLo,
                        indexHi,
                        indexMax
                );
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                long dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);

                // append offset is 0
                //MergeStruct.setDestAppendOffsetFromOffset0(mergeStruct, varColumnStructOffset, 0L);

                // configure offsets
                long fixOffset1;
                final ContiguousVirtualMemory oooVarColumn = task.getOooVarColumn();
                final ContiguousVirtualMemory oooFixColumn = task.getOooFixColumn();
                final long varOffset1;
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        varOffset1 = getVarColumnLength(
                                prefixLo,
                                prefixHi,
                                oooVarColumn.addressOf(0),
                                oooVarColumn.getAppendOffset(),
                                oooFixColumn.getAppendOffset()

                        );
                        break;
                    case OO_BLOCK_DATA:
                        varOffset1 = getVarColumnLength(prefixLo, prefixHi, srcFixAddr, srcFixSize, srcVarSize);
                        break;
                    default:
                        varOffset1 = 0;
                        break;
                }

                fixOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                // offset 2
                final long fixOffset2;
                final long varOffset2;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    long oooLen = getVarColumnLength(
                            mergeOOOLo,
                            mergeOOOHi,
                            oooVarColumn.addressOf(0),
                            oooVarColumn.getAppendOffset(),
                            oooFixColumn.getAppendOffset()
                    );

                    long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcFixAddr, srcFixSize, srcVarSize);

                    fixOffset2 = fixOffset1 + (mergeLen * Long.BYTES);
                    varOffset2 = varOffset1 + oooLen + dataLen;
                } else {
                    fixOffset2 = fixOffset1;
                    varOffset2 = varOffset1;
                }
                break;
            default:
                long srcFd = -mem2.getFd();
                final int shl = ColumnType.pow2SizeOf(columnType);
                dFile(path.trimTo(plen), columnName);
                long srcSize = mem2.getAppendOffset();
                long srcAddr = mapReadWriteOrFail(ff, path, Math.abs(srcFd), srcSize);

                appendTxnToPath(path.trimTo(plen), task.getTxn());
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);

                long dstFd = openReadWriteOrFail(ff, path);
                long dstSize = ((indexHi - indexLo + 1) + dataIndexMax) << shl;
                truncateToSizeOrFail(ff, path, Math.abs(dstFd), dstSize);
                long dstAddr = mapReadWriteOrFail(ff, path, Math.abs(dstFd), dstSize);
                // configure offsets for fixed columns
                long offset0 = 0;
                long offset1 = (prefixHi - prefixLo + 1) << shl;
                long offset2;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    offset2 = offset1 + (mergeLen << shl);
                } else {
                    offset2 = offset1;
                }

                if (isColumnIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    long keyFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    long valFd = openReadWriteOrFail(ff, path);
                    // Transfer value of destination offset to the index start offset
                    // This is where we need to begin indexing from. The index will contain all the values before the offset
//                    MergeStruct.setDestIndexStartOffsetFromOffset(mergeStruct, fixColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnStructOffset, MergeStruct.STAGE_PREFIX));
                }
                break;
        }
    }

    private void oooOpenMidPartitionForAppend(OutOfOrderOpenColumnTask task) {
        final Path path = task.getPath();
        final int plen = path.length();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
        final FilesFacade ff = task.getFf();
        final long indexLo = task.getOooIndexLo();
        final long indexHi = task.getOooIndexHi();
        final long indexMax = task.getOooIndexMax();
        final long dataIndexMax = task.getDataIndexMax();
        final long timestampFd = task.getTimestampFd();
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING: {
                // index files are opened as normal
                iFile(path.trimTo(plen), columnName);
                long dstFixFd = openReadWriteOrFail(ff, path);
                long dstFixSize = (indexHi - indexLo + 1 + dataIndexMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                long dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);
                long dstFixOffset0 = dataIndexMax * Long.BYTES;
//                MergeStruct.setDestAppendOffsetFromOffset0(mergeStruct, fixColumnStructOffset, dataIndexMax * Long.BYTES);

                // open data file now
                dFile(path.trimTo(plen), columnName);
                final long dstVarFd = openReadWriteOrFail(ff, path);
                final long dataOffset = getVarColumnSize(
                        ff,
                        columnType,
                        dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset0 - Long.BYTES)
                );
                long dstVarOffset0 = getOutOfOrderVarColumnSize(
                        task.getOooFixColumn(),
                        task.getOooVarColumn(),
                        indexLo,
                        indexHi,
                        indexMax
                ) + dataOffset;
                truncateToSizeOrFail(ff, path, Math.abs(dstVarFd), dstVarOffset0);
                long dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarOffset0);
            }
            break;
            default: {
                final int shl = ColumnType.pow2SizeOf(columnType);
                long dstFixFd;
                final long dstFixSize = (indexHi - indexLo + 1 + dataIndexMax) << shl;
                final long dstFixAddr;
                final long dstFixOffset0 = dataIndexMax << shl;
                if (timestampFd > 0) {
                    dstFixFd = -timestampFd;
                    truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                    dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);
                } else {
                    dFile(path.trimTo(plen), columnName);
                    dstFixFd = openReadWriteOrFail(ff, path);
                    truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                    dstFixAddr = mapReadWriteOrFail(ff, null, dstFixFd, dstFixSize);

                    // no "src" index files to copy back to
                    if (task.isColumnIndexed()) {
                        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                        long kFd = openReadWriteOrFail(ff, path);
                        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                        long vFd = openReadWriteOrFail(ff, path);
                        // Transfer value of destination offset to the index start offset
                        // This is where we need to begin indexing from. The index will contain all the values before the offset
                        // dstFixOffset0 is where we start indexing from
//                        MergeStruct.setDestIndexStartOffsetFromOffset(mergeStruct, fixColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnStructOffset, MergeStruct.STAGE_PREFIX));
                    }
                }
            }
            break;
        }

        // this is append, so we will be needing stage "2" offsets only
        // copy stage "0" offsets to stage "2"
//        MergeStruct.setDestAppendOffsetFromOffset2(mergeStruct, fixColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnStructOffset, MergeStruct.STAGE_PREFIX));
//        MergeStruct.setDestAppendOffsetFromOffset2(mergeStruct, varColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, varColumnStructOffset, MergeStruct.STAGE_PREFIX));
    }


    private void oooOpenMidPartitionForMerge(OutOfOrderOpenColumnTask task) {
        final Path path = task.getPath();
        final long mergeOOOLo = task.getMergeOOOLo();
        final long mergeOOOHi = task.getMergeOOOHi();
        final long mergeDataLo = task.getMergeDataLo();
        final long mergeDataHi = task.getMergeDataHi();
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final long indexLo = task.getOooIndexLo();
        final long indexHi = task.getOooIndexHi();
        final long indexMax = task.getOooIndexMax();
        final long dataIndexMax = task.getDataIndexMax();
        final int plen = path.length();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
        final long txn = task.getTxn();
        final FilesFacade ff = task.getFf();
        final ContiguousVirtualMemory oooVarColumn = task.getOooVarColumn();
        final ContiguousVirtualMemory oooFixColumn = task.getOooFixColumn();
        final int prefixType = task.getPrefixType();
        final long prefixLo = task.getPrefixLo();
        final long prefixHi = task.getPrefixHi();
        final long timestampFd = task.getTimestampFd();

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                iFile(path.trimTo(plen), task.getColumnName());
                long srcFixFd = openReadWriteOrFail(ff, path);
                long srcFixSize = dataIndexMax * Long.BYTES;
                long srcFixAddr = mapReadWriteOrFail(ff, path, srcFixFd, srcFixSize);

                dFile(path.trimTo(plen), task.getColumnName());
                long srcVarFd = openReadWriteOrFail(ff, path);
                final long srcVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcVarFd,
                        Unsafe.getUnsafe().getLong(srcFixAddr + srcFixSize - Long.BYTES)
                );
                long srcVarAddr = mapReadWriteOrFail(ff, path, srcVarFd, srcVarSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPathAndEnsureDir(ff, path, columnName, FILE_SUFFIX_I);

                long dstFixFd = openReadWriteOrFail(ff, path);
                long dstFixSize = (indexHi - indexLo + 1 + dataIndexMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                long dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);
                long dstFixAppendOffset0 = 0;
//                MergeStruct.setDestAppendOffsetFromOffset0(mergeStruct, fixColumnStructOffset, 0L);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPathAndEnsureDir(ff, path, columnName, FILE_SUFFIX_D);
                long dstVarSize = srcVarSize + getOutOfOrderVarColumnSize(
                        task.getOooFixColumn(),
                        task.getOooVarColumn(),
                        indexLo, indexHi,
                        indexMax
                );
                long dstVarFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                long dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);
                long dstVarAppendOffset0 = 0;
//                MergeStruct.setDestAppendOffsetFromOffset0(mergeStruct, varColumnStructOffset, 0L);

                // configure offsets
                final long dstVarAppendOffset1;
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(
                                prefixLo,
                                prefixHi,
                                oooVarColumn.addressOf(0),
                                oooVarColumn.getAppendOffset(),
                                oooFixColumn.getAppendOffset()

                        );
                        break;
                    case OO_BLOCK_DATA:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcFixAddr, srcFixSize, srcVarSize);
                        break;
                    default:
                        dstVarAppendOffset1 = 0;
                        break;
                }

                final long dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                long dstVarAppendOffset2 = dstVarAppendOffset1;
                long dstFixAppendOffset2 = dstFixAppendOffset1;
                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    long oooLen = getVarColumnLength(
                            mergeOOOLo,
                            mergeOOOHi,
                            oooVarColumn.addressOf(0),
                            oooVarColumn.getAppendOffset(),
                            oooFixColumn.getAppendOffset()
                    );

                    long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcFixAddr, srcFixSize, srcVarSize);

                    dstVarAppendOffset2 += oooLen + dataLen;
                    dstFixAppendOffset2 += mergeLen * Long.BYTES;
                }
                break;

            default:
                final long srcFixFd1;
                if (timestampFd > 0) {
                    // ensure timestamp srcFixFd is always negative, we will close it externally
                    srcFixFd1 = -timestampFd;
                } else {
                    dFile(path.trimTo(plen), columnName);
                    srcFixFd1 = openReadWriteOrFail(ff, path);
                }

                final int shl = ColumnType.pow2SizeOf(columnType);
                final long srcFixSize1 = dataIndexMax << shl;
                dFile(path.trimTo(plen), columnName);
                final long srcFixAddr1 = mapReadWriteOrFail(ff, path, Math.abs(srcFixFd1), srcFixSize1);

                appendTxnToPath(path.trimTo(plen), txn);
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);

                long dstFixFd1 = openReadWriteOrFail(ff, path);
                long dstFixSize1 = ((indexHi - indexLo + 1) + dataIndexMax) << shl;
                truncateToSizeOrFail(ff, path, dstFixFd1, dstFixSize1);
                long dstFixAddr1 = mapReadWriteOrFail(ff, path, dstFixFd1, dstFixSize1);
                long dstFix1AppendOffset0 = 0;
                long dstFix1AppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                long dstFix1AppendOffset2;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFix1AppendOffset2 = dstFix1AppendOffset1 + (mergeLen << shl);
                } else {
                    dstFix1AppendOffset2 = dstFix1AppendOffset1;
                }

                // we have "src" index
                if (task.isColumnIndexed()) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    long kFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    long vFd = openReadWriteOrFail(ff, path);
                    // Transfer value of destination offset to the index start offset
                    // This is where we need to begin indexing from. The index will contain all the values before the offset
                    // index from dstFix1AppendOffset0
//                    MergeStruct.setDestIndexStartOffsetFromOffset(mergeStruct, fixColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnStructOffset, MergeStruct.STAGE_PREFIX));
                }
                break;
        }
    }


    private void oooSetPathAndEnsureDir(FilesFacade ff, Path path, CharSequence columnName, CharSequence suffix) {
        createDirsOrFail(ff, path.concat(columnName).put(suffix).$());
    }

    private void openColumn(OutOfOrderOpenColumnTask task, long cursor, Sequence subSeq) {
        int mode = task.getOpenColumnMode();
        switch (mode) {
            case 1:
                oooOpenMidPartitionForAppend(task);
                break;
            case 2:
                oooOpenLastPartitionForAppend(task);
                break;
            case 3:
                oooOpenMidPartitionForMerge(task);
                break;
            case 4:
                oooOpenLastPartitionForMerge(task);
                break;
            case 5:
                openColumnForAppend(task);
                break;
        }
    }

    private void openColumnForAppend(OutOfOrderOpenColumnTask task) {
        final int columnType = task.getColumnType();
        final long indexLo = task.getOooIndexLo();
        final long indexHi = task.getOooIndexHi();
        final long indexMax = task.getOooIndexMax();
        final Path path = task.getPath();
        final FilesFacade ff = task.getFf();
        final boolean isColumnIndexed = task.isColumnIndexed();
        final CharSequence columnName = task.getColumnName();
        final int plen = path.length();

        final long dataFd;
        final long dataAddr;
        final long dataSize;
        final long dataOffset = 0;
        final long indexFd;
        final long indexAddr;
        final long indexSize;
        final long indexOffset = 0;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_I);
                indexFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, indexFd, (indexHi - indexLo + 1) * Long.BYTES);
                indexSize = (indexHi - indexLo + 1) * Long.BYTES;
                indexAddr = mapReadWriteOrFail(ff, path, indexFd, indexSize);

                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dataFd = openReadWriteOrFail(ff, path);
                dataSize = getOutOfOrderVarColumnSize(
                        task.getOooFixColumn(),
                        task.getOooVarColumn(),
                        indexLo,
                        indexHi,
                        indexMax
                );
                truncateToSizeOrFail(ff, path, dataFd, dataSize);
                dataAddr = mapReadWriteOrFail(ff, path, dataFd, dataSize);

                break;
            default:
                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dataFd = openReadWriteOrFail(ff, path);
                dataSize = (indexHi - indexLo + 1) << ColumnType.pow2SizeOf(columnType);
                truncateToSizeOrFail(ff, path, dataFd, dataSize);
                dataAddr = mapReadWriteOrFail(ff, path, dataFd, dataSize);

                if (isColumnIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    long iFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    long vFd = openReadWriteOrFail(ff, path);
                    // this offset is zero, we are creating new partition
                    long vOffset = 0;
//                    MergeStruct.setDestIndexStartOffsetFromOffset(mergeStruct, fixColumnStructOffset, MergeStruct.getDestAppendOffsetFromOffsetStage(mergeStruct, fixColumnStructOffset, MergeStruct.STAGE_PREFIX));
                }
                break;
        }

    }
}
