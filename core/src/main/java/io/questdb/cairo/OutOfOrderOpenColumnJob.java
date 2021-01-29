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
import io.questdb.tasks.OutOfOrderCopyTask;
import io.questdb.tasks.OutOfOrderOpenColumnTask;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class OutOfOrderOpenColumnJob extends AbstractQueueConsumerJob<OutOfOrderOpenColumnTask> {

    private final CairoConfiguration configuration;
    private final RingQueue<OutOfOrderCopyTask> outboundQueue;
    private final Sequence outboundPubSeq;

    public OutOfOrderOpenColumnJob(
            CairoConfiguration configuration,
            RingQueue<OutOfOrderOpenColumnTask> queue,
            Sequence subSeq,
            RingQueue<OutOfOrderCopyTask> outboundQueue,
            Sequence outboundPubSeq
    ) {
        super(queue, subSeq);
        this.configuration = configuration;
        this.outboundQueue = outboundQueue;
        this.outboundPubSeq = outboundPubSeq;
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

    private long getVarColumnLength(
            long srcLo,
            long srcHi,
            long srcFixAddr,
            long srcFixSize,
            long srcVarSize
    ) {
        final long lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = Unsafe.getUnsafe().getLong(srcFixAddr + (srcHi + 1) * Long.BYTES);
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
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final Path path = task.getPath();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final boolean isIndexed = task.isIndexed();
        final int plen = path.length();

        // todo: cache these
        final AtomicInteger partCounter = new AtomicInteger(1);

        final long srcOooFixAddr = task.getOooFixColumn().addressOf(0);
        final long srcOooFixSize = task.getOooFixColumn().getAppendOffset();
        long srcOooVarAddr = 0;
        long srcOooVarSize = 0;
        long dstFixFd;
        long dstFixAddr;
        long dstFixOffset;
        long dstFixSize;
        long dstVarFd = 0;
        long dstVarSize = 0;
        long dstVarAddr = 0;
        long dstVarOffset = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        long dstIndexOffset = 0;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                final AppendMemory mem2 = task.getVarColumn();
                final AppendMemory mem1 = task.getFixColumn();
                srcOooVarAddr = task.getOooVarColumn().addressOf(0);
                srcOooVarSize = task.getOooVarColumn().getAppendOffset();

                dstFixOffset = mem2.getAppendOffset();
                dstFixFd = -mem2.getFd();
                dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES + dstFixOffset;
                truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);

                dstVarFd = -mem1.getFd();
                dstVarOffset = mem1.getAppendOffset();
                dstVarSize = dstVarOffset + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, null, -dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, null, -dstVarFd, dstVarSize);
                break;
            default:
                long oooSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(columnType);
                final AppendMemory mem = task.getFixColumn();
                dstFixOffset = mem.getAppendOffset();
                dstFixFd = -mem.getFd();
                dstFixSize = oooSize + dstFixOffset;
                truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);
                if (task.isIndexed()) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), task.getColumnName());
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), task.getColumnName());
                    dstVFd = openReadWriteOrFail(ff, path);
                    dstIndexOffset = dstFixOffset;
                }
                break;
        }

        publishCopyTask(
                columnCounter,
                partCounter,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                isIndexed
        );
    }

    private void oooOpenLastPartitionForMerge(OutOfOrderOpenColumnTask task) {
        final AtomicInteger columnCounter = task.getColumnCounter();
        final Path path = task.getPath();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final int mergeType = task.getMergeType();
        final long mergeOOOHi = task.getMergeOOOHi();
        final long mergeOOOLo = task.getMergeOOOLo();
        final long mergeDataHi = task.getMergeDataHi();
        final long mergeDataLo = task.getMergeDataLo();
        final long srcDataMax = task.getSrcDataMax();
        final int prefixType = task.getPrefixType();
        final long prefixLo = task.getPrefixLo();
        final long prefixHi = task.getPrefixHi();
        final int suffixType = task.getSuffixType();
        final long suffixLo = task.getSuffixLo();
        final long suffixHi = task.getSuffixHi();
        final int columnType = task.getColumnType();
        final boolean isIndexed = task.isIndexed();
        final CharSequence columnName = task.getColumnName();
        final FilesFacade ff = task.getFf();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final long srcOooFixAddr = task.getOooFixColumn().addressOf(0);
        final long srcOooFixSize = task.getOooFixColumn().getAppendOffset();
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final int plen = path.length();

        long srcDataFixFd;
        long srcDataFixAddr;
        long srcDataFixSize;
        long srcDataVarFd = 0;
        long srcDataVarSize = 0;
        long srcDataVarAddr = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstVarFd = 0;
        long dstVarSize = 0;
        long dstVarAddr = 0;
        long dstFixAppendOffset1;
        long dstFixAppendOffset2;
        long dstVarAppendOffset1 = 0;
        long dstVarAppendOffset2 = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        long srcOooVarAddr = 0;
        long srcOooVarSize = 0;

        // todo: cache
        final AtomicInteger partCounter = new AtomicInteger();
        int partCount = 0;

        final AppendMemory mem2 = task.getFixColumn();
        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                srcOooVarAddr = task.getOooVarColumn().addressOf(0);
                srcOooVarSize = task.getOooVarColumn().getAppendOffset();
                // index files are opened as normal
                final AppendMemory mem1 = task.getVarColumn();
                iFile(path.trimTo(plen), columnName);
                srcDataFixFd = -mem1.getFd();
                srcDataFixAddr = mem1.getAppendOffset();
                srcDataFixSize = mapReadWriteOrFail(ff, path, Math.abs(srcDataFixFd), srcDataFixAddr);

                // open data file now
                dFile(path.trimTo(plen), columnName);
                srcDataVarFd = -mem2.getFd();
                srcDataVarSize = mem2.getAppendOffset();
                srcDataVarAddr = mapReadWriteOrFail(ff, path, Math.abs(srcDataVarFd), srcDataVarSize);

                appendTxnToPath(path.trimTo(plen), task.getTxn());
                path.concat(columnName);
                final int pColNameLen = path.length();

                path.put(FILE_SUFFIX_I).$();
                createDirsOrFail(ff, path);
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                path.trimTo(pColNameLen);
                path.put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarSize = srcDataVarSize + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);

                // append offset is 0
                //MergeStruct.setDestAppendOffsetFromOffset0(mergeStruct, varColumnStructOffset, 0L);

                // configure offsets
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                        partCount++;
                        break;
                    case OO_BLOCK_DATA:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                        partCount++;
                        break;
                    default:
                        break;
                }

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    long oooLen = getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen * Long.BYTES);
                    dstVarAppendOffset2 = dstVarAppendOffset1 + oooLen + dataLen;
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                    dstVarAppendOffset2 = dstVarAppendOffset1;
                }

                break;
            default:
                srcDataFixFd = -mem2.getFd();
                final int shl = ColumnType.pow2SizeOf(columnType);
                dFile(path.trimTo(plen), columnName);
                srcDataFixSize = mem2.getAppendOffset();
                srcDataFixAddr = mapReadWriteOrFail(ff, path, -srcDataFixFd, srcDataFixSize);

                appendTxnToPath(path.trimTo(plen), task.getTxn());
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax) << shl;
                truncateToSizeOrFail(ff, path, Math.abs(dstFixFd), dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, Math.abs(dstFixFd), dstFixSize);
                // configure offsets for fixed columns
                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                }

                if (isIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }

                if (prefixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                break;
        }

        if (mergeType != OO_BLOCK_NONE) {
            partCount++;
        }

        if (suffixType != OO_BLOCK_NONE) {
            partCount++;
        }

        publishMultCopyTasks(
                partCount,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                suffixType,
                suffixLo,
                suffixHi,
                dstFixFd,
                dstFixAddr,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarSize,
                dstFixAppendOffset1,
                dstFixAppendOffset2,
                dstVarAppendOffset1,
                dstVarAppendOffset2,
                dstKFd,
                dstVFd,
                isIndexed
        );
    }

    private void oooOpenMidPartitionForAppend(OutOfOrderOpenColumnTask task) {
        final Path path = task.getPath();
        final int plen = path.length();
        final int columnType = task.getColumnType();
        final CharSequence columnName = task.getColumnName();
        final FilesFacade ff = task.getFf();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcDataMax = task.getSrcDataMax();
        final long timestampFd = task.getTimestampFd();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final boolean isIndexed = task.isIndexed();

        long srcOooFixAddr = task.getOooFixColumn().addressOf(0);
        long srcOooFixSize = task.getOooFixColumn().getAppendOffset();
        long srcOooVarAddr = 0;
        long srcOooVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        long dstIndexOffset = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstFixOffset;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstVarOffset = 0;
        // todo: cache
        final AtomicInteger partCounter = new AtomicInteger(1);

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:

                srcOooVarAddr = task.getOooVarColumn().addressOf(0);
                srcOooVarSize = task.getOooVarColumn().getAppendOffset();

                // index files are opened as normal
                iFile(path.trimTo(plen), columnName);
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixOffset = srcDataMax * Long.BYTES;

                // open data file now
                dFile(path.trimTo(plen), columnName);
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarOffset = getVarColumnSize(
                        ff,
                        columnType,
                        dstVarFd,
                        Unsafe.getUnsafe().getLong(dstFixAddr + dstFixOffset - Long.BYTES)
                );
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize) + dstVarOffset;
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);
                break;
            default:
                final int shl = ColumnType.pow2SizeOf(columnType);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax) << shl;
                dstFixOffset = srcDataMax << shl;
                if (timestampFd > 0) {
                    dstFixFd = -timestampFd;
                    truncateToSizeOrFail(ff, null, -dstFixFd, dstFixSize);
                    dstFixAddr = mapReadWriteOrFail(ff, null, -dstFixFd, dstFixSize);
                } else {
                    dFile(path.trimTo(plen), columnName);
                    dstFixFd = openReadWriteOrFail(ff, path);
                    truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                    dstFixAddr = mapReadWriteOrFail(ff, null, dstFixFd, dstFixSize);

                    dstIndexOffset = dstFixOffset;
                    if (task.isIndexed()) {
                        BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                        dstKFd = openReadWriteOrFail(ff, path);
                        BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                        dstVFd = openReadWriteOrFail(ff, path);
                    }
                }
                break;
        }

        publishCopyTask(
                columnCounter,
                partCounter,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcOooLo,
                srcOooHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                isIndexed
        );
    }

    private void oooOpenMidPartitionForMerge(OutOfOrderOpenColumnTask task) {
        final Path path = task.getPath();
        final long mergeOOOLo = task.getMergeOOOLo();
        final long mergeOOOHi = task.getMergeOOOHi();
        final long mergeDataLo = task.getMergeDataLo();
        final long mergeDataHi = task.getMergeDataHi();
        final long mergeLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcDataMax = task.getSrcDataMax();
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
        final int suffixType = task.getSuffixType();
        final long suffixLo = task.getSuffixLo();
        final long suffixHi = task.getSuffixHi();
        final int mergeType = task.getMergeType();
        final long timestampFd = task.getTimestampFd();
        final long timestampMergeIndexAddr = task.getTimestampMergeIndexAddr();
        final AtomicInteger partCounter = new AtomicInteger();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final boolean isIndexed = task.isIndexed();

        long srcDataFixFd;
        long srcDataFixSize;
        long srcDataFixAddr;
        long srcDataVarFd = 0;
        long srcDataVarAddr = 0;
        long srcDataVarSize = 0;

        final long srcOooFixAddr = oooFixColumn.addressOf(0);
        final long srcOooFixSize = oooFixColumn.getAppendOffset();
        long srcOooVarAddr = 0;
        long srcOooVarSize = 0;
        long dstFixFd;
        long dstFixSize;
        long dstFixAddr;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;
        int partCount = 0;
        long dstFixAppendOffset1;
        long dstFixAppendOffset2;
        long dstVarAppendOffset1 = 0;
        long dstVarAppendOffset2 = 0;

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                srcOooVarAddr = oooVarColumn.addressOf(0);
                srcOooVarSize = oooVarColumn.getAppendOffset();
                iFile(path.trimTo(plen), task.getColumnName());
                srcDataFixFd = openReadWriteOrFail(ff, path);
                srcDataFixSize = srcDataMax * Long.BYTES;
                srcDataFixAddr = mapReadWriteOrFail(ff, path, srcDataFixFd, srcDataFixSize);

                dFile(path.trimTo(plen), task.getColumnName());
                srcDataVarFd = openReadWriteOrFail(ff, path);
                srcDataVarSize = getVarColumnSize(
                        ff,
                        columnType,
                        srcDataVarFd,
                        Unsafe.getUnsafe().getLong(srcDataFixAddr + srcDataFixSize - Long.BYTES)
                );
                srcDataVarAddr = mapReadWriteOrFail(ff, path, srcDataVarFd, srcDataVarSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPathAndEnsureDir(ff, path, columnName, FILE_SUFFIX_I);

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1 + srcDataMax) * Long.BYTES;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                oooSetPathAndEnsureDir(ff, path, columnName, FILE_SUFFIX_D);
                dstVarSize = srcDataVarSize + getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                dstVarFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);

                // configure offsets
                switch (prefixType) {
                    case OO_BLOCK_OO:
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                        partCount++;
                        break;
                    case OO_BLOCK_DATA:
                        partCount++;
                        dstVarAppendOffset1 = getVarColumnLength(prefixLo, prefixHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                        break;
                    default:
                        dstVarAppendOffset1 = 0;
                        break;
                }

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) * Long.BYTES;

                dstVarAppendOffset2 = dstVarAppendOffset1;
                dstFixAppendOffset2 = dstFixAppendOffset1;
                // offset 2
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    final long oooLen = getVarColumnLength(mergeOOOLo, mergeOOOHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                    final long dataLen = getVarColumnLength(mergeDataLo, mergeDataHi, srcDataFixAddr, srcDataFixSize, srcDataVarSize);
                    dstVarAppendOffset2 += oooLen + dataLen;
                    dstFixAppendOffset2 += mergeLen * Long.BYTES;
                    partCount++;
                }

                if (suffixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                break;
            default:
                if (timestampFd > 0) {
                    // ensure timestamp srcDataFixFd is always negative, we will close it externally
                    srcDataFixFd = -timestampFd;
                } else {
                    dFile(path.trimTo(plen), columnName);
                    srcDataFixFd = openReadWriteOrFail(ff, path);
                }

                final int shl = ColumnType.pow2SizeOf(columnType);
                srcDataFixSize = srcDataMax << shl;
                dFile(path.trimTo(plen), columnName);
                srcDataFixAddr = mapReadWriteOrFail(ff, path, Math.abs(srcDataFixFd), srcDataFixSize);

                appendTxnToPath(path.trimTo(plen), txn);
                final int pDirNameLen = path.length();

                path.concat(columnName).put(FILE_SUFFIX_D).$();
                createDirsOrFail(ff, path);

                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = ((srcOooHi - srcOooLo + 1) + srcDataMax) << shl;
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                dstFixAppendOffset1 = (prefixHi - prefixLo + 1) << shl;
                if (mergeDataLo > -1 && mergeOOOLo > -1) {
                    dstFixAppendOffset2 = dstFixAppendOffset1 + (mergeLen << shl);
                } else {
                    dstFixAppendOffset2 = dstFixAppendOffset1;
                }

                // we have "src" index
                if (task.isIndexed()) {
                    BitmapIndexUtils.keyFileName(path.trimTo(pDirNameLen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(pDirNameLen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }

                if (prefixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                if (mergeType != OO_BLOCK_NONE) {
                    partCount++;
                }

                if (suffixType != OO_BLOCK_NONE) {
                    partCount++;
                }

                break;
        }

        publishMultCopyTasks(
                partCount,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                suffixType,
                suffixLo,
                suffixHi,
                dstFixFd,
                dstFixAddr,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarSize,
                dstFixAppendOffset1,
                dstFixAppendOffset2,
                dstVarAppendOffset1,
                dstVarAppendOffset2,
                dstKFd,
                dstVFd,
                isIndexed
        );
    }

    private void oooOpenNewPartitionForAppend(OutOfOrderOpenColumnTask task) {
        final int columnType = task.getColumnType();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final Path path = task.getPath();
        final FilesFacade ff = task.getFf();
        final boolean isColumnIndexed = task.isIndexed();
        final CharSequence columnName = task.getColumnName();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final boolean isIndexed = task.isIndexed();
        final int plen = path.length();
        final long dstFixFd;
        final long dstFixAddr;
        final long dstFixSize;
        long dstVarFd = 0;
        long dstVarAddr = 0;
        long dstVarSize = 0;
        long dstKFd = 0;
        long dstVFd = 0;

        final long srcOooFixAddr = task.getOooFixColumn().addressOf(0);
        final long srcOooFixSize = task.getOooFixColumn().getAppendOffset();
        long srcOooVarAddr = 0;
        long srcOooVarSize = 0;

        // todo: pool these
        AtomicInteger partCounter = new AtomicInteger(1);

        switch (columnType) {
            case ColumnType.BINARY:
            case ColumnType.STRING:

                srcOooVarAddr = task.getOooVarColumn().addressOf(0);
                srcOooVarSize = task.getOooVarColumn().getAppendOffset();

                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_I);
                dstFixFd = openReadWriteOrFail(ff, path);
                truncateToSizeOrFail(ff, path, dstFixFd, (srcOooHi - srcOooLo + 1) * Long.BYTES);
                dstFixSize = (srcOooHi - srcOooLo + 1) * Long.BYTES;
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstVarFd = openReadWriteOrFail(ff, path);
                dstVarSize = getVarColumnLength(srcOooLo, srcOooHi, srcOooFixAddr, srcOooFixSize, srcOooVarSize);
                truncateToSizeOrFail(ff, path, dstVarFd, dstVarSize);
                dstVarAddr = mapReadWriteOrFail(ff, path, dstVarFd, dstVarSize);
                break;
            default:
                oooSetPathAndEnsureDir(ff, path.trimTo(plen), columnName, FILE_SUFFIX_D);
                dstFixFd = openReadWriteOrFail(ff, path);
                dstFixSize = (srcOooHi - srcOooLo + 1) << ColumnType.pow2SizeOf(columnType);
                truncateToSizeOrFail(ff, path, dstFixFd, dstFixSize);
                dstFixAddr = mapReadWriteOrFail(ff, path, dstFixFd, dstFixSize);

                if (isColumnIndexed) {
                    BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName);
                    dstKFd = openReadWriteOrFail(ff, path);
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName);
                    dstVFd = openReadWriteOrFail(ff, path);
                }
                break;
        }

        publishCopyTask(
                columnCounter,
                partCounter,
                columnType,
                OO_BLOCK_OO,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                dstFixFd,
                dstFixAddr,
                0,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                0,
                dstVarSize,
                dstKFd,
                dstVFd,
                0,
                isIndexed
        );
    }

    private void oooSetPathAndEnsureDir(FilesFacade ff, Path path, CharSequence columnName, CharSequence suffix) {
        createDirsOrFail(ff, path.concat(columnName).put(suffix).$());
    }

    private void openColumn(OutOfOrderOpenColumnTask task, long cursor, Sequence subSeq) {
        int mode = task.getOpenColumnMode();
        switch (mode) {
            case 1:
                // done
                oooOpenMidPartitionForAppend(task);
                break;
            case 2:
                // done
                oooOpenLastPartitionForAppend(task);
                break;
            case 3:
                oooOpenMidPartitionForMerge(task);
                break;
            case 4:
                oooOpenLastPartitionForMerge(task);
                break;
            case 5:
                // done
                oooOpenNewPartitionForAppend(task);
                break;
        }
    }

    private void publishCopyTask(
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            int blockType,
            long timestampMergeIndexAddr,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcDataLo,
            long srcDataHi,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            long srcOooLo,
            long srcOooHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixOffset,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarOffset,
            long dstVarSize,
            long dstKFd,
            long dstVFd,
            long dstIndexOffset,
            boolean isIndexed
    ) {
        long cursor = outboundPubSeq.next();
        // todo: assuming there is space in the queue for now

        OutOfOrderCopyTask task = outboundQueue.get(cursor);
        task.of(
                columnCounter,
                partCounter,
                columnType,
                blockType,
                timestampMergeIndexAddr,
                srcDataFixFd,
                srcDataFixAddr,
                srcDataFixSize,
                srcDataVarFd,
                srcDataVarAddr,
                srcDataVarSize,
                srcDataLo,
                srcDataHi,
                srcOooFixAddr,
                srcOooFixSize,
                srcOooVarAddr,
                srcOooVarSize,
                srcOooLo,
                srcOooHi,
                dstFixFd,
                dstFixAddr,
                dstFixOffset,
                dstFixSize,
                dstVarFd,
                dstVarAddr,
                dstVarOffset,
                dstVarSize,
                dstKFd,
                dstVFd,
                dstIndexOffset,
                isIndexed
        );
        outboundPubSeq.done(cursor);

    }

    private void publishMultCopyTasks(
            int partCount,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long srcDataFixFd,
            long srcDataFixAddr,
            long srcDataFixSize,
            long srcDataVarFd,
            long srcDataVarAddr,
            long srcDataVarSize,
            long srcOooFixAddr,
            long srcOooFixSize,
            long srcOooVarAddr,
            long srcOooVarSize,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long dstFixFd,
            long dstFixAddr,
            long dstFixSize,
            long dstVarFd,
            long dstVarAddr,
            long dstVarSize,
            long dstFixAppendOffset1,
            long dstFixAppendOffset2,
            long dstVarAppendOffset1,
            long dstVarAppendOffset2,
            long dstKFd,
            long dstVFd,
            boolean isIndexed
    ) {
        partCounter.set(partCount);
        switch (prefixType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        prefixType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        0,
                        0,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        prefixLo,
                        prefixHi,
                        dstFixFd,
                        dstFixAddr,
                        0,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        prefixType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        prefixLo,
                        prefixHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        dstFixFd,
                        dstFixAddr,
                        0,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        0,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            default:
                break;
        }

        switch (mergeType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        mergeType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        0,
                        0,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        mergeType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        mergeDataLo,
                        mergeDataHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            case OO_BLOCK_MERGE:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        mergeType,
                        timestampMergeIndexAddr,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        mergeDataLo,
                        mergeDataHi,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        mergeOOOLo,
                        mergeOOOHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset1,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset1,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            default:
                break;
        }

        switch (suffixType) {
            case OO_BLOCK_OO:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        suffixType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        0,
                        0,
                        srcOooFixAddr,
                        srcOooFixSize,
                        srcOooVarAddr,
                        srcOooVarSize,
                        suffixLo,
                        suffixHi,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            case OO_BLOCK_DATA:
                publishCopyTask(
                        columnCounter,
                        partCounter,
                        columnType,
                        prefixType,
                        0,
                        srcDataFixFd,
                        srcDataFixAddr,
                        srcDataFixSize,
                        srcDataVarFd,
                        srcDataVarAddr,
                        srcDataVarSize,
                        suffixLo,
                        suffixHi,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        dstFixFd,
                        dstFixAddr,
                        dstFixAppendOffset2,
                        dstFixSize,
                        dstVarFd,
                        dstVarAddr,
                        dstVarAppendOffset2,
                        dstVarSize,
                        dstKFd,
                        dstVFd,
                        0,
                        isIndexed
                );
                break;
            default:
                break;
        }
    }
}
