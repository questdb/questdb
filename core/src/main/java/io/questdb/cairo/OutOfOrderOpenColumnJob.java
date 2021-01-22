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
import static io.questdb.cairo.TableWriter.OO_BLOCK_DATA;
import static io.questdb.cairo.TableWriter.OO_BLOCK_OO;

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

    private void createDirsOrFail(FilesFacade ff, Path path) {
        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
        }
    }

    private void appendTxnToPath(Path path, long txn) {
        path.put("-n-").put(txn);
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


    private void oooSetPathAndEnsureDir(FilesFacade ff, Path path, CharSequence columnName, CharSequence suffix) {
        createDirsOrFail(ff, path.concat(columnName).put(suffix).$());
    }

    private void openColumn(OutOfOrderOpenColumnTask task, long cursor, Sequence subSeq) {
        int mode = task.getOpenColumnMode();
        switch (mode) {
            case 1:
            case 2:
            case 3:
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
