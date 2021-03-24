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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class OutOfOrderUtils {

    private static final Log LOG = LogFactory.getLog(OutOfOrderUtils.class);
    private static long[] temp8ByteBuf;

    public static void freeBuf() {
        if (temp8ByteBuf != null) {
            for (int i = 0, n = temp8ByteBuf.length; i < n; i++) {
                Unsafe.free(temp8ByteBuf[i], Long.BYTES);
            }
            temp8ByteBuf = null;
        }
    }

    public static void initBuf() {
        initBuf(1);
    }

    public static void initBuf(int workerCount) {
        temp8ByteBuf = new long[workerCount];
        for (int i = 0; i < workerCount; i++) {
            temp8ByteBuf[i] = Unsafe.malloc(Long.BYTES);
        }
    }

    public static void swapPartition(
            FilesFacade ff,
            CharSequence pathToTable,
            long partitionTimestamp,
            long srcDataTxn,
            long txn,
            int partitionBy
    ) {
        if (Os.type == Os.WINDOWS) {
            swapPartitionFiles(ff, pathToTable, partitionTimestamp, srcDataTxn, txn, partitionBy);
        } else {
            swapPartitionDirectories(ff, pathToTable, partitionTimestamp, txn, partitionBy);
        }
    }

    static long get8ByteBuf(int worker) {
        return temp8ByteBuf[worker];
    }

    static long getVarColumnLength(
            long srcLo,
            long srcHi,
            long srcFixAddr,
            long srcFixSize,
            long srcVarSize
    ) {
        final long lo = findVarOffset(srcFixAddr, srcLo, srcHi, srcVarSize);
        final long hi;
        if (srcHi + 1 == srcFixSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = findVarOffset(srcFixAddr, srcHi + 1, srcFixSize / Long.BYTES, srcVarSize);
        }
        return hi - lo;
    }

    static long findVarOffset(long srcFixAddr, long srcLo, long srcHi, long srcVarSize) {
        long lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
        if (lo > -1) {
            return lo;
        }

        while (++srcLo < srcHi) {
            lo = Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
            if (lo > -1) {
                return lo;
            }
        }

        return srcVarSize;
    }

    static void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        final long lo = srcLo * Long.BYTES;
        final long hi = (srcHi + 1) * Long.BYTES;
        final long slo = src + lo;
        final long len = hi - lo;
        for (long o = 0; o < len; o += Long.BYTES) {
            Unsafe.getUnsafe().putLong(dstAddr + o, Unsafe.getUnsafe().getLong(slo + o) - shift);
        }
    }

    static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        final int shl = 4;
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long start = src + lo;
        final long len = hi - lo;
        for (long l = 0; l < len; l += 16) {
            Unsafe.getUnsafe().putLong(dstAddr + l / 2, Unsafe.getUnsafe().getLong(start + l));
        }
    }

    static void unmapAndClose(FilesFacade ff, long dstFixFd, long dstFixAddr, long dstFixSize) {
        unmap(ff, dstFixAddr, dstFixSize);
        close(ff, dstFixFd);
    }

    static void unmap(FilesFacade ff, long addr, long size) {
        if (addr != 0 && size != 0) {
            ff.munmap(addr, size);
        }
    }

    static void close(FilesFacade ff, long fd) {
        if (fd > 0) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            ff.close(fd);
        }
    }

    static long mapRO(FilesFacade ff, long fd, long size) {
        final long address = ff.mmap(fd, size, 0, Files.MAP_RO);
        if (address == FilesFacade.MAP_FAILED) {
            throw CairoException.instance(ff.errno())
                    .put("Could not mmap timestamp column ")
                    .put(" [size=").put(size)
                    .put(", fd=").put(fd)
                    .put(", memUsed=").put(Unsafe.getMemUsed())
                    .put(", fileLen=").put(ff.length(fd))
                    .put(']');
        }
        return address;
    }

    static long mapRW(FilesFacade ff, long fd, long size) {
        allocateDiskSpace(ff, fd, size);
        long addr = ff.mmap(fd, size, 0, Files.MAP_RW);
        if (addr > -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap column [fd=").put(fd).put(", size=").put(size).put(']');
    }

    static long openRW(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd > -1) {
            LOG.debug().$("open [file=").$(path).$(", fd=").$(fd).$(']').$();
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open read-write [file=").put(path).put(", fd=").put(fd).put(']');
    }

    static void allocateDiskSpace(FilesFacade ff, long fd, long size) {
        if (!ff.allocate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("No space left [size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    private static void swapPartitionDirectories(
            FilesFacade ff,
            CharSequence pathToTable,
            long partitionTimestamp,
            long txn,
            int partitionBy
    ) {
        final Path path = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp);
        final int plen = path.length();
        path.$();
        final Path other = Path.getThreadLocal2(path);
        TableUtils.oldPartitionName(other, txn);
        if (ff.rename(path, other.$())) {
            TableUtils.txnPartition(other.trimTo(plen), txn);
            if (ff.rename(other.$(), path)) {
                LOG.info().$("renamed").$();
                return;
            }
            throw CairoException.instance(ff.errno())
                    .put("could not rename [from=").put(other)
                    .put(", to=").put(path).put(']');
        } else {
            throw CairoException.instance(ff.errno())
                    .put("could not rename [from=").put(path)
                    .put(", to=").put(other).put(']');
        }
    }

    private static void swapPartitionFiles(
            FilesFacade ff,
            CharSequence pathToTable,
            long partitionTimestamp,
            long srcDataTxn,
            long txn,
            int partitionBy
    ) {
        // source path - original data partition
        // this partition may already be on non-initial txn
        // todo: test that this code handles data txn > -1
        final Path srcPath = Path.getThreadLocal(pathToTable);
        TableUtils.setPathForPartition(srcPath, partitionBy, partitionTimestamp);
        int coreLen = srcPath.length();

        TableUtils.txnPartitionConditionally(srcPath, srcDataTxn);
        int srcLen = srcPath.length();

        final Path dstPath = Path.getThreadLocal2(srcPath).concat("backup");
        TableUtils.txnPartitionConditionally(dstPath, txn);
        int dstLen = dstPath.length();

        srcPath.put(Files.SEPARATOR);
        srcPath.$();

        dstPath.put(Files.SEPARATOR);
        dstPath.$();

        if (ff.mkdir(dstPath, 502) != 0) {
            throw CairoException.instance(ff.errno()).put("could not create directory [path").put(dstPath).put(']');
        }

        // move all files to "backup-txn"
        moveFiles(ff, srcPath, srcLen, dstPath, dstLen);

        // not move files from "XXX-n-txn" to original partition
        srcPath.trimTo(coreLen);
        TableUtils.txnPartition(srcPath, txn);
        srcLen = srcPath.length();

        dstPath.trimTo(coreLen);
        TableUtils.txnPartitionConditionally(dstPath, srcDataTxn);
        dstLen = dstPath.length();

        moveFiles(ff, srcPath.put(Files.SEPARATOR).$(), srcLen, dstPath.$(), dstLen);
    }

    private static void moveFiles(FilesFacade ff, Path srcPath, int srcLen, Path dstPath, int dstLen) {
        long p = ff.findFirst(srcPath);
        if (p > 0) {
            try {
                do {
                    int type  = ff.findType(p);
                    if (type == Files.DT_REG) {
                        long lpszName = ff.findName(p);
                        srcPath.trimTo(srcLen).concat(lpszName).$();
                        dstPath.trimTo(dstLen).concat(lpszName).$();
                        if (ff.rename(srcPath, dstPath)) {
                            LOG.debug().$("renamed [from=").$(srcPath).$(", to=").$(dstPath, dstLen, dstPath.length()).$(']').$();
                        } else {
                            throw CairoException.instance(ff.errno()).put("could not rename file [from=").put(srcPath).put(", to=").put(dstPath).put(']');
                        }
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
            }
        }
    }
}
