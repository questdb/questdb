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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Vect;

public class O3Utils {

    private static final Log LOG = LogFactory.getLog(O3Utils.class);

    public static void copyFixedSizeCol(
            FilesFacade ff,
            long srcAddr,
            long srcLo,
            long dstAddr,
            long dstFixFileOffset,
            long dstFd,
            boolean mixedIOFlag,
            long len,
            int shl
    ) {
        final long fromAddress = srcAddr + (srcLo << shl);
        o3Copy(ff, dstAddr, dstFixFileOffset, dstFd, fromAddress, len, mixedIOFlag);
    }

    public static void o3Copy(
            FilesFacade ff,
            long dstAddr,
            long dstFileOffset,
            long dstFd,
            long fromAddress,
            long len,
            boolean mixedIOFlag
    ) {
        if (mixedIOFlag) {
            if (ff.write(Math.abs(dstFd), fromAddress, len, dstFileOffset) != len) {
                throw CairoException.critical(ff.errno()).put("cannot copy fixed column prefix [fd=")
                        .put(dstFd).put(", len=").put(len).put(", offset=").put(fromAddress).put(']');
            }
        } else {
            Vect.memcpy(dstAddr, fromAddress, len);
        }
    }

    static void close(FilesFacade ff, long fd) {
        if (fd > 0) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            ff.close(fd);
        }
    }

    // For cleanup that runs on behalf of an O3 commit that already failed. A release that throws, a double close
    // or a failed munmap, is logged instead of propagated: the rest of the cleanup and the count-down that follows
    // still run, and the error that failed the commit stays the one that propagates.
    static void closeQuietly(FilesFacade ff, long fd) {
        try {
            close(ff, fd);
        } catch (Throwable th) {
            LOG.error().$("could not close [fd=").$(fd).$(", e=").$(th).I$();
        }
    }

    static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        Vect.copyFromTimestampIndex(src, srcLo, srcHi, dstAddr);
    }

    static void unmap(FilesFacade ff, long addr, long size) {
        if (addr != 0 && size > 0) {
            ff.munmap(addr, size, MemoryTag.MMAP_O3);
        }
    }

    static void unmapAndClose(FilesFacade ff, long dstFixFd, long dstFixAddr, long dstFixSize) {
        try {
            unmap(ff, dstFixAddr, dstFixSize);
        } finally {
            close(ff, dstFixFd);
        }
    }

    // The failed-commit counterpart of unmapAndClose(), see closeQuietly().
    static void unmapAndCloseQuietly(FilesFacade ff, long fd, long addr, long size) {
        try {
            unmap(ff, addr, size);
        } catch (Throwable th) {
            LOG.error().$("could not unmap [addr=").$(addr).$(", size=").$(size).$(", e=").$(th).I$();
        }
        closeQuietly(ff, fd);
    }
}
