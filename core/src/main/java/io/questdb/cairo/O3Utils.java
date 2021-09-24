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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class O3Utils {

    private static final Log LOG = LogFactory.getLog(O3Utils.class);
    private static long[] temp8ByteBuf;

    public static void freeBuf() {
        if (temp8ByteBuf != null) {
            for (int i = 0, n = temp8ByteBuf.length; i < n; i++) {
                Unsafe.free(temp8ByteBuf[i], Long.BYTES, MemoryTag.NATIVE_O3);
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
            temp8ByteBuf[i] = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_O3);
        }
    }

    static long get8ByteBuf(int worker) {
        return temp8ByteBuf[worker];
    }

    static long getVarColumnLength(long srcLo, long srcHi, long srcFixAddr) {
        return findVarOffset(srcFixAddr, srcHi + 1) - findVarOffset(srcFixAddr, srcLo);
    }

    static long findVarOffset(long srcFixAddr, long srcLo) {
        return Unsafe.getUnsafe().getLong(srcFixAddr + srcLo * Long.BYTES);
    }

    static void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        Vect.shiftCopyFixedSizeColumnData(shift, src, srcLo, srcHi, dstAddr);
    }

    static void copyFromTimestampIndex(
            long src,
            long srcLo,
            long srcHi,
            long dstAddr
    ) {
        Vect.copyFromTimestampIndex(src, srcLo, srcHi, dstAddr);
    }

    static void unmapAndClose(FilesFacade ff, long dstFixFd, long dstFixAddr, long dstFixSize) {
        unmap(ff, dstFixAddr, dstFixSize);
        close(ff, dstFixFd);
    }

    static void unmap(FilesFacade ff, long addr, long size) {
        if (addr != 0 && size > 0) {
            ff.munmap(addr, size, MemoryTag.MMAP_O3);
        }
    }

    static void close(FilesFacade ff, long fd) {
        if (fd > 0) {
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
            ff.close(fd);
        }
    }
}
