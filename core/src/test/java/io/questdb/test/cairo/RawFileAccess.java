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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;

/**
 * Positional (pread/pwrite) peek/poke primitives for the test-only on-disk corruption helpers.
 * <p>
 * Deliberately NOT mmap-based: a writable mapping of one of these files can truncate it on close, which
 * would destroy the very geometry a corruption test is trying to edit in place. Extracted so
 * {@link CvCorruptionUtils} and {@link TxnCorruptionUtils} share one implementation rather than each
 * carrying its own copy.
 */
public final class RawFileAccess {

    private RawFileAccess() {
    }

    public static byte peekByte(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Byte.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Byte.BYTES, ff.read(fd, buf, Byte.BYTES, offset));
            return Unsafe.getByte(buf);
        } finally {
            Unsafe.free(buf, Byte.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    public static int peekInt(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Integer.BYTES, ff.read(fd, buf, Integer.BYTES, offset));
            return Unsafe.getInt(buf);
        } finally {
            Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    public static long peekLong(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    public static void pokeByte(FilesFacade ff, LPSZ path, long offset, byte value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Byte.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(buf, value);
            Assert.assertEquals(Byte.BYTES, ff.write(fd, buf, Byte.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Byte.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    public static void pokeLong(FilesFacade ff, LPSZ path, long offset, long value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, value);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }
}
