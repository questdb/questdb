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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.junit.Assert;

import static io.questdb.cairo.ColumnVersionReader.OFFSET_OFFSET_A_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_OFFSET_B_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_SIZE_A_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_SIZE_B_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_VERSION_64;

/**
 * Test-only corruption helpers for a table's {@code _cv} (column version) file. Used as the negative
 * control in {@link ChecksumTrailerCvEquivalenceTest}: flip a byte strictly inside the currently-LIVE
 * A/B area's data, never the 16-byte trailer that follows it and never the other (inactive) area --
 * mirrors the header layout documented on {@link ColumnVersionReader} (OFFSET_VERSION_64's parity picks
 * A vs B; OFFSET_OFFSET_{A,B}_64 / OFFSET_SIZE_{A,B}_64 give that area's on-disk [offset, offset+size)).
 * A flip in the trailer would only prove ABSENT handling; a flip in the inactive area proves nothing
 * (readers never look at it unless the live area's checksum has already failed).
 */
public final class CvCorruptionUtils {

    private CvCorruptionUtils() {
    }

    /**
     * Flips one byte inside the live area's data bytes of {@code tableName}'s {@code _cv} file, on disk.
     */
    public static void flipByteInLiveArea(CairoEngine engine, String tableName) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken token = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            LPSZ cvPath = path.of(configuration.getDbRoot()).concat(token).concat("_cv").$();

            long version = peekLong(ff, cvPath, OFFSET_VERSION_64);
            boolean areaA = (version & 1L) == 0L;
            long liveOffset = peekLong(ff, cvPath, areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
            long liveSize = peekLong(ff, cvPath, areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64);
            Assert.assertTrue(
                    "live _cv area is empty, cannot corrupt it in-area [offset=" + liveOffset + ", size=" + liveSize + ']',
                    liveSize > 0
            );

            // Flip the first byte of the live area's DATA. Strictly inside [liveOffset, liveOffset + liveSize):
            // never [liveOffset + liveSize, liveOffset + liveSize + 16) (the trailer) and never the other area.
            long flipAt = liveOffset;
            byte orig = peekByte(ff, cvPath, flipAt);
            pokeByte(ff, cvPath, flipAt, (byte) (orig ^ 0x5A));
        }
    }

    /**
     * Evicts every pooled reader/writer for {@code tableName} (a stale pooled {@code TableReader} caches
     * its {@code ColumnVersionReader}'s last-seen version and short-circuits re-verification when the
     * on-disk version word is unchanged -- exactly the case here, since corrupting a data byte does not
     * bump the version) and then opens a genuinely fresh {@code TableReader}, so the checksum verify in
     * {@code ColumnVersionReader.readSafe()} actually runs against the bytes now on disk.
     */
    public static void forceReload(CairoEngine engine, String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        engine.releaseInactive();
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertNotNull(reader);
        }
    }

    private static byte peekByte(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(1, ff.read(fd, buf, 1, offset));
            return Unsafe.getByte(buf);
        } finally {
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    // Positional single-byte write (no mmap, so it cannot truncate the file on close). Mirrors the
    // pokeLong/pokeBytes helpers in ColumnVersionWriterTest.
    private static void pokeByte(FilesFacade ff, LPSZ path, long offset, byte value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putByte(buf, value);
            Assert.assertEquals(1, ff.write(fd, buf, 1, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private static long peekLong(FilesFacade ff, LPSZ path, long offset) {
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
}
