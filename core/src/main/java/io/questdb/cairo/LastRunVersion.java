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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * The {@code .version} file at the root of the database directory names the release that last started on it.
 * Every start that may write to the directory records its own versions, so the next start can tell from the
 * file which release ran before it, and that it is itself older: a downgrade.
 * <p>
 * The file is 16 bytes: the QuestDB (OSS) version, then the QuestDB Enterprise version, {@link #NO_VERSION}
 * when the engine is not Enterprise. Each is a 64-bit number, stored most significant byte first, with one
 * byte per number of the version, so the bytes of the file read in the order of the version and two versions
 * compare as their numbers do:
 * <pre>
 * byte    0     1      2      3      4     5     6     7
 *         0   major  minor  patch   4th   5th   6th   half
 * </pre>
 * A version with a qualifier, such as {@code 4.1.1-SNAPSHOT}, is not yet that release: it counts half a step
 * above the release before it. Half of the unit of its last number is taken off, which sets the top bit of
 * the byte after that number: {@code 4.1.1-SNAPSHOT} is stored as {@code 4.1.0.128}, above {@code 4.1.0} and
 * each of its patches up to {@code 4.1.0.127}, and below {@code 4.1.1}; {@code 4.2.0-SNAPSHOT} is
 * {@code 4.1.255.128}. The last byte holds nothing but that bit, for a version of {@link #MAX_NUMBERS}
 * numbers.
 * <p>
 * A version this class cannot store reads as {@link #NO_VERSION}: one that is not numbers separated by dots
 * (a development build has none), has more than {@link #MAX_NUMBERS} numbers, or a number above 255.
 * <p>
 * The name starts with a dot, as {@link DataID#FILENAME} does, so it cannot clash with a table name.
 */
public final class LastRunVersion {
    public static final String FILE_NAME = ".version";
    public static final long FILE_SIZE = 2 * Long.BYTES;
    public static final int MAX_NUMBERS = 6;
    // No version of this kind: the Enterprise version of an OSS engine, a release that does not know its own
    // version, and a directory without the file.
    public static final long NO_VERSION = 0;
    private static final Log LOG = LogFactory.getLog(LastRunVersion.class);
    private static final int MAJOR_SHIFT = 48;
    private static final long OFFSET_ENT = Long.BYTES;
    private static final long OFFSET_OSS = 0;

    private LastRunVersion() {
    }

    /**
     * The 64-bit number of a version such as {@code 10.2.1}, {@code 4.1.1.1} or {@code 4.1.1-SNAPSHOT}, see the
     * class comment; {@link #NO_VERSION} for {@code null} and for a version the number cannot hold.
     */
    public static long encode(@Nullable CharSequence version) {
        if (version == null) {
            return NO_VERSION;
        }
        final int n = version.length();
        long value = 0;
        int shift = MAJOR_SHIFT + Byte.SIZE;
        int i = 0;
        while (true) {
            if (shift == Byte.SIZE) {
                // more than MAX_NUMBERS numbers
                return NO_VERSION;
            }
            shift -= Byte.SIZE;
            int number = -1;
            for (; i < n; i++) {
                final char c = version.charAt(i);
                if (c < '0' || c > '9') {
                    break;
                }
                number = Math.max(number, 0) * 10 + (c - '0');
                if (number > 0xFF) {
                    return NO_VERSION;
                }
            }
            if (number < 0) {
                // no digit where a number has to be
                return NO_VERSION;
            }
            value |= (long) number << shift;
            if (i == n) {
                return value;
            }
            final char c = version.charAt(i++);
            if (c == '-' && i < n) {
                // Not yet this release: half a step above the one before it. The first number has nothing
                // below it to borrow from when it is 0, and then there is no release before this one.
                value -= 1L << (shift - 1);
                return Math.max(value, NO_VERSION);
            }
            if (c != '.') {
                return NO_VERSION;
            }
        }
    }

    /**
     * The QuestDB Enterprise version of the release that last started on the directory; {@link #NO_VERSION}
     * when that was an OSS release, or the directory has no version file.
     */
    public static long readEntVersion(CairoConfiguration configuration) {
        return read(configuration, OFFSET_ENT);
    }

    /**
     * The QuestDB (OSS) version of the release that last started on the directory; {@link #NO_VERSION} when
     * the directory has no version file: a release older than the file ran last, or none did.
     */
    public static long readOssVersion(CairoConfiguration configuration) {
        return read(configuration, OFFSET_OSS);
    }

    /**
     * Records the versions of the running release, unless the file already holds exactly them: a restart of
     * the same release writes nothing. A build that knows neither of its versions, such as a development
     * build or a unit test, writes nothing either and leaves the file of the last release in place. Fails as
     * any other write of the engine start does.
     *
     * @param ossVersion the QuestDB version, as the build names it
     * @param entVersion the QuestDB Enterprise version, as the build names it; {@code null} for an OSS engine
     */
    public static void save(CairoConfiguration configuration, @Nullable CharSequence ossVersion, @Nullable CharSequence entVersion) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long oss = encode(ossVersion);
        final long ent = encode(entVersion);
        if (oss == NO_VERSION && ent == NO_VERSION) {
            return;
        }
        final long buf = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(FILE_NAME);
            // A file of another size is not a version file, whatever its first bytes are.
            final boolean hasLast = load(ff, path, buf);
            final long lastOss = hasLast ? getVersion(buf, OFFSET_OSS) : NO_VERSION;
            final long lastEnt = hasLast ? getVersion(buf, OFFSET_ENT) : NO_VERSION;
            if (hasLast && lastOss == oss && lastEnt == ent) {
                return;
            }

            Unsafe.putLong(buf + OFFSET_OSS, Long.reverseBytes(oss));
            Unsafe.putLong(buf + OFFSET_ENT, Long.reverseBytes(ent));
            final long fd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            try {
                // The file is far smaller than a page: one write, no partial-write loop.
                if (ff.write(fd, buf, FILE_SIZE, 0) != FILE_SIZE) {
                    throw CairoException.critical(ff.errno())
                            .put("could not write version file [fd=").put(fd).put(", path=").put(path).put(']');
                }
                if (ff.length(fd) != FILE_SIZE && !ff.truncate(fd, FILE_SIZE)) {
                    throw CairoException.critical(ff.errno())
                            .put("could not truncate version file [fd=").put(fd).put(", path=").put(path).put(']');
                }
                ff.fsync(fd);
            } finally {
                ff.close(fd);
            }

            final StringSink ossSink = toSink(oss, new StringSink());
            final StringSink entSink = toSink(ent, new StringSink());
            if (hasLast) {
                final StringSink lastOssSink = toSink(lastOss, new StringSink());
                final StringSink lastEntSink = toSink(lastEnt, new StringSink());
                LOG.info().$("version file updated [lastOss=").$(lastOssSink)
                        .$(", lastEnt=").$(lastEntSink)
                        .$(", oss=").$(ossSink)
                        .$(", ent=").$(entSink)
                        .I$();
            } else {
                LOG.info().$("version file created [oss=").$(ossSink).$(", ent=").$(entSink).I$();
            }
        } finally {
            Unsafe.free(buf, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Prints a version number as its numbers separated by dots, at least three and no trailing zeros beyond
     * them: {@code 10.2.1}, {@code 4.1.1.1}, and {@code 4.1.0.128} for {@code 4.1.1-SNAPSHOT}.
     */
    public static <T extends CharSink<?>> T toSink(long version, T sink) {
        int lastShift = 0;
        while (lastShift < MAJOR_SHIFT - 2 * Byte.SIZE && ((version >>> lastShift) & 0xFF) == 0) {
            lastShift += Byte.SIZE;
        }
        for (int shift = MAJOR_SHIFT; shift >= lastShift; shift -= Byte.SIZE) {
            if (shift < MAJOR_SHIFT) {
                sink.putAscii('.');
            }
            sink.put((version >>> shift) & 0xFF);
        }
        return sink;
    }

    private static long getVersion(long buf, long offset) {
        return Long.reverseBytes(Unsafe.getLong(buf + offset));
    }

    // Whether the directory holds a version file, read into buf, which holds FILE_SIZE bytes.
    private static boolean load(FilesFacade ff, Path path, long buf) {
        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            return false;
        }
        try {
            return ff.length(fd) == FILE_SIZE && ff.read(fd, buf, FILE_SIZE, 0) == FILE_SIZE;
        } finally {
            ff.close(fd);
        }
    }

    private static long read(CairoConfiguration configuration, long offset) {
        final long buf = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(FILE_NAME);
            return load(configuration.getFilesFacade(), path, buf) ? getVersion(buf, offset) : NO_VERSION;
        } finally {
            Unsafe.free(buf, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
