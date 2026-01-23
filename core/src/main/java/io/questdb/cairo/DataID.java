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

import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

/**
 * DataID handles the mapping of the .data_id file located at the root of the database.
 * Its role is to store a unique id (consisting of a randomly generated 128-bit UUID)
 * to uniquely "tag" a `db` directory so the contained tables can be uniquely identified
 * across backups and enterprise replication.
 * <p>
 * One shouldn't modify the data id in an unblank database as it may cause data loss.
 * </p>
 */
public final class DataID implements Sinkable {

    /**
     * The file that contains the serialized DataID value has a name that starts with a `.`
     * as this avoids a name clash with a potentially valid table name.
     * The data within the file is stored as 16 bytes binary and follows the RFC 4122 big endian binary representation.
     */
    public static final String FILENAME = ".data_id";
    public static final long FILE_SIZE = Long.BYTES * 2;  // Storing UUID as binary
    private final CairoConfiguration configuration;
    private final Uuid id;

    public DataID(CairoConfiguration configuration, Uuid id) {
        this.id = id;
        this.configuration = configuration;
    }

    /**
     * Read the `.data_id` file (or creates it if it doesn't exist yet with zero value) and returns its current value.
     *
     * @param configuration the configuration that is used to provide the FileFacade and DbRoot.
     * @return a new data id instance.
     */
    public static DataID open(CairoConfiguration configuration) throws CairoException {
        final Uuid id = readUuid(configuration.getFilesFacade(), configuration.getDbRoot());
        return new DataID(configuration, id);
    }

    /**
     * Change the data id to a new value, overwriting any existing value.
     * This should only be used after point-in-time recovery to give the
     * recovered database a fresh identity.
     * <p>
     * Fails if the data id is not already initialized.
     * </p>
     *
     * @param lo The low bits of the UUID value
     * @param hi The high bits of the UUID value
     * @throws CairoException if the data id is not initialized
     */
    public synchronized void change(long lo, long hi) {
        if (!isInitialized()) {
            throw CairoException.critical(0).put("cannot change DataID: not initialized");
        }
        writeUuid(configuration.getFilesFacade(), configuration.getDbRoot(), lo, hi);
        this.id.of(lo, hi);
    }

    public Uuid get() {
        return id;
    }

    public long getHi() {
        return id.getHi();
    }

    public long getLo() {
        return id.getLo();
    }

    /**
     * Initialize the data id to a new value and writes it to `.data_id`.
     * This function should be used with care as it may lead to data losses from restore/replication.
     *
     * @param lo The low bits of the UUID value
     * @param hi The high bits of the UUID value
     * @return true if the value was initialized, or false if it could not be initialized
     * because it was already set.
     */
    public boolean initialize(long lo, long hi) {
        if (isInitialized()) {
            return false;
        }
        writeUuid(configuration.getFilesFacade(), configuration.getDbRoot(), lo, hi);
        this.id.of(lo, hi);
        return true;
    }

    /**
     * Returns whether the data id has been initialized or not.
     *
     * @return true if the data id is initialized.
     */
    public boolean isInitialized() {
        return !Uuid.isNull(id.getLo(), id.getHi());
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        id.toSink(sink);
    }

    private static Uuid readUuid(FilesFacade ff, String dbRoot) {
        final Uuid result = new Uuid(Numbers.LONG_NULL, Numbers.LONG_NULL);
        long fd = -1;
        long buf = 0;
        try (
                Path path = new Path()
        ) {
            path.of(dbRoot);
            path.concat(FILENAME);

            fd = ff.openRO(path.$());
            if (fd == -1) {
                // File not found: Return NULL Uuid.
                return result;
            }

            buf = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);

            // One-shot read since the file is tiny
            final long readBytes = ff.read(fd, buf, FILE_SIZE, 0);
            if (readBytes != FILE_SIZE) {
                // File too small or read error: Return NULL Uuid.
                return result;
            }

            // Read back the big-endian bytes and reverse them
            final long hi = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf));
            final long lo = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + Long.BYTES));

            result.of(lo, hi);
            return result;
        } finally {
            if (fd != -1) {
                ff.close(fd);
            }

            if (buf != 0) {
                Unsafe.free(buf, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void writeUuid(FilesFacade ff, String dbRoot, long lo, long hi) {
        long fd = -1;
        long buf = 0;
        try (
                Path path = new Path()
        ) {
            path.of(dbRoot);
            path.concat(FILENAME);

            fd = TableUtils.openFileRWOrFail(ff, path.$(), 0);

            // Truncate to nothing. This handles the case of a previous partial write.
            if (!ff.truncate(fd, 0)) {
                throw CairoException.critical(ff.errno())
                        .put("cannot truncate DataID before writing [fd=").put(fd).put(", path=").put(path).put(']');
            }

            buf = Unsafe.malloc(FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().putLong(buf, Long.reverseBytes(hi));
            Unsafe.getUnsafe().putLong(buf + Long.BYTES, Long.reverseBytes(lo));

            // One-shot, no partial-write loop since the file is tiny and significantly smaller than any OS file
            // buffers, which would be at least one page.
            final long written = ff.write(fd, buf, FILE_SIZE, 0);
            if (written != FILE_SIZE) {
                throw CairoException.critical(ff.errno())
                        .put("cannot write DataID [fd=").put(fd).put(", path=").put(path).put(']');
            }

            ff.fsync(fd);
        } finally {
            if (fd != -1) {
                ff.close(fd);
            }

            if (buf != 0) {
                Unsafe.free(buf, FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
