/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
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
     */
    public static final CharSequence FILENAME = ".data_id";
    public static long FILE_SIZE = Long.BYTES * 2;  // Storing UUID as binary
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
        long lo, hi;

        try (Path path = createDataIdPath(configuration)) {
            final FilesFacade ff = configuration.getFilesFacade();
            try (var mem = new MemoryCMRImpl(ff, path.$(), -1, MemoryTag.MMAP_DEFAULT)) {
                if (mem.size() < FILE_SIZE) {
                    // The file has an unexpected size, we represent this as a null UUID - our "uninitialized" state.
                    lo = hi = Numbers.LONG_NULL;
                } else {
                    lo = mem.getLong(0);
                    hi = mem.getLong(Long.BYTES);
                }
            } catch (CairoException e) {
                // The file may not exist, we represent this as a null UUID - our "uninitialized" state.
                lo = hi = Numbers.LONG_NULL;
            }
        }
        return new DataID(configuration, new Uuid(lo, hi));
    }

    public long getHi() {
        return id.getHi();
    }

    public long getLo() {
        return id.getLo();
    }

    /**
     * Returns whether the data id has been initialized or not.
     *
     * @return true if the data id is initialized.
     */
    public boolean isInitialized() {
        return !Uuid.isNull(id.getLo(), id.getHi());
    }

    /**
     * Set the data id to a new value and writes it to `.data_id`.
     * This function should be used with care as it may lead to data losses from restore/replication.
     *
     * @param lo The low bits of the UUID value
     * @param hi The high bits of the UUID value
     */
    public void set(long lo, long hi) {
        if ((lo == id.getLo()) && (hi == id.getHi())) {
            return;
        }
        try (Path path = createDataIdPath(configuration)) {
            final FilesFacade ff = configuration.getFilesFacade();
            try (var mem = new MemoryCMARWImpl(
                    ff,
                    path.$(),
                    FILE_SIZE,
                    -1,
                    MemoryTag.MMAP_DEFAULT, configuration.getWriterFileOpenOpts()
            )) {
                mem.putLong(lo);
                mem.putLong(hi);
                mem.sync(false);
                this.id.of(lo, hi);
            }
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        id.toSink(sink);
    }

    private static Path createDataIdPath(CairoConfiguration configuration) {
        Path path = new Path();
        try {
            path.of(configuration.getDbRoot());
            path.concat(FILENAME);
            return path;
        } catch (Throwable t) {
            path.close();
            throw t;
        }
    }

}
