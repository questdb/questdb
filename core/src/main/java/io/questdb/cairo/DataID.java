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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Uuid;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

/**
 * DataID handles the mapping of the _data_id.d file located at the root of the database.
 * Its role is to store a unique id (consisting of a randomly generated 256 bits number)
 * that is replicated among the database cluster.
 * <p>
 * One shouldn't modify the data id in an unblank database as it may cause data loss.
 * </p>
 */
public final class DataID implements Sinkable {
    public static final CharSequence FILENAME = "_data_id.d";
    public static long FILE_SIZE = 4 + 36 * 2;
    private final CairoConfiguration configuration;
    private final Uuid id;

    public DataID(CairoConfiguration configuration, Uuid id) {
        this.id = id;
        this.configuration = configuration;
    }

    /**
     * Read the `_data_id.d` file (or creates it if it doesn't exist yet with zero value) and returns its current value.
     *
     * @param configuration the configuration that is used to provide the FileFacade and DbRoot.
     * @return a new data id instance.
     */
    public static DataID open(CairoConfiguration configuration) throws CairoException {
        try (MemoryCMARWImpl mem = openDataIDFile(configuration)) {
            CharSequence sq = mem.getStrA(0);
            if (sq.length() == 0) {
                // The _data_id.d file has not been initialized yet.
                return new DataID(configuration, new Uuid(Numbers.LONG_NULL, Numbers.LONG_NULL));
            }

            Uuid id = new Uuid();
            try {
                id.of(sq);
                return new DataID(configuration, id);
            } catch (NumericException e) {
                throw CairoException.critical(CairoException.METADATA_VALIDATION)
                        .put("Invalid data id [id=")
                        .put(sq)
                        .put("]");
            }
        }
    }

    public long getHi() {
        return id.getHi();
    }

    public long getLo() {
        return id.getLo();
    }

    /**
     * Returns whether the data id has been initialized or not.
     * @return true if the data id is initialized.
     */
    public boolean isInitialized() {
        return !Uuid.isNull(id.getLo(), id.getHi());
    }

    /**
     * Set the data id to a new value and writes it to `_data_id.d`.
     * This function should be used with care as it may lead with data losses from restore/replication.
     *
     * @param id the new id to use.
     */
    public void set(Uuid id) {
        if (this.id.equals(id)) {
            return;
        }

        this.id.of(id.getLo(), id.getHi());
        try (MemoryCMARWImpl mem = openDataIDFile(configuration)) {
            StringSink ds = new StringSink();
            id.toSink(ds);
            mem.putStr(ds);
            mem.sync(false);
        }
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        id.toSink(sink);
    }

    private static MemoryCMARWImpl openDataIDFile(CairoConfiguration configuration) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot());
            path.concat(FILENAME);

            final FilesFacade ff = configuration.getFilesFacade();
            return new MemoryCMARWImpl(ff, path.$(), FILE_SIZE, -1, MemoryTag.MMAP_DEFAULT, configuration.getWriterFileOpenOpts());
        }
    }
}
