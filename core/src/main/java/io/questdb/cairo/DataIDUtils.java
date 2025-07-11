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
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;

// DataIDUtils handles the mapping of the _data_id.d file located at the root of the database.
// Its role is to store a unique id (consisting of a randomly generated 256 bits number)
// that is replicated among the database cluster.
//
// One shouldn't modify the data id in an unblank database as it may cause data loss.
public final class DataIDUtils {
    public static final CharSequence FILENAME = "_data_id.d";
    private static final Long256Impl currentId = new Long256Impl();
    public static long FILE_SIZE = 32;

    /**
     * Read the `_data_id.d` file (or creates it if it doesn't exist yet) and returns its current value.
     *
     * @param configuration the configuration that is used to provide the FileFacade and DbRoot.
     * @return the current data id.
     */
    public static Long256 read(CairoConfiguration configuration) {
        if (!Long256Impl.isNull(currentId)) {
            return currentId;
        }

        synchronized (currentId) {
            // Double-checked locking
            if (!Long256Impl.isNull(currentId)) {
                return currentId;
            }

            try (MemoryCMARWImpl mem = openDataIDFile(configuration)) {
                currentId.fromAddress(mem.getAddress());

                // When the file is empty, it is truncated to 32 bytes with zero bytes.
                // In such a case, we should initialize with a new random value.
                if (currentId.equals(Long256Impl.ZERO_LONG256)) {
                    Rnd rnd = new Rnd(configuration.getMicrosecondClock().getTicks(), configuration.getMillisecondClock().getTicks());
                    currentId.fromRnd(rnd);
                    currentId.toAddress(mem.getAddress());
                }
            }
        }

        return currentId;
    }

    /**
     * Set the data id to a new value and writes it to `_data_id.d`.
     * This function should be used with care as it may lead with data losses from restore/replication.
     *
     * @param configuration the configuration that is used to provide the FileFacade and DbRoot.
     * @param dataID        the new data id to set.
     */
    public static void set(CairoConfiguration configuration, Long256Impl dataID) {
        if (currentId.equals(dataID)) {
            return;
        }

        synchronized (currentId) {
            currentId.copyFrom(dataID);
            try (MemoryCMARWImpl mem = openDataIDFile(configuration)) {
                currentId.toAddress(mem.getAddress());
            }
        }
    }

    private static MemoryCMARWImpl openDataIDFile(CairoConfiguration configuration) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot());
            path.concat(FILENAME);

            final FilesFacade ff = configuration.getFilesFacade();
            return new MemoryCMARWImpl(ff, path.$(), FILE_SIZE, -1, MemoryTag.MMAP_DEFAULT, configuration.getWriterFileOpenOpts());
        }
    }

    static {
        DataIDUtils.currentId.copyFrom(Long256Impl.NULL_LONG256);
    }
}
