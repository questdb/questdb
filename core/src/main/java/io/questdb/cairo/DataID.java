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

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;

// DataID handles the mapping of the _data_id file located at the root of the database.
// Its role is to store a unique id (consisting of a randomly generated 256 bits number)
// that is replicated among the database cluster.
//
// One shouldn't modify the data id in an unblank database as it may cause data loss.
public class DataID implements QuietCloseable {
    public static long FILE_SIZE = 32;
    private final CairoConfiguration configuration;
    private final Long256Impl dataID = new Long256Impl();
    private final CharSequence filename;
    private long fd = -1;
    private long addr = 0;

    public DataID(CairoConfiguration configuration, CharSequence filename) {
        this.configuration = configuration;
        this.filename = filename;
    }

    public void open() {
        close();
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        final int rootLen = path.size();
        try {
            path.concat(filename);
            final FilesFacade ff = configuration.getFilesFacade();
            fd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            addr = TableUtils.mapRW(ff, fd, DataID.FILE_SIZE, MemoryTag.MMAP_DEFAULT);
            dataID.fromAddress(addr);
            setup();
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void setup() {
        // When the file is empty, it is truncated to 32 bytes with zero bytes.
        // In such a case, we should initialize with a new random value.
        if (dataID.equals(Long256Impl.ZERO_LONG256)) {
            Rnd rnd = new Rnd(configuration.getMicrosecondClock().getTicks(), configuration.getMillisecondClock().getTicks());
            dataID.fromRnd(rnd);
            dataID.toAddr(addr);
        }
    }

    public void set(Long256 dataID) {
        this.dataID.copyFrom(dataID);
        this.dataID.toAddr(addr);
    }

    public Long256Impl get() {
        return dataID;
    }

    @Override
    public void close() {
        final FilesFacade ff = configuration.getFilesFacade();
        if (addr != 0) {
            ff.munmap(addr, DataID.FILE_SIZE, MemoryTag.MMAP_DEFAULT);
            addr = 0;
        }
        if (ff.close(fd)) {
            fd = -1;
        }
    }
}
