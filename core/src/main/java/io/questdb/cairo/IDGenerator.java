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

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class IDGenerator implements Closeable {
    private final CairoConfiguration configuration;
    private final String uniqueIdFileName;
    private final long uniqueIdMemSize;

    private long uniqueIdFd = -1;
    private long uniqueIdMem = 0;

    public IDGenerator(CairoConfiguration configuration, String uniqueIdFileName) {
        this.configuration = configuration;
        this.uniqueIdFileName = uniqueIdFileName;
        this.uniqueIdMemSize = Files.PAGE_SIZE;
    }

    @Override
    public void close() {
        final FilesFacade ff = configuration.getFilesFacade();
        if (uniqueIdMem != 0) {
            ff.munmap(uniqueIdMem, uniqueIdMemSize, MemoryTag.MMAP_DEFAULT);
            uniqueIdMem = 0;
        }
        if (ff.close(uniqueIdFd)) {
            uniqueIdFd = -1;
        }
    }

    public long getNextId() {
        long next;
        long x = getCurrentId();
        do {
            next = x;
            x = Os.compareAndSwap(uniqueIdMem, next, next + 1);
        } while (next != x);
        return next + 1;
    }

    public void open() {
        open(null);
    }

    public void open(Path path) {
        close();
        if (path == null) {
            path = Path.getThreadLocal(configuration.getRoot());
        }
        final int rootLen = path.size();
        try {
            path.concat(uniqueIdFileName);
            final FilesFacade ff = configuration.getFilesFacade();
            uniqueIdFd = TableUtils.openFileRWOrFail(ff, path.$(), configuration.getWriterFileOpenOpts());
            uniqueIdMem = TableUtils.mapRW(ff, uniqueIdFd, uniqueIdMemSize, MemoryTag.MMAP_DEFAULT);
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            path.trimTo(rootLen);
        }
    }

    // This is not thread safe way to reset the id back to 0
    // It is useful for testing only
    public void reset() {
        Unsafe.getUnsafe().putLong(uniqueIdMem, 0);
    }

    long getCurrentId() {
        assert uniqueIdMem != 0;
        return Unsafe.getUnsafe().getLong(uniqueIdMem);
    }
}
