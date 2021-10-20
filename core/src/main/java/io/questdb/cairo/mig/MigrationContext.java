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

package io.questdb.cairo.mig;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

class MigrationContext {
    private final long tempMemory;
    private final int tempMemoryLen;
    private final MemoryARW tempVirtualMem;
    private final MemoryMARW rwMemory;
    private final CairoEngine engine;
    private Path tablePath;
    private long metadataFd;
    private Path tablePath2;

    public MemoryMARW getRwMemory() {
        return rwMemory;
    }

    public long getTempMemory() {
        return tempMemory;
    }

    public MigrationContext(
            CairoEngine engine,
            long mem,
            int tempMemSize,
            MemoryARW tempVirtualMem,
            MemoryMARW rwMemory
    ) {
        this.engine = engine;
        this.tempMemory = mem;
        this.tempMemoryLen = tempMemSize;
        this.tempVirtualMem = tempVirtualMem;
        this.rwMemory = rwMemory;
    }

    public MemoryMARW createRwMemoryOf(FilesFacade ff, Path path) {
        // re-use same rwMemory
        // assumption that it is re-usable after the close() and then of()  methods called.
        rwMemory.smallFile(ff, path, MemoryTag.NATIVE_DEFAULT);
        return rwMemory;
    }

    public CairoConfiguration getConfiguration() {
        return engine.getConfiguration();
    }

    public FilesFacade getFf() {
        return getConfiguration().getFilesFacade();
    }

    public long getMetadataFd() {
        return metadataFd;
    }

    public int getNextTableId() {
        return (int) engine.getNextTableId();
    }

    public Path getTablePath() {
        return tablePath;
    }

    public Path getTablePath2() {
        return tablePath2;
    }

    public long getTempMemory(int size) {
        if (size <= tempMemoryLen) {
            return tempMemory;
        }
        throw new UnsupportedOperationException("No temp memory of size "
                + size
                + " is allocate. Only "
                + tempMemoryLen
                + " is available");
    }

    public MemoryARW getTempVirtualMem() {
        return tempVirtualMem;
    }

    public MigrationContext of(Path path, Path pathCopy, long metadataFd) {
        this.tablePath = path;
        this.tablePath2 = pathCopy;
        this.metadataFd = metadataFd;
        return this;
    }
}
