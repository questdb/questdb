/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public class ColumnVersionReader implements Closeable {
    private final MemoryCMR mem;
    private final LongList cachedList = new LongList();

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionReader(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = Vm.getCMRInstance(ff, fileName, size, MemoryTag.MMAP_TABLE_READER);
    }

    @Override
    public void close() {
        mem.close();
    }

    public LongList getCachedList() {
        return cachedList;
    }

    public void readUnsafe(long offset, long areaSize) {
        resize(offset + areaSize);

        int i = 0;
        long p = offset;
        long lim = offset + areaSize;

        assert areaSize % ColumnVersionWriter.BLOCK_SIZE_BYTES == 0;

        cachedList.setPos((int) ((areaSize / (ColumnVersionWriter.BLOCK_SIZE_BYTES)) * 4));

        while (p < lim) {
            cachedList.setQuick(i, mem.getLong(p));
            cachedList.setQuick(i + 1, mem.getLong(p + 8));
            cachedList.setQuick(i + 2, mem.getLong(p + 16));
            cachedList.setQuick(i + 3, mem.getLong(p + 24));
            i += ColumnVersionWriter.BLOCK_SIZE;
            p += ColumnVersionWriter.BLOCK_SIZE_BYTES;
        }
    }

    public void resize(long size) {
        mem.resize(size);
    }
}
