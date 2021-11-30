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

import io.questdb.cairo.vm.MemoryCMRImpl;
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
        this.mem = new MemoryCMRImpl(ff, fileName, size, MemoryTag.MMAP_TABLE_READER);
    }

    @Override
    public void close() {
        mem.close();
    }

    public LongList getColumnVersions() {
        return cachedList;
    }

    public void load(long offset, long areaSize) {
        resize(offset + areaSize);

        long p = offset;
        int i = 0;
        long lim = offset + areaSize;
        cachedList.setPos((int) ((areaSize / (ColumnVersionWriter.BLOCK_SIZE * 8)) * 4));

        while (p < lim) {
            cachedList.setQuick(i, mem.getLong(p));
            cachedList.setQuick(i + 1, mem.getLong(p + 8));
            cachedList.setQuick(i + 2, mem.getLong(p + 16));
            cachedList.setQuick(i + 3, mem.getLong(p + 24));
            i += 4;
            p += ColumnVersionWriter.BLOCK_SIZE * 8;
        }
    }

    public void resize(long size) {
        mem.resize(size);
    }
}
