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

package io.questdb.cairo;

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

public class ExtendableOnePageMemory extends OnePageMemory {
    public ExtendableOnePageMemory(FilesFacade ff, LPSZ name, long size) {
        super(ff, name, size);
    }

    public ExtendableOnePageMemory() {
        super();
    }

    @Override
    public void grow(long newSize) {
        final long fileSize = ff.length(fd);
        newSize = Math.max(newSize, fileSize);
        if (newSize <= size) {
            return;
        }

        long offset = absolutePointer - page;
        long previousSize = size;
        if (previousSize > 0) {
            page = ff.mremap(fd, page, previousSize, newSize, 0, Files.MAP_RO);
        } else {
            assert page == -1;
            page = ff.mmap(fd, newSize, 0, Files.MAP_RO);
        }
        if (page == FilesFacade.MAP_FAILED) {
            long fd = this.fd;
            close();
            throw CairoException.instance(ff.errno()).put("Could not remap file [previousSize=").put(previousSize).put(", newSize=").put(newSize).put(", fd=").put(fd).put(']');
        }
        size = newSize;
        absolutePointer = page + offset;
    }

    @Override
    protected void map(FilesFacade ff, LPSZ name, long size) {
        size = Math.min(ff.length(fd), size);
        super.map(ff, name, size);
    }
}
