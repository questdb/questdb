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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public interface MemoryM extends Closeable {

    FilesFacade getFilesFacade();

    default boolean isOpen() {
        return getFd() != -1;
    }

    @Override
    void close();

    long getFd();

    default boolean isDeleted() {
        return !getFilesFacade().exists(getFd());
    }

    boolean isMapped(long offset, long len);

    default void allocate(long size) {
        TableUtils.allocateDiskSpace(getFilesFacade(), getFd(), size);
    }

    /**
     * Maps file to memory
     *  @param ff                the files facade - an abstraction used to simulate failures
     * @param name              the name of the file
     * @param extendSegmentSize for those implementations that can need to extend mapped memory beyond available file size
 *                          should use this parameter as the increment size
     * @param size              size of the initial mapped memory when smaller than the actual file
     * @param memoryTag     memory tag for diagnostics
     */
    void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag);

    default void partialFile(FilesFacade ff, LPSZ name, long size, int memoryTag) {
        of(ff, name, ff.getMapPageSize(), size, memoryTag);
    }

    default void wholeFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getMapPageSize(), ff.length(name), memoryTag);
    }

    default void smallFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getPageSize(), ff.length(name), memoryTag);
    }
}
