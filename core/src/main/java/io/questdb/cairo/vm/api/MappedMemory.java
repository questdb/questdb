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

import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public interface MappedMemory extends Closeable {

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

    /**
     * Maps file to memory
     *
     * @param ff                the files facade - an abstraction used to simulate failures
     * @param name              the name of the file
     * @param extendSegmentSize for those implementations that can need to extend mapped memory beyond available file size
     *                          should use this parameter as the increment size
     * @param size              size of the initial mapped memory when smaller than the actual file
     */
    void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size);

    default void partialFile(FilesFacade ff, LPSZ name, long size) {
        of(ff, name, ff.getMapPageSize(), size);
    }

    default void wholeFile(FilesFacade ff, LPSZ name) {
        of(ff, name, ff.getMapPageSize(), Long.MAX_VALUE);
    }

    default void smallFile(FilesFacade ff, LPSZ name) {
        of(ff, name, ff.getPageSize(), Long.MAX_VALUE);
    }
}
