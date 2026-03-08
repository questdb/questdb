/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

// mapped
public interface MemoryM extends Closeable {

    long addressOf(long offset);

    default void allocate(long size) {
        TableUtils.allocateDiskSpace(getFilesFacade(), getFd(), size);
    }

    @Override
    void close();

    /**
     * Extracts File Descriptor to reuse and unmaps the memory.
     */
    long detachFdClose();

    long getFd();

    FilesFacade getFilesFacade();

    default boolean isDeleted() {
        return !getFilesFacade().exists(getFd());
    }

    boolean isMapped(long offset, long len);

    default boolean isOpen() {
        return getFd() != -1;
    }

    default long map(long offset, long size) {
        if (isMapped(offset, size)) {
            return addressOf(offset);
        }
        return 0;
    }

    /**
     * Maps file to memory
     *
     * @param ff                the files facade - an abstraction used to simulate failures
     * @param name              the name of the file
     * @param extendSegmentSize for those implementations that can need to extend mapped memory beyond available file size
     *                          should use this parameter as the increment size
     * @param size              size of the initial mapped memory when smaller than the actual file
     * @param memoryTag         memory tag for diagnostics
     * @param opts              open file flags
     * @param madviseOpts       madvise flags - madvise call is made when a non-negative value is provided
     */
    void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts);

    default void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        of(ff, name, extendSegmentSize, size, memoryTag, opts, -1);
    }

    default void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag) {
        of(ff, name, extendSegmentSize, size, memoryTag, CairoConfiguration.O_NONE, -1);
    }

    /**
     * Maps file based on its size as reported by the file system. This method must not be used by readers that
     * will map files concurrently with writes performed by other threads. File length is not a reliable way to determine
     * the size of the file contents.
     *
     * @param ff        the facade
     * @param name      file name
     * @param memoryTag the memory tag to track leaks if memory is not released or memory consumption
     */
    default void smallFile(FilesFacade ff, LPSZ name, int memoryTag) {
        of(ff, name, ff.getPageSize(), ff.length(name), memoryTag, CairoConfiguration.O_NONE, -1);
    }
}
