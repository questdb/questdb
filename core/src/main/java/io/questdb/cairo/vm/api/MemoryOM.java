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
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

/**
 * Mapped memory with Offset
 */
public interface MemoryOM extends MemoryM {

    long getOffset();

    default void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts) {
        ofOffset(ff, name, 0L, size, memoryTag, opts);
    }

    default void ofOffset(FilesFacade ff, LPSZ name, long lo, long hi, int memoryTag) {
        ofOffset(ff, name, lo, hi, memoryTag, CairoConfiguration.O_NONE);
    }

    /**
     * Maps file to memory
     *
     * @param ff         the files facade - an abstraction used to simulate failures
     * @param fd         read-only file descriptor to reuse or -1 when file should be opened
     * @param keepFdOpen when true, file descriptor is not closed when memory is closed
     * @param name       the name of the file
     *                   should use this parameter as the increment size
     * @param lo         mapped memory low limit (inclusive)
     * @param hi         mapped memory high limit (exclusive)
     * @param memoryTag  memory tag for diagnostics
     * @param opts       file options
     */
    void ofOffset(FilesFacade ff, long fd, boolean keepFdOpen, LPSZ name, long lo, long hi, int memoryTag, int opts);

    /**
     * Maps file to memory
     *
     * @param ff        the files facade - an abstraction used to simulate failures
     * @param name      the name of the file
     *                  should use this parameter as the increment size
     * @param lo        mapped memory low limit (inclusive)
     * @param hi        mapped memory high limit (exclusive)
     * @param memoryTag memory tag for diagnostics
     * @param opts      file options
     */
    default void ofOffset(FilesFacade ff, LPSZ name, long lo, long hi, int memoryTag, int opts) {
        ofOffset(ff, -1, false, name, lo, hi, memoryTag, opts);
    }
}
