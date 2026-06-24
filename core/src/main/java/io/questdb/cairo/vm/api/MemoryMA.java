/*+*****************************************************************************
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

import io.questdb.cairo.vm.Vm;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;

// mapped appendable
public interface MemoryMA extends MemoryM, MemoryA {

    default void close(boolean truncate) {
        close(truncate, Vm.TRUNCATE_TO_PAGE);
    }

    void close(boolean truncate, byte truncateMode);

    long getAppendAddress();

    long getAppendAddressSize();

    void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts);

    /**
     * Marks this memory as strictly append-only so that {@link #sync(boolean)} may narrow the
     * msync to the written range (and skip when nothing new was appended) instead of flushing the
     * full mapped extent. Only safe for memories whose writes are exclusively appends
     * ({@code put*(value)} / {@code jumpTo} / {@code truncate}); memories that perform in-place
     * {@code put*(offset, value)} updates below the high-water mark must stay full-extent (default).
     * Default is a no-op so implementations that always full-sync are unaffected.
     */
    default void setAppendOnly(boolean appendOnly) {
    }

    default void setSize(long size) {
        jumpTo(size);
    }

    void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode);

    void sync(boolean async);

    default void toTop() {
        jumpTo(0);
    }
}
