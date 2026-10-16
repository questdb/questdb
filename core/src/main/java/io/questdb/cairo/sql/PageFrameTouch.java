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

package io.questdb.cairo.sql;

import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.std.Files;
import io.questdb.std.Unsafe;

public final class PageFrameTouch {
    private PageFrameTouch() {
    }

    /**
     * Fault every intersecting OS page in while the caller holds the source lease.
     */
    public static long touchMemory(long address, long size, SqlExecutionCircuitBreaker circuitBreaker) {
        if (address == 0 || size == 0) {
            return 0;
        }
        assert size > 0;
        final long pageSize = Files.PAGE_SIZE;
        long sum = Unsafe.getByte(address);
        long pages = 1;
        // A chunk can start in the middle of a page. Start the remaining probes
        // at the next boundary, so a short tail on the last page is not missed.
        for (long offset = pageSize - address % pageSize; offset < size; offset += pageSize) {
            if ((pages & 63) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            sum += Unsafe.getByte(address + offset);
            pages++;
        }
        AsyncFilterAtom.PRE_TOUCH_BLACK_HOLE.add(sum);
        return pages;
    }
}
