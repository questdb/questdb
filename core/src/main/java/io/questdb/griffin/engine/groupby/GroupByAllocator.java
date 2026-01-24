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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.Reopenable;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * Specialized allocator used in GROUP BY functions. The allocator is closed
 * after query execution.
 * <p>
 * Note: implementations are not necessarily thread-safe.
 */
public interface GroupByAllocator extends QuietCloseable, Mutable, Reopenable {

    /**
     * @return allocated chunks total (bytes).
     */
    @TestOnly
    long allocated();

    /**
     * Best-effort free memory operation. The memory shouldn't be used after it was called.
     */
    void free(long ptr, long size);

    long malloc(long size);

    long realloc(long ptr, long oldSize, long newSize);
}
