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

import io.questdb.std.MemoryTracker;
import org.jetbrains.annotations.Nullable;

// appendable readable writable
public interface MemoryARW extends MemoryA, MemoryR, MemoryW, MemoryAR {
    long appendAddressFor(long bytes);

    /**
     * Bind a per-query native memory tracker. Implementations that hold their
     * own native heap route all {@code Unsafe.{malloc,realloc,free}} calls
     * through the tracker-aware overloads while the tracker is set; passing
     * {@code null} (the default) restores global-only accounting. Default
     * no-op for implementations whose backing memory is not workload-scoped
     * (memory-mapped files, table writers, etc.).
     */
    default void setMemoryTracker(@Nullable MemoryTracker tracker) {
    }
}
