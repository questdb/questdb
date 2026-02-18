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

package io.questdb.griffin.engine.table;

import io.questdb.std.LongList;

/**
 * Tracks pre-sizing statistics for group-by maps.
 * Used for pre-allocating map capacity based on previous execution stats.
 */
class GroupByMapStats {
    final LongList medianList = new LongList();
    // We don't use median for heap size since heap is mmapped lazily initialized memory.
    long maxHeapSize;
    long medianSize;
    long mergedHeapSize;
    long mergedSize;

    void update(long medianSize, long maxHeapSize, long mergedSize, long mergedHeapSize) {
        this.medianSize = medianSize;
        this.maxHeapSize = maxHeapSize;
        this.mergedSize = mergedSize;
        this.mergedHeapSize = mergedHeapSize;
    }
}
