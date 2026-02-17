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

package io.questdb.std;

import io.questdb.cairo.Reopenable;

/**
 * Off-heap bounded sorted list for long values accompanied by a long index.
 */
public interface DirectLongLongSortedList extends QuietCloseable, Mutable, Reopenable {
    int ASC_ORDER = 0;
    int DESC_ORDER = 1;

    static DirectLongLongSortedList getInstance(int order, int limit, int memoryTag) {
        return order == ASC_ORDER
                ? new DirectLongLongAscList(limit, memoryTag)
                : new DirectLongLongDescList(limit, memoryTag);
    }

    void add(long index, long value);

    int getCapacity();

    Cursor getCursor();

    /**
     * @return 0 for ascending and 1 for descending
     */
    int getOrder();

    void reopen(int capacity);

    int size();

    interface Cursor {

        boolean hasNext();

        long index();

        void toTop();

        long value();
    }
}
