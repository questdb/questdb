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

package io.questdb.cairo.sql;

/**
 * Used internally for index-based (but not only) row access.
 */
public interface RowCursor {

    /**
     * @return true if cursor has more rows, otherwise false.
     */
    boolean hasNext();

    /**
     * Iterates or jumps to given position. Jumping to position has to be performed before
     * attempting to iterate row cursor.
     *
     * @param position row position to jump
     */
    default void jumpTo(long position) {
        while (position-- > 0 && hasNext()) {
            next();
        }
    }

    /**
     * @return numeric index of the next row
     */
    long next();
}
