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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;

public interface ColumnTypes {

    /**
     * Returns total size in bytes in case of all fixed-size columns
     * or -1 if there is a var-size column in the given list.
     */
    static int sizeInBytes(ColumnTypes types) {
        if (types == null) {
            return 0;
        }
        int totalSize = 0;
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnType = types.getColumnType(i);
            final int size = ColumnType.sizeOf(columnType);
            if (size > 0) {
                totalSize += size;
            } else {
                return -1;
            }
        }
        return totalSize;
    }

    int getColumnCount();

    int getColumnType(int columnIndex);

    /**
     * Returns true when column {@code columnIndex} carries the same value at INT and at LONG
     * width, i.e. reading it at INT width and widening loses nothing. Only meaningful for a
     * BYTE / SHORT / INT typed column.
     * <p>
     * A stored column always answers true: its bytes are exactly as wide as its type, and a
     * reader that took them at LONG width would read past the value. An expression does not
     * have to - overflowing INT arithmetic wraps mod 2^32 under {@code getInt()} while
     * {@code getLong()} keeps the full result (see {@link Function#isIntWidthStable}).
     * <p>
     * {@link io.questdb.griffin.RecordToRowCopierUtils} consults this to decide whether an
     * INT source feeding a 64-bit column reads {@code getInt()} or {@code getLong()}, so that
     * the value a row keeps matches the value an explicit cast of the same expression reads.
     * The conservative default is true, which reproduces the INT-width read.
     *
     * @param columnIndex column index
     * @return true if reading the column at INT width and widening equals reading it at LONG width
     */
    default boolean isColumnIntWidthStable(int columnIndex) {
        return true;
    }
}
