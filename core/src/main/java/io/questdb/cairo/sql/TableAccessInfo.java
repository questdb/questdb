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

import io.questdb.cairo.IndexType;
import io.questdb.std.IntList;

/**
 * What a planner needs to know about a table to choose how to read it: its indexes, symbol tables and partitioning.
 * A column index is the column's position in the table's metadata.
 */
public interface TableAccessInfo {

    /**
     * The position of the named column, -1 when the table has no such column.
     */
    int getColumnIndex(CharSequence columnName);

    int getColumnType(int columnIndex);

    /**
     * The writer indexes of the columns the covering index of the column includes; null or empty when it has none.
     */
    IntList getCoveringColumnIndices(int columnIndex);

    /**
     * The position of the column among the columns the covering index of the key column includes, -1 when it does not
     * include the column.
     */
    default int getCoveredPosition(int keyColumnIndex, int columnIndex) {
        final IntList included = getCoveringColumnIndices(keyColumnIndex);
        if (included != null) {
            final int writerIndex = getWriterIndex(columnIndex);
            for (int i = 0, n = included.size(); i < n; i++) {
                if (included.getQuick(i) == writerIndex) {
                    return i;
                }
            }
        }
        return -1;
    }

    byte getIndexType(int columnIndex);

    int getPartitionedBy();

    int getSymbolCapacity(int columnIndex);

    int getSymbolCount(int columnIndex);

    int getWriterIndex(int columnIndex);

    /**
     * Whether the table has a parquet partition and a column whose type a change re-keyed after its creation, so a
     * parquet partition may store it in another type than the table declares.
     */
    boolean hasParquetConvertedColumns();

    /**
     * Whether the covering index of the key column includes every other column of {@code columnIndexes}.
     */
    default boolean isCovering(int keyColumnIndex, IntList columnIndexes) {
        final IntList included = getCoveringColumnIndices(keyColumnIndex);
        if (included == null || included.size() == 0) {
            return false;
        }
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            if (columnIndex != keyColumnIndex && getCoveredPosition(keyColumnIndex, columnIndex) < 0) {
                return false;
            }
        }
        return true;
    }

    default boolean isIndexed(int columnIndex) {
        return IndexType.isIndexed(getIndexType(columnIndex));
    }

    boolean isSymbolTableStatic(int columnIndex);
}
