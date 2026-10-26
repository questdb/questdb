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

import io.questdb.std.ObjList;

/**
 * The facts a {@link PartitionFrameCursorFactory} keeps about the whole table it reads, at the metadata version of its
 * scan: the column names in the table's column order, the designated timestamp, and whether a column changed type
 * after its creation. A compilation builds one per table it scans and never changes it, so the factories of that
 * compilation share it.
 */
public final class TableDescriptor {
    private final ObjList<String> columnNames;
    private final boolean hasConvertedColumns;
    private final int timestampIndex;
    private final int timestampType;

    /**
     * Takes {@code columnNames}, which the caller no longer changes.
     */
    public TableDescriptor(ObjList<String> columnNames, int timestampIndex, int timestampType, boolean hasConvertedColumns) {
        this.columnNames = columnNames;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
        this.hasConvertedColumns = hasConvertedColumns;
    }

    public String getColumnName(int columnIndex) {
        return columnNames.getQuick(columnIndex);
    }

    /**
     * The position of the designated timestamp among the table's columns, -1 when the table has none.
     */
    public int getTimestampIndex() {
        return timestampIndex;
    }

    /**
     * The type of the designated timestamp, {@link io.questdb.cairo.ColumnType#NULL} when the table has none.
     */
    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Whether a column of the table is {@link io.questdb.cairo.TableColumnMetadata#isTypeConverted() type converted}.
     */
    public boolean hasConvertedColumns() {
        return hasConvertedColumns;
    }
}
