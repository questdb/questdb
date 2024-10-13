/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

// This class manages memory for Parquet partition data.
// It handles memory with a different lifetime than the PartitionDescriptor.
public class PartitionDescriptor implements QuietCloseable {
    public static final int COLUMN_ADDR_OFFSET = 3;
    public static final int COLUMN_ENTRY_SIZE = 9;
    public static final int COLUMN_ID_AND_TYPE_OFFSET = 1; // two 4-byte integers packed into a single 8-byte long
    //    The following constants are used to document the column data layout in the columnData DirectLongList
    public static final int COLUMN_NAME_SIZE_OFFSET = 0;
    public static final int COLUMN_SECONDARY_ADDR_OFFSET = 5;
    public static final int COLUMN_SECONDARY_SIZE_OFFSET = 6;
    public static final int COLUMN_SIZE_OFFSET = 4;
    public static final int COLUMN_TOP_OFFSET = 2;
    public static final int SYMBOL_OFFSET_ADDR_OFFSET = 7;
    public static final int SYMBOL_OFFSET_SIZE_OFFSET = 8;
    // A single DirectLongList to store all the column-related data
    protected DirectLongList columnData = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
    // A single DirectUtf8Sink to store all the column names
    protected DirectUtf8Sink columnNames = new DirectUtf8Sink(32);
    protected long partitionRowCount;
    protected DirectUtf8Sink tableName = new DirectUtf8Sink(16);
    protected int timestampIndex = -1;

    public void addColumn(
            final CharSequence columnName,
            int columnType,
            int columnId,
            long columnTop,
            long columnAddr,
            long columnSize,
            long columnSecondaryAddr,
            long columnSecondarySize,
            long symbolOffsetsAddr,
            long symbolOffsetsSize
    ) {
        final int startSize = columnNames.size();
        columnNames.put(columnName);
        final int columnNameSize = columnNames.size() - startSize;
        columnData.add(columnNameSize);
        columnData.add((long) columnId << 32 | columnType);
        columnData.add(columnTop);
        columnData.add(columnAddr);
        columnData.add(columnSize);
        columnData.add(columnSecondaryAddr);
        columnData.add(columnSecondarySize);
        columnData.add(symbolOffsetsAddr);
        columnData.add(symbolOffsetsSize);
    }

    public void clear() {
        tableName.clear();
        columnNames.clear();
        columnData.clear();
    }

    @Override
    public void close() {
        clear();
        tableName = Misc.free(tableName);
        columnNames = Misc.free(columnNames);
        columnData = Misc.free(columnData);
    }

    public int getColumnCount() {
        return (int) (getColumnDataLen() / COLUMN_ENTRY_SIZE);
    }

    public long getColumnDataPtr() {
        return columnData.getAddress();
    }

    public long getColumnDataLen() {
        return columnData.size();
    }

    public long getColumnNamesPtr() {
        return columnNames.ptr();
    }

    public int getColumnNamesLen() {
        return columnNames.size();
    }

    public long getPartitionRowCount() {
        return partitionRowCount;
    }

    public Utf8Sequence getTableName() {
        return tableName;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public PartitionDescriptor of(final CharSequence tableName, long partitionRowCount, int timestampIndex) {
        this.clear();
        this.tableName.put(tableName);
        this.partitionRowCount = partitionRowCount;
        this.timestampIndex = timestampIndex;
        return this;
    }
}