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

import io.questdb.std.*;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

public class PartitionDescriptor implements QuietCloseable {
    private boolean memoryOwner;
    private DirectUtf8Sink tableName = new DirectUtf8Sink(16);
    private DirectUtf8Sink columnNames = new DirectUtf8Sink(32);
    private DirectIntList columnNameLengths = new DirectIntList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectIntList columnIds = new DirectIntList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList columnTops = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectIntList columnTypes = new DirectIntList(16, MemoryTag.NATIVE_DEFAULT);

    private DirectLongList columnAddrs = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList columnSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList columnSecondaryAddrs = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList columnSecondarySizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList symbolOffsetsAddrs = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
    private DirectLongList symbolOffsetsSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);

    private long partitionRowCount;
    private int timestampIndex = -1;

    @Override
    public void close() {
        clear();
        tableName = Misc.free(tableName);
        columnNames = Misc.free(columnNames);
        columnNameLengths = Misc.free(columnNameLengths);
        columnTypes = Misc.free(columnTypes);
        columnIds = Misc.free(columnIds);
        columnTops = Misc.free(columnTops);
        columnAddrs = Misc.free(columnAddrs);
        columnSizes = Misc.free(columnSizes);
        columnSecondaryAddrs = Misc.free(columnSecondaryAddrs);
        columnSecondarySizes = Misc.free(columnSecondarySizes);
        symbolOffsetsAddrs = Misc.free(symbolOffsetsAddrs);
        symbolOffsetsSizes = Misc.free(symbolOffsetsSizes);
    }

    public PartitionDescriptor of(final CharSequence tableName, long partitionRowCount, int timestampIndex, boolean memoryOwner) {
        this.clear();
        this.tableName.put(tableName);
        this.memoryOwner = memoryOwner;
        this.partitionRowCount = partitionRowCount;
        this.timestampIndex = timestampIndex;
        return this;
    }

    public Utf8Sequence getTableName() {
        return tableName;
    }

    public int getColumnCount() {
        return (int) columnAddrs.size();
    }

    public long getPartitionRowCount() {
        return partitionRowCount;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public void clear() {
        if (memoryOwner) {
            for (long i = 0, n = columnAddrs.size(); i < n; i++) {
                Files.munmap(columnAddrs.get(i), columnSizes.get(i), MemoryTag.MMAP_PARTITION_CONVERTER);
            }
            for (long i = 0, n = columnSecondaryAddrs.size(); i < n; i++) {
                Files.munmap(columnSecondaryAddrs.get(i), columnSecondarySizes.get(i), MemoryTag.MMAP_PARTITION_CONVERTER);
            }
            for (long i = 0, n = symbolOffsetsAddrs.size(); i < n; i++) {
                Files.munmap(symbolOffsetsAddrs.get(i), symbolOffsetsSizes.get(i), MemoryTag.MMAP_PARTITION_CONVERTER);
            }
        }

        tableName.clear();
        columnNames.clear();
        columnNameLengths.clear();
        columnIds.clear();
        columnTops.clear();
        columnTypes.clear();
        columnAddrs.clear();
        columnSizes.clear();
        columnSecondaryAddrs.clear();
        columnSecondarySizes.clear();
        symbolOffsetsAddrs.clear();
        symbolOffsetsSizes.clear();
    }

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
        columnNameLengths.add(columnNames.size() - startSize);
        columnIds.add(columnId);
        columnTops.add(columnTop);
        columnTypes.add(columnType);
        columnAddrs.add(columnAddr);
        columnSizes.add(columnSize);
        columnSecondaryAddrs.add(columnSecondaryAddr);
        columnSecondarySizes.add(columnSecondarySize);
        symbolOffsetsAddrs.add(symbolOffsetsAddr);
        symbolOffsetsSizes.add(symbolOffsetsSize);
    }

    public long getColumnNamesPtr() {
        return columnNames.ptr();
    }

    public int getColumnNamesSize() {
        return columnNames.size();
    }

    public long getColumnNameLengthsPtr() {
        return columnNameLengths.getAddress();
    }

    public long getColumnTypesPtr() {
        return columnTypes.getAddress();
    }

    public long getColumnIdsPtr() {
        return columnIds.getAddress();
    }

    public  long getColumnTopsPtr() {
        return columnTops.getAddress();
    }

    public long getColumnAddressesPtr() {
        return columnAddrs.getAddress();
    }

    public long getColumnSizesPtr() {
        return columnSizes.getAddress();
    }

    public long getColumnSecondaryAddressesPtr() {
        return columnSecondaryAddrs.getAddress();
    }

    public long getColumnSecondarySizesPtr() {
        return columnSecondarySizes.getAddress();
    }

    public long getSymbolOffsetsAddressesPtr() {
        return symbolOffsetsAddrs.getAddress();
    }

    public long getSymbolOffsetsSizesPtr() {
        return symbolOffsetsSizes.getAddress();
    }

}
