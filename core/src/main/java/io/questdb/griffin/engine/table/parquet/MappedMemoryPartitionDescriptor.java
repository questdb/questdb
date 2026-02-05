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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.SymbolMapWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;

// This class is used to free memory mapped regions
public class MappedMemoryPartitionDescriptor extends PartitionDescriptor {
    private final FilesFacade ff;

    public MappedMemoryPartitionDescriptor(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void clear() {
        final int columnCount = getColumnCount();
        for (long columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final long rawIndex = columnIndex * COLUMN_ENTRY_SIZE;

            final long columnAddr = columnData.get(rawIndex + COLUMN_ADDR_OFFSET);
            if (columnAddr != 0) {
                final long columnSize = columnData.get(rawIndex + COLUMN_SIZE_OFFSET);
                ff.madvise(columnAddr, columnSize, Files.POSIX_MADV_DONTNEED);
                ff.munmap(columnAddr, columnSize, MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER);
            }
            final long columnSecondaryAddr = columnData.get(rawIndex + COLUMN_SECONDARY_ADDR_OFFSET);
            if (columnSecondaryAddr != 0) {
                final long columnSecondarySize = columnData.get(rawIndex + COLUMN_SECONDARY_SIZE_OFFSET);
                ff.madvise(columnSecondaryAddr, columnSecondarySize, Files.POSIX_MADV_DONTNEED);
                ff.munmap(columnSecondaryAddr, columnSecondarySize, MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER);
            }
            final long symbolOffsetsAddr = columnData.get(rawIndex + SYMBOL_OFFSET_ADDR_OFFSET);
            if (symbolOffsetsAddr != 0) {
                final long symbolOffsetsSize = columnData.get(rawIndex + SYMBOL_OFFSET_SIZE_OFFSET);
                final long offsetsMemSize = SymbolMapWriter.keyToOffset((int) symbolOffsetsSize + 1);
                final long originalAddr = symbolOffsetsAddr - SymbolMapWriter.HEADER_SIZE;
                ff.madvise(originalAddr, offsetsMemSize, Files.POSIX_MADV_DONTNEED);
                ff.munmap(originalAddr, offsetsMemSize, MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER);
            }
        }

        super.clear();
    }
}
