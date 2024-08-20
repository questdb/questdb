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
import io.questdb.cairo.SymbolMapWriter;

public class MappedMemoryPartitionDescriptor extends BorrowedMemoryPartitionDescriptor {
    @Override
    public void clear() {
        for (long i = 0, n = columnAddrs.size(); i < n; i++) {
            Files.munmap(columnAddrs.get(i), columnSizes.get(i), MemoryTag.MMAP_PARTITION_CONVERTER);
        }
        for (long i = 0, n = columnSecondaryAddrs.size(); i < n; i++) {
            Files.munmap(columnSecondaryAddrs.get(i), columnSecondarySizes.get(i), MemoryTag.MMAP_PARTITION_CONVERTER);
        }
        for (long i = 0, n = symbolOffsetsAddrs.size(); i < n; i++) {
            final long offsetsMemSize = SymbolMapWriter.keyToOffset((int) symbolOffsetsSizes.get(i) + 1);
            Files.munmap(symbolOffsetsAddrs.get(i) - SymbolMapWriter.HEADER_SIZE, offsetsMemSize, MemoryTag.MMAP_PARTITION_CONVERTER);
        }
        super.clear();
    }
}
