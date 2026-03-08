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

import io.questdb.cairo.ColumnType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

// This class manages memory for Parquet partition data.
// It ensures that memory with the same lifetime is handled consistently and is responsible for releasing it.
public class OwnedMemoryPartitionDescriptor extends PartitionDescriptor {

    @Override
    public void clear() {
        final int columnCount = getColumnCount();
        for (long columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final long rawIndex = columnIndex * COLUMN_ENTRY_SIZE;
            final int columnType = (int) columnData.get(rawIndex + COLUMN_ID_AND_TYPE_OFFSET);

            final long columnAddr = columnData.get(rawIndex + COLUMN_ADDR_OFFSET);
            final long columnSize = columnData.get(rawIndex + COLUMN_SIZE_OFFSET);
            Unsafe.free(columnAddr, columnSize, MemoryTag.NATIVE_O3);
            // for symbol columns, the secondary memory is provided by the symbol map
            // no need to free it here
            if (!ColumnType.isSymbol(ColumnType.tagOf(columnType))) {
                final long columnSecondaryAddr = columnData.get(rawIndex + COLUMN_SECONDARY_ADDR_OFFSET);
                final long columnSecondarySize = columnData.get(rawIndex + COLUMN_SECONDARY_SIZE_OFFSET);
                Unsafe.free(columnSecondaryAddr, columnSecondarySize, MemoryTag.NATIVE_O3);
            }
        }

        super.clear();
    }
}
