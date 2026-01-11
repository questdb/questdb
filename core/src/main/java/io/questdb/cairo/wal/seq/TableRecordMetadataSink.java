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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableToken;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;

public interface TableRecordMetadataSink extends Mutable {

    void addColumn(
            String columnName,
            int columnType,
            boolean columnIndexed,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex,
            boolean isDedupKey,
            boolean symbolIsCached,
            int symbolCapacity
    );

    void of(
            TableToken tableToken,
            int tableId,
            int timestampIndex,
            int compressedTimestampIndex,
            long structureVersion,
            int columnCount,
            @Transient IntList readColumnOrder
    );
}
