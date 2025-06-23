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

package io.questdb.griffin;

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;

/**
 * This class supports generation of VirtualRecordCursorFactory in allowing functions
 * reference previously used function of the same projection.
 *
 * When generating factory we're compiling functions in the order they appear in the column list. Avoiding
 * multi-pass over the columns we do not know the metadata for the virtual columns upfront. To work around this
 * we allocate metadata slots for all function in the projection and add base column metadata after that.
 */
public class VirtualPriorityMetadata extends AbstractRecordMetadata {
    private final int virtualColumnCount;
    private int pos = 0;

    public VirtualPriorityMetadata(int virtualColumnCount, RecordMetadata baseMetadata) {
        this.virtualColumnCount = virtualColumnCount;
        int baseColumnCount = baseMetadata.getColumnCount();
        columnMetadata.setPos(virtualColumnCount + baseColumnCount);
        for (int i = 0; i < baseColumnCount; i++) {
            TableColumnMetadata m = baseMetadata.getColumnMetadata(i);
            int pos = virtualColumnCount + i;
            columnMetadata.set(pos, m);
            // set the initial position assuming virtual column names can clash
            // with the base column names. We will update the position when we start
            // compiling virtual functions
            columnNameIndexMap.put(m.getColumnName(), pos);
        }
    }

    public void add(TableColumnMetadata m) {
        assert pos < virtualColumnCount;
        // Check if column name is a duplicate. The allowed duplicates are
        // when virtual column takes precedence over the base column. We can check
        // that when column index is over the virtual column count.
        int keyIndex = columnNameIndexMap.keyIndex(m.getColumnName());
        if (keyIndex < 0 && columnNameIndexMap.valueAt(keyIndex) < virtualColumnCount) {
            throw CairoException.duplicateColumn(m.getColumnName());
        }

        columnMetadata.set(pos, m);
        columnNameIndexMap.putAt(keyIndex, m.getColumnName(), pos);
        pos++;
    }
}
