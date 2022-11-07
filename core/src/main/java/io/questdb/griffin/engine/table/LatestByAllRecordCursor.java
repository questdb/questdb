/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByAllRecordCursor extends AbstractDescendingRecordListCursor {

    private final Map map;
    private final RecordSink recordSink;

    public LatestByAllRecordCursor(Map map, DirectLongList rows, RecordSink recordSink, @NotNull IntList columnIndexes) {
        super(rows, columnIndexes);
        this.map = map;
        this.recordSink = recordSink;
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

        DataFrame frame;
        if (!isOpen()) {
            map.reopen();
        }
        try {
            while ((frame = this.dataFrameCursor.next()) != null) {
                final int partitionIndex = frame.getPartitionIndex();
                final long rowLo = frame.getRowLo();
                final long rowHi = frame.getRowHi() - 1;

                recordA.jumpTo(frame.getPartitionIndex(), rowHi);
                for (long row = rowHi; row >= rowLo; row--) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordA.setRecordIndex(row);
                    MapKey key = map.withKey();
                    key.put(recordA, recordSink);
                    if (key.create()) {
                        rows.add(Rows.toRowID(partitionIndex, row));
                    }
                }
            }
        } finally {
            map.clear();
        }
    }

    @Override
    public void close() {
        if (isOpen()) {
            map.close();
            super.close();
        }
    }
}
