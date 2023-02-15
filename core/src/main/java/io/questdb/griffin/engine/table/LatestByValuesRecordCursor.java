/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesRecordCursor extends AbstractDescendingRecordListCursor {

    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final IntIntHashMap map;
    private final IntHashSet symbolKeys;
    private boolean isMapPrepared;

    public LatestByValuesRecordCursor(
            int columnIndex,
            DirectLongList rows,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            @NotNull IntList columnIndexes
    ) {
        super(rows, columnIndexes);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
        this.map = new IntIntHashMap(Numbers.ceilPow2(symbolKeys.size()));
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        isMapPrepared = false;
        super.of(dataFrameCursor, executionContext);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan").meta("on").putColumnName(columnIndex);
    }

    private void prepareMap() {
        if (deferredSymbolKeys != null) {
            // We need to clean up the map when there are deferred keys since
            // they may contain bind variables.
            map.clear();
            for (int i = 0, n = deferredSymbolKeys.size(); i < n; i++) {
                map.put(deferredSymbolKeys.get(i), 0);
            }
        }

        for (int i = 0, n = symbolKeys.size(); i < n; i++) {
            map.put(symbolKeys.get(i), 0);
        }
    }

    @Override
    protected void buildTreeMap() {
        if (!isMapPrepared) {
            prepareMap();
            isMapPrepared = true;
        }

        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
            final int partitionIndex = frame.getPartitionIndex();
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            recordA.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = TableUtils.toIndexKey(recordA.getInt(columnIndex));
                int index = map.keyIndex(key);
                if (index < 0 && map.valueAt(index) == 0) {
                    rows.add(Rows.toRowID(partitionIndex, row));
                    map.putAt(index, key, 1);
                }
            }
        }
    }
}
