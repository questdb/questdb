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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

/**
 * Returns the GROUPING bitmask for its argument columns. For each argument,
 * bit 1 means the column is rolled up (not actively grouped) in the current
 * grouping set, bit 0 means the column is actively grouped.
 *
 * <p>Implements {@link io.questdb.griffin.engine.functions.GroupByFunction} so that the optimizer keeps it in the
 * GROUP BY model's projection. The bitmask value is stored in the map value
 * column: during the data scan, {@link #setCurrentSet(int)} is called per
 * grouping set before the updater runs, and {@link #computeFirst} stores
 * the pre-computed bitmask. During iteration, {@link #getInt(Record)} reads
 * the stored value from the map record.</p>
 */
public class GroupingFunction extends IntFunction implements GroupByFunction {
    private final IntList argColumnIndices;
    private final int position;
    private int currentValue;
    private int[] perSetValues;
    private int valueIndex;

    public GroupingFunction(IntList argColumnIndices, int position) {
        this.argColumnIndices = argColumnIndices;
        this.position = position;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putInt(valueIndex, currentValue);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // All rows in a given grouping set produce the same bitmask,
        // so no update is needed after the first row.
    }

    public IntList getArgColumnIndices() {
        return argColumnIndices;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(valueIndex);
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    /**
     * Pre-computes the per-set bitmask values. Called by
     * {@link io.questdb.griffin.engine.groupby.GroupingSetsRecordCursorFactory}
     * after construction, once the grouping set layout is known.
     *
     * @param groupingIds      per-set bitmask indicating which keys are rolled up
     * @param keyColumnIndices maps key position to base column index
     */
    public void init(IntList groupingIds, IntList keyColumnIndices) throws SqlException {
        int setCount = groupingIds.size();
        int keyCount = keyColumnIndices.size();
        perSetValues = new int[setCount];

        for (int s = 0; s < setCount; s++) {
            int fullMask = groupingIds.getQuick(s);
            int result = 0;
            for (int a = 0, nArgs = argColumnIndices.size(); a < nArgs; a++) {
                int argBaseIdx = argColumnIndices.getQuick(a);
                // Find this arg's position in the key column list
                int keyPos = -1;
                for (int k = 0; k < keyCount; k++) {
                    if (keyColumnIndices.getQuick(k) == argBaseIdx) {
                        keyPos = k;
                        break;
                    }
                }
                if (keyPos < 0) {
                    throw SqlException.$(position, "GROUPING()/GROUPING_ID() argument must be a grouping key column");
                }
                // Extract the bit for this key position from the full bitmask.
                // Bit (keyCount - 1 - keyPos) in the full mask tells whether
                // this key is rolled up (1) or active (0).
                int bit = (fullMask >> (keyCount - 1 - keyPos)) & 1;
                result |= (bit << (argColumnIndices.size() - 1 - a));
            }
            perSetValues[s] = result;
        }
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.INT);
    }

    public void setCurrentSet(int setIndex) {
        currentValue = perSetValues[setIndex];
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, Numbers.INT_NULL);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("grouping(");
        for (int i = 0, n = argColumnIndices.size(); i < n; i++) {
            if (i > 0) {
                sink.val(',');
            }
            sink.putColumnName(argColumnIndices.getQuick(i));
        }
        sink.val(')');
    }
}
