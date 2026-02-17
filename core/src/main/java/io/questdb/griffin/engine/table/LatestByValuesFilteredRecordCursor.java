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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValuesFilteredRecordCursor extends AbstractDescendingRecordListCursor {
    private final int columnIndex;
    private final IntHashSet deferredSymbolKeys;
    private final Function filter;
    private final IntIntHashMap map;
    private final IntHashSet symbolKeys;
    private boolean isMapPrepared;

    public LatestByValuesFilteredRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            int columnIndex,
            @Nullable DirectLongList rows,
            @NotNull IntHashSet symbolKeys,
            @Nullable IntHashSet deferredSymbolKeys,
            @NotNull Function filter
    ) {
        super(configuration, metadata, rows);
        this.columnIndex = columnIndex;
        this.symbolKeys = symbolKeys;
        this.deferredSymbolKeys = deferredSymbolKeys;
        this.map = new IntIntHashMap(Numbers.ceilPow2(symbolKeys.size()));
        this.filter = filter;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        isMapPrepared = false;
        super.of(pageFrameCursor, executionContext);
        filter.init(pageFrameCursor, executionContext);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
    }

    @Override
    public void toTop() {
        super.toTop();
        filter.toTop();
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

        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);

            for (long row = partitionHi - partitionLo; row >= 0; row--) {
                recordA.setRowIndex(row);
                if (filter.getBool(recordA)) {
                    int key = TableUtils.toIndexKey(recordA.getInt(columnIndex));
                    int index = map.keyIndex(key);
                    if (index < 0 && map.valueAt(index) == 0) {
                        rows.add(Rows.toRowID(frameIndex, row));
                        map.putAt(index, key, 1);
                    }
                }
            }
        }
    }
}
