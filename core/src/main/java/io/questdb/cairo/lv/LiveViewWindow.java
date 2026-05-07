/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * Per-row driver that wires a named WINDOW's ANCHOR clause to live-view window
 * functions' {@link WindowFunction#resetPartition(Record)} contract.
 * <p>
 * Built once per refresh cycle when the live view's compiled SELECT contains an
 * anchored named WINDOW. Per-row flow:
 * <ol>
 *     <li>Extract the row's partition key via {@link #partitionByRecord} +
 *     {@link #partitionBySink}.</li>
 *     <li>Evaluate {@link #anchorExpression} against the row.</li>
 *     <li>Compare to the per-partition last-seen anchor value held in
 *     {@link #anchorMap}.</li>
 *     <li>If the anchor changed (or the partition is brand new), dispatch
 *     {@link WindowFunction#resetPartition(Record)} to every function on this
 *     WINDOW, then update the partition's recorded anchor value.</li>
 * </ol>
 * <p>
 * Phase 1 limitations:
 * <ul>
 *     <li>Anchor expressions must return a {@code TIMESTAMP} (the most common
 *     calendar-period anchor case). Other primitive types land alongside
 *     migration of more functions.</li>
 *     <li>One {@code LiveViewWindow} per LV — multi-window LVs with different
 *     anchors are rejected at CREATE (deferred validation).</li>
 *     <li>{@link #functions} is the full set of window functions in the SELECT.
 *     Multi-window queries where only a subset belongs to the anchored WINDOW
 *     are out of scope until the per-WINDOW dispatch landing.</li>
 * </ul>
 */
public class LiveViewWindow implements QuietCloseable {
    // Slot 0: last-seen anchor value (LONG / TIMESTAMP).
    // Slot 1: byte flag — 0 means "uninitialized", 1 means "set". The MapValue's
    // intrinsic isNew() flips to false on first access; we use this explicit flag
    // so the live-view processRow can distinguish "first row of a partition" from
    // "anchor changed between rows."
    private static final int SLOT_ANCHOR_VALUE = 0;
    private static final int SLOT_INITIALIZED = 1;

    private final Function anchorExpression;
    private final int anchorValueType;
    private final Map anchorMap;
    private final ObjList<WindowFunction> functions;
    private final VirtualRecord partitionByRecord;
    private final RecordSink partitionBySink;

    public LiveViewWindow(
            @NotNull Function anchorExpression,
            int anchorValueType,
            @NotNull Map anchorMap,
            @NotNull VirtualRecord partitionByRecord,
            @NotNull RecordSink partitionBySink,
            @NotNull ObjList<WindowFunction> functions
    ) {
        this.anchorExpression = anchorExpression;
        this.anchorValueType = anchorValueType;
        this.anchorMap = anchorMap;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.functions = functions;
    }

    @Override
    public void close() {
        Misc.free(anchorExpression);
        Misc.free(anchorMap);
    }

    public ObjList<WindowFunction> getFunctions() {
        return functions;
    }

    /**
     * Drives the per-row anchor-comparison + reset-dispatch logic for one input row.
     * Must be invoked before the row reaches the underlying window cursor's
     * {@code computeNext}.
     */
    public void processRow(Record record) {
        partitionByRecord.of(record);
        MapKey key = anchorMap.withKey();
        key.put(partitionByRecord, partitionBySink);
        MapValue value = key.createValue();

        long currentAnchor = readAnchorValue(record);
        boolean shouldReset;

        if (value.isNew()) {
            // First row for this partition — anchor map didn't carry it yet. Functions
            // either have no per-partition state yet (in which case resetPartition is
            // a no-op) or have stale state from a prior partition that was evicted —
            // resetting it is the safe default.
            shouldReset = true;
        } else {
            byte initialized = value.getByte(SLOT_INITIALIZED);
            long lastAnchor = value.getLong(SLOT_ANCHOR_VALUE);
            shouldReset = initialized == 0 || lastAnchor != currentAnchor;
        }

        if (shouldReset) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).resetPartition(record);
            }
            value.putLong(SLOT_ANCHOR_VALUE, currentAnchor);
            value.putByte(SLOT_INITIALIZED, (byte) 1);
        }
    }

    /**
     * Returns the column types the anchor map's value layout uses, so factories
     * can construct compatible Maps.
     */
    public static ColumnTypes anchorMapValueTypes() {
        return AnchorMapValueTypes.INSTANCE;
    }

    private long readAnchorValue(Record record) {
        // Phase 1: TIMESTAMP-only path. Other primitive return types (LONG, INT,
        // BOOLEAN, STRING, SYMBOL, DOUBLE) land with the rest of group #6 / migration
        // work — at that point we'd dispatch on anchorValueType and pick the right
        // getter.
        if (ColumnType.tagOf(anchorValueType) == ColumnType.TIMESTAMP) {
            return anchorExpression.getTimestamp(record);
        }
        if (ColumnType.tagOf(anchorValueType) == ColumnType.LONG) {
            return anchorExpression.getLong(record);
        }
        // Fallback: try LONG. Any other type would have been rejected at CREATE
        // (the validator narrows accepted types).
        return anchorExpression.getLong(record);
    }

    /**
     * Static singleton {@link ColumnTypes} for the anchor map's value layout —
     * exposed via {@link #anchorMapValueTypes()} so callers don't have to know
     * the slot order.
     */
    private static final class AnchorMapValueTypes implements ColumnTypes {
        static final AnchorMapValueTypes INSTANCE = new AnchorMapValueTypes();

        @Override
        public int getColumnCount() {
            return 2;
        }

        @Override
        public int getColumnType(int columnIndex) {
            switch (columnIndex) {
                case SLOT_ANCHOR_VALUE:
                    return ColumnType.LONG;
                case SLOT_INITIALIZED:
                    return ColumnType.BYTE;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }
    }
}
