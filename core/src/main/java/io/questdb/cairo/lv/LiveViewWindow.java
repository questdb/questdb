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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.BytecodeAssembler;
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
 *     <li>Build the row's partition-by key directly from the source record via
 *     {@link #partitionKeySink}.</li>
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
 *     <li>Anchor expressions must return a {@code TIMESTAMP} or {@code LONG}
 *     (the most common calendar-period anchor case). Other primitive return
 *     types land alongside migration of more functions.</li>
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
    // Slot 2: byte tombstone — 0 means "alive" (partition saw a row recently), 1
    // means "stale" (anchor crossed and no follow-up row visited the partition
    // since). RFC 123 §"Tombstone tracking and periodic compaction" — the
    // anchor-map compaction trigger in Phase 2a.11 reclaims tombstoned entries.
    private static final int SLOT_ANCHOR_VALUE = 0;
    private static final int SLOT_INITIALIZED = 1;
    private static final int SLOT_TOMBSTONE = 2;

    private final Function anchorExpression;
    private final Map anchorMap;
    private final int anchorValueType;
    private final ObjList<WindowFunction> functions;
    private final RecordSink partitionKeySink;
    private final String windowName;
    // Number of anchor-map entries currently flagged SLOT_TOMBSTONE = 1. Mutated
    // only on the refresh-worker thread inside processRow / toTop; not volatile.
    // Phase 2a.11 consumes this to decide when to fire compaction (threshold
    // configured via cairo.live.view.partition.compact.threshold).
    private long tombstoneCount;

    public LiveViewWindow(
            @NotNull String windowName,
            @NotNull Function anchorExpression,
            int anchorValueType,
            @NotNull Map anchorMap,
            @NotNull RecordSink partitionKeySink,
            @NotNull ObjList<WindowFunction> functions
    ) {
        this.windowName = windowName;
        this.anchorExpression = anchorExpression;
        this.anchorValueType = anchorValueType;
        this.anchorMap = anchorMap;
        this.partitionKeySink = partitionKeySink;
        this.functions = functions;
    }

    /**
     * Returns the column types the anchor map's value layout uses, so factories
     * can construct compatible Maps.
     */
    public static ColumnTypes anchorMapValueTypes() {
        return AnchorMapValueTypes.INSTANCE;
    }

    /**
     * Constructs a {@code LiveViewWindow} bound to {@code projectedMetadata} —
     * the record shape produced by the live view's source-side cursor (the leaf
     * page-frame factory in the compiled SELECT). The {@code partitionColumnNames}
     * come from the persisted {@link LiveViewDefinition.LvAnchorSpec}.
     * <p>
     * Throws {@link CairoException} when:
     * <ul>
     *     <li>{@code partitionColumnNames} is empty (Phase 1 requires at least one
     *     partition column on an anchored WINDOW).</li>
     *     <li>any partition column is not present in {@code projectedMetadata}.</li>
     *     <li>the anchor expression's return type is not TIMESTAMP or LONG.</li>
     * </ul>
     */
    public static LiveViewWindow build(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull String windowName,
            @NotNull RecordMetadata projectedMetadata,
            @NotNull ObjList<String> partitionColumnNames,
            @NotNull Function anchorExpression,
            @NotNull ObjList<WindowFunction> functions
    ) {
        int n = partitionColumnNames.size();
        if (n == 0) {
            throw CairoException.nonCritical()
                    .put("anchored live-view window requires PARTITION BY columns");
        }
        // The RecordSink contract: columnFilter holds 1-based indexes into the
        // source record's metadata, and the ColumnTypes argument carries the
        // FULL source metadata's types (the sink looks up types by source index,
        // not by filter slot). The map's key types — separately — must match
        // the filtered subset.
        ListColumnFilter columnFilter = new ListColumnFilter();
        ArrayColumnTypes mapKeyTypes = new ArrayColumnTypes();
        for (int i = 0; i < n; i++) {
            String name = partitionColumnNames.getQuick(i);
            int idx = projectedMetadata.getColumnIndexQuiet(name);
            if (idx < 0) {
                throw CairoException.nonCritical()
                        .put("partition column not found in projected metadata [column=").put(name).put(']');
            }
            columnFilter.add(idx + 1);
            mapKeyTypes.add(projectedMetadata.getColumnType(idx));
        }
        ArrayColumnTypes sourceColumnTypes = new ArrayColumnTypes();
        for (int i = 0, m = projectedMetadata.getColumnCount(); i < m; i++) {
            sourceColumnTypes.add(projectedMetadata.getColumnType(i));
        }
        RecordSink sink = RecordSinkFactory.getInstance(configuration, asm, sourceColumnTypes, columnFilter, null);
        Map map = MapFactory.createOrderedMap(configuration, mapKeyTypes, anchorMapValueTypes());
        int returnType = anchorExpression.getType();
        int tag = ColumnType.tagOf(returnType);
        if (tag != ColumnType.TIMESTAMP && tag != ColumnType.LONG) {
            Misc.free(map);
            throw CairoException.nonCritical()
                    .put("live-view ANCHOR EXPRESSION must return TIMESTAMP or LONG, got ")
                    .put(ColumnType.nameOf(returnType));
        }
        return new LiveViewWindow(windowName, anchorExpression, returnType, map, sink, functions);
    }

    @Override
    public void close() {
        // The Map and RecordSink are exclusively owned by this object. The anchor
        // Function and the window-functions list are owned upstream
        // (LiveViewInstance and WindowRecordCursorFactory respectively); freeing
        // them here would double-free.
        Misc.free(anchorMap);
    }

    public ObjList<WindowFunction> getFunctions() {
        return functions;
    }

    /**
     * @return number of anchor-map entries currently marked tombstoned
     * (SLOT_TOMBSTONE == 1). Consumed by the Phase 2a.11 compaction trigger.
     */
    public long getTombstoneCount() {
        return tombstoneCount;
    }

    /**
     * @return the user-facing name of the WINDOW clause this object drives.
     * Phase 1 enforces a single anchored WINDOW per live view; the name is
     * persisted into the WINDOW_ANCHOR checkpoint block so future restores
     * can match by-name rather than by-position.
     */
    public String getWindowName() {
        return windowName;
    }

    /**
     * Initialises the anchor expression against {@code baseCursor} so that
     * bind variables, symbol tables, etc. resolve correctly for the rows this
     * window will process. Called once per refresh cycle by the wrapping cursor.
     */
    public void init(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        anchorExpression.init(baseCursor, executionContext);
    }

    /**
     * Drives the per-row anchor-comparison + reset-dispatch logic for one input row.
     * Must be invoked before the row reaches the underlying window cursor's
     * {@code computeNext}.
     */
    public void processRow(Record record) {
        MapKey key = anchorMap.withKey();
        key.put(record, partitionKeySink);
        MapValue value = key.createValue();

        long currentAnchor = readAnchorValue(record);
        boolean shouldReset;
        boolean isNewPartition = value.isNew();

        if (isNewPartition) {
            // First row for this partition — anchor map didn't carry it yet. Functions
            // either have no per-partition state yet (in which case resetPartition is
            // a no-op) or have stale state from a prior partition that was evicted —
            // resetting it is the safe default.
            // Tombstone is implicitly 0 (the Map zero-fills new value bytes); the new
            // partition is alive by definition since this row is creating it.
            shouldReset = true;
        } else {
            // Visiting an existing partition is evidence the partition is alive again -
            // clear any prior tombstone so the compaction trigger does not reclaim it.
            // RFC 123 §"Tombstone tracking and periodic compaction".
            if (value.getByte(SLOT_TOMBSTONE) == 1) {
                value.putByte(SLOT_TOMBSTONE, (byte) 0);
                tombstoneCount--;
            }
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
            // Tombstone semantics: an anchor crossing on an EXISTING partition resets
            // its accumulator to identity. We mark the entry as tombstoned, on the
            // assumption that the partition might go silent after the crossing. A
            // subsequent row visiting the partition clears the bit in the branch
            // above; if no row arrives within the residence window, the compaction
            // trigger (Phase 2a.11) reclaims the entry. Fresh partitions (isNew=true)
            // are not tombstoned - the first row that creates them is also reviving
            // them.
            if (!isNewPartition) {
                value.putByte(SLOT_TOMBSTONE, (byte) 1);
                tombstoneCount++;
            }
        }
    }

    /**
     * Clears the per-partition anchor map and re-initialises the anchor expression.
     * Mirrors {@link RecordCursor#toTop()}: the cursor restarts the underlying
     * source, so partitions that had been seen are about to be re-fed and need
     * a clean reset.
     */
    public void toTop() {
        anchorMap.clear();
        tombstoneCount = 0;
        anchorExpression.toTop();
    }

    private long readAnchorValue(Record record) {
        // Phase 1 supports TIMESTAMP and LONG anchor return types. Other primitive
        // types (INT, BOOLEAN, STRING, SYMBOL, DOUBLE) land with the rest of the
        // window-function migration; build() rejects them.
        if (ColumnType.tagOf(anchorValueType) == ColumnType.TIMESTAMP) {
            return anchorExpression.getTimestamp(record);
        }
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
            return 3;
        }

        @Override
        public int getColumnType(int columnIndex) {
            switch (columnIndex) {
                case SLOT_ANCHOR_VALUE:
                    return ColumnType.LONG;
                case SLOT_INITIALIZED:
                    return ColumnType.BYTE;
                case SLOT_TOMBSTONE:
                    return ColumnType.BYTE;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }
    }
}
