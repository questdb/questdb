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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps a base cursor and emits only the rows selected by a downsampling
 * algorithm (LTTB, M4, MinMax).
 * <p>
 * Two execution paths based on the base factory's capabilities:
 * <ul>
 *   <li><b>Fast path</b> (useDirectAccess=true): for forward-scan factories that
 *       support random access and have a designated timestamp. Buffers only
 *       [rowId(8), timestamp(8), value(8)] = 24 bytes per row. Emits selected
 *       rows via base.recordAt(). No RecordChain, no heap allocation on the
 *       hot path.</li>
 *   <li><b>Fallback path</b> (useDirectAccess=false): for aggregate cursors
 *       (SAMPLE BY, GROUP BY) that lose timestamp designation or don't support
 *       stable random access. Materializes full rows into a RecordChain plus
 *       [chainOffset(8), timestamp(8), value(8)] = 24 bytes per row. Memory
 *       scales with row width.</li>
 * </ul>
 * <p>
 * <b>Important:</b> All algorithms assume the input is ordered by the
 * designated timestamp in ascending order. The fast path requires
 * SCAN_DIRECTION_FORWARD. The fallback path sorts by timestamp after
 * buffering (async cursors may deliver rows out of order).
 */
public class SubsampleRecordCursorFactory extends AbstractRecordCursorFactory {

    public static final int METHOD_LTTB = 0;
    public static final int METHOD_M4 = 1;
    public static final int METHOD_MINMAX = 2;

    private final RecordCursorFactory base;
    private final SubsampleRecordCursor cursor;
    private final int subsamplePosition;
    private final Function targetFunc;
    private final int targetType;

    public SubsampleRecordCursorFactory(
            @Nullable CairoConfiguration configuration,
            RecordCursorFactory base,
            int method,
            Function targetFunc,
            int valueColumnIndex,
            int timestampColumnIndex,
            int subsamplePosition,
            long gapThresholdMicros,
            long maxRows,
            int valueColumnType,
            @Nullable RecordSink recordSink,
            boolean useDirectAccess
    ) {
        super(base.getMetadata());
        this.base = base;
        this.targetFunc = targetFunc;
        this.targetType = ColumnType.tagOf(targetFunc.getType());
        this.subsamplePosition = subsamplePosition;
        this.cursor = new SubsampleRecordCursor(
                configuration, method, valueColumnIndex,
                timestampColumnIndex, subsamplePosition,
                gapThresholdMicros, maxRows, valueColumnType,
                useDirectAccess,
                useDirectAccess ? null : base.getMetadata(),
                recordSink
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        targetFunc.init(null, executionContext);
        final int targetPoints = getTargetPoints();

        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext, targetPoints);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        // SUBSAMPLE output is always timestamp-ascending: fast path requires
        // forward scan, fallback path sorts the buffer ascending.
        return SCAN_DIRECTION_FORWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Subsample");
        sink.attr("method").val(cursor.method == METHOD_LTTB ? "lttb" : cursor.method == METHOD_M4 ? "m4" : "minmax");
        sink.attr("points").val(targetFunc);
        sink.child(base);
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(targetFunc);
        cursor.destroy();
    }

    private int getTargetPoints() throws SqlException {
        long value;
        if (targetType == ColumnType.LONG) {
            value = targetFunc.getLong(null);
            if (value == Numbers.LONG_NULL) {
                throw SqlException.$(subsamplePosition, "target point count must be set");
            }
        } else {
            int intVal = targetFunc.getInt(null);
            if (intVal == Numbers.INT_NULL) {
                throw SqlException.$(subsamplePosition, "target point count must be set");
            }
            value = intVal;
        }
        if (value < 2) {
            throw SqlException.$(subsamplePosition, "target points must be at least 2");
        }
        if (value > Integer.MAX_VALUE) {
            throw SqlException.$(subsamplePosition, "target points exceeds maximum of ").put(Integer.MAX_VALUE);
        }
        return (int) value;
    }

    private static class SubsampleRecordCursor implements RecordCursor {
        private static final int ENTRY_SIZE = 24; // 8 bytes payload + 8 bytes timestamp + 8 bytes value
        private static final int INITIAL_CAPACITY = 1024;

        private final SubsampleAlgorithm algorithm;
        // Fallback path only: materializes full rows
        private final RecordChain chain;
        private final long maxRows;
        private final int method;
        private final DirectLongList selectedIndices;
        private final int subsamplePosition;
        private final int timestampColumnIndex;
        private final boolean useDirectAccess;
        private final int valueColumnIndex;
        private final int valueColumnType;
        private RecordCursor base;
        // Native buffer: [payload(8), timestamp(8), value(8)] per entry.
        // payload = rowId (fast path) or chainOffset (fallback path).
        private long buffer;
        private long bufferCapacity; // in entries
        private long bufferSize; // in entries
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isBuffered;
        private boolean isOpen;
        private boolean isSorted;
        // Fast path only: the base cursor's record, positioned via recordAt()
        private Record record;
        private long selectedCount;
        private long selectedIndex;
        private int targetPoints;

        private SubsampleRecordCursor(
                @Nullable CairoConfiguration configuration,
                int method,
                int valueColumnIndex,
                int timestampColumnIndex,
                int subsamplePosition,
                long gapThresholdMicros,
                long maxRows,
                int valueColumnType,
                boolean useDirectAccess,
                @Nullable io.questdb.cairo.sql.RecordMetadata metadata,
                @Nullable RecordSink recordSink
        ) {
            this.method = method;
            this.valueColumnIndex = valueColumnIndex;
            this.timestampColumnIndex = timestampColumnIndex;
            this.subsamplePosition = subsamplePosition;
            if (maxRows < 1 || maxRows > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().position(subsamplePosition)
                        .put("cairo.sql.subsample.max.rows must be between 1 and ")
                        .put(Integer.MAX_VALUE);
            }
            this.maxRows = maxRows;
            this.valueColumnType = valueColumnType;
            this.useDirectAccess = useDirectAccess;
            this.algorithm = switch (method) {
                case METHOD_LTTB -> new LttbAlgorithm(gapThresholdMicros);
                case METHOD_M4 -> M4Algorithm.INSTANCE;
                case METHOD_MINMAX -> MinMaxAlgorithm.INSTANCE;
                default -> throw new IllegalArgumentException("unknown method: " + method);
            };
            this.selectedIndices = new DirectLongList(INITIAL_CAPACITY, MemoryTag.NATIVE_FUNC_RSS);
            if (useDirectAccess) {
                this.chain = null;
            } else {
                this.chain = new RecordChain(
                        metadata,
                        recordSink,
                        configuration.getSqlSortValuePageSize(),
                        configuration.getSqlSortValueMaxPages()
                );
            }
            this.buffer = 0;
            this.bufferCapacity = 0;
            this.isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                base = Misc.free(base);
                if (chain != null) {
                    chain.clear();
                }
                freeBuffer();
                isOpen = false;
            }
        }

        void destroy() {
            if (isOpen) {
                close();
            }
            freeBuffer();
            selectedIndices.close();
            Misc.free(chain);
            if (algorithm instanceof LttbAlgorithm lttb) {
                lttb.close();
            }
        }

        @Override
        public Record getRecord() {
            if (useDirectAccess) {
                return record;
            }
            return chain.getRecord();
        }

        @Override
        public Record getRecordB() {
            if (useDirectAccess) {
                return base != null ? base.getRecordB() : null;
            }
            return chain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base != null ? base.getSymbolTable(columnIndex) : null;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base != null ? base.newSymbolTable(columnIndex) : null;
        }

        @Override
        public long preComputedStateSize() {
            return isBuffered ? selectedCount : 0;
        }

        @Override
        public boolean hasNext() {
            if (!isBuffered) {
                bufferAndSelect();
                isBuffered = true;
            }
            if (selectedIndex >= selectedCount) {
                return false;
            }
            long bufferIdx = selectedIndices.get(selectedIndex);
            selectedIndex++;
            long payload = getBufferedPayload(bufferIdx);
            if (useDirectAccess) {
                // Fast path: position via base cursor's recordAt()
                base.recordAt(record, payload);
            } else {
                // Fallback: position via chain
                chain.recordAt(chain.getRecord(), payload);
            }
            return true;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (useDirectAccess) {
                if (base != null) {
                    base.recordAt(record, atRowId);
                }
            } else {
                chain.recordAt(record, atRowId);
            }
        }

        @Override
        public long size() {
            return isBuffered ? selectedCount : -1;
        }

        @Override
        public void toTop() {
            if (isBuffered) {
                selectedIndex = 0;
            }
        }

        int getSubsamplePosition() {
            return subsamplePosition;
        }

        void of(RecordCursor baseCursor, SqlExecutionContext executionContext, int targetPoints) {
            if (!isOpen) {
                isOpen = true;
            }
            this.base = baseCursor;
            this.record = baseCursor.getRecord();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.targetPoints = targetPoints;
            this.isBuffered = false;
            this.selectedIndex = 0;
            this.selectedCount = 0;
            this.bufferSize = 0;
            if (chain != null) {
                chain.setSymbolTableResolver(baseCursor);
            }
        }

        private void bufferAndSelect() {
            bufferInput();
            // Sort only fallback path when timestamps are not monotonically ascending.
            // Fast path requires SCAN_DIRECTION_FORWARD (guaranteed ascending).
            // Fallback tracks monotonicity during buffering; if isSorted, skip sort.
            if (!useDirectAccess && bufferSize > 1 && !isSorted) {
                nativeSortBufferByTimestamp();
            }
            if (bufferSize <= targetPoints) {
                selectAll();
            } else {
                algorithm.select(buffer, (int) bufferSize, targetPoints, selectedIndices, circuitBreaker);
                selectedCount = selectedIndices.size();
            }
        }

        private void bufferInput() {
            bufferSize = 0;
            isSorted = true;
            long prevTs = Long.MIN_VALUE;
            if (chain != null) {
                chain.clear();
            }
            if (bufferCapacity == 0) {
                bufferCapacity = INITIAL_CAPACITY;
                buffer = Unsafe.malloc(bufferCapacity * ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
            }
            final Record baseRecord = base.getRecord();
            while (base.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long ts = baseRecord.getTimestamp(timestampColumnIndex);
                double value = getValueAsDouble(baseRecord);
                if (Double.isNaN(value)) {
                    continue; // NaN-filtered rows don't affect monotonicity
                }
                // Track monotonicity for buffered rows only (after NaN filter)
                if (ts < prevTs) {
                    isSorted = false;
                }
                prevTs = ts;
                if (bufferSize >= maxRows) {
                    throw CairoException.nonCritical().position(subsamplePosition)
                            .put("SUBSAMPLE input exceeds maximum of ")
                            .put(maxRows).put(" rows");
                }
                // Compute payload: rowId for fast path, chainOffset for fallback
                long payload;
                if (useDirectAccess) {
                    payload = baseRecord.getRowId();
                } else {
                    payload = chain.put(baseRecord, -1);
                }
                // Grow buffer if needed
                if (bufferSize >= bufferCapacity) {
                    long newCapacity = Numbers.ceilPow2(bufferSize + 1);
                    if (newCapacity < bufferSize + 1) {
                        throw CairoException.nonCritical().position(subsamplePosition)
                                .put("SUBSAMPLE buffer capacity overflow");
                    }
                    buffer = Unsafe.realloc(buffer, bufferCapacity * ENTRY_SIZE, newCapacity * ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
                    bufferCapacity = newCapacity;
                }
                long offset = bufferSize * ENTRY_SIZE;
                Unsafe.getUnsafe().putLong(buffer + offset, payload);
                Unsafe.getUnsafe().putLong(buffer + offset + 8, ts);
                Unsafe.getUnsafe().putDouble(buffer + offset + 16, value);
                bufferSize++;
            }
        }

        private void freeBuffer() {
            if (buffer != 0) {
                Unsafe.free(buffer, bufferCapacity * ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
                buffer = 0;
                bufferCapacity = 0;
                bufferSize = 0;
            }
        }

        private long getBufferedPayload(long index) {
            return Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE);
        }

        private double getValueAsDouble(Record rec) {
            return switch (valueColumnType) {
                case ColumnType.DOUBLE -> rec.getDouble(valueColumnIndex);
                case ColumnType.FLOAT -> rec.getFloat(valueColumnIndex);
                case ColumnType.INT -> {
                    int v = rec.getInt(valueColumnIndex);
                    yield v != Numbers.INT_NULL ? (double) v : Double.NaN;
                }
                case ColumnType.LONG -> {
                    long v = rec.getLong(valueColumnIndex);
                    yield v != Numbers.LONG_NULL ? (double) v : Double.NaN;
                }
                case ColumnType.SHORT -> rec.getShort(valueColumnIndex);
                case ColumnType.BYTE -> rec.getByte(valueColumnIndex);
                default -> rec.getDouble(valueColumnIndex);
            };
        }

        private void selectAll() {
            selectedIndices.clear();
            for (long i = 0; i < bufferSize; i++) {
                selectedIndices.add(i);
            }
            selectedCount = bufferSize;
        }

        /**
         * Sort the 24-byte buffer entries by timestamp using native quicksort.
         * <p>
         * Uses {@code Vect.quickSortLongIndexAscInPlace} which sorts
         * {@code index_t = {uint64_t ts, uint64_t i}} pairs by ts as unsigned.
         * To handle signed timestamps (negative = pre-1970), the sort key is
         * stored as {@code ts ^ Long.MIN_VALUE}, which maps signed ordering to
         * unsigned ordering. The first 8 bytes of each buffer entry (payload)
         * are treated as opaque and carried along during reordering.
         */
        private void nativeSortBufferByTimestamp() {
            final int n = (int) bufferSize;
            final long indexSize = 16L * n;
            final long workspaceSize = (long) ENTRY_SIZE * n;
            long indexAddr = 0;
            long workspaceAddr = 0;
            try {
                indexAddr = Unsafe.malloc(indexSize, MemoryTag.NATIVE_FUNC_RSS);
                // Populate index: (sortableTimestamp, originalBufferIndex)
                for (int i = 0; i < n; i++) {
                    long ts = Unsafe.getUnsafe().getLong(buffer + (long) i * ENTRY_SIZE + 8);
                    long indexOff = (long) i * 16;
                    Unsafe.getUnsafe().putLong(indexAddr + indexOff, ts ^ Long.MIN_VALUE);
                    Unsafe.getUnsafe().putLong(indexAddr + indexOff + 8, i);
                }
                // Native O(N log N) quicksort by the sortable key
                Vect.quickSortLongIndexAscInPlace(indexAddr, n);
                // Reorder buffer entries into workspace in sorted order
                workspaceAddr = Unsafe.malloc(workspaceSize, MemoryTag.NATIVE_FUNC_RSS);
                for (int i = 0; i < n; i++) {
                    long origIdx = Unsafe.getUnsafe().getLong(indexAddr + (long) i * 16 + 8);
                    long srcOff = origIdx * ENTRY_SIZE;
                    long dstOff = (long) i * ENTRY_SIZE;
                    Unsafe.getUnsafe().putLong(workspaceAddr + dstOff, Unsafe.getUnsafe().getLong(buffer + srcOff));
                    Unsafe.getUnsafe().putLong(workspaceAddr + dstOff + 8, Unsafe.getUnsafe().getLong(buffer + srcOff + 8));
                    Unsafe.getUnsafe().putLong(workspaceAddr + dstOff + 16, Unsafe.getUnsafe().getLong(buffer + srcOff + 16));
                }
                // Copy sorted entries back to buffer (preserves buffer pointer/capacity)
                Unsafe.getUnsafe().copyMemory(workspaceAddr, buffer, workspaceSize);
            } finally {
                if (indexAddr != 0) {
                    Unsafe.free(indexAddr, indexSize, MemoryTag.NATIVE_FUNC_RSS);
                }
                if (workspaceAddr != 0) {
                    Unsafe.free(workspaceAddr, workspaceSize, MemoryTag.NATIVE_FUNC_RSS);
                }
            }
        }
    }
}
