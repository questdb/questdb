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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * Wraps a base cursor and emits only the rows selected by a downsampling
 * algorithm (LTTB, M4).
 * <p>
 * When the base cursor supports random access (direct table scans), uses a
 * single-pass approach: buffers (rowId, timestamp, value) triples, runs the
 * algorithm, then emits selected rows via recordAt(). When random access is
 * not available (SAMPLE BY results), uses a two-pass approach: buffers data,
 * runs the algorithm, then rewinds via toTop() and scans sequentially to emit.
 * <p>
 * Native buffer layout per entry:
 * [rowId: long (8 bytes)][timestamp: long (8 bytes)][value: double (8 bytes)] = 24 bytes
 */
public class SubsampleRecordCursorFactory extends AbstractRecordCursorFactory {

    public static final int METHOD_LTTB = 0;
    public static final int METHOD_M4 = 1;
    public static final int METHOD_MINMAX = 2;

    private final RecordCursorFactory base;
    private final SubsampleRecordCursor cursor;
    private final Function targetFunc;

    /**
     * @param base                 the base cursor factory to downsample
     * @param method               algorithm: {@link #METHOD_LTTB} or {@link #METHOD_M4}
     * @param targetFunc           function evaluating to the target number of output points
     * @param valueColumnIndex     index of the numeric column used for visual significance
     * @param timestampColumnIndex index of the designated timestamp column
     * @param subsamplePosition    SQL position of the SUBSAMPLE clause for error reporting
     * @param gapThresholdMicros   gap threshold in microseconds for gap-preserving LTTB (0 = disabled)
     * @param maxRows              maximum input rows before throwing (from configuration)
     */
    public SubsampleRecordCursorFactory(
            RecordCursorFactory base,
            int method,
            Function targetFunc,
            int valueColumnIndex,
            int timestampColumnIndex,
            int subsamplePosition,
            long gapThresholdMicros,
            long maxRows
    ) {
        super(base.getMetadata());
        this.base = base;
        this.targetFunc = targetFunc;
        this.cursor = new SubsampleRecordCursor(
                method, valueColumnIndex,
                timestampColumnIndex, subsamplePosition,
                gapThresholdMicros,
                base.recordCursorSupportsRandomAccess(),
                maxRows
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Evaluate target point count (supports bind variables)
        targetFunc.init(null, executionContext);
        final int targetPoints = targetFunc.getInt(null);
        if (targetPoints < 2) {
            throw SqlException.$(cursor.getSubsamplePosition(), "target points must be at least 2");
        }

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
        return base.getScanDirection();
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

    private static class SubsampleRecordCursor implements RecordCursor {
        private static final int ENTRY_SIZE = 24; // 8 bytes rowId + 8 bytes timestamp + 8 bytes value
        private static final int INITIAL_CAPACITY = 1024;

        private final SubsampleAlgorithm algorithm;
        private final long maxRows;
        private final int method;
        private final DirectLongList selectedIndices;
        private final int subsamplePosition;
        private final boolean supportsRandomAccess;
        private final int timestampColumnIndex;
        private final int valueColumnIndex;
        private RecordCursor base;
        private long buffer;
        private long bufferCapacity; // in entries
        private long bufferSize; // in entries
        private SqlExecutionCircuitBreaker circuitBreaker;
        private long currentBaseRow = -1;
        private boolean isBuffered;
        private Record record;
        private long selectedCount;
        private long selectedIndex;
        private int targetPoints;

        private SubsampleRecordCursor(
                int method,
                int valueColumnIndex,
                int timestampColumnIndex,
                int subsamplePosition,
                long gapThresholdMicros,
                boolean supportsRandomAccess,
                long maxRows
        ) {
            this.method = method;
            this.valueColumnIndex = valueColumnIndex;
            this.timestampColumnIndex = timestampColumnIndex;
            this.subsamplePosition = subsamplePosition;
            this.supportsRandomAccess = supportsRandomAccess;
            this.maxRows = maxRows;
            this.algorithm = switch (method) {
                case METHOD_LTTB -> new LttbAlgorithm(gapThresholdMicros);
                case METHOD_M4 -> M4Algorithm.INSTANCE;
                case METHOD_MINMAX -> MinMaxAlgorithm.INSTANCE;
                default -> throw new IllegalArgumentException("unknown method: " + method);
            };
            this.selectedIndices = new DirectLongList(INITIAL_CAPACITY, MemoryTag.NATIVE_FUNC_RSS);
            this.buffer = 0;
            this.bufferCapacity = 0;
        }

        @Override
        public void close() {
            base = Misc.free(base);
            freeBuffer();
            // Don't close selectedIndices here - the cursor instance is reused
            // across getCursor() calls. DirectLongList is cleared in bufferAndSelect().
        }

        void destroy() {
            // Called from factory._close() for final cleanup
            freeBuffer();
            selectedIndices.close();
        }

        int getSubsamplePosition() {
            return subsamplePosition;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            return base != null ? base.getRecordB() : null;
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
            // Skip rows in the base cursor to reach the next selected index
            return advanceToSelectedRow();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            if (base != null) {
                base.recordAt(record, atRowId);
            }
        }

        @Override
        public long size() {
            return isBuffered ? selectedCount : -1;
        }

        @Override
        public void toTop() {
            if (isBuffered) {
                // Reset output iteration but keep the selected indices
                selectedIndex = 0;
                if (!supportsRandomAccess) {
                    base.toTop();
                    currentBaseRow = -1;
                }
            }
        }

        private boolean advanceToSelectedRow() {
            long bufferIdx = selectedIndices.get(selectedIndex);
            selectedIndex++;
            long rowIdentifier = getBufferedRowIndex(bufferIdx);

            if (supportsRandomAccess) {
                // Single-pass: jump directly to the selected row via rowId
                base.recordAt(record, rowIdentifier);
                return true;
            } else {
                // Two-pass: advance sequentially to the target row
                while (currentBaseRow < rowIdentifier) {
                    if (!base.hasNext()) {
                        return false;
                    }
                    currentBaseRow++;
                }
                return true;
            }
        }

        private void bufferAndSelect() {
            // Pass 1: read all input into native buffer
            bufferInput();
            // Run the downsampling algorithm
            if (bufferSize <= targetPoints) {
                // No downsampling needed - select all rows
                selectAll();
            } else {
                algorithm.select(buffer, (int) bufferSize, targetPoints, selectedIndices, circuitBreaker);
                selectedCount = selectedIndices.size();
            }
            // For sequential access, rewind base cursor for pass 2.
            // For random access, no rewind needed - we use recordAt().
            if (!supportsRandomAccess) {
                base.toTop();
                currentBaseRow = -1;
            }
        }

        private void bufferInput() {
            bufferSize = 0;
            long baseRow = 0;
            if (bufferCapacity == 0) {
                bufferCapacity = INITIAL_CAPACITY;
                buffer = Unsafe.malloc(bufferCapacity * ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
            }
            while (base.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                long ts = record.getTimestamp(timestampColumnIndex);
                double value = record.getDouble(valueColumnIndex);
                // For random-access bases, store the actual rowId for recordAt().
                // For sequential bases, store a sequential counter for the two-pass scan.
                long rowIdentifier = supportsRandomAccess ? record.getRowId() : baseRow;
                baseRow++;
                // Skip NaN values - don't buffer them
                if (Double.isNaN(value)) {
                    continue;
                }
                // Grow buffer if needed
                if (bufferSize >= bufferCapacity) {
                    if (bufferSize >= maxRows) {
                        throw CairoException.nonCritical().position(subsamplePosition)
                                .put("SUBSAMPLE input exceeds maximum of ")
                                .put(maxRows).put(" rows");
                    }
                    long newCapacity = Numbers.ceilPow2(bufferSize + 1);
                    if (newCapacity < bufferSize + 1) {
                        throw CairoException.nonCritical().position(subsamplePosition)
                                .put("SUBSAMPLE buffer capacity overflow");
                    }
                    buffer = Unsafe.realloc(buffer, bufferCapacity * ENTRY_SIZE, newCapacity * ENTRY_SIZE, MemoryTag.NATIVE_FUNC_RSS);
                    bufferCapacity = newCapacity;
                }
                long offset = bufferSize * ENTRY_SIZE;
                Unsafe.getUnsafe().putLong(buffer + offset, rowIdentifier);
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

        private long getBufferedRowIndex(long index) {
            return Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE);
        }

        private double getBufferedTimestamp(long index) {
            return (double) Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE + 8);
        }

        private double getBufferedValue(long index) {
            return Unsafe.getUnsafe().getDouble(buffer + index * ENTRY_SIZE + 16);
        }

        void of(RecordCursor baseCursor, SqlExecutionContext executionContext, int targetPoints) {
            this.base = baseCursor;
            this.record = baseCursor.getRecord();
            this.circuitBreaker = executionContext.getCircuitBreaker();
            this.targetPoints = targetPoints;
            this.isBuffered = false;
            this.selectedIndex = 0;
            this.selectedCount = 0;
            this.currentBaseRow = -1;
            this.bufferSize = 0;
        }

        private void selectAll() {
            selectedIndices.clear();
            for (long i = 0; i < bufferSize; i++) {
                selectedIndices.add(i);
            }
            selectedCount = bufferSize;
        }
    }
}
