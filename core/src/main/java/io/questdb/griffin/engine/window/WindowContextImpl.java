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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class WindowContextImpl implements WindowContext, Mutable {
    private boolean empty = true;
    private int exclusionKind;
    private int exclusionKindPos;
    private int framingMode;
    private boolean ignoreNulls;
    private boolean liveView;
    private int nullsDescPos;
    private int orderByDirection;
    private int orderByPos;
    private boolean ordered;
    private ColumnTypes partitionByKeyTypes;
    private VirtualRecord partitionByRecord;
    private RecordSink partitionBySink;
    private long rowsHi;
    private int rowsHiKindPos;
    private long rowsLo;
    private int rowsLoKindPos;
    private int timestampIndex;
    private int timestampType;

    @Override
    public void clear() {
        this.empty = true;
        this.partitionByRecord = null;
        this.partitionBySink = null;
        this.partitionByKeyTypes = null;
        this.ordered = false;
        this.orderByDirection = RecordCursorFactory.SCAN_DIRECTION_OTHER;
        this.orderByPos = 0;
        this.framingMode = WindowExpression.FRAMING_ROWS;
        this.rowsLo = Long.MIN_VALUE;
        this.rowsHi = Long.MAX_VALUE;
        this.exclusionKind = WindowExpression.EXCLUDE_NO_OTHERS;
        this.rowsLoKindPos = 0;
        this.rowsHiKindPos = 0;
        this.exclusionKindPos = 0;
        this.timestampIndex = -1;
        this.timestampType = ColumnType.UNDEFINED;
        this.ignoreNulls = false;
        this.liveView = false;
        this.nullsDescPos = 0;
    }

    public int getExclusionKind() {
        return exclusionKind;
    }

    @Override
    public int getExclusionKindPos() {
        return exclusionKindPos;
    }

    public int getFramingMode() {
        return framingMode;
    }

    @Override
    public int getNullsDescPos() {
        return nullsDescPos;
    }

    public int getOrderByPos() {
        return orderByPos;
    }

    @Override
    public ColumnTypes getPartitionByKeyTypes() {
        return partitionByKeyTypes;
    }

    @Override
    public VirtualRecord getPartitionByRecord() {
        return partitionByRecord;
    }

    @Override
    public RecordSink getPartitionBySink() {
        return partitionBySink;
    }

    public long getRowsHi() {
        if (exclusionKind == WindowExpression.EXCLUDE_CURRENT_ROW && rowsHi == 0) {
            return -1;
        }
        return rowsHi;
    }

    @Override
    public int getRowsHiKindPos() {
        return rowsHiKindPos;
    }

    public long getRowsLo() {
        return rowsLo;
    }

    @Override
    public int getRowsLoKindPos() {
        return rowsLoKindPos;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public int getTimestampType() {
        return timestampType;
    }

    @Override
    public boolean isDefaultFrame() {
        // default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
        // anything other than that is custom
        return framingMode == WindowExpression.FRAMING_RANGE
                && rowsLo == Long.MIN_VALUE
                && (rowsHi == 0 || rowsHi == Long.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public boolean isIgnoreNulls() {
        return ignoreNulls;
    }

    @Override
    public boolean isLiveView() {
        return liveView;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    public boolean isOrderedByDesignatedTimestamp() {
        return orderByDirection == RecordCursorFactory.SCAN_DIRECTION_FORWARD || orderByDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
    }

    public void of(
            VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes partitionByKeyTypes,
            boolean ordered,
            int orderByDirection,
            int orderByPos,
            int framingMode,
            long rowsLo,
            char rowsLoUint,
            int rowsLoKindPos,
            long rowsHi,
            char rowsHiUint,
            int rowsHiKindPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex,
            int timestampType,
            boolean ignoreNulls,
            int nullsDescPos
    ) throws SqlException {
        // Both bounds convert before any field is written: the caller clears the context after
        // the function it configures, not after a configuration that failed, so a rejected
        // bound must leave the context as it found it rather than half-configured.
        final long convertedRowsLo = rowsLoUint != 0 && ColumnType.isTimestamp(timestampType)
                ? toTimestampUnits(timestampType, rowsLo, rowsLoUint, rowsLoKindPos, "start")
                : rowsLo;
        final long convertedRowsHi = rowsHiUint != 0 && ColumnType.isTimestamp(timestampType)
                ? toTimestampUnits(timestampType, rowsHi, rowsHiUint, rowsHiKindPos, "end")
                : rowsHi;
        this.empty = false;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.partitionByKeyTypes = partitionByKeyTypes;
        this.ordered = ordered;
        this.orderByDirection = orderByDirection;
        this.orderByPos = orderByPos;
        this.framingMode = framingMode;
        this.rowsLo = convertedRowsLo;
        this.rowsLoKindPos = rowsLoKindPos;
        this.rowsHi = convertedRowsHi;
        this.rowsHiKindPos = rowsHiKindPos;
        this.exclusionKind = exclusionKind;
        this.exclusionKindPos = exclusionKindPos;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
        this.ignoreNulls = ignoreNulls;
        this.nullsDescPos = nullsDescPos;
        this.timestampType = timestampType;
    }

    public void setLiveView(boolean liveView) {
        this.liveView = liveView;
    }

    @Override
    public void validate(int position, boolean supportTNullsDesc) throws SqlException {
        if (isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (getNullsDescPos() > 0 && !supportTNullsDesc) {
            throw SqlException.$(getNullsDescPos(), "RESPECT/IGNORE NULLS is not supported for current window function");
        }

        if (!isDefaultFrame()) {
            if (rowsLo > 0) {
                throw SqlException.$(getRowsLoKindPos(), "frame start supports UNBOUNDED PRECEDING, _number_ PRECEDING and CURRENT ROW only");
            }
            if (rowsHi > 0) {
                if (rowsHi != Long.MAX_VALUE) {
                    throw SqlException.$(getRowsHiKindPos(), "frame end supports _number_ PRECEDING and CURRENT ROW only");
                } else if (rowsLo != Long.MIN_VALUE) {
                    throw SqlException.$(getRowsHiKindPos(), "frame end supports UNBOUNDED FOLLOWING only when frame start is UNBOUNDED PRECEDING");
                }
            }
        }

        int exclusionKind = getExclusionKind();
        int exclusionKindPos = getExclusionKindPos();
        if (exclusionKind != WindowExpression.EXCLUDE_NO_OTHERS
                && exclusionKind != WindowExpression.EXCLUDE_CURRENT_ROW) {
            throw SqlException.$(exclusionKindPos, "only EXCLUDE NO OTHERS and EXCLUDE CURRENT ROW exclusion modes are supported");
        }

        if (exclusionKind == WindowExpression.EXCLUDE_CURRENT_ROW) {
            // assumes frame doesn't use 'following'
            if (rowsHi == Long.MAX_VALUE) {
                throw SqlException.$(exclusionKindPos, "EXCLUDE CURRENT ROW not supported with UNBOUNDED FOLLOWING frame boundary");
            }
        }

        if (getFramingMode() == WindowExpression.FRAMING_GROUPS) {
            throw SqlException.$(position, "function not implemented for given window parameters");
        }
    }

    /**
     * Converts one RANGE frame bound from the time unit the user wrote into the designated
     * timestamp's native units, refusing a count the conversion cannot carry.
     * <p>
     * {@link TimestampDriver#from(long, char)} checks neither its multiply for overflow nor its
     * {@code int} narrowing for width, so a bound wider than the timestamp's units can hold comes
     * back as a different width - positive, or negative but far too small, or exactly zero - and
     * the frame the query evaluates is then not the one anybody wrote. {@link #validate(int,
     * boolean)} sees only the sign flip, and reports it as an unsupported frame start, naming a
     * cause the user did not write. The reject belongs here instead, at the bound's own position,
     * where it can name both the width that does not fit and the widest one that does.
     *
     * @param timestampType the designated timestamp type the frame is evaluated against
     * @param bound         the frame bound, negated for PRECEDING, in {@code unit}s
     * @param unit          the time unit the bound is written in
     * @param position      the SQL position of the bound
     * @param boundName     "start" or "end", for the message
     * @return the bound in the designated timestamp's native units
     * @throws SqlException if the bound is too wide for the designated timestamp's units
     */
    private static long toTimestampUnits(
            int timestampType,
            long bound,
            char unit,
            int position,
            CharSequence boundName
    ) throws SqlException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final long maxUnitValue = driver.getMaxUnitValue(unit);
        if (bound < -maxUnitValue || bound > maxUnitValue) {
            // A PRECEDING bound arrives negated, and the widest count the parser accepts,
            // Long.MAX_VALUE, negates onto Long.MIN_VALUE - which negates back onto itself.
            final long width = bound == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bound);
            throw SqlException.$(position, "RANGE frame ").put(boundName)
                    .put(" is out of range for the designated timestamp [width=").put(width).put(unit)
                    .put(", max=").put(maxUnitValue).put(unit)
                    .put(']');
        }
        return driver.from(bound, unit);
    }
}
