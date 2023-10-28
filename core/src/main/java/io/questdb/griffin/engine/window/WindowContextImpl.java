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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class WindowContextImpl implements WindowContext, Mutable {
    private boolean baseSupportsRandomAccess;
    private boolean empty = true;
    private int exclusionKind;

    private int exclusionKindPos;
    private int framingMode;
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

    @Override
    public boolean baseSupportsRandomAccess() {
        return baseSupportsRandomAccess;
    }

    @Override
    public void clear() {
        this.empty = true;
        this.partitionByRecord = null;
        this.partitionBySink = null;
        this.partitionByKeyTypes = null;
        this.ordered = false;
        this.orderByDirection = RecordCursorFactory.SCAN_DIRECTION_OTHER;
        this.orderByPos = 0;
        this.baseSupportsRandomAccess = false;
        this.framingMode = WindowColumn.FRAMING_ROWS;
        this.rowsLo = Long.MIN_VALUE;
        this.rowsHi = Long.MAX_VALUE;
        this.exclusionKind = WindowColumn.EXCLUDE_NO_OTHERS;
        this.rowsLoKindPos = 0;
        this.rowsHiKindPos = 0;
        this.exclusionKindPos = 0;
        this.timestampIndex = -1;
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
    public boolean isDefaultFrame() {
        // default mode is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
        // anything other than that is custom
        return framingMode == WindowColumn.FRAMING_RANGE
                && rowsLo == Long.MIN_VALUE
                && (rowsHi == 0 || rowsHi == Long.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
        return empty;
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
            boolean baseSupportsRandomAccess,
            int framingMode,
            long rowsLo,
            int rowsLoKindPos,
            long rowsHi,
            int rowsHiKindPos,
            int exclusionKind,
            int exclusionKindPos,
            int timestampIndex
    ) {
        this.empty = false;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.partitionByKeyTypes = partitionByKeyTypes;
        this.ordered = ordered;
        this.orderByDirection = orderByDirection;
        this.orderByPos = orderByPos;
        this.baseSupportsRandomAccess = baseSupportsRandomAccess;
        this.framingMode = framingMode;
        this.rowsLo = rowsLo;
        this.rowsLoKindPos = rowsLoKindPos;
        this.rowsHi = rowsHi;
        this.rowsHiKindPos = rowsHiKindPos;
        this.exclusionKind = exclusionKind;
        this.exclusionKindPos = exclusionKindPos;
        this.timestampIndex = timestampIndex;
    }
}
