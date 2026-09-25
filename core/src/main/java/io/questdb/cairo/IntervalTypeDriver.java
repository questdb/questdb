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

package io.questdb.cairo;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.functions.columns.IntervalColumn;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for INTERVAL.
 * <p>
 * INTERVAL is an in-memory value type that is never persisted; the width is that of
 * its two-long value, and it is NULL when both longs are LONG_NULL.
 */
public final class IntervalTypeDriver extends FixedSizeTypeDriver {
    public static final IntervalTypeDriver INSTANCE = new IntervalTypeDriver();

    private IntervalTypeDriver() {
        super(ColumnTypeTag.INTERVAL, 4);
    }

    /**
     * An interval type carries its timestamp precision; the bare tag is the raw interval.
     */
    @Override
    public ConstantFunction getNullConstant(int columnType) {
        if (columnType != ColumnType.INTERVAL) {
            return IntervalUtils.getTimestampDriverByIntervalType(columnType).getIntervalConstantNull();
        }
        return IntervalConstant.RAW_NULL;
    }

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.LONG_NULL;
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return IntervalColumn.newInstance(columnIndex, columnType);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putLong128(Numbers.LONG_NULL, Numbers.LONG_NULL);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryLong(addr, Numbers.LONG_NULL, count * 2);
    }
}
