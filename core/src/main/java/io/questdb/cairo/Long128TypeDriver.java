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
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Long128Constant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for LONG128.
 * <p>
 * A LONG128 is NULL when both longs are LONG_NULL.
 */
public final class Long128TypeDriver extends FixedSizeTypeDriver {
    public static final Long128TypeDriver INSTANCE = new Long128TypeDriver();

    private Long128TypeDriver() {
        super(ColumnTypeTag.LONG128, 4);
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return Long128Constant.NULL;
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
        return Long128Column.newInstance(columnIndex);
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
