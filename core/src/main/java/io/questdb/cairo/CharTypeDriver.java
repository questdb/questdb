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
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.Vect;

/**
 * Type driver for CHAR.
 */
public final class CharTypeDriver extends FixedSizeTypeDriver {
    public static final CharTypeDriver INSTANCE = new CharTypeDriver();

    private CharTypeDriver() {
        super(ColumnTypeTag.CHAR, 1);
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return CharConstant.ZERO;
    }

    @Override
    public long getNullLong(int longIndex) {
        return 0L;
    }

    @Override
    public boolean hasNullSentinel() {
        return false;
    }

    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        return new CharColumn(columnIndex);
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putChar((char) 0);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryShort(addr, (short) 0, count);
    }
}
