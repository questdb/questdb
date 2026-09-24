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

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Vect;

/**
 * Type driver for DOUBLE.
 */
public final class DoubleTypeDriver extends FixedSizeTypeDriver {
    public static final DoubleTypeDriver INSTANCE = new DoubleTypeDriver();

    private DoubleTypeDriver() {
        super(ColumnTypeTag.DOUBLE, 3);
    }

    @Override
    public long getNullLong(int longIndex) {
        return Double.doubleToLongBits(Double.NaN);
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putDouble(Double.NaN);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryDouble(addr, Double.NaN, count);
    }
}
