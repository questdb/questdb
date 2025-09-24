/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.Interval;
import org.jetbrains.annotations.NotNull;

public class IntervalTypeConstant extends IntervalFunction implements TypeConstant, FunctionExtension {
    public static final IntervalTypeConstant RAW_INSTANCE = new IntervalTypeConstant(ColumnType.INTERVAL_RAW);
    public static final IntervalTypeConstant TIMESTAMP_MICRO_INSTANCE = new IntervalTypeConstant(ColumnType.INTERVAL_TIMESTAMP_MICRO);
    public static final IntervalTypeConstant TIMESTAMP_NANO_INSTANCE = new IntervalTypeConstant(ColumnType.INTERVAL_TIMESTAMP_NANO);

    protected IntervalTypeConstant(int intervalType) {
        super(intervalType);
    }

    @Override
    public FunctionExtension extendedOps() {
        return this;
    }

    @Override
    public int getArrayLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return Interval.NULL;
    }

    @Override
    public Record getRecord(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }
}
