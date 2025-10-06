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

package io.questdb.griffin.engine.functions.decimal;

import io.questdb.cairo.sql.Record;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimals;

public abstract class ToDecimal128Function extends Decimal128Function {
    protected final Decimal128 decimal = new Decimal128();
    private boolean isNull;

    public ToDecimal128Function(int targetType) {
        super(targetType);
    }

    @Override
    public long getDecimal128Hi(Record rec) {
        if (!store(rec)) {
            isNull = true;
            return Decimals.DECIMAL128_HI_NULL;
        }
        isNull = false;
        return decimal.getHigh();
    }

    @Override
    public long getDecimal128Lo(Record rec) {
        if (isNull) {
            return Decimals.DECIMAL128_LO_NULL;
        }
        return decimal.getLow();
    }

    /**
     * The implementation must fill the decimal with the store value following the target scale and precision.
     * If the value to store is null, it must return false without doing additional work.
     *
     * @return whether the result is not null.
     */
    protected abstract boolean store(Record rec);
}
