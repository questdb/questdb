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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.DecimalFunction;

public abstract class Decimal16Function extends DecimalFunction implements Function {
    protected Decimal16Function(int type) {
        super(type);
    }

    @Override
    public final long getDecimal128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal256HH(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal256HL(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal256LH(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal256LL(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }
}
