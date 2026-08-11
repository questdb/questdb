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

package io.questdb.griffin.engine.functions.memoization;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;

public final class DecimalFunctionMemoizer extends DecimalFunction implements MemoizerFunction {
    private final Decimal128 decimal128Value = new Decimal128();
    private final Decimal256 decimal256Value = new Decimal256();
    private final Function fn;
    private boolean validValue;
    private long value;

    public DecimalFunctionMemoizer(Function fn) {
        super(fn.getType());
        this.fn = fn;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        if (!validValue) {
            fn.getDecimal128(rec, decimal128Value);
            validValue = true;
        }
        sink.copyRaw(decimal128Value);
    }

    @Override
    public short getDecimal16(Record rec) {
        if (!validValue) {
            value = fn.getDecimal16(rec);
            validValue = true;
        }
        return (short) value;
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        if (!validValue) {
            fn.getDecimal256(rec, decimal256Value);
            validValue = true;
        }
        sink.copyRaw(decimal256Value);
    }

    @Override
    public int getDecimal32(Record rec) {
        if (!validValue) {
            value = fn.getDecimal32(rec);
            validValue = true;
        }
        return (int) value;
    }

    @Override
    public long getDecimal64(Record rec) {
        if (!validValue) {
            value = fn.getDecimal64(rec);
            validValue = true;
        }
        return value;
    }

    @Override
    public byte getDecimal8(Record rec) {
        if (!validValue) {
            value = fn.getDecimal8(rec);
            validValue = true;
        }
        return (byte) value;
    }

    @Override
    public String getName() {
        return "memoize";
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        MemoizerFunction.super.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void memoize(Record record) {
        validValue = false;
    }

    @Override
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }
}
