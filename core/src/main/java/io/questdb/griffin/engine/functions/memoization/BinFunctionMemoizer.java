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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.ByteList;

/**
 * Copies the bytes, since a binary sequence may compute each byte when it is read.
 */
public final class BinFunctionMemoizer extends BinFunction implements MemoizerFunction {
    private final ByteList bytes = new ByteList();
    private final Function fn;
    private final BinarySequence sequence = new BinarySequence() {
        @Override
        public byte byteAt(long index) {
            return bytes.getQuick((int) index);
        }

        @Override
        public long length() {
            return bytes.size();
        }
    };
    private boolean isNull;
    private boolean validValue;

    public BinFunctionMemoizer(Function fn) {
        this.fn = fn;
    }

    @Override
    public void clearMemo() {
        validValue = false;
    }

    @Override
    public Function getArg() {
        return fn;
    }

    @Override
    public BinarySequence getBin(Record rec) {
        memoize(rec);
        return isNull ? null : sequence;
    }

    @Override
    public long getBinLen(Record rec) {
        memoize(rec);
        return isNull ? TableUtils.NULL_LEN : bytes.size();
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
    public boolean supportsRandomAccess() {
        return fn.supportsRandomAccess();
    }

    private void memoize(Record rec) {
        if (!validValue) {
            final BinarySequence value = fn.getBin(rec);
            isNull = value == null;
            bytes.clear();
            for (long i = 0, n = isNull ? 0 : value.length(); i < n; i++) {
                bytes.add(value.byteAt(i));
            }
            validValue = true;
        }
    }
}
