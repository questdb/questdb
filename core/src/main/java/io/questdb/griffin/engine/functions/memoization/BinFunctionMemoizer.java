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
 * Reads the argument's sequence once per row and copies its bytes as they are first read, since a
 * binary sequence may compute each byte when it is read. Length and null-ness come from that same
 * sequence without reading a byte, so a row whose readers ask only for them generates and copies no
 * payload, and one whose readers stop at a prefix copies only the prefix. The deferred copy relies
 * on the argument's sequence staying unchanged until the argument is read again or the record
 * moves, the same lifetime the array memoizer relies on for its view.
 */
public final class BinFunctionMemoizer extends BinFunction implements MemoizerFunction {
    private final ByteList bytes = new ByteList();
    private final Function fn;
    private final BinarySequence sequence = new BinarySequence() {
        @Override
        public byte byteAt(long index) {
            if (index >= bytes.size()) {
                copyBytes(index);
            }
            return bytes.getQuick((int) index);
        }

        @Override
        public long length() {
            return length;
        }
    };
    private long length;
    private boolean validValue;
    private BinarySequence value;

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
        return value == null ? null : sequence;
    }

    @Override
    public long getBinLen(Record rec) {
        memoize(rec);
        return length;
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

    private void copyBytes(long hi) {
        for (long i = bytes.size(); i <= hi && i < length; i++) {
            bytes.add(value.byteAt(i));
        }
    }

    private void memoize(Record rec) {
        if (!validValue) {
            value = fn.getBin(rec);
            length = value == null ? TableUtils.NULL_LEN : value.length();
            bytes.clear();
            validValue = true;
        }
    }
}
