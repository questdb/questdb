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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for SYMBOL.
 * <p>
 * The data vector is a 4-byte symbol key; the symbol table is a separate facet.
 * {@link ColumnType#isFixedSize(int)} reports SYMBOL as not fixed-size; this driver only
 * states the data vector width. Writers wrap {@link #newNullAppender} to also raise the
 * symbol map's null flag.
 */
public final class SymbolTypeDriver extends FixedSizeTypeDriver {
    public static final SymbolTypeDriver INSTANCE = new SymbolTypeDriver();

    private SymbolTypeDriver() {
        super(ColumnTypeTag.SYMBOL, 2);
    }

    /**
     * The query engine parks a missing symbol as INT_NULL, not as the storage key
     * {@link SymbolTable#VALUE_IS_NULL}; both resolve to a null symbol. Kept as is.
     */
    @Override
    public long getNullAsLong() {
        return Numbers.INT_NULL;
    }

    @Override
    public ConstantFunction getNullConstant(int columnType) {
        return SymbolConstant.NULL;
    }

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.encodeLowHighInts(SymbolTable.VALUE_IS_NULL, SymbolTable.VALUE_IS_NULL);
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
    }

    /**
     * A symbol column function needs the symbol table (static or not) and, in a GROUP BY, the
     * map key slot; the callers that have them build it.
     */
    @Override
    public Function newColumnFunction(int columnIndex, int columnType) {
        throw new UnsupportedOperationException("SYMBOL column functions are built by the caller, which has the symbol table");
    }

    @Override
    public Runnable newNullAppender(MemoryA dataMem, MemoryA auxMem) {
        return () -> dataMem.putInt(SymbolTable.VALUE_IS_NULL);
    }

    @Override
    public void setNull(long addr, long count) {
        Vect.setMemoryInt(addr, SymbolTable.VALUE_IS_NULL, count);
    }
}
