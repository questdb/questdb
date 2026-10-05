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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.bool.SymbolKeyBitSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * The membership set of a JIT-compiled {@code symbol IN (...)} list. It is not evaluated as a
 * function: it rides in the compiled filter's bind variable list so that {@code init()} resolves
 * the list's values to symbol keys once per execution, against the symbol table the cursor reads,
 * and {@code AsyncFilterUtils.prepareBindVarMemory} then hands the backend the resulting bitset.
 * <p>
 * The bind variable slot holds the bitset's address in its low eight bytes and the index of its
 * last bit in the high eight; the bit layout is {@link SymbolKeyBitSet}'s - bit 0 is NULL, bit
 * {@code k + 1} is key {@code k}. The backend reads the words as 32-bit little-endian halves, and
 * a key whose bit index is past the last bit tests false, so a symbol appended after resolution
 * is never mistaken for a member.
 * <p>
 * Resolving per execution rather than at compile time is what keeps a cached factory correct
 * across bind variable rebinding, and across symbols the table gains between executions: a
 * literal absent at compile time resolves once it exists.
 */
public class CompiledFilterSymbolInBitSet extends BooleanFunction {
    private final int columnIndex;
    // Literal values; a null element stands for NULL.
    private final ObjList<String> constants;
    // Bind variables, read at init() time.
    private final ObjList<Function> variables;
    private long address;
    private long allocatedSize;
    private int maxBitIndex;

    public CompiledFilterSymbolInBitSet(int columnIndex, ObjList<String> constants, ObjList<Function> variables) {
        this.columnIndex = columnIndex;
        this.constants = constants;
        this.variables = variables;
    }

    /**
     * Whether a bind variable of this type can be an element of a symbol IN list the bitset
     * evaluates, mirroring the types {@code InSymbolFunctionFactory} accepts.
     */
    public static boolean isSupportedVariableType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.CHAR -> true;
            default -> false;
        };
    }

    @Override
    public void close() {
        // The serializer created these link functions for this set alone.
        for (int i = 0, n = variables.size(); i < n; i++) {
            variables.getQuick(i).close();
        }
        if (address != 0) {
            address = Unsafe.free(address, allocatedSize, MemoryTag.NATIVE_FUNC_RSS);
            allocatedSize = 0;
        }
    }

    public long getAddress() {
        return address;
    }

    @Override
    public boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    public int getMaxBitIndex() {
        return maxBitIndex;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = variables.size(); i < n; i++) {
            variables.getQuick(i).init(symbolTableSource, executionContext);
        }
        final StaticSymbolTable symbolTable = (StaticSymbolTable) symbolTableSource.getSymbolTable(columnIndex);

        int maxKey = -1;
        for (int i = 0, n = constants.size(); i < n; i++) {
            maxKey = Math.max(maxKey, symbolTable.keyOf(constants.getQuick(i)));
        }
        for (int i = 0, n = variables.size(); i < n; i++) {
            maxKey = Math.max(maxKey, symbolTable.keyOf(variableValue(variables.getQuick(i))));
        }

        final long bitCount = SymbolKeyBitSet.bitsFor(maxKey);
        final long size = ((bitCount + 63) >>> 6) << 3;
        if (size > allocatedSize) {
            address = Unsafe.realloc(address, allocatedSize, size, MemoryTag.NATIVE_FUNC_RSS);
            allocatedSize = size;
        }
        Vect.memset(address, size, 0);
        maxBitIndex = (int) (bitCount - 1);

        for (int i = 0, n = constants.size(); i < n; i++) {
            setBit(symbolTable.keyOf(constants.getQuick(i)));
        }
        for (int i = 0, n = variables.size(); i < n; i++) {
            setBit(symbolTable.keyOf(variableValue(variables.getQuick(i))));
        }
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isStableWithinExecution() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("?::symbol_set");
    }

    private static CharSequence variableValue(Function func) {
        // CHAR-typed variables don't expose getStrA, see InSymbolFunctionFactory.
        if (ColumnType.tagOf(func.getType()) == ColumnType.CHAR) {
            final char c = func.getChar(null);
            return c != 0 ? String.valueOf(c) : null;
        }
        return func.getStrA(null);
    }

    private void setBit(int key) {
        if (key >= 0 || key == SymbolTable.VALUE_IS_NULL) {
            final int idx = SymbolKeyBitSet.bitIndex(key);
            final long wordAddress = address + ((long) (idx >>> 6) << 3);
            Unsafe.getUnsafe().putLong(wordAddress, Unsafe.getUnsafe().getLong(wordAddress) | (1L << idx));
        }
    }
}
