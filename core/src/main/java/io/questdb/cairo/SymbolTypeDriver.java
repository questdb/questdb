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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.constants.SymbolConstant;
import io.questdb.griffin.engine.functions.constants.SymbolTypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.Vect;

/**
 * Type driver for SYMBOL.
 * <p>
 * The data vector is a 4-byte symbol key; the symbol table is a separate facet.
 * {@link ColumnType#isFixedSize(int)} reports SYMBOL as not fixed-size; this driver only
 * states the data vector width. The table and WAL writers write a SYMBOL NULL themselves
 * ({@code TableWriter} and {@code WalWriter} build its appender): the NULL key and the symbol
 * map's NULL flag together. {@link #newNullAppender} refuses, since an appender that wrote the
 * key alone would leave the flag unset.
 */
public final class SymbolTypeDriver extends FixedSizeTypeDriver {
    public static final SymbolTypeDriver INSTANCE = new SymbolTypeDriver();

    private SymbolTypeDriver() {
        super(
                new TypeFacts(
                        ColumnTypeTag.SYMBOL,
                        PhysicalDescriptor.Movement.W4,
                        PhysicalDescriptor.Arithmetic.NONE,
                        PhysicalDescriptor.Accessor.SYMBOL,
                        NullPolicy.SENTINEL,
                        WireKind.SYMBOL,
                        RelationKind.SYMBOL,
                        0,
                        new short[]{ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR, ColumnType.CHAR, ColumnType.INT, ColumnType.TIMESTAMP},
                        PgTypeOids.PG_VARCHAR,
                        'k',
                        0,
                        Numbers.encodeLowHighInts(SymbolTable.VALUE_IS_NULL, SymbolTable.VALUE_IS_NULL),
                        CastTarget.ALWAYS,
                        "SYMBOL"
                ),
                (service, index, columnType, position) -> {
                    // a SYMBOL variable holds a string
                    service.setStr(index);
                    return ColumnType.STRING;
                },
                columnType -> SymbolConstant.NULL,
                columnType -> columnType == ColumnType.SYMBOL ? SymbolTypeConstant.INSTANCE : null,
                // a symbol column function needs the symbol table (static or not) and, in a GROUP BY, the
                // map key slot; the callers that have them build it
                (columnIndex, columnType) -> {
                    throw new UnsupportedOperationException("SYMBOL column functions are built by the caller, which has the symbol table");
                },
                (dataMem, auxMem) -> {
                    throw CairoException.critical(0)
                            .put("no generic SYMBOL NULL appender: the table and WAL writers write a SYMBOL NULL through the symbol map");
                },
                (addr, count) -> Vect.setMemoryInt(addr, SymbolTable.VALUE_IS_NULL, count)
        );
    }

    /**
     * The query engine parks a missing symbol as INT_NULL, not as the storage key
     * {@link SymbolTable#VALUE_IS_NULL}; both resolve to a null symbol. Kept as is.
     */
    @Override
    public long getNullAsLong() {
        return Numbers.INT_NULL;
    }
}
