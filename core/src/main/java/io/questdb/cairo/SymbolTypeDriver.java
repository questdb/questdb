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
import io.questdb.cairo.sql.SymbolTable;
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

    @Override
    public long getNullLong(int longIndex) {
        return Numbers.encodeLowHighInts(SymbolTable.VALUE_IS_NULL, SymbolTable.VALUE_IS_NULL);
    }

    @Override
    public boolean hasNullSentinel() {
        return true;
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
