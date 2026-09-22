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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.function.LongToIntFunction;

/**
 * A payload source for the build unit tests, which exercise keys, chains, row ids and memory
 * rather than column storage. Every payload column of the row with id {@code r} reads {@code r}
 * as an INT or a LONG and {@code r + 0.25} as a DOUBLE. A SYMBOL column stores the key that
 * {@code symbolKeys} maps the row id to, and resolves it through the tables each reader takes
 * from {@code symbols} when it reopens. The source counts its readers' lifecycle calls.
 */
final class RowIdPayloadSource implements HashJoinPayloadSource {
    private final IntList symbolColumns = new IntList();
    private final LongToIntFunction symbolKeys;
    @Nullable
    private final SymbolTableSource symbols;
    int closeCount;
    int readerCount;
    int reopenCount;

    RowIdPayloadSource() {
        this(null);
    }

    RowIdPayloadSource(@Nullable SymbolTableSource symbols, int... symbolColumns) {
        this(symbols, rowId -> (int) rowId, symbolColumns);
    }

    RowIdPayloadSource(@Nullable SymbolTableSource symbols, LongToIntFunction symbolKeys, int... symbolColumns) {
        this.symbols = symbols;
        this.symbolKeys = symbolKeys;
        for (int column : symbolColumns) {
            this.symbolColumns.add(column);
        }
    }

    @Override
    public HashJoinPayloadSource.Reader newReader() {
        readerCount++;
        return new Reader();
    }

    private final class Reader implements HashJoinPayloadSource.Reader {
        private final ObjList<SymbolTable> tables = new ObjList<>();
        private boolean isOpen;
        private long rowId = Long.MIN_VALUE;

        @Override
        public void close() {
            closeCount++;
            isOpen = false;
            tables.clear();
        }

        @Override
        public double getDouble(int col) {
            return rowId + 0.25;
        }

        @Override
        public int getInt(int col) {
            return symbolColumns.contains(col) ? symbolKeys.applyAsInt(rowId) : (int) rowId;
        }

        @Override
        public long getLong(int col) {
            return rowId;
        }

        @Override
        public long getRowId() {
            return rowId;
        }

        @Override
        public CharSequence getSymA(int col) {
            return tables.getQuick(col).valueOf(getInt(col));
        }

        @Override
        public CharSequence getSymB(int col) {
            return tables.getQuick(col).valueBOf(getInt(col));
        }

        @Override
        public SymbolTable getSymbolTable(int col) {
            return tables.getQuick(col);
        }

        @Override
        public SymbolTable newSymbolTable(int col) {
            assert symbols != null;
            return symbols.newSymbolTable(col);
        }

        @Override
        public void position(long rowId) {
            Assert.assertTrue("reader positioned while closed", isOpen);
            this.rowId = rowId;
        }

        @Override
        public void reopen() {
            reopenCount++;
            isOpen = true;
            tables.clear();
            for (int i = 0, n = symbolColumns.size(); i < n; i++) {
                assert symbols != null;
                final int column = symbolColumns.getQuick(i);
                tables.extendAndSet(column, symbols.newSymbolTable(column));
            }
        }
    }
}
