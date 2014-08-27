/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.lang.cst.impl.ksrc;

import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.lang.cst.KeyCursor;
import com.nfsdb.journal.lang.cst.KeySource;
import com.nfsdb.journal.lang.cst.PartitionSlice;

public class PartialSymbolKeySource implements KeySource, KeyCursor {

    private final String symbol;
    private final String[] values;
    private final int[] keys;
    private SymbolTable symbolTable;
    private int keyIndex;
    private int keyCount;

    public PartialSymbolKeySource(String symbol, String[] values) {
        this.symbol = symbol;
        this.values = values;
        this.keys = new int[values.length];
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (this.symbolTable == null) {
            this.symbolTable = slice.partition.getJournal().getSymbolTable(symbol);
            int keyCount = 0;
            for (int i = 0; i < values.length; i++) {
                int key = symbolTable.getQuick(values[i]);
                if (key >= 0) {
                    keys[keyCount++] = key;
                }
            }
            this.keyCount = keyCount;
        }
        this.keyIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < keyCount;
    }

    @Override
    public int next() {
        return keys[keyIndex++];
    }

    @Override
    public int size() {
        return keyCount;
    }

    @Override
    public void reset() {
        symbolTable = null;
        keyIndex = 0;
    }
}
