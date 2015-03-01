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

package com.nfsdb.lang.cst.impl.ksrc;

import com.nfsdb.lang.cst.KeyCursor;
import com.nfsdb.lang.cst.KeySource;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.storage.SymbolTable;

import java.util.List;

public class PartialSymbolKeySource implements KeySource, KeyCursor {

    private final StringRef symbol;
    private final List<String> values;
    private int[] keys;
    private SymbolTable symbolTable;
    private int keyIndex;
    private int keyCount;

    public PartialSymbolKeySource(StringRef symbol, List<String> values) {
        this.symbol = symbol;
        this.values = values;
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (this.symbolTable == null) {
            if (this.keys == null || this.keys.length < values.size()) {
                this.keys = new int[values.size()];
            }
            this.symbolTable = slice.partition.getJournal().getSymbolTable(symbol.value);
            int keyCount = 0;
            for (int i = 0; i < values.size(); i++) {
                int key = symbolTable.getQuick(values.get(i));
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
    public int size() {
        return keyCount;
    }

    @Override
    public void reset() {
        symbolTable = null;
        keyIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return keyIndex < keyCount;
    }

    @Override
    public int next() {
        return keys[keyIndex++];
    }
}
