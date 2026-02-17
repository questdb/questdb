/*******************************************************************************
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

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.api.MemoryR;

public class EmptySymbolMapReader implements SymbolMapReader {

    public static final EmptySymbolMapReader INSTANCE = new EmptySymbolMapReader();

    @Override
    public boolean containsNullValue() {
        return false;
    }

    @Override
    public int getSymbolCapacity() {
        return 0;
    }

    @Override
    public int getSymbolCount() {
        return 0;
    }

    @Override
    public MemoryR getSymbolOffsetsColumn() {
        return null;
    }

    @Override
    public MemoryR getSymbolValuesColumn() {
        return null;
    }

    @Override
    public boolean isCached() {
        return false;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public int keyOf(CharSequence value) {
        return SymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public StaticSymbolTable newSymbolTableView() {
        return this;
    }

    @Override
    public void updateSymbolCount(int count) {
    }

    @Override
    public CharSequence valueBOf(int key) {
        return null;
    }

    @Override
    public CharSequence valueOf(int key) {
        return null;
    }
}
