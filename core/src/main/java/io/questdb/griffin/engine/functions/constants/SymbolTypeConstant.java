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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.SymbolFunction;

public class SymbolTypeConstant extends SymbolFunction implements TypeConstant {
    public static final SymbolTypeConstant INSTANCE = new SymbolTypeConstant();

    @Override
    public int getInt(Record rec) {
        return SymbolTable.VALUE_IS_NULL;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return null;
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return null;
    }

    @Override
    public boolean isSymbolTableStatic() {
        return false;
    }

    @Override
    public CharSequence valueBOf(int key) {
        return null;
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return null;
    }
}
