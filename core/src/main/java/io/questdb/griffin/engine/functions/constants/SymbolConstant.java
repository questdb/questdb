/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.Chars;
import org.jetbrains.annotations.Nullable;

public class SymbolConstant extends SymbolFunction implements ConstantFunction {
    public static final SymbolConstant NULL = new SymbolConstant(null, VALUE_IS_NULL);
    public static final SymbolConstant TRUE = new SymbolConstant("true", 0);
    public static final SymbolConstant FALSE = new SymbolConstant("false", 0);
    private final String value;
    private final int index;

    public SymbolConstant(CharSequence value, int index) {
        if (value == null) {
            this.value = null;
            this.index = SymbolTable.VALUE_IS_NULL;
        } else {
            if (Chars.startsWith(value, '\'')) {
                this.value = Chars.toString(value, 1, value.length() - 1);
            } else {
                this.value = Chars.toString(value);
            }
            this.index = index;
        }
    }

    public static SymbolConstant newInstance(CharSequence value) {
        if (value == null) {
            return NULL;
        }

        if (SqlKeywords.isTrueKeyword(value)) {
            return TRUE;
        }

        if (SqlKeywords.isFalseKeyword(value)) {
            return FALSE;
        }

        return new SymbolConstant(Chars.toString(value), 0);
    }

    @Override
    public boolean isSymbolTableStatic() {
        return false;
    }

    @Override
    public int getInt(Record rec) {
        return index;
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return value;
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return value;
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return value;
    }

    @Override
    public CharSequence valueBOf(int key) {
        return value;
    }

    @Override
    public @Nullable SymbolTable newSymbolTable() {
        return this;
    }
}
