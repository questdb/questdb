/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class CaseCommon {
    private static final ObjList<CaseFunctionConstructor> constructors = new ObjList<>();

    static {
        constructors.set(0, ColumnType.MAX, null);
        CaseCommon.constructors.extendAndSet(ColumnType.STRING, StrCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.INT, IntCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.LONG, LongCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.BYTE, ByteCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.BOOLEAN, BooleanCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.SHORT, ShortCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.CHAR, CharCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.FLOAT, FloatCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.DOUBLE, DoubleCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.LONG256, Long256CaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.SYMBOL, SymbolCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.DATE, DateCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.TIMESTAMP, TimestampCaseFunction::new);
        CaseCommon.constructors.extendAndSet(ColumnType.BINARY, BinCaseFunction::new);
    }

    @NotNull
    private static CaseFunctionConstructor getCaseFunctionConstructor(int position, int returnType) throws SqlException {
        final CaseFunctionConstructor constructor = constructors.getQuick(returnType);
        if (constructor == null) {
            throw SqlException.$(position, "not implemented for type '").put(ColumnType.nameOf(returnType)).put('\'');
        }
        return constructor;
    }

    static Function getCaseFunction(int position, int returnType, CaseFunctionPicker picker) throws SqlException {
        return getCaseFunctionConstructor(position, returnType).getInstance(position, picker);
    }
}
