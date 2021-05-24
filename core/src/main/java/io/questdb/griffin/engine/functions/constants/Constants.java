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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.TypeConstant;
import io.questdb.std.ObjList;

public final class Constants {
    private static final ObjList<ConstantFunction> nullConstants = new ObjList<>();
    private static final ObjList<TypeConstant> typeConstants = new ObjList<>();

    static {
        Constants.nullConstants.extendAndSet(ColumnType.INT, IntConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.STRING, StrConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.SYMBOL, SymbolConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.LONG, LongConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.DATE, DateConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.TIMESTAMP, TimestampConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.BYTE, ByteConstant.ZERO);
        Constants.nullConstants.extendAndSet(ColumnType.SHORT, ShortConstant.ZERO);
        Constants.nullConstants.extendAndSet(ColumnType.CHAR, CharConstant.ZERO);
        Constants.nullConstants.extendAndSet(ColumnType.BOOLEAN, BooleanConstant.FALSE);
        Constants.nullConstants.extendAndSet(ColumnType.DOUBLE, DoubleConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.FLOAT, FloatConstant.NULL);
        Constants.nullConstants.extendAndSet(ColumnType.BINARY, NullBinConstant.INSTANCE);
        Constants.nullConstants.extendAndSet(ColumnType.LONG256, Long256NullConstant.INSTANCE);

        Constants.typeConstants.extendAndSet(ColumnType.INT, IntTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.STRING, StrTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.SYMBOL, SymbolTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.LONG, LongTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.DATE, DateTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.TIMESTAMP, TimestampTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.BYTE, ByteTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.SHORT, ShortTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.CHAR, CharTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.BOOLEAN, BooleanTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.DOUBLE, DoubleTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.FLOAT, FloatTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.BINARY, BinTypeConstant.INSTANCE);
        Constants.typeConstants.extendAndSet(ColumnType.LONG256, Long256TypeConstant.INSTANCE);
    }

    public static ConstantFunction getNullConstant(int columnType) {
        return nullConstants.getQuick(columnType);
    }

    public static TypeConstant getTypeConstant(int columnType) {
        return typeConstants.getQuick(columnType);
    }
}
