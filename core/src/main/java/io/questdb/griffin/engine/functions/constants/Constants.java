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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.TypeConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public final class Constants {
    private static final ObjList<ConstantFunction> nullConstants = new ObjList<>();
    private static final ObjList<TypeConstant> typeConstants = new ObjList<>();

    static {
        Constants.nullConstants.extendAndSet(ColumnType.INT, new IntConstant(0, Numbers.INT_NaN));
        Constants.nullConstants.extendAndSet(ColumnType.STRING, new StrConstant(0, null));
        Constants.nullConstants.extendAndSet(ColumnType.SYMBOL, new SymbolConstant(0, null, SymbolTable.VALUE_IS_NULL));
        Constants.nullConstants.extendAndSet(ColumnType.LONG, new LongConstant(0, Numbers.LONG_NaN));
        Constants.nullConstants.extendAndSet(ColumnType.DATE, new DateConstant(0, Numbers.LONG_NaN));
        Constants.nullConstants.extendAndSet(ColumnType.TIMESTAMP, new TimestampConstant(0, Numbers.LONG_NaN));
        Constants.nullConstants.extendAndSet(ColumnType.BYTE, new ByteConstant(0, (byte) 0));
        Constants.nullConstants.extendAndSet(ColumnType.SHORT, new ShortConstant(0, (short) 0));
        Constants.nullConstants.extendAndSet(ColumnType.CHAR, new CharConstant(0, (char) 0));
        Constants.nullConstants.extendAndSet(ColumnType.BOOLEAN, new BooleanConstant(0, false));
        Constants.nullConstants.extendAndSet(ColumnType.DOUBLE, new DoubleConstant(0, Double.NaN));
        Constants.nullConstants.extendAndSet(ColumnType.FLOAT, new FloatConstant(0, Float.NaN));
        Constants.nullConstants.extendAndSet(ColumnType.BINARY, new NullBinConstant());
        Constants.nullConstants.extendAndSet(ColumnType.LONG256, new Long256NullConstant());

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
