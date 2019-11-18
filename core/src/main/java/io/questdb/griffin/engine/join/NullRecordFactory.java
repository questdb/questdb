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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.BinFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class NullRecordFactory {

    public static final Long256Impl LONG_256_NULL = new Long256Impl();
    private static final ObjList<Function> constantNulls = new ObjList<>();

    public static Record getInstance(ColumnTypes types) {
        final ObjList<Function> functions = new ObjList<>(types.getColumnCount());
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            Function function = constantNulls.get(types.getColumnType(i));
            assert function != null;
            functions.add(function);
        }

        return new VirtualRecord(functions);
    }

    static {
        LONG_256_NULL.setLong0(-1);
        LONG_256_NULL.setLong1(-1);
        LONG_256_NULL.setLong2(-1);
        LONG_256_NULL.setLong3(-1);

        constantNulls.extendAndSet(ColumnType.INT, new IntConstant(0, Numbers.INT_NaN));
        constantNulls.extendAndSet(ColumnType.STRING, new StrConstant(0, null));
        constantNulls.extendAndSet(ColumnType.SYMBOL, new SymbolFunction(0) {
            @Override
            public int getInt(Record rec) {
                return SymbolTable.VALUE_IS_NULL;
            }

            @Override
            public CharSequence getSymbol(Record rec) {
                return null;
            }
        });
        constantNulls.extendAndSet(ColumnType.LONG, new LongConstant(0, Numbers.LONG_NaN));
        constantNulls.extendAndSet(ColumnType.DATE, new DateConstant(0, Numbers.LONG_NaN));
        constantNulls.extendAndSet(ColumnType.TIMESTAMP, new TimestampConstant(0, Numbers.LONG_NaN));
        constantNulls.extendAndSet(ColumnType.BYTE, new ByteConstant(0, (byte) 0));
        constantNulls.extendAndSet(ColumnType.SHORT, new ShortConstant(0, (short) 0));
        constantNulls.extendAndSet(ColumnType.CHAR, new CharConstant(0, (char) 0));
        constantNulls.extendAndSet(ColumnType.BOOLEAN, new BooleanConstant(0, false));
        constantNulls.extendAndSet(ColumnType.DOUBLE, new DoubleConstant(0, Double.NaN));
        constantNulls.extendAndSet(ColumnType.FLOAT, new FloatConstant(0, Float.NaN));
        constantNulls.extendAndSet(ColumnType.BINARY, new BinFunction(0) {
            @Override
            public BinarySequence getBin(Record rec) {
                return null;
            }

            @Override
            public long getBinLen(Record rec) {
                return TableUtils.NULL_LEN;
            }
        });

        constantNulls.extendAndSet(ColumnType.LONG256, new Long256Function(0) {
            @Override
            public Long256 getLong256A(Record rec) {
                return LONG_256_NULL;
            }

            @Override
            public Long256 getLong256B(Record rec) {
                return LONG_256_NULL;
            }

            @Override
            public void getLong256(Record rec, CharSink sink) {

            }
        });
    }
}
