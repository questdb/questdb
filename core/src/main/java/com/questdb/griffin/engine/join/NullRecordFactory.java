/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.join;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.cairo.sql.VirtualRecord;
import com.questdb.griffin.engine.functions.BinFunction;
import com.questdb.griffin.engine.functions.SymbolFunction;
import com.questdb.griffin.engine.functions.constants.*;
import com.questdb.std.BinarySequence;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;

public class NullRecordFactory {
    private static final ByteConstant BYTE_NULL = new ByteConstant(0, (byte) 0);
    private static final ShortConstant SHORT_NULL = new ShortConstant(0, (short) 0);
    private static final CharConstant CHAR_NULL = new CharConstant(0, (char) 0);
    private static final IntConstant INT_NULL = new IntConstant(0, Numbers.INT_NaN);
    private static final LongConstant LONG_NULL = new LongConstant(0, Numbers.LONG_NaN);
    private static final DateConstant DATE_NULL = new DateConstant(0, Numbers.LONG_NaN);
    private static final TimestampConstant TIMESTAMP_NULL = new TimestampConstant(0, Numbers.LONG_NaN);
    private static final BooleanConstant BOOLEAN_NULL = new BooleanConstant(0, false);
    private static final FloatConstant FLOAT_NULL = new FloatConstant(0, Float.NaN);
    private static final DoubleConstant DOUBLE_NULL = new DoubleConstant(0, Double.NaN);
    private static final StrConstant STRING_NULL = new StrConstant(0, null);
    private static final BinFunction BINARY_NULL = new BinFunction(0) {
        @Override
        public BinarySequence getBin(Record rec) {
            return null;
        }

        @Override
        public long getBinLen(Record rec) {
            return TableUtils.NULL_LEN;
        }
    };

    private static final SymbolFunction SYMBOL_NULL = new SymbolFunction(0) {
        @Override
        public int getInt(Record rec) {
            return SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return null;
        }
    };

    public static Record getInstance(ColumnTypes types) {
        final ObjList<Function> functions = new ObjList<>(types.getColumnCount());
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            switch (types.getColumnType(i)) {
                case ColumnType.INT:
                    functions.add(INT_NULL);
                    break;
                case ColumnType.SYMBOL:
                    functions.add(SYMBOL_NULL);
                    break;
                case ColumnType.LONG:
                    functions.add(LONG_NULL);
                    break;
                case ColumnType.DATE:
                    functions.add(DATE_NULL);
                    break;
                case ColumnType.TIMESTAMP:
                    functions.add(TIMESTAMP_NULL);
                    break;
                case ColumnType.BYTE:
                    functions.add(BYTE_NULL);
                    break;
                case ColumnType.SHORT:
                    functions.add(SHORT_NULL);
                    break;
                case ColumnType.CHAR:
                    functions.add(CHAR_NULL);
                    break;
                case ColumnType.BOOLEAN:
                    functions.add(BOOLEAN_NULL);
                    break;
                case ColumnType.FLOAT:
                    functions.add(FLOAT_NULL);
                    break;
                case ColumnType.DOUBLE:
                    functions.add(DOUBLE_NULL);
                    break;
                case ColumnType.STRING:
                    functions.add(STRING_NULL);
                    break;
                case ColumnType.BINARY:
                    functions.add(BINARY_NULL);
                    break;
                default:
                    break;

            }
        }

        return new VirtualRecord(functions);
    }
}
