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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;

public class NotEqDoubleFunctionFactory extends EqDoubleFunctionFactory {
    @Override
    public String getSignature() {
        return "!=(DD)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);

        if (left.isConstant() && left.getType() == ColumnType.DOUBLE && Double.isNaN(left.getDouble(null))) {
            switch (right.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, right);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, right);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, right);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, right);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, right);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, right);
            }
        } else if (right.isConstant() && right.getType() == ColumnType.DOUBLE && Double.isNaN(right.getDouble(null))) {
            switch (left.getType()) {
                case ColumnType.INT:
                    return new FuncIntIsNaN(position, left);
                case ColumnType.LONG:
                    return new FuncLongIsNaN(position, left);
                case ColumnType.DATE:
                    return new FuncDateIsNaN(position, left);
                case ColumnType.TIMESTAMP:
                    return new FuncTimestampIsNaN(position, left);
                case ColumnType.FLOAT:
                    return new FuncFloatIsNaN(position, left);
                default:
                    // double
                    return new FuncDoubleIsNaN(position, left);
            }
        }
        return new Func(position, args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends EqDoubleFunctionFactory.Func {
        public Func(int position, Function left, Function right) {
            super(position, left, right);
        }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncIntIsNaN extends EqDoubleFunctionFactory.FuncIntIsNaN {
        public FuncIntIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncLongIsNaN extends EqDoubleFunctionFactory.FuncLongIsNaN {
        public FuncLongIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncDateIsNaN extends EqDoubleFunctionFactory.FuncDateIsNaN {
        public FuncDateIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncTimestampIsNaN extends EqDoubleFunctionFactory.FuncTimestampIsNaN {
        public FuncTimestampIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncFloatIsNaN extends EqDoubleFunctionFactory.FuncFloatIsNaN {
        public FuncFloatIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }

    private static class FuncDoubleIsNaN extends EqDoubleFunctionFactory.FuncDoubleIsNaN {
        public FuncDoubleIsNaN(int position, Function arg) { super(position, arg); }

        @Override
        public boolean getBool(Record rec) {
            return !super.getBool(rec);
        }
    }
}
