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

package com.questdb.griffin.engine.functions.conditional;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.IntObjHashMap;
import com.questdb.std.ObjList;

public class SwitchFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "switch(V)";
    }

    @Override
    //todo: unit test this and add support for all types
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        int n = args.size();

        final int keyType = args.getQuick(0).getType();
        final int valueType = args.getQuick(2).getType();
        final Function elseBranch;
        if (n % 2 == 0) {
            elseBranch = args.getLast();
            n--;
        } else {
            elseBranch = null;
        }

        for (int i = 1; i < n; i += 2) {
            final Function keyFunc = args.getQuick(i);
            final int keyArgType = keyFunc.getType();
            if (!keyFunc.isConstant()) {
                throw SqlException.$(keyFunc.getPosition(), "constant expected");
            }

            if (keyArgType != keyType) {
                throw SqlException.position(keyFunc.getPosition())
                        .put("type mismatch [expected=").put(ColumnType.nameOf(keyType))
                        .put(", actual=").put(ColumnType.nameOf(keyArgType))
                        .put(']');
            }

            final Function valueFunc = args.getQuick(i + 1);
            if (valueFunc.getType() != valueType) {
                throw SqlException.position(args.getQuick(i).getPosition())
                        .put("type mismatch [expected=").put(ColumnType.nameOf(valueType))
                        .put(", actual=").put(ColumnType.nameOf(valueFunc.getType()))
                        .put(']');
            }
        }

        switch (keyType) {
            case ColumnType.CHAR:
                final IntObjHashMap<Function> map = new IntObjHashMap<>();
                for (int i = 1; i < n; i += 2) {
                    map.put(args.getQuick(i).getChar(null), args.getQuick(i + 1));
                }

                switch (valueType) {
                    case ColumnType.STRING:
                        return new CharStrSwitchFunction(position, args.getQuick(0), map, elseBranch);
                    default:
                        return null;
                }
            default:
                return null;
        }
    }

    private static class CharStrSwitchFunction extends StrFunction {
        private final IntObjHashMap<Function> functions;
        private final Function value;
        private final Function elseBranch;

        public CharStrSwitchFunction(int position, Function value, IntObjHashMap<Function> functions, Function elseBranch) {
            super(position);
            this.value = value;
            this.functions = functions;
            this.elseBranch = elseBranch;
        }

        @Override
        public CharSequence getStr(Record rec) {
            Function function = functions.get(value.getChar(rec));
            if (function == null) {
                if (elseBranch == null) {
                    return null;
                }
                return elseBranch.getStr(rec);
            }
            return function.getStr(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            Function function = functions.get(value.getChar(rec));
            if (function == null) {
                if (elseBranch == null) {
                    return null;
                }
                return elseBranch.getStrB(rec);
            }
            return function.getStrB(rec);
        }

    }
}
