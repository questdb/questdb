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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;

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
