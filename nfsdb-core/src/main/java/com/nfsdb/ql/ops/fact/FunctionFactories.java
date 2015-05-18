/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.ops.fact;

import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.ql.parser.Signature;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Chars;

public final class FunctionFactories {
    private static final ObjObjHashMap<Signature, FunctionFactory> factories = new ObjObjHashMap<>();
    // intrinsic factories
    private static final StrInOperatorFactory STRING_IN_OPERATOR_FACTORY = new StrInOperatorFactory();
    private static final SymInOperatorFactory SYMBOL_IN_OPERATOR_FACTORY = new SymInOperatorFactory();
    private static final SymEqualsOperatorFactory SYMBOL_EQ_OPERATOR_FACTORY = new SymEqualsOperatorFactory();

    public static FunctionFactory find(Signature sig, ObjList<VirtualColumn> args) {

        // pre-lookup special case, symbol = value operator requires special consideration
        if (Chars.equals("=", sig.name)
                && args.size() == 2
                && args.get(0).getType() == ColumnType.SYMBOL
                && args.get(1).getType() == ColumnType.STRING
                && args.get(1).isConstant()) {
            return SYMBOL_EQ_OPERATOR_FACTORY;
        }

        FunctionFactory factory = factories.get(sig);
        if (factory != null) {
            return factory;
        } else {
            // special cases/intrinsic factories
            if (sig.paramCount < 3 && Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.get(0)) {
                    case STRING:
                        return STRING_IN_OPERATOR_FACTORY;
                    case SYMBOL:
                        return SYMBOL_IN_OPERATOR_FACTORY;
                }
            } else if (sig.paramCount > 2 && Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.getLast()) {
                    case STRING:
                        return STRING_IN_OPERATOR_FACTORY;
                    case SYMBOL:
                        return SYMBOL_IN_OPERATOR_FACTORY;
                }
            }
        }
        return null;
    }

    static {
        factories.put(new Signature().setName("+").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new AddDoubleOperatorFactory());
        factories.put(new Signature().setName("+").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new AddDoubleOperatorFactory());
        factories.put(new Signature().setName("+").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.DOUBLE), new AddDoubleOperatorFactory());
        factories.put(new Signature().setName("+").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.INT), new AddIntOperatorFactory());

        factories.put(new Signature().setName("/").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new DivDoubleOperatorFactory());
        factories.put(new Signature().setName("/").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new DivDoubleOperatorFactory());
        factories.put(new Signature().setName("/").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.DOUBLE), new DivDoubleOperatorFactory());

        factories.put(new Signature().setName("*").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new MultDoubleOperatorFactory());
        factories.put(new Signature().setName("*").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new MultDoubleOperatorFactory());
        factories.put(new Signature().setName("*").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.DOUBLE), new MultDoubleOperatorFactory());

        factories.put(new Signature().setName("-").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new MinusDoubleOperatorFactory());
        factories.put(new Signature().setName("-").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new MinusDoubleOperatorFactory());
        factories.put(new Signature().setName("-").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.DOUBLE), new MinusDoubleOperatorFactory());

        factories.put(new Signature().setName(">").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new DoubleGreaterThanOperatorFactory());
        factories.put(new Signature().setName(">").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new DoubleGreaterThanOperatorFactory());
        factories.put(new Signature().setName(">").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.DOUBLE), new DoubleGreaterThanOperatorFactory());
        factories.put(new Signature().setName(">").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.INT), new IntGreaterThanOperatorFactory());

        factories.put(new Signature().setName("<").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.DOUBLE), new DoubleLessThanOperatorFactory());
        factories.put(new Signature().setName("<").setParamCount(2).paramType(0, ColumnType.DOUBLE).paramType(1, ColumnType.INT), new DoubleLessThanOperatorFactory());

        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.INT).paramType(1, ColumnType.INT), new IntEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.STRING).paramType(1, ColumnType.STRING), new StrEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL).paramType(1, ColumnType.STRING), new StrEqualsOperatorFactory());

        factories.put(new Signature().setName("and").setParamCount(2).paramType(0, ColumnType.BOOLEAN).paramType(1, ColumnType.BOOLEAN), new AndOperatorFactory());
        factories.put(new Signature().setName("or").setParamCount(2).paramType(0, ColumnType.BOOLEAN).paramType(1, ColumnType.BOOLEAN), new OrOperatorFactory());
    }
}
