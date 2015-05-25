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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class FunctionFactories {
    private static final ObjObjHashMap<Signature, FunctionFactory> factories = new ObjObjHashMap<>();
    // intrinsic factories
    private static final StrInOperatorFactory STRING_IN_OPERATOR_FACTORY = new StrInOperatorFactory();
    private static final SymInOperatorFactory SYMBOL_IN_OPERATOR_FACTORY = new SymInOperatorFactory();
    private static final DoubleEqualsNaNOperatorFactory DOUBLE_NAN_FACTORY = new DoubleEqualsNaNOperatorFactory();
    private static final IntEqualsNaNOperatorFactory INT_NAN_FACTORY = new IntEqualsNaNOperatorFactory();

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public static FunctionFactory find(Signature sig, ObjList<VirtualColumn> args) {
        if (Chars.equals("=", sig.name) && sig.paramCount == 2 && sig.paramTypes.getQuick(1) == ColumnType.DOUBLE && args.getQuick(1).isConstant()) {
            double d = args.getQuick(1).getDouble(null);

            // NaN
            if (d != d) {
                switch (sig.paramTypes.getQuick(0)) {
                    case INT:
                        return INT_NAN_FACTORY;
                    case DOUBLE:
                        return DOUBLE_NAN_FACTORY;
                }
            }
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

    private static void binSig(String name, ColumnType lhst, ColumnType rhst, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, true), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, true), f);
    }

    private static void triSig(String name, ColumnType lhst, ColumnType rhst, ColumnType scale, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, true), f);

        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, true), f);
    }

    static {
        binSig("+", ColumnType.DOUBLE, ColumnType.DOUBLE, new AddDoubleOperatorFactory());
        binSig("+", ColumnType.DOUBLE, ColumnType.INT, new AddDoubleOperatorFactory());
        binSig("+", ColumnType.INT, ColumnType.DOUBLE, new AddDoubleOperatorFactory());
        binSig("+", ColumnType.INT, ColumnType.INT, new AddIntOperatorFactory());
        binSig("+", ColumnType.STRING, ColumnType.STRING, new StrConcatOperatorFactory());
        //todo: itoa functions
        binSig("/", ColumnType.DOUBLE, ColumnType.DOUBLE, new DivDoubleOperatorFactory());
        binSig("/", ColumnType.DOUBLE, ColumnType.INT, new DivDoubleOperatorFactory());
        binSig("/", ColumnType.INT, ColumnType.DOUBLE, new DivDoubleOperatorFactory());
        binSig("/", ColumnType.INT, ColumnType.INT, new DivDoubleOperatorFactory());
        binSig("*", ColumnType.DOUBLE, ColumnType.DOUBLE, new MultDoubleOperatorFactory());
        binSig("*", ColumnType.INT, ColumnType.DOUBLE, new MultDoubleOperatorFactory());
        binSig("*", ColumnType.DOUBLE, ColumnType.INT, new MultDoubleOperatorFactory());
        binSig("*", ColumnType.INT, ColumnType.INT, new MultIntOperatorFactory());
        binSig("-", ColumnType.DOUBLE, ColumnType.DOUBLE, new MinusDoubleOperatorFactory());
        binSig("-", ColumnType.INT, ColumnType.DOUBLE, new MinusDoubleOperatorFactory());
        binSig("-", ColumnType.DOUBLE, ColumnType.INT, new MinusDoubleOperatorFactory());
        binSig("-", ColumnType.INT, ColumnType.INT, new MinusIntOperatorFactory());
        binSig(">", ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleGreaterThanOperatorFactory());
        binSig(">", ColumnType.INT, ColumnType.DOUBLE, new DoubleGreaterThanOperatorFactory());
        binSig(">", ColumnType.DOUBLE, ColumnType.INT, new DoubleGreaterThanOperatorFactory());
        binSig(">", ColumnType.INT, ColumnType.INT, new IntGreaterThanOperatorFactory());
        binSig(">=", ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleGreaterOrEqualOperatorFactory());
        binSig(">=", ColumnType.INT, ColumnType.DOUBLE, new DoubleGreaterOrEqualOperatorFactory());
        binSig(">=", ColumnType.DOUBLE, ColumnType.INT, new DoubleGreaterOrEqualOperatorFactory());
        binSig(">=", ColumnType.INT, ColumnType.INT, new IntGreaterOrEqualOperatorFactory());
        binSig("<", ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleLessThanOperatorFactory());
        binSig("<", ColumnType.INT, ColumnType.DOUBLE, new DoubleLessThanOperatorFactory());
        binSig("<", ColumnType.DOUBLE, ColumnType.INT, new DoubleLessThanOperatorFactory());
        binSig("<", ColumnType.INT, ColumnType.INT, new IntLessThanOperatorFactory());
        binSig("<=", ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleLessOrEqualOperatorFactory());
        binSig("<=", ColumnType.INT, ColumnType.DOUBLE, new DoubleLessOrEqualOperatorFactory());
        binSig("<=", ColumnType.DOUBLE, ColumnType.INT, new DoubleLessOrEqualOperatorFactory());
        binSig("<=", ColumnType.INT, ColumnType.INT, new IntLessOrEqualOperatorFactory());
        binSig("=", ColumnType.INT, ColumnType.INT, new IntEqualsOperatorFactory());
        binSig("=", ColumnType.STRING, ColumnType.STRING, new StrEqualsOperatorFactory());
        binSig("=", ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleEqualsOperatorFactory());
        binSig("=", ColumnType.INT, ColumnType.DOUBLE, new DoubleEqualsOperatorFactory());
        binSig("=", ColumnType.DOUBLE, ColumnType.INT, new DoubleEqualsOperatorFactory());
        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleScaledEqualsOperatorFactory());
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, new DoubleScaledEqualsOperatorFactory());
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, new DoubleScaledEqualsOperatorFactory());
        factories.put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.INT, true), new IntNegativeOperatorFactory());
        factories.put(new Signature().setName("-").setParamCount(1).paramType(0, ColumnType.INT, false), new IntNegativeOperatorFactory());

        //todo: double comparison function
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), new StrEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), new SymEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), new SymEqualsROperatorFactory());
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true), new StrRegexOperatorFactory());
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), new SymRegexOperatorFactory());
        binSig("and", ColumnType.BOOLEAN, ColumnType.BOOLEAN, new AndOperatorFactory());
        binSig("or", ColumnType.BOOLEAN, ColumnType.BOOLEAN, new OrOperatorFactory());
    }
}
