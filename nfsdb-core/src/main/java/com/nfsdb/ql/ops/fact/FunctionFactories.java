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

    private FunctionFactories() {
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public static FunctionFactory find(Signature sig, ObjList<VirtualColumn> args) {
        if (Chars.equals("=", sig.name) &&
                sig.paramCount == 2 &&
                sig.paramTypes.getQuick(1) == ColumnType.DOUBLE &&
                args.getQuick(1).isConstant()) {
            double d = args.getQuick(1).getDouble(null);

            // NaN
            if (d != d) {
                switch (sig.paramTypes.getQuick(0)) {
                    case DOUBLE:
                        return DoubleEqualsNaNOperatorFactory.INSTANCE;
                    case INT:
                        return IntEqualsNaNOperatorFactory.INSTANCE;
                    case LONG:
                        return LongEqualsNaNOperatorFactory.INSTANCE;
                }
            }
        }

        FunctionFactory factory = factories.get(sig);
        if (factory != null) {
            return factory;
        } else {
            // special cases/intrinsic factories
            if (Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.getQuick(0)) {
                    case STRING:
                        return StrInOperatorFactory.INSTANCE;
                    case SYMBOL:
                        return SymInOperatorFactory.INSTANCE;
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

    private static void unSig(String name, ColumnType type, FunctionFactory f) {
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, false), f);
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, true), f);
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
        binSig("+", ColumnType.DOUBLE, ColumnType.DOUBLE, AddDoubleOperatorFactory.INSTANCE);
        binSig("+", ColumnType.DOUBLE, ColumnType.LONG, AddDoubleOperatorFactory.INSTANCE);
        binSig("+", ColumnType.LONG, ColumnType.DOUBLE, AddDoubleOperatorFactory.INSTANCE);
        binSig("+", ColumnType.DOUBLE, ColumnType.INT, AddDoubleOperatorFactory.INSTANCE);
        binSig("+", ColumnType.INT, ColumnType.DOUBLE, AddDoubleOperatorFactory.INSTANCE);
        binSig("+", ColumnType.INT, ColumnType.INT, AddIntOperatorFactory.INSTANCE);
        binSig("+", ColumnType.LONG, ColumnType.INT, AddLongOperatorFactory.INSTANCE);
        binSig("+", ColumnType.INT, ColumnType.LONG, AddLongOperatorFactory.INSTANCE);
        binSig("+", ColumnType.LONG, ColumnType.LONG, AddLongOperatorFactory.INSTANCE);
        binSig("+", ColumnType.STRING, ColumnType.STRING, StrConcatOperatorFactory.INSTANCE);

        //todo: itoa functions
        binSig("/", ColumnType.DOUBLE, ColumnType.DOUBLE, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.DOUBLE, ColumnType.INT, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.INT, ColumnType.DOUBLE, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.INT, ColumnType.INT, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.DOUBLE, ColumnType.LONG, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.LONG, ColumnType.DOUBLE, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.LONG, ColumnType.LONG, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.LONG, ColumnType.INT, DivDoubleOperatorFactory.INSTANCE);
        binSig("/", ColumnType.INT, ColumnType.LONG, DivDoubleOperatorFactory.INSTANCE);

        binSig("*", ColumnType.DOUBLE, ColumnType.DOUBLE, MultDoubleOperatorFactory.INSTANCE);
        binSig("*", ColumnType.INT, ColumnType.DOUBLE, MultDoubleOperatorFactory.INSTANCE);
        binSig("*", ColumnType.DOUBLE, ColumnType.INT, MultDoubleOperatorFactory.INSTANCE);
        binSig("*", ColumnType.DOUBLE, ColumnType.LONG, MultDoubleOperatorFactory.INSTANCE);
        binSig("*", ColumnType.LONG, ColumnType.DOUBLE, MultDoubleOperatorFactory.INSTANCE);
        binSig("*", ColumnType.INT, ColumnType.INT, MultIntOperatorFactory.INSTANCE);
        binSig("*", ColumnType.LONG, ColumnType.LONG, MultLongOperatorFactory.INSTANCE);
        binSig("*", ColumnType.INT, ColumnType.LONG, MultLongOperatorFactory.INSTANCE);
        binSig("*", ColumnType.LONG, ColumnType.INT, MultLongOperatorFactory.INSTANCE);

        binSig("-", ColumnType.DOUBLE, ColumnType.DOUBLE, MinusDoubleOperatorFactory.INSTANCE);
        binSig("-", ColumnType.INT, ColumnType.DOUBLE, MinusDoubleOperatorFactory.INSTANCE);
        binSig("-", ColumnType.DOUBLE, ColumnType.INT, MinusDoubleOperatorFactory.INSTANCE);
        binSig("-", ColumnType.DOUBLE, ColumnType.LONG, MinusDoubleOperatorFactory.INSTANCE);
        binSig("-", ColumnType.LONG, ColumnType.DOUBLE, MinusDoubleOperatorFactory.INSTANCE);
        binSig("-", ColumnType.INT, ColumnType.INT, MinusIntOperatorFactory.INSTANCE);
        binSig("-", ColumnType.LONG, ColumnType.LONG, MinusLongOperatorFactory.INSTANCE);
        binSig("-", ColumnType.LONG, ColumnType.INT, MinusLongOperatorFactory.INSTANCE);
        binSig("-", ColumnType.INT, ColumnType.LONG, MinusLongOperatorFactory.INSTANCE);

        binSig(">", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.INT, ColumnType.DOUBLE, DoubleGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.DOUBLE, ColumnType.INT, DoubleGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.INT, ColumnType.INT, new IntGreaterThanOperatorFactory());
        binSig(">", ColumnType.DOUBLE, ColumnType.LONG, DoubleGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.LONG, ColumnType.DOUBLE, DoubleGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.LONG, ColumnType.LONG, LongGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.INT, ColumnType.LONG, LongGreaterThanOperatorFactory.INSTANCE);
        binSig(">", ColumnType.LONG, ColumnType.INT, LongGreaterThanOperatorFactory.INSTANCE);

        binSig(">=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.INT, ColumnType.DOUBLE, DoubleGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.DOUBLE, ColumnType.INT, DoubleGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.DOUBLE, ColumnType.LONG, DoubleGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.LONG, ColumnType.DOUBLE, DoubleGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.INT, ColumnType.INT, IntGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.LONG, ColumnType.LONG, LongGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.LONG, ColumnType.INT, LongGreaterOrEqualOperatorFactory.INSTANCE);
        binSig(">=", ColumnType.INT, ColumnType.LONG, LongGreaterOrEqualOperatorFactory.INSTANCE);

        binSig("<", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.INT, ColumnType.DOUBLE, DoubleLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.DOUBLE, ColumnType.INT, DoubleLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.DOUBLE, ColumnType.LONG, DoubleLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.LONG, ColumnType.DOUBLE, DoubleLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.INT, ColumnType.INT, IntLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.LONG, ColumnType.LONG, LongLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.LONG, ColumnType.INT, LongLessThanOperatorFactory.INSTANCE);
        binSig("<", ColumnType.INT, ColumnType.LONG, LongLessThanOperatorFactory.INSTANCE);

        binSig("<=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.INT, ColumnType.DOUBLE, DoubleLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.DOUBLE, ColumnType.INT, DoubleLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.DOUBLE, ColumnType.LONG, DoubleLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.LONG, ColumnType.DOUBLE, DoubleLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.INT, ColumnType.INT, IntLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.LONG, ColumnType.LONG, LongLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.LONG, ColumnType.INT, LongLessOrEqualOperatorFactory.INSTANCE);
        binSig("<=", ColumnType.INT, ColumnType.LONG, LongLessOrEqualOperatorFactory.INSTANCE);

        binSig("=", ColumnType.INT, ColumnType.INT, IntEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.STRING, ColumnType.STRING, new StrEqualsOperatorFactory());
        binSig("=", ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.INT, ColumnType.DOUBLE, DoubleEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.DOUBLE, ColumnType.INT, DoubleEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.DOUBLE, ColumnType.LONG, DoubleEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.LONG, ColumnType.DOUBLE, DoubleEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.LONG, ColumnType.LONG, LongEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.LONG, ColumnType.INT, LongEqualsOperatorFactory.INSTANCE);
        binSig("=", ColumnType.INT, ColumnType.LONG, LongEqualsOperatorFactory.INSTANCE);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperatorFactory.INSTANCE);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, DoubleScaledEqualsOperatorFactory.INSTANCE);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperatorFactory.INSTANCE);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.DOUBLE, DoubleScaledEqualsOperatorFactory.INSTANCE);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperatorFactory.INSTANCE);

        unSig("-", ColumnType.INT, IntNegativeOperatorFactory.INSTANCE);
        unSig("-", ColumnType.DOUBLE, DoubleNegativeOperatorFactory.INSTANCE);
        unSig("-", ColumnType.LONG, LongNegativeOperatorFactory.INSTANCE);

        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), new StrEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), new SymEqualsOperatorFactory());
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), new SymEqualsROperatorFactory());
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true), new StrRegexOperatorFactory());
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), new SymRegexOperatorFactory());
        binSig("and", ColumnType.BOOLEAN, ColumnType.BOOLEAN, new AndOperatorFactory());
        binSig("or", ColumnType.BOOLEAN, ColumnType.BOOLEAN, new OrOperatorFactory());
    }
}
