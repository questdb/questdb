/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops;

import com.questdb.ql.ops.conv.*;
import com.questdb.ql.ops.count.*;
import com.questdb.ql.ops.div.DivDoubleOperator;
import com.questdb.ql.ops.eq.*;
import com.questdb.ql.ops.first.FirstDoubleAggregator;
import com.questdb.ql.ops.first.FirstFloatAggregator;
import com.questdb.ql.ops.first.FirstIntAggregator;
import com.questdb.ql.ops.first.FirstLongAggregator;
import com.questdb.ql.ops.gt.*;
import com.questdb.ql.ops.gte.*;
import com.questdb.ql.ops.last.*;
import com.questdb.ql.ops.lt.*;
import com.questdb.ql.ops.lte.*;
import com.questdb.ql.ops.max.MaxDateAggregator;
import com.questdb.ql.ops.max.MaxDoubleAggregator;
import com.questdb.ql.ops.max.MaxIntAggregator;
import com.questdb.ql.ops.max.MaxLongAggregator;
import com.questdb.ql.ops.min.MinDateAggregator;
import com.questdb.ql.ops.min.MinDoubleAggregator;
import com.questdb.ql.ops.min.MinIntAggregator;
import com.questdb.ql.ops.min.MinLongAggregator;
import com.questdb.ql.ops.minus.MinusDoubleOperator;
import com.questdb.ql.ops.minus.MinusIntOperator;
import com.questdb.ql.ops.minus.MinusLongOperator;
import com.questdb.ql.ops.mult.MultDoubleOperator;
import com.questdb.ql.ops.mult.MultIntOperator;
import com.questdb.ql.ops.mult.MultLongOperator;
import com.questdb.ql.ops.neg.DoubleNegativeOperator;
import com.questdb.ql.ops.neg.IntNegativeOperator;
import com.questdb.ql.ops.neg.LongNegativeOperator;
import com.questdb.ql.ops.neq.*;
import com.questdb.ql.ops.plus.*;
import com.questdb.ql.ops.regex.PluckStrFunction;
import com.questdb.ql.ops.regex.ReplaceStrWrapper;
import com.questdb.ql.ops.regex.StrRegexOperator;
import com.questdb.ql.ops.round.*;
import com.questdb.ql.ops.stat.AvgAggregator;
import com.questdb.ql.ops.stat.StdDevAggregator;
import com.questdb.ql.ops.stat.VarAggregator;
import com.questdb.ql.ops.sum.SumDoubleAggregator;
import com.questdb.ql.ops.sum.SumIntAggregator;
import com.questdb.ql.ops.sum.SumLongAggregator;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.Chars;
import com.questdb.std.ObjList;
import com.questdb.std.ObjObjHashMap;
import com.questdb.store.ColumnType;

public final class FunctionFactories {

    private static final ObjObjHashMap<Signature, VirtualColumnFactory<Function>> factories = new ObjObjHashMap<>();
    private static final CharSequenceHashSet aggregateFunctionNames = new CharSequenceHashSet();

    private FunctionFactories() {
    }

    public static VirtualColumnFactory<Function> find(Signature sig, ObjList<VirtualColumn> args) {
        final VirtualColumn vc;
        if (sig.paramCount == 2 && (vc = args.getQuick(1)).isConstant()) {

            switch (sig.paramTypes.getQuick(1)) {
                case ColumnType.DOUBLE:
                    double d = vc.getDouble(null);

                    if (d != d) {
                        int columnType = sig.paramTypes.getQuick(0);
                        if (Chars.equals(sig.name, '=')) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleEqualNanOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntEqualNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongEqualNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals("!=", sig.name)) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleNotEqualNanOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntNotEqualNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongNotEqualNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        }
                    }
                    break;
                case ColumnType.STRING:
                    if (vc.getFlyweightStr(null) == null) {
                        int columnType = sig.paramTypes.getQuick(0);
                        if (Chars.equals(sig.name, '=')) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleEqualNanOperator.FACTORY;
                                case ColumnType.STRING:
                                    return StrEqualNullOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntEqualNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongEqualNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals(sig.name, "!=")) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleNotEqualNanOperator.FACTORY;
                                case ColumnType.STRING:
                                    return StrNotEqualNullOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntNotEqualNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongNotEqualNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        VirtualColumnFactory<Function> factory = factories.get(sig);
        if (factory != null) {
            return factory;
        } else {
            // special cases/intrinsic factories
            if (Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.getQuick(0)) {
                    case ColumnType.STRING:
                        return StrInOperator.FACTORY;
                    case ColumnType.SYMBOL:
                        return SymInOperator.FACTORY;
                    case ColumnType.INT:
                    case ColumnType.BYTE:
                    case ColumnType.SHORT:
                        return IntInOperator.FACTORY;
                    case ColumnType.LONG:
                        return LongInOperator.FACTORY;
                    case ColumnType.DATE:
                        if (sig.paramCount < 3) {
                            return null;
                        }
                        return DateInOperator.FACTORY;
                    default:
                        break;
                }
            }
        }
        return null;
    }

    public static boolean isAggregate(CharSequence name) {
        return aggregateFunctionNames.contains(name);
    }

    private static void binSig(String name, int lhst, int rhst, VirtualColumnFactory<Function> f) {
        binSig(name, lhst, false, rhst, false, f);
        binSig(name, lhst, true, rhst, false, f);
        binSig(name, lhst, false, rhst, true, f);
        binSig(name, lhst, true, rhst, true, f);
    }

    private static void binSig(String name, int lhst, boolean lconst, int rhst, boolean rconst, VirtualColumnFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, lconst).paramType(1, rhst, rconst), f);
    }

    private static void binSig(String name, int lhst, int rhst, boolean rconst, VirtualColumnFactory<Function> f) {
        binSig(name, lhst, false, rhst, rconst, f);
        binSig(name, lhst, true, rhst, rconst, f);
    }

    private static void binSig(String name, int lhst, boolean lconst, int rhst, VirtualColumnFactory<Function> f) {
        binSig(name, lhst, lconst, rhst, true, f);
        binSig(name, lhst, lconst, rhst, false, f);
    }

    private static void unSig(String name, int type, VirtualColumnFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, false), f);
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, true), f);
    }

    private static void noargSig(String name, VirtualColumnFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(0), f);
    }

    private static void noargSigAgg(String name, VirtualColumnFactory<Function> f) {
        noargSig(name, f);
        aggregateFunctionNames.add(name);
    }

    private static void unSigAgg(String name, int type, VirtualColumnFactory<Function> f) {
        unSig(name, type, f);
        aggregateFunctionNames.add(name);
    }

    private static void triSig(String name, int one, int two, int three, VirtualColumnFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, false).paramType(1, two, false).paramType(2, three, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, false).paramType(1, two, false).paramType(2, three, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, false).paramType(1, two, true).paramType(2, three, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, false).paramType(1, two, true).paramType(2, three, true), f);

        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, true).paramType(1, two, false).paramType(2, three, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, true).paramType(1, two, false).paramType(2, three, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, true).paramType(1, two, true).paramType(2, three, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, one, true).paramType(1, two, true).paramType(2, three, true), f);
    }

    private static void binSig(String name, VirtualColumnFactory<Function> doubleFactory, VirtualColumnFactory<Function> longFactory, VirtualColumnFactory<Function> intFactory) {
        binSig(name, doubleFactory, longFactory, intFactory, null);
    }

    private static void binSigAgg(String name, VirtualColumnFactory<Function> doubleFactory, VirtualColumnFactory<Function> longFactory, VirtualColumnFactory<Function> intFactory) {
        binSig(name, doubleFactory, longFactory, intFactory, null);
        aggregateFunctionNames.add(name);
    }

    private static void binSig(
            String name,
            VirtualColumnFactory<Function> doubleFactory,
            VirtualColumnFactory<Function> longFactory,
            VirtualColumnFactory<Function> intFactory,
            VirtualColumnFactory<Function> strFactory
    ) {
        binSig(name, ColumnType.DOUBLE, ColumnType.PARAMETER, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.INT, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.LONG, doubleFactory);

        binSig(name, ColumnType.INT, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.INT, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.LONG, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.LONG, ColumnType.FLOAT, doubleFactory);

        binSig(name, ColumnType.FLOAT, ColumnType.PARAMETER, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.LONG, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.INT, doubleFactory);

        binSig(name, ColumnType.LONG, ColumnType.PARAMETER, longFactory);
        binSig(name, ColumnType.LONG, ColumnType.LONG, longFactory);
        binSig(name, ColumnType.LONG, ColumnType.INT, longFactory);

        binSig(name, ColumnType.INT, ColumnType.LONG, longFactory);
        binSig(name, ColumnType.INT, ColumnType.PARAMETER, intFactory);

        binSig(name, ColumnType.INT, ColumnType.INT, intFactory);

        binSig(name, ColumnType.PARAMETER, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.PARAMETER, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.PARAMETER, ColumnType.LONG, longFactory);
        binSig(name, ColumnType.PARAMETER, ColumnType.INT, intFactory);


        binSig(name, ColumnType.SHORT, ColumnType.INT, intFactory);
        binSig(name, ColumnType.INT, ColumnType.SHORT, intFactory);
        binSig(name, ColumnType.SHORT, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.SHORT, doubleFactory);
        binSig(name, ColumnType.SHORT, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.SHORT, doubleFactory);
        binSig(name, ColumnType.SHORT, ColumnType.LONG, longFactory);
        binSig(name, ColumnType.LONG, ColumnType.SHORT, longFactory);

        binSig(name, ColumnType.BYTE, ColumnType.INT, intFactory);
        binSig(name, ColumnType.INT, ColumnType.BYTE, intFactory);
        binSig(name, ColumnType.BYTE, ColumnType.DOUBLE, doubleFactory);
        binSig(name, ColumnType.DOUBLE, ColumnType.BYTE, doubleFactory);
        binSig(name, ColumnType.BYTE, ColumnType.FLOAT, doubleFactory);
        binSig(name, ColumnType.FLOAT, ColumnType.BYTE, doubleFactory);
        binSig(name, ColumnType.BYTE, ColumnType.LONG, longFactory);
        binSig(name, ColumnType.LONG, ColumnType.BYTE, longFactory);
        binSig(name, ColumnType.BYTE, ColumnType.SHORT, intFactory);
        binSig(name, ColumnType.SHORT, ColumnType.BYTE, intFactory);

        if (strFactory != null) {
            binSig(name, ColumnType.STRING, ColumnType.STRING, strFactory);
            binSig(name, ColumnType.PARAMETER, ColumnType.STRING, strFactory);
            binSig(name, ColumnType.STRING, ColumnType.PARAMETER, strFactory);
        }
    }

    static {
        binSig("+", AddDoubleOperator.FACTORY, AddLongOperator.FACTORY, AddIntOperator.FACTORY, StrConcatOperator.FACTORY);
        binSig("+", ColumnType.DATE, ColumnType.LONG, AddDateOperator.FACTORY);
        binSig("+", ColumnType.LONG, ColumnType.DATE, AddDateOperator.FACTORY);
        binSig("+", ColumnType.DATE, ColumnType.DATE, AddDateOperator.FACTORY);
        binSig("+", ColumnType.DATE, ColumnType.INT, AddDateDayLOperator.FACTORY);
        binSig("+", ColumnType.INT, ColumnType.DATE, AddDateDayROperator.FACTORY);

        binSig("*", MultDoubleOperator.FACTORY, MultLongOperator.FACTORY, MultIntOperator.FACTORY);
        binSig("/", DivDoubleOperator.FACTORY, DivDoubleOperator.FACTORY, DivDoubleOperator.FACTORY);
        binSig("-", MinusDoubleOperator.FACTORY, MinusLongOperator.FACTORY, MinusIntOperator.FACTORY);

        binSig(">", DoubleGreaterThanOperator.FACTORY, LongGreaterThanOperator.FACTORY, IntGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.DATE, ColumnType.DATE, DateGreaterThanOperator.FACTORY);
        binSig(">", ColumnType.DATE, ColumnType.STRING, true, DateGreaterThanStrOperator.FACTORY);
        binSig(">", ColumnType.STRING, true, ColumnType.DATE, StrGreaterThanDateOperator.FACTORY);

        binSig(">=", DoubleGreaterOrEqualOperator.FACTORY, LongGreaterOrEqualOperator.FACTORY, IntGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.DATE, ColumnType.DATE, DateGreaterOrEqualOperator.FACTORY);
        binSig(">=", ColumnType.DATE, ColumnType.STRING, true, DateGreaterOrEqualStrOperator.FACTORY);
        binSig(">=", ColumnType.STRING, true, ColumnType.DATE, StrGreaterOrEqualDateOperator.FACTORY);

        binSig("<", DoubleLessThanOperator.FACTORY, LongLessThanOperator.FACTORY, IntLessThanOperator.FACTORY);
        binSig("<", ColumnType.DATE, ColumnType.DATE, DateLessThanOperator.FACTORY);
        binSig("<", ColumnType.DATE, ColumnType.STRING, true, DateLessThanStrOperator.FACTORY);
        binSig("<", ColumnType.STRING, true, ColumnType.DATE, StrLessThanDateOperator.FACTORY);

        binSig("<=", DoubleLessOrEqualOperator.FACTORY, LongLessOrEqualOperator.FACTORY, IntLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.DATE, ColumnType.DATE, LongLessOrEqualOperator.FACTORY);
        binSig("<=", ColumnType.DATE, ColumnType.STRING, true, DateLessOrEqualStrOperator.FACTORY);
        binSig("<=", ColumnType.STRING, true, ColumnType.DATE, StrLessOrEqualDateOperator.FACTORY);

        binSig("=", DoubleEqualOperator.FACTORY, LongEqualOperator.FACTORY, IntEqualOperator.FACTORY, StrEqualStrOperator.FACTORY);
        binSig("=", ColumnType.SYMBOL, false, ColumnType.STRING, false, StrEqualStrOperator.FACTORY);
        binSig("=", ColumnType.SYMBOL, false, ColumnType.STRING, true, SymEqualStrOperator.FACTORY);
        binSig("=", ColumnType.SYMBOL, false, ColumnType.PARAMETER, true, SymEqualStrOperator.FACTORY);
        binSig("=", ColumnType.STRING, true, ColumnType.SYMBOL, false, StrEqualSymOperator.FACTORY);
        binSig("=", ColumnType.DATE, ColumnType.DATE, LongEqualOperator.FACTORY);
        binSig("=", ColumnType.DATE, ColumnType.STRING, true, DateEqualStrConstOperator.FACTORY);
        binSig("=", ColumnType.STRING, true, ColumnType.DATE, StrEqualDateOperator.FACTORY);

        binSig("!=", DoubleNotEqualOperator.FACTORY, LongNotEqualOperator.FACTORY, IntNotEqualOperator.FACTORY, StrNotEqualOperator.FACTORY);
        binSig("!=", ColumnType.DATE, ColumnType.DATE, LongNotEqualOperator.FACTORY);
        binSig("!=", ColumnType.SYMBOL, false, ColumnType.STRING, false, StrNotEqualOperator.FACTORY);
        binSig("!=", ColumnType.SYMBOL, false, ColumnType.STRING, true, SymNotEqualStrOperator.FACTORY);
        binSig("!=", ColumnType.STRING, true, ColumnType.SYMBOL, false, StrNotEqualSymOperator.FACTORY);
        binSig("!=", ColumnType.DATE, ColumnType.STRING, true, DateNotEqualStrConstOperator.FACTORY);
        binSig("!=", ColumnType.STRING, true, ColumnType.DATE, StrNotEqualDateOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualOperator.FACTORY);

        unSig("-", ColumnType.INT, IntNegativeOperator.FACTORY);
        unSig("-", ColumnType.DOUBLE, DoubleNegativeOperator.FACTORY);
        unSig("-", ColumnType.LONG, LongNegativeOperator.FACTORY);

        unSig("not", ColumnType.BOOLEAN, NotOperator.FACTORY);
        unSig("_stoa", ColumnType.SYMBOL, StoAFunction.FACTORY);
        unSig("_atos", ColumnType.STRING, AtoSFunction.FACTORY);
        unSig("atoi", ColumnType.STRING, AtoIFunction.FACTORY);
        unSig("atod", ColumnType.STRING, AtoDFunction.FACTORY);
        unSig("dtol", ColumnType.DATE, DtoLFunction.FACTORY);
        unSig("dtoa4", ColumnType.DATE, DtoA4Function.FACTORY);

        unSig("toDate", ColumnType.LONG, LongToDateFunction.FACTORY);
        unSig("toDate", ColumnType.INT, LongToDateFunction.FACTORY);
        unSig("toDate", ColumnType.STRING, ToDateFunction.FACTORY);
        binSig("toDate", ColumnType.STRING, false, ColumnType.STRING, true, ToDateTwoArgFunction.FACTORY);
        binSig("toDate", ColumnType.STRING, true, ColumnType.STRING, true, ToDateTwoArgFunction.FACTORY);
        factories.put(new Signature().setName("toDate").setParamCount(3).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), ToDateThreeArgFunction.FACTORY);
        factories.put(new Signature().setName("toDate").setParamCount(3).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), ToDateThreeArgFunction.FACTORY);

        binSig("TO_CHAR", ColumnType.DATE, false, ColumnType.STRING, true, DateToCharFunction.FACTORY);
        binSig("TO_CHAR", ColumnType.DATE, true, ColumnType.STRING, true, DateToCharFunction.FACTORY);
        factories.put(new Signature().setName("TO_CHAR").setParamCount(3).paramType(0, ColumnType.DATE, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), DateToCharTZFunction.FACTORY);
        factories.put(new Signature().setName("TO_CHAR").setParamCount(3).paramType(0, ColumnType.DATE, false).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), DateToCharTZFunction.FACTORY);

        factories.put(new Signature().setName("TO_CHAR").setParamCount(4).paramType(0, ColumnType.DATE, false).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true).paramType(3, ColumnType.STRING, true), DateToCharTZLocaleFunction.FACTORY);
        factories.put(new Signature().setName("TO_CHAR").setParamCount(4).paramType(0, ColumnType.DATE, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true).paramType(3, ColumnType.STRING, true), DateToCharTZLocaleFunction.FACTORY);

        unSig("TO_DATE", ColumnType.LONG, LongToDateFunction.FACTORY);
        unSig("TO_DATE", ColumnType.INT, LongToDateFunction.FACTORY);
        unSig("TO_DATE", ColumnType.STRING, ToDateFunction.FACTORY);
        binSig("TO_DATE", ColumnType.STRING, false, ColumnType.STRING, true, ToDateTwoArgFunction.FACTORY);
        binSig("TO_DATE", ColumnType.STRING, true, ColumnType.STRING, true, ToDateTwoArgFunction.FACTORY);
        factories.put(new Signature().setName("TO_DATE").setParamCount(3).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), ToDateThreeArgFunction.FACTORY);
        factories.put(new Signature().setName("TO_DATE").setParamCount(3).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), ToDateThreeArgFunction.FACTORY);


        factories.put(new Signature().setName("SYSDATE").setParamCount(0), SysdateFunction.FACTORY);

        binSig("roundUp", ColumnType.DOUBLE, ColumnType.INT, RoundUpFunction.FACTORY);
        binSig("roundDown", ColumnType.DOUBLE, ColumnType.INT, RoundDownFunction.FACTORY);
        binSig("roundHalfUp", ColumnType.DOUBLE, ColumnType.INT, RoundHalfUpFunction.FACTORY);
        binSig("roundHalfDown", ColumnType.DOUBLE, ColumnType.INT, RoundHalfDownFunction.FACTORY);
        binSig("roundHalfEven", ColumnType.DOUBLE, ColumnType.INT, RoundHalfEvenFunction.FACTORY);

        binSig("~", ColumnType.STRING, false, ColumnType.STRING, true, StrRegexOperator.FACTORY);
        binSig("~", ColumnType.STRING, false, ColumnType.PARAMETER, true, StrRegexOperator.FACTORY);
        binSig("~", ColumnType.SYMBOL, false, ColumnType.STRING, true, SymRegexOperator.FACTORY);
        binSig("~", ColumnType.SYMBOL, false, ColumnType.PARAMETER, true, SymRegexOperator.FACTORY);

        binSig("and", ColumnType.BOOLEAN, ColumnType.BOOLEAN, AndOperator.FACTORY);
        binSig("or", ColumnType.BOOLEAN, ColumnType.BOOLEAN, OrOperator.FACTORY);

        // aggregators
        unSigAgg("sum", ColumnType.DOUBLE, SumDoubleAggregator.FACTORY);
        unSigAgg("sum", ColumnType.FLOAT, SumDoubleAggregator.FACTORY);
        unSigAgg("sum", ColumnType.INT, SumIntAggregator.FACTORY);
        unSigAgg("lsum", ColumnType.INT, SumLongAggregator.FACTORY);
        unSigAgg("sum", ColumnType.LONG, SumLongAggregator.FACTORY);
        unSigAgg("sum", ColumnType.DATE, SumLongAggregator.FACTORY);

        unSigAgg("first", ColumnType.DOUBLE, FirstDoubleAggregator.FACTORY);
        unSigAgg("first", ColumnType.FLOAT, FirstFloatAggregator.FACTORY);
        unSigAgg("first", ColumnType.INT, FirstIntAggregator.FACTORY);
        unSigAgg("first", ColumnType.LONG, FirstLongAggregator.FACTORY);
        unSigAgg("first", ColumnType.DATE, FirstLongAggregator.FACTORY);

        unSigAgg("last", ColumnType.DOUBLE, LastDoubleAggregator.FACTORY);
        unSigAgg("last", ColumnType.FLOAT, LastFloatAggregator.FACTORY);
        unSigAgg("last", ColumnType.INT, LastIntAggregator.FACTORY);
        unSigAgg("last", ColumnType.LONG, LastLongAggregator.FACTORY);
        unSigAgg("last", ColumnType.DATE, LastDateAggregator.FACTORY);

        unSigAgg("avg", ColumnType.DOUBLE, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.INT, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.LONG, AvgAggregator.FACTORY);
        unSigAgg("avg", ColumnType.FLOAT, AvgAggregator.FACTORY);

        unSigAgg("var", ColumnType.DOUBLE, VarAggregator.FACTORY);
        unSigAgg("var", ColumnType.INT, VarAggregator.FACTORY);
        unSigAgg("var", ColumnType.LONG, VarAggregator.FACTORY);
        unSigAgg("var", ColumnType.FLOAT, VarAggregator.FACTORY);

        unSigAgg("stddev", ColumnType.DOUBLE, StdDevAggregator.FACTORY);
        unSigAgg("stddev", ColumnType.INT, StdDevAggregator.FACTORY);
        unSigAgg("stddev", ColumnType.LONG, StdDevAggregator.FACTORY);
        unSigAgg("stddev", ColumnType.FLOAT, StdDevAggregator.FACTORY);

        binSigAgg("vwap", VwapAggregator.FACTORY, VwapAggregator.FACTORY, VwapAggregator.FACTORY);

        unSigAgg("min", ColumnType.DOUBLE, MinDoubleAggregator.FACTORY);
        unSigAgg("min", ColumnType.FLOAT, MinDoubleAggregator.FACTORY);
        unSigAgg("min", ColumnType.INT, MinIntAggregator.FACTORY);
        unSigAgg("min", ColumnType.LONG, MinLongAggregator.FACTORY);
        unSigAgg("min", ColumnType.DATE, MinDateAggregator.FACTORY);

        unSigAgg("max", ColumnType.DOUBLE, MaxDoubleAggregator.FACTORY);
        unSigAgg("max", ColumnType.FLOAT, MaxDoubleAggregator.FACTORY);
        unSigAgg("max", ColumnType.INT, MaxIntAggregator.FACTORY);
        unSigAgg("max", ColumnType.LONG, MaxLongAggregator.FACTORY);
        unSigAgg("max", ColumnType.DATE, MaxDateAggregator.FACTORY);

        noargSigAgg("count", CountAggregator.FACTORY);
        unSigAgg("count", ColumnType.DOUBLE, CountDoubleAggregator.FACTORY);
        unSigAgg("count", ColumnType.FLOAT, CountFloatAggregator.FACTORY);
        unSigAgg("count", ColumnType.INT, CountIntAggregator.FACTORY);
        unSigAgg("count", ColumnType.LONG, CountLongAggregator.FACTORY);
        unSigAgg("count", ColumnType.DATE, CountLongAggregator.FACTORY);
        unSigAgg("count", ColumnType.STRING, CountStrAggregator.FACTORY);
        unSigAgg("count", ColumnType.SYMBOL, CountSymAggregator.FACTORY);

        factories.put(new Signature().setName("pluck").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, false), PluckStrFunction.FACTORY);
        factories.put(new Signature().setName("pluck").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, true), PluckStrFunction.FACTORY);

        factories.put(new Signature().setName("replace").setParamCount(3).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, false), ReplaceStrWrapper.FACTORY);
        factories.put(new Signature().setName("replace").setParamCount(3).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.STRING, true).paramType(2, ColumnType.STRING, true), ReplaceStrWrapper.FACTORY);

        unSig("typeOf", ColumnType.BINARY, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.BOOLEAN, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.BYTE, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.DATE, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.DOUBLE, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.FLOAT, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.INT, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.LONG, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.SHORT, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.STRING, TypeOfFunction.FACTORY);
        unSig("typeOf", ColumnType.SYMBOL, TypeOfFunction.FACTORY);
    }
}
