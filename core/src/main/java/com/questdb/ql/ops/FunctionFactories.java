/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.misc.Chars;
import com.questdb.ql.ops.conv.*;
import com.questdb.ql.ops.count.*;
import com.questdb.ql.ops.div.DivDoubleOperator;
import com.questdb.ql.ops.eq.*;
import com.questdb.ql.ops.first.FirstDoubleAggregator;
import com.questdb.ql.ops.first.FirstFloatAggregator;
import com.questdb.ql.ops.first.FirstIntAggregator;
import com.questdb.ql.ops.first.FirstLongAggregator;
import com.questdb.ql.ops.gt.DoubleGreaterThanOperator;
import com.questdb.ql.ops.gt.IntGreaterThanOperator;
import com.questdb.ql.ops.gt.LongGreaterThanOperator;
import com.questdb.ql.ops.gte.DoubleGreaterOrEqualOperator;
import com.questdb.ql.ops.gte.IntGreaterOrEqualOperator;
import com.questdb.ql.ops.gte.LongGreaterOrEqualOperator;
import com.questdb.ql.ops.last.*;
import com.questdb.ql.ops.lt.DoubleLessThanOperator;
import com.questdb.ql.ops.lt.IntLessThanOperator;
import com.questdb.ql.ops.lt.LongLessThanOperator;
import com.questdb.ql.ops.lte.DoubleLessOrEqualOperator;
import com.questdb.ql.ops.lte.IntLessOrEqualOperator;
import com.questdb.ql.ops.lte.LongLessOrEqualOperator;
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
import com.questdb.ql.ops.stat.AvgAggregator;
import com.questdb.ql.ops.stat.StdDevAggregator;
import com.questdb.ql.ops.stat.VarAggregator;
import com.questdb.ql.ops.sum.SumDoubleAggregator;
import com.questdb.ql.ops.sum.SumIntAggregator;
import com.questdb.ql.ops.sum.SumLongAggregator;
import com.questdb.std.CharSequenceHashSet;
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
                                    return DoubleEqualsNanOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntEqualsNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongEqualsNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals("!=", sig.name)) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleNotEqualsNanOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntNotEqualsNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongNotEqualsNaNOperator.FACTORY;
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
                                    return DoubleEqualsNanOperator.FACTORY;
                                case ColumnType.STRING:
                                    return StrEqualsNullOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntEqualsNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongEqualsNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals(sig.name, "!=")) {
                            switch (columnType) {
                                case ColumnType.DOUBLE:
                                    return DoubleNotEqualsNanOperator.FACTORY;
                                case ColumnType.STRING:
                                    return StrNotEqualsNullOperator.FACTORY;
                                case ColumnType.INT:
                                    return IntNotEqualsNaNOperator.FACTORY;
                                case ColumnType.LONG:
                                case ColumnType.DATE:
                                    return LongNotEqualsNaNOperator.FACTORY;
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
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, true), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, true), f);
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

    private static void triSig(String name, int lhst, int rhst, int scale, VirtualColumnFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, true), f);

        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, true), f);
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
        binSig("+", ColumnType.DATE, ColumnType.INT, AddDateDayLOperator.FACTORY);
        binSig("+", ColumnType.INT, ColumnType.DATE, AddDateDayROperator.FACTORY);

        binSig(">", ColumnType.DATE, ColumnType.DATE, LongGreaterThanOperator.FACTORY);
        binSig(">=", ColumnType.DATE, ColumnType.DATE, LongGreaterOrEqualOperator.FACTORY);
        binSig("<", ColumnType.DATE, ColumnType.DATE, LongLessThanOperator.FACTORY);
        binSig("<=", ColumnType.DATE, ColumnType.DATE, LongLessOrEqualOperator.FACTORY);
        binSig("=", ColumnType.DATE, ColumnType.DATE, LongEqualsOperator.FACTORY);
        binSig("!=", ColumnType.DATE, ColumnType.DATE, LongNotEqualsOperator.FACTORY);

        binSig("*", MultDoubleOperator.FACTORY, MultLongOperator.FACTORY, MultIntOperator.FACTORY);
        binSig("/", DivDoubleOperator.FACTORY, DivDoubleOperator.FACTORY, DivDoubleOperator.FACTORY);
        binSig("-", MinusDoubleOperator.FACTORY, MinusLongOperator.FACTORY, MinusIntOperator.FACTORY);
        binSig(">", DoubleGreaterThanOperator.FACTORY, LongGreaterThanOperator.FACTORY, IntGreaterThanOperator.FACTORY);
        binSig(">=", DoubleGreaterOrEqualOperator.FACTORY, LongGreaterOrEqualOperator.FACTORY, IntGreaterOrEqualOperator.FACTORY);
        binSig("<", DoubleLessThanOperator.FACTORY, LongLessThanOperator.FACTORY, IntLessThanOperator.FACTORY);
        binSig("<=", DoubleLessOrEqualOperator.FACTORY, LongLessOrEqualOperator.FACTORY, IntLessOrEqualOperator.FACTORY);
        binSig("=", DoubleEqualsOperator.FACTORY, LongEqualsOperator.FACTORY, IntEqualsOperator.FACTORY, StrEqualsOperator.FACTORY);

        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), StrEqualsOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymEqualsOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), SymEqualsROperator.FACTORY);

        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.DATE, false).paramType(1, ColumnType.STRING, true), DateEqualsStrConstOperator.FACTORY);
        factories.put(new Signature().setName("=").setParamCount(2).paramType(0, ColumnType.DATE, false).paramType(1, ColumnType.STRING, false), DateEqualsStrConstOperator.FACTORY);

        binSig("!=", DoubleNotEqualsOperator.FACTORY, LongNotEqualsOperator.FACTORY, IntNotEqualsOperator.FACTORY, StrNotEqualsOperator.FACTORY);

        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), StrNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), SymNotEqualsROperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.PARAMETER, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.PARAMETER, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.PARAMETER, DoubleScaledEqualsOperator.FACTORY);

        unSig("-", ColumnType.INT, IntNegativeOperator.FACTORY);
        unSig("-", ColumnType.DOUBLE, DoubleNegativeOperator.FACTORY);
        unSig("-", ColumnType.LONG, LongNegativeOperator.FACTORY);

        unSig("not", ColumnType.BOOLEAN, NotOperator.FACTORY);
        unSig("_stoa", ColumnType.SYMBOL, StoAFunction.FACTORY);
        unSig("_atos", ColumnType.STRING, AtoSFunction.FACTORY);
        unSig("atoi", ColumnType.STRING, AtoIFunction.FACTORY);
        unSig("atod", ColumnType.STRING, AtoDFunction.FACTORY);
        unSig("ltod", ColumnType.LONG, LtoDFunction.FACTORY);
        unSig("ltod", ColumnType.INT, LtoDFunction.FACTORY);
        unSig("dtol", ColumnType.DATE, DtoLFunction.FACTORY);
        unSig("dtoa4", ColumnType.DATE, DtoA4Function.FACTORY);
        unSig("round", ColumnType.DOUBLE, RoundFunction.FACTORY);
        unSig("time24", ColumnType.STRING, Time24ToMillisFunction.FACTORY);
        unSig("toDate", ColumnType.STRING, ToDateFunction.FACTORY);

        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true), StrRegexOperator.FACTORY);
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.PARAMETER, true), StrRegexOperator.FACTORY);
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymRegexOperator.FACTORY);
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.PARAMETER, true), SymRegexOperator.FACTORY);
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
