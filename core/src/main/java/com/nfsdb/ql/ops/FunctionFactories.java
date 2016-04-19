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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.ops;

import com.nfsdb.misc.Chars;
import com.nfsdb.ql.ops.conv.*;
import com.nfsdb.ql.ops.count.*;
import com.nfsdb.ql.ops.div.DivDoubleOperator;
import com.nfsdb.ql.ops.eq.*;
import com.nfsdb.ql.ops.first.FirstDoubleAggregator;
import com.nfsdb.ql.ops.first.FirstFloatAggregator;
import com.nfsdb.ql.ops.first.FirstIntAggregator;
import com.nfsdb.ql.ops.first.FirstLongAggregator;
import com.nfsdb.ql.ops.gt.DoubleGreaterThanOperator;
import com.nfsdb.ql.ops.gt.IntGreaterThanOperator;
import com.nfsdb.ql.ops.gt.LongGreaterThanOperator;
import com.nfsdb.ql.ops.gte.DoubleGreaterOrEqualOperator;
import com.nfsdb.ql.ops.gte.IntGreaterOrEqualOperator;
import com.nfsdb.ql.ops.gte.LongGreaterOrEqualOperator;
import com.nfsdb.ql.ops.last.*;
import com.nfsdb.ql.ops.lt.DoubleLessThanOperator;
import com.nfsdb.ql.ops.lt.IntLessThanOperator;
import com.nfsdb.ql.ops.lt.LongLessThanOperator;
import com.nfsdb.ql.ops.lte.DoubleLessOrEqualOperator;
import com.nfsdb.ql.ops.lte.IntLessOrEqualOperator;
import com.nfsdb.ql.ops.lte.LongLessOrEqualOperator;
import com.nfsdb.ql.ops.max.MaxDateAggregator;
import com.nfsdb.ql.ops.max.MaxDoubleAggregator;
import com.nfsdb.ql.ops.max.MaxIntAggregator;
import com.nfsdb.ql.ops.max.MaxLongAggregator;
import com.nfsdb.ql.ops.min.MinDateAggregator;
import com.nfsdb.ql.ops.min.MinDoubleAggregator;
import com.nfsdb.ql.ops.min.MinIntAggregator;
import com.nfsdb.ql.ops.min.MinLongAggregator;
import com.nfsdb.ql.ops.minus.MinusDoubleOperator;
import com.nfsdb.ql.ops.minus.MinusIntOperator;
import com.nfsdb.ql.ops.minus.MinusLongOperator;
import com.nfsdb.ql.ops.mult.MultDoubleOperator;
import com.nfsdb.ql.ops.mult.MultIntOperator;
import com.nfsdb.ql.ops.mult.MultLongOperator;
import com.nfsdb.ql.ops.neg.DoubleNegativeOperator;
import com.nfsdb.ql.ops.neg.IntNegativeOperator;
import com.nfsdb.ql.ops.neg.LongNegativeOperator;
import com.nfsdb.ql.ops.neq.*;
import com.nfsdb.ql.ops.plus.AddDoubleOperator;
import com.nfsdb.ql.ops.plus.AddIntOperator;
import com.nfsdb.ql.ops.plus.AddLongOperator;
import com.nfsdb.ql.ops.plus.StrConcatOperator;
import com.nfsdb.ql.ops.sum.SumDoubleAggregator;
import com.nfsdb.ql.ops.sum.SumIntAggregator;
import com.nfsdb.ql.ops.sum.SumLongAggregator;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjObjHashMap;
import com.nfsdb.std.ObjectFactory;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class FunctionFactories {

    private static final ObjObjHashMap<Signature, ObjectFactory<Function>> factories = new ObjObjHashMap<>();
    private static final CharSequenceHashSet aggregateFunctionNames = new CharSequenceHashSet();

    private FunctionFactories() {
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public static ObjectFactory<Function> find(Signature sig, ObjList<VirtualColumn> args) {
        final VirtualColumn vc;
        if (sig.paramCount == 2 && (vc = args.getQuick(1)).isConstant()) {

            switch (sig.paramTypes.getQuick(1)) {
                case DOUBLE:
                    double d = vc.getDouble(null);

                    if (d != d) {
                        ColumnType t = sig.paramTypes.getQuick(0);
                        if (Chars.equals(sig.name, '=')) {
                            switch (t) {
                                case DOUBLE:
                                    return DoubleEqualsNanOperator.FACTORY;
                                case INT:
                                    return IntEqualsNaNOperator.FACTORY;
                                case LONG:
                                case DATE:
                                    return LongEqualsNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals("!=", sig.name)) {
                            switch (t) {
                                case DOUBLE:
                                    return DoubleNotEqualsNanOperator.FACTORY;
                                case INT:
                                    return IntNotEqualsNaNOperator.FACTORY;
                                case LONG:
                                case DATE:
                                    return LongNotEqualsNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        }
                    }
                    break;
                case STRING:
                    if (vc.getFlyweightStr(null) == null) {
                        ColumnType t = sig.paramTypes.getQuick(0);
                        if (Chars.equals(sig.name, '=')) {
                            switch (t) {
                                case DOUBLE:
                                    return DoubleEqualsNanOperator.FACTORY;
                                case STRING:
                                    return StrEqualsNullOperator.FACTORY;
                                case INT:
                                    return IntEqualsNaNOperator.FACTORY;
                                case LONG:
                                case DATE:
                                    return LongEqualsNaNOperator.FACTORY;
                                default:
                                    break;
                            }
                        } else if (Chars.equals(sig.name, "!=")) {
                            switch (t) {
                                case DOUBLE:
                                    return DoubleNotEqualsNanOperator.FACTORY;
                                case STRING:
                                    return StrNotEqualsNullOperator.FACTORY;
                                case INT:
                                    return IntNotEqualsNaNOperator.FACTORY;
                                case LONG:
                                case DATE:
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

        ObjectFactory<Function> factory = factories.get(sig);
        if (factory != null) {
            return factory;
        } else {
            // special cases/intrinsic factories
            if (Chars.equals("in", sig.name)) {
                switch (sig.paramTypes.getQuick(0)) {
                    case STRING:
                        return StrInOperator.FACTORY;
                    case SYMBOL:
                        return SymInOperator.FACTORY;
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

    private static void binSig(String name, ColumnType lhst, ColumnType rhst, ObjectFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, false), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, false).paramType(1, rhst, true), f);
        factories.put(new Signature().setName(name).setParamCount(2).paramType(0, lhst, true).paramType(1, rhst, true), f);
    }

    private static void unSig(String name, ColumnType type, ObjectFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, false), f);
        factories.put(new Signature().setName(name).setParamCount(1).paramType(0, type, true), f);
    }

    private static void noargSig(String name, ObjectFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(0), f);
    }

    private static void noargSigAgg(String name, ObjectFactory<Function> f) {
        noargSig(name, f);
        aggregateFunctionNames.add(name);
    }

    private static void unSigAgg(String name, ColumnType type, ObjectFactory<Function> f) {
        unSig(name, type, f);
        aggregateFunctionNames.add(name);
    }

    private static void triSig(String name, ColumnType lhst, ColumnType rhst, ColumnType scale, ObjectFactory<Function> f) {
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, false).paramType(1, rhst, true).paramType(2, scale, true), f);

        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, false).paramType(2, scale, true), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, false), f);
        factories.put(new Signature().setName(name).setParamCount(3).paramType(0, lhst, true).paramType(1, rhst, true).paramType(2, scale, true), f);
    }

    private static void binSig(String name, ObjectFactory<Function> doubleFactory, ObjectFactory<Function> longFactory, ObjectFactory<Function> intFactory) {
        binSig(name, doubleFactory, longFactory, intFactory, null);
    }

    private static void binSigAgg(String name, ObjectFactory<Function> doubleFactory, ObjectFactory<Function> longFactory, ObjectFactory<Function> intFactory) {
        binSig(name, doubleFactory, longFactory, intFactory, null);
        aggregateFunctionNames.add(name);
    }

    private static void binSig(
            String name,
            ObjectFactory<Function> doubleFactory,
            ObjectFactory<Function> longFactory,
            ObjectFactory<Function> intFactory,
            ObjectFactory<Function> strFactory
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
        }
    }

    static {
        binSig("+", AddDoubleOperator.FACTORY, AddLongOperator.FACTORY, AddIntOperator.FACTORY, StrConcatOperator.FACTORY);
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

        binSig("!=", DoubleNotEqualsOperator.FACTORY, LongNotEqualsOperator.FACTORY, IntNotEqualsOperator.FACTORY, StrNotEqualsOperator.FACTORY);

        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, false), StrNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymNotEqualsOperator.FACTORY);
        factories.put(new Signature().setName("!=").setParamCount(2).paramType(0, ColumnType.STRING, true).paramType(1, ColumnType.SYMBOL, false), SymNotEqualsROperator.FACTORY);

        triSig("eq", ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.INT, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.INT, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.DOUBLE, ColumnType.LONG, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);
        triSig("eq", ColumnType.LONG, ColumnType.DOUBLE, ColumnType.DOUBLE, DoubleScaledEqualsOperator.FACTORY);

        unSig("-", ColumnType.INT, IntNegativeOperator.FACTORY);
        unSig("-", ColumnType.DOUBLE, DoubleNegativeOperator.FACTORY);
        unSig("-", ColumnType.LONG, LongNegativeOperator.FACTORY);

        unSig("not", ColumnType.BOOLEAN, NotOperator.FACTORY);
        unSig("_stoa", ColumnType.SYMBOL, StoAFunction.FACTORY);
        unSig("_atos", ColumnType.STRING, AtoSFunction.FACTORY);
        unSig("atoi", ColumnType.STRING, AtoIFunction.FACTORY);
        unSig("ltod", ColumnType.LONG, LtoDFunction.FACTORY);
        unSig("dtol", ColumnType.DATE, DtoLFunction.FACTORY);

        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.STRING, false).paramType(1, ColumnType.STRING, true), StrRegexOperator.FACTORY);
        factories.put(new Signature().setName("~").setParamCount(2).paramType(0, ColumnType.SYMBOL, false).paramType(1, ColumnType.STRING, true), SymRegexOperator.FACTORY);
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
    }
}
