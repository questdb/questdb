/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Expects a target size, and then pairs of (order size, order amount).
 */
public class LevelTwoPriceFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "l2price(DDDV)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args.size() % 2 == 0) {
            throw SqlException.position(argPositions.getLast()).put("l2price requires an odd number of arguments.");
        }

        final Function target = args.getQuick(0);
        if (target.isNullConstant()) {
            return target;
        }

        validateColumnTypes(args, argPositions, true);

        final int numberOfPairs = (args.size() - 1) / 2;
        if (numberOfPairs == 0) {
            throw SqlException.position(argPositions.getLast()).put("not enough arguments for l2price");
        }
        // have to copy, args is mutable
        args = new ObjList<>(args);
        final IntList positions = new IntList();
        positions.addAll(argPositions);
        switch (numberOfPairs) {
            case 1:
                return new L2PriceFunction1(args, positions);
            case 2:
                return new L2PriceFunction2(args, positions);
            case 3:
                return new L2PriceFunction3(args, positions);
            case 4:
                return new L2PriceFunction4(args, positions);
            case 5:
                return new L2PriceFunction5(args, positions);
            default:
                return new L2PriceFunctionN(args, positions);
        }
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) {
        return ColumnType.DOUBLE;
    }

    private static boolean allowedColumnType(int type, boolean allowUndefined) {
        switch (type) {
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.FLOAT:
            case ColumnType.DOUBLE:
                return true;
            case ColumnType.UNDEFINED:
                return allowUndefined;
            default:
                return false;
        }
    }

    private static void validateColumnTypes(ObjList<Function> args, IntList argPositions, boolean allowUndefined) throws SqlException {
        for (int i = 0, n = args.size(); i < n; i++) {
            if (!allowedColumnType(args.getQuick(i).getType(), allowUndefined)) {
                if (!args.getQuick(i).isNullConstant()) {
                    throw SqlException.position(argPositions.getQuick(i))
                            .put("l2price requires arguments of type `DOUBLE`, or convertible to `DOUBLE`, not `")
                            .put(ColumnType.nameOf(args.getQuick(i).getType()))
                            .put("`.");
                }
            }
        }
    }

    private abstract static class L2PriceBaseFunction extends DoubleFunction implements MultiArgFunction {
        protected final ObjList<Function> args;
        private final IntList argPositions;

        public L2PriceBaseFunction(ObjList<Function> args, IntList argPositions) {
            this.args = args;
            this.argPositions = argPositions;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public String getName() {
            return "l2price";
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            MultiArgFunction.super.init(symbolTableSource, executionContext);
            validateColumnTypes(args, argPositions, false);
        }
    }

    /**
     * Unrolled loop for 1 pair.
     */
    private static class L2PriceFunction1 extends L2PriceBaseFunction {
        private final Function size0Func;
        private final Function targetFunc;
        private final Function value0Func;

        public L2PriceFunction1(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
            targetFunc = args.getQuick(0);
            size0Func = args.getQuick(1);
            value0Func = args.getQuick(2);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = targetFunc.getDouble(rec);
            final double size0 = size0Func.getDouble(rec);
            final double value0 = value0Func.getDouble(rec);

            if (size0 >= target) {
                return value0;
            }
            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 2 pairs.
     */
    private static class L2PriceFunction2 extends L2PriceBaseFunction {
        private final Function size0Func;
        private final Function size1Func;
        private final Function targetFunc;
        private final Function value0Func;
        private final Function value1Func;

        public L2PriceFunction2(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
            targetFunc = args.getQuick(0);
            size0Func = args.getQuick(1);
            value0Func = args.getQuick(2);
            size1Func = args.getQuick(3);
            value1Func = args.getQuick(4);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = targetFunc.getDouble(rec);
            final double size0 = size0Func.getDouble(rec);
            final double value0 = value0Func.getDouble(rec);

            if (size0 >= target) {
                return value0;
            }

            final double size1 = size1Func.getDouble(rec);
            final double value1 = value1Func.getDouble(rec);

            if (size0 + size1 >= target) {
                return ((size0 * value0) + (target - size0) * value1) / target;
            }
            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 3 pairs.
     */
    private static class L2PriceFunction3 extends L2PriceBaseFunction {
        private final Function size0Func;
        private final Function size1Func;
        private final Function size2Func;
        private final Function targetFunc;
        private final Function value0Func;
        private final Function value1Func;
        private final Function value2Func;

        public L2PriceFunction3(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
            targetFunc = args.getQuick(0);
            size0Func = args.getQuick(1);
            value0Func = args.getQuick(2);
            size1Func = args.getQuick(3);
            value1Func = args.getQuick(4);
            size2Func = args.getQuick(5);
            value2Func = args.getQuick(6);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = targetFunc.getDouble(rec);
            final double size0 = size0Func.getDouble(rec);
            final double value0 = value0Func.getDouble(rec);

            if (size0 >= target) {
                return value0;
            }

            final double size1 = size1Func.getDouble(rec);
            final double value1 = value1Func.getDouble(rec);

            if (size0 + size1 >= target) {
                return ((size0 * value0) + (target - size0) * value1) / target;
            }

            final double size2 = size2Func.getDouble(rec);
            final double value2 = value2Func.getDouble(rec);

            if (size0 + size1 + size2 >= target) {
                return ((size0 * value0) + (size1 * value1) + (target - size0 - size1) * value2) / target;
            }
            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 4 pairs.
     */
    private static class L2PriceFunction4 extends L2PriceBaseFunction {
        private final Function size0Func;
        private final Function size1Func;
        private final Function size2Func;
        private final Function size3Func;
        private final Function targetFunc;
        private final Function value0Func;
        private final Function value1Func;
        private final Function value2Func;
        private final Function value3Func;

        public L2PriceFunction4(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
            targetFunc = args.getQuick(0);
            size0Func = args.getQuick(1);
            value0Func = args.getQuick(2);
            size1Func = args.getQuick(3);
            value1Func = args.getQuick(4);
            size2Func = args.getQuick(5);
            value2Func = args.getQuick(6);
            size3Func = args.getQuick(7);
            value3Func = args.getQuick(8);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = targetFunc.getDouble(rec);
            final double size0 = size0Func.getDouble(rec);
            final double value0 = value0Func.getDouble(rec);

            if (size0 >= target) {
                return value0;
            }

            final double size1 = size1Func.getDouble(rec);
            final double value1 = value1Func.getDouble(rec);

            if (size0 + size1 >= target) {
                return ((size0 * value0) + (target - size0) * value1) / target;
            }

            final double size2 = size2Func.getDouble(rec);
            final double value2 = value2Func.getDouble(rec);

            if (size0 + size1 + size2 >= target) {
                return ((size0 * value0) + (size1 * value1) + (target - size0 - size1) * value2) / target;
            }

            final double size3 = size3Func.getDouble(rec);
            final double value3 = value3Func.getDouble(rec);

            if (size0 + size1 + size2 + size3 >= target) {
                return ((size0 * value0) + (size1 * value1) + (size2 * value2) + (target - size0 - size1 - size2) * value3) / target;
            }
            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 5 pairs.
     */
    private static class L2PriceFunction5 extends L2PriceBaseFunction {
        private final Function size0Func;
        private final Function size1Func;
        private final Function size2Func;
        private final Function size3Func;
        private final Function size4Func;
        private final Function targetFunc;
        private final Function value0Func;
        private final Function value1Func;
        private final Function value2Func;
        private final Function value3Func;
        private final Function value4Func;

        public L2PriceFunction5(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
            targetFunc = args.getQuick(0);
            size0Func = args.getQuick(1);
            value0Func = args.getQuick(2);
            size1Func = args.getQuick(3);
            value1Func = args.getQuick(4);
            size2Func = args.getQuick(5);
            value2Func = args.getQuick(6);
            size3Func = args.getQuick(7);
            value3Func = args.getQuick(8);
            size4Func = args.getQuick(9);
            value4Func = args.getQuick(10);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = targetFunc.getDouble(rec);
            final double size0 = size0Func.getDouble(rec);
            final double value0 = value0Func.getDouble(rec);

            if (size0 >= target) {
                return value0;
            }

            final double size1 = size1Func.getDouble(rec);
            final double value1 = value1Func.getDouble(rec);

            if (size0 + size1 >= target) {
                return ((size0 * value0) + (target - size0) * value1) / target;
            }

            final double size2 = size2Func.getDouble(rec);
            final double value2 = value2Func.getDouble(rec);

            if (size0 + size1 + size2 >= target) {
                return ((size0 * value0) + (size1 * value1) + (target - size0 - size1) * value2) / target;
            }

            final double size3 = size3Func.getDouble(rec);
            final double value3 = value3Func.getDouble(rec);

            if (size0 + size1 + size2 + size3 >= target) {
                return ((size0 * value0) + (size1 * value1) + (size2 * value2) + (target - size0 - size1 - size2) * value3) / target;
            }

            final double size4 = size4Func.getDouble(rec);
            final double value4 = value4Func.getDouble(rec);

            if (size0 + size1 + size2 + size3 + size4 >= target) {
                return ((size0 * value0) + (size1 * value1)
                        + (size2 * value2) + (size3 * value3)
                        + (target - size0 - size1 - size2 - size3) * value4) / target;
            }
            return Double.NaN;
        }
    }

    private static class L2PriceFunctionN extends L2PriceBaseFunction {

        public L2PriceFunctionN(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double t = args.getQuick(0).getDouble(rec);

            double ta = 0; // target accumulator
            double pa = 0; // price accumulator
            double rt = t; // reduced target
            double pp; // partial price

            // expect (size, value) pairs
            // equation is
            // ((size[0] * value[0]) + (size[n] * value[n]) + ((target - size[0] - size[n]) * value[n+1])) / target
            // ((ra)             + (rt * value[n+1])) / t
            // get final price by partially filling against the last bin
            for (int i = 1, n = args.size(); i < n; i += 2) {
                final double size = args.getQuick(i).getDouble(rec);
                final double value = args.getQuick(i + 1).getDouble(rec);

                // add size to acc
                ta += size;

                if (ta >= t) {
                    // order is fulfilled
                    if (i == 1) {
                        return value;
                    }

                    pp = rt * value;
                    return (pa + pp) / t;
                } else {
                    // order is not fulfilled, so aggregate partial pricing
                    pa += (size * value);
                    rt -= size;
                }
            }

            // if never exceeded the target, then no price, since it can't be fulfilled
            return Double.NaN;
        }
    }
}
