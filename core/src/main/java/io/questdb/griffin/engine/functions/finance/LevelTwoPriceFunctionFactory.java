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
        return "l2price(DV)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
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
        final IntList positions = new IntList();
        positions.addAll(argPositions);
        switch (numberOfPairs) {
            case 1:
                return new L2PriceFunction1(new ObjList<>(args), positions);
            case 2:
                return new L2PriceFunction2(new ObjList<>(args), positions);
            case 3:
                return new L2PriceFunction3(new ObjList<>(args), positions);
            case 4:
                return new L2PriceFunction4(new ObjList<>(args), positions);
            case 5:
                return new L2PriceFunction5(new ObjList<>(args), positions);
            default:
                return new L2PriceFunctionN(new ObjList<>(args), positions);
        }
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
        final IntList argPositions;
        final ObjList<Function> args;

        L2PriceBaseFunction(ObjList<Function> args, IntList argPositions) {
            this.args = args;
            this.argPositions = argPositions;
        }

        @Override
        public ObjList<Function> getArgs() {
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
        public L2PriceFunction1(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = args.getQuick(0).getDouble(rec);
            final double size_0 = args.getQuick(1).getDouble(rec);
            final double value_0 = args.getQuick(2).getDouble(rec);

            if (size_0 >= target)
                return value_0;

            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 2 pairs.
     */
    private static class L2PriceFunction2 extends L2PriceBaseFunction {
        public L2PriceFunction2(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = args.getQuick(0).getDouble(rec);
            final double size_0 = args.getQuick(1).getDouble(rec);
            final double value_0 = args.getQuick(2).getDouble(rec);

            if (size_0 >= target)
                return value_0;

            final double size_1 = args.getQuick(3).getDouble(rec);
            final double value_1 = args.getQuick(4).getDouble(rec);

            if (size_0 + size_1 >= target) {
                return ((size_0 * value_0) + (target - size_0) * value_1) / target;
            }

            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 3 pairs.
     */
    private static class L2PriceFunction3 extends L2PriceBaseFunction {
        public L2PriceFunction3(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = args.getQuick(0).getDouble(rec);
            final double size_0 = args.getQuick(1).getDouble(rec);
            final double value_0 = args.getQuick(2).getDouble(rec);

            if (size_0 >= target)
                return value_0;

            final double size_1 = args.getQuick(3).getDouble(rec);
            final double value_1 = args.getQuick(4).getDouble(rec);

            if (size_0 + size_1 >= target) {
                return ((size_0 * value_0) + (target - size_0) * value_1) / target;
            }

            final double size_2 = args.getQuick(5).getDouble(rec);
            final double value_2 = args.getQuick(6).getDouble(rec);

            if (size_0 + size_1 + size_2 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (target - size_0 - size_1) * value_2) / target;
            }

            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 4 pairs.
     */
    private static class L2PriceFunction4 extends L2PriceBaseFunction {
        public L2PriceFunction4(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = args.getQuick(0).getDouble(rec);
            final double size_0 = args.getQuick(1).getDouble(rec);
            final double value_0 = args.getQuick(2).getDouble(rec);

            if (size_0 >= target)
                return value_0;

            final double size_1 = args.getQuick(3).getDouble(rec);
            final double value_1 = args.getQuick(4).getDouble(rec);

            if (size_0 + size_1 >= target) {
                return ((size_0 * value_0) + (target - size_0) * value_1) / target;
            }

            final double size_2 = args.getQuick(5).getDouble(rec);
            final double value_2 = args.getQuick(6).getDouble(rec);

            if (size_0 + size_1 + size_2 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (target - size_0 - size_1) * value_2) / target;
            }

            final double size_3 = args.getQuick(7).getDouble(rec);
            final double value_3 = args.getQuick(8).getDouble(rec);

            if (size_0 + size_1 + size_2 + size_3 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (size_2 * value_2)
                        + (target - size_0 - size_1 - size_2) * value_3) / target;
            }

            return Double.NaN;
        }
    }

    /**
     * Unrolled loop for 5 pairs.
     */
    private static class L2PriceFunction5 extends L2PriceBaseFunction {
        public L2PriceFunction5(ObjList<Function> args, IntList argPositions) {
            super(args, argPositions);
        }

        @Override
        public double getDouble(Record rec) {
            final double target = args.getQuick(0).getDouble(rec);
            final double size_0 = args.getQuick(1).getDouble(rec);
            final double value_0 = args.getQuick(2).getDouble(rec);


            if (size_0 >= target)
                return value_0;

            final double size_1 = args.getQuick(3).getDouble(rec);
            final double value_1 = args.getQuick(4).getDouble(rec);

            if (size_0 + size_1 >= target) {
                return ((size_0 * value_0) + (target - size_0) * value_1) / target;
            }

            final double size_2 = args.getQuick(5).getDouble(rec);
            final double value_2 = args.getQuick(6).getDouble(rec);

            if (size_0 + size_1 + size_2 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (target - size_0 - size_1) * value_2) / target;
            }

            final double size_3 = args.getQuick(7).getDouble(rec);
            final double value_3 = args.getQuick(8).getDouble(rec);

            if (size_0 + size_1 + size_2 + size_3 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (size_2 * value_2)
                        + (target - size_0 - size_1 - size_2) * value_3) / target;
            }

            final double size_4 = args.getQuick(9).getDouble(rec);
            final double value_4 = args.getQuick(10).getDouble(rec);

            if (size_0 + size_1 + size_2 + size_3 + size_4 >= target) {
                return ((size_0 * value_0)
                        + (size_1 * value_1)
                        + (size_2 * value_2)
                        + (size_3 * value_3)
                        + (target - size_0 - size_1 - size_2 - size_3) * value_4) / target;
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
