/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

package io.questdb.griffin.udf;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Wraps a {@link BinaryScalarUDF} as a Griffin {@link FunctionFactory}.
 *
 * @param <I1> First input type
 * @param <I2> Second input type
 * @param <O>  Output type
 */
public class BinaryScalarUDFFactory<I1, I2, O> implements FunctionFactory {

    private final Class<I1> inputType1;
    private final Class<I2> inputType2;
    private final String name;
    private final Class<O> outputType;
    private final String signature;
    private final BinaryScalarUDF<I1, I2, O> udf;

    public BinaryScalarUDFFactory(
            String name,
            Class<I1> inputType1,
            Class<I2> inputType2,
            Class<O> outputType,
            BinaryScalarUDF<I1, I2, O> udf
    ) {
        this.name = name;
        this.inputType1 = inputType1;
        this.inputType2 = inputType2;
        this.outputType = outputType;
        this.udf = udf;
        this.signature = UDFType.buildSignature(name, inputType1, inputType2);
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function left = args.getQuick(0);
        Function right = args.getQuick(1);
        int outputColumnType = UDFType.toColumnType(outputType);

        return switch (outputColumnType) {
            case ColumnType.DOUBLE -> createDoubleFunction(left, right);
            case ColumnType.LONG -> createLongFunction(left, right);
            case ColumnType.INT -> createIntFunction(left, right);
            case ColumnType.STRING -> createStrFunction(left, right);
            default -> throw new IllegalArgumentException("Unsupported output type: " + outputType);
        };
    }

    private Function createDoubleFunction(Function left, Function right) {
        return new UDFBinaryDoubleFunction(left, right);
    }

    private Function createIntFunction(Function left, Function right) {
        return new UDFBinaryIntFunction(left, right);
    }

    private Function createLongFunction(Function left, Function right) {
        return new UDFBinaryLongFunction(left, right);
    }

    private Function createStrFunction(Function left, Function right) {
        return new UDFBinaryStrFunction(left, right);
    }

    @SuppressWarnings("unchecked")
    private <T> T extractInput(Function arg, Record rec, Class<T> type) {
        int columnType = UDFType.toColumnType(type);
        return (T) switch (columnType) {
            case ColumnType.DOUBLE -> {
                double v = arg.getDouble(rec);
                yield Double.isNaN(v) ? null : v;
            }
            case ColumnType.LONG -> {
                long v = arg.getLong(rec);
                yield v == Long.MIN_VALUE ? null : v;
            }
            case ColumnType.INT -> {
                int v = arg.getInt(rec);
                yield v == Integer.MIN_VALUE ? null : v;
            }
            case ColumnType.STRING -> {
                CharSequence cs = arg.getStrA(rec);
                yield cs == null ? null : cs.toString();
            }
            case ColumnType.BOOLEAN -> arg.getBool(rec);
            case ColumnType.FLOAT -> {
                float v = arg.getFloat(rec);
                yield Float.isNaN(v) ? null : v;
            }
            default -> throw new IllegalArgumentException("Unsupported input type: " + type);
        };
    }

    private class UDFBinaryDoubleFunction extends DoubleFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        UDFBinaryDoubleFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public double getDouble(Record rec) {
            I1 input1 = extractInput(left, rec, inputType1);
            I2 input2 = extractInput(right, rec, inputType2);
            O result = udf.compute(input1, input2);
            return result == null ? Double.NaN : ((Number) result).doubleValue();
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private class UDFBinaryIntFunction extends IntFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        UDFBinaryIntFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int getInt(Record rec) {
            I1 input1 = extractInput(left, rec, inputType1);
            I2 input2 = extractInput(right, rec, inputType2);
            O result = udf.compute(input1, input2);
            return result == null ? Integer.MIN_VALUE : ((Number) result).intValue();
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private class UDFBinaryLongFunction extends LongFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        UDFBinaryLongFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            I1 input1 = extractInput(left, rec, inputType1);
            I2 input2 = extractInput(right, rec, inputType2);
            O result = udf.compute(input1, input2);
            return result == null ? Long.MIN_VALUE : ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private class UDFBinaryStrFunction extends StrFunction implements BinaryFunction {
        private final Function left;
        private final Function right;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        UDFBinaryStrFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            I1 input1 = extractInput(left, rec, inputType1);
            I2 input2 = extractInput(right, rec, inputType2);
            O result = udf.compute(input1, input2);
            if (result == null) {
                return null;
            }
            sinkA.clear();
            sinkA.put(result.toString());
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            I1 input1 = extractInput(left, rec, inputType1);
            I2 input2 = extractInput(right, rec, inputType2);
            O result = udf.compute(input1, input2);
            if (result == null) {
                return null;
            }
            sinkB.clear();
            sinkB.put(result.toString());
            return sinkB;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
