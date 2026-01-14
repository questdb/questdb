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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a {@link VarargsScalarUDF} as a Griffin {@link FunctionFactory}.
 *
 * @param <I> Input element type
 * @param <O> Output type
 */
public class VarargsScalarUDFFactory<I, O> implements FunctionFactory {

    private final String name;
    private final Class<I> inputType;
    private final Class<O> outputType;
    private final VarargsScalarUDF<I, O> udf;
    private final String signature;

    public VarargsScalarUDFFactory(String name, Class<I> inputType, Class<O> outputType, VarargsScalarUDF<I, O> udf) {
        this.name = name;
        this.inputType = inputType;
        this.outputType = outputType;
        this.udf = udf;
        // V = variadic arguments
        this.signature = name + "(V)";
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
        // Copy args list since it may be reused by the parser
        ObjList<Function> argsCopy = new ObjList<>(args);
        int outputColumnType = UDFType.toColumnType(outputType);

        // Handle decimal types (encoded with precision and scale)
        if (ColumnType.isDecimal(outputColumnType)) {
            return new UDFVarargsDecimalFunction(argsCopy, outputColumnType);
        }

        return switch (ColumnType.tagOf(outputColumnType)) {
            case ColumnType.DOUBLE -> new UDFVarargsDoubleFunction(argsCopy);
            case ColumnType.LONG -> new UDFVarargsLongFunction(argsCopy);
            case ColumnType.INT -> new UDFVarargsIntFunction(argsCopy);
            case ColumnType.STRING -> new UDFVarargsStrFunction(argsCopy);
            default -> throw new IllegalArgumentException("Unsupported output type: " + outputType);
        };
    }

    @SuppressWarnings("unchecked")
    private I extractInput(Function arg, Record rec) {
        // Handle BigDecimal input specially
        if (inputType == BigDecimal.class) {
            int argType = arg.getType();
            if (ColumnType.isDecimal(argType)) {
                int scale = ColumnType.getDecimalScale(argType);
                long rawValue = arg.getDecimal64(rec);
                if (rawValue == Decimals.DECIMAL64_NULL) {
                    return null;
                }
                return (I) BigDecimal.valueOf(rawValue, scale);
            }
            // Fallback: try to get as double and convert
            double v = arg.getDouble(rec);
            return (I) (Double.isNaN(v) ? null : BigDecimal.valueOf(v));
        }

        int inputColumnType = UDFType.toColumnType(inputType);
        return (I) switch (ColumnType.tagOf(inputColumnType)) {
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
            default -> throw new IllegalArgumentException("Unsupported input type: " + inputType);
        };
    }

    private void extractInputsInto(ObjList<Function> args, Record rec, List<I> inputs) {
        inputs.clear();
        for (int i = 0, n = args.size(); i < n; i++) {
            inputs.add(extractInput(args.getQuick(i), rec));
        }
    }

    private O safeCompute(List<I> inputs) {
        try {
            return udf.compute(inputs);
        } catch (Exception e) {
            throw CairoException.nonCritical()
                    .put("UDF '")
                    .put(name)
                    .put("' threw exception: ")
                    .put(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
    }

    private class UDFVarargsDoubleFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final List<I> reusableInputs;

        UDFVarargsDoubleFunction(ObjList<Function> args) {
            this.args = args;
            this.reusableInputs = new ArrayList<>(args.size());
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            return result == null ? Double.NaN : ((Number) result).doubleValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsLongFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final List<I> reusableInputs;

        UDFVarargsLongFunction(ObjList<Function> args) {
            this.args = args;
            this.reusableInputs = new ArrayList<>(args.size());
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            return result == null ? Long.MIN_VALUE : ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsIntFunction extends IntFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final List<I> reusableInputs;

        UDFVarargsIntFunction(ObjList<Function> args) {
            this.args = args;
            this.reusableInputs = new ArrayList<>(args.size());
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            return result == null ? Integer.MIN_VALUE : ((Number) result).intValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsStrFunction extends StrFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final List<I> reusableInputs;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        UDFVarargsStrFunction(ObjList<Function> args) {
            this.args = args;
            this.reusableInputs = new ArrayList<>(args.size());
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            if (result == null) {
                return null;
            }
            sinkA.clear();
            sinkA.put(result.toString());
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            if (result == null) {
                return null;
            }
            sinkB.clear();
            sinkB.put(result.toString());
            return sinkB;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private class UDFVarargsDecimalFunction extends Decimal64Function implements MultiArgFunction {
        private final ObjList<Function> args;
        private final List<I> reusableInputs;
        private final int scale;

        UDFVarargsDecimalFunction(ObjList<Function> args, int decimalType) {
            super(decimalType);
            this.args = args;
            this.reusableInputs = new ArrayList<>(args.size());
            this.scale = ColumnType.getDecimalScale(decimalType);
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public long getDecimal64(Record rec) {
            extractInputsInto(args, rec, reusableInputs);
            O result = safeCompute(reusableInputs);
            if (result == null) {
                return Decimals.DECIMAL64_NULL;
            }
            if (result instanceof BigDecimal bd) {
                return bd.setScale(scale, java.math.RoundingMode.HALF_UP)
                        .movePointRight(scale)
                        .longValue();
            }
            // For other numeric types, convert via BigDecimal
            BigDecimal bd = BigDecimal.valueOf(((Number) result).doubleValue());
            return bd.setScale(scale, java.math.RoundingMode.HALF_UP)
                    .movePointRight(scale)
                    .longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
