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
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

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

        return switch (outputColumnType) {
            case ColumnType.DOUBLE -> new UDFVarargsDoubleFunction(argsCopy);
            case ColumnType.LONG -> new UDFVarargsLongFunction(argsCopy);
            case ColumnType.INT -> new UDFVarargsIntFunction(argsCopy);
            case ColumnType.STRING -> new UDFVarargsStrFunction(argsCopy);
            default -> throw new IllegalArgumentException("Unsupported output type: " + outputType);
        };
    }

    @SuppressWarnings("unchecked")
    private I extractInput(Function arg, Record rec) {
        int inputColumnType = UDFType.toColumnType(inputType);
        return (I) switch (inputColumnType) {
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

    private List<I> extractInputs(ObjList<Function> args, Record rec) {
        List<I> inputs = new ArrayList<>(args.size());
        for (int i = 0, n = args.size(); i < n; i++) {
            inputs.add(extractInput(args.getQuick(i), rec));
        }
        return inputs;
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

        UDFVarargsDoubleFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            List<I> inputs = extractInputs(args, rec);
            O result = safeCompute(inputs);
            return result == null ? Double.NaN : ((Number) result).doubleValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsLongFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        UDFVarargsLongFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            List<I> inputs = extractInputs(args, rec);
            O result = safeCompute(inputs);
            return result == null ? Long.MIN_VALUE : ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsIntFunction extends IntFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        UDFVarargsIntFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            List<I> inputs = extractInputs(args, rec);
            O result = safeCompute(inputs);
            return result == null ? Integer.MIN_VALUE : ((Number) result).intValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFVarargsStrFunction extends StrFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        UDFVarargsStrFunction(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            List<I> inputs = extractInputs(args, rec);
            O result = safeCompute(inputs);
            if (result == null) {
                return null;
            }
            sinkA.clear();
            sinkA.put(result.toString());
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            List<I> inputs = extractInputs(args, rec);
            O result = safeCompute(inputs);
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
}
