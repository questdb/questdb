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
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Wraps a {@link ScalarUDF} as a Griffin {@link FunctionFactory}.
 *
 * @param <I> Input type
 * @param <O> Output type
 */
public class ScalarUDFFactory<I, O> implements FunctionFactory {

    private final String name;
    private final Class<I> inputType;
    private final Class<O> outputType;
    private final ScalarUDF<I, O> udf;
    private final String signature;

    public ScalarUDFFactory(String name, Class<I> inputType, Class<O> outputType, ScalarUDF<I, O> udf) {
        this.name = name;
        this.inputType = inputType;
        this.outputType = outputType;
        this.udf = udf;
        this.signature = UDFType.buildSignature(name, inputType);
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
        Function arg = args.getQuick(0);
        int outputColumnType = UDFType.toColumnType(outputType);

        return switch (outputColumnType) {
            case ColumnType.DOUBLE -> new UDFDoubleFunction(arg);
            case ColumnType.LONG -> new UDFLongFunction(arg);
            case ColumnType.INT -> new UDFIntFunction(arg);
            case ColumnType.STRING -> new UDFStrFunction(arg);
            case ColumnType.BOOLEAN -> new UDFBooleanFunction(arg);
            case ColumnType.FLOAT -> new UDFFloatFunction(arg);
            case ColumnType.TIMESTAMP -> new UDFTimestampFunction(arg);
            case ColumnType.DATE -> new UDFDateFunction(arg);
            default -> throw new IllegalArgumentException("Unsupported output type: " + outputType);
        };
    }

    private O safeCompute(I input) {
        try {
            return udf.compute(input);
        } catch (Exception e) {
            throw CairoException.nonCritical()
                    .put("UDF '")
                    .put(name)
                    .put("' threw exception: ")
                    .put(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
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
            case ColumnType.TIMESTAMP -> {
                long v = arg.getTimestamp(rec);
                yield v == Long.MIN_VALUE ? null : new Timestamp(v);
            }
            case ColumnType.DATE -> {
                long v = arg.getDate(rec);
                yield v == Long.MIN_VALUE ? null : new Date(v);
            }
            default -> throw new IllegalArgumentException("Unsupported input type: " + inputType);
        };
    }

    private class UDFDoubleFunction extends DoubleFunction implements UnaryFunction {
        private final Function arg;

        UDFDoubleFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            return result == null ? Double.NaN : ((Number) result).doubleValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFLongFunction extends LongFunction implements UnaryFunction {
        private final Function arg;

        UDFLongFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            return result == null ? Long.MIN_VALUE : ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFIntFunction extends IntFunction implements UnaryFunction {
        private final Function arg;

        UDFIntFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            return result == null ? Integer.MIN_VALUE : ((Number) result).intValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFStrFunction extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        UDFStrFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            if (result == null) {
                return null;
            }
            sinkA.clear();
            sinkA.put(result.toString());
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
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

    private class UDFBooleanFunction extends BooleanFunction implements UnaryFunction {
        private final Function arg;

        UDFBooleanFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            return result != null && (Boolean) result;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFFloatFunction extends FloatFunction implements UnaryFunction {
        private final Function arg;

        UDFFloatFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public float getFloat(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            return result == null ? Float.NaN : ((Number) result).floatValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFTimestampFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;

        UDFTimestampFunction(Function arg) {
            super(ColumnType.TIMESTAMP);
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            if (result == null) {
                return Long.MIN_VALUE;
            }
            if (result instanceof Timestamp) {
                return ((Timestamp) result).getMicros();
            }
            return ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private class UDFDateFunction extends DateFunction implements UnaryFunction {
        private final Function arg;

        UDFDateFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDate(Record rec) {
            I input = extractInput(arg, rec);
            O result = safeCompute(input);
            if (result == null) {
                return Long.MIN_VALUE;
            }
            if (result instanceof Date) {
                return ((Date) result).getMillis();
            }
            return ((Number) result).longValue();
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
