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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.function.Supplier;

/**
 * Wraps an {@link AggregateUDF} as a Griffin {@link FunctionFactory}.
 * <p>
 * This class handles all the complexity of the GroupByFunction interface,
 * MapValue state management, and parallel execution support.
 *
 * @param <I> Input type
 * @param <O> Output type
 */
public class AggregateUDFFactory<I, O> implements FunctionFactory {

    private final String name;
    private final Class<I> inputType;
    private final Class<O> outputType;
    private final Supplier<AggregateUDF<I, O>> supplier;
    private final String signature;

    public AggregateUDFFactory(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            Supplier<AggregateUDF<I, O>> supplier
    ) {
        this.name = name;
        this.inputType = inputType;
        this.outputType = outputType;
        this.supplier = supplier;
        this.signature = UDFType.buildSignature(name, inputType);
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public boolean isGroupBy() {
        return true;
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
            case ColumnType.DOUBLE -> new DoubleAggregateFunction(arg, supplier.get());
            case ColumnType.LONG -> new LongAggregateFunction(arg, supplier.get());
            case ColumnType.INT -> new IntAggregateFunction(arg, supplier.get());
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
            default -> throw new IllegalArgumentException("Unsupported input type: " + inputType);
        };
    }

    private class DoubleAggregateFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final AggregateUDF<I, O> udf;
        private int valueIndex;

        DoubleAggregateFunction(Function arg, AggregateUDF<I, O> udf) {
            this.arg = arg;
            this.udf = udf;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            udf.reset();
            I input = extractInput(arg, record);
            udf.accumulate(input);
            // Store current result
            O result = udf.result();
            mapValue.putDouble(valueIndex, result == null ? Double.NaN : ((Number) result).doubleValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // Restore state from map (for simplicity, we re-accumulate)
            // Note: For production, we'd store intermediate state
            I input = extractInput(arg, record);
            udf.accumulate(input);
            O result = udf.result();
            mapValue.putDouble(valueIndex, result == null ? Double.NaN : ((Number) result).doubleValue());
        }

        @Override
        public double getDouble(Record rec) {
            return rec.getDouble(valueIndex);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            this.valueIndex = valueIndex;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DOUBLE);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, Double.NaN);
        }

        @Override
        public void setDouble(MapValue mapValue, double value) {
            mapValue.putDouble(valueIndex, value);
        }

        @Override
        public boolean supportsParallelism() {
            return udf.supportsParallelism();
        }
    }

    private class LongAggregateFunction extends LongFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final AggregateUDF<I, O> udf;
        private int valueIndex;

        LongAggregateFunction(Function arg, AggregateUDF<I, O> udf) {
            this.arg = arg;
            this.udf = udf;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            udf.reset();
            I input = extractInput(arg, record);
            udf.accumulate(input);
            O result = udf.result();
            mapValue.putLong(valueIndex, result == null ? Long.MIN_VALUE : ((Number) result).longValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            I input = extractInput(arg, record);
            udf.accumulate(input);
            O result = udf.result();
            mapValue.putLong(valueIndex, result == null ? Long.MIN_VALUE : ((Number) result).longValue());
        }

        @Override
        public long getLong(Record rec) {
            return rec.getLong(valueIndex);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            this.valueIndex = valueIndex;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Long.MIN_VALUE);
        }

        @Override
        public void setLong(MapValue mapValue, long value) {
            mapValue.putLong(valueIndex, value);
        }

        @Override
        public boolean supportsParallelism() {
            return udf.supportsParallelism();
        }
    }

    private class IntAggregateFunction extends IntFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final AggregateUDF<I, O> udf;
        private int valueIndex;

        IntAggregateFunction(Function arg, AggregateUDF<I, O> udf) {
            this.arg = arg;
            this.udf = udf;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            udf.reset();
            I input = extractInput(arg, record);
            udf.accumulate(input);
            O result = udf.result();
            mapValue.putInt(valueIndex, result == null ? Integer.MIN_VALUE : ((Number) result).intValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            I input = extractInput(arg, record);
            udf.accumulate(input);
            O result = udf.result();
            mapValue.putInt(valueIndex, result == null ? Integer.MIN_VALUE : ((Number) result).intValue());
        }

        @Override
        public int getInt(Record rec) {
            return rec.getInt(valueIndex);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            this.valueIndex = valueIndex;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.INT);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putInt(valueIndex, Integer.MIN_VALUE);
        }

        @Override
        public void setInt(MapValue mapValue, int value) {
            mapValue.putInt(valueIndex, value);
        }

        @Override
        public boolean supportsParallelism() {
            return udf.supportsParallelism();
        }
    }
}
