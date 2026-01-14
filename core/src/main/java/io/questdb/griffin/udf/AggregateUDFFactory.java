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
import io.questdb.cairo.CairoException;
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
import io.questdb.griffin.engine.functions.decimal.Decimal64Function;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.ObjList;

import java.math.BigDecimal;
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

    private final Class<I> inputType;
    private final String name;
    private final Class<O> outputType;
    private final String signature;
    private final Supplier<AggregateUDF<I, O>> supplier;

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

        // Handle decimal types (encoded with precision and scale)
        if (ColumnType.isDecimal(outputColumnType)) {
            return new DecimalAggregateFunction(arg, supplier.get(), outputColumnType);
        }

        return switch (ColumnType.tagOf(outputColumnType)) {
            case ColumnType.DOUBLE -> new DoubleAggregateFunction(arg, supplier.get());
            case ColumnType.LONG -> new LongAggregateFunction(arg, supplier.get());
            case ColumnType.INT -> new IntAggregateFunction(arg, supplier.get());
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
            default -> throw new IllegalArgumentException("Unsupported input type: " + inputType);
        };
    }

    private void safeAccumulate(AggregateUDF<I, O> udf, I input) {
        try {
            udf.accumulate(input);
        } catch (Exception e) {
            throw CairoException.nonCritical()
                    .put("UDF '")
                    .put(name)
                    .put("' accumulate() threw exception: ")
                    .put(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
    }

    private O safeResult(AggregateUDF<I, O> udf) {
        try {
            return udf.result();
        } catch (Exception e) {
            throw CairoException.nonCritical()
                    .put("UDF '")
                    .put(name)
                    .put("' result() threw exception: ")
                    .put(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        }
    }

    private class DoubleAggregateFunction extends DoubleFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        // Map to store per-group UDF instances, keyed by group ID
        private final LongObjHashMap<AggregateUDF<I, O>> groupUdfs = new LongObjHashMap<>();
        private int valueIndex;
        private int groupIdIndex;
        private long nextGroupId = 0;

        DoubleAggregateFunction(Function arg, AggregateUDF<I, O> ignoredTemplateUdf) {
            this.arg = arg;
            // Template UDF is not used - we create fresh instances per group
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            // Create a new UDF instance for this group
            long groupId = nextGroupId++;
            mapValue.putLong(groupIdIndex, groupId);

            AggregateUDF<I, O> udf = supplier.get();
            udf.reset();
            groupUdfs.put(groupId, udf);

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putDouble(valueIndex, result == null ? Double.NaN : ((Number) result).doubleValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            // Look up the UDF instance for this group
            long groupId = mapValue.getLong(groupIdIndex);
            AggregateUDF<I, O> udf = groupUdfs.get(groupId);

            if (udf == null) {
                // Should not happen, but handle gracefully
                throw CairoException.nonCritical()
                        .put("UDF '")
                        .put(name)
                        .put("' state not found for group");
            }

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putDouble(valueIndex, result == null ? Double.NaN : ((Number) result).doubleValue());
        }

        @Override
        public Function getArg() {
            return arg;
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
            this.groupIdIndex = valueIndex + 1;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.DOUBLE);
            this.groupIdIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // For group ID
        }

        @Override
        public void setDouble(MapValue mapValue, double value) {
            mapValue.putDouble(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putDouble(valueIndex, Double.NaN);
        }

        @Override
        public boolean supportsParallelism() {
            // Disable parallelism for now - would require more complex state management
            return false;
        }

        @Override
        public void toTop() {
            // Reset state between query executions
            groupUdfs.clear();
            nextGroupId = 0;
        }

        @Override
        public void close() {
            super.close();
            groupUdfs.clear();
        }
    }

    private class IntAggregateFunction extends IntFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final LongObjHashMap<AggregateUDF<I, O>> groupUdfs = new LongObjHashMap<>();
        private int valueIndex;
        private int groupIdIndex;
        private long nextGroupId = 0;

        IntAggregateFunction(Function arg, AggregateUDF<I, O> ignoredTemplateUdf) {
            this.arg = arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            long groupId = nextGroupId++;
            mapValue.putLong(groupIdIndex, groupId);

            AggregateUDF<I, O> udf = supplier.get();
            udf.reset();
            groupUdfs.put(groupId, udf);

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putInt(valueIndex, result == null ? Integer.MIN_VALUE : ((Number) result).intValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            long groupId = mapValue.getLong(groupIdIndex);
            AggregateUDF<I, O> udf = groupUdfs.get(groupId);

            if (udf == null) {
                throw CairoException.nonCritical()
                        .put("UDF '")
                        .put(name)
                        .put("' state not found for group");
            }

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putInt(valueIndex, result == null ? Integer.MIN_VALUE : ((Number) result).intValue());
        }

        @Override
        public Function getArg() {
            return arg;
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
            this.groupIdIndex = valueIndex + 1;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.INT);
            this.groupIdIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG);
        }

        @Override
        public void setInt(MapValue mapValue, int value) {
            mapValue.putInt(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putInt(valueIndex, Integer.MIN_VALUE);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }

        @Override
        public void toTop() {
            groupUdfs.clear();
            nextGroupId = 0;
        }

        @Override
        public void close() {
            super.close();
            groupUdfs.clear();
        }
    }

    private class LongAggregateFunction extends LongFunction implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final LongObjHashMap<AggregateUDF<I, O>> groupUdfs = new LongObjHashMap<>();
        private int valueIndex;
        private int groupIdIndex;
        private long nextGroupId = 0;

        LongAggregateFunction(Function arg, AggregateUDF<I, O> ignoredTemplateUdf) {
            this.arg = arg;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            long groupId = nextGroupId++;
            mapValue.putLong(groupIdIndex, groupId);

            AggregateUDF<I, O> udf = supplier.get();
            udf.reset();
            groupUdfs.put(groupId, udf);

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putLong(valueIndex, result == null ? Long.MIN_VALUE : ((Number) result).longValue());
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            long groupId = mapValue.getLong(groupIdIndex);
            AggregateUDF<I, O> udf = groupUdfs.get(groupId);

            if (udf == null) {
                throw CairoException.nonCritical()
                        .put("UDF '")
                        .put(name)
                        .put("' state not found for group");
            }

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putLong(valueIndex, result == null ? Long.MIN_VALUE : ((Number) result).longValue());
        }

        @Override
        public Function getArg() {
            return arg;
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
            this.groupIdIndex = valueIndex + 1;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG);
            this.groupIdIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG);
        }

        @Override
        public void setLong(MapValue mapValue, long value) {
            mapValue.putLong(valueIndex, value);
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Long.MIN_VALUE);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }

        @Override
        public void toTop() {
            groupUdfs.clear();
            nextGroupId = 0;
        }

        @Override
        public void close() {
            super.close();
            groupUdfs.clear();
        }
    }

    private class DecimalAggregateFunction extends Decimal64Function implements GroupByFunction, UnaryFunction {
        private final Function arg;
        private final LongObjHashMap<AggregateUDF<I, O>> groupUdfs = new LongObjHashMap<>();
        private final int scale;
        private int valueIndex;
        private int groupIdIndex;
        private long nextGroupId = 0;

        DecimalAggregateFunction(Function arg, AggregateUDF<I, O> ignoredTemplateUdf, int decimalType) {
            super(decimalType);
            this.arg = arg;
            this.scale = ColumnType.getDecimalScale(decimalType);
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            long groupId = nextGroupId++;
            mapValue.putLong(groupIdIndex, groupId);

            AggregateUDF<I, O> udf = supplier.get();
            udf.reset();
            groupUdfs.put(groupId, udf);

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putLong(valueIndex, convertToDecimal64(result));
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            long groupId = mapValue.getLong(groupIdIndex);
            AggregateUDF<I, O> udf = groupUdfs.get(groupId);

            if (udf == null) {
                throw CairoException.nonCritical()
                        .put("UDF '")
                        .put(name)
                        .put("' state not found for group");
            }

            I input = extractInput(arg, record);
            safeAccumulate(udf, input);
            O result = safeResult(udf);
            mapValue.putLong(valueIndex, convertToDecimal64(result));
        }

        private long convertToDecimal64(O result) {
            if (result == null) {
                return Decimals.DECIMAL64_NULL;
            }
            if (result instanceof BigDecimal bd) {
                return bd.setScale(scale, java.math.RoundingMode.HALF_UP)
                        .movePointRight(scale)
                        .longValue();
            }
            BigDecimal bd = BigDecimal.valueOf(((Number) result).doubleValue());
            return bd.setScale(scale, java.math.RoundingMode.HALF_UP)
                    .movePointRight(scale)
                    .longValue();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDecimal64(Record rec) {
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
            this.groupIdIndex = valueIndex + 1;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
            this.valueIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // DECIMAL64 is stored as LONG
            this.groupIdIndex = columnTypes.getColumnCount();
            columnTypes.add(ColumnType.LONG); // For group ID
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(valueIndex, Decimals.DECIMAL64_NULL);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }

        @Override
        public void toTop() {
            groupUdfs.clear();
            nextGroupId = 0;
        }

        @Override
        public void close() {
            super.close();
            groupUdfs.clear();
        }
    }
}
