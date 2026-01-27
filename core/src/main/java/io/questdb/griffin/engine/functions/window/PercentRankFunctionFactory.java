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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * percent_rank() window function.
 * Returns the relative rank of the current row: (rank - 1) / (total_rows - 1).
 * Returns 0 if there is only one row in the partition.
 */
public class PercentRankFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "percent_rank";
    private static final String SIGNATURE = NAME + "()";

    // Column types for partition-based functions: offset, rank, count
    private static final ArrayColumnTypes PERCENT_RANK_COLUMN_TYPES;

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getNullsDescPos() > 0) {
            throw SqlException.$(windowContext.getNullsDescPos(), "RESPECT/IGNORE NULLS is not supported for current window function");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "percent_rank() does not support framing; remove ROWS/RANGE clause");
        }

        if (windowContext.isOrdered()) {
            // percent_rank() over (partition by xxx order by xxx)
            if (windowContext.getPartitionByRecord() != null) {
                return new PercentRankOverPartitionFunction(
                        windowContext.getPartitionByKeyTypes(),
                        windowContext.getPartitionByRecord(),
                        windowContext.getPartitionBySink(),
                        configuration
                );
            } else {
                // percent_rank() over (order by xxx)
                return new PercentRankFunction();
            }
        } else {
            // percent_rank() over ([partition by xxx | ]), without ORDER BY, all rows are peers.
            // All rows have rank 1, so percent_rank = (1-1)/(n-1) = 0
            return new PercentRankNoOrderFunction(windowContext.getPartitionByRecord());
        }
    }

    // percent_rank() without ORDER BY - all rows are peers with rank 1, so percent_rank = 0
    static class PercentRankNoOrderFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private static final double PERCENT_RANK_CONST = 0.0;
        private final VirtualRecord partitionByRecord;
        private int columnIndex;

        public PercentRankNoOrderFunction(VirtualRecord partitionByRecord) {
            this.partitionByRecord = partitionByRecord;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public double getDouble(Record rec) {
            return PERCENT_RANK_CONST;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (partitionByRecord != null) {
                Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), PERCENT_RANK_CONST);
        }

        @Override
        public void reopen() {
        }

        @Override
        public void reset() {
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val("()");
            if (partitionByRecord != null) {
                sink.val(" over (");
                sink.val("partition by ");
                sink.val(partitionByRecord.getFunctions());
                sink.val(')');
            } else {
                sink.val(" over ()");
            }
        }
    }

    // percent_rank() over (order by xxx) - no partition by
    static class PercentRankFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private int columnIndex;
        private long count = 1;
        private long lastRecordOffset;
        private ObjList<ExpressionNode> orderBy;
        private long rank;
        private RecordComparator recordComparator;
        private long totalRows;

        public PercentRankFunction() {
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void initRecordComparator(SqlCodeGenerator sqlGenerator,
                                         RecordMetadata metadata,
                                         ArrayColumnTypes chainTypes,
                                         IntList orderIndices,
                                         ObjList<ExpressionNode> orderBy,
                                         IntList orderByDirection) throws SqlException {
            IntList indices = orderIndices != null ? orderIndices : sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(chainTypes, indices);
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (count == 1) {
                rank = 1;
            } else {
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastRecordOffset)) != 0) {
                    rank = count;
                }
            }
            lastRecordOffset = recordOffset;
            // Store rank temporarily in the output column (as long)
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            // Read rank stored in pass1
            long storedRank = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));
            // Calculate percent_rank = (rank - 1) / (total_rows - 1)
            double percentRank;
            if (totalRows <= 1) {
                percentRank = 0.0;
            } else {
                percentRank = (double) (storedRank - 1) / (double) (totalRows - 1);
            }
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), percentRank);
        }

        @Override
        public void preparePass2() {
            totalRows = count - 1; // count was incremented after each row
        }

        @Override
        public void reopen() {
            count = 1;
        }

        @Override
        public void reset() {
            count = 1;
            totalRows = 0;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val("()");
            sink.val(" over (");
            sink.val("order by ");
            sink.val(orderBy);
            sink.val(')');
        }

        @Override
        public void toTop() {
            count = 1;
            totalRows = 0;
            super.toTop();
        }
    }

    // percent_rank() over (partition by xxx order by xxx)
    static class PercentRankOverPartitionFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private final CairoConfiguration configuration;
        private final ColumnTypes keyColumnTypes;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private int columnIndex;
        private Map map;
        private ObjList<ExpressionNode> orderBy;
        private RecordComparator recordComparator;

        public PercentRankOverPartitionFunction(
                ColumnTypes keyColumnTypes,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                CairoConfiguration configuration
        ) {
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.keyColumnTypes = keyColumnTypes;
            this.configuration = configuration;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void initRecordComparator(SqlCodeGenerator sqlGenerator,
                                         RecordMetadata metadata,
                                         ArrayColumnTypes chainTypes,
                                         IntList orderIndices,
                                         ObjList<ExpressionNode> orderBy,
                                         IntList orderByDirection) throws SqlException {
            IntList indices = orderIndices != null ? orderIndices : sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            map = MapFactory.createUnorderedMap(
                    configuration,
                    keyColumnTypes,
                    PERCENT_RANK_COLUMN_TYPES
            );
            this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(chainTypes, indices);
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long rank;
            long count;
            if (mapValue.isNew()) {
                rank = 1;
                count = 1;
            } else {
                long lastOffset = mapValue.getLong(0);
                rank = mapValue.getLong(1);
                count = mapValue.getLong(2);
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastOffset)) != 0) {
                    rank = count;
                }
            }

            mapValue.putLong(0, recordOffset);
            mapValue.putLong(1, rank);
            mapValue.putLong(2, count + 1);
            // Store rank temporarily in the output column (as long)
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();

            // Read rank stored in pass1
            long storedRank = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));

            // Get total rows for this partition (count was incremented after each row, so it's total + 1)
            long totalRows = mapValue.getLong(2) - 1;

            // Calculate percent_rank = (rank - 1) / (total_rows - 1)
            double percentRank;
            if (totalRows <= 1) {
                percentRank = 0.0;
            } else {
                percentRank = (double) (storedRank - 1) / (double) (totalRows - 1);
            }
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), percentRank);
        }

        @Override
        public void preparePass2() {
            // Nothing to prepare - each partition's total is in the map
        }

        @Override
        public void reopen() {
            if (map != null) {
                map.reopen();
            }
        }

        @Override
        public void reset() {
            Misc.free(map);
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val("()");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" order by ");
            sink.val(orderBy);
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            Misc.clear(map);
        }
    }

    static {
        PERCENT_RANK_COLUMN_TYPES = new ArrayColumnTypes();
        PERCENT_RANK_COLUMN_TYPES.add(ColumnType.LONG); // offset
        PERCENT_RANK_COLUMN_TYPES.add(ColumnType.LONG); // rank
        PERCENT_RANK_COLUMN_TYPES.add(ColumnType.LONG); // count
    }
}
