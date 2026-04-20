/*+*****************************************************************************
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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * ntile(n) window function.
 * Distributes the rows in an ordered partition into the specified number of buckets (1..n).
 * Returns the bucket number (LONG) for each row.
 */
public class NtileFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "ntile";
    // Column types for partition-based function: row number within partition
    private static final ArrayColumnTypes NTILE_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "(I)";

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
            throw SqlException.$(position, "ntile() does not support framing; remove the frame clause");
        }

        Function bucketCountFunc = args.get(0);
        if (!bucketCountFunc.isConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "bucket count must be a constant");
        }
        int bucketCount = bucketCountFunc.getInt(null);
        if (bucketCount <= 0) {
            throw SqlException.$(argPositions.getQuick(0), "bucket count must be a positive integer");
        }

        if (windowContext.getPartitionByRecord() != null) {
            return new NtileOverPartitionFunction(
                    bucketCount,
                    windowContext.getPartitionByKeyTypes(),
                    windowContext.getPartitionByRecord(),
                    windowContext.getPartitionBySink(),
                    configuration
            );
        } else {
            return new NtileFunction(bucketCount);
        }
    }

    /**
     * Computes the 1-based bucket number for a given row using standard SQL ntile distribution.
     * The first {@code totalRows % bucketCount} buckets each contain {@code ceil(totalRows / bucketCount)} rows,
     * and the remaining buckets each contain {@code floor(totalRows / bucketCount)} rows.
     *
     * @param rowNumber   1-based row number within the partition
     * @param totalRows   total number of rows in the partition
     * @param bucketCount number of buckets
     * @return 1-based bucket number
     */
    private static long computeNtile(long rowNumber, long totalRows, int bucketCount) {
        long bucketSize = totalRows / bucketCount;
        long remainder = totalRows % bucketCount;
        long threshold = remainder * (bucketSize + 1);
        if (rowNumber <= threshold) {
            return (rowNumber - 1) / (bucketSize + 1) + 1;
        } else {
            return (rowNumber - threshold - 1) / bucketSize + 1 + remainder;
        }
    }

    // ntile(n) over ([order by xxx]) - no partition by
    static class NtileFunction extends LongFunction implements Function, WindowFunction, Reopenable {

        private final int bucketCount;
        private int columnIndex;
        private long count = 1;
        private long totalRows;

        public NtileFunction(int bucketCount) {
            this.bucketCount = bucketCount;
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            // Store the 1-based row number
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long rowNumber = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));
            long bucket = computeNtile(rowNumber, totalRows, bucketCount);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), bucket);
        }

        @Override
        public void preparePass2() {
            totalRows = count - 1;
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
            sink.val('(').val(bucketCount).val(')');
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            count = 1;
            totalRows = 0;
        }
    }

    // ntile(n) over (partition by xxx [order by xxx])
    static class NtileOverPartitionFunction extends LongFunction implements Function, WindowFunction, Reopenable {

        private final int bucketCount;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private int columnIndex;
        private Map map;

        public NtileOverPartitionFunction(
                int bucketCount,
                ColumnTypes keyColumnTypes,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                CairoConfiguration configuration
        ) {
            this.bucketCount = bucketCount;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.map = MapFactory.createUnorderedMap(
                    configuration,
                    keyColumnTypes,
                    NTILE_COLUMN_TYPES
            );
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long rowNumber;
            if (mapValue.isNew()) {
                rowNumber = 1;
            } else {
                rowNumber = mapValue.getLong(0) + 1;
            }
            mapValue.putLong(0, rowNumber);
            // Store 1-based row number in the output column
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rowNumber);
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();

            long rowNumber = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));
            // The map stores the final row number (i.e. total rows in this partition)
            long totalRows = mapValue.getLong(0);
            long bucket = computeNtile(rowNumber, totalRows, bucketCount);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), bucket);
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
            sink.val('(').val(bucketCount).val(')');
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            Misc.clear(map);
        }
    }

    static {
        NTILE_COLUMN_TYPES = new ArrayColumnTypes();
        NTILE_COLUMN_TYPES.add(ColumnType.LONG); // row number within partition
    }
}
