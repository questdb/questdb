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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class CountConstWindowFunctionFactory extends AbstractWindowFunctionFactory {
    public static final CountFunctionFactoryHelper.IsRecordNotNull isRecordNotNull = ((arg, record) -> true);

    @Override
    public String getSignature() {
        return "count()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        WindowContext windowContext = sqlExecutionContext.getWindowContext();
        windowContext.validate(position, supportNullsDesc());
        int framingMode = windowContext.getFramingMode();
        RecordSink partitionBySink = windowContext.getPartitionBySink();
        ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
        VirtualRecord partitionByRecord = windowContext.getPartitionByRecord();
        long rowsLo = windowContext.getRowsLo();
        long rowsHi = windowContext.getRowsHi();
        if (rowsHi < rowsLo) {
            return new LongNullFunction(null,
                    CountFunctionFactoryHelper.COUNT_NAME,
                    rowsLo,
                    rowsHi,
                    framingMode == WindowExpression.FRAMING_RANGE,
                    partitionByRecord,
                    0);
        }

        if (partitionByRecord != null) {
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                if (windowContext.isDefaultFrame() && (!windowContext.isOrdered() || windowContext.getRowsHi() == Long.MAX_VALUE)) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountFunctionFactoryHelper.CountOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            null,
                            isRecordNotNull
                    );
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new CountFunctionFactoryHelper.CountOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            null,
                            isRecordNotNull
                    );
                } // range between [unbounded | x] preceding and [x preceding | current row], except unbounded preceding to current row
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    Map map = null;
                    MemoryARW mem = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                CountFunctionFactoryHelper.COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES
                        );
                        mem = Vm.getCARWInstance(
                                configuration.getSqlWindowStorePageSize(),
                                configuration.getSqlWindowStoreMaxPages(),
                                MemoryTag.NATIVE_CIRCULAR_BUFFER
                        );

                        // moving count over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                        return new CountFunctionFactoryHelper.CountOverPartitionRangeFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi,
                                mem,
                                configuration.getSqlWindowInitialRangeBufferSize(),
                                timestampIndex,
                                null,
                                isRecordNotNull
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        Misc.free(mem);
                        throw th;
                    }
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountFunctionFactoryHelper.CountOverUnboundedPartitionRowsFrameFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            null,
                            isRecordNotNull
                    );
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new CountFunctionFactoryHelper.CountOverCurrentRowFunction(null, isRecordNotNull);
                } // whole partition
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    Map map = MapFactory.createUnorderedMap(
                            configuration,
                            partitionByKeyTypes,
                            CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                    );

                    return new CountFunctionFactoryHelper.CountOverPartitionFunction(
                            map,
                            partitionByRecord,
                            partitionBySink,
                            null,
                            isRecordNotNull
                    );
                }
                //between [unbounded | x] preceding and [x preceding | current row]
                else {
                    Map map = null;
                    try {
                        map = MapFactory.createUnorderedMap(
                                configuration,
                                partitionByKeyTypes,
                                CountFunctionFactoryHelper.COUNT_COLUMN_TYPES
                        );

                        return new CountOverPartitionRowsFrameFunction(
                                map,
                                partitionByRecord,
                                partitionBySink,
                                rowsLo,
                                rowsHi
                        );
                    } catch (Throwable th) {
                        Misc.free(map);
                        throw th;
                    }
                }
            }
        } else { // no partition key
            if (framingMode == WindowExpression.FRAMING_RANGE) {
                // if there's no order by then all elements are equal in range mode, thus calculation is done on whole result set
                if (!windowContext.isOrdered() && windowContext.isDefaultFrame()) {
                    return new CountFunctionFactoryHelper.CountOverWholeResultSetFunction(null, isRecordNotNull);
                } // between unbounded preceding and current row
                else if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    // same as for rows because calculation stops at current rows even if there are 'equal' following rows
                    return new CountFunctionFactoryHelper.CountOverUnboundedRowsFrameFunction(null, isRecordNotNull);
                } // range between [unbounded | x] preceding and [x preceding | current row]
                else {
                    if (windowContext.isOrdered() && !windowContext.isOrderedByDesignatedTimestamp()) {
                        throw SqlException.$(windowContext.getOrderByPos(), "RANGE is supported only for queries ordered by designated timestamp");
                    }

                    int timestampIndex = windowContext.getTimestampIndex();

                    // moving count over range between timestamp - rowsLo and timestamp + rowsHi (inclusive)
                    return new CountFunctionFactoryHelper.CountOverRangeFrameFunction(
                            rowsLo,
                            rowsHi,
                            configuration,
                            timestampIndex,
                            null,
                            isRecordNotNull
                    );
                }
            } else if (framingMode == WindowExpression.FRAMING_ROWS) {
                // between unbounded preceding and current row
                if (rowsLo == Long.MIN_VALUE && rowsHi == 0) {
                    return new CountFunctionFactoryHelper.CountOverUnboundedRowsFrameFunction(null, isRecordNotNull);
                } // between current row and current row
                else if (rowsLo == 0 && rowsHi == 0) {
                    return new CountFunctionFactoryHelper.CountOverCurrentRowFunction(null, isRecordNotNull);
                } // whole result set
                else if (rowsLo == Long.MIN_VALUE && rowsHi == Long.MAX_VALUE) {
                    return new CountFunctionFactoryHelper.CountOverWholeResultSetFunction(null, isRecordNotNull);
                } // between [unbounded | x] preceding and [x preceding | current row]
                else {
                    return new CountOverRowsFrameFunction(
                            rowsLo,
                            rowsHi
                    );
                }
            }
        }

        throw SqlException.$(position, "function not implemented for given window parameters");
    }

    // fast path of handles count() over (partition by x [order by o] rows between y and z)
    public static class CountOverPartitionRowsFrameFunction extends BasePartitionedWindowFunction implements WindowLongFunction {
        private final long frameSize;
        private final long rowsHi;
        private final long rowsLo;
        private long count;

        public CountOverPartitionRowsFrameFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi
        ) {
            super(map, partitionByRecord, partitionBySink, null);
            if (rowsLo > Long.MIN_VALUE) {
                this.frameSize = rowsHi - rowsLo + 1;
            } else {
                this.frameSize = Long.MAX_VALUE;
            }
            this.rowsHi = rowsHi;
            this.rowsLo = rowsLo;
        }


        @Override
        public void close() {
            super.close();
        }

        @Override
        public void computeNext(Record record) {
            // map stores:
            // 0 - count
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();

            long currentSize = 0;
            if (!value.isNew()) {
                currentSize = value.getLong(0);
            }
            currentSize++;
            this.count = currentSize + rowsHi;
            if (this.count < 0) {
                this.count = 0;
            } else if (this.count > frameSize) {
                this.count = frameSize;
            }

            value.putLong(0, currentSize);
        }

        @Override
        public long getLong(Record rec) {
            return this.count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), count);
        }

        @Override
        public void reopen() {
            super.reopen();
        }

        @Override
        public void reset() {
            super.reset();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val("(*)");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());

            sink.val(" rows between ");
            if (rowsLo > Long.MIN_VALUE) {
                sink.val(Math.abs(this.rowsLo));
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (this.rowsHi == 0) {
                sink.val("current row");
            } else {
                sink.val(Math.abs(this.rowsHi)).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
        }
    }

    // fast path of handles count() over ([order by o] rows between y and z), there's no partition by.
    public static class CountOverRowsFrameFunction extends BaseWindowFunction implements WindowLongFunction {
        private final long frameSize;
        private final long rowsHi;
        private final long rowsLo;
        private long count;

        public CountOverRowsFrameFunction(long rowsLo, long rowsHi) {
            super(null);
            if (rowsLo > Long.MIN_VALUE) {
                this.frameSize = rowsHi - rowsLo + 1;
            } else {
                this.frameSize = Long.MAX_VALUE;
            }
            this.rowsHi = rowsHi;
            this.rowsLo = rowsLo;
            this.count = rowsHi;
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public void computeNext(Record record) {
            this.count++;
        }

        @Override
        public long getLong(Record rec) {
            if (count < 0) {
                return 0;
            } else if (count > frameSize) {
                return frameSize;
            }
            return count;
        }

        @Override
        public String getName() {
            return CountFunctionFactoryHelper.COUNT_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), getLong(null));
        }

        @Override
        public void reset() {
            super.reset();
            count = rowsHi;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val("(*)");
            sink.val(" over (");
            sink.val(" rows between ");
            if (rowsLo != Long.MIN_VALUE) {
                sink.val(Math.abs(rowsLo));
            } else {
                sink.val("unbounded");
            }
            sink.val(" preceding and ");
            if (this.rowsHi == 0) {
                sink.val("current row");
            } else {
                sink.val(Math.abs(this.rowsHi)).val(" preceding");
            }
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            count = rowsHi;
        }
    }
}
