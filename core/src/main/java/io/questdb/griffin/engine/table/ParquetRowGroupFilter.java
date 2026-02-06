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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

/**
 * Utility class for Parquet row group bloom filter pushdown.
 * <p>
 * This class provides static methods to build filter lists and check if
 * row groups can be skipped based on bloom filter conditions.
 */
public final class ParquetRowGroupFilter {
    public static final int FILTER_BUFFER_MAX_PAGES = 16;
    public static final long FILTER_BUFFER_PAGE_SIZE = 4096;
    public static final int LONGS_PER_FILTER = 2;

    /**
     * Check if a row group can be skipped based on min/max statistics and bloom filter conditions.
     *
     * @param rowGroupIndex            the row group index to check
     * @param decoder                  the Parquet partition decoder
     * @param pushdownFilterConditions the filter conditions to apply
     * @param filterList               reusable buffer for filter descriptors: [encoded(col_idx, count), ptr] per filter
     * @param filterValues             reusable memory buffer for filter values
     * @return true if the row group can be safely skipped (all filter values absent from bloom filter
     * or outside min/max statistics), false otherwise
     */
    public static boolean canSkipRowGroup(
            int rowGroupIndex,
            PartitionDecoder decoder,
            ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions,
            DirectLongList filterList,
            MemoryCARWImpl filterValues
    ) {
        if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
            return false;
        }
        filterList.clear();
        filterList.reopen();
        filterValues.clear();
        PartitionDecoder.Metadata metadata = decoder.metadata();

        for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
            final PushdownFilterExtractor.PushdownFilterCondition condition = pushdownFilterConditions.getQuick(i);
            final ObjList<Function> valueFunctions = condition.getValueFunctions();
            final int valueCount = valueFunctions.size();
            if (valueCount == 0) {
                continue;
            }

            int columnIndex = metadata.getColumnIndex(condition.getColumnName());
            if (columnIndex < 0) {
                continue;
            }
            final int columnType = condition.getColumnType();
            final long valuesOffset = filterValues.getAppendOffset();
            boolean supported = true;
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BYTE:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putInt(valueFunctions.getQuick(j).getByte(null));
                    }
                    break;
                case ColumnType.SHORT:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putInt(valueFunctions.getQuick(j).getShort(null));
                    }
                    break;
                case ColumnType.CHAR:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putInt(valueFunctions.getQuick(j).getChar(null));
                    }
                    break;
                case ColumnType.INT:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putInt(valueFunctions.getQuick(j).getInt(null));
                    }
                    break;
                // don't support timestamp because it may be the value is interval.
                case ColumnType.TIMESTAMP:
                    boolean allTimestamp = true;
                    for (int j = 0; j < valueCount; j++) {
                        if (!ColumnType.isTimestamp(valueFunctions.getQuick(j).getType())) {
                            allTimestamp = false;
                            break;
                        }
                    }
                    if (!allTimestamp) {
                        supported = false;
                        break;
                    }
                    // fallthrough
                case ColumnType.LONG:
                case ColumnType.DATE:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putLong(valueFunctions.getQuick(j).getLong(null));
                    }
                    break;
                case ColumnType.FLOAT:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putFloat(valueFunctions.getQuick(j).getFloat(null));
                    }
                    break;
                case ColumnType.DOUBLE:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putDouble(valueFunctions.getQuick(j).getDouble(null));
                    }
                    break;
                case ColumnType.IPv4:
                    for (int j = 0; j < valueCount; j++) {
                        filterValues.putInt(valueFunctions.getQuick(j).getIPv4(null));
                    }
                    break;
                case ColumnType.UUID:
                    for (int j = 0; j < valueCount; j++) {
                        long lo = valueFunctions.getQuick(j).getLong128Lo(null);
                        long hi = valueFunctions.getQuick(j).getLong128Hi(null);
                        if (lo == Numbers.LONG_NULL && hi == Numbers.LONG_NULL) {
                            filterValues.putLong(lo);
                            filterValues.putLong(hi);
                        } else {
                            filterValues.putLong(Long.reverseBytes(hi));
                            filterValues.putLong(Long.reverseBytes(lo));
                        }
                    }
                    break;
                case ColumnType.LONG128:
                    for (int j = 0; j < valueCount; j++) {
                        long lo = valueFunctions.getQuick(j).getLong128Lo(null);
                        long hi = valueFunctions.getQuick(j).getLong128Hi(null);
                        filterValues.putLong(lo);
                        filterValues.putLong(hi);
                    }
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                case ColumnType.VARCHAR:
                    for (int j = 0; j < valueCount; j++) {
                        Utf8Sequence utf8 = valueFunctions.getQuick(j).getVarcharA(null);
                        if (utf8 != null) {
                            int len = utf8.size();
                            filterValues.putInt(len);
                            filterValues.putVarchar(utf8);
                        } else {
                            filterValues.putInt(-1);
                        }
                    }
                    break;
                default:
                    supported = false;
                    break;
            }

            if (!supported) {
                continue;
            }

            final long valuesPtr = filterValues.getAddress() + valuesOffset;
            filterList.add(encodeColumnAndCount(columnIndex, valueCount));
            filterList.add(valuesPtr);
        }
        final int filterCount = (int) (filterList.size() / LONGS_PER_FILTER);
        if (filterCount == 0) {
            return false;
        }

        return decoder.canSkipRowGroup(rowGroupIndex, filterList);
    }

    public static long encodeColumnAndCount(int columnIndex, int count) {
        return (columnIndex & 0xFFFFFFFFL) | ((long) count << 32);
    }
}
