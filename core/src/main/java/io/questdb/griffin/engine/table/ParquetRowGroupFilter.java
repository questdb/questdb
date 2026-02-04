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
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

/**
 * Utility class for Parquet row group bloom filter pushdown.
 * <p>
 * This class provides static methods to build filter lists and check if
 * row groups can be skipped based on bloom filter conditions.
 */
public final class ParquetRowGroupFilter {
    public static final int LONGS_PER_FILTER = 2;

    /**
     * Check if a row group can be skipped based on bloom filter conditions.
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
            MemoryCARW filterValues
    ) {
        if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
            return false;
        }
        filterList.clear();
        filterValues.truncate();
        final PartitionDecoder.Metadata parquetMeta = decoder.metadata();
        final int parquetColumnCount = parquetMeta.getColumnCount();

        for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
            final PushdownFilterExtractor.PushdownFilterCondition condition = pushdownFilterConditions.getQuick(i);
            final int questdbColumnIndex = condition.getColumnIndex();
            int parquetColumnIndex = questdbColumnIndex;
/*            for (int j = 0; j < parquetColumnCount; j++) {
                if (parquetMeta.getColumnId(j) == questdbColumnIndex) {
                    parquetColumnIndex = j;
                    break;
                }
            }*/
            if (parquetColumnIndex < 0) {
                continue;
            }

            final ObjList<Function> valueFunctions = condition.getValueFunctions();
            final int valueCount = valueFunctions.size();
            if (valueCount == 0) {
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
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
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
                case ColumnType.STRING:
                    for (int j = 0; j < valueCount; j++) {
                        CharSequence cs = valueFunctions.getQuick(j).getStrA(null);
                        if (cs != null) {
                            writeUtf16AsUtf8(filterValues, cs);
                        } else {
                            filterValues.putInt(0);
                        }
                    }
                    break;
                case ColumnType.SYMBOL:
                    for (int j = 0; j < valueCount; j++) {
                        CharSequence cs = valueFunctions.getQuick(j).getSymbol(null);
                        if (cs != null) {
                            writeUtf16AsUtf8(filterValues, cs);
                        } else {
                            filterValues.putInt(0);
                        }
                    }
                    break;
                case ColumnType.VARCHAR:
                    for (int j = 0; j < valueCount; j++) {
                        Utf8Sequence utf8 = valueFunctions.getQuick(j).getVarcharA(null);
                        if (utf8 != null) {
                            int len = utf8.size();
                            filterValues.putInt(len);
                            for (int k = 0; k < len; k++) {
                                filterValues.putByte(utf8.byteAt(k));
                            }
                        } else {
                            filterValues.putInt(0);
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
            filterList.add(encodeColumnAndCount(parquetColumnIndex, valueCount));
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

    private static void writeUtf16AsUtf8(MemoryCARW mem, CharSequence cs) {
        int utf8Len = 0;
        for (int i = 0, n = cs.length(); i < n; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                utf8Len++;
            } else if (c < 0x800) {
                utf8Len += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(cs.charAt(i + 1))) {
                utf8Len += 4;
                i++;
            } else {
                utf8Len += 3;
            }
        }
        mem.putInt(utf8Len);

        for (int i = 0, n = cs.length(); i < n; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                mem.putByte((byte) c);
            } else if (c < 0x800) {
                mem.putByte((byte) (0xC0 | (c >> 6)));
                mem.putByte((byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(cs.charAt(i + 1))) {
                int codePoint = Character.toCodePoint(c, cs.charAt(++i));
                mem.putByte((byte) (0xF0 | (codePoint >> 18)));
                mem.putByte((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                mem.putByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                mem.putByte((byte) (0x80 | (codePoint & 0x3F)));
            } else {
                mem.putByte((byte) (0xE0 | (c >> 12)));
                mem.putByte((byte) (0x80 | ((c >> 6) & 0x3F)));
                mem.putByte((byte) (0x80 | (c & 0x3F)));
            }
        }
    }
}
