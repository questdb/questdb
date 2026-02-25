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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.TestOnly;

/**
 * Utility class for Parquet row group bloom filter pushdown.
 * <p>
 * This class provides static methods to build filter lists and check if
 * row groups can be skipped based on bloom filter conditions.
 */
public final class ParquetRowGroupFilter {
    public static final int FILTER_BUFFER_MAX_PAGES = Integer.MAX_VALUE;
    public static final long FILTER_BUFFER_PAGE_SIZE = 128;
    public static final int LONGS_PER_FILTER = 2;
    private static final Log LOG = LogFactory.getLog(ParquetRowGroupFilter.class);
    private static int rowGroupsSkipped;

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
        try {
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
                            Function f = valueFunctions.getQuick(j);
                            switch (f.getType()) {
                                case ColumnType.SHORT:
                                    filterValues.putInt(f.getShort(null));
                                    break;
                                case ColumnType.CHAR:
                                    filterValues.putInt(f.getChar(null));
                                    break;
                                case ColumnType.INT:
                                    filterValues.putInt(f.getInt(null));
                                    break;
                                case ColumnType.LONG:
                                    // Safe truncation: if long exceeds int range, the truncated
                                    // value won't match any byte bloom filter entry.
                                    filterValues.putInt((int) f.getLong(null));
                                    break;
                                case ColumnType.FLOAT:
                                case ColumnType.DOUBLE:
                                    filterValues.putInt((int) f.getDouble(null));
                                    break;
                                default:
                                    filterValues.putInt(f.getByte(null));
                            }
                        }
                        break;
                    case ColumnType.SHORT:
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            switch (f.getType()) {
                                case ColumnType.INT:
                                    filterValues.putInt(f.getInt(null));
                                    break;
                                case ColumnType.LONG:
                                    filterValues.putInt((int) f.getLong(null));
                                    break;
                                case ColumnType.FLOAT:
                                case ColumnType.DOUBLE:
                                    filterValues.putInt((int) f.getDouble(null));
                                    break;
                                default:
                                    filterValues.putInt(f.getShort(null));
                            }
                        }
                        break;
                    case ColumnType.CHAR:
                        for (int j = 0; j < valueCount; j++) {
                            filterValues.putInt(valueFunctions.getQuick(j).getChar(null));
                        }
                        break;
                    case ColumnType.INT:
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            if (f.getType() == ColumnType.LONG) {
                                filterValues.putInt((int) f.getLong(null));
                            } else if (f.getType() == ColumnType.FLOAT || f.getType() == ColumnType.DOUBLE) {
                                filterValues.putInt((int) f.getDouble(null));
                            } else {
                                filterValues.putInt(f.getInt(null));
                            }
                        }
                        break;
                    case ColumnType.TIMESTAMP: {
                        boolean allCompatible = true;
                        for (int j = 0; j < valueCount; j++) {
                            int vType = valueFunctions.getQuick(j).getType();
                            if (!ColumnType.isTimestamp(vType) && vType != ColumnType.DATE) {
                                allCompatible = false;
                                break;
                            }
                        }
                        if (!allCompatible) {
                            supported = false;
                            break;
                        }

                        TimestampDriver driver = ColumnType.getTimestampDriver(columnType);
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            int vType = f.getType();
                            if (columnType == vType) {
                                filterValues.putLong(f.getTimestamp(null));
                            } else {
                                filterValues.putLong(driver.from(f.getTimestamp(null), ColumnType.getTimestampType(vType)));
                            }
                        }
                        break;
                    }
                    case ColumnType.LONG:
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            if (f.getType() == ColumnType.FLOAT || f.getType() == ColumnType.DOUBLE) {
                                filterValues.putLong((long) f.getDouble(null));
                            } else {
                                filterValues.putLong(f.getLong(null));
                            }
                        }
                        break;
                    case ColumnType.DATE:
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            int vType = f.getType();
                            if (ColumnType.isTimestamp(vType)) {
                                filterValues.putLong(f.getDate(null));
                            } else {
                                filterValues.putLong(f.getLong(null));
                            }
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
                    case ColumnType.DECIMAL8:
                        for (int j = 0; j < valueCount; j++) {
                            filterValues.putByte(valueFunctions.getQuick(j).getDecimal8(null));
                        }
                        break;
                    case ColumnType.DECIMAL16:
                        for (int j = 0; j < valueCount; j++) {
                            filterValues.putShort(Short.reverseBytes(valueFunctions.getQuick(j).getDecimal16(null)));
                        }
                        break;
                    case ColumnType.DECIMAL32:
                        for (int j = 0; j < valueCount; j++) {
                            filterValues.putInt(Integer.reverseBytes(valueFunctions.getQuick(j).getDecimal32(null)));
                        }
                        break;
                    case ColumnType.DECIMAL64:
                        for (int j = 0; j < valueCount; j++) {
                            filterValues.putLong(Long.reverseBytes(valueFunctions.getQuick(j).getDecimal64(null)));
                        }
                        break;
                    case ColumnType.DECIMAL128: {
                        Decimal128 d = Misc.getThreadLocalDecimal128();
                        for (int j = 0; j < valueCount; j++) {
                            valueFunctions.getQuick(j).getDecimal128(null, d);
                            filterValues.putLong(Long.reverseBytes(d.getHigh()));
                            filterValues.putLong(Long.reverseBytes(d.getLow()));
                        }
                        break;
                    }
                    case ColumnType.DECIMAL256: {
                        Decimal256 d = Misc.getThreadLocalDecimal256();
                        for (int j = 0; j < valueCount; j++) {
                            valueFunctions.getQuick(j).getDecimal256(null, d);
                            filterValues.putLong(Long.reverseBytes(d.getHh()));
                            filterValues.putLong(Long.reverseBytes(d.getHl()));
                            filterValues.putLong(Long.reverseBytes(d.getLh()));
                            filterValues.putLong(Long.reverseBytes(d.getLl()));
                        }
                        break;
                    }
                    case ColumnType.UUID:
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            long lo;
                            long hi;
                            int vType = ColumnType.tagOf(f.getType());
                            if (vType == ColumnType.STRING || vType == ColumnType.VARCHAR || vType == ColumnType.SYMBOL) {
                                CharSequence str = f.getStrA(null);
                                if (str == null) {
                                    lo = Numbers.LONG_NULL;
                                    hi = Numbers.LONG_NULL;
                                } else {
                                    try {
                                        Uuid.checkDashesAndLength(str);
                                        lo = Uuid.parseLo(str);
                                        hi = Uuid.parseHi(str);
                                    } catch (NumericException e) {
                                        supported = false;
                                        break;
                                    }
                                }
                            } else {
                                lo = f.getLong128Lo(null);
                                hi = f.getLong128Hi(null);
                            }
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
            boolean skip = decoder.canSkipRowGroup(rowGroupIndex, filterList);
            if (skip) {
                rowGroupsSkipped++;
            }
            return skip;
        } catch (CairoException e) {
            LOG.error().$("error during row group filter pushdown, skipping [rowGroup=").$(rowGroupIndex).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            return false;
        } catch (Throwable e) {
            LOG.error().$("error during row group filter pushdown, skipping [rowGroup=").$(rowGroupIndex).$(", msg=").$(e).$(']').$();
            return false;
        }
    }

    @TestOnly
    public static int getRowGroupsSkipped() {
        return rowGroupsSkipped;
    }

    @TestOnly
    public static void resetRowGroupsSkipped() {
        rowGroupsSkipped = 0;
    }

    private static long encodeColumnAndCount(int columnIndex, int count) {
        return (columnIndex & 0xFFFFFFFFL) | ((long) count << 32);
    }
}
