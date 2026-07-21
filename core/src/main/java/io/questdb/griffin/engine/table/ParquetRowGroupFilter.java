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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.griffin.engine.table.parquet.ParquetRowGroupSkipper;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for Parquet row group bloom filter pushdown.
 * <p>
 * This class provides static methods to build filter lists and check if
 * row groups can be skipped based on bloom filter conditions.
 */
public final class ParquetRowGroupFilter {
    public static final int FILTER_BUFFER_MAX_PAGES = 1_048_576; // 128MB with 128-byte pages
    public static final long FILTER_BUFFER_PAGE_SIZE = 128;
    public static final int LONGS_PER_FILTER = 3;
    private static final Log LOG = LogFactory.getLog(ParquetRowGroupFilter.class);
    private static final AtomicInteger rowGroupsSkipped = new AtomicInteger();

    /**
     * Check if a row group can be skipped based on the prepared filter list.
     * Call {@link #prepareFilterList} once per partition before using this method.
     *
     * @param rowGroupIndex the row group index to check
     * @param skipper       the row group skipper (typically backed by {@link ParquetMetaFileReader} or {@link ParquetFileDecoder})
     * @param filterList    filter descriptors prepared by {@link #prepareFilterList}
     * @return true if the row group can be safely skipped, false otherwise
     */
    public static boolean canSkipRowGroup(
            int rowGroupIndex,
            ParquetRowGroupSkipper skipper,
            DirectLongList filterList,
            long filterBufEnd
    ) {
        try {
            boolean skip = skipper.canSkipRowGroup(rowGroupIndex, filterList, filterBufEnd);
            if (skip) {
                rowGroupsSkipped.incrementAndGet();
            }
            return skip;
        } catch (CairoException e) {
            LOG.error().$("error during row group filter pushdown, skipping [rowGroup=").$(rowGroupIndex).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            return false;
        } catch (Exception e) {
            LOG.error().$("error during row group filter pushdown, skipping [rowGroup=").$(rowGroupIndex).$(", msg=").$(e).$(']').$();
            return false;
        }
    }

    @TestOnly
    public static int getRowGroupsSkipped() {
        return rowGroupsSkipped.get();
    }

    /**
     * Prepare the filter list from pushdown filter conditions. This resolves column indices
     * and serializes filter values into the provided buffers. Call once per partition, then
     * use {@link #canSkipRowGroup} for each row group.
     *
     * @param metadata                 the partition metadata for column index resolution
     * @param pushdownFilterConditions the filter conditions to apply
     * @param filterList               reusable buffer for filter descriptors: [encoded(col_idx, count, op), ptr, columnType] per filter
     * @param filterValues             reusable memory buffer for filter values
     * @param resolveByColumnId        when true, resolve the Parquet column by the condition's
     *                                 stable writer index (column id); when false, by name.
     *                                 Native-table partitions pass true so renamed columns map
     *                                 to the correct Parquet column (its frozen name is stale);
     *                                 the read_parquet() table function passes false because it
     *                                 projects external files by name.
     * @return true if filters were prepared successfully and row group pruning should be attempted
     */
    public static boolean prepareFilterList(
            ParquetFileDecoder.Metadata metadata,
            ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions,
            DirectLongList filterList,
            MemoryCARWImpl filterValues,
            boolean resolveByColumnId
    ) {
        return prepareFilterListImpl(metadata, null, pushdownFilterConditions, filterList, filterValues, resolveByColumnId);
    }

    /**
     * Overload for ParquetMetaFileReader -- resolves columns from the _pm sidecar metadata.
     */
    public static boolean prepareFilterList(
            ParquetMetaFileReader parquetMetaReader,
            ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions,
            DirectLongList filterList,
            MemoryCARWImpl filterValues,
            boolean resolveByColumnId
    ) {
        return prepareFilterListImpl(null, parquetMetaReader, pushdownFilterConditions, filterList, filterValues, resolveByColumnId);
    }

    private static boolean prepareFilterListImpl(
            ParquetFileDecoder.Metadata legacyMetadata,
            ParquetMetaFileReader parquetMetaReader,
            ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions,
            DirectLongList filterList,
            MemoryCARWImpl filterValues,
            boolean resolveByColumnId
    ) {
        try {
            if (pushdownFilterConditions == null || pushdownFilterConditions.size() == 0) {
                return false;
            }
            filterList.clear();
            filterList.reopen();
            filterValues.jumpTo(0);

            for (int i = 0, n = pushdownFilterConditions.size(); i < n; i++) {
                final PushdownFilterExtractor.PushdownFilterCondition condition = pushdownFilterConditions.getQuick(i);
                final int opType = condition.getOperationType();
                final ObjList<Function> valueFunctions = condition.getValueFunctions();
                final int valueCount = valueFunctions.size();

                final int columnIndex;
                if (resolveByColumnId) {
                    // The Parquet column name is frozen at write time and goes stale on rename,
                    // so map the filtered column to the Parquet column by its stable id instead.
                    // Only native-table partitions resolve by id, and they always supply the
                    // ParquetMetaFileReader; the legacy read_parquet() overload resolves by name.
                    assert parquetMetaReader != null;
                    columnIndex = parquetMetaReader.getColumnIndexById(condition.getColumnWriterIndex());
                } else {
                    columnIndex = legacyMetadata != null
                            ? legacyMetadata.getColumnIndex(condition.getColumnName())
                            : parquetMetaReader.getColumnIndex(condition.getColumnName());
                }
                if (columnIndex < 0) {
                    continue;
                }

                // Skip pushdown for type-converted columns -- parquet metadata
                // (bloom filters, min/max stats, null counts) reflects the old column type.
                int parquetColumnType = legacyMetadata != null
                        ? legacyMetadata.getColumnType(columnIndex)
                        : parquetMetaReader.getColumnType(columnIndex);
                if (parquetColumnType != condition.getColumnType()) {
                    continue;
                }

                if (opType == PushdownFilterExtractor.OP_IS_NULL || opType == PushdownFilterExtractor.OP_IS_NOT_NULL) {
                    filterList.add(encodeColumnCountAndOp(columnIndex, 0, opType));
                    filterList.add(0);
                    filterList.add(condition.getColumnType());
                    continue;
                }

                if (valueCount == 0) {
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
                                case ColumnType.LONG:
                                    // Read via getLong() so INT arithmetic (Mul/Add/Sub/Neg)
                                    // returns the long-precise result that the native filter
                                    // sees after BYTE-to-LONG comparison promotion. Clamp into
                                    // INT range for the parquet stats slot; values outside that
                                    // range cannot equal any BYTE row anyway, and the saturated
                                    // bound preserves the GT/GE/LT/LE result against BYTE stats.
                                    filterValues.putInt(clampLongToInt(f.getLong(null)));
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
                                case ColumnType.LONG:
                                    filterValues.putInt(clampLongToInt(f.getLong(null)));
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
                            int vType = f.getType();
                            if (vType == ColumnType.INT || vType == ColumnType.LONG) {
                                filterValues.putInt(clampLongToInt(f.getLong(null)));
                            } else if (vType == ColumnType.FLOAT || vType == ColumnType.DOUBLE) {
                                filterValues.putInt((int) f.getDouble(null));
                            } else {
                                filterValues.putInt(f.getInt(null));
                            }
                        }
                        break;
                    case ColumnType.TIMESTAMP: {
                        if (opType == PushdownFilterExtractor.OP_EQ) {
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
                        }

                        TimestampDriver driver = ColumnType.getTimestampDriver(columnType);
                        for (int j = 0; j < valueCount; j++) {
                            Function f = valueFunctions.getQuick(j);
                            int vType = f.getType();
                            if (ColumnType.isTimestamp(vType) || vType == ColumnType.DATE) {
                                if (columnType == vType) {
                                    filterValues.putLong(f.getTimestamp(null));
                                } else {
                                    filterValues.putLong(driver.from(f.getTimestamp(null), ColumnType.getTimestampType(vType)));
                                }
                            } else {
                                filterValues.putLong(f.getLong(null));
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
                    case ColumnType.STRING, ColumnType.SYMBOL, ColumnType.VARCHAR:
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

                if (!supported || valueCount > 0x00FFFFFF) { // 16_777_215, max value count that fits in 24-bit field of ColumnFilterPacked
                    filterValues.jumpTo(valuesOffset);
                    continue;
                }

                filterList.add(encodeColumnCountAndOp(columnIndex, valueCount, opType));
                filterList.add(valuesOffset);
                filterList.add(columnType);
            }
            final int filterCount = (int) (filterList.size() / LONGS_PER_FILTER);
            if (filterCount == 0) {
                return false;
            }

            final long baseAddress = filterValues.getAddress();
            for (long i = 1, n = filterList.size(); i < n; i += LONGS_PER_FILTER) {
                filterList.set(i, baseAddress + filterList.get(i));
            }
            return true;
        } catch (CairoException e) {
            LOG.error().$("error during filter list preparation [msg=").$(e.getFlyweightMessage()).$(']').$();
            return false;
        } catch (Exception e) {
            LOG.error().$("error during filter list preparation [msg=").$(e).$(']').$();
            return false;
        }
    }

    @TestOnly
    public static void resetRowGroupsSkipped() {
        rowGroupsSkipped.set(0);
    }

    private static int clampLongToInt(long v) {
        if (v == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        // Numbers.INT_NULL == Integer.MIN_VALUE, so a non-null value must not collapse
        // onto the null sentinel after clamping.
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE + 1;
        }
        return (int) v;
    }

    private static long encodeColumnCountAndOp(int columnIndex, int count, int op) {
        return (columnIndex & 0xFFFFFFFFL) | ((long) (count & 0x00FFFFFF) << 32) | ((long) (op & 0xFF) << 56);
    }
}
