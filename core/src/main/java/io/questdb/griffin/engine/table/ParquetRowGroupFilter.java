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
    // The compiled filter's f32 tolerance (jit/impl/consts.h FLOAT_EPSILON). It is
    // (float) DOUBLE_TOLERANCE, i.e. 1.0000000133514320e-10 - slightly LARGER than the double one,
    // and the f32 arm applies it to a difference computed at f32 as well.
    private static final float FLOAT_TOLERANCE = (float) Numbers.DOUBLE_TOLERANCE;
    private static final Log LOG = LogFactory.getLog(ParquetRowGroupFilter.class);
    // How far tryPutFloatFromDouble / tryPutDoubleFromDouble / putDoubleEq may walk a widened bound
    // outward before they give up and decline the pushdown. A couple of steps cover every bound whose
    // magnitude puts the value spacing above the comparison tolerance; below that (a FLOAT bound
    // under roughly 1e-10, or a DOUBLE bound within a tolerance of zero, where "d - tolerance"
    // cancels) every neighbouring value is tolerance-equal to the bound and no reachable bound is
    // safe, so declining is the answer.
    private static final int MAX_BOUND_STEPS = 4;
    // 2^53. A double represents every integer below this exactly, and 2^53 + 1 is the first one it
    // cannot; at or above it a 64-bit column no longer round-trips through the double width the
    // row-level filter compares at.
    private static final double MAX_EXACT_INTEGRAL_DOUBLE = 9_007_199_254_740_992d;
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

                // This routine runs once per parquet partition, and a bound the arms below cannot
                // materialise raises the same exception every time - a string bound on a TIMESTAMP
                // column arrives as a StrConstant whose getLong() throws ImplicitCastException. When
                // every value is a compile-time constant that answer is permanent, so take it from
                // the condition instead of re-deriving it for the next thousand partitions.
                if (condition.isSerializationDeclined()) {
                    continue;
                }

                final int columnType = condition.getColumnType();
                final long valuesOffset = filterValues.getAppendOffset();
                boolean supported = true;
                // The op and value count the filter list carries. The INT arm rewrites the op for a
                // single-value comparison whose bound no INT row can satisfy (see unsatisfiableIntOp),
                // and the DOUBLE arm rewrites an equality into the BETWEEN that spans its tolerance
                // band (see putDoubleEq), which is the one rewrite that changes the count.
                int effectiveOp = opType;
                int effectiveCount = valueCount;
                // Whether a decline below is a property of the values alone. The arms themselves and
                // the generic catch are: they dispatch on the condition's own column type and value
                // functions, and line 197 has already skipped any partition whose parquet type
                // differs. A CairoException is not - its documented cause is a memory limit while
                // appending, which the next partition may well clear - so it must not be cached.
                boolean isDeclineDeterministic = false;
                // A bound the arms below cannot materialise throws rather than returning a
                // flag: PushdownFilterExtractor compiles every value standalone with no target
                // type, so a string literal against a TIMESTAMP column arrives as a StrConstant
                // whose getLong() raises ImplicitCastException. Scope the catch to this one
                // condition -- an outer catch would abandon the whole filter list and cost every
                // other condition in the query its pruning. supported=false rolls the partially
                // written values back at the check below.
                try {
                    switch (ColumnType.tagOf(columnType)) {
                        case ColumnType.BYTE:
                            for (int j = 0; j < valueCount; j++) {
                                Function f = valueFunctions.getQuick(j);
                                switch (f.getType()) {
                                    case ColumnType.SHORT:
                                        filterValues.putInt(f.getShort(null));
                                        break;
                                    case ColumnType.INT:
                                        // INT value compares at INT precision (BYTE promotes to INT):
                                        // getInt() wraps overflowing INT arithmetic like the native
                                        // scan; getLong() would keep the un-wrapped product and prune
                                        // wrongly.
                                        filterValues.putInt(f.getInt(null));
                                        break;
                                    case ColumnType.LONG:
                                        // LONG value compares at LONG precision: take the full width
                                        // and clamp into the INT stats slot (out-of-range values match
                                        // no BYTE row, and the saturated bound preserves GT/GE/LT/LE).
                                        filterValues.putInt(clampLongToInt(f.getLong(null)));
                                        break;
                                    case ColumnType.FLOAT:
                                    case ColumnType.DOUBLE:
                                        if (!tryPutIntFromDouble(filterValues, f.getDouble(null), opType)) {
                                            supported = false;
                                        }
                                        break;
                                    default:
                                        // CHAR lands here too, and must. A CHAR bound on a BYTE column
                                        // resolves to EqShortFunctionFactory (CHAR does not overload to
                                        // BYTE), so the row filter reads it as CharFunction.getShort ->
                                        // castCharToNumber and sees the digit: '5' is 5. getByte() below
                                        // takes the same castCharToNumber route and agrees. Pushing
                                        // getChar()'s code point 53 instead prunes every group whose
                                        // BYTE stats miss 53. The SHORT arm has always routed CHAR here.
                                        filterValues.putInt(f.getByte(null));
                                }
                                // A break inside the switch above leaves the switch, not this loop, so
                                // the remaining values would keep evaluating and appending after the
                                // bound has already been declined. The INT/LONG arms are if/else chains
                                // and break out of the loop directly.
                                if (!supported) {
                                    break;
                                }
                            }
                            break;
                        case ColumnType.SHORT:
                            for (int j = 0; j < valueCount; j++) {
                                Function f = valueFunctions.getQuick(j);
                                switch (f.getType()) {
                                    case ColumnType.INT:
                                        // INT precision (SHORT promotes to INT); getInt() wraps like
                                        // the native scan.
                                        filterValues.putInt(f.getInt(null));
                                        break;
                                    case ColumnType.LONG:
                                        // LONG precision: full width, clamped into the INT stats slot.
                                        filterValues.putInt(clampLongToInt(f.getLong(null)));
                                        break;
                                    case ColumnType.FLOAT:
                                    case ColumnType.DOUBLE:
                                        if (!tryPutIntFromDouble(filterValues, f.getDouble(null), opType)) {
                                            supported = false;
                                        }
                                        break;
                                    default:
                                        filterValues.putInt(f.getShort(null));
                                }
                                // See the BYTE arm: break out of the value loop, not just the switch.
                                if (!supported) {
                                    break;
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
                                if (vType == ColumnType.LONG) {
                                    // An out-of-INT-range LONG bound saturates in the 32-bit stats
                                    // slot and would false-prune a group whose INT stats sit on the
                                    // boundary (all INT_MAX vs "< 5e9"). Unlike BYTE/SHORT, INT stats
                                    // can reach it, so it takes the op no INT row satisfies (prune
                                    // every group) or declines -- see unsatisfiableIntOp.
                                    long v = f.getLong(null);
                                    if (v != Numbers.LONG_NULL && (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE)) {
                                        effectiveOp = unsatisfiableIntOp(opType, valueCount, v > 0);
                                        if (effectiveOp == PushdownFilterExtractor.OP_UNSUPPORTED) {
                                            supported = false;
                                            break;
                                        }
                                        filterValues.putInt(unsatisfiableIntBound(v > 0));
                                        continue;
                                    }
                                    filterValues.putInt(clampLongToInt(v));
                                } else if (vType == ColumnType.FLOAT || vType == ColumnType.DOUBLE) {
                                    final double b = integralBound(f.getDouble(null), opType);
                                    if (Numbers.isNull(b)) {
                                        supported = false;
                                        break;
                                    }
                                    if (b < Integer.MIN_VALUE || b > Integer.MAX_VALUE) {
                                        effectiveOp = unsatisfiableIntOp(opType, valueCount, b > 0);
                                        if (effectiveOp == PushdownFilterExtractor.OP_UNSUPPORTED) {
                                            supported = false;
                                            break;
                                        }
                                        filterValues.putInt(unsatisfiableIntBound(b > 0));
                                        continue;
                                    }
                                    filterValues.putInt((int) b);
                                } else {
                                    // INT (and narrower) compare at INT precision; getInt() wraps
                                    // overflowing INT arithmetic like the native scan.
                                    filterValues.putInt(f.getInt(null));
                                }
                            }
                            break;
                        case ColumnType.TIMESTAMP: {
                            if (opType == PushdownFilterExtractor.OP_EQ) {
                                boolean isAllCompatible = true;
                                for (int j = 0; j < valueCount; j++) {
                                    int vType = valueFunctions.getQuick(j).getType();
                                    if (!ColumnType.isTimestamp(vType) && vType != ColumnType.DATE) {
                                        isAllCompatible = false;
                                        break;
                                    }
                                }
                                if (!isAllCompatible) {
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
                                } else if (vType == ColumnType.FLOAT || vType == ColumnType.DOUBLE) {
                                    // getLong() throws on a FLOAT/DOUBLE function, and the row-level filter
                                    // compares this column at double width, so the bound takes the same
                                    // guard as the LONG arm.
                                    if (!tryPutLongFromDouble(filterValues, f.getDouble(null), opType)) {
                                        supported = false;
                                        break;
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
                                    if (!tryPutLongFromDouble(filterValues, f.getDouble(null), opType)) {
                                        supported = false;
                                        break;
                                    }
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
                                } else if (vType == ColumnType.FLOAT || vType == ColumnType.DOUBLE) {
                                    // Same as the TIMESTAMP arm: getLong() throws on a FLOAT/DOUBLE
                                    // function, and the row-level filter compares at double width.
                                    if (!tryPutLongFromDouble(filterValues, f.getDouble(null), opType)) {
                                        supported = false;
                                        break;
                                    }
                                } else {
                                    filterValues.putLong(f.getLong(null));
                                }
                            }
                            break;
                        case ColumnType.FLOAT:
                            for (int j = 0; j < valueCount; j++) {
                                if (!tryPutFloatFromDouble(filterValues, valueFunctions.getQuick(j).getDouble(null), opType)) {
                                    supported = false;
                                    break;
                                }
                            }
                            break;
                        case ColumnType.DOUBLE:
                            if (opType == PushdownFilterExtractor.OP_EQ) {
                                final int rewrittenCount = putDoubleEq(filterValues, valueFunctions, valueCount);
                                if (rewrittenCount < 0) {
                                    supported = false;
                                    break;
                                }
                                if (rewrittenCount != valueCount) {
                                    effectiveOp = PushdownFilterExtractor.OP_BETWEEN;
                                    effectiveCount = rewrittenCount;
                                }
                                break;
                            }
                            for (int j = 0; j < valueCount; j++) {
                                if (!tryPutDoubleFromDouble(filterValues, valueFunctions.getQuick(j).getDouble(null), opType)) {
                                    supported = false;
                                    break;
                                }
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
                    isDeclineDeterministic = true;
                } catch (CairoException e) {
                    // Debug, not error: declining one bound is a designed fallback that costs only
                    // pruning. The outer catch keeps error level -- losing the whole filter list is
                    // not routine. This arm catches the CairoException family (a memory limit while
                    // appending, say); the routine case, a string bound on a non-designated
                    // TIMESTAMP, raises ImplicitCastException, which extends RuntimeException and so
                    // lands in the generic arm below.
                    LOG.debug().$("skipping filter condition [column=").$safe(condition.getColumnName()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    supported = false;
                } catch (Exception e) {
                    LOG.debug().$("skipping filter condition [column=").$safe(condition.getColumnName()).$(", msg=").$(e).$(']').$();
                    supported = false;
                    // The routine case: a string bound on a TIMESTAMP column raises
                    // ImplicitCastException, and it will raise it again for every partition.
                    isDeclineDeterministic = true;
                }

                if (!supported || valueCount > 0x00FFFFFF) { // 16_777_215, max value count that fits in 24-bit field of ColumnFilterPacked
                    filterValues.jumpTo(valuesOffset);
                    // Only a constant-valued condition can be declined for good: a bind variable is
                    // a runtime constant, and the next execution may bind a value that serializes.
                    if (!supported && isDeclineDeterministic && condition.hasConstantValuesOnly()) {
                        condition.setSerializationDeclined();
                    }
                    continue;
                }

                filterList.add(encodeColumnCountAndOp(columnIndex, effectiveCount, effectiveOp));
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

    // The double a row group would have to hold for the native predicate to prune it wrongly: the
    // first one on the side the predicate drops. Mirrors firstPrunedFloat for the DOUBLE stats slot
    // (which needs no narrowing, only the tolerance corrections).
    private static double firstPrunedDouble(double bound, int opType) {
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
            case PushdownFilterExtractor.OP_GT:
                return bound;
            case PushdownFilterExtractor.OP_LE:
                return Math.nextUp(bound);
            default: // OP_GE
                return Math.nextDown(bound);
        }
    }

    // The float a row group would have to hold for the native predicate to prune it wrongly: the
    // first one on the side the predicate drops. LT prunes on "min >= bound" and GT on
    // "max <= bound", so the bound itself is the first dropped row; LE prunes on "min > bound" and
    // GE on "max < bound", which already exclude it, so its neighbour is.
    private static float firstPrunedFloat(float bound, int opType) {
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
            case PushdownFilterExtractor.OP_GT:
                return bound;
            case PushdownFilterExtractor.OP_LE:
                return Math.nextUp(bound);
            default: // OP_GE
                return Math.nextDown(bound);
        }
    }

    /**
     * Rounds a FLOAT/DOUBLE bound to the integral bound that selects exactly the same integer
     * rows, so a fractional bound still prunes instead of declining pushdown. For an integer
     * column {@code i < 1.5} is {@code i < 2} and {@code i > 1.5} is {@code i > 1}, so each op
     * rounds the way that cannot tighten the predicate: {@code <} and {@code >=} take the ceiling,
     * {@code <=} and {@code >} take the floor. An integral bound comes back unchanged under every
     * op, so an exact bound keeps pushing as it did.
     * <p>
     * The rounding runs on the TOLERANCE-widened bound, not the bound itself, because the
     * row-level filter compares the widened column with {@link Numbers#DOUBLE_TOLERANCE}: {@code <=}
     * and {@code >=} keep a row up to one tolerance beyond the bound (see toleranceBound), and an
     * integer row can sit in that band - {@code i <= 0.99999999999} keeps the row at 1. Rounding the
     * bare bound floors that to {@code i <= 0}, which prunes the group holding it. Widening first
     * lands on {@code i <= 1} and keeps the group. It costs nothing anywhere else: a tolerance is
     * far too small to move the ceiling or floor of any bound that is not already within 1e-10 of
     * an integer.
     * <p>
     * Every other op declines a fractional bound by returning NaN. EQ (and the IN list that shares
     * its op code) has no rounding that preserves it: a fractional bound equals no integer row at
     * all, and the pruner cannot express "no group matches". Declining is safe even for the bound
     * within a tolerance of an integer, which the row-level filter does match - the group simply
     * stays and the filter decides. An integral EQ bound has no such hole: the tolerance band of an
     * integer holds no other integer, so only the exact value matches. BETWEEN never reaches here
     * with a FLOAT/DOUBLE bound - QuestDB only accepts it over a TIMESTAMP column, against TIMESTAMP
     * bounds - and declining is the safe answer if that ever changes. A NULL (NaN) bound stays NaN
     * and declines as well.
     */
    private static double integralBound(double d, int opType) {
        if (!isPushableFloatingBound(d)) {
            // The tolerance widening below turns an infinite bound into a FINITE one
            // (toleranceBound(+Inf, GE) is Math.nextDown(+Inf - 1e-10), i.e. Double.MAX_VALUE),
            // which then reads as an ordinary out-of-range bound and prunes every row group.
            return Double.NaN;
        }
        return switch (opType) {
            case PushdownFilterExtractor.OP_LT, PushdownFilterExtractor.OP_GE -> Math.ceil(toleranceBound(d, opType));
            case PushdownFilterExtractor.OP_LE, PushdownFilterExtractor.OP_GT -> Math.floor(toleranceBound(d, opType));
            default -> d == Math.floor(d) ? d : Double.NaN;
        };
    }

    // Whether the row-level filter calls NO double other than d itself equal to d: its neighbouring
    // doubles must fall outside the DOUBLE_TOLERANCE band around it. That holds once the double
    // spacing (ulp) exceeds the 1e-10 tolerance, i.e. |d| at or above 2^19 (~5.2e5). Below it the band spans many doubles,
    // any of which a row group may hold instead of d, and neither the min/max stats (which would
    // place d outside [min, max]) nor the bloom filter (which hashes the exact bits of d) can see
    // it - so only a certified bound may push as an exact equality. A NULL (NaN) bound is equal to
    // NULL rows alone, which the native side decides from its has_nulls rather than from the stats,
    // so it certifies as exact. That holds only because has_nulls treats a FLOAT or DOUBLE row group
    // as possibly-null whatever its null_count says: the writer counts NaN alone, so an infinity -
    // which Numbers.equals calls equal to this bound - is stored as an ordinary value and left
    // uncounted, and reading null_count directly would prune the group holding it. See
    // ParquetDecoder::nulls_hidden_from_stats. That reasoning covers NaN only, hence the isNaN test
    // rather than Numbers.isNull: the latter also calls +/-Infinity NULL, and an infinite bound is
    // pushed as a real value against the min/max stats - see isPushableFloatingBound.
    private static boolean isExactEqDouble(double d) {
        return Double.isNaN(d)
                || (!Numbers.equals(Math.nextUp(d), d) && !Numbers.equals(Math.nextDown(d), d));
    }

    // The compiled f32 arm's equality: the difference, its absolute value and the comparison all at
    // single precision, and STRICT - float_cmp_epsilon is "epsilon > |lhs - rhs|" on every backend
    // (x86 ucomiss/seta, aarch64 fcmp/cset GT, avx2 vcmpps kLT).
    private static boolean isFloatEqAtSinglePrecision(float l, float bound) {
        return FLOAT_TOLERANCE > Math.abs(l - bound);
    }

    /**
     * Whether row group pruning may push a FLOAT/DOUBLE bound at all.
     * <p>
     * {@link Numbers#isNull(double)} is an exponent-bits test, so it calls {@code +/-Infinity} NULL
     * exactly as it calls NaN NULL, and {@link Numbers#equals(double, double)} therefore calls an
     * infinite bound EQUAL to a NULL row. Every inclusive and equality form of the row filter reads
     * {@code eq || ...} (see {@link #isRowKept} and {@code LtDoubleVVFunctionFactory#getBool}), so
     * {@code c6 >= +Infinity} keeps every NULL row - while NULL rows never appear in a row group's
     * min/max statistics, so no pushed bound can preserve them. Pruning runs before the row filter
     * and nothing downstream can undo it, so an infinite bound has to decline outright.
     * <p>
     * A NaN bound is a genuine SQL NULL and keeps its own handling: the native side decides it from
     * the row group's null count rather than from the stats.
     * <p>
     * Only a runtime constant delivers an infinity here. A constant expression such as
     * {@code 1e308 * 10.0} folds through {@code DoubleConstant#newInstance} to NULL long before
     * pushdown sees it, but {@code PushdownFilterExtractor} accepts any
     * {@code isConstantOrRuntimeConstant()} function and reads it at scan time, and the PGWire
     * DOUBLE binder passes raw IEEE bits straight through {@code Double.longBitsToDouble}.
     */
    private static boolean isPushableFloatingBound(double d) {
        return !Double.isInfinite(d);
    }

    // Whether ANY row-level filter keeps a row holding value l, at the width and with the tolerance
    // it compares at: a FLOAT column widens to double (the caller passes the widened float), a DOUBLE
    // column already is one, and the two are called equal when they lie within DOUBLE_TOLERANCE
    // (LtDoubleVVFunctionFactory is "!equals(l, r) && l < r", its negation "equals(l, r) || l > r",
    // and so on for the rest).
    // <p>
    // The engine has THREE of these filters, and this method models the two that compare at DOUBLE
    // width. Their tolerance test differs at the boundary: Numbers.equals() is inclusive
    // (|l - d| <= tolerance) while the compiled filter's double_cmp_epsilon (jit/impl/x86.h) is
    // strict (|l - d| < tolerance), so at |l - d| == tolerance exactly they disagree - "<" drops the
    // row on the Java filter and keeps it on the compiled one. The strict test therefore decides the
    // ops that exclude equality, the inclusive one the ops that include it.
    // <p>
    // The third is the compiled filter's f32 arm, which a FLOAT column runs whenever it does not
    // widen; isRowKeptByCompiledFloatFilter models it and the FLOAT bound arm ORs it in. A DOUBLE
    // column never reaches it.
    // <p>
    // Which one runs is not a property of the query alone: a frame falls back to the Java filter
    // when it has column tops or type casts, and ALSO when parquet late materialization leaves an
    // unread column's address at 0 - which projecting or ordering by a column the filter does not
    // read is enough to trigger. Pruning is an unconditional drop that no later filter can undo, so
    // certify against ALL of them rather than depend on which one runs: count the row as kept when
    // ANY keeps it. It costs at most a few f32 ulps of bound (see isRowKeptByCompiledFloatFilter),
    // never a row.
    private static boolean isRowKept(double l, double d, int opType) {
        final boolean isEq = Numbers.equals(l, d);
        final boolean isEqStrict = isEq && Math.abs(l - d) < Numbers.DOUBLE_TOLERANCE;
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
                return !isEqStrict && l < d;
            case PushdownFilterExtractor.OP_LE:
                return isEq || l < d;
            case PushdownFilterExtractor.OP_GT:
                return !isEqStrict && l > d;
            default: // OP_GE
                return isEq || l > d;
        }
    }

    /**
     * Whether the compiled filter's f32 arm keeps a row holding float {@code l}, for a FLOAT column
     * compared against bound {@code d}.
     * <p>
     * A FLOAT column that does not widen compares at f32 throughout: the JIT emits
     * {@code float_ne_epsilon AND float_lt} for "&lt;" (jit/x86.h, mirrored in aarch64.h and avx2.h),
     * where equality is {@code FLOAT_EPSILON > |f32(l) - f32(d)|} - STRICT, and with both the
     * subtraction and the tolerance at single precision. Neither matches the two DOUBLE-width
     * filters {@link #isRowKept} models, in two ways that both keep rows they drop:
     * {@code FLOAT_EPSILON} is larger than {@code DOUBLE_TOLERANCE}, and rounding the difference to
     * f32 can carry it from inside the tolerance to outside it. So a row can be kept here and
     * dropped there, and a bound certified against the f64 pair alone prunes the group holding it.
     * <p>
     * {@code float_cmp_epsilon} also answers "equal" when BOTH operands are non-finite. That arm is
     * not modelled because it cannot be reached: {@code tryPutFloatFromDouble} declines before the
     * loop when {@code (float) d} is not finite, so {@code bound} is always finite here, and the
     * one-sided infinity case agrees with the backends as written.
     */
    private static boolean isRowKeptByCompiledFloatFilter(float l, double d, int opType) {
        final float bound = (float) d;
        assert Float.isFinite(bound);
        final boolean isEq = isFloatEqAtSinglePrecision(l, bound);
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
                return !isEq && l < bound;
            case PushdownFilterExtractor.OP_LE:
                return isEq || l < bound;
            case PushdownFilterExtractor.OP_GT:
                return !isEq && l > bound;
            case PushdownFilterExtractor.OP_EQ:
                return isEq;
            default: // OP_GE
                return isEq || l > bound;
        }
    }

    /**
     * Appends the values of a DOUBLE equality (or of the IN list that shares its op code) into the
     * filter, and reports how many slots it wrote: {@code valueCount} to keep the equality as it
     * stands, 2 to replace it with the BETWEEN spanning the bound's tolerance band, or -1 to decline
     * the pushdown.
     * <p>
     * An exact equality is only pushable when every value certifies through {@link #isExactEqDouble}
     * - otherwise the group may hold a row the row-level filter calls equal to the bound, and pruning
     * (which runs first, and which no later filter can undo) would drop it. The tolerance band IS the
     * set of matching rows, though, and for a lone bound the native side can test exactly that band:
     * {@code d = c} becomes {@code d BETWEEN c - tolerance AND c + tolerance}, which prunes every
     * group whose stats lie clear of the band and keeps every group that may hold a match. It gives
     * up the bloom filter (the native side consults it for equality only, and it hashes exact bits,
     * which is the unsound part), so a group whose [min, max] spans the band but holds no matching
     * row now survives pruning; the row-level filter then returns no rows for it.
     * <p>
     * The band's ends must be certified, not just computed. When {@code |d|} is within a tolerance of
     * zero, {@code d - tolerance} cancels and lands a single subnormal ulp off zero - far inside the
     * band the filter keeps (which, near zero, still calls a tiny value on the far side of zero equal
     * to {@code d}). Pruning on that bound would drop such a group. Step each end outward until the
     * first double it would prune away is one the row filter no longer calls equal to {@code d}, or
     * decline when no reachable end is safe. Away from zero the raw ends certify at once.
     * <p>
     * An IN list has no single band and declines instead. The rewrite also does not apply to the
     * NULL (NaN) bound, which certifies as exact and keeps pushing as an equality: a band around NaN
     * is empty, and NULL rows do not reach the min/max stats at all.
     */
    private static int putDoubleEq(MemoryCARWImpl filterValues, ObjList<Function> valueFunctions, int valueCount) {
        // Evaluate and append each bound exactly once, certifying it in the same pass. On the exact
        // path the appended values ARE the result; on the single-bound rewrite path below the one
        // appended value is rolled back and replaced with the tolerance band; on the multi-value
        // decline the caller rolls filterValues back to the offset it captured before this call.
        final long startOffset = filterValues.getAppendOffset();
        boolean isExact = true;
        double d = Double.NaN;
        for (int i = 0; i < valueCount; i++) {
            d = valueFunctions.getQuick(i).getDouble(null);
            if (!isPushableFloatingBound(d)) {
                // The caller rolls filterValues back to the offset it captured before this call.
                return -1;
            }
            filterValues.putDouble(d);
            if (!isExactEqDouble(d)) {
                isExact = false;
                break;
            }
        }
        if (isExact) {
            return valueCount;
        }
        if (valueCount != 1) {
            return -1;
        }
        // Single inexact bound: drop the value just appended and replace it with the tolerance band.
        // d already holds valueFunctions.getQuick(0).getDouble(null) from the loop above.
        filterValues.jumpTo(startOffset);
        // The native BETWEEN prunes a group when max < lo or min > hi, so the first double each end
        // would drop is the one just past it (nextDown(lo) / nextUp(hi)). Certify against the
        // inclusive row filter (the widest of the two, so a superset of what the strict one keeps).
        double lo = Math.nextDown(d - Numbers.DOUBLE_TOLERANCE);
        for (int i = 0; Numbers.equals(Math.nextDown(lo), d); i++) {
            if (i == MAX_BOUND_STEPS) {
                return -1;
            }
            lo = Math.nextDown(lo);
        }
        double hi = Math.nextUp(d + Numbers.DOUBLE_TOLERANCE);
        for (int i = 0; Numbers.equals(Math.nextUp(hi), d); i++) {
            if (i == MAX_BOUND_STEPS) {
                return -1;
            }
            hi = Math.nextUp(hi);
        }
        filterValues.putDouble(lo);
        filterValues.putDouble(hi);
        return 2;
    }

    /**
     * Widens a DOUBLE bound by the comparison tolerance in the direction that keeps pruning safe, or
     * returns NaN to decline the pushdown.
     * <p>
     * A DOUBLE column needs no narrowing (unlike FLOAT, see tryPutFloatFromDouble) but compares with
     * the same {@link Numbers#DOUBLE_TOLERANCE}: {@code d <= c} keeps every row up to {@code c + tolerance}
     * and {@code d >= c} every row down to {@code c - tolerance}, while the native side prunes on
     * {@code min > bound} and {@code max < bound}. The bare bound therefore prunes a group whose rows
     * are merely tolerance-equal to it; pushing the bound one tolerance outward (and one ulp beyond
     * that, so the rounding of the sum cannot land back inside the band) prunes only the groups that
     * hold nothing the filter keeps.
     * <p>
     * The strict ops need no widening: the tolerance makes the filter STRICTER than the pruner there
     * ({@code d < c} really keeps {@code d < c - tolerance}), so a group the exact bound prunes holds
     * no matching row anyway. BETWEEN carries two bounds with no single direction and never reaches a
     * DOUBLE column today (QuestDB accepts it over TIMESTAMP only); decline it. A NULL (NaN) bound
     * stays NaN, and the native side declines to prune on it.
     */
    private static double toleranceBound(double d, int opType) {
        return switch (opType) {
            case PushdownFilterExtractor.OP_LT, PushdownFilterExtractor.OP_GT -> d;
            case PushdownFilterExtractor.OP_LE -> Math.nextUp(d + Numbers.DOUBLE_TOLERANCE);
            case PushdownFilterExtractor.OP_GE -> Math.nextDown(d - Numbers.DOUBLE_TOLERANCE);
            default -> Double.NaN;
        };
    }

    // Appends a DOUBLE bound into a DOUBLE stats slot, or reports that it cannot be pushed down.
    // The strict ops (LT/GT) push the bare bound: the tolerance makes the row filter stricter than
    // the pruner there, so the exact bound already prunes only groups the filter clears. The
    // inclusive ops (LE/GE) absorb the tolerance (see toleranceBound) and then CERTIFY the widened
    // bound against the row filter, exactly as tryPutFloatFromDouble does: near |d| ~ DOUBLE_TOLERANCE
    // the "d - tolerance" / "d + tolerance" subtraction cancels and the raw bound lands a subnormal
    // ulp off zero, far inside the band the filter keeps, so pushing it would false-prune. Step the
    // bound outward until the first double the pruner would drop is one the filter drops too, or
    // decline. Away from zero it certifies at once. Equality does not come through here - putDoubleEq
    // handles it, and it is the only op that can rewrite the whole condition.
    private static boolean tryPutDoubleFromDouble(MemoryCARWImpl filterValues, double d, int opType) {
        if (!isPushableFloatingBound(d)) {
            return false;
        }
        final boolean isStepUp;  // the direction that makes the bound SAFER, not tighter
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
            case PushdownFilterExtractor.OP_GT:
                filterValues.putDouble(d);
                return true;
            case PushdownFilterExtractor.OP_LE:
                isStepUp = true;
                break;
            case PushdownFilterExtractor.OP_GE:
                isStepUp = false;
                break;
            default:
                return false;
        }
        double bound = toleranceBound(d, opType);
        for (int i = 0; i <= MAX_BOUND_STEPS; i++) {
            if (!isRowKept(firstPrunedDouble(bound, opType), d, opType)) {
                filterValues.putDouble(bound);
                return true;
            }
            bound = isStepUp ? Math.nextUp(bound) : Math.nextDown(bound);
        }
        return false;
    }

    // Appends a bound into a FLOAT stats slot, or reports that it cannot be pushed down. The stats
    // are 32-bit floats, but the row-level filter compares a FLOAT column at DOUBLE width (there is
    // no (FLOAT, FLOAT) comparison factory - only the double ones, e.g. LtDoubleVVFunctionFactory
    // "<(DD)" - so both operands promote), and getFloat() narrowed the bound with round-to-NEAREST.
    // Nearest is not pruning-safe: it can move the bound ACROSS the double one, past a group's
    // boundary float, and prune a group whose rows the filter keeps. Pruning runs before the row
    // filter, so those rows are lost outright.
    //
    // Two things separate the pushed float bound from the double comparison, and the bound has to
    // absorb both:
    //  1. The TOLERANCE. QuestDB compares floating point with Numbers.DOUBLE_TOLERANCE (1e-10):
    //     "c < d" is !Numbers.equals(c, d) && c < d, so it really keeps c < d - 1e-10, and
    //     "c >= d" keeps c >= d - 1e-10. Pivot on d - tol for "<" / ">=" and on d + tol for
    //     "<=" / ">". Ignoring the tolerance prunes a group holding a row that is merely
    //     tolerance-EQUAL to the bound.
    //  2. The NARROWING. Round what is left to a float in the direction the op preserves, as
    //     integralBound does for the integer slots: "<" / ">=" need the SMALLEST float >= the
    //     pivot, "<=" / ">" the LARGEST float <= it.
    //
    // Rounding is not enough on its own, because the two corrections are computed in double and can
    // land the bound back ON a row the filter keeps: "d - tolerance" collapses onto a float whenever
    // d sits about one tolerance away from one (the residual is far below half a double ulp), and
    // near zero the tolerance band spans a great many floats, so no single rounding step reaches the
    // end of it. Rather than reason about those cases, ask the filter: step the bound outward until
    // the first row the pruner would DROP is a row the filter drops too. That is the whole safety
    // property, checked directly - a bound is pushed only once isRowKept has certified it - and it
    // takes one step or two for every bound a query realistically carries. Give up after
    // MAX_BOUND_STEPS (only the near-zero band needs more, where every float is
    // tolerance-equal to the bound) and decline the pushdown; a superset scan is always safe.
    //
    // EQ (and the IN list sharing its op code) has no direction to round in - it pushes the nearest
    // float and prunes a group only when that value falls outside [min, max]. That is right exactly
    // while the nearest float is the ONLY one within the tolerance of the bound, which is the case
    // once the float spacing (ulp) exceeds the 1e-10 tolerance, i.e. |bound| at or above 2^-10 (~9.8e-4). Below that the band
    // holds several floats, the group may hold one that is not the nearest, and pruning drops rows
    // the filter keeps: for "c6 = 0.0005", the floats 4.9999997E-4, 5.0E-4 and 5.000001E-4 are all
    // tolerance-equal to the bound, yet only 5.0E-4 is pushed. So certify EQ too - decline unless
    // the neighbouring floats are outside the band. (The hole predates this change: master pushes
    // the identical (float) d. The DOUBLE arm carries the same tolerance blindness - it needs no
    // narrowing, only the tolerance corrections; toleranceBound and putDoubleEq apply them there.)
    //
    // Every op certifies against the compiled filter's f32 arm as well as the two DOUBLE-width ones
    // - see isRowKeptByCompiledFloatFilter. Its tolerance is the widest of the three and it computes
    // the difference at single precision, so it can call a value equal to the bound where the others
    // do not; a bound certified without it lands on such a value and prunes the group holding it.
    // It costs at most an ulp or two of bound: the certification loop stops one step later, and
    // EQ declines in a slightly wider band around the neighbouring floats.
    //
    // BETWEEN carries two bounds with no single direction and never reaches a FLOAT column today
    // (QuestDB accepts it over TIMESTAMP only); decline it. A NULL (NaN) bound compares false
    // against everything, so nothing is kept and the bound certifies at once; the native side
    // rejects a NaN bound anyway.
    private static boolean tryPutFloatFromDouble(MemoryCARWImpl filterValues, double d, int opType) {
        if (!isPushableFloatingBound(d)) {
            return false;
        }
        // A finite bound beyond the FLOAT range narrows to +/-Infinity under (float) d. QuestDB's own
        // writer records an overflowing FLOAT as +/-Infinity in the stats, so an infinite bound prunes
        // safely there; but an external read_parquet() file may keep an infinite row out of its stats,
        // and pruning on an infinite bound would then drop a row the (double-width) row filter keeps.
        // Decline; a superset scan is always safe.
        if (!Float.isFinite((float) d)) {
            return false;
        }
        final boolean isRoundUp;  // "<" / ">=" pivot on d - tolerance, "<=" / ">" on d + tolerance
        final boolean isStepUp;   // the direction that makes the bound SAFER, not tighter
        switch (opType) {
            case PushdownFilterExtractor.OP_LT:
                isRoundUp = true;
                isStepUp = true;
                break;
            case PushdownFilterExtractor.OP_GE:
                isRoundUp = true;
                isStepUp = false;
                break;
            case PushdownFilterExtractor.OP_LE:
                isRoundUp = false;
                isStepUp = true;
                break;
            case PushdownFilterExtractor.OP_GT:
                isRoundUp = false;
                isStepUp = false;
                break;
            case PushdownFilterExtractor.OP_EQ: {
                final float nearest = (float) d;
                final float up = Math.nextUp(nearest);
                final float down = Math.nextDown(nearest);
                if (Numbers.equals((double) up, d)
                        || Numbers.equals((double) down, d)
                        || isRowKeptByCompiledFloatFilter(up, d, opType)
                        || isRowKeptByCompiledFloatFilter(down, d, opType)) {
                    // More than one float is equal to the bound under some filter, so the group may
                    // hold a matching row this one does not reach. The f32 arm has to be asked as
                    // well as the double one: its tolerance is the wider of the two, so it can call
                    // a neighbour equal where Numbers.equals does not. Decline; a superset scan is
                    // always safe.
                    return false;
                }
                filterValues.putFloat(nearest);
                return true;
            }
            default:
                return false;
        }
        final double pivot = isRoundUp ? d - Numbers.DOUBLE_TOLERANCE : d + Numbers.DOUBLE_TOLERANCE;
        float bound = (float) pivot;
        if (isRoundUp) {
            if ((double) bound < pivot) {
                bound = Math.nextUp(bound);
            }
        } else if ((double) bound > pivot) {
            bound = Math.nextDown(bound);
        }
        for (int i = 0; i <= MAX_BOUND_STEPS; i++) {
            // Certify against every filter that can run over a FLOAT column, the compiled f32 arm
            // included: it keeps rows the two DOUBLE-width ones drop, and a bound certified without
            // it lands on such a row and prunes the group holding it.
            final float pruned = firstPrunedFloat(bound, opType);
            if (!isRowKept(pruned, d, opType) && !isRowKeptByCompiledFloatFilter(pruned, d, opType)) {
                filterValues.putFloat(bound);
                return true;
            }
            bound = isStepUp ? Math.nextUp(bound) : Math.nextDown(bound);
        }
        return false;
    }

    // Appends a FLOAT/DOUBLE bound into an INT stats slot (BYTE/SHORT/INT columns), or reports
    // that the bound cannot be pushed down. Row group pruning runs before the row-level filter,
    // so the pushed integer must equal the double exactly: (int) truncates a fractional bound
    // toward zero and saturates an out-of-range one, and either would false-prune a group whose
    // stats sit on the resulting boundary (all-1 vs "< 1.5", all-INT_MAX vs "< 5e9"). integralBound
    // rounds a fractional bound in the direction the op preserves, so "< 1.5" still pushes as
    // "< 2"; a bound outside the INT slot has no in-range equivalent under every op, so it declines
    // and lets the row-level filter evaluate it -- a superset scan is always safe. (The INT arm
    // handles its own out-of-range bounds, where the column's range makes them decidable.)
    private static boolean tryPutIntFromDouble(MemoryCARWImpl filterValues, double d, int opType) {
        final double b = integralBound(d, opType);
        if (Numbers.isNull(b) || b < Integer.MIN_VALUE || b > Integer.MAX_VALUE) {
            return false;
        }
        filterValues.putInt((int) b);
        return true;
    }

    // Appends a FLOAT/DOUBLE bound into a 64-bit stats slot (LONG/TIMESTAMP/DATE columns), or reports
    // that the bound cannot be pushed down. Stricter than tryPutIntFromDouble, because the two sides
    // compare at different widths: there is no (LONG, DOUBLE) comparison, so the row-level filter
    // widens the column to DOUBLE, while pruning compares the stats against the pushed bound at long
    // width. Both agree only below 2^53, where a double still represents every integer exactly. Above
    // it the pruner is the finer of the two and skips groups whose rows the filter keeps:
    // (double) 10000000000000001L is exactly 1e16, so "c6 <= 1e16" keeps that row while the pushed
    // bound (long) 1e16 == 10000000000000000 excludes its group. Decline instead -- a superset scan is
    // always safe. The 2^53 ceiling also subsumes the Long.MIN_VALUE bound (which would push the
    // LONG_NULL sentinel as a real value) and the Long.MAX_VALUE one (where (long) 2^63 saturates yet
    // still round-trips). Below 2^53 a rounded bound is exact across the whole LONG range: every long
    // outside [-2^53, 2^53] widens to a double of magnitude at least 2^53, so it sits on the same
    // side of the bound as the rounded long does.
    private static boolean tryPutLongFromDouble(MemoryCARWImpl filterValues, double d, int opType) {
        final double b = integralBound(d, opType);
        if (Numbers.isNull(b) || Math.abs(b) >= MAX_EXACT_INTEGRAL_DOUBLE) {
            return false;
        }
        filterValues.putLong((long) b);
        return true;
    }

    // The in-range bound that goes with unsatisfiableIntOp's rewritten op. INT_MIN is the native
    // side's NULL sentinel (it declines to prune on it), so the below-range bound saturates one
    // above it; every group whose min stat holds a real value still prunes.
    private static int unsatisfiableIntBound(boolean isAboveRange) {
        return isAboveRange ? Integer.MAX_VALUE : Integer.MIN_VALUE + 1;
    }

    /**
     * Maps a comparison whose bound lies outside the INT range onto the (op, bound) pair that
     * prunes every row group, or {@link PushdownFilterExtractor#OP_UNSUPPORTED} to decline
     * pushdown. The caller pushes {@link #unsatisfiableIntBound} as the value.
     * <p>
     * No INT row can be greater than a bound above INT_MAX - not even a NULL one, since the
     * row-level comparison rejects NULL - so the predicate matches nothing and every group can go.
     * {@code >} does that with the bound saturated to INT_MAX: a group is dropped when its max is
     * at most the bound, and an INT max always is. {@code >=} has no bound with that property - a
     * group whose max is exactly INT_MAX survives {@code >= INT_MAX} - so it rewrites to
     * {@code > INT_MAX}, which selects the same (empty) row set. Below INT_MIN the picture mirrors,
     * with one wrinkle: the native side reads a pushed INT_MIN as the NULL sentinel and declines to
     * prune on it, so the bound saturates to INT_MIN + 1 instead. {@code < INT_MIN + 1} drops every
     * group whose min is at least INT_MIN + 1 - every group whose stats hold a real value - and
     * {@code <=} rewrites to it.
     * <p>
     * The opposite direction ({@code <} / {@code <=} above INT_MAX, {@code >} / {@code >=} below
     * INT_MIN) holds for every INT row: there is nothing to prune, and the saturated bound would
     * false-prune a boundary group, so it declines. EQ (an IN list carries more than one value)
     * and BETWEEN (two bounds) decline as well - the op travels with the whole condition, so only
     * a lone bound can rewrite it, which {@code valueCount} enforces.
     */
    private static int unsatisfiableIntOp(int opType, int valueCount, boolean isAboveRange) {
        if (valueCount != 1) {
            return PushdownFilterExtractor.OP_UNSUPPORTED;
        }
        if (isAboveRange) {
            return opType == PushdownFilterExtractor.OP_GT || opType == PushdownFilterExtractor.OP_GE
                    ? PushdownFilterExtractor.OP_GT
                    : PushdownFilterExtractor.OP_UNSUPPORTED;
        }
        return opType == PushdownFilterExtractor.OP_LT || opType == PushdownFilterExtractor.OP_LE
                ? PushdownFilterExtractor.OP_LT
                : PushdownFilterExtractor.OP_UNSUPPORTED;
    }
}
