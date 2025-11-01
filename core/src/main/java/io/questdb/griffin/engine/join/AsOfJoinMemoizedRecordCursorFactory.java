/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;

/**
 * Optimized ASOF JOIN implementation for single symbol column joins that uses memoization
 * to avoid redundant backward scans of the slave table.
 * <p>
 * This factory is specialized for joins where the join key is a single symbol column.
 * It significantly outperforms {@link AsOfJoinFastRecordCursorFactory} when some symbols
 * appearing on the master side are sparsely distributed in the slave table, requiring long
 * backward scans.
 * <p>
 * <b>Key Optimization Strategies:</b>
 * <ol>
 *   <li><b>Symbol Location Caching:</b> Maintains a map of previously found symbols with their
 *       row locations and validity periods. The validity period of a symbol spans from the
 *       timestamp where it's located to the timestamp where the previous search that found it started.
 *       As soon as the new search reaches the end of the symbol's validity period, the search completes
 *       and reuses the cached location.</li>
 *   <li><b>Negative Result Caching:</b> Memorizes the range where it didn't find a symbol, so it
 *       can avoid repeating the search with a negative outcome as well.</li>
 *   <li><b>Validity Period Extension:</b> When it reuses a cached result, it also extends the validity period
 *       to the current master timestamp.</li>
 *   <li><b>Drive-By Caching:</b> While scanning for the target symbol, caches all other symbols it encounters
 *       on the way. This is especially important for rare symbols in the slave table as it prevents repeated
 *       deep searches.</li>
 *   <li><b>Scanned Range Skipping:</b> Tracks a contiguous timestamp range that was already scanned. When
 *       searching for an uncached symbol, skips the entire range. This works thanks to Drive-By Caching,
 *       and prevents a repeated deep search for a symbol that occurs even earlier than a previously found
 *       rare symbol.</li>
 * </ol>
 *
 * @see AsOfJoinFastRecordCursorFactory
 * @see AbstractKeyedAsOfJoinRecordCursor
 */
public final class AsOfJoinMemoizedRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsofJoinColumnAccessHelper columnAccessHelper;
    private final AsOfJoinMemoizedRecordCursor cursor;
    private final boolean driveByCaching;
    private final int slaveSymbolColumnIndex;
    private final long toleranceInterval;

    public AsOfJoinMemoizedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex,
            AsofJoinColumnAccessHelper columnAccessHelper,
            JoinContext joinContext,
            long toleranceInterval,
            boolean driveByCaching
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.columnAccessHelper = columnAccessHelper;
        this.toleranceInterval = toleranceInterval;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        this.driveByCaching = driveByCaching;
        this.cursor = new AsOfJoinMemoizedRecordCursor(
                configuration,
                columnSplit,
                NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                masterFactory.getMetadata().getTimestampIndex(),
                masterFactory.getMetadata().getTimestampType(),
                slaveFactory.getMetadata().getTimestampIndex(),
                slaveFactory.getMetadata().getTimestampType()
        );
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameRecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Memoized Scan");
        sink.attr("condition").val(joinContext);
        sink.attr("driveByCache").val(driveByCaching);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(cursor);
    }

    private class AsOfJoinMemoizedRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

        private static final long NOT_REMEMBERED = Long.MIN_VALUE;
        private static final int SLOT_REMEMBERED_ROWID = 0;
        private static final int SLOT_VALIDITY_PERIOD_END = 2;
        private static final int SLOT_VALIDITY_PERIOD_START = 1;
        private static final ArrayColumnTypes TYPES_KEY = new ArrayColumnTypes();
        private static final ArrayColumnTypes TYPES_VALUE = new ArrayColumnTypes();
        private final Map rememberedSymbols;
        private long earliestRowId = Long.MIN_VALUE;
        // These track a contiguous range of slave timestamps that we've already scanned.
        // This range doesn't cover everything we've scanned (there may be many disjoint ranges),
        // but we guarantee we did scan everything inside it.
        // Remembering this helps us avoid rescanning vast ranges of the slave table for
        // symbols that occur way in the past from masterTimestamp.
        private long scannedRangeMaxTimestamp = Long.MIN_VALUE;
        private long scannedRangeMinRowId = Long.MAX_VALUE;
        private long scannedRangeMinTimestamp = Long.MAX_VALUE;

        public AsOfJoinMemoizedRecordCursor(
                CairoConfiguration configuration,
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType
        ) {
            super(
                    columnSplit,
                    nullRecord,
                    masterTimestampIndex,
                    masterTimestampType,
                    slaveTimestampIndex,
                    slaveTimestampType,
                    configuration.getSqlAsOfJoinLookAhead()
            );
            rememberedSymbols = MapFactory.createUnorderedMap(configuration, TYPES_KEY, TYPES_VALUE);
        }

        @Override
        public void close() {
            super.close();
            rememberedSymbols.close();
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            rememberedSymbols.reopen();
            rememberedSymbols.clear();
            columnAccessHelper.of(slaveCursor);
            earliestRowId = Long.MIN_VALUE;
        }

        @Override
        public void toTop() {
            super.toTop();
            // toTop() is called from super.of(), so we may end up here before we have reopened rememberedSymbols
            if (rememberedSymbols.isOpen()) {
                rememberedSymbols.clear();
            }
            scannedRangeMinRowId = Long.MAX_VALUE;
            scannedRangeMinTimestamp = Long.MAX_VALUE;
            scannedRangeMaxTimestamp = Long.MIN_VALUE;
        }

        private void carefullyExtendScannedRange(long masterTimestamp, long slaveTimestamp, long rowId) {
            if (!driveByCaching) {
                return;
            }
            // Extend the remembered scanned range's lower bound with the currently scanned range's lower bound.
            if (slaveTimestamp < scannedRangeMinTimestamp) {
                scannedRangeMinTimestamp = slaveTimestamp;
                if (rowId < 0) {
                    rowId = -rowId - 1;
                }
                scannedRangeMinRowId = rowId;
            }
            // Extend the remembered scanned range's upper bound, but only if the existing range
            // overlaps the range scanned in this invocation of performKeyMatching().
            if (scannedRangeMaxTimestamp == Long.MIN_VALUE || scannedRangeMaxTimestamp >= slaveTimestamp) {
                scannedRangeMaxTimestamp = masterTimestamp;
            }
        }

        private boolean isSlaveWithinToleranceInterval(long masterTimestamp, long slaveTimestamp) {
            return toleranceInterval == Numbers.LONG_NULL || slaveTimestamp >= masterTimestamp - toleranceInterval;
        }

        private void memorizeSymbolLocation(
                long masterTimestamp,
                long slaveTimestamp,
                int slaveSymbolKey,
                long slaveRowId,
                boolean onlyIfNew
        ) {
            MapKey key = rememberedSymbols.withKey();
            key.putInt(slaveSymbolKey);
            MapValue value = key.findValue();

            if (value != null) {
                if (onlyIfNew) {
                    // We remember this symbol, but should memorize it only if new, so return.
                    return;
                }

                // This is purely an assertion branch, to make sure our logic holds.
                // Let's explain the logic! We'll use this notation:
                //   ! ---- | we found the symbol at the start of this period
                //   x ---- | we did not find the symbol anywhere within this period
                //   ? ---- | we may or may not have found the symbol within this period
                //
                // We have these two invariants:
                //
                // 1. masterTimestamp (end of the new period) never goes back in time from invocation to invocation
                //    of performKeyMatching().
                // 2. The symbol does not occur anywhere inside either the remembered or the new period. It may only
                //    appear at its very start.
                //
                // So, we have these possibilities:
                //
                //         x ------------ | remembered period
                //  ? ------------------------ | new period
                //
                // ? ------------ | remembered period
                //          x ------------ | new period
                //
                //  ? --------- | remembered period
                //                   ? ---------- | new period
                //
                // Furthermore, since our search never rescans the remembered period, it will never give up
                // in the middle of it and try to memorize it didn't find anything. So, this is impossible:
                //
                // ? ------------ | remembered period
                //          x ------------ | new period
                //
                // Also, in this case:
                //
                // ? ------------ | remembered period
                //                x --------------- | new period
                //
                // the algo reached the remembered period, and used it. It didn't call this method, it just
                // directly extended the validity period.
                //
                // This also won't occur, since it would imply we searched beyond the end of the search
                // space:
                //
                //         x ------------ | remembered period
                //  ? ------------------------ | new period
                //
                // So, the only way to end up here is this:
                //
                //  ? --------- | remembered period
                //                   ? ---------- | new period
                //
                // We started the search from a more recent timestamp, went backwards,
                // and either found a new symbol or gave up, before reaching the remembered period.
                // We must remember all the new data, same as when the symbol is new.
                assert slaveTimestamp > value.getLong(SLOT_VALIDITY_PERIOD_END)
                        : "slaveTimestamp=" + slaveTimestamp + " <= periodEnd=" + value.getLong(SLOT_VALIDITY_PERIOD_END);
            } else {
                // create new entry
                MapKey newKey = rememberedSymbols.withKey();
                newKey.putInt(slaveSymbolKey);
                value = newKey.createValue();
            }

            // Store all three values in the map
            value.putLong(SLOT_REMEMBERED_ROWID, slaveRowId);
            value.putLong(SLOT_VALIDITY_PERIOD_START, slaveTimestamp);
            value.putLong(SLOT_VALIDITY_PERIOD_END, masterTimestamp);
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            int slaveSymbolKey = columnAccessHelper.getSlaveKey(masterRecord);
            if (slaveSymbolKey == StaticSymbolTable.VALUE_NOT_FOUND) {
                // The master record's symbol does not match any symbol in the slave table,
                // we can immediately report no match and return.
                record.hasSlave(false);
                return;
            }
            final long rememberedRowId, validityPeriodStart, validityPeriodEnd;
            MapKey rememberedKey = rememberedSymbols.withKey();
            rememberedKey.putInt(slaveSymbolKey);
            MapValue value = rememberedKey.findValue();
            if (value != null) {
                rememberedRowId = value.getLong(SLOT_REMEMBERED_ROWID);
                validityPeriodStart = value.getLong(SLOT_VALIDITY_PERIOD_START);
                validityPeriodEnd = value.getLong(SLOT_VALIDITY_PERIOD_END);
            } else {
                rememberedRowId = NOT_REMEMBERED;
                validityPeriodStart = scannedRangeMinTimestamp;
                validityPeriodEnd = scannedRangeMaxTimestamp;
            }

            long rowId = slaveRecB.getRowId();
            long frameRowLo = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowLo());

            boolean didJumpOverScannedRange = false;
            for (; ; ) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp >= validityPeriodStart && slaveTimestamp <= validityPeriodEnd) {
                    // Our search is now either within the validity period of the remembered symbol
                    // or within the remembered scanned range. Let's apply this knowledge.
                    if (rememberedRowId != NOT_REMEMBERED && masterTimestamp > validityPeriodEnd) {
                        // We're within the validity period of the remembered symbol. We started our search from a
                        // timestamp that is at least as recent as the remembered period end.
                        // The fact that we got to this point means we haven't found a more recent symbol.
                        // Therefore, the remembered symbol is still the applicable one. Same for the remembered
                        // non-existence of symbol. We can extend the validity period end to current masterTimestamp.
                        MapKey updateKey = rememberedSymbols.withKey();
                        updateKey.putInt(slaveSymbolKey);
                        MapValue updateValue = updateKey.findValue();
                        assert updateValue != null : "updateValue == null";
                        updateValue.putLong(SLOT_VALIDITY_PERIOD_END, masterTimestamp);
                        carefullyExtendScannedRange(masterTimestamp, validityPeriodStart, rememberedRowId);
                    }
                    if (rememberedRowId >= 0) {
                        // We saw this symbol at rememberedRowId. We can now reuse it and complete the search.
                        if (isSlaveWithinToleranceInterval(masterTimestamp, validityPeriodStart)) {
                            record.hasSlave(true);
                            slaveTimeFrameCursor.recordAt(slaveRecB, rememberedRowId);
                        } else {
                            record.hasSlave(false);
                        }
                        break;
                    } else if (rememberedRowId != NOT_REMEMBERED) {
                        // We remembered a period within which the symbol doesn't occur.
                        //
                        // - Invariant 1: before remembering a period where the symbol doesn't occur, we had to
                        //   search either the whole tolerance interval, or reach the start of the slave table.
                        // - Invariant 2: masterTimestamp (the upper bound of the tolerance interval) is always
                        //   at least as large as any previous masterTimestamp.
                        // - Invariant 3: before reaching this point, we already scanned everything from
                        //   masterTimestamp going back to the upper bound of the remembered period.
                        // - Invariant 4: we already extended the remembered period in the block above.
                        //
                        // Therefore, we're all done. Report no slave row and return.
                        record.hasSlave(false);
                        break;
                    } else if (driveByCaching && !didJumpOverScannedRange) {
                        // We're within the remembered scanned range. Since the symbol isn't remembered, we know
                        // it doesn't occur within this range because we memorize all the symbols we observe
                        // while scanning for any symbol. Jump back over the entire period and continue searching,
                        // unless the period to be skipped extends beyond the tolerance interval.
                        if (!isSlaveWithinToleranceInterval(masterTimestamp, validityPeriodStart)) {
                            record.hasSlave(false);
                            break;
                        }
                        rowId = scannedRangeMinRowId;
                        if (rowId == earliestRowId) {
                            record.hasSlave(false);
                            break;
                        }
                        // It may seem we don't need didJumpOverScannedRange since, after jumping over the scanned
                        // range, we can't enter the enclosing if-block that ensures we're within the validity period.
                        // However, we still need this for the edge case where we just jumped over the scanned range,
                        // because the rowId we set is the lower bound of the range, not one less than that.
                        // So, why don't we set rowId to one less? Because that rowId may be less than the
                        // earliest legal rowId, and we don't want to repeat the entire logic related to that here.
                        didJumpOverScannedRange = true;
                        int frameIndex = Rows.toPartitionIndex(rowId);
                        slaveTimeFrameCursor.jumpTo(frameIndex);
                        slaveTimeFrameCursor.open();
                        slaveTimeFrameCursor.recordAt(slaveRecB, rowId);
                        frameRowLo = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                        slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                    }
                }
                if (!isSlaveWithinToleranceInterval(masterTimestamp, slaveTimestamp)) {
                    // We have been searching outside the remembered period and are past the tolerance interval.
                    // Stop and report no match.
                    record.hasSlave(false);
                    // Memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1).
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -rowId - 1, false);
                    carefullyExtendScannedRange(masterTimestamp, slaveTimestamp, rowId);
                    break;
                }
                int thisSymbolKey = slaveRecB.getInt(slaveSymbolColumnIndex);
                if (thisSymbolKey == slaveSymbolKey) {
                    record.hasSlave(true);
                    // We found the row with the symbol, and we don't remember it from before. Memorize it now.
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, rowId, false);
                    carefullyExtendScannedRange(masterTimestamp, slaveTimestamp, rowId);
                    break;
                } else if (driveByCaching) {
                    // This isn't the symbol we're looking for, but memorize it anyway in the hope that some future
                    // master row will need it.
                    memorizeSymbolLocation(masterTimestamp, slaveTimestamp, thisSymbolKey, rowId, true);
                }

                // Move the slave cursor backwards
                if (rowId > frameRowLo) {
                    rowId--;
                } else {
                    // We exhausted this frame, let's try the previous one.
                    if (!slaveTimeFrameCursor.prev()) {
                        // There is no previous frame, our scan reached the beginning of the table.
                        earliestRowId = rowId;
                        // Memorize that we didn't find the matching symbol by saving rowId as (-rowId - 1).
                        memorizeSymbolLocation(masterTimestamp, slaveTimestamp, slaveSymbolKey, -rowId - 1, false);
                        // Remember the exact range we just scanned. Since masterTimestamp is always at least as large
                        // as any previous masterTimestamp, no need to check for overlap with previously remembered
                        // scanned range. We started from at least as late as any previous scan, and ended at the very
                        // beginning.
                        if (driveByCaching) {
                            scannedRangeMinTimestamp = slaveTimestamp;
                            scannedRangeMinRowId = rowId;
                            scannedRangeMaxTimestamp = masterTimestamp;
                        }
                        record.hasSlave(false);
                        break;
                    }
                    slaveTimeFrameCursor.open();

                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowLo = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                    rowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi() - 1);
                }
                slaveTimeFrameCursor.recordAt(slaveRecB, rowId);
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
        }

        static {
            TYPES_KEY.add(ColumnType.INT);
            TYPES_VALUE.add(ColumnType.LONG);
            TYPES_VALUE.add(ColumnType.LONG);
            TYPES_VALUE.add(ColumnType.LONG);
        }
    }
}
