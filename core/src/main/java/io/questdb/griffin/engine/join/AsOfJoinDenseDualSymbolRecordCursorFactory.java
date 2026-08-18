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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;

/**
 * Specialized "Dense" keyed-ASOF cursor for a two-symbol join key (e.g. {@code (exchange, ticker)}).
 * <p>
 * The general {@link AsOfJoinDenseRecordCursorFactory} serializes the composite key into a
 * {@link io.questdb.cairo.SingleRecordSink} via a {@code RecordSink} and compares with {@code memeq} on
 * every scanned slave row. When both keys are static symbols we can translate each master symbol to its
 * slave-local int key and pack the two ints into a single {@code long} map key, mirroring
 * {@link AsOfJoinDenseSingleSymbolRecordCursorFactory}'s direct-int path. That removes the per-row
 * {@code RecordSink} copy and {@code memeq} while keeping Dense's timestamp-density resilience (the scan
 * is still O(slave), never cliffs).
 */
public final class AsOfJoinDenseDualSymbolRecordCursorFactory extends AsOfJoinDenseRecordCursorFactoryBase {
    // Map layout for the packed dual-symbol key: LONG (two int symbol keys packed) -> LONG slave row id.
    private static final ArrayColumnTypes TYPES_KEY_LONG = new ArrayColumnTypes();
    private final SymbolJoinKeyMapping keyMapping0;
    private final SymbolJoinKeyMapping keyMapping1;
    private final int slaveSymbolColumnIndex0;
    private final int slaveSymbolColumnIndex1;

    public AsOfJoinDenseDualSymbolRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex0,
            int slaveSymbolColumnIndex1,
            SymbolJoinKeyMapping keyMapping0,
            SymbolJoinKeyMapping keyMapping1,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, masterFactory, slaveFactory, joinContext, toleranceInterval);
        this.keyMapping0 = keyMapping0;
        this.keyMapping1 = keyMapping1;
        this.slaveSymbolColumnIndex0 = slaveSymbolColumnIndex0;
        this.slaveSymbolColumnIndex1 = slaveSymbolColumnIndex1;
        Map fwdScanKeyToRowId = null;
        Map bwdScanKeyToRowId = null;
        try {
            fwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY_LONG, TYPES_VALUE, false, false);
            bwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY_LONG, TYPES_VALUE, false, false);
            this.cursor = new AsOfJoinDenseDualSymbolRecordCursor(
                    columnSplit,
                    fwdScanKeyToRowId,
                    bwdScanKeyToRowId,
                    NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                    masterFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampType()
            );
            this.cursor.setAdaptiveBackScanBudget(configuration.getSqlAsOfAdaptiveBackScanBudget());
        } catch (Throwable th) {
            Misc.free(bwdScanKeyToRowId);
            Misc.free(fwdScanKeyToRowId);
            close();
            throw th;
        }
    }

    // Pack two non-negative slave symbol keys into a single long map key.
    private static long pack(int key0, int key1) {
        return ((long) key0 << 32) | (key1 & 0xffffffffL);
    }

    @Override
    public void toPlan(PlanSink sink) {
        super.toPlan(sink);
        // Both keys are symbols by construction; surface the same flag the general Dense cursor emits.
        sink.attr("symbolKeyJoin").val(true);
    }

    @Override
    protected void putFactoryType(PlanSink sink) {
        sink.type("AsOf Join Dense Dual Symbol");
    }

    private class AsOfJoinDenseDualSymbolRecordCursor extends AsOfJoinDenseRecordCursorBase {
        // Packed key of the master row currently being resolved; set in setupSymbolKeyToFind().
        private long masterPackedKey;
        // Packed key of the slave row read by the latest getSlaveJoinKey().
        private long slavePackedKey;

        AsOfJoinDenseDualSymbolRecordCursor(
                int columnSplit,
                Map fwdScanKeyToRowId,
                Map bwdScanKeyToRowId,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType
        ) {
            super(
                    columnSplit,
                    fwdScanKeyToRowId,
                    bwdScanKeyToRowId,
                    nullRecord,
                    masterTimestampIndex,
                    masterTimestampType,
                    slaveTimestampIndex,
                    slaveTimestampType
            );
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            keyMapping0.of(slaveCursor);
            keyMapping1.of(slaveCursor);
        }

        @Override
        protected int getSlaveJoinKey() {
            slavePackedKey = pack(slaveRecB.getInt(slaveSymbolColumnIndex0), slaveRecB.getInt(slaveSymbolColumnIndex1));
            return DUMMY_VALUE;
        }

        @Override
        protected boolean joinKeysMatch(int slaveKeyToFind, int slaveKey) {
            return masterPackedKey == slavePackedKey;
        }

        @Override
        protected void putSlaveJoinKey(MapKey key) {
            key.putLong(pack(slaveRecB.getInt(slaveSymbolColumnIndex0), slaveRecB.getInt(slaveSymbolColumnIndex1)));
        }

        @Override
        protected void putSlaveKeyToFind(MapKey key, int slaveKeyToFind) {
            key.putLong(masterPackedKey);
        }

        @Override
        protected int setupSymbolKeyToFind() {
            int key0 = keyMapping0.getSlaveKey(masterRecord);
            if (key0 == StaticSymbolTable.VALUE_NOT_FOUND) {
                return StaticSymbolTable.VALUE_NOT_FOUND;
            }
            int key1 = keyMapping1.getSlaveKey(masterRecord);
            if (key1 == StaticSymbolTable.VALUE_NOT_FOUND) {
                return StaticSymbolTable.VALUE_NOT_FOUND;
            }
            masterPackedKey = pack(key0, key1);
            return DUMMY_VALUE;
        }
    }

    static {
        TYPES_KEY_LONG.add(ColumnType.LONG);
    }
}
