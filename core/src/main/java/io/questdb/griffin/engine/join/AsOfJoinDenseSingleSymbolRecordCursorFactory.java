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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;

public final class AsOfJoinDenseSingleSymbolRecordCursorFactory extends AsOfJoinDenseRecordCursorFactoryBase {

    private final SymbolJoinKeyMapping joinKeyMapping;
    private final int slaveSymbolColumnIndex;

    public AsOfJoinDenseSingleSymbolRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex,
            SymbolJoinKeyMapping joinKeyMapping,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, masterFactory, slaveFactory, joinContext, toleranceInterval);
        this.joinKeyMapping = joinKeyMapping;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        Map fwdScanKeyToRowId = null;
        Map bwdScanKeyToRowId = null;
        try {
            fwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY, TYPES_VALUE);
            bwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, TYPES_KEY, TYPES_VALUE);
            this.cursor = new AsOfJoinDenseSingleSymbolRecordCursor(
                    columnSplit,
                    fwdScanKeyToRowId,
                    bwdScanKeyToRowId,
                    NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                    masterFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampType()
            );
        } catch (Throwable th) {
            Misc.free(bwdScanKeyToRowId);
            Misc.free(fwdScanKeyToRowId);
            close();
            throw th;
        }
    }


    @Override
    protected void putFactoryType(PlanSink sink) {
        sink.type("AsOf Join Dense Single Symbol");
    }

    private class AsOfJoinDenseSingleSymbolRecordCursor extends AsOfJoinDenseRecordCursorBase {

        AsOfJoinDenseSingleSymbolRecordCursor(
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
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            joinKeyMapping.of(slaveCursor);
        }

        @Override
        protected int getSlaveJoinKey() {
            return slaveRecB.getInt(slaveSymbolColumnIndex);
        }

        @Override
        protected boolean joinKeysMatch(int slaveKeyToFind, int slaveKey) {
            return slaveKeyToFind == slaveKey;
        }

        @Override
        protected void putSlaveJoinKey(MapKey key) {
            key.putInt(slaveRecB.getInt(slaveSymbolColumnIndex));
        }

        @Override
        protected void putSlaveKeyToFind(MapKey key, int slaveKeyToFind) {
            key.putInt(slaveKeyToFind);
        }

        @Override
        protected int setupSymbolKeyToFind() {
            return joinKeyMapping.getSlaveKey(masterRecord);
        }
    }
}
