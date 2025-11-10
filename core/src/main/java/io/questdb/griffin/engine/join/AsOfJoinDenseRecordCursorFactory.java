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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public final class AsOfJoinDenseRecordCursorFactory extends AsOfJoinDenseRecordCursorFactoryBase {
    private final RecordSink masterKeyCopier;
    private final RecordSink slaveKeyCopier;

    public AsOfJoinDenseRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordSink masterKeyCopier,
            RecordCursorFactory slaveFactory,
            RecordSink slaveKeyCopier,
            int columnSplit,
            @Transient ColumnTypes keyTypes,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, masterFactory, slaveFactory, joinContext, toleranceInterval);
        this.masterKeyCopier = masterKeyCopier;
        this.slaveKeyCopier = slaveKeyCopier;
        Map fwdScanKeyToRowId = null;
        Map bwdScanKeyToRowId = null;
        try {
            long maxSinkTargetHeapSize = (long)
                    configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
            fwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, keyTypes, TYPES_VALUE);
            bwdScanKeyToRowId = MapFactory.createUnorderedMap(configuration, keyTypes, TYPES_VALUE);
            this.cursor = new AsOfJoinDenseRecordCursor(
                    columnSplit,
                    fwdScanKeyToRowId,
                    bwdScanKeyToRowId,
                    NullRecordFactory.getInstance(slaveFactory.getMetadata()),
                    masterFactory.getMetadata().getTimestampIndex(),
                    masterFactory.getMetadata().getTimestampType(),
                    new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN),
                    slaveFactory.getMetadata().getTimestampIndex(),
                    slaveFactory.getMetadata().getTimestampType(),
                    new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN)
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
        sink.type("AsOf Join Dense");
    }

    private class AsOfJoinDenseRecordCursor extends AsOfJoinDenseRecordCursorBase {
        private final SingleRecordSink masterSinkTarget;
        private final SingleRecordSink slaveSinkTarget;

        AsOfJoinDenseRecordCursor(
                int columnSplit,
                Map fwdScanKeyToRowId,
                Map bwdScanKeyToRowId,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                int slaveTimestampType,
                SingleRecordSink slaveSinkTarget
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
            this.masterSinkTarget = masterSinkTarget;
            this.slaveSinkTarget = slaveSinkTarget;
        }

        @Override
        public void close() {
            Misc.free(slaveSinkTarget);
            Misc.free(masterSinkTarget);
            super.close();
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            super.of(masterCursor, slaveCursor, circuitBreaker);
            masterSinkTarget.reopen();
            slaveSinkTarget.reopen();
        }

        @Override
        protected int getSlaveJoinKey() {
            slaveSinkTarget.clear();
            slaveKeyCopier.copy(slaveRecB, slaveSinkTarget);
            return DUMMY_VALUE;
        }

        @Override
        protected boolean joinKeysMatch(int slaveKeyToFind, int slaveKey) {
            return masterSinkTarget.memeq(slaveSinkTarget);
        }

        @Override
        protected void putSlaveJoinKey(MapKey key) {
            key.put(slaveRecB, slaveKeyCopier);
        }

        @Override
        protected void putSlaveKeyToFind(MapKey key, int slaveKeyToFind) {
            key.put(masterRecord, masterKeyCopier);
        }

        @Override
        protected int setupSymbolKeyToFind() {
            masterSinkTarget.clear();
            masterKeyCopier.copy(masterRecord, masterSinkTarget);
            return DUMMY_VALUE;
        }
    }
}
