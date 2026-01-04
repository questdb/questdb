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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.Misc;

public class IntervalPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final int baseOrder;
    private final RuntimeIntrinsicIntervalModel intervalModel;
    private final int timestampIndex;
    private IntervalBwdPartitionFrameCursor bwdCursor;
    private IntervalFwdPartitionFrameCursor fwdCursor;

    public IntervalPartitionFrameCursorFactory(
            TableToken tableToken,
            long metadataVersion,
            RuntimeIntrinsicIntervalModel intervalModel,
            int timestampIndex,
            RecordMetadata metadata,
            int baseOrder,
            String viewName,
            int viewPosition,
            boolean updateQuery
    ) {
        super(tableToken, metadataVersion, metadata, viewName, viewPosition, updateQuery);
        this.timestampIndex = timestampIndex;
        this.intervalModel = intervalModel;
        this.baseOrder = baseOrder;
    }

    @Override
    public void close() {
        super.close();
        fwdCursor = Misc.free(fwdCursor);
        bwdCursor = Misc.free(bwdCursor);
        Misc.free(intervalModel);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, IntList columnIndexes, int order) throws SqlException {
        authorizeSelect(executionContext, columnIndexes);
        final TableReader reader = getReader(executionContext);
        try {
            if (order == ORDER_ASC || ((order == ORDER_ANY || order < 0) && baseOrder != ORDER_DESC)) {
                if (fwdCursor == null) {
                    fwdCursor = new IntervalFwdPartitionFrameCursor(intervalModel, timestampIndex);
                }
                return fwdCursor.of(reader, executionContext);
            }

            if (bwdCursor == null) {
                bwdCursor = new IntervalBwdPartitionFrameCursor(intervalModel, timestampIndex);
            }
            return bwdCursor.of(reader, executionContext);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    @Override
    public int getOrder() {
        return baseOrder;
    }

    @Override
    public void toPlan(PlanSink sink) {
        int order = sink.getOrder();
        if (order == ORDER_ANY || order < 0) {
            order = baseOrder;
        }
        if (order == ORDER_DESC) {
            sink.type("Interval backward scan");
        } else {
            sink.type("Interval forward scan");
        }
        super.toPlan(sink);
        sink.attr("intervals").val(intervalModel);
    }
}
