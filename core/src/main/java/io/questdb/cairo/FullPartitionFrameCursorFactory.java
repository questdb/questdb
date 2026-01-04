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
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

public class FullPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final int baseOrder;
    private FullBwdPartitionFrameCursor bwdCursor;
    private FullFwdPartitionFrameCursor fwdCursor;

    public FullPartitionFrameCursorFactory(
            TableToken tableToken,
            long metadataVersion,
            RecordMetadata metadata,
            int order,
            String viewName,
            int viewPosition,
            boolean updateQuery
    ) {
        super(tableToken, metadataVersion, metadata, viewName, viewPosition, updateQuery);
        this.baseOrder = order;
    }

    @Override
    public void close() {
        super.close();
        fwdCursor = Misc.free(fwdCursor);
        bwdCursor = Misc.free(bwdCursor);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, @NotNull IntList columnIndexes, int order) throws SqlException {
        authorizeSelect(executionContext, columnIndexes);
        final TableReader reader = getReader(executionContext);
        try {
            if (order == ORDER_ASC || ((order == ORDER_ANY || order < 0) && baseOrder != ORDER_DESC)) {
                if (fwdCursor == null) {
                    fwdCursor = new FullFwdPartitionFrameCursor();
                }
                return fwdCursor.of(reader);
            }

            // Create backward scanning cursor when needed. Factory requesting backward cursor must
            // still return records in ascending timestamp order.
            if (bwdCursor == null) {
                bwdCursor = new FullBwdPartitionFrameCursor();
            }
            return bwdCursor.of(reader);
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
            sink.type("Frame backward scan");
        } else {
            sink.type("Frame forward scan");
        }
        super.toPlan(sink);
    }
}
