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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class FullFwdPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final FullFwdPartitionFrameCursor cursor;
    private FullBwdPartitionFrameCursor bwdCursor;

    public FullFwdPartitionFrameCursorFactory(TableToken tableToken, long metadataVersion, RecordMetadata metadata) {
        super(tableToken, metadataVersion, metadata);
        this.cursor = new FullFwdPartitionFrameCursor();
    }

    @Override
    public void close() {
        super.close();
        Misc.free(cursor);
        Misc.free(bwdCursor);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, int order) {
        final TableReader reader = getReader(executionContext);
        try {
            if (order == ORDER_ASC || order == ORDER_ANY) {
                return cursor.of(reader);
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
        return ORDER_ASC;
    }

    @Override
    public boolean hasInterval() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (sink.getOrder() == ORDER_DESC) {
            sink.type("Frame backward scan");
        } else {
            sink.type("Frame forward scan");
        }
        super.toPlan(sink);
    }
}
