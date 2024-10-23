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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class LimitPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor();

    public LimitPartitionFrameCursorFactory(TableToken tableToken, long tableVersion, GenericRecordMetadata metadata,
                                            long limit) {
        super(tableToken, tableVersion, metadata);
        cursor.setLimit(limit);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (order != ORDER_ASC && order != ORDER_ANY) {
            throw SqlException.$(0, "Unsupported order; only ASC/ANY supported for this cursor");
        }

        final TableReader reader = getReader(executionContext);
        try {
            return cursor.of(reader);
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
    public boolean followedLimitAdvice() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (sink.getOrder() == ORDER_DESC) {
            sink.type("Frame backward scan");
        } else {
            sink.type("Frame limit scan lo: " + Math.abs(cursor.getLimit()));
        }
        super.toPlan(sink);
    }
}