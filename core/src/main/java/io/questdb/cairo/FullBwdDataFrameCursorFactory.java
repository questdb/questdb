/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;

public class FullBwdDataFrameCursorFactory extends AbstractDataFrameCursorFactory {
    private final FullBwdDataFrameCursor cursor = new FullBwdDataFrameCursor();

    private FullFwdDataFrameCursor fwdCursor;

    public FullBwdDataFrameCursorFactory(TableToken tableToken, int tableId, long tableVersion, GenericRecordMetadata metadata) {
        super(tableToken, tableVersion, metadata);
    }

    @Override
    public DataFrameCursor getCursor(SqlExecutionContext executionContext, int order) {
        if (order == ORDER_DESC || order == ORDER_ANY) {
            return cursor.of(getReader(executionContext));
        }

        // Create forward scanning cursor when needed. Factory requesting forward cursor must
        // still return records in descending timestamp order.
        if (fwdCursor == null) {
            fwdCursor = new FullFwdDataFrameCursor();
        }
        return fwdCursor.of(getReader(executionContext));
    }

    @Override
    public int getOrder() {
        return ORDER_DESC;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (sink.getOrder() == ORDER_ASC) {
            sink.type("Frame forward scan");
        } else {
            sink.type("Frame backward scan");
        }
        super.toPlan(sink);
    }
}
