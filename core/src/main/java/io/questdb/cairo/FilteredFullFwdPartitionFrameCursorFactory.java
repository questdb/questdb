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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class FilteredFullFwdPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final FilteredFullFwdPartitionFrameCursor cursor;
    private final int filterColumnIndex;
    private final Function filterKeyFunc;

    public FilteredFullFwdPartitionFrameCursorFactory(TableToken tableToken,
                                                      long metadataVersion,
                                                      RecordMetadata metadata,
                                                      int filterColumnIndex,
                                                      Function filterKeyFunc
    ) {
        super(tableToken, metadataVersion, metadata);
        this.cursor = new FilteredFullFwdPartitionFrameCursor(filterColumnIndex, filterKeyFunc.getHash(null));
        this.filterKeyFunc = filterKeyFunc;
        this.filterColumnIndex = filterColumnIndex;
    }

    @Override
    public void close() {
        super.close();
        Misc.free(cursor);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, int order) {
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
    public void toPlan(PlanSink sink) {
        sink.type("Skip filtered frame forward scan");
        super.toPlan(sink);
        sink.attr("where").val(getMetadata().getColumnName(filterColumnIndex)).val('=').val(filterKeyFunc);
    }
}
