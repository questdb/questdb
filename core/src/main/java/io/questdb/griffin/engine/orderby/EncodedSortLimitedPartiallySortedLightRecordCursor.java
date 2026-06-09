/*+*****************************************************************************
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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

/**
 * Top-K cursor over a base already sorted on its designated timestamp. When the
 * leading sort key is that timestamp and we want the first N rows, scanning can
 * stop as soon as a completed timestamp group pushes the cumulative row count
 * past the limit: any further row carries a strictly larger leading key than
 * every collected row, so it cannot enter the top-K slice.
 */
final class EncodedSortLimitedPartiallySortedLightRecordCursor extends EncodedSortLimitedLightRecordCursor {
    private final int timestampIndex;
    private long groupTimestamp;
    private long rowsInGroup;
    private long rowsSoFar;
    private boolean timestampInitialized;

    EncodedSortLimitedPartiallySortedLightRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            IntList sortColumnFilter,
            int timestampIndex
    ) {
        super(configuration, metadata, sortColumnFilter);
        this.timestampIndex = timestampIndex;
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        super.of(baseCursor, executionContext);
        baseCursor.expectLimitedIteration();
        rowsInGroup = 0;
        rowsSoFar = 0;
        groupTimestamp = Numbers.LONG_NULL;
        timestampInitialized = false;
    }

    @Override
    protected void runBuild() {
        if (!timestampInitialized) {
            if (!baseCursor.hasNext()) {
                return;
            }
            encodeAndAppendCurrentRow();
            groupTimestamp = baseRecord.getTimestamp(timestampIndex);
            rowsInGroup = 1;
            timestampInitialized = true;
        }
        while (baseCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final long currentTimestamp = baseRecord.getTimestamp(timestampIndex);
            if (groupTimestamp == currentTimestamp) {
                rowsInGroup++;
            } else {
                rowsSoFar += rowsInGroup;
                if (rowsSoFar > limit) {
                    return;
                }
                groupTimestamp = currentTimestamp;
                rowsInGroup = 1;
            }
            encodeAndAppendCurrentRow();
        }
    }
}
