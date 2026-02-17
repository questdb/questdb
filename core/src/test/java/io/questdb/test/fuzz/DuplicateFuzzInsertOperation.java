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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.cairo.TestRecord;

public class DuplicateFuzzInsertOperation extends FuzzInsertOperation {

    private final boolean identical;
    private final IntHashSet upsertKeyIndexes;
    private boolean generatedNonIdentical;

    public DuplicateFuzzInsertOperation(FuzzInsertOperation insertOperation, IntHashSet upsertKeyIndexes, boolean identical) {
        super(insertOperation);
        this.upsertKeyIndexes = upsertKeyIndexes;
        this.identical = identical;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals) {
        generatedNonIdentical = false;
        return super.apply(rnd, engine, tableWriter, virtualTimestampIndex, excludedTsIntervals);
    }

    @Override
    protected void appendColumnValue(
            Rnd rnd,
            int type,
            TableWriter.Row row,
            int columnIndex,
            boolean isNull,
            Utf8StringSink utf8StringSink,
            TestRecord.ArrayBinarySequence binarySequence
    ) {
        if (upsertKeyIndexes.contains(columnIndex) || identical || generatedNonIdentical) {
            super.appendColumnValue(
                    rnd,
                    type,
                    row,
                    columnIndex,
                    isNull,
                    utf8StringSink,
                    binarySequence
            );
            return;
        }

        rnd.nextBoolean();
        // Generate 1 different value for non-key column
        super.appendColumnValue(
                rnd,
                type,
                row,
                columnIndex,
                isNull,
                utf8StringSink,
                binarySequence
        );
        generatedNonIdentical = true;
    }
}
