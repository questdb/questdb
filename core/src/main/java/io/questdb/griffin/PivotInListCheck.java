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

package io.questdb.griffin;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;

/**
 * The values a PIVOT {@code IN (subquery)} returned when the query was compiled. They name the
 * PIVOT's output columns and are part of its plan, so a plan compiled once may be used again only
 * while the subquery still returns them. {@link #verify} runs the subquery again and throws
 * {@link TableReferenceOutOfDateException} when it does not, which makes a cached plan's user
 * recompile the query, as it does after a schema change.
 * <p>
 * The values are compared the way the optimiser read them: each printed by
 * {@link SqlOptimiser#printRecordColumnOrNull}, repeats dropped, in the order the subquery
 * returned them.
 */
public final class PivotInListCheck implements QuietCloseable {
    private final ObjList<String> expected = new ObjList<>();
    private final int position;
    private final CharSequenceHashSet seen = new CharSequenceHashSet();
    private final StringSink sink = new StringSink();
    private RecordCursorFactory factory;

    PivotInListCheck(RecordCursorFactory factory, int position) {
        this.factory = factory;
        this.position = position;
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
    }

    /**
     * Runs the subquery and checks that it returns the values the plan was compiled with.
     *
     * @param executionContext the context of the execution about to use the plan
     * @throws TableReferenceOutOfDateException when the subquery returns other values
     * @throws SqlException                     when the subquery fails
     */
    public void verify(SqlExecutionContext executionContext) throws SqlException {
        seen.clear();
        int matched = 0;
        final RecordMetadata metadata = factory.getMetadata();
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                SqlOptimiser.printRecordColumnOrNull(record, metadata, sink, position);
                if (!seen.add(sink)) {
                    continue;
                }
                if (matched == expected.size() || !Chars.equals(expected.getQuick(matched), sink)) {
                    throw TableReferenceOutOfDateException.ofPivotInList(sink);
                }
                matched++;
            }
        }
        if (matched != expected.size()) {
            throw TableReferenceOutOfDateException.ofPivotInList(expected.getQuick(matched));
        }
    }

    void addExpected(CharSequence value) {
        expected.add(Chars.toString(value));
    }
}
