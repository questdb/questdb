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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.MutableUtf16Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class InTimestampTimestampTest extends AbstractCairoTest {
    @Test
    public void testBindStr() throws SqlException {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts)");

        assertSql("", "test where ts in $1");
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory factory = compiler.compile("test where ts in $1", sqlExecutionContext).getRecordCursorFactory()) {
                bindVariableService.setInt(1, 10);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    RecordMetadata metadata = factory.getMetadata();
                    ((MutableUtf16Sink) sink).clear();
                    CursorPrinter.println(metadata, sink);

                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        TestUtils.println(record, metadata, sink);
                    }
                }
            }
            TestUtils.assertEquals("", sink);
        }
    }
}
