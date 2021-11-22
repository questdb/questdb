/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.LongList;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;


public class LatestByAllIndexedFilteredAfterRecordCursorTest extends LatestByAllIndexedFilteredRecordCursorTest {
    @Test
    public void testSingleIndexedColLatestByWithWhereOnOtherCol() throws Exception {
        // id   name value   ts
        // d1   c1   101.1   2021-10-05T11:31:35.878000Z
        // d1   c1   101.2   2021-10-05T12:31:35.878000Z
        // d1   c1   101.3   2021-10-05T13:31:35.878000Z
        // d1   c1   101.4   2021-10-05T14:31:35.878000Z
        // d1   c2   102.1   2021-10-05T11:31:35.878000Z
        // d1   c2   102.2   2021-10-05T12:31:35.878000Z
        // d1   c2   102.3   2021-10-05T13:31:35.878000Z
        // d1   c2   102.4   2021-10-05T14:31:35.878000Z
        // d1   c2   102.5   2021-10-05T15:31:35.878000Z
        // d2   c1   201.1   2021-10-05T11:31:35.878000Z
        // d2   c1   201.2   2021-10-05T12:31:35.878000Z
        // d2   c1   201.3   2021-10-05T13:31:35.878000Z
        // d2   c1   201.4   2021-10-05T14:31:35.878000Z
        // d2   c2   401.1   2021-10-06T11:31:35.878000Z
        // d2   c1   401.2   2021-10-06T12:31:35.878000Z
        // d2   c1   111.7   2021-10-06T15:31:35.878000Z
        assertMemoryLeak(() -> {
            String tableName = "tab";
            createTable(tableName);
            insertRows(tableName);
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                 LatestByAllIndexedFilteredAfterRecordCursorFactory factory = new LatestByAllIndexedFilteredAfterRecordCursorFactory(
                         reader.getMetadata(),
                         configuration,
                         new FullBwdDataFrameCursorFactory(
                                 engine,
                                 tableName,
                                 -1,
                                 -1),
                         ID_IDX, // select id, name, value, ts from tab latest by id where name = "c2"
                         createFilter(NAME_IDX, "c2"),
                         SELECT_ALL_IDXS,
                         new LongList(0)
                 );
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                TestUtils.assertCursor("id\tname\tvalue\tts\n" +
                                "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n",
                        cursor,
                        reader.getMetadata(),
                        true,
                        sink);
            }
        });
    }
}
