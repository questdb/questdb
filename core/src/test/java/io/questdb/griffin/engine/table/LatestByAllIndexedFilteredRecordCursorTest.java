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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;


public class LatestByAllIndexedFilteredRecordCursorTest extends AbstractGriffinTest {
    // TODO: smelly test, smelly test why are you broken
    @Test
    public void testIndexedLatestByWithWhereOnOtherCol() throws Exception {

        assertMemoryLeak(() -> {

        });

        String tableName = "tab";

        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            model.col("id", ColumnType.SYMBOL).indexed(true, 2)
                    .col("name", ColumnType.SYMBOL).indexed(true, 2)
                    .col("value", ColumnType.DOUBLE)
                    .col("ts", ColumnType.TIMESTAMP);
            CairoTestUtils.create(model);
        }
        insertRows(tableName);

        // tab latest by id where name = "c2"
        int columnIndex = 0; // id
        IntList columnIndexes = new IntList(4);
        columnIndexes.add(0); // id
        columnIndexes.add(1); // name
        columnIndexes.add(2); // value
        columnIndexes.add(3); // ts
        ObjList<Function> args = new ObjList<>(2);
        args.add(new SymbolColumn(1, true));  // name
        args.add(new StrConstant("c2"));
        Function filter = new EqSymStrFunctionFactory().newInstance(
                0, args, null, configuration, sqlExecutionContext);
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
             LatestByAllIndexedFilteredRecordCursorFactory factory = new LatestByAllIndexedFilteredRecordCursorFactory(
                     reader.getMetadata(),
                     configuration,
                     new FullBwdDataFrameCursorFactory(
                             engine,
                             tableName,
                             -1,
                             -1),
                     columnIndex,
                     filter,
                     columnIndexes,
                     new LongList(0)
             );
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            TestUtils.assertCursor("id\tname\tvalue\tts\n" +
                            "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
                            "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n",
                    cursor,
                    reader.getMetadata(),
                    true,
                    sink);
        }
    }

    private void insertRows(CharSequence tableName) throws Exception {
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "latest-by-insert")) {
            appendRow(writer.newRow(), "d1", "c1", 101.1, "2021-10-05T11:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c1", 101.2, "2021-10-05T12:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c1", 101.3, "2021-10-05T13:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c1", 101.4, "2021-10-05T14:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c2", 102.1, "2021-10-05T11:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c2", 102.2, "2021-10-05T12:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c2", 102.3, "2021-10-05T13:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c2", 102.4, "2021-10-05T14:31:35.878Z");
            appendRow(writer.newRow(), "d1", "c2", 102.5, "2021-10-05T15:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 201.1, "2021-10-05T11:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 201.2, "2021-10-05T12:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 201.3, "2021-10-05T13:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 201.4, "2021-10-05T14:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c2", 401.1, "2021-10-06T11:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 401.2, "2021-10-06T12:31:35.878Z");
            appendRow(writer.newRow(), "d2", "c1", 111.7, "2021-10-06T15:31:35.878Z");
            writer.commit();
        }
        printSqlResult(
                "id\tname\tvalue\tts\n" +
                        "d1\tc1\t101.1\t2021-10-05T11:31:35.878000Z\n" +
                        "d1\tc1\t101.2\t2021-10-05T12:31:35.878000Z\n" +
                        "d1\tc1\t101.3\t2021-10-05T13:31:35.878000Z\n" +
                        "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                        "d1\tc2\t102.1\t2021-10-05T11:31:35.878000Z\n" +
                        "d1\tc2\t102.2\t2021-10-05T12:31:35.878000Z\n" +
                        "d1\tc2\t102.3\t2021-10-05T13:31:35.878000Z\n" +
                        "d1\tc2\t102.4\t2021-10-05T14:31:35.878000Z\n" +
                        "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
                        "d2\tc1\t201.1\t2021-10-05T11:31:35.878000Z\n" +
                        "d2\tc1\t201.2\t2021-10-05T12:31:35.878000Z\n" +
                        "d2\tc1\t201.3\t2021-10-05T13:31:35.878000Z\n" +
                        "d2\tc1\t201.4\t2021-10-05T14:31:35.878000Z\n" +
                        "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
                        "d2\tc1\t401.2\t2021-10-06T12:31:35.878000Z\n" +
                        "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n",
                tableName,
                null,
                null,
                null,
                true,
                true,
                true,
                true,
                null
        );
    }

    private static void appendRow(TableWriter.Row row, CharSequence id, CharSequence name, double value, CharSequence ts) {
        row.putSym(0, id);
        row.putSym(1, name);
        row.putDouble(2, value);
        row.putTimestamp(3, ts);
        row.append();
    }
}
