/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FullBwdDataFrameCursorTest extends AbstractCairoTest {
    @Test
    public void testReload() throws Exception {

        final String expected = "-409854405\t339631474\t1970-01-04T00:00:00.000000Z\n" +
                "1569490116\t1573662097\t1970-01-03T16:00:00.000000Z\n" +
                "806715481\t1545253512\t1970-01-03T08:00:00.000000Z\n" +
                "-1436881714\t-1575378703\t1970-01-03T00:00:00.000000Z\n" +
                "-1191262516\t-2041844972\t1970-01-02T16:00:00.000000Z\n" +
                "1868723706\t-847531048\t1970-01-02T08:00:00.000000Z\n" +
                "1326447242\t592859671\t1970-01-02T00:00:00.000000Z\n" +
                "73575701\t-948263339\t1970-01-01T16:00:00.000000Z\n" +
                "1548800833\t-727724771\t1970-01-01T08:00:00.000000Z\n" +
                "-1148479920\t315515118\t1970-01-01T00:00:00.000000Z\n";

        final String expectedNext = "-1975183723\t-1252906348\t1975-01-04T00:00:00.000000Z\n" +
                "-1125169127\t1631244228\t1975-01-03T16:00:00.000000Z\n" +
                "1404198\t-1715058769\t1975-01-03T08:00:00.000000Z\n" +
                "-1101822104\t-1153445279\t1975-01-03T00:00:00.000000Z\n" +
                "-1844391305\t-1520872171\t1975-01-02T16:00:00.000000Z\n" +
                "-85170055\t-1792928964\t1975-01-02T08:00:00.000000Z\n" +
                "-1432278050\t426455968\t1975-01-02T00:00:00.000000Z\n" +
                "1125579207\t-1849627000\t1975-01-01T16:00:00.000000Z\n" +
                "-1532328444\t-1458132197\t1975-01-01T08:00:00.000000Z\n" +
                "1530831067\t1904508147\t1975-01-01T00:00:00.000000Z\n";
        assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.INT).
                    col("b", ColumnType.INT).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }


            Rnd rnd = new Rnd();
            long timestamp = 0;
            long increment = 3600000000L * 8;
            int N = 10;

            try (TableWriter w = new TableWriter(configuration, "x")) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = w.newRow(timestamp);
                    row.putInt(0, rnd.nextInt());
                    row.putInt(1, rnd.nextInt());
                    row.append();
                    timestamp += increment;
                }
                w.commit();
                Assert.assertEquals(N, w.size());

                FullBwdDataFrameCursorFactory factory = new FullBwdDataFrameCursorFactory(engine, "x", TableUtils.ANY_TABLE_ID,0);
                final TableReaderRecord record = new TableReaderRecord();

                try (final DataFrameCursor cursor = factory.getCursor(AllowAllSqlSecurityContext.INSTANCE)) {
                    printCursor(record, cursor);

                    TestUtils.assertEquals(expected, sink);

                    // now add some more rows

                    timestamp = TimestampFormatUtils.parseTimestamp("1975-01-01T00:00:00.000Z");
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = w.newRow(timestamp);
                        row.putInt(0, rnd.nextInt());
                        row.putInt(1, rnd.nextInt());
                        row.append();
                        timestamp += increment;
                    }
                    w.commit();

                    Assert.assertTrue(cursor.reload());
                    printCursor(record, cursor);
                    TestUtils.assertEquals(expectedNext + expected, sink);
                }

                w.removeColumn("a");

                try {
                    factory.getCursor(AllowAllSqlSecurityContext.INSTANCE);
                    Assert.fail();
                } catch (ReaderOutOfDateException ignored) {
                }
            }
        });

    }

    private void printCursor(TableReaderRecord record, DataFrameCursor cursor) {
        sink.clear();
        record.of(cursor.getTableReader());
        DataFrame frame;
        while ((frame = cursor.next()) != null) {
            record.jumpTo(frame.getPartitionIndex(), 0);
            for (long index = frame.getRowHi() - 1, lo = frame.getRowLo() - 1; index > lo; index--) {
                record.setRecordIndex(index);
                printer.print(record, cursor.getTableReader().getMetadata(), sink);
            }
        }
    }
}