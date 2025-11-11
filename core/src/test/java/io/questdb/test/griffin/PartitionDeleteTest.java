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

package io.questdb.test.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionDeleteTest extends AbstractCairoTest {

    @Test
    public void testBCSequence() throws SqlException, NumericException {
        execute("create table events (sequence long, event binary, timestamp timestamp) timestamp(timestamp) partition by DAY");
        engine.releaseAllWriters();

        try (TableWriter writer = newOffPoolWriter(configuration, "events")) {
            long ts = MicrosFormatUtils.parseTimestamp("2020-06-30T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = writer.newRow(ts);
                r.putLong(0, i);
                r.append();
            }
            ts = MicrosFormatUtils.parseTimestamp("2020-07-01T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = writer.newRow(ts);
                r.putLong(0, 100 + i);
                r.append();
            }

            ts = MicrosFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = writer.newRow(ts);
                r.putLong(0, 200 + i);
                r.append();
            }
            writer.commit();
        }

        try (
                TableReader reader = newOffPoolReader(configuration, "events");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }

            try (TableWriter writer = newOffPoolWriter(configuration, "events")) {
                long ts = MicrosFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.putLong(0, 250 + i);
                    row.append();
                }
                writer.commit();

                Assert.assertTrue(writer.removePartition(MicrosFormatUtils.parseTimestamp("2020-06-30T00:00:00.000000Z")));

                reader.reload();

                cursor.toTop();
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {

                }

                ts = MicrosFormatUtils.parseTimestamp("2020-07-03T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.putLong(0, 300 + i);
                    row.append();
                }
                writer.commit();

                ts = MicrosFormatUtils.parseTimestamp("2020-07-04T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.putLong(0, 400 + i);
                    row.append();
                }
                writer.commit();

                ts = MicrosFormatUtils.parseTimestamp("2020-07-05T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.putLong(0, 500 + i);
                    row.append();
                }
                writer.commit();

                Assert.assertTrue(writer.removePartition(MicrosFormatUtils.parseTimestamp("2020-07-01T00:00:00.000000Z")));
                Assert.assertTrue(writer.removePartition(MicrosFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z")));
                Assert.assertTrue(writer.removePartition(MicrosFormatUtils.parseTimestamp("2020-07-03T00:00:00.000000Z")));

                Assert.assertTrue(reader.reload());

                cursor.toTop();
                println(reader.getMetadata(), cursor);

                String expected = "sequence\tevent\ttimestamp\n" +
                        "400\t\t2020-07-04T00:00:00.000000Z\n" +
                        "401\t\t2020-07-04T00:00:00.000000Z\n" +
                        "402\t\t2020-07-04T00:00:00.000000Z\n" +
                        "403\t\t2020-07-04T00:00:00.000000Z\n" +
                        "404\t\t2020-07-04T00:00:00.000000Z\n" +
                        "405\t\t2020-07-04T00:00:00.000000Z\n" +
                        "406\t\t2020-07-04T00:00:00.000000Z\n" +
                        "407\t\t2020-07-04T00:00:00.000000Z\n" +
                        "408\t\t2020-07-04T00:00:00.000000Z\n" +
                        "409\t\t2020-07-04T00:00:00.000000Z\n" +
                        "500\t\t2020-07-05T00:00:00.000000Z\n" +
                        "501\t\t2020-07-05T00:00:00.000000Z\n" +
                        "502\t\t2020-07-05T00:00:00.000000Z\n" +
                        "503\t\t2020-07-05T00:00:00.000000Z\n" +
                        "504\t\t2020-07-05T00:00:00.000000Z\n" +
                        "505\t\t2020-07-05T00:00:00.000000Z\n" +
                        "506\t\t2020-07-05T00:00:00.000000Z\n" +
                        "507\t\t2020-07-05T00:00:00.000000Z\n" +
                        "508\t\t2020-07-05T00:00:00.000000Z\n" +
                        "509\t\t2020-07-05T00:00:00.000000Z\n";

                TestUtils.assertEquals(expected, sink);
            }
        }
    }
}
