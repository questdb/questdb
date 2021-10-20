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

package io.questdb.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionDeleteTest extends AbstractGriffinTest {
    @Test
    public void testBCSequence() throws SqlException, NumericException {
        compiler.compile("create table events (sequence long, event binary, timestamp timestamp) timestamp(timestamp) partition by DAY", sqlExecutionContext);
        engine.releaseAllWriters();

        try (TableWriter w = new TableWriter(configuration, "events")) {
            long ts = TimestampFormatUtils.parseTimestamp("2020-06-30T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow(ts);
                r.putLong(0, i);
                r.append();
            }
            ts = TimestampFormatUtils.parseTimestamp("2020-07-01T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow(ts);
                r.putLong(0, 100 + i);
                r.append();
            }

            ts = TimestampFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z");
            for (int i = 0; i < 10; i++) {
                TableWriter.Row r = w.newRow(ts);
                r.putLong(0, 200 + i);
                r.append();
            }
            w.commit();
        }

        try (TableReader r = new TableReader(configuration, "events")) {
            RecordCursor cursor = r.getCursor();
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {

            }

            try (TableWriter w = new TableWriter(configuration, "events")) {
                long ts = TimestampFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, 250 + i);
                    row.append();
                }
                w.commit();

                Assert.assertTrue(w.removePartition(TimestampFormatUtils.parseTimestamp("2020-06-30T00:00:00.000000Z")));

                r.reload();

                cursor.toTop();
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {

                }

                ts = TimestampFormatUtils.parseTimestamp("2020-07-03T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, 300 + i);
                    row.append();
                }
                w.commit();

                ts = TimestampFormatUtils.parseTimestamp("2020-07-04T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, 400 + i);
                    row.append();
                }
                w.commit();

                ts = TimestampFormatUtils.parseTimestamp("2020-07-05T00:00:00.000000Z");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = w.newRow(ts);
                    row.putLong(0, 500 + i);
                    row.append();
                }
                w.commit();

                Assert.assertTrue(w.removePartition(TimestampFormatUtils.parseTimestamp("2020-07-01T00:00:00.000000Z")));
                Assert.assertTrue(w.removePartition(TimestampFormatUtils.parseTimestamp("2020-07-02T00:00:00.000000Z")));
                Assert.assertTrue(w.removePartition(TimestampFormatUtils.parseTimestamp("2020-07-03T00:00:00.000000Z")));

                Assert.assertTrue(r.reload());

                sink.clear();
                cursor.toTop();
                printer.print(cursor, r.getMetadata(), true, sink);

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
