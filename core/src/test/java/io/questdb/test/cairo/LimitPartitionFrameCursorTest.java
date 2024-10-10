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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LimitPartitionFrameCursorTest extends AbstractCairoTest {
    @Test
    public void testLimit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.INT).
                    col("b", ColumnType.INT).
                    timestamp();
            AbstractCairoTest.create(model);

            long timestamp;
            final Rnd rnd = new Rnd();
            // format arguments: (day, hour)
            String fmtBase = "1970-01-%02dT%02d:00:00.000Z";
            try (TableWriter writer = newOffPoolWriter(configuration, "x", metrics)) {
                // Partitions are done by day; provide 5 entries per partition, for ten days
                for (int day = 1; day < 11; ++day) {
                    for (int hour = 0; hour < 10; ++hour) {
                        String curTs = String.format(fmtBase, day, hour);
                        timestamp = TimestampFormatUtils.parseTimestamp(curTs);
                        TableWriter.Row row = writer.newRow(timestamp);

                        // todo: are these random rows required for something?
                        row.putInt(0, rnd.nextInt());
                        row.putInt(1, rnd.nextInt());
                        row.append();
                    }
                }
                writer.commit();

                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor()
                ) {
                    cursor.setLimit(-1);
                    cursor.of(reader);

                    PartitionFrame frame = cursor.next();
                    // we should only have one frame here..
                    Assert.assertEquals(frame.getRowLo(), 9);
                    Assert.assertEquals(frame.getRowHi(), 10);
                    Assert.assertEquals(frame.getPartitionIndex(), reader.getPartitionCount() - 1);
                    Assert.assertNull(cursor.next()); // only one result, simple..

                    Assert.assertEquals(1, cursor.size());
                }

                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor()
                ) {
                    // consume entire last partition
                    cursor.setLimit(-10);
                    cursor.of(reader);
                    PartitionFrame frame = cursor.next();
                    // we should only have one frame here..
                    Assert.assertEquals(frame.getRowLo(), 0);
                    Assert.assertEquals(frame.getRowHi(), 10);
                    Assert.assertEquals(frame.getPartitionIndex(), reader.getPartitionCount() - 1);
                    Assert.assertNull(cursor.next()); // consume entire partition, ok so far

                    Assert.assertEquals(10, cursor.size());
                }

                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor()
                ) {
                    // consume entire last partition, and partially consume the next one
                    cursor.setLimit(-15);
                    cursor.of(reader);
                    PartitionFrame frame = cursor.next();

                    // we should only have one frame here..
                    Assert.assertEquals(frame.getRowLo(), 5);
                    Assert.assertEquals(frame.getRowHi(), 10);
                    Assert.assertEquals(frame.getPartitionIndex(), reader.getPartitionCount() - 2);

                    frame = cursor.next();
                    Assert.assertNotNull(frame);
                    Assert.assertEquals(frame.getRowLo(), 0);
                    Assert.assertEquals(frame.getRowHi(), 10);

                    frame = cursor.next();
                    Assert.assertNull(frame);
                    Assert.assertEquals(15, cursor.size());
                }

                // try with limit = num rows
                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor()
                ) {
                    Assert.assertEquals(100, reader.size());
                    cursor.setLimit(-100);
                    cursor.of(reader);
                    long count = 0;
                    for (PartitionFrame frame = cursor.next(); frame != null; frame = cursor.next()) {
                        count += frame.getRowHi() - frame.getRowLo();
                    }

                    Assert.assertEquals(100, count);
                    Assert.assertEquals(100, cursor.size());
                }

                // try with limit > num rows
                try (
                        TableReader reader = newOffPoolReader(configuration, "x");
                        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor()
                ) {
                    Assert.assertEquals(100, reader.size());
                    cursor.setLimit(-200);
                    cursor.of(reader);
                    long count = 0;
                    for (PartitionFrame frame = cursor.next(); frame != null; frame = cursor.next()) {
                        count += frame.getRowHi() - frame.getRowLo();
                    }
                    Assert.assertEquals(100, count);
                    Assert.assertEquals(100, cursor.size());
                }
            }
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLimitNotNegativeUnsupported() {
        LimitPartitionFrameCursor cursor = new LimitPartitionFrameCursor();
        cursor.setLimit(10);
    }
}
