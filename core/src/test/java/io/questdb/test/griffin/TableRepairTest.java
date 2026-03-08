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

package io.questdb.test.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TableRepairTest extends AbstractCairoTest {

    @Test
    public void testDeleteActivePartition() throws Exception {
        // this test deletes partition files, simulating manual intervention
        assertMemoryLeak(() -> {
            execute("create atomic table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000l) t from long_sequence(100000)) timestamp (t)) timestamp(t) partition by DAY");

            engine.releaseAllWriters();

            try (TableReader reader = newOffPoolReader(configuration, "tst")) {
                Assert.assertEquals(100000, reader.size());

                // last and "active" partition is "1970-01-12"
                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tst");
                    path.of(configuration.getDbRoot()).concat(tableToken).concat("1970-01-12").$();
                    Assert.assertTrue(Files.rmdir(path, true));
                }

                Assert.assertEquals(100000, reader.size());

                // repair by opening and closing writer

                try (TableWriter writer = newOffPoolWriter(configuration, "tst")) {
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(95040, reader.size());
                    Assert.assertEquals(950390000000L, writer.getMaxTimestamp());
                    TableWriter.Row row = writer.newRow(writer.getMaxTimestamp());
                    row.putInt(0, 150);
                    row.putDouble(1, 0.67);
                    row.append();
                    writer.commit();
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(95041, reader.size());
            }
        });
    }

    @Test
    public void testDeletePartitionInTheMiddle() throws Exception {
        // this test deletes partition files, simulating manual intervention
        assertMemoryLeak(() -> {
            execute("create atomic table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000l) t from long_sequence(100000)) timestamp (t)) timestamp(t) partition by DAY");

            engine.releaseAllWriters();

            try (TableReader reader = newOffPoolReader(configuration, "tst")) {

                Assert.assertEquals(100000, reader.size());

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tst");
                    path.of(configuration.getDbRoot()).concat(tableToken).concat("1970-01-09").$();
                    Assert.assertTrue(Files.rmdir(path, true));
                }

                Assert.assertEquals(100000, reader.size());

                // repair by opening and closing writer

                newOffPoolWriter(configuration, "tst").close();

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(91360, reader.size());
            }
        });
    }
}
