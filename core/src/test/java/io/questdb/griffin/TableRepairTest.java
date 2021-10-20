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
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class TableRepairTest extends AbstractGriffinTest {

    @Test
    public void testDeleteActivePartition() throws Exception {
        // this delete partition actually deletes files, simulating manual intervention
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000l) t from long_sequence(100000)) timestamp (t)) timestamp(t) partition by DAY",
                    sqlExecutionContext
            );
            engine.releaseAllWriters();
            try (TableReader reader = new TableReader(configuration, "tst")) {

                Assert.assertEquals(100000, reader.size());

                // last and "active" partition is "1970-01-12"
                try (Path path = new Path()) {
                    path.of(configuration.getRoot()).concat("tst").concat("1970-01-12").$();
                    Assert.assertEquals(0, Files.rmdir(path));
                }

                Assert.assertEquals(100000, reader.size());

                // repair by opening and closing writer

                try (TableWriter w = new TableWriter(configuration, "tst")) {

                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(95040, reader.size());

                    Assert.assertEquals(950390000000L, w.getMaxTimestamp());

                    TableWriter.Row row = w.newRow(w.getMaxTimestamp());
                    row.putInt(0, 150);
                    row.putDouble(1, 0.67);
                    row.append();

                    w.commit();
                }

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(95041, reader.size());


            }
        });
    }

    @Test
    public void testDeletePartitionInTheMiddle() throws Exception {
        // this delete partition actually deletes files, simulating manual intervention
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000l) t from long_sequence(100000)) timestamp (t)) timestamp(t) partition by DAY",
                    sqlExecutionContext
            );
            engine.releaseAllWriters();
            try (TableReader reader = new TableReader(configuration, "tst")) {

                Assert.assertEquals(100000, reader.size());

                try (Path path = new Path()) {
                    path.of(configuration.getRoot()).concat("tst").concat("1970-01-09").$();
                    Assert.assertEquals(0, Files.rmdir(path));
                }

                Assert.assertEquals(100000, reader.size());

                // repair by opening and closing writer

                new TableWriter(configuration, "tst").close();

                Assert.assertTrue(reader.reload());
                Assert.assertEquals(91360, reader.size());
            }
        });
    }

}
