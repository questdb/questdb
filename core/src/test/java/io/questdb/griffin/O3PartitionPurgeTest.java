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

package io.questdb.griffin;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class O3PartitionPurgeTest extends AbstractGriffinTest {
    @Test
    public void testManyReadersOpenClosedAscDense() throws Exception {
        testManyReadersOpenClosedDense(0, 1, 5);
    }

    @Test
    public void testManyReadersOpenClosedAscSparse() throws Exception {
        testManyReadersOpenClosedSparse(0, 1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescDense() throws Exception {
        testManyReadersOpenClosedDense(3, -1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescSparse() throws Exception {
        testManyReadersOpenClosedSparse(4, -1, 5);
    }

    @Test
    public void testReaderUsesPartition() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY", sqlExecutionContext);

            // OOO insert
            compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl")) {

                // in order insert
                compiler.compile("insert into tbl select 2, '1970-01-10T11'", sqlExecutionContext);

                // OOO insert
                compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

                // This should not fail
                rdr.openPartition(0);
            }

            if (Os.type == Os.WINDOWS) {
                engine.releaseInactive();
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.length();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Chars.toString(path), Files.exists(path));
                }
            }
        });
    }

    @Test
    public void testReaderUsesPartitionOfNonO3Commit() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY", sqlExecutionContext);

            // OOO insert
            compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

            // in order insert
            compiler.compile("insert into tbl select 2, '1970-01-10T11'", sqlExecutionContext);

            // This should lock partition 1970-01-10.1 from being deleted from disk
            try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl")) {

                // OOO insert
                compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

                // This should not fail
                rdr.openPartition(0);
            }

            if(Os.type == Os.WINDOWS) {
                engine.releaseInactive();
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.length();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Chars.toString(path), Files.exists(path));
                }
            }
        });
    }

    private void runPartitionPurgeJobs() {
        try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tbl", "purge tasks")) {
            PartitionBy.PartitionFloorMethod partitionFloorMethod = PartitionBy.getPartitionFloorMethod(PartitionBy.DAY);
            assert partitionFloorMethod != null;
            long lastPartition = partitionFloorMethod.floor(writer.getMaxTimestamp());
            writer.o3QueuePartitionForPurge(lastPartition, writer.getTxn());
            writer.consumeO3PartitionRemoveTasks();
        }
    }

    private void testManyReadersOpenClosedDense(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY", sqlExecutionContext);

            TableReader[] readers = new TableReader[iterations];
            for (int i = 0; i < iterations; i++) {
                TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl");
                readers[i] = rdr;

                // OOO insert
                compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

                runPartitionPurgeJobs();
            }

            // Unwind readers one by one old to new
            for (int i = start; i >= 0 && i < iterations; i += increment) {
                TableReader reader = readers[i];
                reader.openPartition(0);
                reader.close();

                if (Os.type == Os.WINDOWS) {
                    engine.releaseInactive();
                }
                runPartitionPurgeJobs();
            }

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.length();
                for (int i = 0; i < iterations; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Chars.toString(path), Files.exists(path));
                }

                path.trimTo(len).put(".").put(Integer.toString(iterations)).concat("x.d").$();
                Assert.assertTrue(Files.exists(path));
            }
        });
    }

    private void testManyReadersOpenClosedSparse(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY", sqlExecutionContext);
            TableReader[] readers = new TableReader[2 * iterations];

            for (int i = 0; i < iterations; i++) {
                TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl");
                readers[2 * i] = rdr;

                // in order insert
                compiler.compile("insert into tbl select 2, '1970-01-10T11'", sqlExecutionContext);

                runPartitionPurgeJobs();

                TableReader rdr2 = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl");
                readers[2 * i + 1] = rdr2;
                // OOO insert
                compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

                runPartitionPurgeJobs();
            }

            // Unwind readers one by in set order
            for (int i = start; i >= 0 && i < iterations; i += increment) {
                TableReader reader = readers[2 * i];
                reader.openPartition(0);
                reader.close();

                // when reader is returned to pool it remains in open state
                // holding files such that purge fails with access violation
                if (Os.type == Os.WINDOWS) {
                    engine.releaseInactive();
                }

                runPartitionPurgeJobs();

                reader = readers[2 * i + 1];
                reader.openPartition(0);
                reader.close();
                // when reader is returned to pool it remains in open state
                // holding files such that purge fails with access violation
                if (Os.type == Os.WINDOWS) {
                    engine.releaseInactive();
                }

                runPartitionPurgeJobs();
            }

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getRoot()).concat("tbl").concat("1970-01-10");
                int len = path.length();
                for (int i = 0; i < 2 * iterations; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Chars.toString(path), Files.exists(path));
                }

                path.trimTo(len).put(".").put(Integer.toString(2 * iterations)).concat("x.d").$();
                Assert.assertTrue(Files.exists(path));
            }
        });
    }
}
