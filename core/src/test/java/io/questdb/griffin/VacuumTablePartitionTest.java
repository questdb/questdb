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

import io.questdb.cairo.O3PurgeDiscoveryJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VacuumTablePartitionTest extends AbstractGriffinTest {

    @Test
    public void testVacuumExceedsQueueSize() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table \"таблица\"  (x long, ts timestamp) timestamp(ts) partition by month", sqlExecutionContext);
            try {
                int n = engine.getConfiguration().getO3PurgeDiscoveryQueueCapacity() * 2;
                for (int i = 0; i < n; i++) {
                    compiler.compile("VACUUM partitions \"таблица\";", sqlExecutionContext);
                }
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot schedule vacuum action, queue is full, please retry " +
                        "or increase Purge Discovery Queue Capacity");
                Assert.assertEquals("VACUUM partitions ".length(), ex.getPosition());
            }

            // cleanup
            try (O3PurgeDiscoveryJob purgeDiscoveryJob = new O3PurgeDiscoveryJob(engine.getMessageBus(), 1)) {
                //noinspection StatementWithEmptyBody
                while (purgeDiscoveryJob.run(0)) {

                }
            }
        });
    }

    @Test
    public void testVacuumSyntaxError1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("vacuum asdf", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "'partitions' expected");
                Assert.assertEquals("vacuum ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxError2() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("vacuum partitions asdfad", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table 'asdfad' does not exist");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxError4() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("vacuum partitions ", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table name expected");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxErrorNoEOL() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl (x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            try {
                compiler.compile("vacuum partitions tbl asdf", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "end of line or ';' expected");
                Assert.assertEquals("vacuum partitions tbl ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxErrorNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl (x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            try {
                compiler.compile("vacuum partitions tbl", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table 'tbl' is not partitioned");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxErrorTableSpecialChars() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("VACUUM partitions ..\\root", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "'.' is an invalid table name");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxQuotedTableOk() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tbl (x long, ts timestamp) timestamp(ts) partition by month", sqlExecutionContext);
            compiler.compile("VACUUM partitions 'tbl'", sqlExecutionContext);
            compiler.compile("VACUUM PARTITIONS tbl;", sqlExecutionContext);

            compiler.compile("create table \"tbl with space\" (x long, ts timestamp) timestamp(ts) partition by month", sqlExecutionContext);
            compiler.compile("VACUUM PARTITIONS \"tbl with space\";", sqlExecutionContext);
        });
    }
}
