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

import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VacuumTablePartitionTest extends AbstractCairoTest {

    @Test
    public void testVacuumExceedsQueueSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"таблица\"  (x long, ts timestamp) timestamp(ts) partition by month");
            try {
                int n = engine.getConfiguration().getO3PurgeDiscoveryQueueCapacity() * 2;
                for (int i = 0; i < n; i++) {
                    execute("VACUUM partitions \"таблица\";");
                }
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "cannot schedule vacuum action, queue is full, please retry " +
                        "or increase Purge Discovery Queue Capacity");
                Assert.assertEquals("VACUUM partitions ".length(), ex.getPosition());
            }

            // cleanup
            try (O3PartitionPurgeJob purgeDiscoveryJob = new O3PartitionPurgeJob(engine, 1)) {
                purgeDiscoveryJob.drain(0);
            }
        });
    }

    @Test
    public void testVacuumSyntaxError1() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("vacuum asdf");
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
                assertExceptionNoLeakCheck("vacuum partitions asdfad");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table does not exist [table=asdfad]");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxError4() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("vacuum partitions ");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table name expected");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxErrorNoEOL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl (x long, ts timestamp) timestamp(ts)");
            try {
                assertExceptionNoLeakCheck("vacuum partitions tbl asdf");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "end of line or ';' expected");
                Assert.assertEquals("vacuum partitions tbl ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxErrorNonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl (x long, ts timestamp) timestamp(ts)");
            try {
                assertExceptionNoLeakCheck("vacuum partitions tbl");
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
                assertExceptionNoLeakCheck("VACUUM partitions ..\\root");
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "'.' is an invalid table name");
                Assert.assertEquals("vacuum partitions ".length(), ex.getPosition());
            }
        });
    }

    @Test
    public void testVacuumSyntaxQuotedTableOk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl (x long, ts timestamp) timestamp(ts) partition by month");
            execute("VACUUM partitions 'tbl'");
            execute("VACUUM PARTITIONS tbl;");

            execute("create table \"tbl with space\" (x long, ts timestamp) timestamp(ts) partition by month");
            execute("VACUUM PARTITIONS \"tbl with space\";");
        });
    }
}
