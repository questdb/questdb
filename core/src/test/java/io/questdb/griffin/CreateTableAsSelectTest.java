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

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CreateTableAsSelectTest extends AbstractGriffinTest {

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrder() throws Exception {
        testCreatePartitionedTableAsSelectWithOrderBy("");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrder() throws Exception {
        testCreatePartitionedTableAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrder() throws Exception {
        testCreatePartitionedTableAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertFailure(
                    "create table dest as (select * from src where v % 2 = 0 order by ts desc) timestamp(ts);",
                    "Could not create table. See log for details.",
                    13
            );
        });
    }

    private void testCreatePartitionedTableAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            compiler.compile("create table dest as (select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;", sqlExecutionContext);

            String expected = "ts\tv\n" +
                    "1970-01-01T00:00:00.000000Z\t0\n" +
                    "1970-01-01T00:00:00.020000Z\t2\n" +
                    "1970-01-01T00:00:00.040000Z\t4\n";

            assertQuery(
                    expected,
                    "dest",
                    "ts",
                    true,
                    true
            );
        });
    }

    private void createSrcTable() throws SqlException {
        compiler.compile("create table src (ts timestamp, v long) timestamp(ts) partition by day;", sqlExecutionContext);
        executeInsert("insert into src values (0, 0);");
        executeInsert("insert into src values (10000, 1);");
        executeInsert("insert into src values (20000, 2);");
        executeInsert("insert into src values (30000, 3);");
        executeInsert("insert into src values (40000, 4);");
    }

    private void assertFailure(String sql, String message, int position) {
        try {
            compiler.compile(sql, sqlExecutionContext);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            Assert.assertEquals(position, e.getPosition());
        }
    }
}
