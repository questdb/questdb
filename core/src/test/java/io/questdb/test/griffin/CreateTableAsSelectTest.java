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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CreateTableAsSelectTest extends AbstractCairoTest {

    @Test
    public void testCreateAsSelectAndLikeIsInvalid() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertException(
                    "create table dest as (select * from src) like src",
                    41,
                    "unexpected token [like]"
            );
        });
    }

    @Test
    public void testCreateNonPartitionedTableAsSelectTimestampDescOrder() throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            assertException(
                    "create table dest as (select * from src where v % 2 = 0 order by ts desc) timestamp(ts);",
                    13,
                    "cannot insert rows out of order to non-partitioned table."
            );
        });
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampAscOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts asc", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampDescOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("order by ts desc", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrder() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrderBatched() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("", 54, "");
    }

    @Test
    public void testCreatePartitionedTableAsSelectTimestampNoOrderBatchedAndLagged() throws Exception {
        createPartitionedTableAsSelectWithOrderBy("", 26, "1000ms");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampAscOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("order by ts asc");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampDescOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("order by ts desc");
    }

    @Test
    public void testCreatePartitionedTableAtomicAsSelectTimestampNoOrder() throws Exception {
        createPartitionedTableAtomicAsSelectWithOrderBy("");
    }

    private void createPartitionedTableAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            execute("create table dest as (select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;");

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

    private void createPartitionedTableAsSelectWithOrderBy(String orderByClause, int batchSize, String o3MaxLag) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            String sql = "create ";

            if (batchSize != -1) {
                sql += "batch " + batchSize;
            }

            if (!o3MaxLag.isEmpty()) {
                sql += " o3MaxLag " + o3MaxLag;
            }

            sql += " table dest as ";

            sql += "(select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;";
            execute(sql);

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

    private void createPartitionedTableAtomicAsSelectWithOrderBy(String orderByClause) throws Exception {
        assertMemoryLeak(() -> {
            createSrcTable();

            String sql = "create atomic table dest as ";


            sql += "(select * from src where v % 2 = 0 " + orderByClause + ") timestamp(ts) partition by day;";
            execute(sql);

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
        execute("create table src (ts timestamp, v long) timestamp(ts) partition by day;");
        execute("insert into src values (0, 0);");
        execute("insert into src values (10000, 1);");
        execute("insert into src values (20000, 2);");
        execute("insert into src values (30000, 3);");
        execute("insert into src values (40000, 4);");
    }
}
