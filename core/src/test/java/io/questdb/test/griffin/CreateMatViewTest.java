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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

public class CreateMatViewTest extends AbstractCairoTest {
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";

    @Test
    public void testCreateMatViewMultipleTablesTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createTable(TABLE3);

            try {
                ddl("create materialized view test as select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }

            try {
                ddl("create materialized view test as (select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s)");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }

            try {
                ddl("create materialized view test as (select ts, avg(v) from " + TABLE3 + " sample by 30s " +
                        "union select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s)");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "More than one table used in query, base table has to be set using 'WITH BASE'");
            }
        });
    }

    @Ignore
    @Test
    public void testCreateMatViewNoSampleByTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            try {
                ddl("create materialized view test as select * from " + TABLE1 + " where v % 2 = 0");
                fail("Expected SqlException missing");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Materialized view query has no aggregation interval, should contain SAMPLE BY");
            }
        });
    }

    @Test
    public void testCreateMatViewTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            ddl("create materialized view test as select ts, avg(v) from " + TABLE1 + " sample by 30s");

            final String expected = "ts\tavg\n" +
                    "1970-01-01T00:00:00.000000Z\t1.0\n" +
                    "1970-01-01T00:00:30.000000Z\t4.0\n" +
                    "1970-01-01T00:01:00.000000Z\t7.0\n";

            assertQuery(
                    expected,
                    "test",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateMatViewWithBaseTest() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            ddl("create materialized view test with base " + TABLE1
                    + " as select t1.ts, avg(t1.v) from " + TABLE1 + " as t1 join " + TABLE2 + " as t2 on v sample by 30s");

            final String expected = "ts\tavg\n" +
                    "1970-01-01T00:00:00.000000Z\t1.0\n" +
                    "1970-01-01T00:00:30.000000Z\t4.0\n" +
                    "1970-01-01T00:01:00.000000Z\t7.0\n";

            assertQuery(
                    expected,
                    "test",
                    "ts",
                    true,
                    true
            );
        });
    }

    private void createTable(String tableName) throws SqlException {
        ddl("create table " + tableName + " (ts timestamp, v long) timestamp(ts) partition by day");
        for (int i = 0; i < 9; i++) {
            insert("insert into " + tableName + " values (" + (i * 10000000) + ", " + i + ")");
        }
    }
}
