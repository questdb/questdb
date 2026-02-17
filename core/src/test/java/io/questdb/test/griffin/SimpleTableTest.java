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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SimpleTableTest extends AbstractCairoTest {
    @Test
    public void testTimeStampWithTimezone() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (timestamp timestamp) timestamp(timestamp);");
            execute("insert into t values (1);");

            String expected1 = "time\n" +
                    "1970-01-01T00:00:00.000001Z\n";

            assertSql(expected1, "select timestamp time from t;");

            try {
                assertExceptionNoLeakCheck("select timestamp with time zone from t;");
            } catch (SqlException e) {
                Assert.assertEquals(31, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "String literal expected after 'timestamp with time zone'");
            }

            String expected2 = "timestamp\n" +
                    "1970-01-01T00:00:00.000001Z\n";

            assertSql(expected2, "select timestamp from t;");

            String expected3 = "time\ttimestamp\n" +
                    "2020-12-31T15:15:51.663000Z\t1970-01-01T00:00:00.000001Z\n";

            assertSql(expected3, "select timestamp with time zone '2020-12-31 15:15:51.663+00:00' time, timestamp from t;");

            assertSql(expected3, "select cast('2020-12-31 15:15:51.663+00:00' as timestamp with time zone) time, timestamp from t;");
        });
    }

    @Test
    public void testWhereIsColumnNameInsensitive() throws SqlException, NumericException {
        TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE);
        tm.timestamp("ts").col("ID", ColumnType.INT);
        createPopulateTable(tm, 2, "2020-01-01", 1);

        assertSql("ts\n" +
                "2020-01-01T00:00:00.000000Z\n", "select ts from tab1 where id > 1");
    }
}
