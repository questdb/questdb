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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TouchTableFunctionTest extends AbstractCairoTest {

    private static final String DDL = "create table x as " +
            "(" +
            "  select" +
            "    rnd_geohash(40) g," +
            "    rnd_double(0)*100 a," +
            "    rnd_symbol(5,4,4,1) b," +
            "    timestamp_sequence(0, 100000000000) k" +
            " from long_sequence(20)" +
            "), index(b) timestamp(k) partition by DAY";

    @Test
    public void testNoSegfaultWhenAddingNewColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t1 AS (
                      SELECT generate_series as t,\s
                             (generate_series % 123456)::int as i\s
                      FROM generate_series('2025-12-25', '2025-12-26', '1s')
                    ) PARTITION BY DAY WAL;
                    """);

            drainWalQueue();

            String query = "select touch(select * from t1);";

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");

            execute("alter table t1 add column f float;");
            drainWalQueue();

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");

            execute("update t1 set f = 5.2f");
            drainWalQueue();

            sink.clear();
            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            TestUtils.assertContains(sink, "data_pages");
        });
    }

    @Test
    public void testTouchTableNoTimestampColumnSelected() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select g,a,b from x where k in '1970-01-22')";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "query does not support framing execution and cannot be pre-touched");
            }
        });
    }

    @Test
    public void testTouchTableThrowOnComplexFilter() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k in '1970-01-22' and a > 100.0)";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "query does not support framing execution and cannot be pre-touched");
            }
        });
    }

    @Test
    public void testTouchTableTimeInterval() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k in '1970-01-22')";
            try {
                execute(DDL, sqlExecutionContext);
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

    @Test
    public void testTouchTableTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select touch(select * from x where k > '1970-01-18T00:00:00.000000Z')";
            execute(DDL, sqlExecutionContext);
            try {
                TestUtils.printSql(engine, sqlExecutionContext, query, sink);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

    @Test
    public void testTouchUpdateTouchAgain() throws Exception {
        assertMemoryLeak(() -> {

            final String query = "select touch(select * from x)";

            final String ddl2 = "insert into x select * from (" +
                    " select" +
                    " rnd_geohash(40)," +
                    " rnd_double(0)*100," +
                    " 'VTJW'," +
                    " to_timestamp('2019', 'yyyy') t" +
                    " from long_sequence(100)" +
                    ") timestamp (t)";

            try {
                execute(DDL);
                printSql(query);
                execute(ddl2);
                printSql(query);
            } catch (SqlException ex) {
                Assert.fail(ex.getMessage());
            }
        });
    }

}
