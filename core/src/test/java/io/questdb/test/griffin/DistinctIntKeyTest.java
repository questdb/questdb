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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DistinctIntKeyTest extends AbstractCairoTest {

    @Test
    public void testDistinctFailAllocRosti() throws Exception {
        // fail Rosti instance #3
        int workerCount = 4;
        int failInstance = 3;
        configOverrideRostiAllocFacade(
                new RostiAllocFacadeImpl() {
                    int count = 0;

                    @Override
                    public long alloc(ColumnTypes types, long capacity) {
                        if (++count == failInstance) {
                            return 0;
                        }
                        return super.alloc(types, capacity);
                    }
                }
        );

        // override worker count to allocate multiple Rosti instances
        final SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount);

        assertMemoryLeak(() -> {
            execute(
                    "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::int i from long_sequence(10000))" +
                            " timestamp(ts) PARTITION BY MONTH",
                    sqlExecutionContext
            );

            try {
                assertExceptionNoLeakCheck("select DISTINCT i from tab order by 1 LIMIT 3", sqlExecutionContext);
            } catch (CairoException e) {
                Assert.assertTrue(e.isOutOfMemory());
            }
        });
    }

    @Test
    public void testDistinctInt() throws Exception {
        assertQuery(
                "i\n" +
                        "0\n" +
                        "2\n" +
                        "4\n" +
                        "6\n",
                "select DISTINCT i from tab WHERE ts in '2020-03' order by 1 LIMIT 4",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, (2 * (x % 10))::int i from long_sequence(10000))" +
                        " timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctIntFilteredFramed() throws Exception {
        assertQuery(
                "i\n" +
                        "12\n" +
                        "14\n" +
                        "16\n" +
                        "18\n",
                "select DISTINCT i from tab WHERE ts in '2020-03' order by 1 LIMIT -4",
                "create table tab as (" +
                        "select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, (2 * (x % 10))::int i from long_sequence(10000)" +
                        ") timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctIntWithAnotherCol() throws Exception {
        assertQuery(
                "i\tmonth\n" +
                        "1\t1\n" +
                        "2\t1\n" +
                        "3\t1\n" +
                        "4\t1\n" +
                        "5\t1\n" +
                        "6\t1\n" +
                        "7\t1\n" +
                        "8\t1\n" +
                        "9\t1\n" +
                        "10\t1\n",
                "select DISTINCT i, month(ts) from tab order by 1 LIMIT 10",
                "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::int i from long_sequence(10000))" +
                        " timestamp(ts) PARTITION BY MONTH",
                null,
                true,
                true
        );
    }

    @Test
    public void testDistinctOnBrokenTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tab as (select timestamp_sequence('2020-01-01', 10 * 60 * 1000000L) ts, x::int i from long_sequence(10000))" +
                            " timestamp(ts) PARTITION BY MONTH"
            );

            // remove partition
            final String partition = "2020-02";

            TableToken tableToken = engine.verifyTableName("tab");
            try (Path path = new Path().of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(partition)) {
                Assert.assertTrue(Files.rmdir(path, true));
            }

            try {
                assertExceptionNoLeakCheck("select DISTINCT i from tab order by 1 LIMIT 3");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Partition '2020-02' does not exist in table 'tab' directory");
            }
        });
    }
}
