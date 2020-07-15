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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.griffin.CompiledQuery.ALTER;

public class AlterTableDropPartitionTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testDropMalformedPartition() throws Exception {
        assertMemoryLeak(() -> {
            createX("DAY", 72000000);

            try {
                compiler.compile("alter table x drop partition '2017-01'", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(29, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "'YYYY-MM-DD' expected");
            }
                }
        );
    }

    @Test
    public void testDropNonExistentPartition() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 72000000);

                    try {
                        compiler.compile("alter table x drop partition '2017-01-05'", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertEquals(29, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not remove partition");
                    }
                }
        );
    }

    @Test
    public void testDropPartitionExpectName() throws Exception {
        assertFailure("alter table x drop partition", 28, "partition name expected");
    }

    @Test
    public void testDropPartitionNameMissing() throws Exception {
        assertFailure("alter table x drop partition ,", 29, "partition name missing");
    }

    @Test
    public void testDropTwoPartitionsByDay() throws Exception {
        assertMemoryLeak(() -> {
            createX("DAY", 720000000);

            String expectedBeforeDrop = "count\n" +
                    "120\n";

            assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

            Assert.assertEquals(ALTER, compiler.compile("alter table x drop partition '2018-01-05', '2018-01-07'", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByDayUpperCase() throws Exception {
        assertMemoryLeak(() -> {
                    createX("DAY", 720000000);

                    String expectedBeforeDrop = "count\n" +
                            "120\n";

                    assertPartitionResult(expectedBeforeDrop, "2018-01-07");
                    assertPartitionResult(expectedBeforeDrop, "2018-01-05");

                    Assert.assertEquals(ALTER, compiler.compile("alter table x DROP partition '2018-01-05', '2018-01-07'", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-01-05");
                    assertPartitionResult(expectedAfterDrop, "2018-01-07");
                }
        );
    }


    @Test
    public void testDropTwoPartitionsByMonth() throws Exception {
        assertMemoryLeak(() -> {
                    createX("MONTH", 3 * 7200000000L);

                    assertPartitionResult("count\n" +
                                    "112\n",
                            "2018-02");

                    assertPartitionResult("count\n" +
                            "120\n", "2018-04");

            Assert.assertEquals(ALTER, compiler.compile("alter table x drop partition '2018-02', '2018-04'", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2018-02");
                    assertPartitionResult(expectedAfterDrop, "2018-04");
                }
        );
    }

    @Test
    public void testDropTwoPartitionsByYear() throws Exception {
        assertMemoryLeak(() -> {
                    createX("YEAR", 3 * 72000000000L);

                    assertPartitionResult("count\n" +
                                    "147\n",
                            "2020");

                    assertPartitionResult("count\n" +
                            "146\n", "2022");

            Assert.assertEquals(ALTER, compiler.compile("alter table x drop partition '2020', '2022'", sqlExecutionContext).getType());

                    String expectedAfterDrop = "count\n" +
                            "0\n";

                    assertPartitionResult(expectedAfterDrop, "2020");
                    assertPartitionResult(expectedAfterDrop, "2022");
                }
        );
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX("YEAR", 720000000);
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void assertPartitionResult(String expectedBeforeDrop, String intervalSearch) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile("select count() from x where timestamp = '" + intervalSearch + "'", sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                sink.clear();
                printer.print(cursor, factory.getMetadata(), true);
                TestUtils.assertEquals(expectedBeforeDrop, sink);
            }
        }
    }

    private void createX(String partitionBy, long increment) throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * " + increment + " timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(1000)" +
                        ") timestamp (timestamp)" +
                        "partition by " + partitionBy,
                sqlExecutionContext
        );
    }
}
