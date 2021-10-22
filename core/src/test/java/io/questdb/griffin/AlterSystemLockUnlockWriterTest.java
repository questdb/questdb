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

public class AlterSystemLockUnlockWriterTest extends AbstractGriffinTest {

    //alter system [lock|unlock] writer <tableName>

    @Test
    public void testAlterExpectTableName1() throws Exception {
        assertFailure("alter system lock writer", 24, "table name expected");
    }

    @Test
    public void testAlterExpectTableName2() throws Exception {
        assertFailure("alter system unlock writer", 26, "table name expected");
    }

    @Test
    public void testBadSyntax0() throws Exception {
        assertFailure("alter systm", 6, "'table' or 'system' expected");
    }

    @Test
    public void testBadSyntax1() throws Exception {
        assertFailure("alter system", 12, "'lock' or 'unlock' expected");
    }

    @Test
    public void testBadSyntax2() throws Exception {
        assertFailure("alter system lck", 13, "'lock' or 'unlock' expected");
    }

    @Test
    public void testBadSyntax3() throws Exception {
        assertFailure("alter system lock", 17, "'writer' expected");
    }

    @Test
    public void testBadSyntax4() throws Exception {
        assertFailure("alter system unlock", 19, "'writer' expected");
    }

    @Test
    public void testNonExistentTable() throws Exception {
        assertFailure("alter system unlock writer z", 27, "table 'z' does not exist");
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                compiler.compile(sql, sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(position, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), message);
            }
        });
    }

    private void createX() throws SqlException {
        compiler.compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
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
                        " from long_sequence(10)" +
                        ") timestamp (timestamp);",
                sqlExecutionContext
        );
    }
}
