/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateTruncFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testInvalidKind() {
        try {
            compiler.compile(
                    "select DATE_TRUNC('invalid', TIMESTAMP '2000-12-17T02:09:30.111111Z') as truncated",
                    sqlExecutionContext
            );
            Assert.fail();
        } catch (SqlException e) {
            assertEquals(18, e.getPosition());
            TestUtils.assertContains("invalid unit 'invalid'", e.getFlyweightMessage());
        }
    }

    @Test
    public void testNullKind() {
        try {
            compiler.compile(
                    "select DATE_TRUNC(null,    TIMESTAMP '2000-12-17T02:09:30.111111Z') as truncated",
                    sqlExecutionContext
            );
            Assert.fail();
        } catch (SqlException e) {
            assertEquals(18, e.getPosition());
            TestUtils.assertContains("invalid unit 'null'", e.getFlyweightMessage());
        }
    }

    @Test
    public void testNullUpstream() throws Exception {
        assertTimestamp("SELECT DATE_TRUNC('month', null) as truncated", "");
    }

    @Test
    public void testSimple() throws Exception {
        // this could be moved to a single query as it's in the TimestampCeilFloorFunctionFactoryTest
        // the test would likely run faster. the price would be decreased clarity in the case of a failure
        assertTimestamp("SELECT DATE_TRUNC('microseconds',  TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T02:09:30.111111Z");
        assertTimestamp("SELECT DATE_TRUNC('microseconds',  TIMESTAMP '2017-03-17T02:09:30.000000Z') as truncated", "2017-03-17T02:09:30.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('milliseconds',  TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T02:09:30.111000Z");
        assertTimestamp("SELECT DATE_TRUNC('second',        TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T02:09:30.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('minute',        TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T02:09:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('hour',          TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T02:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('day',           TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-17T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('week',          TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-13T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('week',          TIMESTAMP '2017-03-13T02:09:30.111111Z') as truncated", "2017-03-13T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('week',          TIMESTAMP '2020-01-01T02:09:30.111111Z') as truncated", "2019-12-30T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('month',         TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-03-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('quarter',       TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('quarter',       TIMESTAMP '2017-04-17T02:09:30.111111Z') as truncated", "2017-04-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('quarter',       TIMESTAMP '2017-07-17T02:09:30.111111Z') as truncated", "2017-07-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('quarter',       TIMESTAMP '2017-10-17T02:09:30.111111Z') as truncated", "2017-10-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('quarter',       TIMESTAMP '2017-12-31T02:09:30.111111Z') as truncated", "2017-10-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('year',          TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2017-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('decade',        TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2010-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('decade',        TIMESTAMP '2000-03-17T02:09:30.111111Z') as truncated", "2000-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('century',       TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2001-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('century',       TIMESTAMP '2000-03-17T02:09:30.111111Z') as truncated", "1901-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('millennium',    TIMESTAMP '2017-03-17T02:09:30.111111Z') as truncated", "2001-01-01T00:00:00.000000Z");
        assertTimestamp("SELECT DATE_TRUNC('millennium',    TIMESTAMP '2000-12-17T02:09:30.111111Z') as truncated", "1001-01-01T00:00:00.000000Z");
    }

    @Test
    public void testWithFunctionUpstream() throws Exception {
        assertTimestamp("SELECT DATE_TRUNC('millennium', concat('2001','-01-01T00:00:00.000000Z')) as truncated", "2001-01-01T00:00:00.000000Z");
    }

    private void assertTimestamp(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                "truncated\n" +
                        expected + "\n"
        ));
    }

}
