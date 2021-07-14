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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ToUTCTimestampFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testAreaName() throws Exception {
        assertToUTC("select to_utc(0, 'Europe/Prague')", "1969-12-31T23:00:00.000000Z\n");
    }

    @Test
    public void testInvalidConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("select to_utc(0, 'UUU')", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone name");
            }
        });
    }

    @Test
    public void testNullConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("select to_utc(0, null)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testInvalidConstantOffset() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("select to_utc(0, '25:40')", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone name");
            }
        });
    }

    @Test
    public void testZoneName() throws Exception {
        assertToUTC(
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), 'PST')",
                "2020-03-12T22:30:00.000000Z\n"
        );
    }

    @Test
    public void testTimeOffset() throws Exception {
        assertToUTC(
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), '-07:40')",
                        "2020-03-12T23:10:00.000000Z\n"
        );
    }

    @Test
    public void testVarTimezone() throws Exception {
        assertToUTC(
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), zone) from (select '-07:40' zone)",
                "2020-03-12T23:10:00.000000Z\n"
        );
    }

    @Test
    public void testVarInvalidTimezone() throws Exception {
        assertToUTC(
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), zone) from (select 'XU' zone)",
                "2020-03-12T15:30:00.000000Z\n"
        );
    }

    @Test
    public void testVarNullTimezone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile("select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), zone) from (select null zone)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(64, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    private void assertToUTC(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                "to_utc\n" +
                        expected
        ));
    }

}