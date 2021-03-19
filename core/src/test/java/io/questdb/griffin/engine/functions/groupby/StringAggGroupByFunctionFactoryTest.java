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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StringAggGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testGroupKeyedUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                            "select * from (" +
                            "   select " +
                            "       rnd_symbol('a','b','c','d','e','f') a," +
                            "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                            "       timestamp_sequence(0, 100000) ts " +
                            "   from long_sequence(5)" +
                            ") timestamp(ts))",
                    sqlExecutionContext
            );
            try {
                compiler.compile("select a, string_agg(s, ',') from x", sqlExecutionContext);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "value type is not supported: STRING");
            }
        });
    }

    @Test
    public void testGroupNotKeyed() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "abc,bbb,aaa,ccc,aaa\n",
                "select string_agg(s, ',') from x",
                "create table x as (select * from (select rnd_str('abc', 'aaa', 'bbb', 'ccc') s, timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                false,
                true
        );
    }

    @Test
    public void testConstantString() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "aaa,aaa,aaa,aaa,aaa\n",
                "select string_agg('aaa', ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                false,
                true
        );
    }

    @Test
    public void testConstantNull() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "\n",
                "select string_agg(null, ',') from x",
                "create table x as (select * from (select timestamp_sequence(0, 100000) ts from long_sequence(5)) timestamp(ts))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testSkipNull() throws Exception {
        assertQuery(
                "string_agg\n" +
                        "\n",
                "select string_agg(s, ',') from x",
                "create table x as (select * from (select cast(null as string) s from long_sequence(5)))",
                null,
                "insert into x select 'abc' from long_sequence(1)",
                "string_agg\n" +
                        "abc\n",
                false,
                false,
                true
        );
    }
}
