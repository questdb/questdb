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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StringAggGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testGroupKeyed() throws Exception {
        // a	s	ts
        //a	bbb	1970-01-01T00:00:00.000000Z
        //b	ccc	1970-01-01T00:00:00.100000Z
        //f	ccc	1970-01-01T00:00:00.200000Z
        //c	ccc	1970-01-01T00:00:00.300000Z
        //a	abc	1970-01-01T00:00:00.400000Z
        assertQuery(
                "s\tstring_agg\n" +
                "bbb\ta\n" +
                "ccc\tb,f,c\n" +
                "abc\ta\n",
                "select s, string_agg(a, ',') from x",
                "create table x as (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') a," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') s, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyedStr() throws Exception {
        // s	a	b
        //a	bbb	gg
        //f	aaa	ff
        //c	ccc	ee
        //e	abc	def
        //e	ccc	gg
        assertQuery(
                "s\tstring_agg\tstring_agg1\n" +
                        "a\tbbb\tgg\n" +
                        "f\taaa\tff\n" +
                        "c\tccc\tee\n" +
                        "e\tabc,ccc\tdef:gg\n",
                "select s, string_agg(a, ','), string_agg(b, ':') from x",
                "create table x as (" +
                        "   select " +
                        "       rnd_symbol('a','b','c','d','e','f') s," +
                        "       rnd_str('abc', 'aaa', 'bbb', 'ccc') a, " +
                        "       rnd_str('def', 'gg', 'ee', 'ff') b, " +
                        "       timestamp_sequence(0, 100000) ts " +
                        "   from long_sequence(5)" +
                        ")",
                null,
                true,
                true,
                true
        );
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

    @Test
    public void testConstant() throws Exception {
        assertSql(
                "select string_agg('aaa', ',')",
                "string_agg\naaa\n"
        );
    }
}
