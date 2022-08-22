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

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class FirstStringGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testKeyed() throws Exception {
        // a	sym
        // 0	aaab
        // 3	c
        // 1	c
        // 2	bab
        // 1	bab
        assertMemoryLeak(() -> assertQuery(
                "a\tfirst\n" +
                        "0\taaab\n" +
                        "3\tc\n" +
                        "1\tc\n" +
                        "2\tbab\n",
                "select a, first(sym) from tab",
                "create table tab as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym from long_sequence(5))",
                null,
                true,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "a\tfirst\n",
                "select a, first(sym) from tab2",
                "create table tab2 as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym from long_sequence(0))",
                null,
                true,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "first\n\n",
                "select first(sym) from tab3",
                "create table tab3 as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym from long_sequence(0))",
                null,
                false,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "first\nc\n",
                "select first(sym) from tab4",
                "create table tab4 as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym from long_sequence(10))",
                null,
                false,
                true,
                true
        ));
        assertMemoryLeak(() -> assertQuery(
                "first\tfirst1\nbab\tg\n",
                "select first(sym), first(sym2) from tab5",
                "create table tab5 as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym, rnd_str('def', 'fff', 'g') sym2 from long_sequence(10))",
                null,
                false,
                true,
                true
        ));
        // a	sym	sym2
        //-2	bab	def
        //-4	c	def
        //-1	bab	g
        //4	c	def
        //-4	aaab	g
        //-3	aaab	def
        //1	bab	fff
        //-2	aaab	fff
        //1	c	g
        //0	c	fff
        assertMemoryLeak(() -> assertQuery(
                "a\tfirst\tfirst1\n" +
                        "-2\tbab\tdef\n" +
                        "-4\tc\tdef\n" +
                        "-1\tbab\tg\n" +
                        "4\tc\tdef\n" +
                        "-3\taaab\tdef\n" +
                        "1\tbab\tfff\n" +
                        "0\tc\tfff\n",
                "select a, first(sym), first(sym2) from tab6",
                "create table tab6 as (select rnd_int() % 5 a, rnd_str('aaab', 'bab', 'c') sym, rnd_str('def', 'fff', 'g') sym2 from long_sequence(10))",
                null,
                true,
                true,
                true
        ));
    }
}