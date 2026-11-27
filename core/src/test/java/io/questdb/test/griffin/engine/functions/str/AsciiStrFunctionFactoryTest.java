/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class AsciiStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstants() throws Exception {
        // code point of the first character
        assertQuery("select ascii('A')").expectSize().returns("ascii\n65\n");
        assertQuery("select ascii('abc')").expectSize().returns("ascii\n97\n");
        // Unicode code point (matches PostgreSQL)
        assertQuery("select ascii('é')").expectSize().returns("ascii\n233\n");
        // first character outside the BMP returns its full code point (surrogate pair)
        assertQuery("select ascii('😀')").expectSize().returns("ascii\n128512\n");
        // empty string returns 0
        assertQuery("select ascii('')").expectSize().returns("ascii\n0\n");
    }

    @Test
    public void testNullAndColumn() throws Exception {
        assertQuery("select s, ascii(s) from t")
                .ddl("create table t (s string)",
                        "insert into t values ('A'), ('abc'), (null), ('')")
                .expectSize()
                .returns(
                        "s\tascii\n" +
                                "A\t65\n" +
                                "abc\t97\n" +
                                "\tnull\n" +
                                "\t0\n"
                );
    }

    @Test
    public void testVarcharColumn() throws Exception {
        assertQuery("select v, ascii(v) from t")
                .ddl("create table t (v varchar)",
                        "insert into t values ('A'), ('é'), (null), ('')")
                .expectSize()
                .returns(
                        "v\tascii\n" +
                                "A\t65\n" +
                                "é\t233\n" +
                                "\tnull\n" +
                                "\t0\n"
                );
    }

    @Test
    public void testVarcharConstants() throws Exception {
        // the VARCHAR overload reads the same code point through the UTF8 accessor
        assertQuery("select ascii('A'::varchar)").expectSize().returns("ascii\n65\n");
        // non-ASCII input takes the UTF8-to-UTF16 decode path
        assertQuery("select ascii('é'::varchar)").expectSize().returns("ascii\n233\n");
        // first character outside the BMP returns its full code point (surrogate pair)
        assertQuery("select ascii('😀'::varchar)").expectSize().returns("ascii\n128512\n");
        // empty VARCHAR returns 0, NULL VARCHAR returns NULL
        assertQuery("select ascii(''::varchar)").expectSize().returns("ascii\n0\n");
        assertQuery("select ascii(null::varchar)").expectSize().returns("ascii\nnull\n");
    }
}
