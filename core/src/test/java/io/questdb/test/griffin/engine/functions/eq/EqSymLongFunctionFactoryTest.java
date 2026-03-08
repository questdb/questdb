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

package io.questdb.test.griffin.engine.functions.eq;


import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqSymLongFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testDynamicCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a, abs(rnd_long())%5 b  from long_sequence(10))");
            assertSql(
                    "a\tb\n" +
                            "1\t1\n" +
                            "1\t1\n" +
                            "1\t1\n",
                    "select a,b from x where a = b"
            );
        });
    }

    @Test
    public void testDynamicCastNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a, abs(rnd_long())%5 b  from long_sequence(20))");
            assertSql(
                    "a\tb\n" +
                            "\t3\n" +
                            "\t1\n" +
                            "\t2\n" +
                            "\t0\n" +
                            "\t3\n",
                    "select a,b from x where a = null::long"
            );
        });
    }

    @Test
    public void testDynamicCastNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a, abs(rnd_long())%5 b  from long_sequence(20))");
            assertSql(
                    "a\tb\n" +
                            "1\t1\n" +
                            "3\t3\n" +
                            "1\t1\n" +
                            "1\t1\n" +
                            "3\t3\n" +
                            "1\t1\n",
                    "select a,b from x where a = b"
            );
        });
    }

    @Test
    public void testDynamicSymbolTable() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "x\n" +
                        "3\n" +
                        "8\n" +
                        "10\n",
                "select x from long_sequence(10) where rnd_symbol('1','3','5') = 3"
        ));
    }

    @Test
    public void testStaticSymbolTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5') a from long_sequence(10))");
            assertSql(
                    "a\n" +
                            "3\n" +
                            "3\n" +
                            "3\n",
                    "select a from x where a = 3"
            );
        });
    }

    @Test
    public void testStaticSymbolTableNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_symbol('1','3','5', null) a from long_sequence(30))");
            assertSql(
                    "a\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n" +
                            "\n",
                    "select a from x where a = null::long"
            );
        });
    }
}