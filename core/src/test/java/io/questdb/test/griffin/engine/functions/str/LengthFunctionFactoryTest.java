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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LengthFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBinSimple() throws Exception {
        assertQuery(
                """
                        bin\tlength
                        00000000 41 1d\t2
                        00000000 8a 17 fa d8\t4
                        00000000 ce f1\t2
                        \t-1
                        00000000 91\t1
                        00000000 db f3 04 1b\t4
                        00000000 de a0\t2
                        \t-1
                        00000000 15 68\t2
                        00000000 af 19 c4 95\t4
                        """,
                "select bin,length(bin) from x",
                "create table x as (" +
                        "select rnd_bin(1,5,5) as bin\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrSimple() throws Exception {
        assertQuery(
                """
                        str\tlength
                        abc\t3
                        \t0
                        x\t1
                        \t-1
                        x\t1
                        """,
                "select str,length(str) from x",
                "create table x as (" +
                        "select rnd_str('abc','x','',NULL) as str\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testSymbolSimple() throws Exception {
        assertQuery(
                """
                        sym\tlength
                        WC\t2
                        \t-1
                        EH\t2
                        \t-1
                        EH\t2
                        SWH\t3
                        T\t1
                        T\t1
                        T\t1
                        \t-1
                        """,
                "select sym,length(sym) from x",
                "create table x as (" +
                        "select rnd_symbol(5,1,3,5) as sym\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharLengthBytesSimple() throws Exception {
        assertQuery(
                """
                        v\tlength_bytes
                        abc\t3
                        тест\t8
                        тест\t8
                        x\t1
                        x\t1
                        """,
                "select v, length_bytes(v) from x",
                "create table x as (" +
                        "select rnd_varchar('abc','x','','тест',NULL) as v\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVarcharSimple() throws Exception {
        assertQuery(
                """
                        v\tlength
                        abc\t3
                        едно-две-три\t12
                        едно-две-три\t12
                        x\t1
                        x\t1
                        """,
                "select v,length(v) from x",
                "create table x as (" +
                        "select rnd_varchar('abc','x','','едно-две-три',NULL) as v\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true
        );
    }
}
