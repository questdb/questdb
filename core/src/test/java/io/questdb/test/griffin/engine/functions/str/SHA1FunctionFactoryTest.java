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

public class SHA1FunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBinSimple() throws Exception {
        assertQuery(
                "bin\tsha1\n" +
                        "00000000 41 1d\t19d96d00f1b31c0989201c239323810387cba8b1\n" +
                        "00000000 8a 17 fa d8\tb8c3c7c3ea98c4d78d31f45170db8bf24f1ddfd2\n" +
                        "00000000 ce f1\t1cadbd8a1f474a2dae06ec330f39ea3bcb26d1c8\n" +
                        "\t\n" +
                        "00000000 91\te9f987c3ab268ba6cf1c2ca075d6d26b01791214\n" +
                        "00000000 db f3 04 1b\t336637871f2a3800236e6d21c410476e10e85b32\n" +
                        "00000000 de a0\t52c39fe9b75dd04fff343e9db6016d41bb41eb91\n" +
                        "\t\n" +
                        "00000000 15 68\tae09e4e424ec1a933aa4fe667ede18438e8395b1\n" +
                        "00000000 af 19 c4 95\t3456bc75bb44625f9ba45cc88a0aeb5d1ec914a7\n",
                "select bin,sha1(bin) from x",
                "create table x as (" +
                        "select rnd_bin(1,5,5) as bin\n" +
                        "from long_sequence(10)" +
                        ")",
                null,
                true,
                true);
    }

    @Test
    public void testStrSimple() throws Exception {
        assertQuery(
                "str\tsha1\n" +
                        "abc\ta9993e364706816aba3e25717850c26c9cd0d89d\n" +
                        "\tda39a3ee5e6b4b0d3255bfef95601890afd80709\n" +
                        "x\t11f6ad8ec52a2984abaafd7c3b516503785c2072\n" +
                        "\t\n" +
                        "x\t11f6ad8ec52a2984abaafd7c3b516503785c2072\n",
                "select str,sha1(str) from x",
                "create table x as (" +
                        "select rnd_str('abc','x','',NULL) as str\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true);
    }

    @Test
    public void testVarcharSimple() throws Exception {
        assertQuery(
                "v\tsha1\n" +
                        "abc\ta9993e364706816aba3e25717850c26c9cd0d89d\n" +
                        "едно-две-три\t3c5c42ebd4b836889894f81551fab5eee6b9270b\n" +
                        "едно-две-три\t3c5c42ebd4b836889894f81551fab5eee6b9270b\n" +
                        "x\t11f6ad8ec52a2984abaafd7c3b516503785c2072\n" +
                        "x\t11f6ad8ec52a2984abaafd7c3b516503785c2072\n",
                "select v,sha1(v) from x",
                "create table x as (" +
                        "select rnd_varchar('abc','x','','едно-две-три',NULL) as v\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true);
    }
}
