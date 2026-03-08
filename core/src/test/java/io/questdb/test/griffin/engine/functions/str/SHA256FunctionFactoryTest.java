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

public class SHA256FunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBinSimple() throws Exception {
        assertQuery(
                "bin\tsha256\n" +
                        "00000000 41 1d\t3fe6b3fdfd367573eb0a8c0ca6ca2089615b95f7d7d66389dc9896107ae0e346\n"
                        +
                        "00000000 8a 17 fa d8\t9ff870606b17e69a98a5b72a22d5665958e36cd7958b7b7918984774953c6dc3\n"
                        +
                        "00000000 ce f1\tc8d1c780294644c8455124e1e5810caf670654a357298be73b6b9880e9fa032e\n"
                        +
                        "\t\n" +
                        "00000000 91\t7da59d0dfbe21f43e842e8afb43e12a6445bbac07c2fc26984c71d0de3f99c9c\n"
                        +
                        "00000000 db f3 04 1b\t582c7476c48493ef238476834d2f9bfc68df64a6b09d4e44b2a3d7ca622e801b\n"
                        +
                        "00000000 de a0\t3550f11369efc974c1268d54853a7f7d4d5a219bda26ff5c1c3b1e531c1cf1a7\n"
                        +
                        "\t\n" +
                        "00000000 15 68\t6a52e0c71cd3a0d4ef748dfa917b79f810b4390aa342a4f92734681b2ceb0f3d\n"
                        +
                        "00000000 af 19 c4 95\t821405564907a5e52db7d8330af7708831290ec823f00e75e661fd0cf706aac3\n",
                "select bin,sha256(bin) from x",
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
                "str\tsha256\n" +
                        "abc\tba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"
                        +
                        "\te3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n" +
                        "x\t2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881\n"
                        +
                        "\t\n" +
                        "x\t2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881\n",
                "select str,sha256(str) from x",
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
                "v\tsha256\n" +
                        "abc\tba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n"
                        +
                        "едно-две-три\t8f6a2a5c5f0bbee2c776eaff26e5c244a39d9e60a65fed2e1dfc5e91b0260279\n"
                        +
                        "едно-две-три\t8f6a2a5c5f0bbee2c776eaff26e5c244a39d9e60a65fed2e1dfc5e91b0260279\n"
                        +
                        "x\t2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881\n"
                        +
                        "x\t2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881\n",
                "select v,sha256(v) from x",
                "create table x as (" +
                        "select rnd_varchar('abc','x','','едно-две-три',NULL) as v\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true);
    }
}
