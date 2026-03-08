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

public class MD5FunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBinSimple() throws Exception {
        assertQuery(
                "bin\tmd5\n" +
                        "00000000 41 1d\td95cbbe288c2b14e4980750cecc36067\n" +
                        "00000000 8a 17 fa d8\t84d095be93683b086bbd85f1830217dd\n" +
                        "00000000 ce f1\t85a5888c0b9c2152c091e06016c94a9c\n" +
                        "\t\n" +
                        "00000000 91\t40412202892318d52822e62195a65762\n" +
                        "00000000 db f3 04 1b\tec2ed6ee17d7f188b10716a7c41d77cf\n" +
                        "00000000 de a0\t4d1b74d753b4b4d6ccb71aa9c581fbcd\n" +
                        "\t\n" +
                        "00000000 15 68\t17b3bbce052f9f8317f44c8b9f4fc77d\n" +
                        "00000000 af 19 c4 95\t9d7386e4f52d076e9994cb3d1880f21e\n",
                "select bin,md5(bin) from x",
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
                "str\tmd5\n" +
                        "abc\t900150983cd24fb0d6963f7d28e17f72\n" +
                        "\td41d8cd98f00b204e9800998ecf8427e\n" +
                        "x\t9dd4e461268c8034f5c8564e155c67a6\n" +
                        "\t\n" +
                        "x\t9dd4e461268c8034f5c8564e155c67a6\n",
                "select str,md5(str) from x",
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
                "v\tmd5\n" +
                        "abc\t900150983cd24fb0d6963f7d28e17f72\n" +
                        "едно-две-три\t6c9f2d4ac2b6442dd685906b5d61aeb8\n" +
                        "едно-две-три\t6c9f2d4ac2b6442dd685906b5d61aeb8\n" +
                        "x\t9dd4e461268c8034f5c8564e155c67a6\n" +
                        "x\t9dd4e461268c8034f5c8564e155c67a6\n",
                "select v,md5(v) from x",
                "create table x as (" +
                        "select rnd_varchar('abc','x','','едно-две-три',NULL) as v\n" +
                        "from long_sequence(5)" +
                        ")",
                null,
                true,
                true);
    }
}
