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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SumLong256VecGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery("sum\n\n", "select sum(f) from tab", "create table tab as (select cast(null as long256) f from long_sequence(33))", null, "insert into tab select cast(123L as long256) from long_sequence(1)", "sum\n0x7b\n", false, true, false);
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "sum\tsum1\n5050\t0x13ba\n",
                "select sum(x), sum(y) from tab",
                "create table tab as (select x, cast(x as long256) y from long_sequence(100))\n",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumBigFive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab (x long256)");
            execute("insert into tab values (0xb00ee5505bd95e51dd18889bae1dee3404d446e61d5293f55ff29ba4a01ab073)");
            execute("insert into tab values (0x6f64ae42c48c96a19e099d7a980099af601f70d614b709804ea60bf902c30e3e)");
            execute("insert into tab values (0x7c6ec2b2ffd4a89ec87dd041359f34661ce5fa64b58567a438c725aa47e609dd)");
            execute("insert into tab values (0x15ed3484045af1d460b09ed4a3984890458c09608a4ce455731bed64a1545c05)");
            execute("insert into tab values (0xb6292e820db4d91ba9a74c8c459676d127590af55a4eccba93a826db814c49c6)");
            String query = "select sum(x) from tab";
            String ex = "sum\n0x67f8b94c324a68824df7e1b864ec7baaeebec676cc2ab629ee23e1880d646e59\n";
            printSqlResult(ex, query, null, false, true);
        });
    }
}