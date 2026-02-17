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

public class SumLong256GroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSumAllNull() throws Exception {
        assertQuery(
                "sum\n\n",
                "select sum(x) from (select cast(null as long256) x from long_sequence(100000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumOverUnionAll() throws Exception {
        assertQuery(
                "sm\n0x06\n",
                "select * from ( select sum(x) as sm from (select * from test union all select * from test ) )",
                "create table test as (select cast(x as long256) x from long_sequence(2))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumSeq() throws Exception {
        assertQuery(
                "sum\tsum1\n0x012a06b550\t5000050000\n",
                "select sum(x), sum(y) from (select cast(x as long256) x, x as y from long_sequence(100000))",
                null,
                false,
                true
        );
    }

    @Test
    public void testSumSeqWithFilter() throws Exception {
        assertQuery(
                "sum\tsum1\n0x01270bb148\t4950045000\n",
                "select sum(x), sum(y) from (select cast(x as long256) x, x as y from long_sequence(100000) where x > 10000)",
                null,
                false,
                true
        );
    }
}
