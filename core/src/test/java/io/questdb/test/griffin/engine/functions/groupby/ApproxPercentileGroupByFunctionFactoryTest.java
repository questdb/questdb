/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class ApproxPercentileGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testApproxPercentileDoubleColumn() throws Exception {
        compile("create table test (col double)");
        insert("insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "5.0029296875\n", "select approx_percentile(0.5, col) from test")
        );
    }

    @Test
    public void testApproxPercentileDoubleNullColumns() throws Exception {
        compile("create table test (col long)");
        insert("insert into test values (null), (null), (null)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "NaN\n", "select approx_percentile(0.5, col) from test")
        );
    }

    @Test
    public void testApproxPercentileDoubleColumnWithNull() throws Exception {
        compile("create table test (col long)");
        insert("insert into test values (1.0), (null), (1.0), (1.0)");
        assertMemoryLeak(() -> assertSql("approx_percentile\n" +
                "1.0\n", "select approx_percentile(0.5, col) from test")
        );
    }
}
