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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqrtFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSqrtDouble() throws Exception {
        assertSqrt("select sqrt(4000.32)", "63.2480829749013\n");
    }

    @Test
    public void testSqrtDoubleNull() throws Exception {
        assertSqrt("select sqrt(NaN)", "null\n");
    }

    private void assertSqrt(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                engine,
                sqlExecutionContext,
                sql,
                sink,
                "sqrt\n" +
                        expected
        ));
    }
}