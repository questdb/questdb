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

public class NegFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNegByte() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select cast(10 as byte) x)", "-10\tBYTE\n");
    }

    @Test
    public void testNegDouble() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select 5.6 x)", "-5.6\tDOUBLE\n");
    }

    @Test
    public void testNegFloat() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select cast(10 as float) x)", "-10.0\tFLOAT\n");
    }

    @Test
    public void testNegInt() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select 20 x)", "-20\tINT\n");
    }

    @Test
    public void testNegIntNull() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select cast(null as int) x)", "null\tINT\n");
    }

    @Test
    public void testNegLong() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select 20L x)", "-20\tLONG\n");
    }

    @Test
    public void testNegLongNull() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select cast(null as long) x)", "null\tLONG\n");
    }

    @Test
    public void testNegShort() throws Exception {
        assertNeg("select -x, typeOf(-x) from (select cast(10 as short) x)", "-10\tSHORT\n");
    }

    private void assertNeg(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                engine,
                sqlExecutionContext,
                sql,
                sink,
                "column\ttypeOf\n" +
                        expected
        ));
    }

}