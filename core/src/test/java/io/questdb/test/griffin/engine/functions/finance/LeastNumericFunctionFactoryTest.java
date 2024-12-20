/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.LeastNumericFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LeastNumericFunctionFactoryTest extends AbstractFunctionFactoryTest {


    @Test
    public void testLeastNumericFunctionFactoryBytes() throws Exception {
        assertSqlWithTypes("least\n1:BYTE\n", "select least(1::byte, 40::byte)");
        assertSqlWithTypes("least\n1:BYTE\n", "select least(1::byte, 4::byte, 3::byte, 12::byte, 8::byte)");
    }

    @Test
    public void testLeastNumericFunctionFactoryConversions() throws Exception {
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 4)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1, 4L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1::short, 4L)");
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 4::byte)");
        assertSqlWithTypes("least\n1.0000:FLOAT\n", "select least(1::short, 4f)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1::short, 4.0)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1f, 4.0::double)");
        assertSqlWithTypes("least\n1.0000:FLOAT\n", "select least(1f, 4::int)");
    }

    @Test
    public void testLeastNumericFunctionFactoryDoubles() throws Exception {
        assertSqlWithTypes("least\n5.3:DOUBLE\n", "select least(5.3, 9.2)");
        assertSqlWithTypes("least\n3.2:DOUBLE\n", "select least(5.3, 9.2, 6.5, 11.6, 3.2)");
    }

    @Test
    public void testLeastNumericFunctionFactoryFloats() throws Exception {
        assertSqlWithTypes("least\n5.3000:FLOAT\n", "select least(5.3f, 9.2f)");
        assertSqlWithTypes("least\n3.2000:FLOAT\n", "select least(5.3f, 9.2f, 6.5f, 11.6f, 3.2f)");
    }

    @Test
    public void testLeastNumericFunctionFactoryInts() throws Exception {
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 40)");
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 4, 3, 12, 8)");
    }

    @Test
    public void testLeastNumericFunctionFactoryLongs() throws Exception {
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, 40L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, 4L, 3L, 12L, 8L)");
    }

    @Test
    public void testLeastNumericFunctionFactoryNulls() throws Exception {
        assertSqlWithTypes("least\nnull:NULL\n", "select least(1L, null, 2L)");
    }

    @Test
    public void testLeastNumericFunctionFactoryShorts() throws Exception {
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 40::short)");
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 4::short, 3::short, 12::short, 8::short)");
    }

    @Test
    public void testLeastNumericFunctionFactoryUnsupportedTypes() throws Exception {
        assertException("select least(5, 5.2, 'abc', 2)", 21, "unsupported type");
        assertException("select least(5, 5.2, 'abc'::varchar, 2)", 26, "unsupported type");
    }

    @Test
    public void testLeastNumericFunctionFactoryWithData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_int() a, rnd_int() b from long_sequence(20))");
            drainWalQueue();
            assertSqlWithTypes("least\n" +
                    "-1148479920:INT\n" +
                    "-727724771:INT\n" +
                    "-948263339:INT\n" +
                    "592859671:INT\n" +
                    "-847531048:INT\n" +
                    "-2041844972:INT\n" +
                    "-1575378703:INT\n" +
                    "806715481:INT\n" +
                    "1569490116:INT\n" +
                    "-409854405:INT\n" +
                    "1530831067:INT\n" +
                    "-1532328444:INT\n" +
                    "-1849627000:INT\n" +
                    "-1432278050:INT\n" +
                    "-1792928964:INT\n" +
                    "-1844391305:INT\n" +
                    "-1153445279:INT\n" +
                    "-1715058769:INT\n" +
                    "-1125169127:INT\n" +
                    "-1975183723:INT\n", "select least(a, b) from x");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LeastNumericFunctionFactory();
    }
}
