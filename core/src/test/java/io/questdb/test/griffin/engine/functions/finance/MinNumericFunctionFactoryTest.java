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
import io.questdb.griffin.engine.functions.finance.MinNumericFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class MinNumericFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testMinNumericFunctionFactoryBytes() throws Exception {
        assertSqlWithTypes("min\n1:BYTE\n", "select min(1::byte, 40::byte)");
        assertSqlWithTypes("min\n1:BYTE\n", "select min(1::byte, 4::byte, 3::byte, 12::byte, 8::byte)");
    }

    @Test
    public void testMinNumericFunctionFactoryConversions() throws Exception {
        assertSqlWithTypes("min\n1:INT\n", "select min(1, 4)");
        assertSqlWithTypes("min\n1:LONG\n", "select min(1, 4L)");
        assertSqlWithTypes("min\n1:LONG\n", "select min(1::short, 4L)");
        assertSqlWithTypes("min\n1:SHORT\n", "select min(1::short, 4::byte)");
        assertSqlWithTypes("min\n1.0000:FLOAT\n", "select min(1::short, 4f)");
        assertSqlWithTypes("min\n1.0:DOUBLE\n", "select min(1::short, 4.0)");
        assertSqlWithTypes("min\n1.0:DOUBLE\n", "select min(1f, 4.0::double)");
        assertSqlWithTypes("min\n1.0000:FLOAT\n", "select min(1f, 4::int)");
    }

    @Test
    public void testMinNumericFunctionFactoryDoubles() throws Exception {
        assertSqlWithTypes("min\n5.3:DOUBLE\n", "select min(5.3, 9.2)");
        assertSqlWithTypes("min\n3.2:DOUBLE\n", "select min(5.3, 9.2, 6.5, 11.6, 3.2)");
    }

    @Test
    public void testMinNumericFunctionFactoryFloats() throws Exception {
        assertSqlWithTypes("min\n5.3000:FLOAT\n", "select min(5.3f, 9.2f)");
        assertSqlWithTypes("min\n3.2000:FLOAT\n", "select min(5.3f, 9.2f, 6.5f, 11.6f, 3.2f)");
    }

    @Test
    public void testMinNumericFunctionFactoryInts() throws Exception {
        assertSqlWithTypes("min\n1:INT\n", "select min(1, 40)");
        assertSqlWithTypes("min\n1:INT\n", "select min(1, 4, 3, 12, 8)");
    }

    @Test
    public void testMinNumericFunctionFactoryLongs() throws Exception {
        assertSqlWithTypes("min\n1:LONG\n", "select min(1L, 40L)");
        assertSqlWithTypes("min\n1:LONG\n", "select min(1L, 4L, 3L, 12L, 8L)");
    }

    @Test
    public void testMinNumericFunctionFactoryShorts() throws Exception {
        assertSqlWithTypes("min\n1:SHORT\n", "select min(1::short, 40::short)");
        assertSqlWithTypes("min\n1:SHORT\n", "select min(1::short, 4::short, 3::short, 12::short, 8::short)");
    }

    @Test
    public void testMinNumericFunctionFactoryUnsupportedTypes() throws Exception {
        assertException("select min(5, 5.2, 'abc', 2)", 19, "unsupported type");
        assertException("select min(5, 5.2, 'abc'::varchar, 2)", 24, "unsupported type");
    }

    @Test
    public void testMinNumericFunctionFactoryWithData() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select rnd_int() a, rnd_int() b from long_sequence(20))");
            drainWalQueue();
            assertSqlWithTypes("min\n" +
                    "-1532328444:INT\n" +
                    "-409854405:INT\n" +
                    "-1792928964:INT\n" +
                    "-1432278050:INT\n" +
                    "592859671:INT\n" +
                    "-1849627000:INT\n" +
                    "806715481:INT\n" +
                    "-1148479920:INT\n" +
                    "-1575378703:INT\n" +
                    "-1715058769:INT\n" +
                    "-1153445279:INT\n" +
                    "-1975183723:INT\n" +
                    "1530831067:INT\n" +
                    "-2041844972:INT\n" +
                    "-948263339:INT\n" +
                    "-727724771:INT\n" +
                    "-1844391305:INT\n" +
                    "1569490116:INT\n" +
                    "-847531048:INT\n" +
                    "-1125169127:INT\n", "select min(a, b) from x");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new MinNumericFunctionFactory();
    }
}
