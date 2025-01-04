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
import io.questdb.griffin.engine.functions.math.GreatestNumericFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class GreatestNumericFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testGreatestNumericFunctionFactoryBytes() throws Exception {
        assertSqlWithTypes("greatest\n40:BYTE\n", "select greatest(1::byte, 40::byte)");
        assertSqlWithTypes("greatest\n12:BYTE\n", "select greatest(1::byte, 4::byte, 3::byte, 12::byte, 8::byte)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryConversions() throws Exception {
        assertSqlWithTypes("greatest\n4:INT\n", "select greatest(1, 4)");
        assertSqlWithTypes("greatest\n4:LONG\n", "select greatest(1, 4L)");
        assertSqlWithTypes("greatest\n4:LONG\n", "select greatest(1::short, 4L)");
        assertSqlWithTypes("greatest\n4:SHORT\n", "select greatest(1::short, 4::byte)");
        assertSqlWithTypes("greatest\n4.0000:FLOAT\n", "select greatest(1::short, 4f)");
        assertSqlWithTypes("greatest\n4.0:DOUBLE\n", "select greatest(1::short, 4.0)");
        assertSqlWithTypes("greatest\n4.0:DOUBLE\n", "select greatest(1f, 4.0::double)");
        assertSqlWithTypes("greatest\n4.0000:FLOAT\n", "select greatest(1f, 4::int)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryDoubles() throws Exception {
        assertSqlWithTypes("greatest\n9.2:DOUBLE\n", "select greatest(5.3, 9.2)");
        assertSqlWithTypes("greatest\n11.6:DOUBLE\n", "select greatest(5.3, 9.2, 6.5, 11.6, 3.2)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryFloats() throws Exception {
        assertSqlWithTypes("greatest\n9.2000:FLOAT\n", "select greatest(5.3f, 9.2f)");
        assertSqlWithTypes("greatest\n11.6000:FLOAT\n", "select greatest(5.3f, 9.2f, 6.5f, 11.6f, 3.2f)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryInts() throws Exception {
        assertSqlWithTypes("greatest\n40:INT\n", "select greatest(1, 40)");
        assertSqlWithTypes("greatest\n12:INT\n", "select greatest(1, 4, 3, 12, 8)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryLongs() throws Exception {
        assertSqlWithTypes("greatest\n40:LONG\n", "select greatest(1L, 40L)");
        assertSqlWithTypes("greatest\n12:LONG\n", "select greatest(1L, 4L, 3L, 12L, 8L)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryNulls() throws Exception {
        assertSqlWithTypes("greatest\nnull:NULL\n", "select greatest(1L, null, 2L)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryShorts() throws Exception {
        assertSqlWithTypes("greatest\n40:SHORT\n", "select greatest(1::short, 40::short)");
        assertSqlWithTypes("greatest\n12:SHORT\n", "select greatest(1::short, 4::short, 3::short, 12::short, 8::short)");
    }

    @Test
    public void testGreatestNumericFunctionFactoryUnsupportedTypes() throws Exception {
        assertException("select greatest(5, 5.2, 'abc', 2)", 24, "unsupported type");
        assertException("select greatest(5, 5.2, 'abc'::varchar, 2)", 29, "unsupported type");
    }

    @Test
    public void testGreatestNumericFunctionFactoryWithData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_int() a, rnd_int() b from long_sequence(20))");
            drainWalQueue();
            assertSqlWithTypes("greatest\n" +
                    "315515118:INT\n" +
                    "1548800833:INT\n" +
                    "73575701:INT\n" +
                    "1326447242:INT\n" +
                    "1868723706:INT\n" +
                    "-1191262516:INT\n" +
                    "-1436881714:INT\n" +
                    "1545253512:INT\n" +
                    "1573662097:INT\n" +
                    "339631474:INT\n" +
                    "1904508147:INT\n" +
                    "-1458132197:INT\n" +
                    "1125579207:INT\n" +
                    "426455968:INT\n" +
                    "-85170055:INT\n" +
                    "-1520872171:INT\n" +
                    "-1101822104:INT\n" +
                    "1404198:INT\n" +
                    "1631244228:INT\n" +
                    "-1252906348:INT\n", "select greatest(a, b) from x");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new GreatestNumericFunctionFactory();
    }
}
