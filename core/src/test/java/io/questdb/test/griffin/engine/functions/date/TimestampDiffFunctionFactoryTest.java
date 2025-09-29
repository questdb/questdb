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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampDiffFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class TimestampDiffFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testDayConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('d', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('d', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "365\n" +
                        "365\n",
                "select datediff('d', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "365\n" +
                        "365\n",
                "select datediff('d', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "365\n" +
                        "365\n",
                "select datediff('d', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "365\n" +
                        "365\n",
                "select datediff('d', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('d', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('d', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayEndNan() throws Exception {
        assertMemoryLeak(() -> call('d', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testDayNegative() throws Exception {
        assertMemoryLeak(() -> call('d', 1587707359886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testDaySimple() throws Exception {
        assertMemoryLeak(() -> call('d', 1587275359886758L, 1587707359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testDayStartNan() throws Exception {
        assertMemoryLeak(() -> call('d', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testDynamicFunction() throws Exception {
        execute("create table x as (" +
                "select " +
                "rnd_symbol('u', 'n', 'T') as s," +
                "timestamp_sequence(0, 1000000) as ts, " +
                "timestamp_sequence(0::timestamp_ns, 2000000000) as ts_ns " +
                "from long_sequence(10)" +
                ") timestamp(ts)");
        assertQuery(
                "datediff\n" +
                        "1000000\n" +
                        "0\n" +
                        "1000000\n" +
                        "2000000\n" +
                        "3000000\n" +
                        "4000000\n" +
                        "5000000\n" +
                        "6000000\n" +
                        "7000000\n" +
                        "8000000\n",
                "select datediff('u', 1000000::timestamp, ts) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1000000\n" +
                        "0\n" +
                        "1000000\n" +
                        "2000000\n" +
                        "3000000\n" +
                        "4000000\n" +
                        "5000000\n" +
                        "6000000\n" +
                        "7000000\n" +
                        "8000000\n",
                "select datediff('u', 1000000000::timestamp_ns, ts) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1000000000\n" +
                        "0\n" +
                        "1000000000\n" +
                        "2000000000\n" +
                        "3000000000\n" +
                        "4000000000\n" +
                        "5000000000\n" +
                        "6000000000\n" +
                        "7000000000\n" +
                        "8000000000\n",
                "select datediff('n', ts, 1000000000::timestamp_ns) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1000000\n" +
                        "0\n" +
                        "1000000\n" +
                        "2000000\n" +
                        "3000000\n" +
                        "4000000\n" +
                        "5000000\n" +
                        "6000000\n" +
                        "7000000\n" +
                        "8000000\n",
                "select datediff('u', ts, 1000000::timestamp) from x;",
                null,
                true,
                true
        );

        assertQuery(
                "s\tdatediff\n" +
                        "u\t0\n" +
                        "u\t1000000\n" +
                        "n\t2000000000\n" +
                        "T\t3000\n" +
                        "T\t4000\n" +
                        "T\t5000\n" +
                        "T\t6000\n" +
                        "n\t7000000000\n" +
                        "u\t8000000\n" +
                        "n\t9000000000\n",
                "select s, datediff(s, ts, ts_ns) from x;",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('h', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('h', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "8760\n" +
                        "8760\n",
                "select datediff('h', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "8760\n" +
                        "8760\n",
                "select datediff('h', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "8760\n" +
                        "8760\n",
                "select datediff('h', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "8760\n" +
                        "8760\n",
                "select datediff('h', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('h', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('h', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourEndNan() throws Exception {
        assertMemoryLeak(() -> call('h', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testHourNegative() throws Exception {
        assertMemoryLeak(() -> call('h', 1587293359886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testHourSimple() throws Exception {
        assertMemoryLeak(() -> call('h', 1587275359886758L, 1587293359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testHourStartNan() throws Exception {
        assertMemoryLeak(() -> call('h', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMicroConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('u', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('u', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMicroConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "31536000000000\n" +
                        "31536000000000\n",
                "select datediff('u', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000000000\n" +
                        "31536000000000\n",
                "select datediff('u', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "31536000000000\n" +
                        "31536000000000\n",
                "select datediff('u', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "31536000000000\n" +
                        "31536000000000\n",
                "select datediff('u', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMicroConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('u', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('u', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMicroEndNan() throws Exception {
        assertMemoryLeak(() -> call('u', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMicroNegative() throws Exception {
        assertMemoryLeak(() -> call('u', 1587275364886753L, 1587275364886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMicroSimple() throws Exception {
        assertMemoryLeak(() -> call('u', 1587275359886758L, 1587275359886763L).andAssert(5, 0.0001));
    }

    @Test
    public void testMicroStartNan() throws Exception {
        assertMemoryLeak(() -> call('u', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMilliConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('T', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('T', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMilliConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "31536000000\n" +
                        "31536000000\n",
                "select datediff('T', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000000\n" +
                        "31536000000\n",
                "select datediff('T', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000000\n" +
                        "31536000000\n",
                "select datediff('T', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000000\n" +
                        "31536000000\n",
                "select datediff('T', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMilliConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('T', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('T', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMilliEndNan() throws Exception {
        assertMemoryLeak(() -> call('T', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMilliNegative() throws Exception {
        assertMemoryLeak(() -> call('T', 1587275364881758L, 1587275364886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMilliSimple() throws Exception {
        assertMemoryLeak(() -> call('T', 1587275359886758L, 1587275359891758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMilliStartNan() throws Exception {
        assertMemoryLeak(() -> call('T', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMinuteConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('m', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('m', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "525600\n" +
                        "525600\n",
                "select datediff('m', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "525600\n" +
                        "525600\n",
                "select datediff('m', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "525600\n" +
                        "525600\n",
                "select datediff('m', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "525600\n" +
                        "525600\n",
                "select datediff('m', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('m', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('m', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteEndNan() throws Exception {
        assertMemoryLeak(() -> call('m', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMinuteNegative() throws Exception {
        assertMemoryLeak(() -> call('m', 1587275659886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMinuteSimple() throws Exception {
        assertMemoryLeak(() -> call('m', 1587275359886758L, 1587275659886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMinuteStartNan() throws Exception {
        assertMemoryLeak(() -> call('m', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMonthConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('M', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('M', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "12\n" +
                        "12\n",
                "select datediff('M', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "12\n" +
                        "12\n",
                "select datediff('M', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "12\n" +
                        "12\n",
                "select datediff('M', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "12\n" +
                        "12\n",
                "select datediff('M', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('M', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('M', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthEndNan() throws Exception {
        assertMemoryLeak(() -> call('M', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMonthNegative() throws Exception {
        assertMemoryLeak(() -> call('M', 1600494559886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMonthSimple() throws Exception {
        assertMemoryLeak(() -> call('M', 1587275359886758L, 1600494559886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testMonthStartNan() throws Exception {
        assertMemoryLeak(() -> call('M', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testNanoConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('n', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('n', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testNanoConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "31536000000000000\n" +
                        "31536000000000000\n",
                "select datediff('n', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000000000000\n" +
                        "31536000000000000\n",
                "select datediff('n', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "31536000000000000\n" +
                        "31536000000000000\n",
                "select datediff('n', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
        assertQuery(
                "datediff\n" +
                        "31536000000000000\n" +
                        "31536000000000000\n",
                "select datediff('n', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testNanoConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('n', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('n', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testNanoEndNan() throws Exception {
        assertMemoryLeak(() -> call('n', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testNanoNegative() throws Exception {
        assertMemoryLeak(() -> call('n', 1587275364886753L, 1587275364886758L).andAssert(5000, 0.0001));
    }

    @Test
    public void testNanoSimple() throws Exception {
        assertMemoryLeak(() -> call('n', 1587275359886758L, 1587275359886763L).andAssert(5000, 0.0001));
    }

    @Test
    public void testNanoStartNan() throws Exception {
        assertMemoryLeak(() -> call('n', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testSecondConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('s', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('s', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('s', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondEndNan() throws Exception {
        assertMemoryLeak(() -> call('s', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testSecondNegative() throws Exception {
        assertMemoryLeak(() -> call('s', 1587275364886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testSecondSimple() throws Exception {
        assertMemoryLeak(() -> call('s', 1587275359886758L, 1587275364886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testSecondStartNan() throws Exception {
        assertMemoryLeak(() -> call('s', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testUnknownPeriod() throws Exception {
        assertMemoryLeak(() -> call('/', 1587275359886758L, 1587275364886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testWeekConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('w', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('w', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "52\n" +
                        "52\n",
                "select datediff('w', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "52\n" +
                        "52\n",
                "select datediff('w', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "52\n" +
                        "52\n",
                "select datediff('w', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "52\n" +
                        "52\n",
                "select datediff('w', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('w', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('w', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekEndNan() throws Exception {
        assertMemoryLeak(() -> call('w', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testWeekNegative() throws Exception {
        assertMemoryLeak(() -> call('w', 1590299359886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testWeekSimple() throws Exception {
        assertMemoryLeak(() -> call('w', 1587275359886758L, 1590299359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testWeekStartNan() throws Exception {
        assertMemoryLeak(() -> call('w', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testYearConstantEndNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('y', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('y', to_timestamp_ns(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearConstantSimple() throws Exception {
        assertQuery(
                "datediff\n" +
                        "1\n" +
                        "1\n",
                "select datediff('y', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1\n" +
                        "1\n",
                "select datediff('y', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1\n" +
                        "1\n",
                "select datediff('y', to_timestamp(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "1\n" +
                        "1\n",
                "select datediff('y', to_timestamp_ns(concat('202',x),'yyyy'), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearConstantStartNaN() throws Exception {
        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('y', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );

        assertQuery(
                "datediff\n" +
                        "null\n" +
                        "null\n",
                "select datediff('y', cast(NaN as long), to_timestamp_ns(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearEndNan() throws Exception {
        assertMemoryLeak(() -> call('y', 1587275364886758L, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testYearNegative() throws Exception {
        assertMemoryLeak(() -> call('y', 1745041759886758L, 1587275359886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testYearSimple() throws Exception {
        assertMemoryLeak(() -> call('y', 1587275359886758L, 1745041759886758L).andAssert(5, 0.0001));
    }

    @Test
    public void testYearStartNan() throws Exception {
        assertMemoryLeak(() -> call('y', Numbers.LONG_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampDiffFunctionFactory();
    }
}
