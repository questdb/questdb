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
import io.questdb.griffin.SqlException;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Before;
import org.junit.Test;

public class TimestampShuffleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Before
    public void setUp() {
        sqlExecutionContext.setRandom(new Rnd() {
            boolean ran;
            long val;

            @Override
            public long nextPositiveLong() {
                if (!ran) {
                    val = super.nextPositiveLong();
                    ran = true;
                }
                return val;
            }
        });
    }

    @Test
    public void testEndBeforeStart() throws SqlException {
        call(1000000L, 0L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
        call(1000000L, 0L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
    }

    @Test
    public void testStartBeforeEnd() throws SqlException {
        call(0L, 1000000L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
    }

    @Test
    public void testVanilla() throws Exception {
        assertSql(
                "timestamp_shuffle\n" +
                        "1970-01-01T00:00:00.643856Z\n",
                "select timestamp_shuffle(0, 1000000) from long_sequence(1)");
        assertSql(
                "timestamp_shuffle\n" +
                        "1970-01-01T00:00:00.000967856Z\n",
                "select timestamp_shuffle(1::timestamp, 1000000::timestamp_ns) from long_sequence(1)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new io.questdb.griffin.engine.functions.date.TimestampShuffleFunctionFactory();
    }
}
