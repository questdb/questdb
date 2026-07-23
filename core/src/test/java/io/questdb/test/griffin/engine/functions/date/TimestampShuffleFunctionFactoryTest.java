/*+*****************************************************************************
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
package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
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
    public void testCrossZeroRangeDoesNotOverflow() throws Exception {
        final ObjList<Function> args = new ObjList<>();
        args.add(TimestampConstant.newInstance(Long.MIN_VALUE + 1, ColumnType.TIMESTAMP_MICRO));
        args.add(TimestampConstant.newInstance(Long.MAX_VALUE, ColumnType.TIMESTAMP_MICRO));
        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            function.init(null, sqlExecutionContext);
            for (int i = 0; i < 10_000; i++) {
                final long value = function.getTimestamp(null);
                Assert.assertTrue("value below lower bound: " + value, value > Long.MIN_VALUE);
                Assert.assertTrue("value reached exclusive upper bound", value < Long.MAX_VALUE);
            }
        }
    }

    @Test
    public void testEqualEndpointsReturnEndpoint() throws SqlException {
        call(42L, 42L).andInit(sqlExecutionContext).andAssertTimestamp(42L);
    }

    @Test
    public void testEndBeforeStart() throws SqlException {
        call(1000000L, 0L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
        call(1000000L, 0L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
    }

    @Test
    public void testRandomMetadataIsDeclared() throws Exception {
        final ObjList<Function> args = new ObjList<>();
        args.add(TimestampConstant.newInstance(0, ColumnType.TIMESTAMP_MICRO));
        args.add(TimestampConstant.newInstance(1, ColumnType.TIMESTAMP_MICRO));
        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertTrue(function.isRandom());
            Assert.assertTrue(function.isNonDeterministic());
        }
    }

    @Test
    public void testStartBeforeEnd() throws SqlException {
        call(0L, 1000000L).andInit(sqlExecutionContext).andAssertTimestamp(643856L);
    }

    @Test
    public void testVanilla() throws Exception {
        assertQuery("select timestamp_shuffle(0, 1000000) from long_sequence(1)")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("timestamp_shuffle\n" +
                        "1970-01-01T00:00:00.643856Z\n");
        assertQuery("select timestamp_shuffle(1::timestamp, 1000000::timestamp_ns) from long_sequence(1)")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("timestamp_shuffle\n" +
                        "1970-01-01T00:00:00.000967856Z\n");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new io.questdb.griffin.engine.functions.date.TimestampShuffleFunctionFactory();
    }
}
