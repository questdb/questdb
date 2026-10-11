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


package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToBooleanFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CastStrToBooleanFunctionFactoryTest {

    @Test
    public void testNonBooleanTextAllocatesNothing() throws Exception {
        final ObjList<Function> args = new ObjList<>();
        args.add(new StrFunction() {
            private int callCount;

            @Override
            public CharSequence getStrA(Record rec) {
                return (callCount++ & 1) == 0 ? "abc" : "";
            }

            @Override
            public CharSequence getStrB(Record rec) {
                return getStrA(rec);
            }
        });
        final IntList argPositions = new IntList();
        argPositions.add(0);
        try (
                TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope();
                Function cast = new CastStrToBooleanFunctionFactory().newInstance(0, args, argPositions, null, null)
        ) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            for (int i = 0; i < 20_000; i++) {
                Assert.assertFalse(cast.getBool(null));
            }
            long minAllocatedBytes = Long.MAX_VALUE;
            int falseCount = 0;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 10_000; i++) {
                    if (!cast.getBool(null)) {
                        falseCount++;
                    }
                }
                minAllocatedBytes = Math.min(minAllocatedBytes,
                        threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
            }
            Assert.assertEquals(50_000, falseCount);
            Assert.assertEquals(0, minAllocatedBytes);
        }
    }
}
