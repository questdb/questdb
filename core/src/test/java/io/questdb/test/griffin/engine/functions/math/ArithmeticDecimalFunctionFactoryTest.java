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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public abstract class ArithmeticDecimalFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final Decimal256 decimal256 = new Decimal256();

    protected void createFunctionAndAssert(Function left, Function right, long hh, long hl, long lh, long ll, int expectedType) throws SqlException {
        args.clear();
        args.add(left);
        args.add(right);
        try (Function func = getFactory().newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            DecimalUtil.load(decimal256, Misc.getThreadLocalDecimal128(), func, null);
            Assert.assertEquals(hh, decimal256.getHh());
            Assert.assertEquals(hl, decimal256.getHl());
            Assert.assertEquals(lh, decimal256.getLh());
            Assert.assertEquals(ll, decimal256.getLl());
            Assert.assertEquals(expectedType, func.getType());
        }
    }

    protected void createFunctionAndAssertFails(Function left, Function right, String expectedMessage) throws SqlException {
        args.clear();
        args.add(left);
        args.add(right);
        try {
            try (Function func = getFactory().newInstance(-1, args, null, configuration, sqlExecutionContext)) {
                DecimalUtil.load(decimal256, Misc.getThreadLocalDecimal128(), func, null);
                Assert.fail();
            }
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    protected void createFunctionAndAssertNull(Function left, Function right, int expectedType) throws SqlException {
        createFunctionAndAssert(
                left,
                right,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }

    protected abstract FunctionFactory getFactory();
}
