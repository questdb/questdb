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
import io.questdb.std.ObjList;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

public abstract class ArithmeticDecimalFunctionFactoryTest extends AbstractTest {

    protected void createFunctionAndAssert(ObjList<Function> args, long hh, long hl, long lh, long ll, int expectedType) throws SqlException {
        try (Function func = getFactory().newInstance(-1, args, null, null, null)) {
            Decimal256 value = new Decimal256();
            DecimalUtil.load(value, func, null);
            Assert.assertEquals(hh, value.getHh());
            Assert.assertEquals(hl, value.getHl());
            Assert.assertEquals(lh, value.getLh());
            Assert.assertEquals(ll, value.getLl());
            Assert.assertEquals(expectedType, func.getType());
        }
    }

    protected void createFunctionAndAssertFails(ObjList<Function> args, String expectedMessage) throws SqlException {
        try {
            try (Function func = getFactory().newInstance(-1, args, null, null, null)) {
                Decimal256 value = new Decimal256();
                DecimalUtil.load(value, func, null);
                Assert.fail();
            }
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
        }
    }

    protected void createFunctionAndAssertNull(ObjList<Function> args, int expectedType) throws SqlException {
        createFunctionAndAssert(
                args,
                Decimals.DECIMAL256_HH_NULL,
                Decimals.DECIMAL256_HL_NULL,
                Decimals.DECIMAL256_LH_NULL,
                Decimals.DECIMAL256_LL_NULL,
                expectedType
        );
    }

    protected abstract FunctionFactory getFactory();
}
