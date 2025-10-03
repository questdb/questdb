/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.Decimal32Constant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.math.CeilDecimalFunctionFactory;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * We only do basic tests here, more complex tests are in {@link RoundDecimalFunctionFactoryTest} and testing
 * the different rounding modes already occurs in Decimal*Test classes.
 * We just want to ensure that this factory uses the correct rounding mode.
 */
public class CeilDecimalFunctionFactoryTest extends AbstractCairoTest {
    private final ObjList<Function> args = new ObjList<>();
    private final CeilDecimalFunctionFactory factory = new CeilDecimalFunctionFactory();

    @Test
    public void testCeilNegative() {
        args.clear();
        args.add(new Decimal32Constant(-999999, ColumnType.getDecimalType(7, 3)));
        args.add(IntConstant.newInstance(-1));
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            int value = func.getDecimal32(null);
            Assert.assertEquals(-990, value);
            Assert.assertEquals(ColumnType.getDecimalType(5, 0), func.getType());
        }
    }

    @Test
    public void testCeilPositive() {
        args.clear();
        args.add(new Decimal32Constant(100001, ColumnType.getDecimalType(7, 3)));
        args.add(IntConstant.newInstance(1));
        try (Function func = factory.newInstance(-1, args, null, configuration, sqlExecutionContext)) {
            int value = func.getDecimal32(null);
            Assert.assertEquals(1001, value);
            Assert.assertEquals(ColumnType.getDecimalType(6, 1), func.getType());
        }
    }
}