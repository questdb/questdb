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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import org.junit.Assert;
import org.junit.Test;

public class DoubleConstantTest {
    @Test
    public void testConstant() {
        DoubleConstant constant = new DoubleConstant(123.45);
        Assert.assertTrue(constant.isConstant());
        Assert.assertEquals(123.45, constant.getDouble(null), 0.00001);
    }

    @Test
    public void testIsEquivalentToDistinguishesPositiveAndNegativeZero() {
        // Bit-pattern equality: +0.0 and -0.0 differ even though == returns true.
        DoubleConstant posZero = new DoubleConstant(0.0);
        DoubleConstant negZero = new DoubleConstant(-0.0);
        Assert.assertFalse(posZero.isEquivalentTo(negZero));
        Assert.assertFalse(negZero.isEquivalentTo(posZero));
        Assert.assertTrue(posZero.isEquivalentTo(new DoubleConstant(0.0)));
        Assert.assertTrue(negZero.isEquivalentTo(new DoubleConstant(-0.0)));
    }

    @Test
    public void testIsEquivalentToTreatsNaNAsEqual() {
        // Raw-bit equality makes NaN constants dedupe-able even though == would return false.
        DoubleConstant nan1 = new DoubleConstant(Double.NaN);
        DoubleConstant nan2 = new DoubleConstant(Double.NaN);
        Assert.assertTrue(nan1.isEquivalentTo(nan2));
    }

    @Test
    public void testIsEquivalentToReflexiveIdentity() {
        DoubleConstant c = new DoubleConstant(42.0);
        Assert.assertTrue(c.isEquivalentTo(c));
    }

    @Test
    public void testIsEquivalentToDifferentTypeReturnsFalse() {
        DoubleConstant c = new DoubleConstant(1.0);
        Assert.assertFalse(c.isEquivalentTo(null));
    }
}
