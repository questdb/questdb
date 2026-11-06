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

package io.questdb.test.griffin.plan.logical;

import io.questdb.griffin.plan.logical.ComparisonOperator;
import org.junit.Assert;
import org.junit.Test;

public class ComparisonOperatorTest {
    private static final long[] VALUES = {-2, -1, 0, 1, 2};

    @Test
    public void testComplementHoldsExactlyWhereTheComparisonFails() {
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            for (long left : VALUES) {
                for (long right : VALUES) {
                    Assert.assertNotEquals(operator.name(), operator.test(left, right), operator.complement().test(left, right));
                }
            }
            Assert.assertSame(operator, operator.complement().complement());
        }
    }

    @Test
    public void testMirrorSwapsTheOperands() {
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            for (long left : VALUES) {
                for (long right : VALUES) {
                    Assert.assertEquals(operator.name(), operator.test(left, right), operator.mirror().test(right, left));
                }
            }
            Assert.assertSame(operator, operator.mirror().mirror());
        }
    }

    @Test
    public void testOfReadsEveryComparisonSpelling() {
        Assert.assertSame(ComparisonOperator.EQUAL, ComparisonOperator.of("="));
        Assert.assertSame(ComparisonOperator.NOT_EQUAL, ComparisonOperator.of("!="));
        Assert.assertSame(ComparisonOperator.NOT_EQUAL, ComparisonOperator.of("<>"));
        Assert.assertSame(ComparisonOperator.LESS, ComparisonOperator.of("<"));
        Assert.assertSame(ComparisonOperator.LESS_OR_EQUAL, ComparisonOperator.of("<="));
        Assert.assertSame(ComparisonOperator.GREATER, ComparisonOperator.of(">"));
        Assert.assertSame(ComparisonOperator.GREATER_OR_EQUAL, ComparisonOperator.of(">="));
        Assert.assertNull(ComparisonOperator.of("~"));
        Assert.assertNull(ComparisonOperator.of("in"));
        Assert.assertNull(ComparisonOperator.of("not"));
    }
}
