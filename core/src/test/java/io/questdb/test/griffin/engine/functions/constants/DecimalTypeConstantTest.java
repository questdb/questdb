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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.griffin.engine.functions.constants.DecimalTypeConstant;
import io.questdb.std.Decimals;
import org.junit.Assert;
import org.junit.Test;

public class DecimalTypeConstantTest {
    private static final DecimalTypeConstant function = new DecimalTypeConstant(1, 0);

    @Test
    public void testConstantDecimal128() {
        Assert.assertEquals(Decimals.DECIMAL128_HI_NULL, function.getDecimal128Hi(null));
        Assert.assertEquals(Decimals.DECIMAL128_LO_NULL, function.getDecimal128Lo(null));
    }

    @Test
    public void testConstantDecimal16() {
        Assert.assertEquals(Decimals.DECIMAL16_NULL, function.getDecimal16(null));
    }

    @Test
    public void testConstantDecimal256() {
        Assert.assertEquals(Decimals.DECIMAL256_HH_NULL, function.getDecimal256HH(null));
        Assert.assertEquals(Decimals.DECIMAL256_HL_NULL, function.getDecimal256HL(null));
        Assert.assertEquals(Decimals.DECIMAL256_LH_NULL, function.getDecimal256LH(null));
        Assert.assertEquals(Decimals.DECIMAL256_LL_NULL, function.getDecimal256LL(null));
    }

    @Test
    public void testConstantDecimal32() {
        Assert.assertEquals(Decimals.DECIMAL32_NULL, function.getDecimal32(null));
    }

    @Test
    public void testConstantDecimal64() {
        Assert.assertEquals(Decimals.DECIMAL64_NULL, function.getDecimal64(null));
    }

    @Test
    public void testConstantDecimal8() {
        Assert.assertEquals(Decimals.DECIMAL8_NULL, function.getDecimal8(null));
    }
}
