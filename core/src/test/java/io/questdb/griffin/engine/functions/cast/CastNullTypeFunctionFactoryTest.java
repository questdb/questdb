/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Assert;
import org.junit.Test;


public class CastNullTypeFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testSignature() throws Exception {
        Assert.assertEquals("cast(oV)", new CastNullTypeFunctionFactory().getSignature());
    }

    @Test
    public void testCastNullToNonExisingTypeShouldFail() {
        try {
            assertQuery(null, "cast(null as NON_EXISTING_TYPE)", null, null);
            Assert.fail();
        } catch (Exception expected) {
            Assert.assertEquals("[13] invalid constant: NON_EXISTING_TYPE", expected.getMessage());
        }
    }

    @Test
    public void testCastNullToNonCastFriendlyTypeShouldFail() {
        try {
            assertQuery(null, "cast(null as CURSOR)", null, null);
            Assert.fail();
        } catch (Exception expected) {
            Assert.assertEquals("[13] invalid constant: CURSOR", expected.getMessage());
        }
    }
}
