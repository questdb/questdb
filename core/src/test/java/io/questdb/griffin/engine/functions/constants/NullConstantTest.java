/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class NullConstantTest {
    @Test
    public void testConstant() {
        NullConstant constant = new NullConstant(0);
        Assert.assertTrue(constant.isConstant());
        Assert.assertNull(constant.getStr(null));
        Assert.assertNull(constant.getStrB(null));
        Assert.assertEquals(-1, constant.getStrLen(null));

        StringSink sink = new StringSink();
        constant.getStr(null, sink);
        Assert.assertEquals(0, sink.length());
    }
}