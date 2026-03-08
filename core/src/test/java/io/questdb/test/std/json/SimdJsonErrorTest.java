/*******************************************************************************
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

package io.questdb.test.std.json;

import io.questdb.std.json.SimdJsonError;
import org.junit.Assert;
import org.junit.Test;

public class SimdJsonErrorTest {
    @Test
    public void testLastErrorCode() {
        Assert.assertEquals("TRAILING_CONTENT: Unexpected trailing content in the JSON input.", SimdJsonError.getMessage(SimdJsonError.TRAILING_CONTENT));
    }

    @Test
    public void testMemAlloc() {
        Assert.assertEquals("MEMALLOC: Error allocating memory, we're most likely out of memory", SimdJsonError.getMessage(SimdJsonError.MEMALLOC));
    }

    @Test
    public void testNegErrorCode() {
        Assert.assertEquals("Unknown error code -1", SimdJsonError.getMessage(-1));
    }

    @Test
    public void testOnePastLastErrorCode() {
        final int onePast = SimdJsonError.TRAILING_CONTENT + 1;
        Assert.assertEquals(SimdJsonError.NUM_ERROR_CODES, onePast);
        Assert.assertEquals("Unknown error code " + onePast, SimdJsonError.getMessage(onePast));
    }

    @Test
    public void testSuccess() {
        Assert.assertEquals("SUCCESS: No error", SimdJsonError.getMessage(SimdJsonError.SUCCESS));
    }

}
