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

package io.questdb.test.std.json;

import io.questdb.std.json.JsonException;
import org.junit.Assert;
import org.junit.Test;

public class JsonExceptionTest {
    @Test
    public void testSuccess() {
        JsonException ex = new JsonException(JsonException.SUCCESS);
        Assert.assertEquals(JsonException.SUCCESS, ex.getCode());
        Assert.assertEquals("SUCCESS: No error", ex.getMessage());
    }

    @Test
    public void testMemAlloc() {
        JsonException ex = new JsonException(JsonException.MEMALLOC);
        Assert.assertEquals(JsonException.MEMALLOC, ex.getCode());
        Assert.assertEquals("MEMALLOC: Error allocating memory, we're most likely out of memory", ex.getMessage());
    }

    @Test
    public void testNegErrorCode() {
        JsonException ex = new JsonException(-1);
        Assert.assertEquals(-1, ex.getCode());
        Assert.assertEquals("Unknown error code -1", ex.getMessage());
    }

    @Test
    public void testLastErrorCode() {
        JsonException ex = new JsonException(JsonException.TRAILING_CONTENT);
        Assert.assertEquals(JsonException.TRAILING_CONTENT, ex.getCode());
        Assert.assertEquals("TRAILING_CONTENT: Unexpected trailing content in the JSON input.", ex.getMessage());
    }

    @Test
    public void testOnePastLastErrorCode() {
        final int onePast = JsonException.TRAILING_CONTENT + 1;
        JsonException ex = new JsonException(onePast);
        Assert.assertEquals(JsonException.NUM_ERROR_CODES, ex.getCode());
        Assert.assertEquals("Unknown error code " + onePast, ex.getMessage());
    }

}
