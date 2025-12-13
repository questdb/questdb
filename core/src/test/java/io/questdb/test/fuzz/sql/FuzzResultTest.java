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

package io.questdb.test.fuzz.sql;

import org.junit.Assert;
import org.junit.Test;

public class FuzzResultTest {

    @Test
    public void testParsedOk() {
        FuzzResult result = FuzzResult.parsedOk();

        Assert.assertEquals(FuzzResult.Type.PARSED_OK, result.type());
        Assert.assertTrue(result.isParsedOk());
        Assert.assertFalse(result.isSyntaxError());
        Assert.assertFalse(result.isTimeout());
        Assert.assertFalse(result.isCrash());
        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.isFailure());
        Assert.assertNull(result.exception());
        Assert.assertNull(result.errorMessage());
    }

    @Test
    public void testSyntaxError() {
        FuzzResult result = FuzzResult.syntaxError();

        Assert.assertEquals(FuzzResult.Type.SYNTAX_ERROR, result.type());
        Assert.assertFalse(result.isParsedOk());
        Assert.assertTrue(result.isSyntaxError());
        Assert.assertFalse(result.isTimeout());
        Assert.assertFalse(result.isCrash());
        Assert.assertTrue(result.isSuccess());
        Assert.assertFalse(result.isFailure());
    }

    @Test
    public void testSyntaxErrorWithMessage() {
        FuzzResult result = FuzzResult.syntaxError("unexpected token");

        Assert.assertEquals(FuzzResult.Type.SYNTAX_ERROR, result.type());
        Assert.assertTrue(result.isSyntaxError());
        Assert.assertEquals("unexpected token", result.errorMessage());
    }

    @Test
    public void testTimeout() {
        FuzzResult result = FuzzResult.timeout();

        Assert.assertEquals(FuzzResult.Type.TIMEOUT, result.type());
        Assert.assertFalse(result.isParsedOk());
        Assert.assertFalse(result.isSyntaxError());
        Assert.assertTrue(result.isTimeout());
        Assert.assertFalse(result.isCrash());
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.isFailure());
    }

    @Test
    public void testCrash() {
        RuntimeException ex = new RuntimeException("test crash");
        FuzzResult result = FuzzResult.crash(ex);

        Assert.assertEquals(FuzzResult.Type.CRASH, result.type());
        Assert.assertFalse(result.isParsedOk());
        Assert.assertFalse(result.isSyntaxError());
        Assert.assertFalse(result.isTimeout());
        Assert.assertTrue(result.isCrash());
        Assert.assertFalse(result.isSuccess());
        Assert.assertTrue(result.isFailure());
        Assert.assertSame(ex, result.exception());
    }

    @Test
    public void testSingletonInstances() {
        // PARSED_OK and SYNTAX_ERROR should be singletons
        Assert.assertSame(FuzzResult.parsedOk(), FuzzResult.parsedOk());
        Assert.assertSame(FuzzResult.syntaxError(), FuzzResult.syntaxError());
        Assert.assertSame(FuzzResult.timeout(), FuzzResult.timeout());

        // Syntax error with message should be new instance
        Assert.assertNotSame(FuzzResult.syntaxError(), FuzzResult.syntaxError("msg"));

        // Crash should always be new instance
        RuntimeException ex = new RuntimeException();
        Assert.assertNotSame(FuzzResult.crash(ex), FuzzResult.crash(ex));
    }

    @Test
    public void testToString() {
        Assert.assertEquals("PARSED_OK", FuzzResult.parsedOk().toString());
        Assert.assertEquals("SYNTAX_ERROR", FuzzResult.syntaxError().toString());
        Assert.assertEquals("TIMEOUT", FuzzResult.timeout().toString());
        Assert.assertEquals("SYNTAX_ERROR: unexpected token", FuzzResult.syntaxError("unexpected token").toString());

        RuntimeException ex = new RuntimeException("test message");
        Assert.assertTrue(FuzzResult.crash(ex).toString().contains("CRASH"));
        Assert.assertTrue(FuzzResult.crash(ex).toString().contains("RuntimeException"));
        Assert.assertTrue(FuzzResult.crash(ex).toString().contains("test message"));
    }
}
