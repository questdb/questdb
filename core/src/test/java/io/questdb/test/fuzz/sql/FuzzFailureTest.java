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

public class FuzzFailureTest {

    @Test
    public void testCrashFailure() {
        RuntimeException ex = new RuntimeException("test crash");
        FuzzFailure failure = new FuzzFailure(
                42,
                123456789L,
                987654321L,
                "SELECT a FROM t",
                FuzzResult.crash(ex),
                FuzzFailure.GenerationMode.VALID
        );

        Assert.assertEquals(42, failure.iteration());
        Assert.assertEquals(123456789L, failure.seed0());
        Assert.assertEquals(987654321L, failure.seed1());
        Assert.assertArrayEquals(new long[]{123456789L, 987654321L}, failure.seeds());
        Assert.assertEquals("SELECT a FROM t", failure.sql());
        Assert.assertEquals(FuzzResult.Type.CRASH, failure.result().type());
        Assert.assertEquals(FuzzFailure.GenerationMode.VALID, failure.generationMode());
        Assert.assertTrue(failure.isCrash());
        Assert.assertFalse(failure.isTimeout());
        Assert.assertSame(ex, failure.exception());
    }

    @Test
    public void testTimeoutFailure() {
        FuzzFailure failure = new FuzzFailure(
                100,
                111L,
                222L,
                "SELECT * FROM large_table",
                FuzzResult.timeout(),
                FuzzFailure.GenerationMode.CORRUPT
        );

        Assert.assertEquals(100, failure.iteration());
        Assert.assertEquals(FuzzResult.Type.TIMEOUT, failure.result().type());
        Assert.assertEquals(FuzzFailure.GenerationMode.CORRUPT, failure.generationMode());
        Assert.assertTrue(failure.isTimeout());
        Assert.assertFalse(failure.isCrash());
        Assert.assertNull(failure.exception());
    }

    @Test
    public void testReproductionCode() {
        RuntimeException ex = new RuntimeException("crash msg");
        FuzzFailure failure = new FuzzFailure(
                42,
                123456789L,
                987654321L,
                "SELECT a FROM t",
                FuzzResult.crash(ex),
                FuzzFailure.GenerationMode.VALID
        );

        String code = failure.toReproductionCode();

        Assert.assertTrue(code.contains("// Reproduction for failure #42"));
        Assert.assertTrue(code.contains("// Mode: VALID"));
        Assert.assertTrue(code.contains("// Result: CRASH"));
        Assert.assertTrue(code.contains("Rnd rnd = new Rnd(123456789L, 987654321L);"));
        Assert.assertTrue(code.contains("String sql = \"SELECT a FROM t\";"));
    }

    @Test
    public void testReproductionCodeWithEscaping() {
        FuzzFailure failure = new FuzzFailure(
                1,
                1L,
                2L,
                "SELECT \"col\" FROM t WHERE s = 'hello\\nworld'",
                FuzzResult.timeout(),
                FuzzFailure.GenerationMode.GARBAGE
        );

        String code = failure.toReproductionCode();

        // Check escaping
        Assert.assertTrue(code.contains("\\\"col\\\""));  // Escaped double quotes
        Assert.assertTrue(code.contains("\\\\"));  // Escaped backslash
    }

    @Test
    public void testReproductionCodeWithSpecialChars() {
        FuzzFailure failure = new FuzzFailure(
                1,
                1L,
                2L,
                "SELECT\ta\nFROM\tt",
                FuzzResult.timeout(),
                FuzzFailure.GenerationMode.VALID
        );

        String code = failure.toReproductionCode();

        Assert.assertTrue(code.contains("\\t"));  // Escaped tab
        Assert.assertTrue(code.contains("\\n"));  // Escaped newline
    }

    @Test
    public void testSummary() {
        RuntimeException ex = new RuntimeException("msg");
        FuzzFailure failure = new FuzzFailure(
                42,
                123L,
                456L,
                "SELECT a FROM t",
                FuzzResult.crash(ex),
                FuzzFailure.GenerationMode.VALID
        );

        String summary = failure.toSummary();

        Assert.assertTrue(summary.contains("Failure #42"));
        Assert.assertTrue(summary.contains("[VALID]"));
        Assert.assertTrue(summary.contains("seeds=(123L, 456L)"));
        Assert.assertTrue(summary.contains("result=CRASH"));
        Assert.assertTrue(summary.contains("RuntimeException"));
    }

    @Test
    public void testToString() {
        FuzzFailure failure = new FuzzFailure(
                42,
                123L,
                456L,
                "SELECT a FROM t",
                FuzzResult.timeout(),
                FuzzFailure.GenerationMode.CORRUPT
        );

        String str = failure.toString();

        Assert.assertTrue(str.contains("FuzzFailure"));
        Assert.assertTrue(str.contains("iteration=42"));
        Assert.assertTrue(str.contains("seed0=123"));
        Assert.assertTrue(str.contains("seed1=456"));
        Assert.assertTrue(str.contains("mode=CORRUPT"));
        Assert.assertTrue(str.contains("result=TIMEOUT"));
        Assert.assertTrue(str.contains("SELECT a FROM t"));
    }

    @Test
    public void testToStringTruncatesLongSql() {
        StringBuilder longSql = new StringBuilder("SELECT ");
        for (int i = 0; i < 50; i++) {
            if (i > 0) longSql.append(", ");
            longSql.append("col").append(i);
        }
        longSql.append(" FROM t");

        FuzzFailure failure = new FuzzFailure(
                1,
                1L,
                2L,
                longSql.toString(),
                FuzzResult.timeout(),
                FuzzFailure.GenerationMode.VALID
        );

        String str = failure.toString();

        // Should be truncated
        Assert.assertTrue(str.contains("..."));
        Assert.assertTrue(str.length() < longSql.length() + 100);  // Much shorter than original
    }

    @Test
    public void testAllGenerationModes() {
        for (FuzzFailure.GenerationMode mode : FuzzFailure.GenerationMode.values()) {
            FuzzFailure failure = new FuzzFailure(
                    1, 1L, 2L, "sql", FuzzResult.timeout(), mode
            );
            Assert.assertEquals(mode, failure.generationMode());
        }
    }
}
